import json
import uuid
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import asyncpg
import pytest

from consumer.main import process_batch
from shared.config import settings


class FakeDLQ:
    def __init__(self):
        self.sent = []
    async def send_and_wait(self, topic, value, key=None):
        self.sent.append(json.loads(value))


@pytest.fixture
def run_id():
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
async def pool(run_id):
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=3)
    yield pool
    await pool.execute("DELETE FROM events WHERE properties->>'run_id' = $1", run_id)
    await pool.close()


_offset = iter(range(1_000_000))


def make_msg(run_id, event_id=None, properties=None):
    now = datetime.now(timezone.utc)
    envelope = {
        "ingested_at": now.isoformat(),
        "event": {
            "event_id": str(event_id or uuid.uuid4()),
            "event_type": "click",
            "user_id": "u1",
            "session_id": "s1",
            "timestamp": (now - timedelta(seconds=1)).isoformat(),
            "properties": {"run_id": run_id, **(properties or {})},
        },
    }
    return SimpleNamespace(
        topic="events.raw", partition=0, offset=next(_offset),
        key=b"u1", value=json.dumps(envelope).encode(),
    )


async def count(pool, run_id):
    return await pool.fetchval(
        "SELECT count(*) FROM events WHERE properties->>'run_id' = $1", run_id
    )


async def test_batch_is_inserted(pool, run_id):
    msgs = [make_msg(run_id) for _ in range(50)]
    await process_batch(pool, FakeDLQ(), msgs)
    assert await count(pool, run_id) == 50


async def test_duplicates_inside_one_batch_are_skipped(pool, run_id):
    eid = uuid.uuid4()
    msgs = [make_msg(run_id, eid) for _ in range(5)]
    await process_batch(pool, FakeDLQ(), msgs)
    assert await count(pool, run_id) == 1


async def test_replay_inserts_nothing_new(pool, run_id):
    """Simulates: DB commit succeeded, crash before offset commit, Kafka redelivers."""
    msgs = [make_msg(run_id) for _ in range(100)]
    await process_batch(pool, FakeDLQ(), msgs)
    await process_batch(pool, FakeDLQ(), msgs)  # redelivery
    await process_batch(pool, FakeDLQ(), msgs)  # and again
    assert await count(pool, run_id) == 100


async def test_malformed_message_goes_to_dlq(pool, run_id):
    bad = SimpleNamespace(topic="events.raw", partition=0, offset=next(_offset),
                          key=b"u1", value=b"{not json")
    dlq = FakeDLQ()
    await process_batch(pool, dlq, [make_msg(run_id), bad])
    assert await count(pool, run_id) == 1
    assert len(dlq.sent) == 1
    assert dlq.sent[0]["error"].startswith("validation:")


async def test_poison_row_isolated_rest_of_batch_commits(pool, run_id):
    good = [make_msg(run_id) for _ in range(4)]
    poison = make_msg(run_id, properties={"note": "bad\u0000value"})
    dlq = FakeDLQ()
    await process_batch(pool, dlq, good[:2] + [poison] + good[2:])
    assert await count(pool, run_id) == 4
    assert len(dlq.sent) == 1
    assert dlq.sent[0]["error"].startswith("db_rejected:")