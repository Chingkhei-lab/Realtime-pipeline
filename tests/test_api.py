from datetime import datetime, timedelta, timezone
from uuid import uuid4

from fastapi.testclient import TestClient
from api.main import app


class FakeProducer:
    def __init__(self):
        self.sent = []
    async def send_and_wait(self, topic, value=None, key=None):
        self.sent.append((topic, key, value))


def payload(**overrides):
    base = {
        "event_id": str(uuid4()),
        "event_type": "click",
        "user_id": "u1",
        "session_id": "s1",
        "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
        "properties": {},
    }
    base.update(overrides)
    return base


def test_valid_event_returns_202_and_publishes():
    # No `with` block, so the real Kafka lifespan is NOT started
    client = TestClient(app)
    fake = FakeProducer()
    app.state.producer = fake
    r = client.post("/events", json=payload())
    assert r.status_code == 202
    assert len(fake.sent) == 1
    assert fake.sent[0][1] == b"u1"  # keyed by user_id


def test_invalid_event_returns_422_and_publishes_nothing():
    client = TestClient(app)
    fake = FakeProducer()
    app.state.producer = fake
    r = client.post("/events", json=payload(event_type="page_view", properties={}))
    assert r.status_code == 422
    assert fake.sent == []