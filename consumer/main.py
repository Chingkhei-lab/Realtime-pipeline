import asyncio
import json
import logging
from datetime import datetime

import asyncpg
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from shared.config import settings
from shared.schemas import Event

logger = logging.getLogger("consumer")
logging.basicConfig(level=logging.INFO)

GROUP_ID = "event-writers"
BATCH_SIZE = 500

# Errors that mean "the database is unavailable", not "this data is bad"
TRANSIENT = (
    OSError,
    asyncio.TimeoutError,
    asyncpg.InterfaceError,
    asyncpg.PostgresConnectionError,
    asyncpg.CannotConnectNowError,
)
MAX_DB_RETRIES = 10  # with the backoff below, roughly 90 seconds of outage tolerated

INSERT_SQL = """
INSERT INTO events (event_id, event_type, user_id, session_id, event_ts, properties, ingested_at)
SELECT event_id, event_type, user_id, session_id, event_ts, properties::jsonb, ingested_at
FROM unnest($1::uuid[], $2::text[], $3::text[], $4::text[],
            $5::timestamptz[], $6::text[], $7::timestamptz[])
  AS t(event_id, event_type, user_id, session_id, event_ts, properties, ingested_at)
ON CONFLICT (event_id) DO NOTHING
"""


async def send_to_dlq(dlq: AIOKafkaProducer, msg, error: str) -> None:
    payload = {
        "error": error,
        "source_topic": msg.topic,
        "source_partition": msg.partition,
        "source_offset": msg.offset,
        "original_value": msg.value.decode("utf-8", errors="replace"),
    }
    await dlq.send_and_wait(settings.dlq_topic, json.dumps(payload).encode(), key=msg.key)


def parse_batch(messages):
    """Return (good, bad). good = [(msg, row)], bad = [(msg, error_string)]."""
    good = []
    bad = []
    for msg in messages:
        try:
            envelope = json.loads(msg.value)
            event = Event.model_validate(envelope["event"])
            ingested_at = datetime.fromisoformat(envelope["ingested_at"])
            row = (
                event.event_id,
                event.event_type.value,
                event.user_id,
                event.session_id,
                event.timestamp,
                json.dumps(event.properties),
                ingested_at,
            )
            good.append((msg, row))
        except (ValueError, KeyError, TypeError) as exc:
            # pydantic.ValidationError and JSONDecodeError are both ValueErrors
            bad.append((msg, f"validation: {type(exc).__name__}: {exc}"))
    return good, bad


async def write_rows(pool: asyncpg.Pool, rows) -> int:
    """Insert rows in ONE statement/transaction. Returns number actually inserted."""
    columns = list(zip(*rows))  # 7 lists, one per column
    async with pool.acquire() as conn:
        async with conn.transaction():
            status = await conn.execute(INSERT_SQL, *columns)
    return int(status.split()[-1])  # "INSERT 0 N" -> N


async def write_with_retry(pool: asyncpg.Pool, rows) -> int:
    """Retry only transient errors. Permanent errors propagate immediately."""
    for attempt in range(1, MAX_DB_RETRIES + 1):
        try:
            return await write_rows(pool, rows)
        except TRANSIENT as exc:
            logger.error("DB unavailable (attempt %d/%d): %s", attempt, MAX_DB_RETRIES, exc)
            if attempt == MAX_DB_RETRIES:
                raise  # crash WITHOUT committing; restart replays the batch
            await asyncio.sleep(min(2 ** attempt, 10))


async def process_batch(pool, dlq, messages) -> None:
    good, bad = parse_batch(messages)
    dlq_count = 0

    for msg, error in bad:
        await send_to_dlq(dlq, msg, error)
        dlq_count += 1

    inserted = 0
    if good:
        rows = [row for _, row in good]
        try:
            inserted = await write_with_retry(pool, rows)
        except TRANSIENT:
            raise  # DB is down: do not commit offsets, let the process restart
        except asyncpg.PostgresError as exc:
            # Permanent error: some row in this batch is unstorable. Find it.
            logger.warning("Batch rejected by DB (%s); isolating bad rows", exc)
            for msg, row in good:
                try:
                    inserted += await write_with_retry(pool, [row])
                except TRANSIENT:
                    raise
                except asyncpg.PostgresError as row_exc:
                    await send_to_dlq(dlq, msg, f"db_rejected: {type(row_exc).__name__}: {row_exc}")
                    dlq_count += 1

    logger.info(
        "batch=%d inserted=%d duplicates_skipped=%d dlq=%d",
        len(messages), inserted, len(good) - inserted - (dlq_count - len(bad)), dlq_count,
    )


async def main() -> None:
    pool = await asyncpg.create_pool(settings.database_url, min_size=1, max_size=5)
    dlq = AIOKafkaProducer(bootstrap_servers=settings.kafka_bootstrap_servers, acks="all")
    consumer = AIOKafkaConsumer(
        settings.raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id=GROUP_ID,
        enable_auto_commit=False,      # WE decide when an offset is committed
        auto_offset_reset="earliest",
    )
    await dlq.start()
    await consumer.start()
    logger.info("Consumer started, group=%s", GROUP_ID)
    try:
        while True:
            batches = await consumer.getmany(timeout_ms=500, max_records=BATCH_SIZE)
            messages = [m for msgs in batches.values() for m in msgs]
            if not messages:
                continue
            await process_batch(pool, dlq, messages)
            await consumer.commit()    # only AFTER the DB transaction succeeded
    finally:
        await consumer.stop()
        await dlq.stop()
        await pool.close()
        logger.info("Consumer stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
