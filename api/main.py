import asyncio
import json
import logging
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer
from aiokafka.errors import KafkaError
from fastapi import FastAPI, HTTPException, Request, status

from shared.config import settings
from shared.schemas import BatchRequest, Event

logger = logging.getLogger("ingestion-api")
logging.basicConfig(level=logging.INFO)


@asynccontextmanager
async def lifespan(app: FastAPI):
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        acks="all",                 # wait for the broker to confirm the write
        enable_idempotence=True,    # broker dedupes producer retries
        linger_ms=5,                # wait up to 5ms to batch messages together
        compression_type="gzip",
    )
    await producer.start()
    app.state.producer = producer
    logger.info("Kafka producer started")
    yield
    await producer.stop()
    logger.info("Kafka producer stopped")


app = FastAPI(title="Event Ingestion API", lifespan=lifespan)


def to_message(event: Event) -> tuple[bytes, bytes]:
    envelope = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "event": event.model_dump(mode="json"),
    }
    return event.user_id.encode(), json.dumps(envelope).encode()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/events", status_code=status.HTTP_202_ACCEPTED)
async def ingest_event(event: Event, request: Request):
    producer: AIOKafkaProducer = request.app.state.producer
    key, value = to_message(event)
    try:
        await producer.send_and_wait(settings.raw_topic, value=value, key=key)
    except KafkaError as exc:
        logger.error("Kafka publish failed: %s", exc)
        raise HTTPException(status_code=503, detail="Event queue unavailable, retry later")
    return {"status": "accepted", "event_id": str(event.event_id)}


@app.post("/events/batch", status_code=status.HTTP_202_ACCEPTED)
async def ingest_batch(batch: BatchRequest, request: Request):
    producer: AIOKafkaProducer = request.app.state.producer
    try:
        # send() enqueues and returns a future; we enqueue all, then await all together
        futures = []
        for event in batch.events:
            key, value = to_message(event)
            futures.append(await producer.send(settings.raw_topic, value=value, key=key))
        await asyncio.gather(*futures)
    except KafkaError as exc:
        logger.error("Kafka batch publish failed: %s", exc)
        raise HTTPException(status_code=503, detail="Event queue unavailable, retry later")
    return {"status": "accepted", "count": len(batch.events)}
