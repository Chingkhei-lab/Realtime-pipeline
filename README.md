# realtime-pipeline

A small event-ingestion pipeline: FastAPI accepts events, Kafka buffers them, and an
async consumer writes them to Postgres exactly once.

```
client -> FastAPI (:8000) -> Kafka events.raw -> consumer -> Postgres (:5433)
                                                   |
                                                   +-> Kafka events.dlq (bad rows)
```

## Ports

| Service  | Host port | Why |
|----------|-----------|-----|
| Kafka    | 9092      | PLAINTEXT listener advertised at `localhost:9092` |
| Postgres | **5433**  | a native PostgreSQL Windows service (`postgresql-x64-18`) already owns `127.0.0.1:5432` |
| API      | 8000      | uvicorn |

Inside the Postgres container the database still listens on 5432 — 5433 is only the
host-side mapping in `docker-compose.yml`. Connecting from the host therefore needs
port 5433:

```
psql "postgresql://pipeline:pipeline@localhost:5433/analytics"
```

`docker exec postgres psql -U pipeline -d analytics ...` runs *inside* the container and
uses 5432, so it never touches the host port and cannot detect a clash.

## Setup

```bash
cp .env.example .env      # DATABASE_URL points at localhost:5433
docker compose up -d      # kafka + postgres
python -m venv .venv && .venv/Scripts/pip install -r requirements.txt
docker exec -i postgres psql -U pipeline -d analytics < sql/init.sql
```

Run the two processes in separate terminals:

```bash
.venv/Scripts/python -m uvicorn api.main:app --port 8000
.venv/Scripts/python -m consumer.main
```

## Guarantees

- **Exactly once into `events`.** The consumer disables auto-commit and commits Kafka
  offsets only after the DB transaction returns. A crash mid-batch replays the batch,
  and `ON CONFLICT (event_id) DO NOTHING` plus the UUID primary key makes the replay a
  no-op. Verified: killing the consumer with 2000 uncommitted events left
  `count(*) = count(DISTINCT event_id)`.
- **At least once into `events.dlq`.** The DLQ send happens before the offset commit, so
  a crash in between re-sends the message on replay.
- **Transient vs permanent failures.** Connection errors and timeouts are retried 10
  times with capped exponential backoff; if the DB never comes back the process exits
  *without* committing and the batch replays. Data Postgres refuses (e.g. `\u0000` in a
  JSONB value) is a permanent error: the batch is retried row by row, the offending row
  goes to the DLQ with its `source_partition` / `source_offset`, and the rest commit.
  A poison message therefore cannot wedge the pipeline.
