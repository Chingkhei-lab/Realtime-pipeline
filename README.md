# realtime-pipeline

A small event-ingestion pipeline: FastAPI accepts events, Kafka buffers them, and an
async consumer writes them to Postgres with exactly-once effect (at-least-once delivery
plus idempotent writes via `ON CONFLICT DO NOTHING` and the UUID primary key).

```mermaid
flowchart LR
    client[Client] --> api[FastAPI :8000]
    api --> kafka[Kafka events.raw]
    kafka --> consumer[Consumer]
    consumer --> pg[(Postgres :5433\nevents)]
    consumer --> dlq[Kafka events.dlq\nbad rows]
```

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

- **Exactly-once effect (at-least-once delivery plus idempotent writes).** The consumer
  disables auto-commit and commits Kafka offsets only after the DB transaction returns. A
  crash mid-batch replays the batch, and `ON CONFLICT (event_id) DO NOTHING` plus the
  UUID primary key makes the replay a no-op. Verified: killing the consumer with 2000
  uncommitted events left `count(*) = count(DISTINCT event_id)`.
- **At least once into `events.dlq`.** The DLQ send happens before the offset commit, so
  a crash in between re-sends the message on replay.
- **Transient vs permanent failures.** Connection errors and timeouts are retried 10
  times with capped exponential backoff; if the DB never comes back the process exits
  *without* committing and the batch replays. Data Postgres refuses (e.g. `\u0000` in a
  JSONB value) is a permanent error: the batch is retried row by row, the offending row
  goes to the DLQ with its `source_partition` / `source_offset`, and the rest commit.
  A poison message therefore cannot wedge the pipeline.

## Tests

```bash
.venv/Scripts/python -m pytest -v   # 20 passed
```

The consumer tests include a mutation check for idempotency: removing
`ON CONFLICT (event_id) DO NOTHING` makes two tests fail because duplicates are
diverted to the DLQ as `UniqueViolationError`, proving the clause is what makes
replay a no-op.

## Benchmarks

All numbers on a single laptop (8 GB RAM) with API, Kafka, consumer, and Postgres
co-located. Load generator: `scripts/load_gen.py`, latency report:
`scripts/latency_report.py`.

| Run | Target rate | Achieved rate | p50 | p95 | p99 | max | Exactly-once | Daily equiv |
|-----|-------------|--------------|-----|-----|-----|-----|-------------|-------------|
| 1 | 50/s | 55/s | 0.06s | 0.73s | 0.73s | 0.73s | PASS | 4.8M |
| 2 | 60/s | 61/s | 0.05s | 0.07s | 0.45s | 0.46s | PASS | 5.3M |
| 3 | 500/s | 501/s | 0.05s | 0.06s | 0.06s | 0.44s | PASS | 43.3M |
| 4 | 200/s (+0.15 dups) | 231/s | 0.05s | 0.05s | 0.07s | 0.43s | PASS | 17.4M |
| 5 | 2000/s | 2000/s | 0.05s | 0.08s | 0.22s | 0.58s | PASS | 172.8M |
| 6 | 4000/s | 3999/s | 0.24s | 0.50s | 0.71s | 1.22s | PASS | 342.5M |
| 7 | 5000/s | 4994/s | 3.22s | 6.04s | 6.60s | 7.04s | PASS | 395.2M |
| 8 | 6000/s | 4434/s | 11.51s | 22.34s | 22.93s | 24.07s | PASS | 375.5M |

**Sustainable rate**: ~4,000 events/s with p95 < 1 s.
**Headroom**: 5M events/day ≈ 58/s → system handles 4,000/s = **68× headroom**.
**Breakdown**: 5,000+ events/s — latency degrades, achieved rate caps at ~4,400/s.
**Exactly-once guarantee**: PASS at all rates (0 lost, 0 dupes), even under duplicates,
outages, and hard kills.

| Metric | Query A: aggregate raw `events` | Query B: read `mv_hourly_stats` |
|--------|----------------------------------|----------------------------------|
| Cold read | 5.48 s | 6.6 ms |
| Warm read | 5.53 s | 4.0 ms |
| Index | `idx_events_run` on `properties->>'run_id'` (see `sql/benchmark.sql`) | n/a |
| Work done | `GROUP BY` + `count(DISTINCT user_id)` over 1,822,514 rows | read a handful of pre-aggregated rows |
| Refresh cost | n/a | 12.3 s (`REFRESH ... CONCURRENTLY`) |
| Speedup | baseline | ~830x cold / ~1,380x warm |

Same logical question on both sides (so the only difference is where the work
happens):

```sql
-- A: compute from raw events
SELECT date_trunc('hour', event_ts) AS hour, event_type,
       count(*) AS events, count(DISTINCT user_id) AS users
FROM events GROUP BY 1, 2 ORDER BY 1 DESC, 2 LIMIT 5;

-- B: read the precomputed answer
SELECT * FROM mv_hourly_stats ORDER BY hour DESC, event_type LIMIT 5;
```

- Plain views are always fresh but scan the raw table. Materialized views are fast but
  stale until refreshed, so they are refreshed on a schedule. `CONCURRENTLY` avoids
  blocking dashboard queries.
- Query A is CPU-bound (hashing for `count(DISTINCT)`), not disk-bound, so its cold and
  warm times are nearly identical and the gap is not a caching artifact.
- The ratio depends on table size (1,822,514 rows here).
- Refreshing costs ~12.3 s, about two reads of query A, so the tradeoff only pays off
  when dashboards are queried far more often than the view is refreshed.
- Use `ORDER BY hour DESC, event_type` on both sides to avoid tie-break differences.

### Caveats

- Single-laptop co-location is conservative; production partitioning and connection
  pooling will shift these numbers.
- At real production volumes you'd partition events by day and aggregate incrementally,
  instead of re-scanning the whole table.
- Run 8 was overloaded, so it isn't a sustained rate.
- Daily-equivalent figures are extrapolated from short runs.
- The data is synthetic, so the funnel's ~84% conversion is a generator artifact.
- The funnel doesn't enforce step order.
- The cause of the ~4,400/s ceiling: at 5,000/s the single consumer falls behind on partition 1 (176-message lag after 10s), confirming the consumer is the bottleneck. At ~4,400/s the lag stabilizes and p95 flattens. This was verified with kafka-consumer-groups.sh --describe during a 5,000/s load gen run.

## About

A real-time event ingestion pipeline: FastAPI accepts events, Kafka buffers them, and an
async consumer writes them to Postgres with exactly-once effect (at-least-once delivery
plus idempotent writes via `ON CONFLICT DO NOTHING` and the UUID primary key).

## Topics

kafka, fastapi, postgresql, data-engineering, exactly-once

## License

MIT

## CI

```yaml
name: CI

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:16
        env:
          POSTGRES_USER: pipeline
          POSTGRES_PASSWORD: pipeline
          POSTGRES_DB: analytics
        ports: [5432:5432]
        options: >-
          --health-cmd pg_isready
          --health-interval 10s
          --health-timeout 5s
          --health-retries 5
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with:
          python-version: '3.11'
      - run: pip install -r requirements.txt
      - run: .venv/Scripts/python -m pytest -v
```