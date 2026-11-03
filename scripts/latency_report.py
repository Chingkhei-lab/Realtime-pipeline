"""Waits for a run to drain into Postgres, then prints latency + correctness stats."""
import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
from shared.config import settings

STATS_SQL = """SELECT
  count(*)                                   AS total,
  count(DISTINCT event_id)                   AS distinct_ids,
  min(event_ts)                              AS first_event,
  max(processed_at)                          AS last_processed,
  percentile_cont(0.50) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - event_ts))  AS e2e_p50,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - event_ts))  AS e2e_p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - event_ts))  AS e2e_p99,
  max(extract(epoch FROM processed_at - event_ts))                                           AS e2e_max,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY extract(epoch FROM processed_at - ingested_at)) AS pipe_p95
FROM events
WHERE properties->>'run_id' = $1"""


async def main(args):
    conn = await asyncpg.connect(settings.database_url)
    deadline = time.time() + args.timeout
    count = 0
    while time.time() < deadline:
        count = await conn.fetchval(
            "SELECT count(*) FROM events WHERE properties->>'run_id' = $1", args.run_id
        )
        if count >= args.expected:
            break
        print(f"  waiting for drain: {count}/{args.expected}")
        await asyncio.sleep(2)

    r = await conn.fetchrow(STATS_SQL, args.run_id)
    await conn.close()
    span = (r["last_processed"] - r["first_event"]).total_seconds() if r["total"] else 0
    print(f"\n--- report for {args.run_id} ---")
    print(f"expected unique events : {args.expected}")
    print(f"rows in Postgres       : {r['total']}")
    print(f"distinct event_ids     : {r['distinct_ids']}")
    ok = r["total"] == r["distinct_ids"] == args.expected
    print(f"exactly-once check     : {'PASS' if ok else 'FAIL'} "
          f"  (lost={max(0, args.expected - r['total'])}, dupes={r['total'] - r['distinct_ids']})")
    if r["total"]:
        print(f"end-to-end latency     : p50={r['e2e_p50']:.2f}s p95={r['e2e_p95']:.2f}s "
              f"p99={r['e2e_p99']:.2f}s max={r['e2e_max']:.2f}s")
        print(f"pipeline latency (p95) : {r['pipe_p95']:.2f}s")
        print(f"drain throughput       : {r['total'] / span:.0f} events/s over {span:.1f}s")
        print(f"daily equivalent       : {r['total'] / span * 86400 / 1e6:.1f}M events/day")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-id", required=True)
    ap.add_argument("--expected", type=int, required=True)
    ap.add_argument("--timeout", type=int, default=120)
    asyncio.run(main(ap.parse_args()))