"""Load generator: sends events to the ingestion API at a target rate."""
import argparse
import asyncio
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

TYPES = ["page_view", "click", "add_to_cart", "purchase", "search"]
WEIGHTS = [50, 25, 10, 5, 10]
URLS = ["/home", "/products", "/cart", "/checkout", "/search", "/account"]
QUERIES = ["shoes", "laptop", "phone", "headphones", "watch"]


def make_event(run_id: str) -> dict:
    et = random.choices(TYPES, weights=WEIGHTS)[0]
    props = {"run_id": run_id}
    if et == "page_view":
        props["url"] = random.choice(URLS)
    elif et == "purchase":
        props["amount"] = round(random.uniform(5, 500), 2)
        props["currency"] = "INR"
    elif et == "search":
        props["query"] = random.choice(QUERIES)
    elif et in ("click", "add_to_cart"):
        props["element"] = f"btn-{random.randint(1, 20)}"
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": et,
        "user_id": f"u{random.randint(1, 10000)}",
        "session_id": f"s{random.randint(1, 50000)}",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "properties": props,
    }


async def main(args):
    run_id = args.run_id or time.strftime("run-%H%M%S")
    interval = args.batch_size / args.rate  # seconds between batches
    total_batches = max(1, int(args.duration / interval))
    sem = asyncio.Semaphore(args.concurrency)
    recent: list[dict] = []  # pool for duplicate injection
    stats = {"fresh": 0, "dups": 0, "ok": 0, "failed": 0}
    req_times: list[float] = []

    async with httpx.AsyncClient(base_url=args.url, timeout=30) as client:

        async def send(batch: list[dict]):
            async with sem:
                t = time.perf_counter()
                try:
                    r = await client.post("/events/batch", json={"events": batch})
                    r.raise_for_status()
                    stats["ok"] += len(batch)
                except Exception as exc:
                    stats["failed"] += len(batch)
                    print(f"  request failed: {type(exc).__name__}: {exc}")
                req_times.append(time.perf_counter() - t)

        print(f"run_id={run_id} rate={args.rate}/s duration={args.duration}s "
              f"batch={args.batch_size} dup_rate={args.dup_rate}")
        start = time.perf_counter()
        tasks = []
        for i in range(total_batches):
            # sleep until this batch's scheduled time (absolute schedule = no drift)
            delay = start + i * interval - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            batch = [make_event(run_id) for _ in range(args.batch_size)]
            stats["fresh"] += len(batch)
            # simulate client retries: re-send earlier events with the SAME event_id
            n_dups = sum(random.random() < args.dup_rate for _ in batch)
            if recent and n_dups:
                batch += random.choices(recent, k=n_dups)
                stats["dups"] += n_dups
            recent.extend(batch[: args.batch_size])
            del recent[:-2000]
            tasks.append(asyncio.create_task(send(batch)))
        await asyncio.gather(*tasks)
        elapsed = time.perf_counter() - start
    req_times.sort()
    p = lambda q: req_times[int(q * (len(req_times) - 1))] * 1000
    print("\n--- generator summary ---")
    print(f"elapsed            : {elapsed:.1f}s")
    print(f"unique events sent : {stats['fresh']}")
    print(f"duplicates re-sent : {stats['dups']}")
    print(f"accepted by API    : {stats['ok']}   failed: {stats['failed']}")
    print(f"achieved rate      : {(stats['ok'] + stats['failed']) / elapsed:.0f} events/s (incl. dups)")
    print(f"API request time   : p50={p(0.5):.0f}ms p95={p(0.95):.0f}ms max={req_times[-1]*1000:.0f}ms")
    print(f"\nNext: python scripts/latency_report.py --run-id {run_id} --expected {stats['fresh']}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default="http://localhost:8000")
    ap.add_argument("--rate", type=float, default=100, help="target events/sec")
    ap.add_argument("--duration", type=float, default=30, help="seconds")
    ap.add_argument("--batch-size", type=int, default=100)
    ap.add_argument("--concurrency", type=int, default=20)
    ap.add_argument("--dup-rate", type=float, default=0.0, help="fraction of extra duplicate events")
    ap.add_argument("--run-id", default=None)
    asyncio.run(main(ap.parse_args()))