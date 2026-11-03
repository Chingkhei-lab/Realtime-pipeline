import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from uuid import uuid4

from pydantic import ValidationError

from shared.schemas import Event

good = {
    "event_id": str(uuid4()),
    "event_type": "purchase",
    "user_id": "u123",
    "session_id": "s456",
    "timestamp": (datetime.now(timezone.utc) - timedelta(minutes=1)).isoformat(),
    "properties": {"amount": 49.99, "currency": "INR"},
}
print("GOOD:", Event(**good).model_dump_json())

bad_cases = {
    "missing amount": {**good, "properties": {}},
    "naive timestamp": {**good, "timestamp": "2026-11-03T10:00:00"},
    "bad event type": {**good, "event_type": "teleport"},
    "extra field": {**good, "hacker": "yes"},
    "bad uuid": {**good, "event_id": "not-a-uuid"},
}
for name, payload in bad_cases.items():
    try:
        Event(**payload)
        print(f"FAIL: '{name}' was accepted!")
    except ValidationError as e:
        print(f"OK, rejected '{name}': {e.errors()[0]['msg']}")
