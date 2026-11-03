from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from shared.schemas import BatchRequest, Event


def make(**overrides):
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


def test_valid_event_accepted():
    assert Event(**make()).user_id == "u1"


@pytest.mark.parametrize("overrides", [
    {"event_type": "teleport"},
    {"event_id": "not-a-uuid"},
    {"user_id": ""},
    {"timestamp": "2026-11-03T10:00:00"},  # naive timestamp
    {"timestamp": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()},
    {"hacker": "yes"},  # extra field
    {"event_type": "purchase", "properties": {}},
    {"event_type": "purchase", "properties": {"amount": -5}},
    {"event_type": "page_view", "properties": {}},
    {"event_type": "search", "properties": {}},
    {"properties": {f"k{i}": i for i in range(21)}},
])
def test_invalid_events_rejected(overrides):
    with pytest.raises(ValidationError):
        Event(**make(**overrides))

def test_batch_limits():
    with pytest.raises(ValidationError):
        BatchRequest(events=[])
    with pytest.raises(ValidationError):
        BatchRequest(events=[make() for _ in range(501)])