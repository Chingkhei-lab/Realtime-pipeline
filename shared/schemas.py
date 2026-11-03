from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class EventType(str, Enum):
    PAGE_VIEW = "page_view"
    CLICK = "click"
    ADD_TO_CART = "add_to_cart"
    PURCHASE = "purchase"
    SEARCH = "search"


class Event(BaseModel):
    model_config = ConfigDict(extra="forbid")  # reject unknown fields

    event_id: UUID
    event_type: EventType
    user_id: str = Field(min_length=1, max_length=64)
    session_id: str = Field(min_length=1, max_length=64)
    timestamp: datetime
    properties: dict[str, Any] = Field(default_factory=dict)

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: datetime) -> datetime:
        if v.tzinfo is None:
            raise ValueError("timestamp must include a timezone (e.g. 2026-11-03T10:00:00Z)")
        if v > datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("timestamp is too far in the future")
        return v.astimezone(timezone.utc)

    @field_validator("properties")
    @classmethod
    def validate_properties_size(cls, v: dict[str, Any]) -> dict[str, Any]:
        if len(v) > 20:
            raise ValueError("properties can have at most 20 keys")
        return v

    @model_validator(mode="after")
    def validate_type_specific_rules(self) -> "Event":
        p = self.properties
        if self.event_type == EventType.PURCHASE:
            if "amount" not in p or not isinstance(p["amount"], (int, float)) or p["amount"] <= 0:
                raise ValueError("purchase events require a positive numeric 'amount'")
        if self.event_type == EventType.PAGE_VIEW and "url" not in p:
            raise ValueError("page_view events require a 'url'")
        if self.event_type == EventType.SEARCH and not p.get("query"):
            raise ValueError("search events require a 'query'")
        return self


class BatchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    events: list[Event] = Field(min_length=1, max_length=500)
