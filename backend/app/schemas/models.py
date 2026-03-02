from dataclasses import dataclass
from datetime import datetime
from typing import Any


@dataclass(slots=True)
class Item:
    item_id: str
    source_name: str
    vertical: str
    title: str
    url: str
    published_at: datetime
    fetched_at: datetime
    summary: str | None = None
    raw_text: str | None = None


@dataclass(slots=True)
class SourceRawItem:
    title: str
    url: str
    published_at: datetime
    summary: str | None = None
    raw_text: str | None = None


@dataclass(slots=True)
class SourceHealth:
    source_name: str
    last_success: datetime | None
    last_error: str | None
    items_fetched: int
    latency_ms: int
    updated_at: datetime


TopicPayload = dict[str, Any]
