from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Callable

from app.schemas.models import SourceRawItem


Fetcher = Callable[[int], Awaitable[list[SourceRawItem]]]


@dataclass(slots=True)
class SourceDefinition:
    name: str
    vertical: str
    category: str
    parser: str
    cadence_minutes: int
    max_items: int
    failover_behavior: str
    fetcher: Fetcher


@dataclass(slots=True)
class SourceFetchOutcome:
    source: SourceDefinition
    items: list[SourceRawItem]
    fetched_at: datetime
    latency_ms: int
    error: str | None = None
