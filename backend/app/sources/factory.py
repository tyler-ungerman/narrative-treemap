from collections.abc import Callable

from app.core.config import settings
from app.sources.base import SourceDefinition
from app.sources.rss_common import fetch_rss_feed


def build_rss_source(
    *,
    name: str,
    vertical: str,
    category: str,
    feed_url: str,
    cadence_minutes: int,
    max_items: int,
    failover_behavior: str,
) -> SourceDefinition:
    async def fetcher(limit: int):
        return await fetch_rss_feed(
            feed_url=feed_url,
            max_items=min(limit, max_items),
            timeout_seconds=settings.request_timeout_seconds,
            user_agent=settings.user_agent,
        )

    return SourceDefinition(
        name=name,
        vertical=vertical,
        category=category,
        parser="rss",
        cadence_minutes=cadence_minutes,
        max_items=max_items,
        failover_behavior=failover_behavior,
        fetcher=fetcher,
    )
