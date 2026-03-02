from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime

import feedparser
import httpx

from app.schemas.models import SourceRawItem


def parse_entry_datetime(entry: dict) -> datetime:
    if entry.get("published_parsed"):
        parsed = datetime(*entry["published_parsed"][:6], tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    for key in ("published", "updated"):
        text = entry.get(key)
        if not text:
            continue
        try:
            parsed = parsedate_to_datetime(text)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            continue
    return datetime.now(timezone.utc)


def parse_rss_payload(payload: bytes, max_items: int) -> list[SourceRawItem]:
    parsed = feedparser.parse(payload)
    output: list[SourceRawItem] = []
    for entry in parsed.entries[:max_items]:
        title = (entry.get("title") or "").strip()
        link = (entry.get("link") or "").strip()
        if not title or not link:
            continue
        summary = (entry.get("summary") or "").strip() or None
        output.append(
            SourceRawItem(
                title=title,
                url=link,
                published_at=parse_entry_datetime(entry),
                summary=summary,
            )
        )
    return output


async def fetch_rss_feed(
    feed_url: str,
    max_items: int,
    timeout_seconds: float,
    user_agent: str,
) -> list[SourceRawItem]:
    headers = {"User-Agent": user_agent, "Accept": "application/rss+xml, application/xml, text/xml"}
    async with httpx.AsyncClient(timeout=timeout_seconds, follow_redirects=True, headers=headers) as client:
        response = await client.get(feed_url)
        response.raise_for_status()
        return parse_rss_payload(response.content, max_items=max_items)
