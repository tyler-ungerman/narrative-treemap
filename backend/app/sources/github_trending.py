from __future__ import annotations

from datetime import datetime, timezone

import httpx
from bs4 import BeautifulSoup

from app.core.config import settings
from app.schemas.models import SourceRawItem
from app.sources.base import SourceDefinition

TRENDING_URL = "https://github.com/trending?since=daily"


async def fetch_github_trending(max_items: int) -> list[SourceRawItem]:
    headers = {"User-Agent": settings.user_agent, "Accept": "text/html"}
    async with httpx.AsyncClient(timeout=settings.request_timeout_seconds, follow_redirects=True, headers=headers) as client:
        response = await client.get(TRENDING_URL)
        response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    rows = soup.select("article.Box-row")
    now = datetime.now(timezone.utc)
    items: list[SourceRawItem] = []

    for row in rows[:max_items]:
        header_link = row.select_one("h2 a")
        if not header_link:
            continue
        repo_name = " ".join(header_link.get_text(strip=True).split())
        href = header_link.get("href", "")
        description = row.select_one("p")
        summary = description.get_text(" ", strip=True) if description else None

        if not href:
            continue
        url = f"https://github.com{href.strip()}"
        items.append(
            SourceRawItem(
                title=f"GitHub Trending: {repo_name}",
                url=url,
                published_at=now,
                summary=summary,
                raw_text=summary,
            )
        )

    return items


SOURCE = SourceDefinition(
    name="github_trending",
    vertical="tech",
    category="Tech/Startups",
    parser="html_css_selectors",
    cadence_minutes=60,
    max_items=50,
    failover_behavior="Skip source for this cycle and retain previous cache.",
    fetcher=fetch_github_trending,
)
