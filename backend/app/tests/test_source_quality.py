from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.pipeline.source_quality import build_source_quality_scores
from app.sources.base import SourceDefinition


async def _noop_fetcher(_: int):
    return []


def _source(name: str, category: str, cadence_minutes: int = 30, max_items: int = 100) -> SourceDefinition:
    return SourceDefinition(
        name=name,
        vertical="world",
        category=category,
        parser="rss",
        cadence_minutes=cadence_minutes,
        max_items=max_items,
        failover_behavior="skip",
        fetcher=_noop_fetcher,
    )


def test_source_quality_prefers_stronger_sources_with_good_health():
    now = datetime.now(timezone.utc)
    sources = [
        _source(name="arxiv_cs", category="Science/Research"),
        _source(name="gnews_geo_new_york_city", category="World/Regional Local"),
    ]
    health_rows = [
        {
            "source_name": "arxiv_cs",
            "last_success": now - timedelta(minutes=5),
            "last_error": None,
            "items_fetched": 90,
            "latency_ms": 220,
        },
        {
            "source_name": "gnews_geo_new_york_city",
            "last_success": now - timedelta(minutes=190),
            "last_error": "timeout",
            "items_fetched": 12,
            "latency_ms": 3400,
        },
    ]

    scores = build_source_quality_scores(
        sources=sources,
        source_health_rows=health_rows,
        now=now,
    )

    assert 0.45 <= scores["arxiv_cs"] <= 1.45
    assert 0.45 <= scores["gnews_geo_new_york_city"] <= 1.45
    assert scores["arxiv_cs"] > scores["gnews_geo_new_york_city"]
