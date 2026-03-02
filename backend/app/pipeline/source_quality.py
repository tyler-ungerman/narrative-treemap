from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.sources.base import SourceDefinition


CATEGORY_BASE_WEIGHTS: dict[str, float] = {
    "World/Regional Local": 0.74,
    "World/Regional": 0.8,
    "World/Geopolitics": 1.06,
    "Tech/Startups": 1.02,
    "Programming": 1.08,
    "Science/Research": 1.12,
    "Business/Markets": 1.04,
    "Sports": 0.96,
    "Entertainment": 0.9,
    "Health": 1.08,
    "Gaming": 0.96,
    "Security": 1.14,
}

SOURCE_BASE_OVERRIDES: dict[str, float] = {
    "reuters_world": 1.2,
    "ft_world": 1.14,
    "economist_world": 1.12,
    "bloomberg_markets": 1.12,
    "nyt_world": 1.1,
    "bbc_world": 1.08,
    "npr_world": 1.08,
    "hackernews": 1.12,
    "arxiv_cs": 1.18,
    "arxiv_physics": 1.18,
    "nature_news": 1.16,
    "sciencedaily": 1.0,
    "sciencedaily_health": 1.02,
    "federal_reserve": 1.24,
    "ecb_press": 1.2,
    "who_news": 1.16,
    "cdc_newsroom": 1.16,
    "the_hacker_news": 1.08,
    "securityweek": 1.08,
    "darkreading": 1.06,
    "krebs_security": 1.18,
}

_SOURCE_HINT_TOKENS = (
    "news",
    "wire",
    "press",
    "blog",
    "world",
    "markets",
    "sports",
    "daily",
)


def clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(value, maximum))


def _base_weight(source: SourceDefinition) -> float:
    if source.name in SOURCE_BASE_OVERRIDES:
        return SOURCE_BASE_OVERRIDES[source.name]
    if source.name.startswith("gnews_geo_"):
        return 0.22
    if source.name.startswith("gnews_region_"):
        return 0.5

    base_from_category = CATEGORY_BASE_WEIGHTS.get(source.category, 1.0)
    source_name = source.name.lower()
    if any(token in source_name for token in _SOURCE_HINT_TOKENS):
        base_from_category *= 0.98
    return clamp(base_from_category, 0.62, 1.3)


def build_source_quality_scores(
    *,
    sources: list[SourceDefinition],
    source_health_rows: list[dict[str, Any]],
    now: datetime | None = None,
) -> dict[str, float]:
    now = now or datetime.now(timezone.utc)
    health_by_name = {
        row["source_name"]: row
        for row in source_health_rows
        if row.get("source_name")
    }
    scores: dict[str, float] = {}

    for source in sources:
        row = health_by_name.get(source.name, {})
        base = _base_weight(source)

        last_success = row.get("last_success")
        last_error = row.get("last_error")
        latency_ms = int(row.get("latency_ms") or 0)
        items_fetched = int(row.get("items_fetched") or 0)

        freshness_factor = 0.93
        if isinstance(last_success, datetime):
            age_minutes = max((now - last_success).total_seconds() / 60.0, 0.0)
            cadence = max(source.cadence_minutes, 1)
            if age_minutes <= cadence * 1.5:
                freshness_factor = 1.0
            elif age_minutes <= cadence * 3:
                freshness_factor = 0.95
            else:
                freshness_factor = 0.9

        reliability_factor = 0.88 if last_error else 1.0

        expected_items = max(source.max_items, 1)
        throughput_ratio = items_fetched / expected_items
        throughput_factor = clamp(0.9 + throughput_ratio * 0.18, 0.86, 1.06)

        latency_factor = clamp(1.03 - min(latency_ms, 5000) / 5000 * 0.14, 0.86, 1.03)

        score = base * freshness_factor * reliability_factor * throughput_factor * latency_factor
        scores[source.name] = round(clamp(score, 0.45, 1.45), 4)

    return scores
