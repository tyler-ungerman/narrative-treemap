from __future__ import annotations

import re
from datetime import datetime

from typing import Mapping

TOKEN_PATTERN = re.compile(r"[a-zA-Z][a-zA-Z0-9\-]{2,}")


def compute_momentum(volume_now: float, volume_prev: float) -> float:
    return (volume_now - volume_prev) / max(volume_prev, 1)


def compute_novelty(current_terms: set[str], baseline_terms: set[str]) -> float:
    if not current_terms:
        return 0.0
    new_terms = current_terms - baseline_terms
    return len(new_terms) / len(current_terms)


def estimate_previous_volume(
    prev_items: list[dict],
    signals: set[str],
    source_weights: Mapping[str, float] | None = None,
) -> float:
    if not signals:
        return 0.0
    expanded_signals: set[str] = set()
    for signal in signals:
        expanded_signals.update(token.lower() for token in TOKEN_PATTERN.findall(signal))
    if not expanded_signals:
        return 0.0
    count = 0.0
    for item in prev_items:
        title_tokens = set(token.lower() for token in TOKEN_PATTERN.findall(item.get("title") or ""))
        summary_tokens = set(token.lower() for token in TOKEN_PATTERN.findall(item.get("summary") or ""))
        if title_tokens.intersection(expanded_signals) or summary_tokens.intersection(expanded_signals):
            source_name = item.get("source_name") or ""
            source_weight = source_weights.get(source_name, 1.0) if source_weights else 1.0
            count += max(0.2, float(source_weight))
    return count


def build_sparkline(
    timestamps: list[datetime],
    start_at: datetime,
    end_at: datetime,
    buckets: int,
) -> list[int]:
    if buckets <= 0:
        return []
    if not timestamps:
        return [0 for _ in range(buckets)]

    total_seconds = max((end_at - start_at).total_seconds(), 1.0)
    bucket_width = total_seconds / buckets
    sparkline = [0 for _ in range(buckets)]

    for timestamp in timestamps:
        delta = (timestamp - start_at).total_seconds()
        index = int(delta / bucket_width)
        if index < 0:
            index = 0
        if index >= buckets:
            index = buckets - 1
        sparkline[index] += 1
    return sparkline
