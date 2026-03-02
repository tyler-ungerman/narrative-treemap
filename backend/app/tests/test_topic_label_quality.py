from __future__ import annotations

import numpy as np

from app.pipeline.topic_model import build_phrase_candidates, extract_entities


def test_extract_entities_requires_support_in_larger_clusters():
    texts = [
        "Sydney council approves budget update",
        "Sydney council debates local transport changes",
        "Sydney council signs new budget framework",
        "Sydney council discusses housing plan",
        "Sydney council confirms permit updates",
        "Man charged in Sydney case mentions Ilhan Omar once",
    ]

    entities = extract_entities(texts, max_entities=10)

    assert "Ilhan Omar" not in entities


def test_phrase_candidates_filter_single_story_noise_in_large_clusters():
    cluster_items = [
        {"title": "Interest rates outlook drives bond repricing", "source_name": "source_a"},
        {"title": "Interest rates outlook pressures growth equities", "source_name": "source_b"},
        {"title": "Interest rates outlook shifts dollar sentiment", "source_name": "source_c"},
        {"title": "Interest rates outlook dominates macro desks", "source_name": "source_d"},
        {"title": "Interest rates outlook adds volatility risk", "source_name": "source_e"},
        {"title": "Ilhan Omar unrelated local headline appears once", "source_name": "source_f"},
    ]
    similarities = np.asarray([0.95, 0.93, 0.92, 0.91, 0.9, 0.2], dtype=np.float32)

    candidates = build_phrase_candidates(
        cluster_items=cluster_items,
        similarity_scores=similarities,
        source_quality_by_name={item["source_name"]: 1.0 for item in cluster_items},
    )

    top_phrases = [phrase for phrase, _ in candidates]
    assert all("ilhan omar" not in phrase for phrase in top_phrases)

