from datetime import datetime, timedelta, timezone

import numpy as np

from app.pipeline.topic_model import build_topics


def test_build_topics_output_shape():
    now = datetime.now(timezone.utc)
    current_items = [
        {
            "item_id": "1",
            "source_name": "source_a",
            "vertical": "tech",
            "title": "AI chips improve efficiency",
            "url": "https://example.com/1",
            "published_at": now - timedelta(minutes=30),
            "summary": "New designs reduce latency",
        },
        {
            "item_id": "2",
            "source_name": "source_b",
            "vertical": "tech",
            "title": "Inference servers expand globally",
            "url": "https://example.com/2",
            "published_at": now - timedelta(minutes=20),
            "summary": "Datacenter growth accelerates",
        },
        {
            "item_id": "3",
            "source_name": "source_c",
            "vertical": "world",
            "title": "Regional talks continue",
            "url": "https://example.com/3",
            "published_at": now - timedelta(minutes=15),
            "summary": "Leaders discuss border concerns",
        },
        {
            "item_id": "4",
            "source_name": "source_d",
            "vertical": "world",
            "title": "Ceasefire proposal drafted",
            "url": "https://example.com/4",
            "published_at": now - timedelta(minutes=5),
            "summary": "Negotiators call for observers",
        },
    ]
    previous_items = [
        {
            "item_id": "p1",
            "source_name": "source_a",
            "vertical": "tech",
            "title": "AI hardware roadmap",
            "url": "https://example.com/p1",
            "published_at": now - timedelta(hours=2),
            "summary": "Roadmap announced",
        }
    ]
    baseline_items = [
        {
            "item_id": "b1",
            "source_name": "source_x",
            "vertical": "tech",
            "title": "Cloud expansion",
            "url": "https://example.com/b1",
            "published_at": now - timedelta(days=1),
            "summary": "Infrastructure grows",
        }
    ]

    embeddings = np.asarray(
        [
            [0.8, 0.2, 0.1],
            [0.79, 0.21, 0.12],
            [0.1, 0.7, 0.8],
            [0.09, 0.69, 0.81],
        ],
        dtype=np.float32,
    )

    topics, assignments, algorithm = build_topics(
        window="1h",
        current_items=current_items,
        previous_items=previous_items,
        previous_embeddings=np.asarray([[0.78, 0.22, 0.11]], dtype=np.float32),
        baseline_items=baseline_items,
        embeddings=embeddings,
        window_start=now - timedelta(hours=1),
        window_end=now,
    )

    assert len(topics) >= 2
    assert algorithm
    assert assignments

    required = {
        "topic_id",
        "label",
        "vertical",
        "volume_now",
        "momentum",
        "novelty",
        "diversity",
        "sparkline",
        "representative_items",
        "keywords",
        "entities",
        "related_topic_ids",
        "summary",
    }
    assert required.issubset(topics[0].keys())
