from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="arxiv_physics",
    vertical="science",
    category="Science/Research",
    feed_url="https://rss.arxiv.org/rss/physics",
    cadence_minutes=60,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
