from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="nature_news",
    vertical="science",
    category="Science/Research",
    feed_url="https://www.nature.com/nature.rss",
    cadence_minutes=120,
    max_items=80,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
