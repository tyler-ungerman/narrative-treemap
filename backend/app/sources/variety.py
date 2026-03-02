from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="variety",
    vertical="entertainment",
    category="Entertainment",
    feed_url="https://variety.com/feed/",
    cadence_minutes=30,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
