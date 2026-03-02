from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="hollywood_reporter",
    vertical="entertainment",
    category="Entertainment",
    feed_url="https://www.hollywoodreporter.com/feed/",
    cadence_minutes=25,
    max_items=160,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
