from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="statnews",
    vertical="health",
    category="Health",
    feed_url="https://www.statnews.com/feed/",
    cadence_minutes=30,
    max_items=160,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
