from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="polygon",
    vertical="gaming",
    category="Gaming",
    feed_url="https://www.polygon.com/rss/index.xml",
    cadence_minutes=20,
    max_items=160,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
