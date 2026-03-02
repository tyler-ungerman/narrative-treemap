from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="pcgamer",
    vertical="gaming",
    category="Gaming",
    feed_url="https://www.pcgamer.com/rss/",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
