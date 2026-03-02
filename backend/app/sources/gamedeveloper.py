from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="gamedeveloper",
    vertical="gaming",
    category="Gaming",
    feed_url="https://www.gamedeveloper.com/rss.xml",
    cadence_minutes=20,
    max_items=160,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
