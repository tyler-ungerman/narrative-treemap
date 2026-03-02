from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="npr_world",
    vertical="world",
    category="World/Geopolitics",
    feed_url="https://feeds.npr.org/1004/rss.xml",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
