from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="bbc_world",
    vertical="world",
    category="World/Geopolitics",
    feed_url="http://feeds.bbci.co.uk/news/world/rss.xml",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
