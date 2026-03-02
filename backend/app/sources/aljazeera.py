from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="aljazeera",
    vertical="world",
    category="World/Geopolitics",
    feed_url="https://www.aljazeera.com/xml/rss/all.xml",
    cadence_minutes=30,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
