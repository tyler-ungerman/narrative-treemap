from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="guardian_world",
    vertical="world",
    category="World/Geopolitics",
    feed_url="https://www.theguardian.com/world/rss",
    cadence_minutes=30,
    max_items=100,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
