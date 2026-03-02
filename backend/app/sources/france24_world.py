from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="france24_world",
    vertical="world",
    category="World/Geopolitics",
    feed_url="https://www.france24.com/en/world/rss",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
