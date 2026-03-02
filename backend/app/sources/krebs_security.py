from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="krebs_security",
    vertical="security",
    category="Security",
    feed_url="https://krebsonsecurity.com/feed/",
    cadence_minutes=30,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
