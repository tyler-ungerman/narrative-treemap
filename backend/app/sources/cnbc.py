from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="cnbc",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://www.cnbc.com/id/100003114/device/rss/rss.html",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
