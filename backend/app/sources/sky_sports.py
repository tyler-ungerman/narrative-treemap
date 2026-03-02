from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="sky_sports",
    vertical="sports",
    category="Sports",
    feed_url="https://www.skysports.com/rss/12040",
    cadence_minutes=20,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
