from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="ecb_press",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://www.ecb.europa.eu/rss/press.html",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
