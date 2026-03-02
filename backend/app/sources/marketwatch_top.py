from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="marketwatch_top",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://feeds.marketwatch.com/marketwatch/topstories/",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
