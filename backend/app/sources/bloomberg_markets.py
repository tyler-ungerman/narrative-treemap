from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="bloomberg_markets",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://feeds.bloomberg.com/markets/news.rss",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
