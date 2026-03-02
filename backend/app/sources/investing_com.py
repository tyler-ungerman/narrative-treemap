from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="investing_com",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://www.investing.com/rss/news.rss",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
