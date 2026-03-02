from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="yahoo_finance",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://finance.yahoo.com/news/rssindex",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
