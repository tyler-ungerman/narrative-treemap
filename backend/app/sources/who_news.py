from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="who_news",
    vertical="health",
    category="Health",
    feed_url="https://www.who.int/rss-feeds/news-english.xml",
    cadence_minutes=60,
    max_items=100,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
