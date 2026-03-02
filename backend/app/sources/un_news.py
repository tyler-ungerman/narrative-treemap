from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="un_news",
    vertical="world",
    category="World/Geopolitics",
    feed_url="https://news.un.org/feed/subscribe/en/news/all/rss.xml",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
