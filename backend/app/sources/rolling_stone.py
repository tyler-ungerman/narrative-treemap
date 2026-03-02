from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="rolling_stone",
    vertical="entertainment",
    category="Entertainment",
    feed_url="https://www.rollingstone.com/music/music-news/feed/",
    cadence_minutes=25,
    max_items=160,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
