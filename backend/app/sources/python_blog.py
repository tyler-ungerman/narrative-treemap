from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="python_blog",
    vertical="programming",
    category="Programming",
    feed_url="https://blog.python.org/feeds/posts/default?alt=rss",
    cadence_minutes=60,
    max_items=100,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
