from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="kotlin_blog",
    vertical="programming",
    category="Programming",
    feed_url="https://blog.jetbrains.com/kotlin/feed/",
    cadence_minutes=45,
    max_items=140,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
