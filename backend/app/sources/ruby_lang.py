from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="ruby_lang",
    vertical="programming",
    category="Programming",
    feed_url="https://www.ruby-lang.org/en/feeds/news.rss",
    cadence_minutes=45,
    max_items=140,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
