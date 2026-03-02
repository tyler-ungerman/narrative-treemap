from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="the_hacker_news",
    vertical="security",
    category="Security",
    feed_url="https://feeds.feedburner.com/TheHackersNews",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
