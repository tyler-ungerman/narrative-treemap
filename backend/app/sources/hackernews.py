from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="hackernews",
    vertical="tech",
    category="Tech/Startups",
    feed_url="https://hnrss.org/frontpage",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
