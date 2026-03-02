from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="lobsters",
    vertical="tech",
    category="Tech/Startups",
    feed_url="https://lobste.rs/rss",
    cadence_minutes=30,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
