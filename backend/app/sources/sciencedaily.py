from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="sciencedaily",
    vertical="science",
    category="Science/Research",
    feed_url="https://www.sciencedaily.com/rss/all.xml",
    cadence_minutes=60,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
