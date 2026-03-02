from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="sciencedaily_health",
    vertical="health",
    category="Health",
    feed_url="https://www.sciencedaily.com/rss/health_medicine.xml",
    cadence_minutes=45,
    max_items=160,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
