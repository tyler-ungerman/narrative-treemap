from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="drugs_medical_news",
    vertical="health",
    category="Health",
    feed_url="https://www.drugs.com/feeds/medical_news.xml",
    cadence_minutes=45,
    max_items=140,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
