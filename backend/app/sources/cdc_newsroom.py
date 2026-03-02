from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="cdc_newsroom",
    vertical="health",
    category="Health",
    feed_url="https://tools.cdc.gov/api/v2/resources/media/132608.rss",
    cadence_minutes=60,
    max_items=100,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
