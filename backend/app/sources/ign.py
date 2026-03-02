from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="ign",
    vertical="entertainment",
    category="Entertainment/Gaming",
    feed_url="https://feeds.ign.com/ign/all",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
