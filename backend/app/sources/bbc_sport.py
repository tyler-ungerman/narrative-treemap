from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="bbc_sport",
    vertical="sports",
    category="Sports",
    feed_url="http://feeds.bbci.co.uk/sport/rss.xml",
    cadence_minutes=20,
    max_items=120,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
