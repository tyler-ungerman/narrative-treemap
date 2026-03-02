from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="coindesk",
    vertical="markets",
    category="Business/Markets",
    feed_url="https://www.coindesk.com/arc/outboundfeeds/rss/",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
