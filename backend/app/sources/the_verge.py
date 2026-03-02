from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="the_verge",
    vertical="tech",
    category="Tech/Startups",
    feed_url="https://www.theverge.com/rss/index.xml",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
