from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="mit_tech_review",
    vertical="tech",
    category="Tech/Startups",
    feed_url="https://www.technologyreview.com/feed/",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
