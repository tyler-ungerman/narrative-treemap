from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="slashdot",
    vertical="tech",
    category="Tech/Startups",
    feed_url="http://rss.slashdot.org/Slashdot/slashdotMain",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
