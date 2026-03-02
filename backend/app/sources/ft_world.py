from app.sources.factory import build_rss_source

SOURCE = build_rss_source(
    name="ft_world",
    vertical="world",
    category="World/Geopolitics",
    feed_url="https://www.ft.com/world?format=rss",
    cadence_minutes=25,
    max_items=180,
    failover_behavior="Skip source on failure and continue with partial ingestion.",
)
