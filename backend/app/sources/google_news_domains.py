from __future__ import annotations

from urllib.parse import quote_plus

from app.sources.factory import build_rss_source

_FAILOVER = "Skip source on failure and continue with partial ingestion."
_CADENCE_MINUTES = 75
_MAX_ITEMS = 80


def _gnews_search_url(query: str) -> str:
    return (
        "https://news.google.com/rss/search"
        f"?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )


_SEARCH_FEEDS: list[tuple[str, str, str, str]] = [
    ("gnews_tech_ai_startups", "tech", "Tech/Domain", "AI startups funding"),
    ("gnews_tech_semiconductors", "tech", "Tech/Domain", "semiconductor supply chain"),
    ("gnews_tech_cloud", "tech", "Tech/Domain", "cloud infrastructure platform"),
    ("gnews_tech_mobile", "tech", "Tech/Domain", "mobile platform ecosystem"),
    ("gnews_tech_robots", "tech", "Tech/Domain", "robotics automation industry"),
    ("gnews_tech_enterprise_saas", "tech", "Tech/Domain", "enterprise SaaS updates"),
    ("gnews_tech_data_centers", "tech", "Tech/Domain", "data center expansion"),
    ("gnews_tech_open_source", "tech", "Tech/Domain", "open source maintainers"),
    ("gnews_science_space", "science", "Science/Domain", "space exploration mission"),
    ("gnews_science_quantum", "science", "Science/Domain", "quantum computing research"),
    ("gnews_science_fusion", "science", "Science/Domain", "fusion energy research"),
    ("gnews_science_climate", "science", "Science/Domain", "climate science findings"),
    ("gnews_science_neuroscience", "science", "Science/Domain", "neuroscience study"),
    ("gnews_science_biotech", "science", "Science/Domain", "biotech clinical research"),
    ("gnews_science_astronomy", "science", "Science/Domain", "astronomy telescope discovery"),
    ("gnews_world_elections", "world", "World/Domain", "national election campaign"),
    ("gnews_world_conflict", "world", "World/Domain", "geopolitical conflict update"),
    ("gnews_world_diplomacy", "world", "World/Domain", "international diplomacy summit"),
    ("gnews_world_migration", "world", "World/Domain", "migration and border policy"),
    ("gnews_world_climate_policy", "world", "World/Domain", "global climate policy"),
    ("gnews_markets_macro", "markets", "Markets/Domain", "macro economy outlook"),
    ("gnews_markets_inflation", "markets", "Markets/Domain", "inflation CPI PCE"),
    ("gnews_markets_rates", "markets", "Markets/Domain", "interest rates federal reserve"),
    ("gnews_markets_energy", "markets", "Markets/Domain", "oil gas energy markets"),
    ("gnews_markets_banking", "markets", "Markets/Domain", "banking sector earnings"),
    ("gnews_markets_equities", "markets", "Markets/Domain", "equity markets volatility"),
    ("gnews_markets_crypto", "markets", "Markets/Domain", "bitcoin ethereum regulation"),
    ("gnews_markets_supply_chain", "markets", "Markets/Domain", "global supply chain"),
    ("gnews_sports_nfl", "sports", "Sports/Domain", "NFL trade injury report"),
    ("gnews_sports_nba", "sports", "Sports/Domain", "NBA standings playoff race"),
    ("gnews_sports_mlb", "sports", "Sports/Domain", "MLB offseason transactions"),
    ("gnews_sports_soccer", "sports", "Sports/Domain", "football transfer rumors"),
    ("gnews_sports_f1", "sports", "Sports/Domain", "Formula 1 race weekend"),
    ("gnews_sports_college", "sports", "Sports/Domain", "college sports recruiting"),
    ("gnews_ent_streaming", "entertainment", "Entertainment/Domain", "streaming platform releases"),
    ("gnews_ent_box_office", "entertainment", "Entertainment/Domain", "box office performance"),
    ("gnews_ent_music", "entertainment", "Entertainment/Domain", "music industry charts"),
    ("gnews_ent_celebrity", "entertainment", "Entertainment/Domain", "celebrity interview profile"),
    ("gnews_ent_tv", "entertainment", "Entertainment/Domain", "television series renewal"),
    ("gnews_health_public", "health", "Health/Domain", "public health advisory"),
    ("gnews_health_cdc", "health", "Health/Domain", "CDC health update"),
    ("gnews_health_fda", "health", "Health/Domain", "FDA approval safety"),
    ("gnews_health_vaccines", "health", "Health/Domain", "vaccine effectiveness study"),
    ("gnews_health_outbreaks", "health", "Health/Domain", "infectious disease outbreak"),
    ("gnews_health_hospital", "health", "Health/Domain", "hospital capacity workforce"),
    ("gnews_gaming_esports", "gaming", "Gaming/Domain", "esports tournament results"),
    ("gnews_gaming_console", "gaming", "Gaming/Domain", "console game releases"),
    ("gnews_gaming_pc", "gaming", "Gaming/Domain", "PC gaming hardware launch"),
    ("gnews_gaming_live_service", "gaming", "Gaming/Domain", "live service game update"),
    ("gnews_gaming_steam", "gaming", "Gaming/Domain", "Steam top sellers"),
    ("gnews_security_ransomware", "security", "Security/Domain", "ransomware attack incident"),
    ("gnews_security_zero_day", "security", "Security/Domain", "zero day vulnerability"),
    ("gnews_security_breaches", "security", "Security/Domain", "data breach disclosure"),
    ("gnews_security_nation_state", "security", "Security/Domain", "nation state cyber operations"),
    ("gnews_security_supply_chain", "security", "Security/Domain", "software supply chain attack"),
    ("gnews_prog_python", "programming", "Programming/Domain", "python language release"),
    ("gnews_prog_rust", "programming", "Programming/Domain", "rust language release"),
    ("gnews_prog_javascript", "programming", "Programming/Domain", "javascript framework release"),
    ("gnews_prog_devops", "programming", "Programming/Domain", "devops tooling release"),
    ("gnews_prog_databases", "programming", "Programming/Domain", "database engine release"),
    ("gnews_prog_ai_frameworks", "programming", "Programming/Domain", "machine learning framework updates"),
]

SOURCES = [
    build_rss_source(
        name=name,
        vertical=vertical,
        category=category,
        feed_url=_gnews_search_url(query),
        cadence_minutes=_CADENCE_MINUTES,
        max_items=_MAX_ITEMS,
        failover_behavior=_FAILOVER,
    )
    for name, vertical, category, query in _SEARCH_FEEDS
]
