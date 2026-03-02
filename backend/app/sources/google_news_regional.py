from __future__ import annotations

from urllib.parse import quote, quote_plus

from app.sources.factory import build_rss_source

_FAILOVER = "Skip source on failure and continue with partial ingestion."
_CADENCE_MINUTES = 90
_MAX_ITEMS = 70


def _gnews_geo_url(location: str) -> str:
    return (
        "https://news.google.com/rss/headlines/section/geo/"
        f"{quote(location, safe='')}?hl=en-US&gl=US&ceid=US:en"
    )


def _gnews_search_url(query: str) -> str:
    return (
        "https://news.google.com/rss/search"
        f"?q={quote_plus(query)}&hl=en-US&gl=US&ceid=US:en"
    )


_REGIONAL_FEEDS: list[tuple[str, str]] = [
    ("gnews_geo_new_york_city", "New York City"),
    ("gnews_geo_los_angeles", "Los Angeles"),
    ("gnews_geo_chicago", "Chicago"),
    ("gnews_geo_houston", "Houston"),
    ("gnews_geo_phoenix", "Phoenix"),
    ("gnews_geo_philadelphia", "Philadelphia"),
    ("gnews_geo_san_antonio", "San Antonio"),
    ("gnews_geo_san_diego", "San Diego"),
    ("gnews_geo_dallas", "Dallas"),
    ("gnews_geo_san_jose", "San Jose"),
    ("gnews_geo_austin", "Austin"),
    ("gnews_geo_jacksonville", "Jacksonville"),
    ("gnews_geo_fort_worth", "Fort Worth"),
    ("gnews_geo_columbus", "Columbus"),
    ("gnews_geo_charlotte", "Charlotte"),
    ("gnews_geo_indianapolis", "Indianapolis"),
    ("gnews_geo_san_francisco", "San Francisco"),
    ("gnews_geo_seattle", "Seattle"),
    ("gnews_geo_denver", "Denver"),
    ("gnews_geo_washington_dc", "Washington DC"),
    ("gnews_geo_boston", "Boston"),
    ("gnews_geo_detroit", "Detroit"),
    ("gnews_geo_nashville", "Nashville"),
    ("gnews_geo_las_vegas", "Las Vegas"),
    ("gnews_geo_portland", "Portland"),
    ("gnews_geo_atlanta", "Atlanta"),
    ("gnews_geo_miami", "Miami"),
    ("gnews_geo_new_orleans", "New Orleans"),
    ("gnews_geo_minneapolis", "Minneapolis"),
    ("gnews_geo_cleveland", "Cleveland"),
]

_WORLD_REGION_SEARCHES: list[tuple[str, str]] = [
    ("gnews_region_europe", "Europe regional news"),
    ("gnews_region_middle_east", "Middle East regional news"),
    ("gnews_region_africa", "Africa regional news"),
    ("gnews_region_latin_america", "Latin America regional news"),
    ("gnews_region_south_asia", "South Asia regional news"),
    ("gnews_region_southeast_asia", "Southeast Asia regional news"),
    ("gnews_region_east_asia", "East Asia regional news"),
    ("gnews_region_australia", "Australia regional news"),
    ("gnews_region_canada", "Canada regional news"),
    ("gnews_region_uk", "United Kingdom regional news"),
]

SOURCES = [
    build_rss_source(
        name=name,
        vertical="local",
        category="World/Regional Local",
        feed_url=_gnews_geo_url(location),
        cadence_minutes=_CADENCE_MINUTES,
        max_items=_MAX_ITEMS,
        failover_behavior=_FAILOVER,
    )
    for name, location in _REGIONAL_FEEDS
]

SOURCES.extend(
    [
        build_rss_source(
            name=name,
            vertical="world",
            category="World/Regional",
            feed_url=_gnews_search_url(query),
            cadence_minutes=_CADENCE_MINUTES,
            max_items=_MAX_ITEMS,
            failover_behavior=_FAILOVER,
        )
        for name, query in _WORLD_REGION_SEARCHES
    ]
)
