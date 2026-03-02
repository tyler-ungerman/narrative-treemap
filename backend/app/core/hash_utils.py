import hashlib
import re
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_QUERY_KEYS = {
    "utm_source",
    "utm_medium",
    "utm_campaign",
    "utm_term",
    "utm_content",
    "fbclid",
    "gclid",
}


def stable_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def normalize_title(title: str) -> str:
    cleaned = re.sub(r"[^a-z0-9\s]", " ", title.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def normalize_title_hash(title: str) -> str:
    return stable_hash(normalize_title(title))


def clean_url(url: str) -> str:
    try:
        parts = urlsplit(url)
    except Exception:
        return url
    filtered_query = [
        (key, value)
        for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if key.lower() not in TRACKING_QUERY_KEYS
    ]
    normalized_query = urlencode(filtered_query)
    return urlunsplit((parts.scheme, parts.netloc.lower(), parts.path, normalized_query, ""))
