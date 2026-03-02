from datetime import timedelta

WINDOW_DELTAS: dict[str, timedelta] = {
    "1h": timedelta(hours=1),
    "6h": timedelta(hours=6),
    "24h": timedelta(hours=24),
    "7d": timedelta(days=7),
}

DEFAULT_WINDOW = "24h"
SPARKLINE_BUCKETS = 12


def parse_window(window: str) -> timedelta:
    if window not in WINDOW_DELTAS:
        raise ValueError(f"Unsupported window: {window}")
    return WINDOW_DELTAS[window]


def supported_windows() -> list[str]:
    return list(WINDOW_DELTAS.keys())
