from __future__ import annotations

import math
import re
from datetime import datetime, timedelta, timezone
from dataclasses import dataclass
from typing import Any

DECISION_PROFILE_WEIGHTS: dict[str, dict[str, float]] = {
    "investor": {
        "momentum": 1.55,
        "novelty": 1.15,
        "volume": 1.0,
        "diversity": 0.2,
        "confidence": 0.35,
        "quality": 0.65,
    },
    "research": {
        "momentum": 1.1,
        "novelty": 1.55,
        "volume": 0.75,
        "diversity": 0.3,
        "confidence": 0.45,
        "quality": 0.45,
    },
    "operations": {
        "momentum": 1.35,
        "novelty": 0.85,
        "volume": 1.25,
        "diversity": 0.22,
        "confidence": 0.4,
        "quality": 0.65,
    },
    "security": {
        "momentum": 1.7,
        "novelty": 1.0,
        "volume": 0.95,
        "diversity": 0.28,
        "confidence": 0.42,
        "quality": 0.78,
    },
}

TRADE_THEME_RULES: list[dict[str, Any]] = [
    {
        "name": "ai_compute",
        "tokens": {
            "ai",
            "llm",
            "gpu",
            "chip",
            "chips",
            "semiconductor",
            "inference",
            "datacenter",
            "nvidia",
            "amd",
        },
        "phrases": ["language model", "foundation model", "ai model", "gpu cluster", "model training", "data center"],
        "strong_tokens": {"llm", "gpu", "nvidia", "amd", "semiconductor", "chip", "chips"},
        "preferred_verticals": {"tech", "science", "programming"},
        "require_strong": True,
        "min_hits": 2,
        "norm": 5.0,
        "tickers": ["NVDA", "AMD", "SMH", "MSFT"],
        "direction": "Long",
        "thesis": "AI compute and model tooling demand is rising.",
    },
    {
        "name": "biotech_research",
        "tokens": {"drug", "molecular", "biotech", "fda", "clinical", "alzheimer", "protein"},
        "phrases": ["drug discovery", "clinical trial", "protein folding", "molecular graph"],
        "strong_tokens": {"fda", "clinical", "biotech", "alzheimer"},
        "preferred_verticals": {"science", "health"},
        "min_hits": 2,
        "norm": 4.5,
        "tickers": ["XBI", "IBB", "REGN", "MRNA"],
        "direction": "Long",
        "thesis": "Scientific discovery cadence can re-rate biotech sentiment.",
    },
    {
        "name": "cybersecurity",
        "tokens": {"breach", "ransomware", "exploit", "security", "cyber", "zero-day"},
        "phrases": ["data breach", "zero day", "critical vulnerability"],
        "strong_tokens": {"ransomware", "exploit", "zero-day", "breach"},
        "preferred_verticals": {"security", "tech"},
        "min_hits": 2,
        "norm": 4.0,
        "tickers": ["CRWD", "PANW", "ZS", "CIBR"],
        "direction": "Long",
        "thesis": "Threat activity supports spending on security platforms.",
    },
    {
        "name": "energy_geopolitics",
        "tokens": {"ukraine", "russia", "oil", "gas", "sanction", "shipping", "hormuz", "pipeline", "supply"},
        "phrases": ["oil supply", "shipping lane", "strait of hormuz", "pipeline disruption", "energy sanctions"],
        "strong_tokens": {"oil", "gas", "sanction", "shipping", "hormuz", "pipeline", "production"},
        "preferred_verticals": {"world", "markets"},
        "require_strong": True,
        "require_strong_in_headline": True,
        "require_vertical_alignment": True,
        "min_hits": 2,
        "norm": 4.0,
        "tickers": ["XLE", "XOM", "CVX", "GLD"],
        "direction": "Long",
        "thesis": "Geopolitical risk can support energy and defensive assets.",
    },
    {
        "name": "macro_policy",
        "tokens": {"election", "congress", "tariff", "federal", "policy", "inflation", "rates", "budget", "sanction"},
        "phrases": ["fiscal policy", "trade policy", "rate decision", "policy shift", "central bank"],
        "strong_tokens": {"election", "tariff", "federal", "inflation", "rates", "sanction", "congress"},
        "preferred_verticals": {"world", "markets"},
        "require_strong": True,
        "require_strong_in_headline": True,
        "require_vertical_alignment": True,
        "min_hits": 2,
        "norm": 4.0,
        "tickers": ["SPY", "TLT", "UUP", "GLD"],
        "direction": "Hedge",
        "thesis": "Policy uncertainty tends to increase cross-asset volatility.",
    },
    {
        "name": "sports_betting_media",
        "tokens": {
            "betting",
            "wager",
            "sportsbook",
            "odds",
            "handle",
            "rights",
            "broadcast",
            "licensing",
            "legalization",
            "regulation",
            "nfl",
            "nba",
            "mlb",
        },
        "phrases": ["sports betting", "betting handle", "broadcast rights", "media rights", "sportsbook revenue", "betting regulation"],
        "strong_tokens": {"betting", "wager", "sportsbook", "handle", "rights", "broadcast", "legalization", "regulation"},
        "preferred_verticals": {"sports"},
        "require_strong": True,
        "require_strong_in_headline": True,
        "require_vertical_alignment": True,
        "min_hits": 2,
        "norm": 4.0,
        "tickers": ["DKNG", "FLUT", "MGM", "DIS"],
        "direction": "Long",
        "thesis": "Sports attention can lift engagement and betting-media exposure.",
    },
    {
        "name": "entertainment_media",
        "tokens": {"streaming", "subscriber", "box", "office", "studio", "franchise", "ratings", "ad"},
        "phrases": ["box office", "streaming platform", "subscriber growth", "ad revenue"],
        "strong_tokens": {"streaming", "subscriber", "office"},
        "preferred_verticals": {"entertainment"},
        "min_hits": 2,
        "norm": 4.0,
        "tickers": ["NFLX", "DIS", "PARA", "WBD"],
        "direction": "Long",
        "thesis": "Entertainment cycle shifts can flow into media platform narratives.",
    },
    {
        "name": "crypto",
        "tokens": {"bitcoin", "crypto", "ethereum", "blockchain", "stablecoin"},
        "phrases": ["spot bitcoin etf", "bitcoin etf", "crypto market", "ethereum etf"],
        "strong_tokens": {"bitcoin", "ethereum", "etf"},
        "preferred_verticals": {"markets", "tech"},
        "require_strong": True,
        "require_strong_in_headline": True,
        "min_hits": 2,
        "norm": 4.0,
        "tickers": ["BTC-USD", "COIN", "MSTR"],
        "direction": "Long",
        "thesis": "Crypto narrative momentum can spill into liquid crypto proxies.",
    },
]

HIGH_IMPACT_PHRASES = {
    "earnings guidance",
    "rate cut",
    "rate hike",
    "federal reserve",
    "central bank",
    "merger agreement",
    "acquisition",
    "bankruptcy filing",
    "supply disruption",
    "export controls",
    "regulatory approval",
    "sanctions package",
    "production cut",
    "ceasefire talks",
}
LOW_IMPACT_PHRASES = {
    "charged with",
    "murder",
    "recap",
    "preview",
    "highlights",
    "weekend box office",
    "trailer",
    "draw announced",
    "combine prospects",
    "lineup revealed",
    "opening for season",
    "grand opening",
    "city council",
    "county",
    "downtown",
    "museum",
    "festival",
    "community",
    "rest players",
    "transfer rumor",
    "draw",
}
SURPRISE_POSITIVE_PHRASES = {
    "unexpected",
    "surprise",
    "first time",
    "emergency",
    "unplanned",
    "sudden",
    "halted",
    "approved",
    "rejected",
    "withdrawn",
    "cut forecast",
    "raised forecast",
}
SURPRISE_NEGATIVE_PHRASES = {
    "scheduled",
    "preview",
    "recap",
    "annual",
    "weekly",
    "daily briefing",
    "opening for season",
    "preseason",
    "regular season",
    "draw",
    "combine",
}
LIQUIDITY_LINK_PHRASES = {
    "guidance",
    "revenue",
    "margin",
    "regulation",
    "sanction",
    "rate",
    "yield",
    "supply",
    "production",
    "credit",
    "default",
    "bank",
}
EDITORIAL_TEMPLATE_PHRASES = {
    "opinion",
    "commentary",
    "analysis",
    "vs",
    "face-off",
    "what to watch",
    "preview",
    "rankings",
    "best of",
    "picks",
    "recap",
}
EDITORIAL_SOURCES = {
    "seeking_alpha",
}
LIQUID_ANCHOR_TERMS = {
    "spy",
    "qqq",
    "iwm",
    "tlt",
    "uup",
    "dxy",
    "vix",
    "yield",
    "treasury",
    "crude",
    "brent",
    "bitcoin",
    "ethereum",
    "nasdaq",
    "s&p",
    "futures",
    "fed",
    "ecb",
    "boj",
    "boe",
}
DIRECT_PROXY_COMPANY_MAP = {
    "nvidia": "NVDA",
    "advanced micro devices": "AMD",
    "amd": "AMD",
    "microsoft": "MSFT",
    "meta": "META",
    "alphabet": "GOOGL",
    "google": "GOOGL",
    "amazon": "AMZN",
    "tesla": "TSLA",
    "apple": "AAPL",
    "netflix": "NFLX",
    "disney": "DIS",
    "draftkings": "DKNG",
    "coinbase": "COIN",
    "microstrategy": "MSTR",
    "palantir": "PLTR",
    "crowdstrike": "CRWD",
    "salesforce": "CRM",
    "warner bros discovery": "WBD",
    "warner bros. discovery": "WBD",
    "warner bros": "WBD",
    "whirlpool": "WHR",
}
TICKER_IN_HEADLINE_PATTERN = re.compile(r"\(([A-Z]{1,5})(?:\.[A-Z]{1,2})?\)")
TICKER_PREFIX_PATTERN = re.compile(r"^\s*([A-Z]{2,5})(?:\.[A-Z]{1,2})?\s*[:\-]")
SINGLE_NAME_CATALYST_PHRASES = {
    "earnings",
    "guidance",
    "revenue",
    "profit",
    "loss",
    "dividend",
    "buyback",
    "downgrade",
    "upgrade",
    "job cuts",
    "layoffs",
}
NEGATIVE_MOVE_TERMS = {
    "drops",
    "drop",
    "fall",
    "fell",
    "falls",
    "slide",
    "slides",
    "selloff",
    "slumps",
    "plunges",
    "plunge",
    "down",
    "cuts",
    "cut",
    "downgrade",
}
POSITIVE_MOVE_TERMS = {
    "surges",
    "surge",
    "jumps",
    "jump",
    "rises",
    "rise",
    "up",
    "beats",
    "raised",
    "approval",
}

RISK_LIMITS_BY_PROFILE: dict[str, dict[str, float | int]] = {
    "investor": {
        "max_simultaneous_themes": 3,
        "max_daily_notional_pct": 6.0,
        "max_position_notional_pct": 2.0,
        "cooldown_hours": 6,
        "max_new_trades_per_day": 5,
    },
    "research": {
        "max_simultaneous_themes": 2,
        "max_daily_notional_pct": 3.0,
        "max_position_notional_pct": 1.5,
        "cooldown_hours": 8,
        "max_new_trades_per_day": 3,
    },
    "operations": {
        "max_simultaneous_themes": 3,
        "max_daily_notional_pct": 5.0,
        "max_position_notional_pct": 1.75,
        "cooldown_hours": 8,
        "max_new_trades_per_day": 4,
    },
    "security": {
        "max_simultaneous_themes": 2,
        "max_daily_notional_pct": 4.5,
        "max_position_notional_pct": 2.25,
        "cooldown_hours": 4,
        "max_new_trades_per_day": 5,
    },
}


@dataclass
class DecisionComputation:
    topic_id: str
    score: float


def supported_profiles() -> list[str]:
    return sorted(DECISION_PROFILE_WEIGHTS.keys())


def resolve_profile(profile: str) -> tuple[str, dict[str, float]]:
    normalized = profile.strip().lower()
    if normalized not in DECISION_PROFILE_WEIGHTS:
        normalized = "investor"
    return normalized, DECISION_PROFILE_WEIGHTS[normalized]


def _weighted_now(topic: dict[str, Any]) -> float:
    weighted = topic.get("weighted_volume_now")
    if weighted is not None:
        return float(weighted)
    return float(topic.get("volume_now", 0) or 0.0)


def _weighted_prev(topic: dict[str, Any]) -> float:
    weighted = topic.get("weighted_volume_prev")
    if weighted is not None:
        return float(weighted)
    return float(topic.get("volume_prev", 0) or 0.0)


def compute_decision_score(topic: dict[str, Any], weights: dict[str, float]) -> float:
    weighted_volume = max(_weighted_now(topic), 0.0)
    momentum = float(topic.get("momentum", 0.0))
    novelty = float(topic.get("novelty", 0.0))
    diversity = float(topic.get("diversity", 0.0))
    label_confidence = float(topic.get("label_confidence", 0.45) or 0.45)
    quality_score = float(topic.get("source_quality_score", 1.0) or 1.0)
    tempered_momentum = max(-1.5, min(1.5, momentum))
    novelty_reliability = min(1.0, max(diversity, 1.0) / 4.0)
    tempered_novelty = novelty * novelty_reliability

    score = (
        weights["momentum"] * tempered_momentum
        + weights["novelty"] * tempered_novelty
        + weights["volume"] * math.log(weighted_volume + 1.0)
        + weights["diversity"] * diversity
        + weights["confidence"] * label_confidence
        + weights["quality"] * quality_score
    )
    return float(score)


def _clamp(value: float, minimum: float, maximum: float) -> float:
    return max(minimum, min(maximum, value))


TRUST_LABEL_ALIGNMENT_MIN = 0.55
TRUST_PROXY_CONFIDENCE_MIN = 0.5
TRUST_SOURCE_QUALITY_MIN = 0.72
TRUST_NOVELTY_CONFIDENCE_MIN = 0.2
TRUST_LIQUIDITY_MIN = 0.3
TRUST_MARKET_IMPACT_MIN = 0.45


def _signal_reliability(topic: dict[str, Any]) -> float:
    diversity = float(topic.get("diversity", 0) or 0)
    quality = float(topic.get("source_quality_score", 1.0) or 1.0)
    label_confidence = float(topic.get("label_confidence", 0.45) or 0.45)

    diversity_component = min(1.0, diversity / 6.0)
    quality_component = min(1.0, max(0.0, (quality - 0.55) / 0.65))
    confidence_component = min(1.0, max(0.0, label_confidence))

    return float(0.45 * diversity_component + 0.3 * quality_component + 0.25 * confidence_component)


def _build_trust_contract(
    *,
    topic: dict[str, Any],
    impact: dict[str, Any],
    matched_theme: dict[str, Any] | None,
    theme_confidence: float,
    reliability: float,
    momentum: float,
    market_impact_score: float,
) -> dict[str, Any]:
    topic_contract = topic.get("trust_contract") if isinstance(topic.get("trust_contract"), dict) else {}
    label_alignment_confidence = float(
        topic_contract.get("label_alignment_confidence", topic.get("label_confidence", 0.45)) or 0.45
    )
    source_quality_score = float(
        topic_contract.get("source_quality_score", topic.get("source_quality_score", 1.0)) or 1.0
    )
    novelty_confidence = _clamp(
        float(topic_contract.get("novelty_confidence", topic.get("novelty", 0.0)) or 0.0),
        0.0,
        1.0,
    )

    direct_proxy_ticker = str(impact.get("direct_proxy_ticker") or "").strip().upper()
    market_move_signature = bool(impact.get("market_move_signature", False))
    if direct_proxy_ticker:
        proxy_confidence = 0.68
        if market_move_signature:
            proxy_confidence += 0.2
        proxy_confidence += 0.08 * _clamp(float(impact.get("surprise_score", 0.0) or 0.0), 0.0, 1.0)
    elif matched_theme:
        proxy_confidence = 0.6 * _clamp(theme_confidence, 0.0, 1.0) + 0.4 * _clamp(reliability, 0.0, 1.0)
    else:
        proxy_confidence = 0.0
    proxy_confidence = _clamp(proxy_confidence, 0.0, 1.0)

    liquidity_link_confidence: float | None = None
    if direct_proxy_ticker or matched_theme:
        liquidity_link_confidence = _clamp(float(impact.get("liquidity_linkage", 0.0) or 0.0), 0.0, 1.0)

    warnings: list[str] = list(topic_contract.get("warnings") or [])
    if label_alignment_confidence < TRUST_LABEL_ALIGNMENT_MIN:
        warnings.append("Label alignment is below threshold; treat as unresolved narrative.")
    if proxy_confidence < TRUST_PROXY_CONFIDENCE_MIN:
        warnings.append("Proxy confidence is below threshold; no high-trust trade mapping.")
    if source_quality_score < TRUST_SOURCE_QUALITY_MIN:
        warnings.append("Source quality score is below threshold for Act now.")
    if novelty_confidence < TRUST_NOVELTY_CONFIDENCE_MIN:
        warnings.append("Novelty confidence is below threshold for Act now.")
    if liquidity_link_confidence is not None and liquidity_link_confidence < TRUST_LIQUIDITY_MIN:
        warnings.append("Liquidity linkage is below threshold for Act now.")
    if market_impact_score < TRUST_MARKET_IMPACT_MIN:
        warnings.append("Market-impact score is below threshold for Act now.")
    if momentum <= 0:
        warnings.append("Momentum is non-positive; Act now is disabled.")

    eligible_for_act_now = (
        label_alignment_confidence >= TRUST_LABEL_ALIGNMENT_MIN
        and proxy_confidence >= TRUST_PROXY_CONFIDENCE_MIN
        and source_quality_score >= TRUST_SOURCE_QUALITY_MIN
        and novelty_confidence >= TRUST_NOVELTY_CONFIDENCE_MIN
        and momentum > 0
        and market_impact_score >= TRUST_MARKET_IMPACT_MIN
        and (liquidity_link_confidence is None or liquidity_link_confidence >= TRUST_LIQUIDITY_MIN)
    )

    deduped_warnings: list[str] = []
    seen_warnings: set[str] = set()
    for warning in warnings:
        if warning in seen_warnings:
            continue
        seen_warnings.add(warning)
        deduped_warnings.append(warning)

    return {
        "label_alignment_confidence": round(label_alignment_confidence, 4),
        "proxy_confidence": round(proxy_confidence, 4),
        "source_quality_score": round(source_quality_score, 4),
        "liquidity_link_confidence": None if liquidity_link_confidence is None else round(liquidity_link_confidence, 4),
        "novelty_confidence": round(novelty_confidence, 4),
        "eligible_for_act_now": eligible_for_act_now,
        "warnings": deduped_warnings,
    }


def _resolve_lead_keyword(topic: dict[str, Any]) -> str:
    keywords = [str(value).strip().lower() for value in (topic.get("keywords") or []) if str(value).strip()]
    if not keywords:
        return "narrative"
    top_item = (topic.get("representative_items") or [{}])[0]
    headline = str(top_item.get("title") or "").lower()
    headline_tokens = set(token for token in re.findall(r"[a-z0-9]+", headline) if token)
    for keyword in keywords:
        keyword_tokens = set(token for token in re.findall(r"[a-z0-9]+", keyword) if token)
        if keyword_tokens and keyword_tokens.intersection(headline_tokens):
            return keyword
    return keywords[0]


def classify_action_bucket(
    *,
    score: float,
    momentum: float,
    reliability: float,
    diversity: int,
    theme_confidence: float,
    market_impact_score: float,
    economic_relevance: float,
    surprise_score: float,
    liquidity_linkage: float,
    editorial_noise: bool,
    market_move_signature: bool,
    has_direct_proxy: bool,
    is_local: bool,
    profile: str,
) -> str:
    min_diversity_for_act_now = 3 if profile in {"investor", "operations", "security"} else 2
    min_reliability_for_act_now = 0.5 if profile in {"investor", "operations", "security"} else 0.45
    min_theme_confidence_for_act_now = 0.45 if profile in {"investor", "operations"} else 0.35

    if is_local and profile in {"investor", "operations"}:
        return "Ignore"
    if market_impact_score < 0.18 or economic_relevance < 0.2 or liquidity_linkage < 0.08:
        return "Ignore"
    if editorial_noise and market_impact_score < 0.68 and surprise_score < 0.45:
        return "Ignore"
    # Negative momentum cannot produce "Act now" in the current strategy set.
    if momentum < 0:
        if has_direct_proxy and market_move_signature and market_impact_score >= 0.55 and liquidity_linkage >= 0.35:
            return "Monitor"
        return "Ignore" if momentum <= -0.08 else "Monitor"
    if (
        has_direct_proxy
        and market_move_signature
        and reliability >= 0.45
        and diversity >= 3
        and market_impact_score >= 0.5
        and economic_relevance >= 0.45
        and liquidity_linkage >= 0.35
        and momentum > 0.0
        and abs(momentum) >= 0.01
    ):
        return "Act now"
    if market_impact_score < 0.48 or economic_relevance < 0.45:
        if has_direct_proxy and market_move_signature and market_impact_score >= 0.4 and liquidity_linkage >= 0.3:
            return "Monitor"
        if momentum > 0.12 and (economic_relevance >= 0.36 or liquidity_linkage >= 0.28 or market_move_signature):
            return "Monitor"
        return "Ignore"
    if surprise_score < 0.25:
        if market_move_signature or has_direct_proxy:
            return "Monitor"
        return "Ignore" if momentum <= 0.35 else "Monitor"
    if diversity < 2 or reliability < 0.35:
        return "Monitor" if momentum > 0.15 else "Ignore"
    if (
        score >= 3.0
        and momentum >= 0.15
        and reliability >= min_reliability_for_act_now
        and (theme_confidence >= min_theme_confidence_for_act_now or has_direct_proxy)
        and diversity >= min_diversity_for_act_now
        and market_impact_score >= 0.42
        and economic_relevance >= 0.4
        and liquidity_linkage >= 0.28
        and (surprise_score >= 0.18 or market_move_signature or has_direct_proxy)
    ):
        return "Act now"
    if score >= 2.2 and momentum > -0.25:
        return "Monitor"
    return "Ignore"


def build_topic_signals(topics: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "topic_id": topic.get("topic_id"),
            "label": topic.get("label"),
            "vertical": topic.get("vertical"),
            "momentum": float(topic.get("momentum", 0.0)),
            "weighted_volume_now": _weighted_now(topic),
            "weighted_volume_prev": _weighted_prev(topic),
            "diversity": int(topic.get("diversity", 0) or 0),
        }
        for topic in topics
        if topic.get("topic_id")
    ]


def _bucket_rationale(topic: dict[str, Any], action_bucket: str, impact: dict[str, Any] | None = None) -> str:
    momentum_pct = int(round(float(topic.get("momentum", 0.0)) * 100))
    diversity = int(topic.get("diversity", 0) or 0)
    quality = float(topic.get("source_quality_score", 1.0) or 1.0)
    lead_keyword = _resolve_lead_keyword(topic)

    if action_bucket == "Act now":
        message = (
            f"Momentum is {momentum_pct:+d}% with {diversity} sources confirming the signal; "
            f"coverage quality is {quality:.2f} and '{lead_keyword}' is central."
        )
    elif action_bucket == "Monitor":
        message = (
            f"Signal is mixed ({momentum_pct:+d}% momentum) but distributed across {diversity} sources. "
            f"'{lead_keyword}' remains active and worth tracking."
        )
    else:
        message = (
            f"Signal is cooling ({momentum_pct:+d}% momentum) with weaker urgency despite {diversity} sources. "
            f"Keep '{lead_keyword}' in backlog unless momentum reverses."
        )

    if impact:
        impact_note = str(impact.get("impact_reason") or "").strip()
        if impact_note:
            message = f"{message} Market transmission check: {impact_note}."
        if action_bucket != "Act now" and float(impact.get("impact_score", 0.0) or 0.0) < 0.6:
            message = f"{message} Economic transmission to liquid assets is weak."
    return message


def _bucket_next_step(action_bucket: str, window: str) -> str:
    if action_bucket == "Act now":
        return f"Validate within the next {window}: read top links, set alerts, and assign an owner."
    if action_bucket == "Monitor":
        return "Track for two refresh cycles and trigger an alert if momentum rises above +15%."
    return "No immediate action. Revisit only if novelty rises or momentum turns positive."


def _compose_signal_text(topic: dict[str, Any]) -> str:
    top_item = (topic.get("representative_items") or [{}])[0]
    title = str(top_item.get("title") or "").strip()
    keywords = [str(value) for value in (topic.get("keywords") or [])[:3] if str(value).strip()]
    if title:
        return f"Acting on: {title}"
    if keywords:
        return f"Acting on: {' / '.join(keywords)} narrative build-up"
    return f"Acting on: {topic.get('label', 'narrative signal')}"


def _topic_tokens(topic: dict[str, Any]) -> set[str]:
    parts = [
        str(topic.get("label") or ""),
        " ".join(str(value) for value in (topic.get("keywords") or [])),
        " ".join(str(value) for value in (topic.get("entities") or [])),
    ]
    top_item = (topic.get("representative_items") or [{}])[0]
    parts.append(str(top_item.get("title") or ""))
    text = " ".join(parts).lower()
    return set(token for token in re.findall(r"[a-z0-9]+", text) if token)


def _topic_text(topic: dict[str, Any]) -> str:
    parts = [
        str(topic.get("label") or ""),
        " ".join(str(value) for value in (topic.get("keywords") or [])),
        " ".join(str(value) for value in (topic.get("entities") or [])),
    ]
    top_item = (topic.get("representative_items") or [{}])[0]
    parts.append(str(top_item.get("title") or ""))
    return " ".join(parts).lower()


def _extract_headline_proxy_ticker(headline_original: str) -> str | None:
    prefix_match = TICKER_PREFIX_PATTERN.search(headline_original)
    if prefix_match:
        candidate = str(prefix_match.group(1) or "").upper()
        if 2 <= len(candidate) <= 5:
            return candidate

    for match in TICKER_IN_HEADLINE_PATTERN.findall(headline_original):
        candidate = match.upper()
        if 1 <= len(candidate) <= 5:
            return candidate

    headline_lower = headline_original.lower()
    for company_name, ticker in DIRECT_PROXY_COMPANY_MAP.items():
        if company_name in headline_lower:
            return ticker
    return None


def _extract_context_proxy_ticker(topic: dict[str, Any]) -> str | None:
    context_text = " ".join(
        [
            str(topic.get("label") or ""),
            " ".join(str(value) for value in (topic.get("entities") or [])),
            " ".join(str(value) for value in (topic.get("keywords") or [])),
        ]
    ).lower()
    for company_name, ticker in DIRECT_PROXY_COMPANY_MAP.items():
        if company_name in context_text:
            return ticker
    return None


def _extract_direct_proxy_ticker(topic: dict[str, Any]) -> str | None:
    top_item = (topic.get("representative_items") or [{}])[0]
    headline_original = str(top_item.get("title") or "")
    headline_proxy = _extract_headline_proxy_ticker(headline_original)
    if headline_proxy:
        return headline_proxy
    return _extract_context_proxy_ticker(topic)


def _is_editorial_story(headline: str, context_text: str, source_name: str) -> bool:
    headline_text = headline.lower()
    if any(phrase in headline_text for phrase in EDITORIAL_TEMPLATE_PHRASES):
        return True
    if source_name in EDITORIAL_SOURCES and (
        " vs " in headline_text or "face-off" in headline_text or "commentary" in headline_text
    ):
        return True
    if "daily briefing" in headline_text:
        return True
    return False


def _has_market_move_signature(headline: str, context_text: str) -> bool:
    text = f"{headline} {context_text}"
    if re.search(
        r"\b(stock|shares?|futures)\b.{0,32}\b(up|down|drop|drops|fall|falls|fell|slide|slides|rose|surge|jump|plunge|selloff)\b",
        text,
    ):
        return True
    if re.search(r"\b(up|down|drop|drops|fall|falls|fell|slide|slides|rose|surge|jump|plunge|selloff)\s+\d{1,3}%", text):
        return True
    return False


def _infer_direct_proxy_direction(topic: dict[str, Any]) -> str:
    top_item = (topic.get("representative_items") or [{}])[0]
    headline = str(top_item.get("title") or "").lower()
    headline_tokens = set(token for token in re.findall(r"[a-z0-9]+", headline) if token)
    if headline_tokens.intersection(NEGATIVE_MOVE_TERMS):
        return "Short"
    if headline_tokens.intersection(POSITIVE_MOVE_TERMS):
        return "Long"
    return "Long"


def _market_impact_assessment(
    topic: dict[str, Any],
    matched_theme: dict[str, Any] | None,
    reliability: float,
) -> dict[str, float | str]:
    top_item = (topic.get("representative_items") or [{}])[0]
    headline = str(top_item.get("title") or topic.get("label") or "").lower()
    context_text = " ".join(
        [
            _topic_text(topic),
            str(topic.get("summary") or ""),
            str(topic.get("search_corpus") or ""),
        ]
    ).lower()
    diversity = int(topic.get("diversity", 0) or 0)
    momentum = float(topic.get("momentum", 0.0) or 0.0)
    quality = float(topic.get("source_quality_score", 1.0) or 1.0)
    vertical = str(topic.get("vertical") or "").lower()
    source_name = str(top_item.get("source_name") or "")
    theme_confidence = float(matched_theme.get("confidence", 0.0) or 0.0) if matched_theme else 0.0
    headline_direct_proxy_ticker = _extract_headline_proxy_ticker(str(top_item.get("title") or ""))
    context_direct_proxy_ticker = _extract_context_proxy_ticker(topic)
    direct_proxy_ticker = headline_direct_proxy_ticker or context_direct_proxy_ticker
    proxy_resolution_basis = (
        "headline"
        if headline_direct_proxy_ticker
        else "context"
        if context_direct_proxy_ticker
        else "none"
    )
    proxy_integrity = "none"
    if direct_proxy_ticker:
        proxy_integrity = "strong"
    if headline_direct_proxy_ticker and context_direct_proxy_ticker and headline_direct_proxy_ticker != context_direct_proxy_ticker:
        proxy_integrity = "mismatch"
    editorial_noise = _is_editorial_story(headline, context_text, source_name)
    market_move_signature = _has_market_move_signature(headline, context_text)
    topic_tokens = set(token for token in re.findall(r"[a-z0-9]+", context_text) if token)
    anchor_hits = sum(1 for token in LIQUID_ANCHOR_TERMS if token in topic_tokens or token in headline)
    if direct_proxy_ticker:
        anchor_hits += 2
    has_liquid_anchor = anchor_hits > 0 or market_move_signature

    high_impact_hits = sum(1 for phrase in HIGH_IMPACT_PHRASES if phrase in headline or phrase in context_text)
    low_impact_hits = sum(1 for phrase in LOW_IMPACT_PHRASES if phrase in headline or phrase in context_text)
    surprise_positive_hits = sum(1 for phrase in SURPRISE_POSITIVE_PHRASES if phrase in headline or phrase in context_text)
    surprise_negative_hits = sum(1 for phrase in SURPRISE_NEGATIVE_PHRASES if phrase in headline or phrase in context_text)
    liquidity_hits = sum(1 for phrase in LIQUIDITY_LINK_PHRASES if phrase in headline or phrase in context_text)

    quality_component = _clamp((quality - 0.55) / 0.8, 0.0, 1.0)
    diversity_component = _clamp(diversity / 10.0, 0.0, 1.0)
    momentum_component = _clamp(momentum / 1.2, 0.0, 1.0)
    high_component = _clamp(high_impact_hits / 2.0, 0.0, 1.0)
    low_component = _clamp(low_impact_hits / 2.0, 0.0, 1.0)
    liquidity_component = _clamp(liquidity_hits / 3.0, 0.0, 1.0)
    anchor_component = _clamp(anchor_hits / 3.0, 0.0, 1.0)
    editorial_penalty = 0.3 if editorial_noise else 0.0

    local_penalty = 0.0
    if vertical == "local" or source_name.startswith("gnews_geo_"):
        local_penalty += 0.4
    if source_name.startswith("gnews_region_"):
        local_penalty += 0.16
    if vertical == "sports" and low_impact_hits > 0:
        local_penalty += 0.22
    if source_name.startswith("gnews_"):
        local_penalty += 0.08

    theme_economic_component = 0.14 * theme_confidence if has_liquid_anchor else 0.04 * theme_confidence

    economic_relevance = _clamp(
        0.12
        + 0.34 * high_component
        + 0.2 * quality_component
        + 0.14 * diversity_component
        + theme_economic_component
        + 0.08 * momentum_component
        + (0.18 if market_move_signature else 0.0)
        - 0.48 * low_component
        - 0.3 * editorial_penalty
        - local_penalty,
        0.0,
        1.0,
    )
    if editorial_noise:
        economic_relevance = min(economic_relevance, 0.48)

    surprise_score = _clamp(
        0.1
        + 0.34 * _clamp(surprise_positive_hits / 2.0, 0.0, 1.0)
        - 0.52 * _clamp(surprise_negative_hits / 2.0, 0.0, 1.0)
        + 0.14 * momentum_component
        + (0.14 if market_move_signature else 0.0)
        - 0.26 * editorial_penalty,
        0.0,
        1.0,
    )
    if editorial_noise or (vertical in {"sports", "entertainment"} and low_impact_hits > 0):
        surprise_score = min(surprise_score, 0.35)

    theme_liquidity_component = 0.24 * theme_confidence if has_liquid_anchor else 0.05 * theme_confidence
    liquidity_linkage = _clamp(
        (0.16 if matched_theme and has_liquid_anchor else 0.0)
        + theme_liquidity_component
        + 0.18 * liquidity_component
        + 0.14 * anchor_component
        + (0.15 if market_move_signature else 0.0)
        + 0.12 * quality_component
        + 0.08 * reliability
        - 0.26 * low_component
        - 0.32 * local_penalty
        - 0.22 * editorial_penalty
        - (0.14 if not has_liquid_anchor else 0.0),
        0.0,
        1.0,
    )
    if not has_liquid_anchor and not direct_proxy_ticker:
        liquidity_linkage = min(liquidity_linkage, 0.44)
    if source_name.startswith("gnews_geo_"):
        liquidity_linkage = min(liquidity_linkage, 0.34)
    if vertical in {"entertainment", "sports"} and not direct_proxy_ticker and not market_move_signature:
        liquidity_linkage = min(liquidity_linkage, 0.34)

    impact_score = _clamp(
        0.52 * economic_relevance + 0.26 * surprise_score + 0.22 * liquidity_linkage,
        0.0,
        1.0,
    )

    verdict = "strong" if impact_score >= 0.7 else "mixed" if impact_score >= 0.52 else "weak"
    proxy_status = "resolved" if direct_proxy_ticker else "unresolved"
    reason = (
        f"impact={int(round(impact_score * 100))}% "
        f"(economic={int(round(economic_relevance * 100))}%, "
        f"surprise={int(round(surprise_score * 100))}%, "
        f"liquidity={int(round(liquidity_linkage * 100))}%, "
        f"proxy={proxy_status}, "
        f"proxy_basis={proxy_resolution_basis}, "
        f"proxy_integrity={proxy_integrity})"
    )
    return {
        "economic_relevance": round(economic_relevance, 4),
        "surprise_score": round(surprise_score, 4),
        "liquidity_linkage": round(liquidity_linkage, 4),
        "impact_score": round(impact_score, 4),
        "impact_verdict": verdict,
        "impact_reason": reason,
        "editorial_noise": editorial_noise,
        "direct_proxy_ticker": direct_proxy_ticker or "",
        "headline_direct_proxy_ticker": headline_direct_proxy_ticker or "",
        "proxy_resolution_basis": proxy_resolution_basis,
        "proxy_integrity": proxy_integrity,
        "market_move_signature": market_move_signature,
        "has_liquid_anchor": has_liquid_anchor,
    }


def _select_trade_theme(topic: dict[str, Any]) -> dict[str, Any] | None:
    if str(topic.get("vertical") or "").lower() == "local":
        return None
    vertical = str(topic.get("vertical") or "").lower()
    tokens = _topic_tokens(topic)
    text = _topic_text(topic)
    headline = str(((topic.get("representative_items") or [{}])[0]).get("title") or "").lower()
    headline_tokens = set(token for token in re.findall(r"[a-z0-9]+", headline) if token)
    ranked_rules: list[dict[str, Any]] = []
    for rule in TRADE_THEME_RULES:
        rule_tokens = set(rule.get("tokens", set()))
        rule_strong_tokens = set(rule.get("strong_tokens", set()))
        matched_tokens = sorted(tokens.intersection(rule_tokens))
        matched_strong_tokens = sorted(tokens.intersection(rule_strong_tokens))
        matched_phrases = [phrase for phrase in rule.get("phrases", []) if phrase in text]
        token_hits = len(matched_tokens)
        strong_hits = len(matched_strong_tokens)
        phrase_hits = len(matched_phrases)
        if bool(rule.get("require_strong")) and strong_hits <= 0:
            continue
        if bool(rule.get("require_strong_in_headline")):
            headline_strong_matches = sorted(headline_tokens.intersection(rule_strong_tokens))
            headline_phrase_matches = [phrase for phrase in rule.get("phrases", []) if phrase in headline]
            headline_strong_hits = len(headline_strong_matches)
            headline_phrase_hits = len(headline_phrase_matches)
            if headline_strong_hits <= 0 and headline_phrase_hits <= 0:
                continue
        else:
            headline_strong_matches = sorted(headline_tokens.intersection(rule_strong_tokens))
            headline_phrase_matches = [phrase for phrase in rule.get("phrases", []) if phrase in headline]
        raw_hits = float(token_hits + 1.8 * strong_hits + 1.4 * phrase_hits)
        if raw_hits < float(rule.get("min_hits", 1)):
            continue
        base_confidence = min(1.0, raw_hits / float(rule.get("norm", 4.0)))
        preferred_verticals = set(rule.get("preferred_verticals", set()))
        vertical_alignment = vertical in preferred_verticals if preferred_verticals else True
        if bool(rule.get("require_vertical_alignment")) and not vertical_alignment:
            continue
        if preferred_verticals:
            if vertical_alignment:
                base_confidence += 0.12
            else:
                base_confidence -= 0.08
        confidence = max(0.0, min(1.0, base_confidence))
        if confidence <= 0.2:
            continue
        ranked_rules.append(
            {
                "rule": rule,
                "confidence": confidence,
                "raw_hits": raw_hits,
                "matched_tokens": matched_tokens,
                "matched_strong_tokens": matched_strong_tokens,
                "matched_phrases": matched_phrases,
                "headline_strong_matches": headline_strong_matches,
                "headline_phrase_matches": headline_phrase_matches,
                "vertical_alignment": vertical_alignment,
            }
        )
    if not ranked_rules:
        return None
    ranked_rules.sort(key=lambda row: float(row["confidence"]), reverse=True)
    return ranked_rules[0]


def _explicit_execution_plan(
    topic: dict[str, Any],
    action_bucket: str,
    window: str,
    reliability: float,
    theme_confidence: float,
    impact: dict[str, Any],
    matched_theme: dict[str, Any] | None,
) -> tuple[str, list[str], str, str, float, str | None]:
    momentum_pct = int(round(float(topic.get("momentum", 0.0)) * 100))
    novelty_pct = int(round(float(topic.get("novelty", 0.0)) * 100))
    top_item = (topic.get("representative_items") or [{}])[0]
    headline = str(top_item.get("title") or "").lower()
    diversity = int(topic.get("diversity", 0) or 0)
    theme = matched_theme["rule"] if matched_theme else None
    detected_theme_confidence = float(matched_theme.get("confidence", 0.0) or 0.0) if matched_theme else 0.0
    effective_trade_confidence = max(0.0, min(1.0, 0.5 * reliability + 0.5 * max(theme_confidence, detected_theme_confidence)))
    impact_score = float(impact.get("impact_score", 0.0) or 0.0)
    surprise_score = float(impact.get("surprise_score", 0.0) or 0.0)
    economic_relevance = float(impact.get("economic_relevance", 0.0) or 0.0)
    liquidity_linkage = float(impact.get("liquidity_linkage", 0.0) or 0.0)
    market_move_signature = bool(impact.get("market_move_signature", False))
    direct_proxy_ticker = str(impact.get("direct_proxy_ticker") or "").strip().upper()
    direct_proxy_direction = _infer_direct_proxy_direction(topic) if direct_proxy_ticker else "Long"
    single_name_catalyst = bool(direct_proxy_ticker) and any(
        phrase in headline for phrase in SINGLE_NAME_CATALYST_PHRASES
    )
    position_size_pct = round(0.35 + 1.85 * effective_trade_confidence, 2)
    entry_trigger = (
        f"Entry trigger: next refresh must keep momentum >= +20%, novelty >= +20%, and diversity >= {max(3, min(6, diversity))}."
    )

    if action_bucket == "Ignore":
        return (
            "No trade deployment. Keep this topic in backlog and re-open only if momentum turns positive.",
            [],
            "None",
            "Risk guardrail: do not allocate capital while momentum is negative.",
            0.0,
            None,
        )

    if single_name_catalyst and action_bucket in {"Act now", "Monitor"}:
        if (
            action_bucket == "Act now"
            and momentum_pct > 0
            and market_move_signature
            and impact_score >= 0.52
            and economic_relevance >= 0.4
            and surprise_score >= 0.12
            and liquidity_linkage >= 0.3
        ):
            plan = (
                f"Single-name catalyst detected ({direct_proxy_ticker}). "
                f"Deploy {direct_proxy_direction} {direct_proxy_ticker} with {position_size_pct:.2f}% starter size. "
                f"{entry_trigger} Re-check after {window}."
            )
            guardrail = (
                "Risk guardrail: reduce/exit if momentum turns negative, market-move signature disappears, "
                "or liquidity linkage falls below 30%."
            )
            return (
                plan,
                [direct_proxy_ticker],
                direct_proxy_direction,
                guardrail,
                position_size_pct,
                "single_name_direct_proxy",
            )
        return (
            f"Single-name catalyst detected ({direct_proxy_ticker}) but confirmation is incomplete. "
            "Monitor one additional refresh before deployment.",
            [direct_proxy_ticker],
            direct_proxy_direction,
            "Risk guardrail: no deployment until market-move signature persists and impact stays above 52%.",
            0.0,
            "single_name_direct_proxy",
        )

    if direct_proxy_ticker and market_move_signature and action_bucket in {"Act now", "Monitor"}:
        if action_bucket == "Act now":
            plan = (
                f"Direct proxy signal confirmed: {direct_proxy_ticker}. "
                f"Deploy a {direct_proxy_direction} starter position with {position_size_pct:.2f}% notional. "
                "Entry trigger: next refresh must preserve a market move signature and keep impact above 45%."
            )
            return (
                plan,
                [direct_proxy_ticker],
                direct_proxy_direction,
                "Risk guardrail: reduce/exit if momentum reverses sign or liquidity linkage drops below 30% on next refresh.",
                position_size_pct,
                "single_name_direct_proxy",
            )
        return (
            f"Direct proxy candidate detected: {direct_proxy_ticker}. Wait one confirming refresh before deployment.",
            [direct_proxy_ticker],
            direct_proxy_direction,
            "Risk guardrail: no deployment until impact >= 45%, economic relevance >= 34%, and liquidity linkage >= 30%.",
            0.0,
            "single_name_direct_proxy",
        )

    if theme is None:
        if direct_proxy_ticker:
            if action_bucket == "Act now":
                plan = (
                    f"Direct proxy candidate detected: {direct_proxy_ticker}. "
                    f"Use a {direct_proxy_direction} starter position only if next refresh confirms momentum >= +15% "
                    "and impact remains above 62%."
                )
                return (
                    plan,
                    [direct_proxy_ticker],
                    direct_proxy_direction,
                    "Risk guardrail: close if momentum turns negative on next refresh or liquidity linkage falls below 30%.",
                    0.75,
                    "single_name_direct_proxy",
                )
            return (
                f"Direct proxy candidate detected: {direct_proxy_ticker}. Wait for one confirming refresh before deployment.",
                [direct_proxy_ticker],
                direct_proxy_direction,
                "Risk guardrail: no deployment until impact >= 58%, surprise >= 32%, and momentum is positive for two cycles.",
                0.0,
                "single_name_direct_proxy",
            )
        if action_bucket == "Act now":
            return (
                f"No clean liquid trade proxy. Operational action: assign owner now, gather 3 confirming sources, and publish a decision note inside {window}.",
                [],
                "Operational",
                "Risk guardrail: escalate only if momentum remains above +20% for two refreshes.",
                0.0,
                None,
            )
        return (
            "No direct trade proxy yet (ticker unresolved). Monitor and trigger action only after two consecutive positive refresh cycles.",
            [],
            "Operational",
            "Risk guardrail: no position until momentum > +15% and diversity >= 4.",
            0.0,
            None,
        )

    tickers = list(theme["tickers"])
    direction = str(theme["direction"])
    target_pct = round(2.5 + 5.5 * min(1.0, max(0.0, float(topic.get("momentum", 0.0)) / 1.5)), 1)
    stop_pct = round(max(1.8, target_pct * 0.5), 1)

    if action_bucket == "Act now":
        if (
            impact_score < 0.52
            or economic_relevance < 0.45
            or liquidity_linkage < 0.32
            or (surprise_score < 0.18 and not market_move_signature and not direct_proxy_ticker)
        ):
            return (
                "No direct trade deployment. Market-impact filter blocked this signal because economic transmission is weak.",
                [],
                "Operational",
                "Risk guardrail: require impact >= 52%, economic relevance >= 45%, and liquidity linkage >= 32% before capital deployment.",
                0.0,
                None,
            )
        plan = (
            f"{direction} {', '.join(tickers)} with {position_size_pct:.2f}% notional starter size. "
            f"{entry_trigger} Current signal is {momentum_pct:+d}% momentum / {novelty_pct:+d}% novelty. "
            f"Initial target {target_pct:.1f}% and stop {stop_pct:.1f}%; reassess after {window}."
        )
        guardrail = (
            "Risk guardrail: reduce/exit if momentum turns negative, diversity drops below 3, "
            "or the top source set collapses to a single outlet."
        )
        return plan, tickers, direction, guardrail, position_size_pct, str(theme.get("name"))

    if impact_score < 0.45 or economic_relevance < 0.34:
        return (
            "No direct trade proxy yet. Signal momentum is present, but market-impact transmission is not strong enough for deployment.",
            [],
            "Operational",
            "Risk guardrail: no position until impact >= 45% and economic relevance >= 34% for two consecutive refreshes.",
            0.0,
            None,
        )

    plan = (
        f"Place alerts on {', '.join(tickers)} and wait. Trigger only if momentum re-accelerates above +15% "
        "for two consecutive refreshes, diversity stays >= 3, and impact score remains above 58%."
    )
    guardrail = "Risk guardrail: skip trade if signal remains mixed (momentum <= 0) or novelty decays."
    return plan, tickers, direction, guardrail, position_size_pct, str(theme.get("name"))


def _asset_mapping_payload(matched_theme: dict[str, Any] | None) -> dict[str, Any] | None:
    if not matched_theme:
        return None
    rule = matched_theme.get("rule") or {}
    matched_tokens = list(matched_theme.get("matched_tokens") or [])
    matched_strong_tokens = list(matched_theme.get("matched_strong_tokens") or [])
    matched_phrases = list(matched_theme.get("matched_phrases") or [])
    headline_tokens = list(matched_theme.get("headline_strong_matches") or [])
    headline_phrases = list(matched_theme.get("headline_phrase_matches") or [])
    vertical_alignment = bool(matched_theme.get("vertical_alignment", True))
    confidence = float(matched_theme.get("confidence", 0.0) or 0.0)

    evidence_parts = []
    if matched_strong_tokens:
        evidence_parts.append(f"strong tokens: {', '.join(matched_strong_tokens[:4])}")
    if matched_phrases:
        evidence_parts.append(f"phrases: {', '.join(matched_phrases[:3])}")
    if headline_tokens or headline_phrases:
        headline_evidence = ", ".join((headline_tokens + headline_phrases)[:3])
        if headline_evidence:
            evidence_parts.append(f"headline evidence: {headline_evidence}")
    evidence_parts.append("vertical aligned" if vertical_alignment else "cross-vertical weak match")

    return {
        "theme_name": str(rule.get("name") or ""),
        "thesis": str(rule.get("thesis") or ""),
        "tickers": list(rule.get("tickers") or []),
        "direction": str(rule.get("direction") or ""),
        "confidence": round(confidence, 4),
        "matched_tokens": matched_tokens[:8],
        "matched_strong_tokens": matched_strong_tokens[:8],
        "matched_phrases": matched_phrases[:4],
        "headline_evidence": (headline_tokens + headline_phrases)[:6],
        "vertical_alignment": vertical_alignment,
        "evidence_summary": "; ".join(evidence_parts),
    }


def _resolve_risk_limits(profile: str) -> dict[str, float | int]:
    normalized = profile.strip().lower()
    if normalized not in RISK_LIMITS_BY_PROFILE:
        normalized = "investor"
    return RISK_LIMITS_BY_PROFILE[normalized]


def _parse_event_time(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        else:
            text = str(value)
            parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)
    except Exception:
        return None


def apply_risk_controls(
    *,
    decisions: list[dict[str, Any]],
    profile: str,
    generated_at: datetime,
    historical_executions: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    limits = _resolve_risk_limits(profile)
    max_themes = int(limits["max_simultaneous_themes"])
    max_daily_notional_pct = float(limits["max_daily_notional_pct"])
    max_position_notional_pct = float(limits["max_position_notional_pct"])
    cooldown_hours = int(limits["cooldown_hours"])
    max_new_trades_per_day = int(limits["max_new_trades_per_day"])

    day_start = generated_at.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    used_daily_notional = 0.0
    new_trades_today = 0
    active_themes: set[str] = set()
    cooldown_until_by_theme: dict[str, datetime] = {}

    for event in historical_executions:
        if event.get("execution_status") != "approved":
            continue
        theme = str(event.get("trade_theme") or "").strip()
        if not theme:
            continue
        opened_at = _parse_event_time(event.get("opened_at"))
        if not opened_at:
            continue
        if opened_at >= day_start:
            active_themes.add(theme)
            new_trades_today += 1
            used_daily_notional += float(event.get("approved_notional_pct", 0.0) or 0.0)

        cooldown_until = opened_at + timedelta(hours=cooldown_hours)
        existing = cooldown_until_by_theme.get(theme)
        if existing is None or cooldown_until > existing:
            cooldown_until_by_theme[theme] = cooldown_until

    executed_now: list[dict[str, Any]] = []
    blocked_count = 0

    for decision in decisions:
        decision.setdefault("trade_theme", None)
        decision.setdefault("proposed_notional_pct", 0.0)
        decision.setdefault("approved_notional_pct", 0.0)
        decision.setdefault("execution_status", "operational")
        decision.setdefault("risk_block_reason", None)

        action_bucket = str(decision.get("action_bucket") or "")
        trade_theme = str(decision.get("trade_theme") or "").strip()
        trade_direction = str(decision.get("trade_direction") or "")
        trade_tickers = list(decision.get("trade_tickers") or [])

        if action_bucket != "Act now" or trade_direction in {"Operational", "None"} or not trade_theme or not trade_tickers:
            decision["execution_status"] = "operational" if trade_direction == "Operational" else "no_trade"
            decision["approved_notional_pct"] = 0.0
            decision["risk_block_reason"] = None
            continue

        proposed = float(decision.get("proposed_notional_pct", 0.0) or 0.0)
        proposed = max(0.0, min(proposed, max_position_notional_pct))
        decision["proposed_notional_pct"] = round(proposed, 2)

        block_reason: str | None = None
        cooldown_until = cooldown_until_by_theme.get(trade_theme)
        if cooldown_until and cooldown_until > generated_at:
            block_reason = f"cooldown active for theme '{trade_theme}' until {cooldown_until.isoformat()}"
        elif trade_theme in active_themes:
            block_reason = f"theme '{trade_theme}' already active today (no pyramiding)"
        elif len(active_themes) >= max_themes:
            block_reason = f"max simultaneous themes reached ({max_themes})"
        elif new_trades_today >= max_new_trades_per_day:
            block_reason = f"max new trades per day reached ({max_new_trades_per_day})"
        elif used_daily_notional + proposed > max_daily_notional_pct:
            block_reason = (
                f"daily notional cap {max_daily_notional_pct:.2f}% exceeded "
                f"(used {used_daily_notional:.2f}% + proposed {proposed:.2f}%)"
            )

        if block_reason:
            blocked_count += 1
            decision["execution_status"] = "blocked_risk_cap"
            decision["risk_block_reason"] = block_reason
            decision["approved_notional_pct"] = 0.0
            if "already active today" in block_reason:
                decision["execution_plan"] = (
                    f"Trade thesis remains active, but no pyramiding is allowed: {block_reason}."
                )
            else:
                decision["execution_plan"] = f"Blocked by portfolio risk controls: {block_reason}."
            continue

        decision["execution_status"] = "approved"
        decision["risk_block_reason"] = None
        decision["approved_notional_pct"] = round(proposed, 2)
        active_themes.add(trade_theme)
        new_trades_today += 1
        used_daily_notional += proposed
        executed_now.append(
            {
                "topic_id": decision.get("topic_id"),
                "trade_theme": trade_theme,
                "trade_direction": decision.get("trade_direction"),
                "trade_tickers": list(decision.get("trade_tickers") or []),
                "approved_notional_pct": round(proposed, 2),
                "execution_status": "approved",
                "opened_at": generated_at.isoformat(),
            }
        )

    risk_summary = {
        "max_simultaneous_themes": max_themes,
        "max_daily_notional_pct": round(max_daily_notional_pct, 2),
        "max_position_notional_pct": round(max_position_notional_pct, 2),
        "cooldown_hours": cooldown_hours,
        "max_new_trades_per_day": max_new_trades_per_day,
        "active_themes_count": len(active_themes),
        "used_daily_notional_pct": round(used_daily_notional, 2),
        "approved_trade_count": len(executed_now),
        "blocked_trade_count": blocked_count,
    }
    return decisions, executed_now, risk_summary


def build_decisions(
    topics: list[dict[str, Any]],
    *,
    profile: str,
    top_n: int,
    window: str,
) -> tuple[list[dict[str, Any]], dict[str, float]]:
    _, weights = resolve_profile(profile)
    scored: list[DecisionComputation] = []
    for topic in topics:
        topic_id = topic.get("topic_id")
        if not topic_id:
            continue
        raw_score = compute_decision_score(topic, weights)
        reliability = _signal_reliability(topic)
        reliability_multiplier = 0.62 + 0.48 * reliability
        local_penalty = 2.25 if str(topic.get("vertical") or "").lower() == "local" and profile == "investor" else 0.0
        final_score = raw_score * reliability_multiplier - local_penalty
        scored.append(DecisionComputation(topic_id=topic_id, score=float(final_score)))
    score_by_topic = {row.topic_id: row.score for row in scored}
    ranked_topics = sorted(
        (topic for topic in topics if topic.get("topic_id") in score_by_topic),
        key=lambda topic: score_by_topic[topic["topic_id"]],
        reverse=True,
    )

    decisions: list[dict[str, Any]] = []
    for topic in ranked_topics:
        score = float(score_by_topic[topic["topic_id"]])
        momentum = float(topic.get("momentum", 0.0))
        diversity = int(topic.get("diversity", 0) or 0)
        reliability = _signal_reliability(topic)
        matched_theme = _select_trade_theme(topic)
        headline_title = str(((topic.get("representative_items") or [{}])[0]).get("title") or "")
        headline_direct_proxy_ticker = _extract_headline_proxy_ticker(headline_title)
        proxy_theme_mismatch = False
        if matched_theme and headline_direct_proxy_ticker:
            theme_tickers = {
                str(ticker).upper()
                for ticker in ((matched_theme.get("rule") or {}).get("tickers") or [])
                if str(ticker).strip()
            }
            if theme_tickers and headline_direct_proxy_ticker not in theme_tickers:
                proxy_theme_mismatch = True
                matched_theme = None
        theme_confidence = float(matched_theme.get("confidence", 0.0) or 0.0) if matched_theme else 0.0
        impact = _market_impact_assessment(topic, matched_theme, reliability)
        market_impact_score = float(impact.get("impact_score", 0.0) or 0.0)
        economic_relevance = float(impact.get("economic_relevance", 0.0) or 0.0)
        surprise_score = float(impact.get("surprise_score", 0.0) or 0.0)
        liquidity_linkage = float(impact.get("liquidity_linkage", 0.0) or 0.0)

        if matched_theme:
            trade_confidence = _clamp(
                0.4 * theme_confidence + 0.3 * reliability + 0.3 * market_impact_score,
                0.0,
                1.0,
            )
        else:
            trade_confidence = 0.15 if bool(str(impact.get("direct_proxy_ticker") or "").strip()) else 0.0
            if proxy_theme_mismatch:
                trade_confidence = min(trade_confidence, 0.2)
        raw_action_bucket = classify_action_bucket(
            score=score,
            momentum=momentum,
            reliability=reliability,
            diversity=diversity,
            theme_confidence=theme_confidence,
            market_impact_score=market_impact_score,
            economic_relevance=economic_relevance,
            surprise_score=surprise_score,
            liquidity_linkage=liquidity_linkage,
            editorial_noise=bool(impact.get("editorial_noise", False)),
            market_move_signature=bool(impact.get("market_move_signature", False)),
            has_direct_proxy=bool(str(impact.get("direct_proxy_ticker") or "").strip()),
            is_local=str(topic.get("vertical") or "").lower() == "local",
            profile=profile,
        )
        trust_contract = _build_trust_contract(
            topic=topic,
            impact=impact,
            matched_theme=matched_theme,
            theme_confidence=theme_confidence,
            reliability=reliability,
            momentum=momentum,
            market_impact_score=market_impact_score,
        )
        action_bucket = raw_action_bucket
        if raw_action_bucket == "Act now" and not trust_contract["eligible_for_act_now"]:
            action_bucket = "Monitor"
            trust_contract["warnings"].append("Act now downgraded by trust contract gate.")

        decision_label = str(topic.get("label") or "")
        if trust_contract["label_alignment_confidence"] < TRUST_LABEL_ALIGNMENT_MIN:
            unresolved_vertical = str(topic.get("vertical") or "narrative").strip().lower() or "narrative"
            decision_label = f"Unresolved {unresolved_vertical} narrative"

        signal_statement = _compose_signal_text(topic)
        execution_plan, trade_tickers, trade_direction, risk_guardrail, proposed_notional_pct, trade_theme = _explicit_execution_plan(
            topic,
            action_bucket,
            window,
            reliability,
            theme_confidence,
            impact,
            matched_theme,
        )
        decisions.append(
            {
                "topic_id": topic.get("topic_id"),
                "label": decision_label,
                "vertical": topic.get("vertical"),
                "decision_score": round(score, 4),
                "action_bucket": action_bucket,
                "rationale": _bucket_rationale(topic, action_bucket, impact),
                "next_step": _bucket_next_step(action_bucket, window),
                "signal_statement": signal_statement,
                "execution_plan": execution_plan,
                "trade_tickers": trade_tickers,
                "trade_direction": trade_direction,
                "trade_theme": trade_theme,
                "proposed_notional_pct": round(proposed_notional_pct, 2),
                "approved_notional_pct": 0.0,
                "execution_status": "pending",
                "risk_block_reason": None,
                "risk_guardrail": risk_guardrail,
                "signal_reliability": round(reliability, 4),
                "trade_confidence": round(trade_confidence, 4),
                "asset_mapping": _asset_mapping_payload(matched_theme),
                "market_impact_score": round(market_impact_score, 4),
                "economic_relevance": round(economic_relevance, 4),
                "surprise_score": round(surprise_score, 4),
                "liquidity_linkage": round(liquidity_linkage, 4),
                "impact_verdict": str(impact.get("impact_verdict") or "weak"),
                "impact_reason": str(impact.get("impact_reason") or "")
                + ("; theme_proxy_mismatch=1" if proxy_theme_mismatch else ""),
                "momentum": momentum,
                "novelty": float(topic.get("novelty", 0.0)),
                "weighted_volume_now": round(_weighted_now(topic), 4),
                "weighted_volume_prev": round(_weighted_prev(topic), 4),
                "diversity": int(topic.get("diversity", 0) or 0),
                "label_confidence": float(topic.get("label_confidence", 0.45) or 0.45),
                "source_quality_score": float(topic.get("source_quality_score", 1.0) or 1.0),
                "keywords": (topic.get("keywords") or [])[:8],
                "entities": (topic.get("entities") or [])[:8],
                "representative_items": (topic.get("representative_items") or [])[:5],
                "related_topic_ids": (topic.get("related_topic_ids") or [])[:6],
                "trust_contract": trust_contract,
            }
        )

    action_priority = {"Act now": 2, "Monitor": 1, "Ignore": 0}
    decisions.sort(
        key=lambda decision: (
            action_priority.get(str(decision.get("action_bucket") or "Ignore"), 0),
            float(decision.get("decision_score", 0.0) or 0.0),
        ),
        reverse=True,
    )
    return decisions[:top_n], score_by_topic


def build_changes(
    *,
    current_signals: list[dict[str, Any]],
    previous_signals: list[dict[str, Any]],
    score_by_topic: dict[str, float],
    limit: int = 10,
) -> dict[str, list[dict[str, Any]]]:
    previous_by_topic = {
        str(signal.get("topic_id")): signal
        for signal in previous_signals
        if signal.get("topic_id")
    }

    new_narratives: list[dict[str, Any]] = []
    accelerating: list[dict[str, Any]] = []
    fading: list[dict[str, Any]] = []

    for signal in current_signals:
        topic_id = str(signal.get("topic_id"))
        if not topic_id:
            continue
        current_momentum = float(signal.get("momentum", 0.0))
        current_weighted = float(signal.get("weighted_volume_now", 0.0))
        previous = previous_by_topic.get(topic_id)

        if previous is None:
            new_narratives.append(
                {
                    "topic_id": topic_id,
                    "label": signal.get("label"),
                    "vertical": signal.get("vertical"),
                    "momentum": round(current_momentum, 4),
                    "weighted_volume_now": round(current_weighted, 4),
                    "delta_momentum": None,
                    "delta_weighted_volume": None,
                }
            )
            continue

        previous_momentum = float(previous.get("momentum", 0.0))
        previous_weighted = float(previous.get("weighted_volume_now", 0.0))
        delta_momentum = current_momentum - previous_momentum
        delta_weighted = current_weighted - previous_weighted

        if delta_momentum >= 0.18 and current_weighted >= previous_weighted * 1.12:
            accelerating.append(
                {
                    "topic_id": topic_id,
                    "label": signal.get("label"),
                    "vertical": signal.get("vertical"),
                    "momentum": round(current_momentum, 4),
                    "weighted_volume_now": round(current_weighted, 4),
                    "delta_momentum": round(delta_momentum, 4),
                    "delta_weighted_volume": round(delta_weighted, 4),
                }
            )
        elif delta_momentum <= -0.18 and current_weighted <= previous_weighted * 0.9:
            fading.append(
                {
                    "topic_id": topic_id,
                    "label": signal.get("label"),
                    "vertical": signal.get("vertical"),
                    "momentum": round(current_momentum, 4),
                    "weighted_volume_now": round(current_weighted, 4),
                    "delta_momentum": round(delta_momentum, 4),
                    "delta_weighted_volume": round(delta_weighted, 4),
                }
            )

    def rank(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(items, key=lambda row: score_by_topic.get(str(row.get("topic_id")), 0.0), reverse=True)[:limit]

    return {
        "new_narratives": rank(new_narratives),
        "accelerating": rank(accelerating),
        "fading": rank(fading),
    }
