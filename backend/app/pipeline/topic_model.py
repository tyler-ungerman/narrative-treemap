from __future__ import annotations

import logging
import re
from collections import Counter, defaultdict
from datetime import datetime
from typing import Any

import numpy as np
from sklearn.cluster import AgglomerativeClustering, KMeans
from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS, TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from app.core.hash_utils import stable_hash
from app.core.time_windows import SPARKLINE_BUCKETS
from app.pipeline.metrics import (
    build_sparkline,
    compute_momentum,
    compute_novelty,
)

logger = logging.getLogger(__name__)
_LOGGED_HDBSCAN_UNAVAILABLE = False

STOPWORDS = set(ENGLISH_STOP_WORDS)
STOPWORDS.update(
    {
        "said",
        "says",
        "new",
        "news",
        "today",
        "year",
        "years",
        "day",
        "days",
        "week",
        "weeks",
        "month",
        "months",
        "amid",
        "amidst",
    }
)
LABEL_BLOCKLIST = {
    "com",
    "www",
    "http",
    "https",
    "amp",
    "latest",
    "breaking",
    "video",
    "watch",
    "photos",
    "photo",
    "report",
    "reports",
    "update",
    "updates",
    "live",
    "post",
    "posts",
    "newsletter",
    "analysis",
    "confirm",
    "confirms",
    "confirmed",
    "reveals",
    "reveal",
    "says",
    "said",
    "preview",
    "review",
    "opinion",
    "exclusive",
    "official",
    "watchlist",
    "highlights",
    "highlight",
    "free",
    "best",
    "top",
    "get",
    "guide",
    "vs",
    "rss",
    "feed",
}
ACRONYMS = {
    "ai",
    "ml",
    "gpu",
    "cpu",
    "api",
    "sql",
    "aws",
    "ipo",
    "sec",
    "fed",
    "ecb",
    "cdc",
    "fda",
    "who",
    "uk",
    "us",
    "eu",
    "nfl",
    "nba",
    "mlb",
    "f1",
    "esg",
}
ENTITY_PATTERN = re.compile(r"\b(?:[A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})(?:\s+(?:Inc|Corp|LLC|Ltd|University|Agency))?\b")
TOKEN_PATTERN = re.compile(r"[a-zA-Z][a-zA-Z0-9\-]{2,}")
SOURCE_SUFFIX_HINTS = {
    "reuters",
    "bbc",
    "aljazeera",
    "guardian",
    "yahoo",
    "google",
    "cnbc",
    "bloomberg",
    "economist",
    "associated",
    "press",
    "ap",
    "npr",
    "news",
    "world",
    "finance",
    "sports",
}
ENTITY_BLOCKLIST = {
    "donald trump",
    "trump",
    "breaking news",
    "daily briefing",
    "associated press",
}
VERTICAL_PREFIX_MAP = {
    "tech": "Tech",
    "science": "Science",
    "world": "World",
    "local": "Local",
    "markets": "Markets",
    "sports": "Sports",
    "entertainment": "Entertainment",
    "health": "Health",
    "gaming": "Gaming",
    "security": "Security",
    "programming": "Programming",
}
SOURCE_PREFIX_DAMPING: tuple[tuple[str, float], ...] = (
    ("gnews_geo_", 0.35),
    ("gnews_region_", 0.58),
)
SOURCE_VOLUME_EXPONENT = 0.68
TOPIC_LABEL_ALIGNMENT_MIN = 0.52
TOPIC_SOURCE_QUALITY_MIN = 0.72
TOPIC_NOVELTY_MIN = 0.2
TOPIC_MOMENTUM_MIN = 0.18
TOPIC_DIVERSITY_MIN = 3


def tokenize(text: str) -> list[str]:
    return [token.lower() for token in TOKEN_PATTERN.findall(text) if token.lower() not in STOPWORDS]


def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
    return max(minimum, min(maximum, value))


def source_prefix_damping(source_name: str) -> float:
    for prefix, damping in SOURCE_PREFIX_DAMPING:
        if source_name.startswith(prefix):
            return damping
    return 1.0


def effective_source_volume_contribution(
    *,
    source_name: str,
    item_count: int,
    source_quality: float,
) -> float:
    if item_count <= 0:
        return 0.0
    damped_count = float(item_count ** SOURCE_VOLUME_EXPONENT)
    return damped_count * source_quality * source_prefix_damping(source_name)


def compute_effective_weighted_volume(
    items: list[dict[str, Any]],
    source_quality_by_name: dict[str, float],
) -> float:
    if not items:
        return 0.0
    counts_by_source = Counter(item.get("source_name") or "" for item in items)
    weighted_total = 0.0
    for source_name, item_count in counts_by_source.items():
        source_quality = source_quality_by_name.get(source_name, 1.0)
        weighted_total += effective_source_volume_contribution(
            source_name=source_name,
            item_count=item_count,
            source_quality=source_quality,
        )
    return weighted_total


def matching_signal_items(items: list[dict[str, Any]], signals: set[str]) -> list[dict[str, Any]]:
    if not items or not signals:
        return []
    expanded_signals: set[str] = set()
    for signal in signals:
        expanded_signals.update(token.lower() for token in TOKEN_PATTERN.findall(signal))
    if not expanded_signals:
        return []

    matched: list[dict[str, Any]] = []
    for item in items:
        title_tokens = set(token.lower() for token in TOKEN_PATTERN.findall(item.get("title") or ""))
        summary_tokens = set(token.lower() for token in TOKEN_PATTERN.findall(item.get("summary") or ""))
        if title_tokens.intersection(expanded_signals) or summary_tokens.intersection(expanded_signals):
            matched.append(item)
    return matched


def normalize_label_term(term: str) -> str:
    parts = [part.lower() for part in TOKEN_PATTERN.findall(term)]
    cleaned_parts = [
        part
        for part in parts
        if part not in STOPWORDS and part not in LABEL_BLOCKLIST and len(part) >= 2
    ]
    if not cleaned_parts:
        return ""
    return " ".join(cleaned_parts[:3])


def display_label_term(term: str) -> str:
    words = [word for word in term.split(" ") if word]
    if not words:
        return ""
    display_words: list[str] = []
    for word in words:
        if word in ACRONYMS:
            display_words.append(word.upper())
        else:
            display_words.append(word.capitalize())
    return " ".join(display_words)


def _looks_like_source_suffix(segment: str) -> bool:
    tokens = [token.lower() for token in TOKEN_PATTERN.findall(segment)]
    if not tokens:
        return True
    if "." in segment and len(tokens) <= 5:
        return True
    if len(tokens) <= 4 and any(token in SOURCE_SUFFIX_HINTS for token in tokens):
        return True
    return False


def clean_title_for_label(title: str) -> str:
    compact = re.sub(r"\s+", " ", title).strip(" \t\n-|")
    compact = re.sub(r"^\[[^\]]+\]\s*", "", compact)
    segments = re.split(r"\s[-|]\s", compact)
    if len(segments) <= 1:
        return compact

    cleaned_segments: list[str] = []
    for segment in segments:
        stripped = segment.strip()
        if not stripped:
            continue
        if cleaned_segments and _looks_like_source_suffix(stripped):
            break
        cleaned_segments.append(stripped)
    return " - ".join(cleaned_segments) if cleaned_segments else compact


def label_tokens_from_title(title: str, max_tokens: int = 7) -> list[str]:
    cleaned_title = clean_title_for_label(title)
    tokens: list[str] = []
    for token in TOKEN_PATTERN.findall(cleaned_title):
        normalized = token.lower()
        if normalized in STOPWORDS or normalized in LABEL_BLOCKLIST:
            continue
        if len(normalized) <= 2 and normalized not in ACRONYMS:
            continue
        tokens.append(normalized)
        if len(tokens) >= max_tokens:
            break
    return tokens


def label_phrase_from_title(title: str, max_words: int = 4) -> str:
    tokens = label_tokens_from_title(title, max_tokens=max_words + 2)
    if len(tokens) < 2:
        return ""
    phrase = " ".join(tokens[:max_words])
    return display_label_term(phrase)


def extract_keywords(titles: list[str], top_n: int = 8) -> list[str]:
    if not titles:
        return []
    cleaned_titles = [clean_title_for_label(title) for title in titles]
    if len(titles) == 1:
        single_keywords: list[str] = []
        for token in tokenize(cleaned_titles[0]):
            normalized = normalize_label_term(token)
            if not normalized or normalized in single_keywords:
                continue
            single_keywords.append(normalized)
            if len(single_keywords) >= top_n:
                break
        return single_keywords

    try:
        vectorizer = TfidfVectorizer(stop_words="english", ngram_range=(1, 2), max_features=500)
        matrix = vectorizer.fit_transform(cleaned_titles)
        scores = np.asarray(matrix.mean(axis=0)).ravel()
        terms = np.array(vectorizer.get_feature_names_out())
        ranked_indices = np.argsort(scores)[::-1]
    except ValueError:
        merged_tokens = []
        for title in cleaned_titles:
            merged_tokens.extend(tokenize(title))
        return merged_tokens[:top_n]

    keywords: list[str] = []
    for index in ranked_indices:
        term = terms[index].strip()
        normalized = normalize_label_term(term)
        if not normalized:
            continue
        if normalized in keywords:
            continue
        keywords.append(normalized)
        if len(keywords) >= top_n:
            break
    return keywords


def extract_entities(texts: list[str], max_entities: int = 8) -> list[str]:
    candidates: Counter[str] = Counter()
    document_hits: dict[str, set[int]] = defaultdict(set)
    for index, text in enumerate(texts):
        for match in ENTITY_PATTERN.findall(text):
            normalized = match.strip()
            if len(normalized) < 3:
                continue
            if normalized.lower() in STOPWORDS:
                continue
            if normalized.lower() in LABEL_BLOCKLIST or normalized.lower() in ENTITY_BLOCKLIST:
                continue
            candidates[normalized] += 1
            document_hits[normalized].add(index)

    if not candidates:
        return []
    min_document_support = 1
    if len(texts) >= 5:
        min_document_support = 2
    if len(texts) >= 12:
        min_document_support = 3

    ranked_entities = []
    for entity, score in candidates.most_common():
        if len(document_hits.get(entity, set())) < min_document_support:
            continue
        ranked_entities.append(entity)
        if len(ranked_entities) >= max_entities:
            break
    return ranked_entities


def _valid_phrase(tokens: list[str]) -> bool:
    if len(tokens) < 2:
        return False
    if tokens[0] in LABEL_BLOCKLIST or tokens[-1] in LABEL_BLOCKLIST:
        return False
    meaningful = [token for token in tokens if token not in LABEL_BLOCKLIST and token not in STOPWORDS]
    return len(meaningful) >= 2


def build_phrase_candidates(
    *,
    cluster_items: list[dict[str, Any]],
    similarity_scores: np.ndarray,
    source_quality_by_name: dict[str, float],
    max_candidates: int = 6,
) -> list[tuple[str, float]]:
    phrase_scores: dict[str, float] = defaultdict(float)
    phrase_document_hits: dict[str, set[int]] = defaultdict(set)
    for index, item in enumerate(cluster_items):
        title_tokens = label_tokens_from_title(item.get("title") or "")
        if len(title_tokens) < 2:
            continue

        source_quality = source_quality_by_name.get(item.get("source_name") or "", 1.0)
        similarity = float(similarity_scores[index]) if index < len(similarity_scores) else 0.0
        similarity_weight = 0.65 + 0.55 * max((similarity + 1.0) / 2.0, 0.0)
        item_weight = max(0.25, source_quality * similarity_weight)

        lead_window = min(len(title_tokens), 6)
        for phrase_size in (3, 2):
            if lead_window < phrase_size:
                continue

            lead_tokens = title_tokens[:phrase_size]
            if _valid_phrase(lead_tokens):
                lead_phrase = " ".join(lead_tokens)
                phrase_scores[lead_phrase] += item_weight * 1.35
                phrase_document_hits[lead_phrase].add(index)

            max_start = min(3, lead_window - phrase_size + 1)
            for start in range(1, max_start):
                phrase_tokens = title_tokens[start : start + phrase_size]
                if _valid_phrase(phrase_tokens):
                    phrase = " ".join(phrase_tokens)
                    phrase_scores[phrase] += item_weight * 0.8
                    phrase_document_hits[phrase].add(index)

    min_document_support = 1
    if len(cluster_items) >= 5:
        min_document_support = 2
    if len(cluster_items) >= 12:
        min_document_support = 3

    filtered_scores = [
        (phrase, score)
        for phrase, score in phrase_scores.items()
        if len(phrase_document_hits.get(phrase, set())) >= min_document_support
    ]
    ranked_phrases = sorted(filtered_scores, key=lambda entry: entry[1], reverse=True)
    return ranked_phrases[:max_candidates]


def build_topic_label(
    vertical: str,
    keywords: list[str],
    entities: list[str],
    representative_title: str,
    phrase_candidates: list[tuple[str, float]],
) -> tuple[str, float]:
    vertical_prefix = VERTICAL_PREFIX_MAP.get(vertical, vertical.title())

    if phrase_candidates:
        phrase, score = phrase_candidates[0]
        runner_up_score = phrase_candidates[1][1] if len(phrase_candidates) > 1 else 0.0
        label_core = display_label_term(phrase)
        confidence_gap = max(score - runner_up_score, 0.0)
        confidence = min(0.95, 0.45 + min(confidence_gap, 2.5) * 0.16)
        return f"{vertical_prefix}: {label_core}", round(confidence, 4)

    title_phrase = label_phrase_from_title(representative_title)
    if title_phrase:
        return f"{vertical_prefix}: {title_phrase}", 0.48

    selected_terms: list[str] = []
    for keyword in keywords:
        if any(keyword in existing or existing in keyword for existing in selected_terms):
            continue
        selected_terms.append(keyword)
        if len(selected_terms) >= 2:
            break

    if not selected_terms and entities:
        title_tokens = set(tokenize(representative_title))
        entity_terms: list[str] = []
        for entity in entities:
            normalized_entity = normalize_label_term(entity)
            if not normalized_entity:
                continue
            entity_tokens = set(normalized_entity.split())
            if entity_tokens and entity_tokens.intersection(title_tokens):
                entity_terms.append(normalized_entity)
        selected_terms = entity_terms[:1]
    if not selected_terms:
        return f"{vertical_prefix} Misc", 0.2

    label_core = ", ".join(display_label_term(term) for term in selected_terms if term)
    if not label_core:
        return f"{vertical_prefix} Misc", 0.2
    return f"{vertical_prefix}: {label_core}", 0.35


def compute_label_alignment_confidence(
    *,
    label: str,
    representative_title: str,
    label_confidence: float,
    keywords: list[str],
    entities: list[str],
) -> float:
    label_core = label.split(":", 1)[-1] if ":" in label else label
    label_tokens = set(tokenize(label_core))
    if not label_tokens:
        return round(_clamp(label_confidence), 4)

    headline_tokens = set(tokenize(clean_title_for_label(representative_title)))
    keyword_tokens = set()
    for keyword in keywords[:4]:
        keyword_tokens.update(tokenize(keyword))
    entity_tokens = set()
    for entity in entities[:3]:
        entity_tokens.update(tokenize(entity))
    evidence_tokens = headline_tokens.union(keyword_tokens).union(entity_tokens)
    overlap_ratio = len(label_tokens.intersection(evidence_tokens)) / max(len(label_tokens), 1)
    combined = _clamp((label_confidence * 0.55) + (overlap_ratio * 0.45))
    return round(combined, 4)


def build_topic_trust_contract(
    *,
    label_alignment_confidence: float,
    source_quality_score: float,
    novelty: float,
    momentum: float,
    diversity: int,
) -> dict[str, Any]:
    novelty_confidence = _clamp(novelty)
    warnings: list[str] = []

    if label_alignment_confidence < TOPIC_LABEL_ALIGNMENT_MIN:
        warnings.append("Label-to-content alignment is low; entity naming suppressed.")
    if source_quality_score < TOPIC_SOURCE_QUALITY_MIN:
        warnings.append("Source quality is below act-now threshold.")
    if novelty_confidence < TOPIC_NOVELTY_MIN:
        warnings.append("Novelty confidence is below act-now threshold.")
    if momentum < TOPIC_MOMENTUM_MIN:
        warnings.append("Momentum is below act-now threshold.")
    if diversity < TOPIC_DIVERSITY_MIN:
        warnings.append("Cross-source diversity is below act-now threshold.")

    eligible_for_act_now = (
        label_alignment_confidence >= TOPIC_LABEL_ALIGNMENT_MIN
        and source_quality_score >= TOPIC_SOURCE_QUALITY_MIN
        and novelty_confidence >= TOPIC_NOVELTY_MIN
        and momentum >= TOPIC_MOMENTUM_MIN
        and diversity >= TOPIC_DIVERSITY_MIN
    )

    return {
        "label_alignment_confidence": round(label_alignment_confidence, 4),
        "proxy_confidence": 0.0,
        "source_quality_score": round(source_quality_score, 4),
        "liquidity_link_confidence": None,
        "novelty_confidence": round(novelty_confidence, 4),
        "eligible_for_act_now": eligible_for_act_now,
        "warnings": warnings,
    }


def select_representative_items(
    *,
    cluster_items: list[dict[str, Any]],
    cluster_vectors: np.ndarray,
    centroid: np.ndarray,
    source_quality_by_name: dict[str, float],
    limit: int = 12,
) -> tuple[list[dict[str, Any]], str, np.ndarray]:
    if not cluster_items:
        return [], "", np.asarray([], dtype=np.float32)

    similarity_scores = cosine_similarity(cluster_vectors, centroid.reshape(1, -1)).ravel()
    published_values = [item.get("published_at") for item in cluster_items if item.get("published_at")]
    newest = max(published_values) if published_values else None
    oldest = min(published_values) if published_values else None
    recency_range_seconds = (
        max((newest - oldest).total_seconds(), 1.0) if newest and oldest else 1.0
    )

    scored_indices: list[tuple[float, int]] = []
    for index, item in enumerate(cluster_items):
        source_quality = source_quality_by_name.get(item.get("source_name") or "", 1.0)
        normalized_similarity = max((float(similarity_scores[index]) + 1.0) / 2.0, 0.0)
        published_at = item.get("published_at")
        recency = 0.5
        if newest and oldest and published_at:
            recency = (published_at - oldest).total_seconds() / recency_range_seconds
        source_factor = min(max(source_quality, 0.45), 1.45) / 1.45
        score = (normalized_similarity * 0.58) + (recency * 0.2) + (source_factor * 0.22)
        scored_indices.append((score, index))

    scored_indices.sort(reverse=True)
    representative_rows: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    for _, index in scored_indices:
        item = cluster_items[index]
        item_url = item.get("url")
        if not item_url or item_url in seen_urls:
            continue
        seen_urls.add(item_url)
        representative_rows.append(
            {
                "title": item.get("title", ""),
                "url": item_url,
                "source_name": item.get("source_name", ""),
                "published_at": item["published_at"].isoformat() if item.get("published_at") else None,
                "source_quality": round(source_quality_by_name.get(item.get("source_name") or "", 1.0), 4),
            }
        )
        if len(representative_rows) >= limit:
            break

    representative_title = cluster_items[scored_indices[0][1]].get("title", "")
    return representative_rows, representative_title, similarity_scores


def build_topic_summary(
    label: str,
    representative_items: list[dict[str, Any]],
    diversity: int,
    momentum: float,
    novelty: float,
) -> str:
    if not representative_items:
        return f"{label} has limited recent coverage."

    head_title = representative_items[0]["title"]
    source_names = sorted({item["source_name"] for item in representative_items})
    source_text = ", ".join(source_names[:3])
    trend_text = "rising" if momentum > 0.1 else "cooling" if momentum < -0.1 else "stable"

    return (
        f"{label} is {trend_text} with {len(representative_items)} highlighted items "
        f"from {diversity} sources. Coverage centers on '{head_title}' and related updates from {source_text}. "
        f"Estimated novelty is {novelty:.0%}."
    )


def run_clustering(vectors: np.ndarray) -> tuple[np.ndarray, str]:
    global _LOGGED_HDBSCAN_UNAVAILABLE
    item_count = len(vectors)
    if item_count <= 2:
        return np.zeros(item_count, dtype=int), "single_cluster"

    try:
        import hdbscan  # type: ignore

        clusterer = hdbscan.HDBSCAN(min_cluster_size=max(2, min(6, item_count // 6)), metric="euclidean")
        labels = clusterer.fit_predict(vectors)
        singleton_ratio = float(np.mean(labels == -1)) if item_count else 0.0
        unique_clusters = len({label for label in labels if label != -1})
        if singleton_ratio < 0.65 and unique_clusters >= 1:
            return labels.astype(int), "hdbscan"
    except Exception as exc:
        if not _LOGGED_HDBSCAN_UNAVAILABLE:
            logger.debug("hdbscan_unavailable", extra={"extra": {"reason": str(exc)}})
            _LOGGED_HDBSCAN_UNAVAILABLE = True

    try:
        clusters = max(2, min(10, int(np.sqrt(item_count)) + 1))
        model = AgglomerativeClustering(n_clusters=clusters)
        labels = model.fit_predict(vectors)
        return labels.astype(int), "agglomerative"
    except Exception:
        clusters = max(2, min(8, int(np.sqrt(item_count))))
        labels = KMeans(n_clusters=clusters, n_init="auto", random_state=42).fit_predict(vectors)
        return labels.astype(int), "kmeans"


def _cluster_indices(labels: np.ndarray, minimum_size: int = 2) -> dict[str, list[int]]:
    grouped: dict[int, list[int]] = defaultdict(list)
    for index, label in enumerate(labels):
        grouped[int(label)].append(index)

    clusters: dict[str, list[int]] = {}
    misc_indices: list[int] = []
    topic_counter = 0

    for label, indices in grouped.items():
        if label == -1 or len(indices) < minimum_size:
            misc_indices.extend(indices)
            continue
        clusters[str(topic_counter)] = indices
        topic_counter += 1

    if misc_indices:
        clusters["misc"] = misc_indices
    return clusters


def estimate_previous_volume_by_similarity(
    centroid: np.ndarray,
    previous_vectors: np.ndarray,
    previous_items: list[dict[str, Any]] | None = None,
    source_quality_by_name: dict[str, float] | None = None,
    threshold: float = 0.38,
) -> float:
    if previous_vectors.size == 0:
        return 0.0

    similarities = cosine_similarity(previous_vectors, centroid.reshape(1, -1)).ravel()
    if similarities.size == 0:
        return 0.0

    adaptive_floor = float(np.percentile(similarities, 65))
    similarity_threshold = max(threshold, adaptive_floor)
    hit_indices = np.where(similarities >= similarity_threshold)[0]
    if source_quality_by_name and previous_items:
        hit_items: list[dict[str, Any]] = []
        for index in hit_indices:
            if index >= len(previous_items):
                continue
            hit_items.append(previous_items[index])
        return compute_effective_weighted_volume(hit_items, source_quality_by_name)
    return float(len(hit_indices))


def build_topics(
    *,
    window: str,
    current_items: list[dict[str, Any]],
    previous_items: list[dict[str, Any]],
    previous_embeddings: np.ndarray,
    baseline_items: list[dict[str, Any]],
    embeddings: np.ndarray,
    window_start: datetime,
    window_end: datetime,
    source_quality_by_name: dict[str, float] | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str]:
    if not current_items:
        return [], [], "none"
    source_quality_by_name = source_quality_by_name or {}

    items_by_vertical: dict[str, list[tuple[int, dict[str, Any]]]] = defaultdict(list)
    for index, item in enumerate(current_items):
        items_by_vertical[item["vertical"]].append((index, item))

    previous_indices_by_vertical: dict[str, list[int]] = defaultdict(list)
    for index, item in enumerate(previous_items):
        previous_indices_by_vertical[item["vertical"]].append(index)

    baseline_terms: set[str] = set()
    for baseline_item in baseline_items:
        baseline_terms.update(tokenize((baseline_item.get("title") or "") + " " + (baseline_item.get("summary") or "")))

    topics: list[dict[str, Any]] = []
    assignments: list[dict[str, Any]] = []
    centroids: dict[str, np.ndarray] = {}
    algorithms_used: set[str] = set()

    for vertical, indexed_items in items_by_vertical.items():
        vertical_indices = [index for index, _ in indexed_items]
        vertical_vectors = embeddings[vertical_indices]
        previous_vertical_items = [item for item in previous_items if item["vertical"] == vertical]
        previous_vertical_indices = previous_indices_by_vertical.get(vertical, [])

        labels, algorithm = run_clustering(vertical_vectors)
        algorithms_used.add(algorithm)
        grouped_clusters = _cluster_indices(labels)

        for cluster_name, local_indices in grouped_clusters.items():
            cluster_global_indices = [vertical_indices[local_index] for local_index in local_indices]
            cluster_items = [current_items[index] for index in cluster_global_indices]
            cluster_vectors = embeddings[cluster_global_indices]
            cluster_centroid = np.mean(cluster_vectors, axis=0)

            titles = [clean_title_for_label(item["title"]) for item in cluster_items]
            combined_texts = [f"{item['title']} {item.get('summary') or ''}" for item in cluster_items]
            seed_similarity_scores = cosine_similarity(cluster_vectors, cluster_centroid.reshape(1, -1)).ravel()
            phrase_candidates = build_phrase_candidates(
                cluster_items=cluster_items,
                similarity_scores=seed_similarity_scores,
                source_quality_by_name=source_quality_by_name,
            )
            keywords = extract_keywords(titles)
            entities = extract_entities(combined_texts)
            signals = set(keywords)
            novelty_terms = set(keywords)
            for phrase, _ in phrase_candidates[:3]:
                phrase_tokens = set(tokenize(phrase))
                signals.update(phrase_tokens)
                novelty_terms.update(phrase_tokens)
            for entity in entities:
                entity_tokens = set(tokenize(entity))
                signals.update(entity_tokens)
                novelty_terms.update(entity_tokens)

            volume_now = len(cluster_items)
            weighted_volume_now = compute_effective_weighted_volume(cluster_items, source_quality_by_name)
            previous_vertical_vectors = (
                previous_embeddings[previous_vertical_indices]
                if previous_vertical_indices and previous_embeddings.size > 0
                else np.empty((0, cluster_centroid.shape[0]), dtype=np.float32)
            )
            previous_signal_matches = matching_signal_items(previous_vertical_items, signals)

            weighted_similarity_prev = estimate_previous_volume_by_similarity(
                centroid=cluster_centroid,
                previous_vectors=previous_vertical_vectors,
                previous_items=previous_vertical_items,
                source_quality_by_name=source_quality_by_name,
            )
            weighted_token_prev = compute_effective_weighted_volume(previous_signal_matches, source_quality_by_name)
            weighted_volume_prev = max(weighted_similarity_prev, weighted_token_prev)

            raw_similarity_prev = estimate_previous_volume_by_similarity(
                centroid=cluster_centroid,
                previous_vectors=previous_vertical_vectors,
            )
            raw_token_prev = float(len(previous_signal_matches))
            volume_prev = int(round(max(raw_similarity_prev, raw_token_prev)))

            momentum = compute_momentum(weighted_volume_now, weighted_volume_prev)
            momentum = float(np.clip(momentum, -4.0, 4.0))
            novelty = compute_novelty(novelty_terms, baseline_terms)
            diversity = len({item["source_name"] for item in cluster_items})
            source_quality_score = float(
                np.mean(
                    [
                        source_quality_by_name.get(item.get("source_name") or "", 1.0)
                        for item in cluster_items
                    ]
                )
            )
            local_source_count = sum(
                1 for item in cluster_items if (item.get("source_name") or "").startswith("gnews_geo_")
            )
            local_source_ratio = local_source_count / max(volume_now, 1)
            topic_vertical = "local" if vertical == "world" and local_source_ratio >= 0.55 else vertical
            sparkline = build_sparkline(
                timestamps=[item["published_at"] for item in cluster_items if item.get("published_at")],
                start_at=window_start,
                end_at=window_end,
                buckets=SPARKLINE_BUCKETS,
            )

            representative_rows, representative_title, cluster_similarity_scores = select_representative_items(
                cluster_items=cluster_items,
                cluster_vectors=cluster_vectors,
                centroid=cluster_centroid,
                source_quality_by_name=source_quality_by_name,
            )
            search_corpus = " ".join(
                [
                    " ".join(item["title"] for item in cluster_items),
                    " ".join((item.get("summary") or "") for item in cluster_items),
                    " ".join(keywords),
                    " ".join(entities),
                    " ".join(phrase for phrase, _ in phrase_candidates),
                ]
            ).lower()

            topic_id_source = f"{window}|{vertical}|{cluster_name}|{'|'.join(sorted(item['item_id'] for item in cluster_items))}"
            topic_id = stable_hash(topic_id_source)[:16]
            label, label_confidence = build_topic_label(
                vertical=topic_vertical,
                keywords=keywords,
                entities=entities,
                representative_title=representative_title,
                phrase_candidates=phrase_candidates,
            )
            label_alignment_confidence = compute_label_alignment_confidence(
                label=label,
                representative_title=representative_title,
                label_confidence=label_confidence,
                keywords=keywords,
                entities=entities,
            )
            trust_contract = build_topic_trust_contract(
                label_alignment_confidence=label_alignment_confidence,
                source_quality_score=source_quality_score,
                novelty=novelty,
                momentum=momentum,
                diversity=diversity,
            )
            if label_alignment_confidence < TOPIC_LABEL_ALIGNMENT_MIN:
                label = f"Unresolved {topic_vertical} narrative"
            summary = build_topic_summary(
                label=label,
                representative_items=representative_rows,
                diversity=diversity,
                momentum=momentum,
                novelty=novelty,
            )

            topic_row = {
                "topic_id": topic_id,
                "label": label,
                "vertical": topic_vertical,
                "volume_now": volume_now,
                "volume_prev": volume_prev,
                "momentum": round(momentum, 4),
                "novelty": round(novelty, 4),
                "diversity": diversity,
                "weighted_volume_now": round(weighted_volume_now, 3),
                "weighted_volume_prev": round(weighted_volume_prev, 3),
                "source_quality_score": round(source_quality_score, 4),
                "label_confidence": round(label_confidence, 4),
                "trust_contract": trust_contract,
                "sparkline": sparkline,
                "representative_items": representative_rows,
                "keywords": keywords[:10],
                "entities": entities[:10],
                "related_topic_ids": [],
                "summary": summary,
                "search_corpus": search_corpus,
            }
            topics.append(topic_row)
            centroids[topic_id] = cluster_centroid

            for local_item_index, item in enumerate(cluster_items):
                assignments.append({
                    "topic_id": topic_id,
                    "item_id": item["item_id"],
                    "score": round(float(cluster_similarity_scores[local_item_index]), 6)
                    if local_item_index < len(cluster_similarity_scores)
                    else None,
                })

    if topics:
        topic_ids = [topic["topic_id"] for topic in topics]
        centroid_matrix = np.asarray([centroids[topic_id] for topic_id in topic_ids], dtype=np.float32)
        similarities = cosine_similarity(centroid_matrix)

        for row_index, topic in enumerate(topics):
            ranking = np.argsort(similarities[row_index])[::-1]
            related: list[str] = []
            for candidate_index in ranking:
                if candidate_index == row_index:
                    continue
                candidate_score = similarities[row_index][candidate_index]
                if candidate_score < 0.2:
                    continue
                related.append(topic_ids[candidate_index])
                if len(related) >= 3:
                    break
            topic["related_topic_ids"] = related

    algorithm_label = "+".join(sorted(algorithms_used)) if algorithms_used else "none"
    return topics, assignments, algorithm_label
