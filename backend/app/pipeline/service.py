from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import httpx
import numpy as np

from app.core.config import settings
from app.core.database import Database
from app.core.hash_utils import clean_url, stable_hash
from app.core.time_windows import DEFAULT_WINDOW, parse_window, supported_windows
from app.pipeline.decision_briefing import (
    RISK_LIMITS_BY_PROFILE,
    apply_risk_controls,
    build_changes,
    build_decisions,
    build_topic_signals,
    resolve_profile,
    supported_profiles,
)
from app.pipeline.embeddings import EmbeddingEngine
from app.pipeline.market_data import MarketDataService
from app.pipeline.source_quality import build_source_quality_scores
from app.pipeline.topic_model import build_topics
from app.sources.base import SourceDefinition
from app.sources.registry import SOURCES
from app.schemas.models import Item, SourceRawItem

logger = logging.getLogger(__name__)
DECISION_ENGINE_VERSION = "2026-02-impact-v3"
LIFECYCLE_STAGE_ORDER = ("candidates", "considered", "recommended", "approved", "opened", "closed", "expired")
TOPIC_LABEL_ALIGNMENT_MIN = 0.52
TOPIC_SOURCE_QUALITY_MIN = 0.72
TOPIC_NOVELTY_MIN = 0.2
TOPIC_MOMENTUM_MIN = 0.18
TOPIC_DIVERSITY_MIN = 3


class NarrativeService:
    def __init__(self, database: Database):
        self.database = database
        self.sources: list[SourceDefinition] = SOURCES
        self.embedding_engine = EmbeddingEngine(database=database)
        self.market_data = MarketDataService(database=database)
        self._refresh_tasks: dict[str, asyncio.Task] = {}
        self._refresh_lock = asyncio.Lock()

    def get_source_quality_scores(self) -> dict[str, float]:
        source_health_rows = self.database.get_source_health()
        return build_source_quality_scores(
            sources=self.sources,
            source_health_rows=source_health_rows,
        )

    def source_health_snapshot(self) -> list[dict[str, Any]]:
        persisted_rows = {row["source_name"]: row for row in self.database.get_source_health()}
        snapshot: list[dict[str, Any]] = []
        for source in self.sources:
            row = persisted_rows.get(source.name)
            if row:
                snapshot.append(
                    {
                        "source_name": row["source_name"],
                        "last_success": row["last_success"].isoformat() if row["last_success"] else None,
                        "last_error": row["last_error"],
                        "items_fetched": row["items_fetched"],
                        "latency_ms": row["latency_ms"],
                        "updated_at": row["updated_at"].isoformat() if row["updated_at"] else None,
                    }
                )
                continue
            snapshot.append(
                {
                    "source_name": source.name,
                    "last_success": None,
                    "last_error": None,
                    "items_fetched": 0,
                    "latency_ms": 0,
                    "updated_at": None,
                }
            )
        return snapshot

    def seed_cache_if_needed(self) -> None:
        seed_payloads: dict[str, dict[str, Any]] = {}
        for window in supported_windows():
            seed_file = Path(settings.seed_directory) / f"seed_topics_{window}.json"
            if not seed_file.exists():
                continue
            with seed_file.open("r", encoding="utf-8") as file_handle:
                seed_payloads[window] = json.load(file_handle)
        if seed_payloads:
            self.database.seed_topic_cache_if_empty(seed_payloads)

    async def _fetch_source_with_backoff(
        self,
        source: SourceDefinition,
        force: bool = False,
    ) -> tuple[list[SourceRawItem], int, str | None, bool]:
        now = datetime.now(timezone.utc)
        if not force:
            last_success = self.database.get_source_last_success(source.name)
            if last_success and (now - last_success) < timedelta(minutes=source.cadence_minutes):
                return [], 0, None, True

        max_items = min(source.max_items, settings.max_items_per_source)
        attempts = 3
        delay_seconds = 0.7

        for attempt in range(attempts):
            started = time.perf_counter()
            try:
                items = await source.fetcher(max_items)
                latency_ms = int((time.perf_counter() - started) * 1000)
                return items, latency_ms, None, False
            except Exception as exc:
                latency_ms = int((time.perf_counter() - started) * 1000)
                if attempt >= attempts - 1:
                    return [], latency_ms, str(exc), False
                await asyncio.sleep(delay_seconds)
                delay_seconds *= 2

        return [], 0, "unknown fetch failure", False

    def _normalize_item(self, source: SourceDefinition, raw_item: SourceRawItem, fetched_at: datetime) -> Item | None:
        cleaned = clean_url(raw_item.url)
        if not cleaned:
            return None

        published_at = raw_item.published_at.astimezone(timezone.utc)
        identity_key = f"{cleaned}|{published_at.isoformat()}|{raw_item.title.strip().lower()}"

        return Item(
            item_id=stable_hash(identity_key),
            source_name=source.name,
            vertical=source.vertical,
            title=raw_item.title.strip(),
            url=cleaned,
            published_at=published_at,
            fetched_at=fetched_at,
            summary=raw_item.summary,
            raw_text=raw_item.raw_text,
        )

    async def ingest_sources(self, force: bool = False) -> dict[str, Any]:
        semaphore = asyncio.Semaphore(4)
        ingested_total = 0
        inserted_total = 0
        failures: list[dict[str, str]] = []

        async def process_source(source: SourceDefinition) -> None:
            nonlocal ingested_total, inserted_total
            async with semaphore:
                fetched_at = datetime.now(timezone.utc)
                raw_items, latency_ms, error, skipped = await self._fetch_source_with_backoff(
                    source=source,
                    force=force,
                )
                if skipped:
                    return

                if error:
                    previous_success = self.database.get_source_last_success(source.name)
                    self.database.upsert_source_health(
                        source_name=source.name,
                        last_success=previous_success,
                        last_error=error,
                        items_fetched=0,
                        latency_ms=latency_ms,
                    )
                    failures.append({"source": source.name, "error": error})
                    logger.warning(
                        "source_fetch_failed",
                        extra={"extra": {"source": source.name, "error": error}},
                    )
                    return

                normalized_batch: list[Item] = []
                seen_urls: set[str] = set()
                for raw_item in raw_items:
                    normalized = self._normalize_item(source=source, raw_item=raw_item, fetched_at=fetched_at)
                    if not normalized:
                        continue
                    if normalized.url in seen_urls:
                        continue
                    seen_urls.add(normalized.url)
                    normalized_batch.append(normalized)

                ingested_total += len(normalized_batch)
                inserted = self.database.upsert_items(normalized_batch)
                inserted_total += inserted

                self.database.upsert_source_health(
                    source_name=source.name,
                    last_success=fetched_at,
                    last_error=None,
                    items_fetched=len(normalized_batch),
                    latency_ms=latency_ms,
                )

        await asyncio.gather(*(process_source(source) for source in self.sources))

        return {
            "ingested_items": ingested_total,
            "inserted_items": inserted_total,
            "failures": failures,
        }

    async def generate_topics(self, window: str) -> dict[str, Any]:
        window_delta = parse_window(window)
        now = datetime.now(timezone.utc)
        window_start = now - window_delta
        previous_start = window_start - window_delta
        baseline_start = now - timedelta(days=1)
        source_quality_scores = self.get_source_quality_scores()

        current_items = self.database.get_items_between(start_at=window_start, end_at=now)
        previous_items = self.database.get_items_between(start_at=previous_start, end_at=window_start)
        baseline_items = self.database.get_items_between(start_at=baseline_start, end_at=window_start)

        algorithm = "none"
        topics: list[dict[str, Any]] = []
        assignments: list[dict[str, Any]] = []
        previous_embeddings = np.empty((0, 0), dtype=np.float32)

        if current_items:
            texts = [f"{item['title']} {item.get('summary') or ''}".strip() for item in current_items]
            item_ids = [item["item_id"] for item in current_items]
            embeddings, embedding_model_name = await self.embedding_engine.embed_items(item_ids=item_ids, texts=texts)

            if previous_items:
                previous_texts = [f"{item['title']} {item.get('summary') or ''}".strip() for item in previous_items]
                previous_item_ids = [item["item_id"] for item in previous_items]
                previous_embeddings, _ = await self.embedding_engine.embed_items(
                    item_ids=previous_item_ids,
                    texts=previous_texts,
                )

            topics, assignments, clustering_algorithm = build_topics(
                window=window,
                current_items=current_items,
                previous_items=previous_items,
                previous_embeddings=previous_embeddings,
                baseline_items=baseline_items,
                embeddings=embeddings,
                window_start=window_start,
                window_end=now,
                source_quality_by_name=source_quality_scores,
            )
            algorithm = f"{embedding_model_name}+{clustering_algorithm}"

        metadata = {
            "generated_at": now.isoformat(),
            "window": window,
            "item_count": len(current_items),
            "source_health": self.source_health_snapshot(),
            "source_quality": source_quality_scores,
            "algorithm": algorithm,
        }
        payload = {
            "topics": topics,
            "metadata": metadata,
        }

        run_id = stable_hash(f"{window}:{metadata['generated_at']}:{metadata['item_count']}")
        self.database.save_topic_run(
            run_id=run_id,
            window=window,
            generated_at=now,
            item_count=len(current_items),
            algorithm=algorithm,
            topic_rows=topics,
            assignments=assignments,
        )
        self.database.save_topic_cache(window=window, generated_at=now, payload=payload)
        return payload

    async def refresh_all_windows(self, force: bool = False) -> dict[str, Any]:
        async with self._refresh_lock:
            ingest_summary = await self.ingest_sources(force=force)
            results: dict[str, Any] = {}
            for window in supported_windows():
                results[window] = await self.generate_topics(window=window)
            alert_summary: dict[str, Any] | None = None
            try:
                alert_summary = self.evaluate_alert_rules()
            except Exception as exc:
                logger.warning(
                    "alert_evaluation_failed",
                    extra={"extra": {"error": str(exc)}},
                )
            return {
                "ingest": ingest_summary,
                "windows": list(results.keys()),
                "alerts": alert_summary,
            }

    def _load_seed_payload(self, window: str) -> dict[str, Any] | None:
        seed_file = Path(settings.seed_directory) / f"seed_topics_{window}.json"
        if not seed_file.exists():
            return None
        with seed_file.open("r", encoding="utf-8") as file_handle:
            return json.load(file_handle)

    def _stale(self, generated_at: str | None) -> bool:
        if not generated_at:
            return True
        try:
            parsed = datetime.fromisoformat(generated_at)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            age_seconds = (datetime.now(timezone.utc) - parsed.astimezone(timezone.utc)).total_seconds()
            return age_seconds >= settings.cache_stale_seconds
        except Exception:
            return True

    @staticmethod
    def _parse_iso_datetime(value: str | None) -> datetime | None:
        if not value:
            return None
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except Exception:
            return None

    def _build_filter_signature(
        self,
        *,
        window: str,
        verticals: list[str] | None,
        sources: list[str] | None,
        only_rising: bool,
        search: str | None,
    ) -> str:
        normalized_verticals, normalized_sources = self._normalize_filter_scope(
            verticals=verticals,
            sources=sources,
        )
        signature_payload = {
            "window": window,
            "verticals": normalized_verticals or [],
            "sources": normalized_sources or [],
            "only_rising": bool(only_rising),
            "search": (search or "").strip().lower(),
        }
        return stable_hash(json.dumps(signature_payload, sort_keys=True))

    def _normalize_filter_scope(
        self,
        *,
        verticals: list[str] | None,
        sources: list[str] | None,
    ) -> tuple[list[str] | None, list[str] | None]:
        normalized_verticals = sorted({str(value).strip() for value in (verticals or []) if str(value).strip()})
        normalized_sources = sorted({str(value).strip() for value in (sources or []) if str(value).strip()})

        all_verticals = {source.vertical for source in self.sources}
        all_sources = {source.name for source in self.sources}

        if normalized_verticals and set(normalized_verticals).issuperset(all_verticals):
            normalized_verticals = []
        if normalized_sources and set(normalized_sources).issuperset(all_sources):
            normalized_sources = []

        return (normalized_verticals or None, normalized_sources or None)

    @staticmethod
    def _is_current_decision_snapshot(payload: dict[str, Any]) -> bool:
        version = str(payload.get("decision_engine_version") or "").strip()
        return version == DECISION_ENGINE_VERSION

    @staticmethod
    def _build_lifecycle_summary(
        *,
        candidates: int,
        considered: int,
        recommended: int,
        approved: int,
        opened: int,
        closed: int,
        expired: int,
    ) -> dict[str, Any]:
        counts = {
            "candidates": max(0, int(candidates)),
            "considered": max(0, int(considered)),
            "recommended": max(0, int(recommended)),
            "approved": max(0, int(approved)),
            "opened": max(0, int(opened)),
            "closed": max(0, int(closed)),
            "expired": max(0, int(expired)),
        }
        explanations: list[str] = []
        corrective_ctas: list[str] = []

        gap_rules: list[tuple[str, str, str, str]] = [
            (
                "candidates",
                "considered",
                "Candidates exist but none were considered in this pass.",
                "Refresh the window and check filter scope to move candidates into considered.",
            ),
            (
                "considered",
                "recommended",
                "Narratives were considered but none cleared recommendation gates.",
                "Review trust warnings and widen source/vertical scope for more qualified recommendations.",
            ),
            (
                "recommended",
                "approved",
                "Recommendations exist but none were approved by risk controls.",
                "Open Decision Briefing and adjust profile/risk caps to approve candidates.",
            ),
            (
                "approved",
                "opened",
                "Trades were approved but no positions are open.",
                "Open the Paper Portfolio view to inspect expiry/closure state and entry timing.",
            ),
            (
                "opened",
                "closed",
                "Open positions exist but none have reached a closed state yet.",
                "Wait for expiry/exit windows, then review realized outcomes in Paper Portfolio.",
            ),
        ]
        for upstream, downstream, explanation, cta in gap_rules:
            if counts[upstream] > 0 and counts[downstream] == 0:
                explanations.append(explanation)
                corrective_ctas.append(cta)

        if all(counts[stage] == 0 for stage in LIFECYCLE_STAGE_ORDER):
            explanations.append("No lifecycle activity yet for this filter scope.")
            corrective_ctas.append("Start in Action Center or Decision Briefing to generate the first lifecycle events.")

        return {
            **counts,
            "explanations": explanations,
            "corrective_ctas": corrective_ctas,
        }

    @staticmethod
    def _clamp(value: float, minimum: float = 0.0, maximum: float = 1.0) -> float:
        return max(minimum, min(maximum, value))

    def _ensure_topic_trust_contract(self, topic: dict[str, Any]) -> dict[str, Any]:
        topic_row = dict(topic)
        trust_contract = topic_row.get("trust_contract") if isinstance(topic_row.get("trust_contract"), dict) else None
        if trust_contract is None:
            label_alignment_confidence = float(topic_row.get("label_confidence", 0.45) or 0.45)
            source_quality_score = float(topic_row.get("source_quality_score", 1.0) or 1.0)
            novelty_confidence = self._clamp(float(topic_row.get("novelty", 0.0) or 0.0), 0.0, 1.0)
            momentum = float(topic_row.get("momentum", 0.0) or 0.0)
            diversity = int(topic_row.get("diversity", 0) or 0)
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
            trust_contract = {
                "label_alignment_confidence": round(label_alignment_confidence, 4),
                "proxy_confidence": 0.0,
                "source_quality_score": round(source_quality_score, 4),
                "liquidity_link_confidence": None,
                "novelty_confidence": round(novelty_confidence, 4),
                "eligible_for_act_now": (
                    label_alignment_confidence >= TOPIC_LABEL_ALIGNMENT_MIN
                    and source_quality_score >= TOPIC_SOURCE_QUALITY_MIN
                    and novelty_confidence >= TOPIC_NOVELTY_MIN
                    and momentum >= TOPIC_MOMENTUM_MIN
                    and diversity >= TOPIC_DIVERSITY_MIN
                ),
                "warnings": warnings,
            }
            topic_row["trust_contract"] = trust_contract

        if float(trust_contract.get("label_alignment_confidence", 0.0) or 0.0) < TOPIC_LABEL_ALIGNMENT_MIN:
            vertical = str(topic_row.get("vertical") or "narrative").strip().lower() or "narrative"
            topic_row["label"] = f"Unresolved {vertical} narrative"
        return topic_row

    def _filter_topics(
        self,
        payload: dict[str, Any],
        verticals: list[str] | None,
        sources: list[str] | None,
        only_rising: bool,
        search: str | None,
    ) -> dict[str, Any]:
        topics = payload.get("topics", [])
        filtered_topics: list[dict[str, Any]] = []
        search_text = (search or "").strip().lower()
        search_terms = [token for token in re.findall(r"[a-z0-9]+", search_text) if token]

        for raw_topic in topics:
            topic = self._ensure_topic_trust_contract(raw_topic)
            if verticals and topic.get("vertical") not in verticals:
                continue
            if only_rising and float(topic.get("momentum", 0.0)) <= 0:
                continue
            if sources:
                representative_sources = {
                    item.get("source_name") for item in topic.get("representative_items", [])
                }
                if representative_sources.isdisjoint(sources):
                    continue
            if search_terms:
                representative_titles = " ".join(
                    item.get("title", "") for item in topic.get("representative_items", [])
                )
                haystack = " ".join(
                    [
                        topic.get("label", ""),
                        topic.get("summary", ""),
                        " ".join(topic.get("keywords", [])),
                        " ".join(topic.get("entities", [])),
                        representative_titles,
                        topic.get("search_corpus", ""),
                    ]
                ).lower()
                if not all(term in haystack for term in search_terms):
                    continue
            filtered_topics.append(topic)

        metadata = payload.get("metadata", {})
        metadata["source_health"] = self.source_health_snapshot()
        recommended_count = sum(
            1
            for topic in filtered_topics
            if bool((topic.get("trust_contract") or {}).get("eligible_for_act_now", False))
        )
        metadata["lifecycle"] = self._build_lifecycle_summary(
            candidates=len(filtered_topics),
            considered=len(filtered_topics),
            recommended=recommended_count,
            approved=0,
            opened=0,
            closed=0,
            expired=0,
        )

        return {
            "topics": filtered_topics,
            "metadata": metadata,
        }

    def briefing_profiles(self) -> list[str]:
        return supported_profiles()

    def get_decision_briefing(
        self,
        *,
        window: str,
        profile: str,
        top_n: int,
        verticals: list[str] | None,
        sources: list[str] | None,
        only_rising: bool,
        search: str | None,
    ) -> dict[str, Any]:
        profile_key, _ = resolve_profile(profile)
        clamped_top_n = max(5, min(top_n, 50))
        normalized_verticals, normalized_sources = self._normalize_filter_scope(
            verticals=verticals,
            sources=sources,
        )

        topics_payload = self.get_topics(
            window=window,
            verticals=normalized_verticals,
            sources=normalized_sources,
            only_rising=only_rising,
            search=search,
        )
        topics = topics_payload.get("topics", [])
        metadata = topics_payload.get("metadata", {})

        decisions, score_by_topic = build_decisions(
            topics=topics,
            profile=profile_key,
            top_n=clamped_top_n,
            window=window,
        )
        current_signals = build_topic_signals(topics)

        generated_at_raw = str(metadata.get("generated_at") or datetime.now(timezone.utc).isoformat())
        try:
            generated_at = datetime.fromisoformat(generated_at_raw.replace("Z", "+00:00"))
            if generated_at.tzinfo is None:
                generated_at = generated_at.replace(tzinfo=timezone.utc)
        except Exception:
            generated_at = datetime.now(timezone.utc)

        filter_signature = self._build_filter_signature(
            window=window,
            verticals=normalized_verticals,
            sources=normalized_sources,
            only_rising=only_rising,
            search=search,
        )
        previous_snapshot = self.database.get_latest_decision_snapshot(
            window=window,
            filter_signature=filter_signature,
            profile=profile_key,
            before_generated_at=generated_at,
        )
        if previous_snapshot and not self._is_current_decision_snapshot(previous_snapshot.get("payload") or {}):
            previous_snapshot = None

        previous_signals: list[dict[str, Any]] = []
        previous_generated_at: str | None = None
        if previous_snapshot:
            previous_generated_at = previous_snapshot.get("generated_at")
            payload = previous_snapshot.get("payload", {})
            previous_signals = payload.get("topic_signals", [])

        day_start = generated_at.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        snapshots_today = self.database.get_decision_snapshots_since(
            window=window,
            filter_signature=filter_signature,
            profile=profile_key,
            since_generated_at=day_start,
        )
        historical_executions: list[dict[str, Any]] = []
        for snapshot_row in snapshots_today:
            snapshot_payload = snapshot_row.get("payload", {})
            if not self._is_current_decision_snapshot(snapshot_payload):
                continue
            historical_executions.extend(snapshot_payload.get("executed_trades", []))

        decisions, executed_now, risk_summary = apply_risk_controls(
            decisions=decisions,
            profile=profile_key,
            generated_at=generated_at,
            historical_executions=historical_executions,
        )
        default_holding_delta = parse_window(window)
        for execution in executed_now:
            opened_at = self._parse_iso_datetime(str(execution.get("opened_at") or ""))
            if not opened_at:
                continue
            if not execution.get("expires_at"):
                execution["expires_at"] = (opened_at + default_holding_delta).isoformat()
            execution.setdefault("position_status", "OPEN")

        changes = build_changes(
            current_signals=current_signals,
            previous_signals=previous_signals,
            score_by_topic=score_by_topic,
            limit=min(clamped_top_n, 12),
        )

        action_counts = {
            "act_now": sum(1 for decision in decisions if decision.get("action_bucket") == "Act now"),
            "monitor": sum(1 for decision in decisions if decision.get("action_bucket") == "Monitor"),
            "ignore": sum(1 for decision in decisions if decision.get("action_bucket") == "Ignore"),
        }
        historical_opened = 0
        historical_closed = 0
        historical_expired = 0
        for execution in historical_executions:
            status = str(execution.get("position_status") or execution.get("status") or "OPEN").strip().upper()
            if status == "CLOSED":
                historical_closed += 1
            elif status == "EXPIRED":
                historical_expired += 1
            else:
                historical_opened += 1

        lifecycle = self._build_lifecycle_summary(
            candidates=len(topics),
            considered=len(current_signals),
            recommended=action_counts["act_now"] + action_counts["monitor"],
            approved=risk_summary["approved_trade_count"],
            opened=historical_opened + risk_summary["approved_trade_count"],
            closed=historical_closed,
            expired=historical_expired,
        )
        summary = {
            "window": window,
            "profile": profile_key,
            "generated_at": generated_at.isoformat(),
            "previous_generated_at": previous_generated_at,
            "considered_topics": len(topics),
            "top_n": clamped_top_n,
            "actionable_now": action_counts["act_now"],
            "monitor_count": action_counts["monitor"],
            "ignore_count": action_counts["ignore"],
            "new_count": len(changes["new_narratives"]),
            "accelerating_count": len(changes["accelerating"]),
            "fading_count": len(changes["fading"]),
            "approved_trade_count": risk_summary["approved_trade_count"],
            "blocked_trade_count": risk_summary["blocked_trade_count"],
            "used_daily_notional_pct": risk_summary["used_daily_notional_pct"],
            "max_daily_notional_pct": risk_summary["max_daily_notional_pct"],
            "max_simultaneous_themes": risk_summary["max_simultaneous_themes"],
            "lifecycle": lifecycle,
        }

        snapshot_payload = {
            "summary": summary,
            "topic_signals": current_signals,
            "decision_topic_ids": [decision["topic_id"] for decision in decisions if decision.get("topic_id")],
            "decisions": decisions,
            "executed_trades": executed_now,
            "risk_summary": risk_summary,
            "filter_signature": filter_signature,
            "decision_engine_version": DECISION_ENGINE_VERSION,
        }
        snapshot_recorded_at = datetime.now(timezone.utc)
        snapshot_id = stable_hash(
            f"{window}|{profile_key}|{filter_signature}|{generated_at.isoformat()}|{snapshot_recorded_at.isoformat()}|{len(current_signals)}"
        )[:24]
        self.database.save_decision_snapshot(
            snapshot_id=snapshot_id,
            window=window,
            filter_signature=filter_signature,
            profile=profile_key,
            generated_at=snapshot_recorded_at,
            payload=snapshot_payload,
        )

        return {
            "summary": summary,
            "changes": changes,
            "decisions": decisions,
            "risk_controls": risk_summary,
            "metadata": {
                "generated_at": metadata.get("generated_at"),
                "window": metadata.get("window", window),
                "item_count": metadata.get("item_count", 0),
                "algorithm": metadata.get("algorithm", "unknown"),
            },
        }

    @staticmethod
    def _topic_signal_from_snapshot(payload: dict[str, Any]) -> list[dict[str, Any]]:
        decisions = payload.get("decisions") or []
        if isinstance(decisions, list) and decisions:
            return [
                {
                    "topic_id": row.get("topic_id"),
                    "label": row.get("label"),
                    "vertical": row.get("vertical"),
                    "momentum": float(row.get("momentum", 0.0) or 0.0),
                    "action_bucket": row.get("action_bucket"),
                    "execution_status": row.get("execution_status"),
                    "trade_tickers": list(row.get("trade_tickers") or []),
                    "trade_direction": row.get("trade_direction") or "Operational",
                    "trade_theme": row.get("trade_theme"),
                    "asset_mapping": row.get("asset_mapping"),
                }
                for row in decisions
                if row.get("topic_id")
            ]
        topic_signals = payload.get("topic_signals") or []
        return [
            {
                "topic_id": row.get("topic_id"),
                "label": row.get("label"),
                "vertical": row.get("vertical"),
                "momentum": float(row.get("momentum", 0.0) or 0.0),
                "action_bucket": None,
                "execution_status": None,
                "trade_tickers": [],
                "trade_direction": "Operational",
                "trade_theme": None,
                "asset_mapping": None,
            }
            for row in topic_signals
            if row.get("topic_id")
        ]

    @staticmethod
    def _find_signal_match(snapshot_payload: dict[str, Any], event: dict[str, Any]) -> dict[str, Any] | None:
        topic_id = str(event.get("topic_id") or "")
        label = str(event.get("label") or "").strip().lower()
        candidates = NarrativeService._topic_signal_from_snapshot(snapshot_payload)
        for candidate in candidates:
            if topic_id and str(candidate.get("topic_id") or "") == topic_id:
                return candidate
        for candidate in candidates:
            if label and str(candidate.get("label") or "").strip().lower() == label:
                return candidate
        return None

    def get_backtest(
        self,
        *,
        window: str,
        profile: str,
        verticals: list[str] | None,
        sources: list[str] | None,
        only_rising: bool,
        search: str | None,
    ) -> dict[str, Any]:
        profile_key, _ = resolve_profile(profile)
        normalized_verticals, normalized_sources = self._normalize_filter_scope(
            verticals=verticals,
            sources=sources,
        )
        filter_signature = self._build_filter_signature(
            window=window,
            verticals=normalized_verticals,
            sources=normalized_sources,
            only_rising=only_rising,
            search=search,
        )
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=45)

        def load_snapshots(scope_signature: str | None) -> list[dict[str, Any]]:
            snapshot_rows = self.database.get_decision_snapshots_range(
                window=window,
                filter_signature=scope_signature,
                profile=profile_key,
                since_generated_at=since,
            )
            loaded: list[dict[str, Any]] = []
            for row in snapshot_rows:
                generated_at = self._parse_iso_datetime(row.get("generated_at"))
                if not generated_at:
                    continue
                payload = row.get("payload") or {}
                if not self._is_current_decision_snapshot(payload):
                    continue
                loaded.append(
                    {
                        "generated_at": generated_at,
                        "payload": payload,
                    }
                )
            return loaded

        def collect_signal_events(snapshot_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
            events: list[dict[str, Any]] = []
            for snapshot in snapshot_rows:
                decisions = (snapshot["payload"].get("decisions") or [])
                if not isinstance(decisions, list):
                    continue
                for decision in decisions:
                    if decision.get("action_bucket") != "Act now":
                        continue
                    events.append(
                        {
                            "event_time": snapshot["generated_at"],
                            "topic_id": decision.get("topic_id"),
                            "label": decision.get("label"),
                            "vertical": decision.get("vertical"),
                            "momentum_at_signal": float(decision.get("momentum", 0.0) or 0.0),
                            "trade_direction": str(decision.get("trade_direction") or "Operational"),
                            "trade_tickers": list(decision.get("trade_tickers") or []),
                            "execution_status": decision.get("execution_status"),
                        }
                    )
            return events

        snapshots = load_snapshots(filter_signature)
        signal_events = collect_signal_events(snapshots)
        snapshot_scope = "filtered"
        if len(snapshots) < 3 or len(signal_events) < 2:
            relaxed_snapshots = load_snapshots(None)
            relaxed_events = collect_signal_events(relaxed_snapshots)
            if len(relaxed_events) > len(signal_events):
                snapshots = relaxed_snapshots
                signal_events = relaxed_events
                snapshot_scope = "all_filters_fallback"

        signal_events.sort(key=lambda row: row["event_time"])

        horizons: list[dict[str, Any]] = []
        for days in (7, 30):
            cutoff = now - timedelta(days=days)
            evaluated_events = [event for event in signal_events if event["event_time"] <= cutoff]
            evaluation_mode = "full"
            expected_horizon = f"{days}d"
            is_fallback = False
            fallback_reason: str | None = None
            effective_horizon = expected_horizon
            if not evaluated_events:
                evaluation_mode = "warmup"
                evaluated_events = list(signal_events)
                is_fallback = True
                fallback_reason = "insufficient_mature_followups_for_expected_horizon"
                effective_horizon = "next_snapshot"
            momentum_hits = 0
            momentum_evaluated = 0
            proxy_hits = 0
            proxy_evaluated = 0
            proxy_returns: list[float] = []
            full_horizon_evaluated = 0
            samples: list[dict[str, Any]] = []

            for event in evaluated_events:
                target_time = event["event_time"] + timedelta(days=days)
                followup_snapshot = next((row for row in snapshots if row["generated_at"] >= target_time), None)
                horizon_coverage = 1.0
                if not followup_snapshot and evaluation_mode == "warmup":
                    followup_snapshot = next((row for row in snapshots if row["generated_at"] > event["event_time"]), None)
                    if followup_snapshot:
                        elapsed_days = max(
                            0.0,
                            (followup_snapshot["generated_at"] - event["event_time"]).total_seconds() / 86400.0,
                        )
                        horizon_coverage = max(0.0, min(1.0, elapsed_days / max(days, 1)))
                if not followup_snapshot:
                    continue
                if followup_snapshot["generated_at"] <= event["event_time"]:
                    continue
                matched_signal = self._find_signal_match(followup_snapshot["payload"], event)
                if not matched_signal:
                    continue

                followup_momentum = float(matched_signal.get("momentum", 0.0) or 0.0)
                momentum_evaluated += 1
                if horizon_coverage >= 1.0:
                    full_horizon_evaluated += 1
                momentum_threshold = 0.08 if horizon_coverage >= 1.0 else -0.02
                momentum_persisted = followup_momentum >= momentum_threshold
                if momentum_persisted:
                    momentum_hits += 1

                proxy_result = {
                    "available": False,
                    "basket_return": None,
                    "leg_returns": [],
                }
                tickers = list(event.get("trade_tickers") or [])
                direction = str(event.get("trade_direction") or "Operational")
                if tickers and direction not in {"None", "Operational"}:
                    proxy_result = self.market_data.basket_return(
                        tickers=tickers,
                        entry_at=event["event_time"],
                        exit_at=followup_snapshot["generated_at"],
                        direction=direction,
                    )
                proxy_hit = None
                if proxy_result["available"] and proxy_result["basket_return"] is not None:
                    proxy_evaluated += 1
                    basket_return = float(proxy_result["basket_return"])
                    proxy_returns.append(basket_return)
                    proxy_hit = basket_return > 0
                    if proxy_hit:
                        proxy_hits += 1

                if len(samples) < 25:
                    samples.append(
                        {
                            "topic_id": event.get("topic_id"),
                            "label": event.get("label"),
                            "event_time": event["event_time"].isoformat(),
                            "followup_time": followup_snapshot["generated_at"].isoformat(),
                            "momentum_at_signal": round(float(event.get("momentum_at_signal", 0.0)), 4),
                            "momentum_at_horizon": round(followup_momentum, 4),
                            "momentum_persisted": momentum_persisted,
                            "proxy_available": bool(proxy_result["available"]),
                            "proxy_return": proxy_result["basket_return"],
                            "proxy_positive": proxy_hit,
                            "trade_tickers": tickers,
                            "horizon_coverage": round(horizon_coverage, 4),
                            "evaluation_mode": evaluation_mode,
                            "is_fallback": bool(is_fallback or horizon_coverage < 1.0),
                            "fallback_reason": "next_snapshot_followup_used"
                            if (is_fallback or horizon_coverage < 1.0)
                            else None,
                            "effective_horizon": "next_snapshot" if (is_fallback or horizon_coverage < 1.0) else expected_horizon,
                            "expected_horizon": expected_horizon,
                        }
                    )

            momentum_precision = (momentum_hits / momentum_evaluated) if momentum_evaluated else 0.0
            proxy_precision = (proxy_hits / proxy_evaluated) if proxy_evaluated else 0.0
            average_proxy_return = (sum(proxy_returns) / len(proxy_returns)) if proxy_returns else 0.0

            horizons.append(
                {
                    "days": days,
                    "evaluated_signals": momentum_evaluated,
                    "momentum_persisted": momentum_hits,
                    "momentum_precision": round(momentum_precision, 4),
                    "proxy_evaluated": proxy_evaluated,
                    "proxy_positive": proxy_hits,
                    "proxy_precision": round(proxy_precision, 4),
                    "average_proxy_return": round(average_proxy_return, 6),
                    "evaluation_mode": evaluation_mode,
                    "full_horizon_evaluated": full_horizon_evaluated,
                    "warmup_evaluated": max(momentum_evaluated - full_horizon_evaluated, 0),
                    "is_fallback": is_fallback,
                    "fallback_reason": fallback_reason,
                    "effective_horizon": effective_horizon,
                    "expected_horizon": expected_horizon,
                    "samples": samples,
                }
            )

        approved_signals = sum(
            1
            for event in signal_events
            if str(event.get("execution_status") or "").strip().lower() == "approved"
        )
        lifecycle = self._build_lifecycle_summary(
            candidates=len(signal_events),
            considered=len(signal_events),
            recommended=len(signal_events),
            approved=approved_signals,
            opened=approved_signals,
            closed=0,
            expired=0,
        )

        return {
            "summary": {
                "window": window,
                "profile": profile_key,
                "generated_at": now.isoformat(),
                "filter_signature": filter_signature,
                "snapshot_scope": snapshot_scope,
                "history_snapshots": len(snapshots),
                "act_now_signals": len(signal_events),
                "lifecycle": lifecycle,
            },
            "horizons": horizons,
        }

    def get_paper_portfolio(
        self,
        *,
        window: str,
        profile: str,
        verticals: list[str] | None,
        sources: list[str] | None,
        only_rising: bool,
        search: str | None,
    ) -> dict[str, Any]:
        profile_key, _ = resolve_profile(profile)
        normalized_verticals, normalized_sources = self._normalize_filter_scope(
            verticals=verticals,
            sources=sources,
        )
        filter_signature = self._build_filter_signature(
            window=window,
            verticals=normalized_verticals,
            sources=normalized_sources,
            only_rising=only_rising,
            search=search,
        )
        now = datetime.now(timezone.utc)
        since = now - timedelta(days=60)
        default_holding_delta = parse_window(window)

        def load_snapshots(scope_signature: str | None) -> list[dict[str, Any]]:
            snapshot_rows = self.database.get_decision_snapshots_range(
                window=window,
                filter_signature=scope_signature,
                profile=profile_key,
                since_generated_at=since,
            )
            loaded: list[dict[str, Any]] = []
            for row in snapshot_rows:
                generated_at = self._parse_iso_datetime(row.get("generated_at"))
                if not generated_at:
                    continue
                payload = row.get("payload") or {}
                if not self._is_current_decision_snapshot(payload):
                    continue
                loaded.append(
                    {
                        "generated_at": generated_at,
                        "payload": payload,
                    }
                )
            return loaded

        def collect_positions(snapshot_rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
            collected_positions: dict[str, dict[str, Any]] = {}
            for snapshot in snapshot_rows:
                payload = snapshot["payload"]
                decision_map = {
                    str(row.get("topic_id")): row
                    for row in (payload.get("decisions") or [])
                    if row.get("topic_id")
                }
                for trade in payload.get("executed_trades", []):
                    topic_id = str(trade.get("topic_id") or "")
                    trade_theme = str(trade.get("trade_theme") or "")
                    opened_at = self._parse_iso_datetime(str(trade.get("opened_at") or ""))
                    if not opened_at:
                        continue
                    trade_key = stable_hash(f"{topic_id}|{trade_theme}|{opened_at.isoformat()}")[:24]
                    if trade_key in collected_positions:
                        continue
                    decision_row = decision_map.get(topic_id) or {}
                    closed_at = self._parse_iso_datetime(str(trade.get("closed_at") or ""))
                    expires_at = self._parse_iso_datetime(str(trade.get("expires_at") or ""))
                    if not expires_at:
                        expires_at = opened_at + default_holding_delta
                    raw_status = str(
                        trade.get("position_status")
                        or trade.get("status")
                        or ("OPEN" if not closed_at else "CLOSED")
                    ).strip().upper()
                    collected_positions[trade_key] = {
                        "trade_id": trade_key,
                        "topic_id": topic_id,
                        "label": decision_row.get("label") or topic_id or "Narrative",
                        "vertical": decision_row.get("vertical") or "unknown",
                        "trade_theme": trade_theme,
                        "trade_direction": trade.get("trade_direction") or "Long",
                        "trade_tickers": list(trade.get("trade_tickers") or []),
                        "notional_pct": float(trade.get("approved_notional_pct", 0.0) or 0.0),
                        "opened_at": opened_at,
                        "closed_at": closed_at,
                        "expires_at": expires_at,
                        "raw_status": raw_status if raw_status in {"OPEN", "CLOSED", "EXPIRED"} else "OPEN",
                        "asset_mapping": decision_row.get("asset_mapping"),
                    }
            return collected_positions

        def resolve_position_lifecycle(position_row: dict[str, Any]) -> tuple[str, datetime | None]:
            opened_at = position_row["opened_at"]
            closed_at = position_row.get("closed_at")
            expires_at = position_row.get("expires_at")
            raw_status = str(position_row.get("raw_status") or "OPEN")

            if isinstance(closed_at, datetime) and closed_at > opened_at and closed_at <= now:
                return "CLOSED", closed_at
            if isinstance(expires_at, datetime) and expires_at > opened_at and expires_at <= now:
                return "EXPIRED", expires_at
            if raw_status == "CLOSED":
                return "CLOSED", closed_at if isinstance(closed_at, datetime) else None
            if raw_status == "EXPIRED":
                return "EXPIRED", expires_at if isinstance(expires_at, datetime) else None
            return "OPEN", None

        snapshots = load_snapshots(filter_signature)
        snapshot_scope = "filtered"
        positions_raw = collect_positions(snapshots)

        if not positions_raw:
            relaxed_snapshots = load_snapshots(None)
            relaxed_positions = collect_positions(relaxed_snapshots)
            if len(relaxed_positions) > len(positions_raw):
                snapshots = relaxed_snapshots
                positions_raw = relaxed_positions
                snapshot_scope = "all_filters_fallback"

        positions: list[dict[str, Any]] = []
        cumulative_notional = 0.0
        open_notional = 0.0
        realized_pnl_pct = 0.0
        unrealized_pnl_known = 0.0
        open_positions_count = 0
        closed_positions_count = 0
        expired_positions_count = 0
        open_unpriced_positions = 0
        unrealized_priced_positions = 0
        closed_priced_positions = 0
        winning_closed_positions = 0
        priced_positions = 0

        for raw_position in positions_raw.values():
            tickers = list(raw_position["trade_tickers"])
            direction = str(raw_position["trade_direction"])
            notional = float(raw_position["notional_pct"])
            lifecycle_status, lifecycle_exit_at = resolve_position_lifecycle(raw_position)
            opened_at = raw_position["opened_at"]

            cumulative_notional += notional
            if lifecycle_status == "OPEN":
                open_positions_count += 1
                open_notional += notional
            elif lifecycle_status == "CLOSED":
                closed_positions_count += 1
            else:
                expired_positions_count += 1

            basket_result = {
                "available": False,
                "basket_return": None,
                "leg_returns": [],
            }
            trade_is_priceable = bool(tickers) and direction not in {"None", "Operational"}
            pricing_exit_at = now if lifecycle_status == "OPEN" else lifecycle_exit_at
            if trade_is_priceable and pricing_exit_at and pricing_exit_at > opened_at:
                basket_result = self.market_data.basket_return(
                    tickers=tickers,
                    entry_at=opened_at,
                    exit_at=pricing_exit_at,
                    direction=direction,
                )

            basket_return = basket_result["basket_return"] if basket_result["available"] else None
            position_pnl_pct = None
            realized_position_pnl_pct = None
            unrealized_position_pnl_pct = None
            if basket_return is not None:
                position_pnl_pct = notional * float(basket_return)
                priced_positions += 1
                if lifecycle_status == "OPEN":
                    unrealized_position_pnl_pct = position_pnl_pct
                    unrealized_pnl_known += position_pnl_pct
                    unrealized_priced_positions += 1
                else:
                    realized_position_pnl_pct = position_pnl_pct
                    realized_pnl_pct += position_pnl_pct
                    closed_priced_positions += 1
                    if position_pnl_pct > 0:
                        winning_closed_positions += 1
            elif lifecycle_status == "OPEN" and trade_is_priceable:
                open_unpriced_positions += 1

            position_end_at = lifecycle_exit_at if lifecycle_exit_at and lifecycle_exit_at > opened_at else now
            days_open = max((position_end_at - opened_at).days, 0)

            positions.append(
                {
                    "trade_id": raw_position["trade_id"],
                    "topic_id": raw_position["topic_id"],
                    "label": raw_position["label"],
                    "vertical": raw_position["vertical"],
                    "trade_theme": raw_position["trade_theme"],
                    "trade_direction": direction,
                    "trade_tickers": tickers,
                    "notional_pct": round(notional, 4),
                    "opened_at": opened_at.isoformat(),
                    "closed_at": raw_position["closed_at"].isoformat()
                    if isinstance(raw_position.get("closed_at"), datetime)
                    else None,
                    "expires_at": raw_position["expires_at"].isoformat()
                    if isinstance(raw_position.get("expires_at"), datetime)
                    else None,
                    "position_status": lifecycle_status,
                    "days_open": days_open,
                    "basket_return": round(float(basket_return), 6) if basket_return is not None else None,
                    "position_pnl_pct": round(float(position_pnl_pct), 6) if position_pnl_pct is not None else None,
                    "realized_pnl_pct": round(float(realized_position_pnl_pct), 6)
                    if realized_position_pnl_pct is not None
                    else None,
                    "unrealized_pnl_pct": round(float(unrealized_position_pnl_pct), 6)
                    if unrealized_position_pnl_pct is not None
                    else None,
                    "leg_returns": basket_result["leg_returns"],
                    "asset_mapping": raw_position.get("asset_mapping"),
                }
            )

        positions.sort(key=lambda row: str(row.get("opened_at") or ""), reverse=True)
        positions.sort(key=lambda row: 0 if row.get("position_status") == "OPEN" else 1)

        risk_limits = RISK_LIMITS_BY_PROFILE.get(profile_key, RISK_LIMITS_BY_PROFILE["investor"])
        max_daily_notional = float(risk_limits["max_daily_notional_pct"])
        max_position_notional = float(risk_limits["max_position_notional_pct"])
        unrealized_pnl_pct = (
            round(unrealized_pnl_known, 6) if open_positions_count == 0 or open_unpriced_positions == 0 else None
        )
        total_pnl_pct = realized_pnl_pct + (
            unrealized_pnl_pct if unrealized_pnl_pct is not None else unrealized_pnl_known
        )

        curve_points: list[dict[str, Any]] = []
        recent_days = 30
        for day_index in range(recent_days):
            day_end = (now - timedelta(days=(recent_days - day_index - 1))).replace(
                hour=23,
                minute=59,
                second=59,
                microsecond=0,
            )
            day_start = day_end.replace(hour=0, minute=0, second=0, microsecond=0)
            opened_today = 0.0
            cumulative_notional_by_day = 0.0
            open_notional_by_day = 0.0
            for position in positions:
                opened_at = self._parse_iso_datetime(position.get("opened_at"))
                if not opened_at:
                    continue
                position_closed_at = self._parse_iso_datetime(position.get("closed_at")) or self._parse_iso_datetime(
                    position.get("expires_at")
                )
                if day_start <= opened_at <= day_end:
                    opened_today += float(position.get("notional_pct", 0.0) or 0.0)
                if opened_at <= day_end:
                    notional_value = float(position.get("notional_pct", 0.0) or 0.0)
                    cumulative_notional_by_day += notional_value
                    if position_closed_at is None or position_closed_at > day_end:
                        open_notional_by_day += notional_value
            curve_points.append(
                {
                    "date": day_end.date().isoformat(),
                    "opened_notional_pct": round(opened_today, 4),
                    "open_notional_pct": round(open_notional_by_day, 4),
                    "cumulative_notional_pct": round(cumulative_notional_by_day, 4),
                    "utilization": round(open_notional_by_day / max(max_daily_notional, 0.01), 4),
                }
            )

        return {
            "summary": {
                "window": window,
                "profile": profile_key,
                "generated_at": now.isoformat(),
                "positions": len(positions),
                "open_positions_count": open_positions_count,
                "closed_positions_count": closed_positions_count,
                "expired_positions_count": expired_positions_count,
                "priced_positions": priced_positions,
                "win_rate": round((winning_closed_positions / closed_priced_positions), 4)
                if closed_priced_positions
                else 0.0,
                "total_notional_pct": round(cumulative_notional, 4),
                "cumulative_approved_notional_pct": round(cumulative_notional, 4),
                "open_notional_pct": round(open_notional, 4),
                "total_pnl_pct": round(total_pnl_pct, 6),
                "realized_pnl_pct": round(realized_pnl_pct, 6),
                "unrealized_pnl_pct": unrealized_pnl_pct,
                "unrealized_priced_positions": unrealized_priced_positions,
                "open_unpriced_positions": open_unpriced_positions,
                "total_pnl_is_partial": open_unpriced_positions > 0,
                "max_daily_notional_pct": round(max_daily_notional, 4),
                "max_position_notional_pct": round(max_position_notional, 4),
                "risk_utilization": round(open_notional / max(max_daily_notional, 0.01), 4),
                "history_snapshots": len(snapshots),
                "snapshot_scope": snapshot_scope,
                "lifecycle": self._build_lifecycle_summary(
                    candidates=len(positions),
                    considered=len(positions),
                    recommended=len(positions),
                    approved=len(positions),
                    opened=open_positions_count,
                    closed=closed_positions_count,
                    expired=expired_positions_count,
                ),
            },
            "positions": positions[:120],
            "risk_curve": curve_points,
        }

    @staticmethod
    def _format_alert_message(rule: dict[str, Any], topic: dict[str, Any], generated_at: datetime) -> str:
        momentum_pct = int(round(float(topic.get("momentum", 0.0) or 0.0) * 100))
        quality = float(topic.get("source_quality_score", 1.0) or 1.0)
        diversity = int(topic.get("diversity", 0) or 0)
        top_item = (topic.get("representative_items") or [{}])[0]
        top_url = str(top_item.get("url") or "")
        return (
            f"[Narrative Alert · {rule['window']}] {topic.get('label')} | "
            f"momentum {momentum_pct:+d}% | diversity {diversity} | quality {quality:.2f} | "
            f"generated {generated_at.isoformat()} | {top_url}"
        )

    def _deliver_alert(self, *, rule: dict[str, Any], payload: dict[str, Any]) -> tuple[str, str | None]:
        channel = str(rule.get("channel_type") or "webhook").strip().lower()
        endpoint_url = str(rule.get("endpoint_url") or "").strip()
        if not endpoint_url:
            return "failed", "missing endpoint URL"

        if channel == "slack":
            body: dict[str, Any] = {"text": payload["message"]}
        elif channel == "discord":
            body = {"content": payload["message"]}
        else:
            body = payload

        try:
            with httpx.Client(timeout=httpx.Timeout(8.0, connect=4.0)) as client:
                response = client.post(endpoint_url, json=body)
            if response.status_code >= 400:
                return "failed", f"HTTP {response.status_code}"
            return "sent", None
        except Exception as exc:
            return "failed", str(exc)

    def upsert_alert_rule(self, *, payload: dict[str, Any], rule_id: str | None = None) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        next_rule_id = rule_id or stable_hash(f"{payload.get('name','rule')}|{payload.get('endpoint_url','')}|{now.isoformat()}")[:16]
        existing = self.database.get_alert_rule(next_rule_id)
        created_at = existing.get("created_at") if existing else now.isoformat()
        normalized = {
            "rule_id": next_rule_id,
            "name": str(payload.get("name") or "Narrative alert").strip()[:120],
            "channel_type": str(payload.get("channel_type") or "webhook").strip().lower(),
            "endpoint_url": str(payload.get("endpoint_url") or "").strip(),
            "window": str(payload.get("window") or "24h").strip(),
            "momentum_threshold": float(payload.get("momentum_threshold", 0.2) or 0.2),
            "diversity_threshold": int(payload.get("diversity_threshold", 3) or 3),
            "min_quality_score": float(payload.get("min_quality_score", 0.8) or 0.8),
            "verticals": sorted({str(value) for value in (payload.get("verticals") or []) if str(value).strip()}),
            "sources": sorted({str(value) for value in (payload.get("sources") or []) if str(value).strip()}),
            "enabled": bool(payload.get("enabled", True)),
            "created_at": created_at,
            "last_triggered_at": existing.get("last_triggered_at") if existing else None,
        }
        if normalized["window"] not in supported_windows():
            normalized["window"] = "24h"
        if normalized["channel_type"] not in {"webhook", "discord", "slack"}:
            normalized["channel_type"] = "webhook"
        self.database.save_alert_rule(normalized)
        stored = self.database.get_alert_rule(next_rule_id)
        return stored or normalized

    def list_alert_rules(self) -> list[dict[str, Any]]:
        return self.database.get_alert_rules(enabled_only=False)

    def delete_alert_rule(self, rule_id: str) -> None:
        self.database.delete_alert_rule(rule_id)

    def list_alert_events(self, limit: int = 100) -> list[dict[str, Any]]:
        return self.database.get_alert_events(limit=limit)

    def evaluate_alert_rules(self, *, rule_id: str | None = None) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        rules = [self.database.get_alert_rule(rule_id)] if rule_id else self.database.get_alert_rules(enabled_only=True)
        evaluated_rules = [rule for rule in rules if rule and rule.get("enabled")]

        results: list[dict[str, Any]] = []
        total_sent = 0
        total_failed = 0
        total_candidates = 0

        for rule in evaluated_rules:
            rule_window = str(rule.get("window") or "24h")
            topics_payload = self.get_topics(
                window=rule_window,
                verticals=rule.get("verticals") or None,
                sources=rule.get("sources") or None,
                only_rising=False,
                search=None,
            )
            topics = topics_payload.get("topics", [])
            metadata = topics_payload.get("metadata", {})
            generated_at = self._parse_iso_datetime(metadata.get("generated_at")) or now

            candidates = [
                topic
                for topic in topics
                if float(topic.get("momentum", 0.0) or 0.0) >= float(rule.get("momentum_threshold", 0.0))
                and int(topic.get("diversity", 0) or 0) >= int(rule.get("diversity_threshold", 0))
                and float(topic.get("source_quality_score", 1.0) or 1.0) >= float(rule.get("min_quality_score", 0.0))
            ]
            candidates.sort(
                key=lambda topic: (
                    float(topic.get("momentum", 0.0) or 0.0)
                    * np.log(float(topic.get("weighted_volume_now") or topic.get("volume_now") or 1.0) + 1.0)
                ),
                reverse=True,
            )
            total_candidates += len(candidates)

            sent = 0
            failed = 0
            for topic in candidates[:12]:
                topic_id = str(topic.get("topic_id") or "")
                if not topic_id:
                    continue
                event_bucket = generated_at.replace(minute=(generated_at.minute // 30) * 30, second=0, microsecond=0)
                event_id = stable_hash(f"{rule['rule_id']}|{topic_id}|{event_bucket.isoformat()}")[:24]
                if self.database.has_alert_event(event_id):
                    continue

                alert_payload = {
                    "rule_id": rule["rule_id"],
                    "rule_name": rule["name"],
                    "window": rule_window,
                    "topic": {
                        "topic_id": topic_id,
                        "label": topic.get("label"),
                        "vertical": topic.get("vertical"),
                        "momentum": topic.get("momentum"),
                        "diversity": topic.get("diversity"),
                        "source_quality_score": topic.get("source_quality_score"),
                    },
                    "generated_at": generated_at.isoformat(),
                    "message": self._format_alert_message(rule, topic, generated_at),
                }
                status, delivery_error = self._deliver_alert(rule=rule, payload=alert_payload)
                self.database.save_alert_event(
                    {
                        "event_id": event_id,
                        "rule_id": rule["rule_id"],
                        "topic_id": topic_id,
                        "topic_label": topic.get("label") or topic_id,
                        "channel_type": rule.get("channel_type") or "webhook",
                        "delivery_status": status,
                        "delivery_error": delivery_error,
                        "payload": alert_payload,
                        "triggered_at": now.isoformat(),
                    }
                )
                if status == "sent":
                    sent += 1
                    total_sent += 1
                    self.database.set_alert_rule_last_triggered(rule["rule_id"], now)
                else:
                    failed += 1
                    total_failed += 1

            results.append(
                {
                    "rule_id": rule["rule_id"],
                    "name": rule["name"],
                    "window": rule_window,
                    "candidates": len(candidates),
                    "sent": sent,
                    "failed": failed,
                }
            )

        return {
            "generated_at": now.isoformat(),
            "evaluated_rules": len(evaluated_rules),
            "total_candidates": total_candidates,
            "total_sent": total_sent,
            "total_failed": total_failed,
            "results": results,
        }

    def schedule_refresh(self, window: str = DEFAULT_WINDOW, force: bool = False) -> None:
        existing = self._refresh_tasks.get(window)
        if existing and not existing.done():
            return

        async def task_runner() -> None:
            try:
                await self.ingest_sources(force=force)
                await self.generate_topics(window=window)
            except Exception as exc:
                logger.exception(
                    "scheduled_refresh_failed",
                    extra={"extra": {"window": window, "error": str(exc)}},
                )

        try:
            running_loop = asyncio.get_running_loop()
        except RuntimeError:
            logger.warning(
                "scheduled_refresh_skipped_no_running_loop",
                extra={"extra": {"window": window, "force": force}},
            )
            return

        self._refresh_tasks[window] = running_loop.create_task(task_runner())

    def get_topics(
        self,
        *,
        window: str,
        verticals: list[str] | None,
        sources: list[str] | None,
        only_rising: bool,
        search: str | None,
    ) -> dict[str, Any]:
        payload = self.database.get_topic_cache(window=window)
        if not payload:
            payload = self._load_seed_payload(window=window) or {
                "topics": [],
                "metadata": {
                    "generated_at": datetime.now(timezone.utc).isoformat(),
                    "window": window,
                    "item_count": 0,
                    "source_health": [],
                    "algorithm": "seed-none",
                },
            }

        generated_at = payload.get("metadata", {}).get("generated_at")
        if self._stale(generated_at):
            self.schedule_refresh(window=window)

        return self._filter_topics(
            payload=payload,
            verticals=verticals,
            sources=sources,
            only_rising=only_rising,
            search=search,
        )
