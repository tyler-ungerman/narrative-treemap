import json
import sqlite3
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.core.hash_utils import normalize_title_hash
from app.schemas.models import Item


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00") if value.endswith("Z") else value
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class Database:
    def __init__(self, database_path: Path):
        self.database_path = database_path
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._connection = sqlite3.connect(database_path, check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._lock = threading.Lock()
        self.initialize_schema()

    def initialize_schema(self) -> None:
        schema = """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS items (
            item_id TEXT PRIMARY KEY,
            source_name TEXT NOT NULL,
            vertical TEXT NOT NULL,
            title TEXT NOT NULL,
            url TEXT NOT NULL,
            published_at TEXT NOT NULL,
            published_date TEXT NOT NULL,
            fetched_at TEXT NOT NULL,
            summary TEXT,
            raw_text TEXT,
            normalized_title_hash TEXT NOT NULL
        );
        CREATE UNIQUE INDEX IF NOT EXISTS idx_items_url_unique ON items(url);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_items_title_day_unique ON items(normalized_title_hash, published_date);
        CREATE INDEX IF NOT EXISTS idx_items_published_at ON items(published_at);
        CREATE INDEX IF NOT EXISTS idx_items_vertical ON items(vertical);
        CREATE INDEX IF NOT EXISTS idx_items_source ON items(source_name);

        CREATE TABLE IF NOT EXISTS embeddings (
            item_id TEXT NOT NULL,
            model_name TEXT NOT NULL,
            vector_json TEXT NOT NULL,
            created_at TEXT NOT NULL,
            PRIMARY KEY (item_id, model_name)
        );

        CREATE TABLE IF NOT EXISTS topic_runs (
            run_id TEXT PRIMARY KEY,
            window TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            item_count INTEGER NOT NULL,
            algorithm TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS topic_assignments (
            run_id TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            item_id TEXT NOT NULL,
            score REAL,
            PRIMARY KEY (run_id, topic_id, item_id)
        );

        CREATE TABLE IF NOT EXISTS topic_metrics (
            run_id TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            window TEXT NOT NULL,
            label TEXT NOT NULL,
            vertical TEXT NOT NULL,
            volume_now INTEGER NOT NULL,
            volume_prev INTEGER NOT NULL,
            momentum REAL NOT NULL,
            novelty REAL NOT NULL,
            source_diversity INTEGER NOT NULL,
            sparkline_json TEXT NOT NULL,
            keywords_json TEXT NOT NULL,
            entities_json TEXT NOT NULL,
            representative_items_json TEXT NOT NULL,
            related_topic_ids_json TEXT NOT NULL,
            summary TEXT NOT NULL,
            PRIMARY KEY (run_id, topic_id)
        );

        CREATE TABLE IF NOT EXISTS topic_cache (
            window TEXT PRIMARY KEY,
            generated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS decision_snapshots (
            snapshot_id TEXT PRIMARY KEY,
            window TEXT NOT NULL,
            filter_signature TEXT NOT NULL,
            profile TEXT NOT NULL,
            generated_at TEXT NOT NULL,
            payload_json TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_decision_snapshots_lookup
            ON decision_snapshots(window, filter_signature, profile, generated_at DESC);

        CREATE TABLE IF NOT EXISTS asset_price_cache (
            ticker TEXT NOT NULL,
            price_date TEXT NOT NULL,
            close REAL NOT NULL,
            fetched_at TEXT NOT NULL,
            PRIMARY KEY (ticker, price_date)
        );
        CREATE INDEX IF NOT EXISTS idx_asset_price_lookup
            ON asset_price_cache(ticker, price_date);

        CREATE TABLE IF NOT EXISTS alert_rules (
            rule_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            channel_type TEXT NOT NULL,
            endpoint_url TEXT NOT NULL,
            window TEXT NOT NULL,
            momentum_threshold REAL NOT NULL,
            diversity_threshold INTEGER NOT NULL,
            min_quality_score REAL NOT NULL,
            verticals_json TEXT NOT NULL,
            sources_json TEXT NOT NULL,
            enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            last_triggered_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_alert_rules_enabled
            ON alert_rules(enabled, updated_at DESC);

        CREATE TABLE IF NOT EXISTS alert_events (
            event_id TEXT PRIMARY KEY,
            rule_id TEXT NOT NULL,
            topic_id TEXT NOT NULL,
            topic_label TEXT NOT NULL,
            channel_type TEXT NOT NULL,
            delivery_status TEXT NOT NULL,
            delivery_error TEXT,
            payload_json TEXT NOT NULL,
            triggered_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_alert_events_rule_time
            ON alert_events(rule_id, triggered_at DESC);

        CREATE TABLE IF NOT EXISTS source_health (
            source_name TEXT PRIMARY KEY,
            last_success TEXT,
            last_error TEXT,
            items_fetched INTEGER NOT NULL DEFAULT 0,
            latency_ms INTEGER NOT NULL DEFAULT 0,
            updated_at TEXT NOT NULL
        );

        UPDATE items
        SET vertical = 'local'
        WHERE source_name LIKE 'gnews_geo_%'
          AND vertical != 'local';
        """
        with self._lock:
            self._connection.executescript(schema)
            self._connection.commit()

    def close(self) -> None:
        with self._lock:
            self._connection.close()

    def upsert_items(self, items: list[Item]) -> int:
        if not items:
            return 0
        payload = [
            (
                item.item_id,
                item.source_name,
                item.vertical,
                item.title,
                item.url,
                item.published_at.isoformat(),
                item.published_at.date().isoformat(),
                item.fetched_at.isoformat(),
                item.summary,
                item.raw_text,
                normalize_title_hash(item.title),
            )
            for item in items
        ]
        with self._lock:
            before = self._connection.total_changes
            self._connection.executemany(
                """
                INSERT OR IGNORE INTO items (
                    item_id,
                    source_name,
                    vertical,
                    title,
                    url,
                    published_at,
                    published_date,
                    fetched_at,
                    summary,
                    raw_text,
                    normalized_title_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                payload,
            )
            self._connection.commit()
            return self._connection.total_changes - before

    def get_items_between(
        self,
        start_at: datetime,
        end_at: datetime,
        verticals: list[str] | None = None,
        sources: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT item_id, source_name, vertical, title, url, published_at, fetched_at, summary, raw_text
            FROM items
            WHERE published_at >= ? AND published_at < ?
        """
        params: list[Any] = [start_at.isoformat(), end_at.isoformat()]

        if verticals:
            query += f" AND vertical IN ({','.join(['?'] * len(verticals))})"
            params.extend(verticals)
        if sources:
            query += f" AND source_name IN ({','.join(['?'] * len(sources))})"
            params.extend(sources)
        query += " ORDER BY published_at DESC"

        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        records: list[dict[str, Any]] = []
        for row in rows:
            records.append(
                {
                    "item_id": row["item_id"],
                    "source_name": row["source_name"],
                    "vertical": row["vertical"],
                    "title": row["title"],
                    "url": row["url"],
                    "published_at": parse_datetime(row["published_at"]),
                    "fetched_at": parse_datetime(row["fetched_at"]),
                    "summary": row["summary"],
                    "raw_text": row["raw_text"],
                }
            )
        return records

    def get_embeddings(self, item_ids: list[str], model_name: str) -> dict[str, list[float]]:
        if not item_ids:
            return {}
        placeholders = ",".join(["?"] * len(item_ids))
        query = f"""
            SELECT item_id, vector_json
            FROM embeddings
            WHERE model_name = ? AND item_id IN ({placeholders})
        """
        params = [model_name, *item_ids]
        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return {row["item_id"]: json.loads(row["vector_json"]) for row in rows}

    def save_embeddings(self, model_name: str, vectors: dict[str, list[float]]) -> None:
        if not vectors:
            return
        now_iso = utc_now().isoformat()
        payload = [
            (
                item_id,
                model_name,
                json.dumps(vector),
                now_iso,
            )
            for item_id, vector in vectors.items()
        ]
        with self._lock:
            self._connection.executemany(
                """
                INSERT OR REPLACE INTO embeddings (item_id, model_name, vector_json, created_at)
                VALUES (?, ?, ?, ?)
                """,
                payload,
            )
            self._connection.commit()

    def save_topic_cache(self, window: str, generated_at: datetime, payload: dict[str, Any]) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO topic_cache (window, generated_at, payload_json)
                VALUES (?, ?, ?)
                """,
                (window, generated_at.isoformat(), json.dumps(payload, default=str)),
            )
            self._connection.commit()

    def get_topic_cache(self, window: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT generated_at, payload_json FROM topic_cache WHERE window = ?",
                (window,),
            ).fetchone()
        if not row:
            return None
        payload = json.loads(row["payload_json"])
        payload.setdefault("metadata", {})
        payload["metadata"]["generated_at"] = row["generated_at"]
        return payload

    def save_decision_snapshot(
        self,
        *,
        snapshot_id: str,
        window: str,
        filter_signature: str,
        profile: str,
        generated_at: datetime,
        payload: dict[str, Any],
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO decision_snapshots (
                    snapshot_id,
                    window,
                    filter_signature,
                    profile,
                    generated_at,
                    payload_json
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    snapshot_id,
                    window,
                    filter_signature,
                    profile,
                    generated_at.isoformat(),
                    json.dumps(payload, default=str),
                ),
            )
            self._connection.commit()

    def get_latest_decision_snapshot(
        self,
        *,
        window: str,
        filter_signature: str,
        profile: str,
        before_generated_at: datetime | None = None,
    ) -> dict[str, Any] | None:
        query = """
            SELECT generated_at, payload_json
            FROM decision_snapshots
            WHERE window = ?
              AND filter_signature = ?
              AND profile = ?
        """
        params: list[Any] = [window, filter_signature, profile]
        if before_generated_at:
            query += " AND generated_at < ?"
            params.append(before_generated_at.isoformat())
        query += " ORDER BY generated_at DESC LIMIT 1"

        with self._lock:
            row = self._connection.execute(query, params).fetchone()
        if not row:
            return None
        return {
            "generated_at": row["generated_at"],
            "payload": json.loads(row["payload_json"]),
        }

    def get_decision_snapshots_since(
        self,
        *,
        window: str,
        filter_signature: str,
        profile: str,
        since_generated_at: datetime,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT generated_at, payload_json
                FROM decision_snapshots
                WHERE window = ?
                  AND filter_signature = ?
                  AND profile = ?
                  AND generated_at >= ?
                ORDER BY generated_at ASC
                """,
                (window, filter_signature, profile, since_generated_at.isoformat()),
            ).fetchall()
        return [
            {
                "generated_at": row["generated_at"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def get_decision_snapshots_range(
        self,
        *,
        window: str,
        filter_signature: str | None,
        profile: str,
        since_generated_at: datetime,
        until_generated_at: datetime | None = None,
    ) -> list[dict[str, Any]]:
        query = """
            SELECT generated_at, payload_json
            FROM decision_snapshots
            WHERE window = ?
              AND profile = ?
              AND generated_at >= ?
        """
        params: list[Any] = [window, profile, since_generated_at.isoformat()]
        if filter_signature is not None:
            query += " AND filter_signature = ?"
            params.append(filter_signature)
        if until_generated_at:
            query += " AND generated_at <= ?"
            params.append(until_generated_at.isoformat())
        query += " ORDER BY generated_at ASC"

        with self._lock:
            rows = self._connection.execute(query, params).fetchall()
        return [
            {
                "generated_at": row["generated_at"],
                "payload": json.loads(row["payload_json"]),
            }
            for row in rows
        ]

    def save_asset_prices(self, ticker: str, prices: list[dict[str, Any]]) -> None:
        if not prices:
            return
        now_iso = utc_now().isoformat()
        payload = [
            (
                ticker.upper(),
                str(row["price_date"]),
                float(row["close"]),
                now_iso,
            )
            for row in prices
            if row.get("price_date") and row.get("close") is not None
        ]
        if not payload:
            return
        with self._lock:
            self._connection.executemany(
                """
                INSERT OR REPLACE INTO asset_price_cache (ticker, price_date, close, fetched_at)
                VALUES (?, ?, ?, ?)
                """,
                payload,
            )
            self._connection.commit()

    def get_asset_prices(
        self,
        *,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
    ) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT ticker, price_date, close, fetched_at
                FROM asset_price_cache
                WHERE ticker = ?
                  AND price_date >= ?
                  AND price_date <= ?
                ORDER BY price_date ASC
                """,
                (
                    ticker.upper(),
                    start_date.date().isoformat(),
                    end_date.date().isoformat(),
                ),
            ).fetchall()
        return [
            {
                "ticker": row["ticker"],
                "price_date": row["price_date"],
                "close": float(row["close"]),
                "fetched_at": parse_datetime(row["fetched_at"]),
            }
            for row in rows
        ]

    def get_latest_asset_price_before(
        self,
        *,
        ticker: str,
        on_or_before: datetime,
    ) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT ticker, price_date, close, fetched_at
                FROM asset_price_cache
                WHERE ticker = ?
                  AND price_date <= ?
                ORDER BY price_date DESC
                LIMIT 1
                """,
                (ticker.upper(), on_or_before.date().isoformat()),
            ).fetchone()
        if not row:
            return None
        return {
            "ticker": row["ticker"],
            "price_date": row["price_date"],
            "close": float(row["close"]),
            "fetched_at": parse_datetime(row["fetched_at"]),
        }

    def save_alert_rule(self, rule: dict[str, Any]) -> None:
        now_iso = utc_now().isoformat()
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO alert_rules (
                    rule_id,
                    name,
                    channel_type,
                    endpoint_url,
                    window,
                    momentum_threshold,
                    diversity_threshold,
                    min_quality_score,
                    verticals_json,
                    sources_json,
                    enabled,
                    created_at,
                    updated_at,
                    last_triggered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    rule["rule_id"],
                    rule["name"],
                    rule["channel_type"],
                    rule["endpoint_url"],
                    rule["window"],
                    float(rule["momentum_threshold"]),
                    int(rule["diversity_threshold"]),
                    float(rule["min_quality_score"]),
                    json.dumps(rule.get("verticals") or []),
                    json.dumps(rule.get("sources") or []),
                    1 if bool(rule.get("enabled", True)) else 0,
                    rule.get("created_at") or now_iso,
                    now_iso,
                    rule.get("last_triggered_at"),
                ),
            )
            self._connection.commit()

    def get_alert_rules(self, enabled_only: bool = False) -> list[dict[str, Any]]:
        query = """
            SELECT
                rule_id,
                name,
                channel_type,
                endpoint_url,
                window,
                momentum_threshold,
                diversity_threshold,
                min_quality_score,
                verticals_json,
                sources_json,
                enabled,
                created_at,
                updated_at,
                last_triggered_at
            FROM alert_rules
        """
        if enabled_only:
            query += " WHERE enabled = 1"
        query += " ORDER BY updated_at DESC, created_at DESC"

        with self._lock:
            rows = self._connection.execute(query).fetchall()

        return [
            {
                "rule_id": row["rule_id"],
                "name": row["name"],
                "channel_type": row["channel_type"],
                "endpoint_url": row["endpoint_url"],
                "window": row["window"],
                "momentum_threshold": float(row["momentum_threshold"]),
                "diversity_threshold": int(row["diversity_threshold"]),
                "min_quality_score": float(row["min_quality_score"]),
                "verticals": json.loads(row["verticals_json"] or "[]"),
                "sources": json.loads(row["sources_json"] or "[]"),
                "enabled": bool(row["enabled"]),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
                "last_triggered_at": row["last_triggered_at"],
            }
            for row in rows
        ]

    def get_alert_rule(self, rule_id: str) -> dict[str, Any] | None:
        with self._lock:
            row = self._connection.execute(
                """
                SELECT
                    rule_id,
                    name,
                    channel_type,
                    endpoint_url,
                    window,
                    momentum_threshold,
                    diversity_threshold,
                    min_quality_score,
                    verticals_json,
                    sources_json,
                    enabled,
                    created_at,
                    updated_at,
                    last_triggered_at
                FROM alert_rules
                WHERE rule_id = ?
                """,
                (rule_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "rule_id": row["rule_id"],
            "name": row["name"],
            "channel_type": row["channel_type"],
            "endpoint_url": row["endpoint_url"],
            "window": row["window"],
            "momentum_threshold": float(row["momentum_threshold"]),
            "diversity_threshold": int(row["diversity_threshold"]),
            "min_quality_score": float(row["min_quality_score"]),
            "verticals": json.loads(row["verticals_json"] or "[]"),
            "sources": json.loads(row["sources_json"] or "[]"),
            "enabled": bool(row["enabled"]),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "last_triggered_at": row["last_triggered_at"],
        }

    def delete_alert_rule(self, rule_id: str) -> None:
        with self._lock:
            self._connection.execute("DELETE FROM alert_rules WHERE rule_id = ?", (rule_id,))
            self._connection.commit()

    def set_alert_rule_last_triggered(self, rule_id: str, triggered_at: datetime) -> None:
        with self._lock:
            self._connection.execute(
                """
                UPDATE alert_rules
                SET last_triggered_at = ?, updated_at = ?
                WHERE rule_id = ?
                """,
                (triggered_at.isoformat(), utc_now().isoformat(), rule_id),
            )
            self._connection.commit()

    def save_alert_event(self, event: dict[str, Any]) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO alert_events (
                    event_id,
                    rule_id,
                    topic_id,
                    topic_label,
                    channel_type,
                    delivery_status,
                    delivery_error,
                    payload_json,
                    triggered_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event["event_id"],
                    event["rule_id"],
                    event["topic_id"],
                    event["topic_label"],
                    event["channel_type"],
                    event["delivery_status"],
                    event.get("delivery_error"),
                    json.dumps(event.get("payload") or {}, default=str),
                    event["triggered_at"],
                ),
            )
            self._connection.commit()

    def has_alert_event(self, event_id: str) -> bool:
        with self._lock:
            row = self._connection.execute(
                "SELECT 1 AS present FROM alert_events WHERE event_id = ? LIMIT 1",
                (event_id,),
            ).fetchone()
        return row is not None

    def get_alert_events(self, limit: int = 100) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT event_id, rule_id, topic_id, topic_label, channel_type, delivery_status, delivery_error, payload_json, triggered_at
                FROM alert_events
                ORDER BY triggered_at DESC
                LIMIT ?
                """,
                (max(1, min(limit, 500)),),
            ).fetchall()
        return [
            {
                "event_id": row["event_id"],
                "rule_id": row["rule_id"],
                "topic_id": row["topic_id"],
                "topic_label": row["topic_label"],
                "channel_type": row["channel_type"],
                "delivery_status": row["delivery_status"],
                "delivery_error": row["delivery_error"],
                "payload": json.loads(row["payload_json"] or "{}"),
                "triggered_at": row["triggered_at"],
            }
            for row in rows
        ]

    def save_topic_run(
        self,
        run_id: str,
        window: str,
        generated_at: datetime,
        item_count: int,
        algorithm: str,
        topic_rows: list[dict[str, Any]],
        assignments: list[dict[str, Any]],
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO topic_runs (run_id, window, generated_at, item_count, algorithm)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, window, generated_at.isoformat(), item_count, algorithm),
            )
            self._connection.executemany(
                """
                INSERT OR REPLACE INTO topic_assignments (run_id, topic_id, item_id, score)
                VALUES (?, ?, ?, ?)
                """,
                [
                    (run_id, row["topic_id"], row["item_id"], row.get("score"))
                    for row in assignments
                ],
            )
            self._connection.executemany(
                """
                INSERT OR REPLACE INTO topic_metrics (
                    run_id,
                    topic_id,
                    window,
                    label,
                    vertical,
                    volume_now,
                    volume_prev,
                    momentum,
                    novelty,
                    source_diversity,
                    sparkline_json,
                    keywords_json,
                    entities_json,
                    representative_items_json,
                    related_topic_ids_json,
                    summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        run_id,
                        topic["topic_id"],
                        window,
                        topic["label"],
                        topic["vertical"],
                        topic["volume_now"],
                        topic["volume_prev"],
                        topic["momentum"],
                        topic["novelty"],
                        topic["diversity"],
                        json.dumps(topic["sparkline"]),
                        json.dumps(topic["keywords"]),
                        json.dumps(topic["entities"]),
                        json.dumps(topic["representative_items"]),
                        json.dumps(topic["related_topic_ids"]),
                        topic["summary"],
                    )
                    for topic in topic_rows
                ],
            )
            self._connection.commit()

    def upsert_source_health(
        self,
        source_name: str,
        last_success: datetime | None,
        last_error: str | None,
        items_fetched: int,
        latency_ms: int,
    ) -> None:
        with self._lock:
            self._connection.execute(
                """
                INSERT OR REPLACE INTO source_health (
                    source_name,
                    last_success,
                    last_error,
                    items_fetched,
                    latency_ms,
                    updated_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    source_name,
                    last_success.isoformat() if last_success else None,
                    last_error,
                    items_fetched,
                    latency_ms,
                    utc_now().isoformat(),
                ),
            )
            self._connection.commit()

    def get_source_health(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT source_name, last_success, last_error, items_fetched, latency_ms, updated_at
                FROM source_health
                ORDER BY source_name
                """
            ).fetchall()
        output: list[dict[str, Any]] = []
        for row in rows:
            output.append(
                {
                    "source_name": row["source_name"],
                    "last_success": parse_datetime(row["last_success"]),
                    "last_error": row["last_error"],
                    "items_fetched": int(row["items_fetched"] or 0),
                    "latency_ms": int(row["latency_ms"] or 0),
                    "updated_at": parse_datetime(row["updated_at"]),
                }
            )
        return output

    def get_source_last_success(self, source_name: str) -> datetime | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT last_success FROM source_health WHERE source_name = ?",
                (source_name,),
            ).fetchone()
        if not row:
            return None
        return parse_datetime(row["last_success"])

    def seed_topic_cache_if_empty(self, seed_payloads: dict[str, dict[str, Any]]) -> None:
        with self._lock:
            row = self._connection.execute("SELECT COUNT(*) AS count FROM topic_cache").fetchone()
            existing_count = int(row["count"])
            if existing_count > 0:
                return
            now_iso = utc_now().isoformat()
            for window, payload in seed_payloads.items():
                self._connection.execute(
                    """
                    INSERT OR REPLACE INTO topic_cache (window, generated_at, payload_json)
                    VALUES (?, ?, ?)
                    """,
                    (window, now_iso, json.dumps(payload, default=str)),
                )
            self._connection.commit()
