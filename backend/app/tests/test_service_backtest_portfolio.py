from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.core.database import Database
from app.pipeline.service import DECISION_ENGINE_VERSION, NarrativeService


def _decision(
    topic_id: str,
    label: str,
    *,
    momentum: float,
    action_bucket: str = "Act now",
    trade_direction: str = "Operational",
    trade_tickers: list[str] | None = None,
    execution_status: str = "approved",
) -> dict:
    return {
        "topic_id": topic_id,
        "label": label,
        "vertical": "tech",
        "momentum": momentum,
        "action_bucket": action_bucket,
        "trade_direction": trade_direction,
        "trade_tickers": trade_tickers or [],
        "execution_status": execution_status,
        "trade_theme": "ai_compute",
    }


def _snapshot(
    generated_at: datetime,
    *,
    decisions: list[dict],
    executed_trades: list[dict] | None = None,
) -> dict:
    return {
        "generated_at": generated_at.isoformat(),
        "payload": {
            "decision_engine_version": DECISION_ENGINE_VERSION,
            "decisions": decisions,
            "executed_trades": executed_trades or [],
        },
    }


def _service(tmp_path: Path) -> NarrativeService:
    database = Database(tmp_path / "test-service.sqlite")
    return NarrativeService(database=database)


def test_backtest_warmup_returns_explicit_fallback_metadata(tmp_path, monkeypatch):
    service = _service(tmp_path)
    now = datetime.now(timezone.utc)
    event_time = now - timedelta(hours=4)
    followup_time = now - timedelta(hours=2)

    snapshots = [
        _snapshot(
            event_time,
            decisions=[_decision("topic_a", "Topic A", momentum=1.0)],
        ),
        _snapshot(
            followup_time,
            decisions=[_decision("topic_a", "Topic A", momentum=0.4, action_bucket="Monitor")],
        ),
    ]
    monkeypatch.setattr(service.database, "get_decision_snapshots_range", lambda **_: snapshots)

    payload = service.get_backtest(
        window="24h",
        profile="investor",
        verticals=None,
        sources=None,
        only_rising=False,
        search=None,
    )
    horizon_by_days = {row["days"]: row for row in payload["horizons"]}
    seven_day = horizon_by_days[7]

    assert seven_day["evaluation_mode"] == "warmup"
    assert seven_day["is_fallback"] is True
    assert seven_day["fallback_reason"] == "insufficient_mature_followups_for_expected_horizon"
    assert seven_day["effective_horizon"] == "next_snapshot"
    assert seven_day["expected_horizon"] == "7d"
    assert seven_day["samples"]
    assert seven_day["samples"][0]["is_fallback"] is True
    assert seven_day["samples"][0]["effective_horizon"] == "next_snapshot"
    assert seven_day["samples"][0]["expected_horizon"] == "7d"
    assert payload["summary"]["lifecycle"]["candidates"] == 1
    assert payload["summary"]["lifecycle"]["recommended"] == 1


def test_backtest_full_horizon_keeps_non_fallback_metadata(tmp_path, monkeypatch):
    service = _service(tmp_path)
    now = datetime.now(timezone.utc)
    event_time = now - timedelta(days=10)
    followup_time = event_time + timedelta(days=8)

    snapshots = [
        _snapshot(
            event_time,
            decisions=[_decision("topic_b", "Topic B", momentum=0.9)],
        ),
        _snapshot(
            followup_time,
            decisions=[_decision("topic_b", "Topic B", momentum=0.15, action_bucket="Monitor")],
        ),
    ]
    monkeypatch.setattr(service.database, "get_decision_snapshots_range", lambda **_: snapshots)

    payload = service.get_backtest(
        window="24h",
        profile="investor",
        verticals=None,
        sources=None,
        only_rising=False,
        search=None,
    )
    horizon_by_days = {row["days"]: row for row in payload["horizons"]}
    seven_day = horizon_by_days[7]

    assert seven_day["evaluation_mode"] == "full"
    assert seven_day["is_fallback"] is False
    assert seven_day["fallback_reason"] is None
    assert seven_day["effective_horizon"] == "7d"
    assert seven_day["expected_horizon"] == "7d"
    assert seven_day["samples"]
    assert seven_day["samples"][0]["is_fallback"] is False
    assert seven_day["samples"][0]["effective_horizon"] == "7d"
    assert payload["summary"]["lifecycle"]["approved"] == 1


def test_portfolio_uses_open_notional_and_splits_realized_vs_unrealized(tmp_path, monkeypatch):
    service = _service(tmp_path)
    now = datetime.now(timezone.utc)
    open_time = now - timedelta(hours=3)
    closed_time = now - timedelta(days=3)
    expired_time = now - timedelta(days=2)

    decisions = [
        _decision("open_t", "Open Position", momentum=0.3, trade_direction="Long", trade_tickers=["AAA"]),
        _decision("closed_t", "Closed Position", momentum=0.2, trade_direction="Long", trade_tickers=["BBB"]),
        _decision("expired_t", "Expired Position", momentum=0.1, trade_direction="Long", trade_tickers=["CCC"]),
    ]
    executed = [
        {
            "topic_id": "open_t",
            "trade_theme": "ai_compute",
            "trade_direction": "Long",
            "trade_tickers": ["AAA"],
            "approved_notional_pct": 2.0,
            "opened_at": open_time.isoformat(),
            "expires_at": (now + timedelta(hours=6)).isoformat(),
            "position_status": "OPEN",
            "execution_status": "approved",
        },
        {
            "topic_id": "closed_t",
            "trade_theme": "ai_compute",
            "trade_direction": "Long",
            "trade_tickers": ["BBB"],
            "approved_notional_pct": 1.0,
            "opened_at": closed_time.isoformat(),
            "closed_at": (now - timedelta(days=1)).isoformat(),
            "position_status": "CLOSED",
            "execution_status": "approved",
        },
        {
            "topic_id": "expired_t",
            "trade_theme": "ai_compute",
            "trade_direction": "Long",
            "trade_tickers": ["CCC"],
            "approved_notional_pct": 1.5,
            "opened_at": expired_time.isoformat(),
            "expires_at": (now - timedelta(hours=4)).isoformat(),
            "execution_status": "approved",
        },
    ]
    snapshots = [_snapshot(now - timedelta(minutes=5), decisions=decisions, executed_trades=executed)]
    monkeypatch.setattr(service.database, "get_decision_snapshots_range", lambda **_: snapshots)
    monkeypatch.setattr(
        service.market_data,
        "basket_return",
        lambda **_: {
            "available": True,
            "basket_return": 0.1,
            "leg_returns": [],
        },
    )

    payload = service.get_paper_portfolio(
        window="24h",
        profile="investor",
        verticals=None,
        sources=None,
        only_rising=False,
        search=None,
    )
    summary = payload["summary"]

    assert summary["positions"] == 3
    assert summary["open_positions_count"] == 1
    assert summary["closed_positions_count"] == 1
    assert summary["expired_positions_count"] == 1
    assert summary["open_notional_pct"] == 2.0
    assert summary["total_notional_pct"] == 4.5
    assert summary["risk_utilization"] == 0.3333
    assert summary["realized_pnl_pct"] == 0.25
    assert summary["unrealized_pnl_pct"] == 0.2

    positions_by_topic = {row["topic_id"]: row for row in payload["positions"]}
    assert positions_by_topic["open_t"]["position_status"] == "OPEN"
    assert positions_by_topic["open_t"]["unrealized_pnl_pct"] == 0.2
    assert positions_by_topic["closed_t"]["position_status"] == "CLOSED"
    assert positions_by_topic["closed_t"]["realized_pnl_pct"] == 0.1
    assert positions_by_topic["expired_t"]["position_status"] == "EXPIRED"
    assert positions_by_topic["expired_t"]["realized_pnl_pct"] == 0.15


def test_portfolio_legacy_records_default_to_open_and_unrealized_na(tmp_path, monkeypatch):
    service = _service(tmp_path)
    now = datetime.now(timezone.utc)
    opened_at = now - timedelta(hours=2)
    decisions = [
        _decision("legacy_t", "Legacy Position", momentum=0.2, trade_direction="Long", trade_tickers=["AAA"]),
    ]
    executed = [
        {
            "topic_id": "legacy_t",
            "trade_theme": "ai_compute",
            "trade_direction": "Long",
            "trade_tickers": ["AAA"],
            "approved_notional_pct": 1.25,
            "opened_at": opened_at.isoformat(),
            "execution_status": "approved",
        }
    ]
    snapshots = [_snapshot(now - timedelta(minutes=2), decisions=decisions, executed_trades=executed)]
    monkeypatch.setattr(service.database, "get_decision_snapshots_range", lambda **_: snapshots)
    monkeypatch.setattr(
        service.market_data,
        "basket_return",
        lambda **_: {
            "available": False,
            "basket_return": None,
            "leg_returns": [],
        },
    )

    payload = service.get_paper_portfolio(
        window="24h",
        profile="investor",
        verticals=None,
        sources=None,
        only_rising=False,
        search=None,
    )
    summary = payload["summary"]
    position = payload["positions"][0]

    assert summary["open_positions_count"] == 1
    assert summary["open_unpriced_positions"] == 1
    assert summary["unrealized_pnl_pct"] is None
    assert summary["total_pnl_is_partial"] is True
    assert position["position_status"] == "OPEN"
    assert position["unrealized_pnl_pct"] is None


def test_portfolio_relaxes_to_all_filter_history_when_filtered_scope_is_empty(tmp_path, monkeypatch):
    service = _service(tmp_path)
    now = datetime.now(timezone.utc)
    decisions = [_decision("fallback_t", "Fallback Position", momentum=0.2, trade_direction="Long", trade_tickers=["AAA"])]
    executed = [
        {
            "topic_id": "fallback_t",
            "trade_theme": "ai_compute",
            "trade_direction": "Long",
            "trade_tickers": ["AAA"],
            "approved_notional_pct": 1.0,
            "opened_at": (now - timedelta(hours=1)).isoformat(),
            "execution_status": "approved",
        }
    ]
    snapshots = [_snapshot(now - timedelta(minutes=2), decisions=decisions, executed_trades=executed)]

    def fake_snapshot_loader(**kwargs):
        if kwargs.get("filter_signature") is not None:
            return []
        return snapshots

    monkeypatch.setattr(service.database, "get_decision_snapshots_range", fake_snapshot_loader)
    monkeypatch.setattr(
        service.market_data,
        "basket_return",
        lambda **_: {
            "available": True,
            "basket_return": 0.05,
            "leg_returns": [],
        },
    )

    payload = service.get_paper_portfolio(
        window="24h",
        profile="investor",
        verticals=["tech"],
        sources=["wired"],
        only_rising=False,
        search=None,
    )

    assert payload["summary"]["snapshot_scope"] == "all_filters_fallback"
    assert payload["summary"]["positions"] == 1
    assert payload["summary"]["lifecycle"]["approved"] == 1
    assert payload["summary"]["lifecycle"]["opened"] == 1


def test_get_topics_enriches_trust_contract_and_suppresses_low_alignment_labels(tmp_path, monkeypatch):
    service = _service(tmp_path)
    now = datetime.now(timezone.utc)
    payload = {
        "topics": [
            {
                "topic_id": "topic_unresolved",
                "label": "Entertainment: Samsung Galaxy",
                "vertical": "entertainment",
                "volume_now": 9,
                "volume_prev": 2,
                "momentum": 0.61,
                "novelty": 0.88,
                "diversity": 5,
                "sparkline": [1, 1, 2, 2],
                "summary": "demo",
                "keywords": ["returnal", "ps5", "discount"],
                "entities": ["IGN"],
                "representative_items": [
                    {
                        "title": "Returnal for PS5 is over 50% off right now",
                        "url": "https://example.com/returnal",
                        "source_name": "ign",
                        "published_at": now.isoformat(),
                    }
                ],
                "related_topic_ids": [],
                "source_quality_score": 1.0,
                "label_confidence": 0.1,
            }
        ],
        "metadata": {
            "generated_at": now.isoformat(),
            "window": "24h",
            "item_count": 9,
            "source_health": [],
            "algorithm": "test",
        },
    }
    monkeypatch.setattr(service.database, "get_topic_cache", lambda **_: payload)

    response = service.get_topics(
        window="24h",
        verticals=None,
        sources=None,
        only_rising=False,
        search=None,
    )

    assert response["topics"]
    topic = response["topics"][0]
    assert topic["label"] == "Unresolved entertainment narrative"
    assert topic["trust_contract"]["eligible_for_act_now"] is False
    assert topic["trust_contract"]["warnings"]
