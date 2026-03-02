from __future__ import annotations

from datetime import datetime, timedelta, timezone

from app.pipeline.decision_briefing import apply_risk_controls, build_changes, build_decisions


def _topic(
    topic_id: str,
    label: str,
    *,
    momentum: float,
    novelty: float,
    weighted_now: float,
    weighted_prev: float,
    diversity: int,
    vertical: str = "tech",
    keywords: list[str] | None = None,
    entities: list[str] | None = None,
    representative_title: str | None = None,
    source_name: str = "wired",
) -> dict:
    representative_items = []
    if representative_title:
        representative_items = [
            {
                "title": representative_title,
                "url": "https://example.com/story",
                "source_name": source_name,
                "published_at": "2026-02-25T00:00:00+00:00",
            }
        ]
    return {
        "topic_id": topic_id,
        "label": label,
        "vertical": vertical,
        "momentum": momentum,
        "novelty": novelty,
        "weighted_volume_now": weighted_now,
        "weighted_volume_prev": weighted_prev,
        "diversity": diversity,
        "label_confidence": 0.7,
        "source_quality_score": 1.1,
        "keywords": keywords or ["ai", "chips"],
        "entities": entities or ["NVIDIA"],
        "representative_items": representative_items,
        "related_topic_ids": [],
    }


def test_decision_score_orders_higher_signal_first():
    topics = [
        _topic(
            "t_hot",
            "AI Infrastructure Expansion",
            momentum=0.72,
            novelty=0.45,
            weighted_now=120.0,
            weighted_prev=48.0,
            diversity=9,
        ),
        _topic(
            "t_cool",
            "Legacy Product Maintenance",
            momentum=-0.22,
            novelty=0.06,
            weighted_now=18.0,
            weighted_prev=25.0,
            diversity=2,
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert decisions[0]["topic_id"] == "t_hot"
    assert decisions[0]["decision_score"] > decisions[1]["decision_score"]
    assert decisions[0]["action_bucket"] in {"Act now", "Monitor"}
    assert decisions[0]["signal_statement"].startswith("Acting on:")
    assert decisions[0]["execution_plan"]
    assert decisions[0]["risk_guardrail"]
    assert 0.0 <= decisions[0]["signal_reliability"] <= 1.0
    assert 0.0 <= decisions[0]["trade_confidence"] <= 1.0


def test_change_sets_identify_new_accelerating_and_fading():
    current = [
        {
            "topic_id": "new_topic",
            "label": "New Topic",
            "vertical": "world",
            "momentum": 0.51,
            "weighted_volume_now": 66.0,
        },
        {
            "topic_id": "accel_topic",
            "label": "Accelerating Topic",
            "vertical": "science",
            "momentum": 0.65,
            "weighted_volume_now": 75.0,
        },
        {
            "topic_id": "fade_topic",
            "label": "Fading Topic",
            "vertical": "markets",
            "momentum": -0.4,
            "weighted_volume_now": 24.0,
        },
    ]
    previous = [
        {
            "topic_id": "accel_topic",
            "label": "Accelerating Topic",
            "vertical": "science",
            "momentum": 0.3,
            "weighted_volume_now": 50.0,
        },
        {
            "topic_id": "fade_topic",
            "label": "Fading Topic",
            "vertical": "markets",
            "momentum": -0.1,
            "weighted_volume_now": 36.0,
        },
    ]
    score_by_topic = {
        "new_topic": 4.2,
        "accel_topic": 3.8,
        "fade_topic": 2.0,
    }

    changes = build_changes(
        current_signals=current,
        previous_signals=previous,
        score_by_topic=score_by_topic,
        limit=10,
    )

    assert len(changes["new_narratives"]) == 1
    assert changes["new_narratives"][0]["topic_id"] == "new_topic"
    assert len(changes["accelerating"]) == 1
    assert changes["accelerating"][0]["topic_id"] == "accel_topic"
    assert len(changes["fading"]) == 1
    assert changes["fading"][0]["topic_id"] == "fade_topic"


def test_single_source_signal_is_not_marked_act_now():
    topics = [
        _topic(
            "single_source",
            "AI Compute Rally",
            momentum=0.95,
            novelty=0.9,
            weighted_now=88.0,
            weighted_prev=10.0,
            diversity=1,
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] != "Act now"


def test_investor_profile_requires_three_sources_for_act_now():
    topics = [
        _topic(
            "two_source_signal",
            "AI Compute Rally",
            momentum=0.95,
            novelty=0.9,
            weighted_now=88.0,
            weighted_prev=10.0,
            diversity=2,
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] != "Act now"


def test_asset_mapping_explainability_is_present_for_tradeable_signal():
    topics = [
        _topic(
            "mapped_signal",
            "AI Chip Platform",
            momentum=0.8,
            novelty=0.6,
            weighted_now=140.0,
            weighted_prev=30.0,
            diversity=5,
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    mapping = decisions[0]["asset_mapping"]
    assert mapping is not None
    assert mapping["theme_name"] == "ai_compute"
    assert len(mapping["matched_tokens"]) > 0
    assert isinstance(mapping["evidence_summary"], str)
    assert mapping["evidence_summary"]


def test_local_crime_headline_is_blocked_by_market_impact_filter():
    topics = [
        _topic(
            "local_crime",
            "World: Ilhan Omar",
            momentum=0.9,
            novelty=0.85,
            weighted_now=180.0,
            weighted_prev=32.0,
            diversity=10,
            vertical="world",
            keywords=["sydney", "murder", "lawyer", "charged"],
            entities=["Ilhan Omar"],
            representative_title="Man charged with Chris Baghsarian murder in Sydney likely no mastermind, lawyer says",
            source_name="guardian_world",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] == "Ignore"
    assert decisions[0]["trade_tickers"] == []
    assert decisions[0]["trade_direction"] in {"Operational", "None"}
    assert decisions[0]["market_impact_score"] < 0.35


def test_nfl_combine_coverage_does_not_trigger_sports_betting_trade():
    topics = [
        _topic(
            "nfl_combine",
            "Sports: NFL Combine",
            momentum=1.1,
            novelty=0.9,
            weighted_now=140.0,
            weighted_prev=24.0,
            diversity=6,
            vertical="sports",
            keywords=["nfl", "combine", "prospects", "draft"],
            entities=["NFL"],
            representative_title="NFL Combine: Prospects with the most to prove in Indy",
            source_name="yahoo_sports",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] != "Act now"
    assert decisions[0]["trade_theme"] is None
    assert decisions[0]["trade_tickers"] == []


def test_diplomatic_story_without_supply_shock_does_not_map_to_energy_trade():
    topics = [
        _topic(
            "embassy_story",
            "World: West Bank Embassy Services",
            momentum=0.6,
            novelty=0.9,
            weighted_now=90.0,
            weighted_prev=40.0,
            diversity=9,
            vertical="world",
            keywords=["embassy", "west", "bank", "settlement", "services"],
            entities=["U.S."],
            representative_title="U.S. Will Offer Embassy Services in a West Bank Settlement for the First Time",
            source_name="nyt_world",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] != "Act now"
    assert decisions[0]["trade_theme"] is None
    assert decisions[0]["trade_tickers"] == []


def test_editorial_finance_comparison_is_deweighted_and_non_tradeable():
    topics = [
        _topic(
            "editorial_markets",
            "Markets: Mortgage Rates",
            momentum=0.34,
            novelty=0.8,
            weighted_now=72.0,
            weighted_prev=38.0,
            diversity=10,
            vertical="markets",
            keywords=["jepi", "divo", "yield", "comparison"],
            entities=["JEPI", "DIVO"],
            representative_title="JEPI Vs. DIVO: High-Yield Funds Face-Off",
            source_name="seeking_alpha",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["impact_verdict"] == "weak"
    assert decisions[0]["market_impact_score"] < 0.5
    assert decisions[0]["trade_tickers"] == []


def test_local_data_center_story_does_not_overstate_liquidity_or_trade_mapping():
    topics = [
        _topic(
            "local_data_center",
            "Tech: Data Center",
            momentum=0.02,
            novelty=0.9,
            weighted_now=90.0,
            weighted_prev=88.0,
            diversity=11,
            vertical="tech",
            keywords=["data center", "jobs", "county", "expansion"],
            entities=["Lee County"],
            representative_title="Data center expansion and manufacturing project expected to bring hundreds of jobs to Lee County",
            source_name="gnews_tech_data_centers",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["liquidity_linkage"] < 0.6
    assert decisions[0]["trade_tickers"] == []
    assert decisions[0]["action_bucket"] in {"Monitor", "Ignore"}


def test_single_name_move_without_resolved_ticker_calls_out_unresolved_proxy():
    topics = [
        _topic(
            "single_name_unresolved",
            "Markets: FY26 Slides",
            momentum=0.45,
            novelty=1.0,
            weighted_now=84.0,
            weighted_prev=41.0,
            diversity=12,
            vertical="markets",
            keywords=["stock", "slides", "revenue", "growth"],
            entities=["Experience Co"],
            representative_title="Experience Co 1H26 slides show 5% revenue growth, stock drops 14%",
            source_name="investing_com",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["trade_tickers"] == []
    assert "ticker unresolved" in decisions[0]["execution_plan"].lower()


def test_direct_proxy_negative_move_prefers_short_direction():
    topics = [
        _topic(
            "nvidia_move",
            "Markets: Stock Futures Fall Nvidia",
            momentum=0.22,
            novelty=1.0,
            weighted_now=96.0,
            weighted_prev=44.0,
            diversity=10,
            vertical="markets",
            keywords=["stock futures", "nvidia", "shares", "selloff"],
            entities=["NVIDIA"],
            representative_title="Stock futures fall as Nvidia shares slide in late trading",
            source_name="marketwatch_top",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] in {"Act now", "Monitor"}
    assert decisions[0]["trade_direction"] == "Short"
    assert "NVDA" in decisions[0]["trade_tickers"]


def test_negative_momentum_signal_never_enters_act_now_bucket():
    topics = [
        _topic(
            "negative_salesforce",
            "Markets: Supply Chain",
            momentum=-0.38,
            novelty=1.0,
            weighted_now=96.0,
            weighted_prev=112.0,
            diversity=8,
            vertical="markets",
            keywords=["earnings", "ai ambitions", "cloud"],
            entities=["Salesforce", "Google"],
            representative_title="Salesforce shares drop 14% after earnings as AI ambitions pressure margins",
            source_name="yahoo_finance",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] != "Act now"
    assert "GOOGL" not in decisions[0]["trade_tickers"]


def test_low_alignment_topic_is_marked_unresolved_with_trust_warning():
    topics = [
        {
            **_topic(
                "misaligned_label",
                "Entertainment: Hilary Duff",
                momentum=0.66,
                novelty=0.91,
                weighted_now=102.0,
                weighted_prev=28.0,
                diversity=5,
                vertical="entertainment",
                keywords=["laura pausini", "cover story", "billboard"],
                entities=["Laura Pausini"],
                representative_title="Laura Pausini is Billboard's latest cover story",
                source_name="billboard",
            ),
            "trust_contract": {
                "label_alignment_confidence": 0.2,
                "proxy_confidence": 0.0,
                "source_quality_score": 1.0,
                "liquidity_link_confidence": None,
                "novelty_confidence": 0.9,
                "eligible_for_act_now": False,
                "warnings": ["Label-to-content alignment is low; entity naming suppressed."],
            },
        },
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["label"].startswith("Unresolved entertainment narrative")
    assert decisions[0]["trust_contract"]["label_alignment_confidence"] < 0.55
    assert decisions[0]["trust_contract"]["warnings"]


def test_act_now_candidate_is_downgraded_when_trust_contract_is_not_eligible():
    topics = [
        {
            **_topic(
                "act_now_trust_gate",
                "Tech: AI Infrastructure",
                momentum=0.92,
                novelty=0.05,
                weighted_now=160.0,
                weighted_prev=36.0,
                diversity=8,
                vertical="tech",
                keywords=["ai", "gpu", "chip", "datacenter"],
                entities=["NVIDIA"],
                representative_title="NVIDIA guidance beats estimates as AI demand accelerates",
                source_name="marketwatch_top",
            ),
        }
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["action_bucket"] != "Act now"
    assert decisions[0]["trust_contract"]["eligible_for_act_now"] is False
    assert any("below threshold" in warning.lower() for warning in decisions[0]["trust_contract"]["warnings"])


def test_salesforce_headline_proxy_does_not_leak_to_google_proxy():
    topics = [
        _topic(
            "salesforce_proxy",
            "Markets: AI Ambitions",
            momentum=0.42,
            novelty=1.0,
            weighted_now=120.0,
            weighted_prev=54.0,
            diversity=9,
            vertical="markets",
            keywords=["ai ambitions", "earnings", "cloud"],
            entities=["Salesforce", "Google"],
            representative_title="Salesforce shares jump 8% after earnings beat and raises guidance",
            source_name="yahoo_finance",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    if decisions[0]["trade_tickers"]:
        assert decisions[0]["trade_tickers"] == ["CRM"]
    assert "GOOGL" not in decisions[0]["trade_tickers"]


def test_wbd_earnings_prefers_single_name_proxy_over_peer_basket():
    topics = [
        _topic(
            "wbd_earnings",
            "Entertainment: Warner Bros Discovery",
            momentum=0.62,
            novelty=1.0,
            weighted_now=118.0,
            weighted_prev=50.0,
            diversity=6,
            vertical="entertainment",
            keywords=["streaming", "box office", "earnings"],
            entities=["Warner Bros. Discovery"],
            representative_title="Warner Bros. Discovery shares jump 12% after earnings beat and raises guidance",
            source_name="variety",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    if decisions[0]["trade_tickers"]:
        assert decisions[0]["trade_tickers"] == ["WBD"]
    assert not {"NFLX", "DIS", "PARA"}.intersection(set(decisions[0]["trade_tickers"]))


def test_bxsl_ticker_prefix_resolves_proxy():
    topics = [
        _topic(
            "bxsl_proxy",
            "Markets: Aristotle Growth Equity",
            momentum=0.31,
            novelty=1.0,
            weighted_now=88.0,
            weighted_prev=42.0,
            diversity=8,
            vertical="markets",
            keywords=["dividend", "credit", "yield"],
            entities=["BXSL"],
            representative_title="BXSL: It's A Buy Despite A Likely Dividend Cut",
            source_name="seeking_alpha",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=5, window="24h")

    assert len(decisions) == 1
    assert "proxy=resolved" in decisions[0]["impact_reason"]


def test_top_n_ranking_prioritizes_actionable_decisions_over_higher_score_ignores():
    topics = [
        _topic(
            "ignore_high",
            "World: Ilhan Omar",
            momentum=0.9,
            novelty=0.85,
            weighted_now=180.0,
            weighted_prev=32.0,
            diversity=10,
            vertical="world",
            keywords=["sydney", "murder", "lawyer", "charged"],
            entities=["Ilhan Omar"],
            representative_title="Man charged with Chris Baghsarian murder in Sydney likely no mastermind, lawyer says",
            source_name="guardian_world",
        ),
        _topic(
            "monitor_lower",
            "Markets: FY26 Slides",
            momentum=0.45,
            novelty=1.0,
            weighted_now=84.0,
            weighted_prev=41.0,
            diversity=12,
            vertical="markets",
            keywords=["stock", "slides", "revenue", "growth"],
            entities=["Experience Co"],
            representative_title="Experience Co 1H26 slides show 5% revenue growth, stock drops 14%",
            source_name="investing_com",
        ),
    ]

    decisions, _ = build_decisions(topics, profile="investor", top_n=1, window="24h")

    assert len(decisions) == 1
    assert decisions[0]["topic_id"] == "monitor_lower"
    assert decisions[0]["action_bucket"] in {"Act now", "Monitor"}


def _risk_decision(
    topic_id: str,
    *,
    action_bucket: str = "Act now",
    trade_theme: str = "ai_compute",
    proposed_notional_pct: float = 2.0,
    trade_direction: str = "Long",
    trade_tickers: list[str] | None = None,
) -> dict:
    return {
        "topic_id": topic_id,
        "label": topic_id,
        "vertical": "tech",
        "action_bucket": action_bucket,
        "trade_theme": trade_theme,
        "trade_direction": trade_direction,
        "trade_tickers": trade_tickers or ["NVDA"],
        "proposed_notional_pct": proposed_notional_pct,
        "approved_notional_pct": 0.0,
        "execution_status": "pending",
        "risk_block_reason": None,
        "execution_plan": "starter",
    }


def test_risk_controls_block_pyramiding_and_enforce_position_cap():
    generated_at = datetime(2026, 2, 25, 21, 0, tzinfo=timezone.utc)
    decisions = [
        _risk_decision("d1", trade_theme="ai_compute", proposed_notional_pct=4.2),
        _risk_decision("d2", trade_theme="ai_compute", proposed_notional_pct=1.4),
    ]

    adjusted, executed_now, risk_summary = apply_risk_controls(
        decisions=decisions,
        profile="investor",
        generated_at=generated_at,
        historical_executions=[],
    )

    assert adjusted[0]["execution_status"] == "approved"
    assert adjusted[0]["approved_notional_pct"] == 2.0
    assert adjusted[1]["execution_status"] == "blocked_risk_cap"
    assert "already active today" in str(adjusted[1]["risk_block_reason"])
    assert len(executed_now) == 1
    assert risk_summary["approved_trade_count"] == 1
    assert risk_summary["blocked_trade_count"] == 1
    assert risk_summary["used_daily_notional_pct"] == 2.0


def test_risk_controls_keep_act_now_bucket_when_blocked():
    generated_at = datetime(2026, 2, 25, 21, 0, tzinfo=timezone.utc)
    historical_executions = [
        {
            "topic_id": "existing_theme",
            "trade_theme": "ai_compute",
            "trade_direction": "Long",
            "trade_tickers": ["NVDA"],
            "approved_notional_pct": 2.0,
            "execution_status": "approved",
            "opened_at": (generated_at - timedelta(hours=1)).isoformat(),
        }
    ]
    decisions = [_risk_decision("d1", trade_theme="ai_compute", proposed_notional_pct=1.5)]

    adjusted, executed_now, risk_summary = apply_risk_controls(
        decisions=decisions,
        profile="investor",
        generated_at=generated_at,
        historical_executions=historical_executions,
    )

    assert adjusted[0]["execution_status"] == "blocked_risk_cap"
    assert adjusted[0]["action_bucket"] == "Act now"
    assert adjusted[0]["trade_direction"] == "Long"
    assert adjusted[0]["trade_tickers"] == ["NVDA"]
    assert "blocked by portfolio risk controls" in str(adjusted[0]["execution_plan"]).lower() or "no pyramiding" in str(
        adjusted[0]["execution_plan"]
    ).lower()
    assert len(executed_now) == 0
    assert risk_summary["approved_trade_count"] == 0
    assert risk_summary["blocked_trade_count"] == 1


def test_risk_controls_block_when_daily_notional_is_exhausted():
    generated_at = datetime(2026, 2, 25, 21, 0, tzinfo=timezone.utc)
    historical_executions = [
        {
            "topic_id": "old_1",
            "trade_theme": "macro_policy",
            "trade_direction": "Hedge",
            "trade_tickers": ["SPY", "TLT"],
            "approved_notional_pct": 5.5,
            "execution_status": "approved",
            "opened_at": (generated_at - timedelta(hours=4)).isoformat(),
        }
    ]
    decisions = [_risk_decision("d1", trade_theme="ai_compute", proposed_notional_pct=1.5)]

    adjusted, executed_now, risk_summary = apply_risk_controls(
        decisions=decisions,
        profile="investor",
        generated_at=generated_at,
        historical_executions=historical_executions,
    )

    assert adjusted[0]["execution_status"] == "blocked_risk_cap"
    assert "daily notional cap" in str(adjusted[0]["risk_block_reason"])
    assert len(executed_now) == 0
    assert risk_summary["approved_trade_count"] == 0
    assert risk_summary["blocked_trade_count"] == 1
    assert risk_summary["used_daily_notional_pct"] == 5.5
