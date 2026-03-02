from datetime import datetime

from pydantic import BaseModel, Field


class TrustContractModel(BaseModel):
    label_alignment_confidence: float
    proxy_confidence: float
    source_quality_score: float
    liquidity_link_confidence: float | None = None
    novelty_confidence: float
    eligible_for_act_now: bool
    warnings: list[str] = Field(default_factory=list)


class LifecycleModel(BaseModel):
    candidates: int
    considered: int
    recommended: int
    approved: int
    opened: int
    closed: int
    expired: int
    explanations: list[str] = Field(default_factory=list)
    corrective_ctas: list[str] = Field(default_factory=list)


class TopicItem(BaseModel):
    title: str
    url: str
    source_name: str
    published_at: datetime | None
    source_quality: float | None = None


class TopicModel(BaseModel):
    topic_id: str
    label: str
    vertical: str
    volume_now: int
    volume_prev: int
    momentum: float
    novelty: float
    diversity: int
    sparkline: list[int]
    representative_items: list[TopicItem]
    keywords: list[str]
    entities: list[str]
    related_topic_ids: list[str]
    summary: str
    weighted_volume_now: float | None = None
    weighted_volume_prev: float | None = None
    source_quality_score: float | None = None
    label_confidence: float | None = None
    trust_contract: TrustContractModel | None = None


class SourceHealthModel(BaseModel):
    source_name: str
    last_success: datetime | None
    last_error: str | None
    items_fetched: int
    latency_ms: int
    updated_at: datetime | None


class TopicsMetadata(BaseModel):
    generated_at: datetime
    window: str
    item_count: int
    source_health: list[SourceHealthModel]
    source_quality: dict[str, float] | None = None
    algorithm: str
    lifecycle: LifecycleModel | None = None


class TopicsResponse(BaseModel):
    topics: list[TopicModel]
    metadata: TopicsMetadata


class DecisionChangeItem(BaseModel):
    topic_id: str
    label: str
    vertical: str
    momentum: float
    weighted_volume_now: float
    delta_momentum: float | None = None
    delta_weighted_volume: float | None = None


class DecisionChanges(BaseModel):
    new_narratives: list[DecisionChangeItem]
    accelerating: list[DecisionChangeItem]
    fading: list[DecisionChangeItem]


class DecisionItem(BaseModel):
    topic_id: str
    label: str
    vertical: str
    decision_score: float
    action_bucket: str
    rationale: str
    next_step: str
    signal_statement: str
    execution_plan: str
    trade_tickers: list[str]
    trade_direction: str
    trade_theme: str | None
    proposed_notional_pct: float
    approved_notional_pct: float
    execution_status: str
    risk_block_reason: str | None = None
    risk_guardrail: str
    signal_reliability: float
    trade_confidence: float
    asset_mapping: dict | None = None
    market_impact_score: float
    economic_relevance: float
    surprise_score: float
    liquidity_linkage: float
    impact_verdict: str
    impact_reason: str
    momentum: float
    novelty: float
    weighted_volume_now: float
    weighted_volume_prev: float
    diversity: int
    label_confidence: float
    source_quality_score: float
    keywords: list[str]
    entities: list[str]
    representative_items: list[TopicItem]
    related_topic_ids: list[str]
    trust_contract: TrustContractModel


class DecisionSummary(BaseModel):
    window: str
    profile: str
    generated_at: datetime
    previous_generated_at: datetime | None
    considered_topics: int
    top_n: int
    actionable_now: int
    monitor_count: int
    ignore_count: int
    new_count: int
    accelerating_count: int
    fading_count: int
    approved_trade_count: int
    blocked_trade_count: int
    used_daily_notional_pct: float
    max_daily_notional_pct: float
    max_simultaneous_themes: int
    lifecycle: LifecycleModel


class DecisionBriefingMetadata(BaseModel):
    generated_at: datetime | None
    window: str
    item_count: int
    algorithm: str


class DecisionRiskControls(BaseModel):
    max_simultaneous_themes: int
    max_daily_notional_pct: float
    max_position_notional_pct: float
    cooldown_hours: int
    max_new_trades_per_day: int
    active_themes_count: int
    used_daily_notional_pct: float
    approved_trade_count: int
    blocked_trade_count: int


class DecisionBriefingResponse(BaseModel):
    summary: DecisionSummary
    changes: DecisionChanges
    decisions: list[DecisionItem]
    risk_controls: DecisionRiskControls
    metadata: DecisionBriefingMetadata


class BacktestSample(BaseModel):
    topic_id: str | None
    label: str | None
    event_time: datetime
    followup_time: datetime
    momentum_at_signal: float
    momentum_at_horizon: float
    momentum_persisted: bool
    proxy_available: bool
    proxy_return: float | None
    proxy_positive: bool | None
    trade_tickers: list[str]
    horizon_coverage: float | None = None
    evaluation_mode: str | None = None
    is_fallback: bool = False
    fallback_reason: str | None = None
    effective_horizon: str | None = None
    expected_horizon: str | None = None


class BacktestHorizon(BaseModel):
    days: int
    evaluated_signals: int
    momentum_persisted: int
    momentum_precision: float
    proxy_evaluated: int
    proxy_positive: int
    proxy_precision: float
    average_proxy_return: float
    evaluation_mode: str
    full_horizon_evaluated: int
    warmup_evaluated: int
    is_fallback: bool = False
    fallback_reason: str | None = None
    effective_horizon: str | None = None
    expected_horizon: str | None = None
    samples: list[BacktestSample]


class BacktestSummary(BaseModel):
    window: str
    profile: str
    generated_at: datetime
    filter_signature: str
    snapshot_scope: str
    history_snapshots: int
    act_now_signals: int
    lifecycle: LifecycleModel


class BacktestResponse(BaseModel):
    summary: BacktestSummary
    horizons: list[BacktestHorizon]


class PortfolioPosition(BaseModel):
    trade_id: str
    topic_id: str
    label: str
    vertical: str
    trade_theme: str
    trade_direction: str
    trade_tickers: list[str]
    notional_pct: float
    opened_at: datetime
    closed_at: datetime | None = None
    expires_at: datetime | None = None
    position_status: str = "OPEN"
    days_open: int
    basket_return: float | None
    position_pnl_pct: float | None
    realized_pnl_pct: float | None = None
    unrealized_pnl_pct: float | None = None
    leg_returns: list[dict]
    asset_mapping: dict | None = None


class PortfolioRiskPoint(BaseModel):
    date: str
    opened_notional_pct: float
    open_notional_pct: float
    cumulative_notional_pct: float
    utilization: float


class PortfolioSummary(BaseModel):
    window: str
    profile: str
    generated_at: datetime
    positions: int
    open_positions_count: int = 0
    closed_positions_count: int = 0
    expired_positions_count: int = 0
    priced_positions: int
    win_rate: float
    total_notional_pct: float
    cumulative_approved_notional_pct: float = 0.0
    open_notional_pct: float = 0.0
    total_pnl_pct: float
    realized_pnl_pct: float = 0.0
    unrealized_pnl_pct: float | None = None
    unrealized_priced_positions: int = 0
    open_unpriced_positions: int = 0
    total_pnl_is_partial: bool = False
    max_daily_notional_pct: float
    max_position_notional_pct: float
    risk_utilization: float
    history_snapshots: int
    snapshot_scope: str
    lifecycle: LifecycleModel


class PortfolioResponse(BaseModel):
    summary: PortfolioSummary
    positions: list[PortfolioPosition]
    risk_curve: list[PortfolioRiskPoint]


class AlertRuleModel(BaseModel):
    rule_id: str
    name: str
    channel_type: str
    endpoint_url: str
    window: str
    momentum_threshold: float
    diversity_threshold: int
    min_quality_score: float
    verticals: list[str]
    sources: list[str]
    enabled: bool
    created_at: datetime
    updated_at: datetime
    last_triggered_at: datetime | None


class AlertRuleUpsertRequest(BaseModel):
    name: str
    channel_type: str
    endpoint_url: str
    window: str = "24h"
    momentum_threshold: float = 0.2
    diversity_threshold: int = 3
    min_quality_score: float = 0.8
    verticals: list[str] = Field(default_factory=list)
    sources: list[str] = Field(default_factory=list)
    enabled: bool = True


class AlertEventModel(BaseModel):
    event_id: str
    rule_id: str
    topic_id: str
    topic_label: str
    channel_type: str
    delivery_status: str
    delivery_error: str | None
    payload: dict
    triggered_at: datetime


class AlertRulesResponse(BaseModel):
    rules: list[AlertRuleModel]
    events: list[AlertEventModel]


class AlertEvaluateResponse(BaseModel):
    generated_at: datetime
    evaluated_rules: int
    total_candidates: int
    total_sent: int
    total_failed: int
    results: list[dict]
