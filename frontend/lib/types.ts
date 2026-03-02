export type WindowOption = "1h" | "6h" | "24h" | "7d";
export type TreemapDensityOption = "readable" | "expanded" | "all";
export type DecisionProfile = "investor" | "research" | "operations" | "security";
export type ViewTab = "map" | "action" | "briefing" | "backtest" | "portfolio" | "alerts";

export interface TrustContract {
  label_alignment_confidence: number;
  proxy_confidence: number;
  source_quality_score: number;
  liquidity_link_confidence: number | null;
  novelty_confidence: number;
  eligible_for_act_now: boolean;
  warnings: string[];
}

export interface LifecycleModel {
  candidates: number;
  considered: number;
  recommended: number;
  approved: number;
  opened: number;
  closed: number;
  expired: number;
  explanations: string[];
  corrective_ctas: string[];
}

export interface TopicItem {
  title: string;
  url: string;
  source_name: string;
  published_at: string | null;
  source_quality?: number;
}

export interface Topic {
  topic_id: string;
  label: string;
  vertical: string;
  volume_now: number;
  volume_prev: number;
  momentum: number;
  novelty: number;
  diversity: number;
  sparkline: number[];
  representative_items: TopicItem[];
  keywords: string[];
  entities: string[];
  related_topic_ids: string[];
  summary: string;
  weighted_volume_now?: number;
  weighted_volume_prev?: number;
  source_quality_score?: number;
  label_confidence?: number;
  trust_contract?: TrustContract | null;
}

export interface SourceHealth {
  source_name: string;
  last_success: string | null;
  last_error: string | null;
  items_fetched: number;
  latency_ms: number;
  updated_at: string | null;
}

export interface TopicsMetadata {
  generated_at: string;
  window: WindowOption;
  item_count: number;
  source_health: SourceHealth[];
  source_quality?: Record<string, number>;
  algorithm: string;
  lifecycle?: LifecycleModel | null;
}

export interface TopicsResponse {
  topics: Topic[];
  metadata: TopicsMetadata;
}

export interface TopicFilters {
  window: WindowOption;
  verticals: string[];
  sources: string[];
  onlyRising: boolean;
  search: string;
}

export interface DecisionChangeItem {
  topic_id: string;
  label: string;
  vertical: string;
  momentum: number;
  weighted_volume_now: number;
  delta_momentum: number | null;
  delta_weighted_volume: number | null;
}

export interface DecisionChanges {
  new_narratives: DecisionChangeItem[];
  accelerating: DecisionChangeItem[];
  fading: DecisionChangeItem[];
}

export interface DecisionItem {
  topic_id: string;
  label: string;
  vertical: string;
  decision_score: number;
  action_bucket: "Act now" | "Monitor" | "Ignore";
  rationale: string;
  next_step: string;
  signal_statement: string;
  execution_plan: string;
  trade_tickers: string[];
  trade_direction: string;
  trade_theme: string | null;
  proposed_notional_pct: number;
  approved_notional_pct: number;
  execution_status: string;
  risk_block_reason: string | null;
  risk_guardrail: string;
  signal_reliability: number;
  trade_confidence: number;
  asset_mapping: {
    theme_name: string;
    thesis: string;
    tickers: string[];
    direction: string;
    confidence: number;
    matched_tokens: string[];
    matched_strong_tokens: string[];
    matched_phrases: string[];
    headline_evidence: string[];
    vertical_alignment: boolean;
    evidence_summary: string;
  } | null;
  market_impact_score: number;
  economic_relevance: number;
  surprise_score: number;
  liquidity_linkage: number;
  impact_verdict: "strong" | "mixed" | "weak";
  impact_reason: string;
  momentum: number;
  novelty: number;
  weighted_volume_now: number;
  weighted_volume_prev: number;
  diversity: number;
  label_confidence: number;
  source_quality_score: number;
  keywords: string[];
  entities: string[];
  representative_items: TopicItem[];
  related_topic_ids: string[];
  trust_contract: TrustContract;
}

export interface DecisionSummary {
  window: WindowOption;
  profile: DecisionProfile;
  generated_at: string;
  previous_generated_at: string | null;
  considered_topics: number;
  top_n: number;
  actionable_now: number;
  monitor_count: number;
  ignore_count: number;
  new_count: number;
  accelerating_count: number;
  fading_count: number;
  approved_trade_count: number;
  blocked_trade_count: number;
  used_daily_notional_pct: number;
  max_daily_notional_pct: number;
  max_simultaneous_themes: number;
  lifecycle: LifecycleModel;
}

export interface DecisionBriefingMetadata {
  generated_at: string | null;
  window: WindowOption;
  item_count: number;
  algorithm: string;
}

export interface DecisionBriefingResponse {
  summary: DecisionSummary;
  changes: DecisionChanges;
  decisions: DecisionItem[];
  risk_controls: {
    max_simultaneous_themes: number;
    max_daily_notional_pct: number;
    max_position_notional_pct: number;
    cooldown_hours: number;
    max_new_trades_per_day: number;
    active_themes_count: number;
    used_daily_notional_pct: number;
    approved_trade_count: number;
    blocked_trade_count: number;
  };
  metadata: DecisionBriefingMetadata;
}

export interface BacktestSample {
  topic_id: string | null;
  label: string | null;
  event_time: string;
  followup_time: string;
  momentum_at_signal: number;
  momentum_at_horizon: number;
  momentum_persisted: boolean;
  proxy_available: boolean;
  proxy_return: number | null;
  proxy_positive: boolean | null;
  trade_tickers: string[];
  horizon_coverage?: number | null;
  evaluation_mode?: string | null;
  is_fallback?: boolean;
  fallback_reason?: string | null;
  effective_horizon?: string | null;
  expected_horizon?: string | null;
}

export interface BacktestHorizon {
  days: number;
  evaluated_signals: number;
  momentum_persisted: number;
  momentum_precision: number;
  proxy_evaluated: number;
  proxy_positive: number;
  proxy_precision: number;
  average_proxy_return: number;
  evaluation_mode: string;
  full_horizon_evaluated: number;
  warmup_evaluated: number;
  is_fallback?: boolean;
  fallback_reason?: string | null;
  effective_horizon?: string | null;
  expected_horizon?: string | null;
  samples: BacktestSample[];
}

export interface BacktestResponse {
  summary: {
    window: WindowOption;
    profile: DecisionProfile;
    generated_at: string;
    filter_signature: string;
    snapshot_scope: string;
    history_snapshots: number;
    act_now_signals: number;
    lifecycle: LifecycleModel;
  };
  horizons: BacktestHorizon[];
}

export interface PortfolioPosition {
  trade_id: string;
  topic_id: string;
  label: string;
  vertical: string;
  trade_theme: string;
  trade_direction: string;
  trade_tickers: string[];
  notional_pct: number;
  opened_at: string;
  closed_at?: string | null;
  expires_at?: string | null;
  position_status?: "OPEN" | "CLOSED" | "EXPIRED" | string;
  days_open: number;
  basket_return: number | null;
  position_pnl_pct: number | null;
  realized_pnl_pct?: number | null;
  unrealized_pnl_pct?: number | null;
  leg_returns: Array<{
    ticker: string;
    entry_price: number;
    exit_price: number;
    raw_return: number;
    directional_return: number;
  }>;
  asset_mapping: DecisionItem["asset_mapping"];
}

export interface PortfolioResponse {
  summary: {
    window: WindowOption;
    profile: DecisionProfile;
    generated_at: string;
    positions: number;
    open_positions_count?: number;
    closed_positions_count?: number;
    expired_positions_count?: number;
    priced_positions: number;
    win_rate: number;
    total_notional_pct: number;
    cumulative_approved_notional_pct?: number;
    open_notional_pct?: number;
    total_pnl_pct: number;
    realized_pnl_pct?: number;
    unrealized_pnl_pct?: number | null;
    unrealized_priced_positions?: number;
    open_unpriced_positions?: number;
    total_pnl_is_partial?: boolean;
    max_daily_notional_pct: number;
    max_position_notional_pct: number;
    risk_utilization: number;
    history_snapshots: number;
    snapshot_scope: string;
    lifecycle: LifecycleModel;
  };
  positions: PortfolioPosition[];
  risk_curve: Array<{
    date: string;
    opened_notional_pct: number;
    open_notional_pct?: number;
    cumulative_notional_pct: number;
    utilization: number;
  }>;
}

export type AlertChannelType = "webhook" | "discord" | "slack";

export interface AlertRule {
  rule_id: string;
  name: string;
  channel_type: AlertChannelType;
  endpoint_url: string;
  window: WindowOption;
  momentum_threshold: number;
  diversity_threshold: number;
  min_quality_score: number;
  verticals: string[];
  sources: string[];
  enabled: boolean;
  created_at: string;
  updated_at: string;
  last_triggered_at: string | null;
}

export interface AlertEvent {
  event_id: string;
  rule_id: string;
  topic_id: string;
  topic_label: string;
  channel_type: AlertChannelType;
  delivery_status: string;
  delivery_error: string | null;
  payload: Record<string, unknown>;
  triggered_at: string;
}

export interface AlertRulesResponse {
  rules: AlertRule[];
  events: AlertEvent[];
}

export interface AlertEvaluationResponse {
  generated_at: string;
  evaluated_rules: number;
  total_candidates: number;
  total_sent: number;
  total_failed: number;
  results: Array<{
    rule_id: string;
    name: string;
    window: WindowOption;
    candidates: number;
    sent: number;
    failed: number;
  }>;
}
