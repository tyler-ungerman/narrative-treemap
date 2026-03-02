"use client";

import { useEffect, useState } from "react";

import { fetchPortfolio } from "@/lib/api";
import { formatGeneratedTime, formatPercent } from "@/lib/format";
import { DecisionProfile, PortfolioResponse, TopicFilters } from "@/lib/types";

interface PaperPortfolioPanelProps {
  filters: TopicFilters;
}

const PROFILE_OPTIONS: Array<{ value: DecisionProfile; label: string }> = [
  { value: "investor", label: "Investor" },
  { value: "research", label: "Research" },
  { value: "operations", label: "Operations" },
  { value: "security", label: "Security" }
];

export function PaperPortfolioPanel({ filters }: PaperPortfolioPanelProps) {
  const [profile, setProfile] = useState<DecisionProfile>("investor");
  const [payload, setPayload] = useState<PortfolioResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetchPortfolio(filters, profile);
        if (cancelled) return;
        setPayload(response);
      } catch (fetchError) {
        if (cancelled) return;
        setError(fetchError instanceof Error ? fetchError.message : "Unable to load portfolio");
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [filters, profile]);

  return (
    <section className="analytics-panel">
      <header className="analytics-header">
        <div>
          <p className="hero-kicker">Paper Portfolio</p>
          <h2>Auto-tracked approved decisions</h2>
          <p>Tracks open paper positions, current P/L, and risk utilization based on approved decision trades.</p>
        </div>
        <div className="decision-profile-group">
          {PROFILE_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              className={profile === option.value ? "is-selected" : ""}
              onClick={() => setProfile(option.value)}
            >
              {option.label}
            </button>
          ))}
        </div>
      </header>

      {error ? <p className="error-banner">{error}</p> : null}

      {loading ? (
        <div className="decision-loading">
          <p>Updating paper portfolio...</p>
        </div>
      ) : payload ? (
        <div className="analytics-grid">
          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Portfolio summary</h3>
              <p>Generated {formatGeneratedTime(payload.summary.generated_at)}</p>
              <p>
                Snapshots: {payload.summary.history_snapshots} · Scope:{" "}
                {payload.summary.snapshot_scope === "filtered" ? "Current filters" : "All-filter fallback"}
              </p>
            </div>
            <div className="decision-change-chips">
              <span>Positions: {payload.summary.positions}</span>
              <span>Open: {payload.summary.open_positions_count ?? 0}</span>
              <span>
                Closed/Expired: {(payload.summary.closed_positions_count ?? 0) + (payload.summary.expired_positions_count ?? 0)}
              </span>
              <span>Priced: {payload.summary.priced_positions}</span>
              <span>Win rate: {formatPercent(payload.summary.win_rate)}</span>
              <span>Realized P/L: {formatPercent(payload.summary.realized_pnl_pct ?? 0)}</span>
              <span>
                Unrealized P/L:{" "}
                {payload.summary.unrealized_pnl_pct == null ? "N/A" : formatPercent(payload.summary.unrealized_pnl_pct)}
              </span>
              <span>Total P/L: {formatPercent(payload.summary.total_pnl_pct)}</span>
              <span>Open notional: {(payload.summary.open_notional_pct ?? 0).toFixed(2)}%</span>
              <span>Risk utilization (open): {formatPercent(payload.summary.risk_utilization)}</span>
              <span>
                Cumulative approved notional:{" "}
                {(payload.summary.cumulative_approved_notional_pct ?? payload.summary.total_notional_pct).toFixed(2)}%
              </span>
            </div>
            <p className="decision-item-next">
              Lifecycle: {payload.summary.lifecycle.candidates} candidates → {payload.summary.lifecycle.considered} considered
              → {payload.summary.lifecycle.recommended} recommended → {payload.summary.lifecycle.approved} approved
              → {payload.summary.lifecycle.opened} opened → {payload.summary.lifecycle.closed} closed /{" "}
              {payload.summary.lifecycle.expired} expired
            </p>
            {payload.summary.lifecycle.explanations.length > 0 ? (
              <p className="decision-item-next">
                {payload.summary.lifecycle.explanations[0]}{" "}
                {payload.summary.lifecycle.corrective_ctas[0]
                  ? `Corrective action: ${payload.summary.lifecycle.corrective_ctas[0]}`
                  : ""}
              </p>
            ) : null}
            {payload.summary.total_pnl_is_partial ? (
              <p className="decision-item-next">
                Unrealized P/L is partial. {payload.summary.open_unpriced_positions ?? 0} open position(s) do not have mark prices yet.
              </p>
            ) : null}
            <div className="portfolio-curve">
              {payload.risk_curve.map((point) => (
                <div key={point.date} className="portfolio-curve-row">
                  <span>{point.date.slice(5)}</span>
                  <div className="portfolio-curve-track">
                    <div
                      className="portfolio-curve-fill"
                      style={{ width: `${Math.max(0, Math.min(point.utilization * 100, 100))}%` }}
                    />
                  </div>
                  <span>{formatPercent(point.utilization)}</span>
                </div>
              ))}
            </div>
          </section>

          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Positions</h3>
              <p>Latest 40 positions sorted by entry time (open, closed, expired).</p>
            </div>
            {payload.positions.length === 0 ? (
              <p className="decision-empty">
                No positions yet. Corrective action: approve a trust-qualified recommendation in Decision Briefing to open
                the first paper trade.
              </p>
            ) : (
            <ul className="decision-list">
              {payload.positions.slice(0, 40).map((position) => (
                <li key={position.trade_id} className="decision-item">
                  <div className="decision-item-head">
                    <span>{position.label}</span>
                    <span
                      className={
                        position.position_status === "OPEN"
                          ? position.position_pnl_pct == null
                            ? "decision-badge monitor"
                            : position.position_pnl_pct > 0
                              ? "decision-badge act-now"
                              : "decision-badge ignore"
                          : "decision-badge ignore"
                      }
                    >
                      {position.position_status ?? "OPEN"} ·{" "}
                      {position.position_pnl_pct == null ? "Unpriced" : position.position_pnl_pct > 0 ? "Profitable" : "Underwater"}
                    </span>
                  </div>
                  <p className="decision-item-metrics">
                    {position.trade_direction} {position.trade_tickers.join(", ")} · {position.notional_pct.toFixed(2)}% notional ·{" "}
                    {position.days_open}d held
                  </p>
                  <p className="decision-item-metrics">
                    Basket return: {position.basket_return == null ? "n/a" : formatPercent(position.basket_return)} · Realized:{" "}
                    {position.realized_pnl_pct == null ? "n/a" : formatPercent(position.realized_pnl_pct)} · Unrealized:{" "}
                    {position.unrealized_pnl_pct == null ? "n/a" : formatPercent(position.unrealized_pnl_pct)}
                  </p>
                  {position.closed_at || position.expires_at ? (
                    <p className="decision-item-next">
                      Exit marker: {formatGeneratedTime(position.closed_at ?? position.expires_at ?? position.opened_at)}
                    </p>
                  ) : null}
                  {position.asset_mapping ? (
                    <p className="decision-item-next">
                      Explainability: {position.asset_mapping.theme_name} · {position.asset_mapping.evidence_summary}
                    </p>
                  ) : null}
                </li>
              ))}
            </ul>
            )}
          </section>
        </div>
      ) : (
        <div className="decision-loading">
          <p>No portfolio data yet.</p>
        </div>
      )}
    </section>
  );
}
