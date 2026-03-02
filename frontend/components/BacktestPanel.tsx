"use client";

import { useEffect, useMemo, useState } from "react";

import { fetchBacktest } from "@/lib/api";
import { isWarmupFallbackHorizon } from "@/lib/backtest";
import { formatGeneratedTime, formatPercent } from "@/lib/format";
import { BacktestResponse, DecisionProfile, Topic, TopicFilters } from "@/lib/types";

interface BacktestPanelProps {
  filters: TopicFilters;
  topics: Topic[];
  onInspectTopic: (topic: Topic) => void;
}

const PROFILE_OPTIONS: Array<{ value: DecisionProfile; label: string }> = [
  { value: "investor", label: "Investor" },
  { value: "research", label: "Research" },
  { value: "operations", label: "Operations" },
  { value: "security", label: "Security" }
];

export function BacktestPanel({ filters, topics, onInspectTopic }: BacktestPanelProps) {
  const [profile, setProfile] = useState<DecisionProfile>("investor");
  const [payload, setPayload] = useState<BacktestResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetchBacktest(filters, profile);
        if (cancelled) return;
        setPayload(response);
      } catch (fetchError) {
        if (cancelled) return;
        setError(fetchError instanceof Error ? fetchError.message : "Unable to load backtest");
      } finally {
        if (!cancelled) setLoading(false);
      }
    };
    void load();
    return () => {
      cancelled = true;
    };
  }, [filters, profile]);

  const topicById = useMemo(() => new Map(topics.map((topic) => [topic.topic_id, topic])), [topics]);
  const sevenDayHorizon = payload?.horizons.find((horizon) => horizon.days === 7);
  const totalEvaluatedSignals = useMemo(
    () => payload?.horizons.reduce((sum, horizon) => sum + horizon.evaluated_signals, 0) ?? 0,
    [payload]
  );

  const describeHorizonMode = (horizon: BacktestResponse["horizons"][number]) => {
    if (isWarmupFallbackHorizon(horizon)) {
      return {
        badge: "Warmup / Next-snapshot",
        badgeClassName: "decision-badge monitor",
        copy: "Fallback mode: not enough mature history for true horizon follow-up yet."
      };
    }
    return {
      badge: "True horizon",
      badgeClassName: "decision-badge act-now",
      copy: "Evaluated with full expected horizon follow-up."
    };
  };

  return (
    <section className="analytics-panel">
      <header className="analytics-header">
        <div>
          <p className="hero-kicker">Backtest</p>
          <h2>
            {totalEvaluatedSignals > 0
              ? "Signal precision over expected 7d and 30d horizons"
              : "Backtest readiness and horizon maturity"}
          </h2>
          <p>
            {totalEvaluatedSignals > 0
              ? "Measures whether Act now narratives persist in momentum and whether mapped proxy baskets move. Warmup rows are explicitly marked when next-snapshot fallback is used."
              : "No evaluable performance yet. This panel shows lifecycle readiness and fallback horizon metadata until a true-horizon sample exists."}
          </p>
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
          <p>Running backtest...</p>
        </div>
      ) : payload ? (
        <div className="analytics-grid">
          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Backtest summary</h3>
              <p>
                Signals: {payload.summary.act_now_signals} · Snapshots: {payload.summary.history_snapshots} · Generated:{" "}
                {formatGeneratedTime(payload.summary.generated_at)}
              </p>
              <p>Snapshot scope: {payload.summary.snapshot_scope === "filtered" ? "Current filters" : "All-filter fallback"}</p>
              <p>
                Lifecycle: {payload.summary.lifecycle.candidates} candidates → {payload.summary.lifecycle.considered} considered
                → {payload.summary.lifecycle.recommended} recommended → {payload.summary.lifecycle.approved} approved
              </p>
              {payload.summary.lifecycle.explanations.length > 0 ? (
                <p>
                  {payload.summary.lifecycle.explanations[0]}{" "}
                  {payload.summary.lifecycle.corrective_ctas[0]
                    ? `Corrective action: ${payload.summary.lifecycle.corrective_ctas[0]}`
                    : ""}
                </p>
              ) : null}
            </div>
            {totalEvaluatedSignals === 0 ? (
              <p className="decision-empty">
                No evaluable Act now signals yet. Backtest performance is unavailable until at least one signal has a valid
                follow-up window.
              </p>
            ) : null}
            <div className="backtest-horizon-cards">
              {payload.horizons.map((horizon) => {
                const mode = describeHorizonMode(horizon);
                return (
                <article key={horizon.days} className="backtest-card">
                  <div className="backtest-card-head">
                    <h4>{horizon.expected_horizon ?? `${horizon.days}d`} expected horizon</h4>
                    <span className={mode.badgeClassName}>{mode.badge}</span>
                  </div>
                  {horizon.evaluated_signals > 0 && !horizon.is_fallback ? (
                    <>
                      <p>
                        Momentum precision: <strong>{formatPercent(horizon.momentum_precision)}</strong>
                      </p>
                      <p>
                        Proxy precision: <strong>{formatPercent(horizon.proxy_precision)}</strong>
                      </p>
                      <p>
                        Avg proxy return: <strong>{formatPercent(horizon.average_proxy_return)}</strong>
                      </p>
                    </>
                  ) : horizon.evaluated_signals > 0 ? (
                    <p>Warmup diagnostic only (next-snapshot fallback); maturity performance is intentionally hidden.</p>
                  ) : (
                    <p>No evaluable signals for this horizon yet.</p>
                  )}
                  <p>
                    Evaluated: {horizon.evaluated_signals} · Proxy priced: {horizon.proxy_evaluated}
                  </p>
                  <p>
                    {mode.copy}
                  </p>
                  <p>
                    Fallback: {horizon.is_fallback ? "true" : "false"} · Reason: {horizon.fallback_reason ?? "n/a"}
                  </p>
                  <p>
                    Effective: {horizon.effective_horizon ?? "n/a"} · Expected: {horizon.expected_horizon ?? `${horizon.days}d`}
                  </p>
                  <p>
                    Full: {horizon.full_horizon_evaluated} · Warmup: {horizon.warmup_evaluated}
                  </p>
                </article>
                );
              })}
            </div>
          </section>

          <section className="decision-panel">
              <div className="decision-panel-head">
                <h3>Recent scored samples</h3>
                <p>
                  {totalEvaluatedSignals === 0
                    ? "No sample set yet for this profile/window."
                    : `Most recent evaluable Act now signals from the ${
                        sevenDayHorizon?.is_fallback ? "7d warmup fallback set" : "7d true-horizon set"
                      }.`}
                </p>
              </div>
            {totalEvaluatedSignals === 0 ? (
              <p className="decision-empty">
                No sample rows available yet. Corrective action: approve at least one trust-qualified recommendation and wait
                for a follow-up snapshot.
              </p>
            ) : (
            <ul className="decision-list">
              {(payload.horizons.find((horizon) => horizon.days === 7)?.samples ?? []).slice(0, 18).map((sample) => {
                const topic = sample.topic_id ? topicById.get(sample.topic_id) : undefined;
                return (
                  <li key={`${sample.topic_id ?? "unknown"}-${sample.event_time}`} className="decision-item">
                    <div className="decision-item-head">
                      {topic ? (
                        <button type="button" className="action-topic-link" onClick={() => onInspectTopic(topic)}>
                          {sample.label ?? sample.topic_id}
                        </button>
                      ) : (
                        <span>{sample.label ?? sample.topic_id ?? "Unknown topic"}</span>
                      )}
                      <span className={sample.momentum_persisted ? "decision-badge act-now" : "decision-badge ignore"}>
                        {sample.momentum_persisted ? "Persisted" : "Did not persist"}
                      </span>
                    </div>
                    <p className="decision-item-metrics">
                      Signal momentum {formatPercent(sample.momentum_at_signal)} {"->"} {formatPercent(sample.momentum_at_horizon)}
                    </p>
                    <p className="decision-item-metrics">
                      Proxy return: {sample.proxy_return == null ? "n/a" : formatPercent(sample.proxy_return)} · Tickers:{" "}
                      {sample.trade_tickers.length > 0 ? sample.trade_tickers.join(", ") : "none"}
                    </p>
                    <p className="decision-item-next">
                      {formatGeneratedTime(sample.event_time)} {"->"} {formatGeneratedTime(sample.followup_time)}
                    </p>
                    {sample.horizon_coverage != null ? (
                      <p className="decision-item-next">
                        Horizon coverage: {formatPercent(sample.horizon_coverage)} · Effective:{" "}
                        {sample.effective_horizon ?? "n/a"} · Mode:{" "}
                        {sample.is_fallback ? "Warmup / Next-snapshot" : sample.evaluation_mode ?? "n/a"}
                      </p>
                    ) : null}
                  </li>
                );
              })}
            </ul>
            )}
          </section>
        </div>
      ) : (
        <div className="decision-loading">
          <p>No backtest data yet.</p>
        </div>
      )}
    </section>
  );
}
