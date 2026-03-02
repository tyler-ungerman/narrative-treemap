"use client";

import { useEffect, useMemo, useState } from "react";

import { fetchDecisionBriefing } from "@/lib/api";
import { formatGeneratedTime, formatPercent, formatTime } from "@/lib/format";
import {
  DecisionBriefingResponse,
  DecisionItem,
  DecisionProfile,
  Topic,
  TopicFilters
} from "@/lib/types";

interface DecisionBriefingProps {
  filters: TopicFilters;
  topics: Topic[];
  onInspectTopic: (topic: Topic) => void;
}

const PROFILE_OPTIONS: Array<{ value: DecisionProfile; label: string; description: string }> = [
  { value: "investor", label: "Investor", description: "Emphasizes momentum and coverage quality." },
  { value: "research", label: "Research", description: "Prioritizes novelty and emergent signals." },
  { value: "operations", label: "Operations", description: "Prioritizes sustained volume and execution risk." },
  { value: "security", label: "Security", description: "Prioritizes fast-moving and high-confidence threats." }
];
const TOP_N_OPTIONS = [10, 15, 20, 25, 30, 40] as const;

function parseInitialProfile(): DecisionProfile {
  if (typeof window === "undefined") {
    return "investor";
  }
  const raw = new URLSearchParams(window.location.search).get("briefing_profile");
  if (raw === "investor" || raw === "research" || raw === "operations" || raw === "security") {
    return raw;
  }
  return "investor";
}

function parseInitialTopN(): number {
  if (typeof window === "undefined") {
    return 15;
  }
  const raw = Number(new URLSearchParams(window.location.search).get("briefing_top_n"));
  if (TOP_N_OPTIONS.includes(raw as (typeof TOP_N_OPTIONS)[number])) {
    return raw;
  }
  return 15;
}

function escapeCsvCell(value: string | number): string {
  const asString = String(value);
  const escaped = asString.replaceAll("\"", "\"\"");
  return `"${escaped}"`;
}

function buildMarkdownBriefing(payload: DecisionBriefingResponse): string {
  const visibleDecisions = payload.decisions.filter((decision) => decision.action_bucket !== "Ignore");
  const lines: string[] = [
    `# Decision Briefing (${payload.summary.window})`,
    `Generated: ${formatGeneratedTime(payload.summary.generated_at)}`,
    `Profile: ${payload.summary.profile}`,
    "",
    "## Snapshot",
    `- Considered topics: ${payload.summary.considered_topics}`,
    `- Action now: ${payload.summary.actionable_now}`,
    `- Monitor: ${payload.summary.monitor_count}`,
    `- New: ${payload.summary.new_count}`,
    `- Accelerating: ${payload.summary.accelerating_count}`,
    `- Fading: ${payload.summary.fading_count}`,
    `- Approved trades: ${payload.summary.approved_trade_count}`,
    `- Blocked trades: ${payload.summary.blocked_trade_count}`,
    `- Daily notional used: ${payload.summary.used_daily_notional_pct.toFixed(2)}% / ${payload.summary.max_daily_notional_pct.toFixed(2)}%`,
    "",
    "## Priority Decisions"
  ];

  if (visibleDecisions.length === 0) {
    lines.push("No actionable decisions for the current filter and profile.");
    return lines.join("\n");
  }

  visibleDecisions.forEach((decision, index) => {
    const topLink = decision.representative_items[0];
    lines.push(`${index + 1}. ${decision.label}`);
    lines.push(`   - Action: ${decision.action_bucket}`);
    lines.push(`   - Score: ${decision.decision_score.toFixed(2)}`);
    lines.push(
      `   - Momentum: ${formatPercent(decision.momentum)} | Novelty: ${formatPercent(decision.novelty)} | Sources: ${decision.diversity}`
    );
    lines.push(
      `   - Market impact: ${Math.round(decision.market_impact_score * 100)}% (economic ${Math.round(decision.economic_relevance * 100)}% | surprise ${Math.round(decision.surprise_score * 100)}% | liquidity ${Math.round(decision.liquidity_linkage * 100)}%)`
    );
    lines.push(`   - Acting on: ${decision.signal_statement.replace("Acting on: ", "")}`);
    lines.push(`   - Why it matters: ${decision.rationale}`);
    lines.push(`   - Execution: ${decision.execution_plan}`);
    if (decision.trade_tickers.length > 0) {
      lines.push(`   - Trade: ${decision.trade_direction} ${decision.trade_tickers.join(", ")}`);
    } else {
      lines.push(`   - Trade: ${decision.trade_direction}`);
    }
    if (decision.asset_mapping) {
      lines.push(
        `   - Mapping: ${decision.asset_mapping.theme_name} (${Math.round(decision.asset_mapping.confidence * 100)}%)`
      );
      lines.push(`   - Evidence: ${decision.asset_mapping.evidence_summary}`);
    }
    lines.push(`   - Risk: ${decision.risk_guardrail}`);
    lines.push(
      `   - Reliability: ${Math.round(decision.signal_reliability * 100)}% | Trade confidence: ${Math.round(decision.trade_confidence * 100)}%`
    );
    lines.push(
      `   - Portfolio: status=${decision.execution_status} | approved=${decision.approved_notional_pct.toFixed(2)}% | proposed=${decision.proposed_notional_pct.toFixed(2)}%`
    );
    if (decision.risk_block_reason) {
      lines.push(`   - Block reason: ${decision.risk_block_reason}`);
    }
    lines.push(`   - Baseline step: ${decision.next_step}`);
    if (topLink) {
      lines.push(`   - Link: ${topLink.title} (${topLink.url})`);
    }
  });

  return lines.join("\n");
}

function buildCsvBriefing(payload: DecisionBriefingResponse): string {
  const visibleDecisions = payload.decisions.filter((decision) => decision.action_bucket !== "Ignore");
  const header = [
    "rank",
    "topic_id",
    "label",
    "vertical",
    "action_bucket",
    "signal_statement",
    "execution_plan",
    "trade_direction",
    "trade_tickers",
    "trade_theme",
    "execution_status",
    "risk_block_reason",
    "proposed_notional_pct",
    "approved_notional_pct",
    "risk_guardrail",
    "signal_reliability",
    "trade_confidence",
    "asset_theme",
    "asset_confidence",
    "asset_evidence",
    "market_impact_score",
    "economic_relevance",
    "surprise_score",
    "liquidity_linkage",
    "impact_verdict",
    "impact_reason",
    "decision_score",
    "momentum",
    "novelty",
    "weighted_volume_now",
    "weighted_volume_prev",
    "diversity",
    "label_confidence",
    "source_quality_score",
    "top_source",
    "top_title",
    "top_url"
  ];
  const rows = visibleDecisions.map((decision, index) => {
    const topItem = decision.representative_items[0];
    return [
      index + 1,
      decision.topic_id,
      decision.label,
      decision.vertical,
      decision.action_bucket,
      decision.signal_statement,
      decision.execution_plan,
      decision.trade_direction,
      decision.trade_tickers.join(" "),
      decision.trade_theme ?? "",
      decision.execution_status,
      decision.risk_block_reason ?? "",
      decision.proposed_notional_pct.toFixed(2),
      decision.approved_notional_pct.toFixed(2),
      decision.risk_guardrail,
      decision.signal_reliability.toFixed(4),
      decision.trade_confidence.toFixed(4),
      decision.asset_mapping?.theme_name ?? "",
      decision.asset_mapping ? decision.asset_mapping.confidence.toFixed(4) : "",
      decision.asset_mapping?.evidence_summary ?? "",
      decision.market_impact_score.toFixed(4),
      decision.economic_relevance.toFixed(4),
      decision.surprise_score.toFixed(4),
      decision.liquidity_linkage.toFixed(4),
      decision.impact_verdict,
      decision.impact_reason,
      decision.decision_score.toFixed(4),
      decision.momentum.toFixed(4),
      decision.novelty.toFixed(4),
      decision.weighted_volume_now.toFixed(4),
      decision.weighted_volume_prev.toFixed(4),
      decision.diversity,
      decision.label_confidence.toFixed(4),
      decision.source_quality_score.toFixed(4),
      topItem?.source_name ?? "",
      topItem?.title ?? "",
      topItem?.url ?? ""
    ];
  });
  return [header, ...rows]
    .map((row) => row.map((cell) => escapeCsvCell(cell)).join(","))
    .join("\n");
}

function badgeClass(bucket: DecisionItem["action_bucket"]): string {
  if (bucket === "Act now") return "decision-badge act-now";
  if (bucket === "Monitor") return "decision-badge monitor";
  return "decision-badge ignore";
}

function sourceWord(count: number): string {
  return count === 1 ? "source" : "sources";
}

function isUnresolvedDecision(decision: DecisionItem): boolean {
  return (
    decision.label.toLowerCase().startsWith("unresolved ") ||
    decision.trust_contract.warnings.some((warning) => warning.toLowerCase().includes("label alignment"))
  );
}

export function DecisionBriefing({ filters, topics, onInspectTopic }: DecisionBriefingProps) {
  const [profile, setProfile] = useState<DecisionProfile>(parseInitialProfile);
  const [topN, setTopN] = useState<number>(parseInitialTopN);
  const [briefing, setBriefing] = useState<DecisionBriefingResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");
  const [shareState, setShareState] = useState<"idle" | "copied" | "failed">("idle");

  useEffect(() => {
    let cancelled = false;
    const loadBriefing = async () => {
      if (!briefing) {
        setLoading(true);
      } else {
        setRefreshing(true);
      }
      setError(null);
      try {
        const payload = await fetchDecisionBriefing(filters, profile, topN);
        if (cancelled) return;
        setBriefing(payload);
      } catch (fetchError) {
        if (cancelled) return;
        setError(fetchError instanceof Error ? fetchError.message : "Unable to load decision briefing");
      } finally {
        if (cancelled) return;
        setLoading(false);
        setRefreshing(false);
      }
    };

    void loadBriefing();
    return () => {
      cancelled = true;
    };
  }, [filters, profile, topN]);

  useEffect(() => {
    if (typeof window === "undefined") return;
    const url = new URL(window.location.href);
    url.searchParams.set("view", "briefing");
    url.searchParams.set("briefing_profile", profile);
    url.searchParams.set("briefing_top_n", String(topN));
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, [profile, topN]);

  const topicById = useMemo(() => new Map(topics.map((topic) => [topic.topic_id, topic])), [topics]);
  const visibleDecisions = useMemo(
    () => briefing?.decisions.filter((decision) => decision.action_bucket !== "Ignore") ?? [],
    [briefing]
  );

  const copyMarkdown = async () => {
    if (!briefing || typeof navigator === "undefined" || !navigator.clipboard?.writeText) {
      setCopyState("failed");
      window.setTimeout(() => setCopyState("idle"), 2000);
      return;
    }
    try {
      await navigator.clipboard.writeText(buildMarkdownBriefing(briefing));
      setCopyState("copied");
    } catch {
      setCopyState("failed");
    } finally {
      window.setTimeout(() => setCopyState("idle"), 2200);
    }
  };

  const copyShareLink = async () => {
    if (typeof window === "undefined" || typeof navigator === "undefined" || !navigator.clipboard?.writeText) {
      setShareState("failed");
      if (typeof window !== "undefined") {
        window.setTimeout(() => setShareState("idle"), 2200);
      }
      return;
    }
    try {
      const url = new URL(window.location.href);
      url.searchParams.set("view", "briefing");
      url.searchParams.set("briefing_profile", profile);
      url.searchParams.set("briefing_top_n", String(topN));
      await navigator.clipboard.writeText(url.toString());
      setShareState("copied");
    } catch {
      setShareState("failed");
    } finally {
      window.setTimeout(() => setShareState("idle"), 2200);
    }
  };

  const downloadCsv = () => {
    if (!briefing || typeof window === "undefined") return;
    const csv = buildCsvBriefing(briefing);
    const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
    const url = window.URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = `decision-briefing-${briefing.summary.window}-${briefing.summary.profile}.csv`;
    document.body.appendChild(anchor);
    anchor.click();
    anchor.remove();
    window.URL.revokeObjectURL(url);
  };

  const openTopic = (topicId: string) => {
    const topic = topicById.get(topicId);
    if (topic) {
      onInspectTopic(topic);
    }
  };

  return (
    <section className="decision-briefing">
      <header className="decision-header">
        <div>
          <p className="hero-kicker">Decision Briefing</p>
          <h2>Prioritized narrative actions</h2>
          <p>
            Converts narrative clusters into explicit decisions with role-based scoring, change detection, and
            shareable outputs.
          </p>
        </div>
        <div className="decision-header-meta">
          <p>Status: {refreshing ? "Refreshing..." : "Ready"}</p>
          <p>Topics considered: {briefing?.summary.considered_topics ?? 0}</p>
          <p>Window: {filters.window}</p>
          <p>Generated: {briefing ? formatGeneratedTime(briefing.summary.generated_at) : "n/a"}</p>
        </div>
      </header>

      <div className="decision-controls">
        <div className="decision-profile-group">
          {PROFILE_OPTIONS.map((option) => (
            <button
              key={option.value}
              type="button"
              className={profile === option.value ? "is-selected" : ""}
              onClick={() => setProfile(option.value)}
              title={option.description}
            >
              {option.label}
            </button>
          ))}
        </div>
        <label className="decision-topn-control">
          Top decisions
          <select value={topN} onChange={(event) => setTopN(Number(event.target.value))}>
            {TOP_N_OPTIONS.map((value) => (
              <option key={value} value={value}>
                {value}
              </option>
            ))}
          </select>
        </label>
      </div>

      {error ? <p className="error-banner">{error}</p> : null}

      {loading ? (
        <div className="decision-loading">
          <p>Building decision briefing...</p>
        </div>
      ) : briefing ? (
        <div className="decision-grid">
          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Change summary</h3>
              <p>
                {briefing.summary.previous_generated_at
                  ? `Compared to ${formatGeneratedTime(briefing.summary.previous_generated_at)}`
                  : "No previous snapshot yet. Refresh after the next data cycle to track deltas."}
              </p>
            </div>
            <div className="decision-change-chips">
              <span>New: {briefing.summary.new_count}</span>
              <span>Accelerating: {briefing.summary.accelerating_count}</span>
              <span>Fading: {briefing.summary.fading_count}</span>
              <span>Act now: {briefing.summary.actionable_now}</span>
              <span>Monitor: {briefing.summary.monitor_count}</span>
              <span>Approved: {briefing.summary.approved_trade_count}</span>
              <span>Blocked: {briefing.summary.blocked_trade_count}</span>
            </div>
            <p className="decision-item-next">
              Lifecycle: {briefing.summary.lifecycle.candidates} candidates → {briefing.summary.lifecycle.considered} considered
              → {briefing.summary.lifecycle.recommended} recommended → {briefing.summary.lifecycle.approved} approved
              → {briefing.summary.lifecycle.opened} opened → {briefing.summary.lifecycle.closed} closed /{" "}
              {briefing.summary.lifecycle.expired} expired
            </p>
            {briefing.summary.lifecycle.explanations.length > 0 ? (
              <div className="decision-item-next">
                <p>{briefing.summary.lifecycle.explanations[0]}</p>
                {briefing.summary.lifecycle.corrective_ctas[0] ? (
                  <p>
                    Corrective action: <strong>{briefing.summary.lifecycle.corrective_ctas[0]}</strong>
                  </p>
                ) : null}
              </div>
            ) : null}
            <p className="decision-risk-strip">
              Risk caps: {briefing.risk_controls.used_daily_notional_pct.toFixed(2)}% /{" "}
              {briefing.risk_controls.max_daily_notional_pct.toFixed(2)}% notional ·{" "}
              {briefing.risk_controls.active_themes_count}/{briefing.risk_controls.max_simultaneous_themes} themes ·{" "}
              cooldown {briefing.risk_controls.cooldown_hours}h
            </p>
            <div className="decision-changes-grid">
              <article>
                <h4>New narratives</h4>
                <ul>
                  {briefing.changes.new_narratives.slice(0, 6).map((change) => (
                    <li key={change.topic_id}>
                      <button type="button" onClick={() => openTopic(change.topic_id)}>
                        {change.label}
                      </button>
                    </li>
                  ))}
                  {briefing.changes.new_narratives.length === 0 ? <li className="decision-empty-line">None</li> : null}
                </ul>
              </article>
              <article>
                <h4>Accelerating</h4>
                <ul>
                  {briefing.changes.accelerating.slice(0, 6).map((change) => (
                    <li key={change.topic_id}>
                      <button type="button" onClick={() => openTopic(change.topic_id)}>
                        {change.label}
                      </button>
                    </li>
                  ))}
                  {briefing.changes.accelerating.length === 0 ? (
                    <li className="decision-empty-line">None</li>
                  ) : null}
                </ul>
              </article>
              <article>
                <h4>Fading</h4>
                <ul>
                  {briefing.changes.fading.slice(0, 6).map((change) => (
                    <li key={change.topic_id}>
                      <button type="button" onClick={() => openTopic(change.topic_id)}>
                        {change.label}
                      </button>
                    </li>
                  ))}
                  {briefing.changes.fading.length === 0 ? <li className="decision-empty-line">None</li> : null}
                </ul>
              </article>
            </div>
          </section>

          <section className="decision-panel decision-priority-panel">
            <div className="decision-panel-head">
              <h3>Priority decisions</h3>
              <p>Sorted by profile-specific decision score.</p>
            </div>
            {visibleDecisions.length === 0 ? (
              <p className="decision-empty">No actionable decisions for the current filters.</p>
            ) : (
              <ul className="decision-list">
                {visibleDecisions.map((decision) => {
                  const topItem = decision.representative_items[0];
                  const unresolved = isUnresolvedDecision(decision);
                  return (
                    <li key={decision.topic_id} className="decision-item">
                      <div className="decision-item-head">
                        <button type="button" className="action-topic-link" onClick={() => openTopic(decision.topic_id)}>
                          {decision.label}
                        </button>
                        <span className={badgeClass(decision.action_bucket)}>{decision.action_bucket}</span>
                      </div>
                      <p className="decision-item-metrics">
                        Score {decision.decision_score.toFixed(2)} · {formatPercent(decision.momentum)} momentum ·{" "}
                        {formatPercent(decision.novelty)} novelty · {decision.diversity} {sourceWord(decision.diversity)} ·
                        impact {Math.round(decision.market_impact_score * 100)}%
                      </p>
                      <p className="decision-item-signal">{decision.signal_statement}</p>
                      <p className="decision-item-rationale">{decision.rationale}</p>
                      <p className="decision-item-risk">
                        <strong>Impact gate:</strong> {decision.impact_verdict} · economic{" "}
                        {Math.round(decision.economic_relevance * 100)}% · surprise{" "}
                        {Math.round(decision.surprise_score * 100)}% · liquidity{" "}
                        {Math.round(decision.liquidity_linkage * 100)}%
                      </p>
                      <p className="decision-item-next">{decision.impact_reason}</p>
                      <p className="decision-item-execution">
                        <strong>Execution:</strong> {decision.execution_plan}
                      </p>
                      <p className="decision-item-execution">
                        <strong>Trade:</strong>{" "}
                        {decision.trade_tickers.length > 0
                          ? `${decision.trade_direction} ${decision.trade_tickers.join(", ")}`
                          : decision.trade_direction}
                      </p>
                      <p className="decision-item-risk">
                        <strong>Risk:</strong> {decision.risk_guardrail}
                      </p>
                      {decision.asset_mapping ? (
                        <div className="decision-item-explainability">
                          <p className="decision-item-risk">
                            <strong>Narrative-to-asset:</strong> {decision.asset_mapping.theme_name} (
                            {Math.round(decision.asset_mapping.confidence * 100)}%)
                          </p>
                          <p className="decision-item-next">{decision.asset_mapping.evidence_summary}</p>
                          <p className="decision-item-next">
                            Tokens: {decision.asset_mapping.matched_tokens.slice(0, 5).join(", ") || "n/a"} ·
                            Phrases: {decision.asset_mapping.matched_phrases.slice(0, 3).join(", ") || "n/a"}
                          </p>
                        </div>
                      ) : null}
                      <p className="decision-item-risk">
                        <strong>Reliability:</strong> {Math.round(decision.signal_reliability * 100)}% ·{" "}
                        <strong>Trade confidence:</strong> {Math.round(decision.trade_confidence * 100)}%
                      </p>
                      {decision.trust_contract.warnings.length > 0 ? (
                        <p className="decision-item-risk">
                          <strong>Trust warnings:</strong> {decision.trust_contract.warnings.join(" | ")}
                        </p>
                      ) : null}
                      <p className="decision-item-risk">
                        <strong>Portfolio:</strong> {decision.execution_status} · approved{" "}
                        {decision.approved_notional_pct.toFixed(2)}% / proposed {decision.proposed_notional_pct.toFixed(2)}%
                        {decision.risk_block_reason ? ` · ${decision.risk_block_reason}` : ""}
                      </p>
                      <p className="decision-item-next">{decision.next_step}</p>
                      {unresolved ? (
                        <div className="decision-item-link">
                          {decision.representative_items.slice(0, 3).map((item) => (
                            <span key={`${decision.topic_id}-${item.url}`}>
                              <a href={item.url} target="_blank" rel="noreferrer">
                                {item.title}
                              </a>
                              <span>
                                {item.source_name} · {formatTime(item.published_at)}
                              </span>
                            </span>
                          ))}
                        </div>
                      ) : topItem ? (
                        <p className="decision-item-link">
                          <a href={topItem.url} target="_blank" rel="noreferrer">
                            {topItem.title}
                          </a>
                          <span>
                            {topItem.source_name} · {formatTime(topItem.published_at)}
                          </span>
                        </p>
                      ) : null}
                    </li>
                  );
                })}
              </ul>
            )}
          </section>

          <aside className="decision-panel decision-export-panel">
            <div className="decision-panel-head">
              <h3>Export and share</h3>
              <p>Send a concise briefing to notes, docs, or chat.</p>
            </div>
            <div className="decision-export-controls">
              <button type="button" onClick={copyMarkdown} disabled={visibleDecisions.length === 0}>
                Copy markdown
              </button>
              <button type="button" onClick={downloadCsv} disabled={visibleDecisions.length === 0}>
                Download CSV
              </button>
              <button type="button" onClick={copyShareLink}>
                Copy share link
              </button>
              <span>
                {copyState === "copied"
                  ? "Brief copied"
                  : copyState === "failed"
                    ? "Brief copy failed"
                    : shareState === "copied"
                      ? "Share link copied"
                      : shareState === "failed"
                        ? "Share copy failed"
                        : ""}
              </span>
            </div>
            <p className="decision-export-note">
              Profile: <strong>{briefing.summary.profile}</strong> · Top N: <strong>{briefing.summary.top_n}</strong>
            </p>
            <p className="decision-export-note">
              Algorithm: <strong>{briefing.metadata.algorithm}</strong>
            </p>
            <p className="decision-export-note">
              Caps: <strong>{briefing.risk_controls.max_simultaneous_themes}</strong> themes ·{" "}
              <strong>{briefing.risk_controls.max_position_notional_pct.toFixed(2)}%</strong> max position ·{" "}
              <strong>{briefing.risk_controls.max_new_trades_per_day}</strong> new trades/day
            </p>
          </aside>
        </div>
      ) : (
        <div className="decision-loading">
          <p>No briefing available.</p>
        </div>
      )}
    </section>
  );
}
