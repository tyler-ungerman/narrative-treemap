"use client";

import { useEffect, useMemo, useState } from "react";

import { formatPercent, formatTime } from "@/lib/format";
import { LifecycleModel, Topic, WindowOption } from "@/lib/types";

interface ActionCenterProps {
  topics: Topic[];
  timeWindow: WindowOption;
  generatedAt: string | null;
  lifecycle?: LifecycleModel | null;
  onInspectTopic: (topic: Topic) => void;
}

const WATCHLIST_STORAGE_KEY = "narrative_treemap_watchlist_v1";

function actionScore(topic: Topic): number {
  const weightedVolume = topic.weighted_volume_now ?? topic.volume_now;
  const quality = topic.source_quality_score ?? 1;
  const confidence = topic.label_confidence ?? 0.45;

  return (
    Math.log(weightedVolume + 1) * 0.9 +
    topic.momentum * 1.65 +
    topic.novelty * 1.15 +
    topic.diversity * 0.08 +
    quality * 0.55 +
    confidence * 0.35
  );
}

function suggestedAction(topic: Topic): { label: string; rationale: string } {
  const trust = topic.trust_contract;
  if (topic.momentum >= 0.45 && topic.novelty >= 0.3) {
    if (trust && !trust.eligible_for_act_now) {
      return {
        label: "Monitor",
        rationale: trust.warnings[0] ?? "blocked by trust contract"
      };
    }
    return {
      label: "Act now",
      rationale: "rapidly rising and still novel"
    };
  }
  if (topic.momentum >= 0.18 && topic.diversity >= 4) {
    return {
      label: "Monitor",
      rationale: "building momentum across multiple sources"
    };
  }
  if (topic.momentum <= -0.3) {
    return {
      label: "De-prioritize",
      rationale: "cooling relative to the previous window"
    };
  }
  return {
    label: "Track",
    rationale: "signal is present but not urgent"
  };
}

function isUnresolvedNarrative(topic: Topic): boolean {
  return topic.label.toLowerCase().startsWith("unresolved ");
}

function buildBriefing(topics: Topic[], timeWindow: WindowOption, generatedAt: string | null): string {
  const header = [
    `Narrative Action Briefing (${timeWindow})`,
    `Generated: ${generatedAt ? new Date(generatedAt).toLocaleString() : "n/a"}`
  ];

  const lines = topics.map((topic, index) => {
    const action = suggestedAction(topic);
    const topItem = topic.representative_items[0];
    const score = actionScore(topic).toFixed(2);
    return [
      `${index + 1}. ${topic.label}`,
      `   Action: ${action.label} (${action.rationale})`,
      `   Momentum: ${formatPercent(topic.momentum)} | Novelty: ${formatPercent(topic.novelty)} | Sources: ${topic.diversity} | Score: ${score}`,
      topItem ? `   Link: ${topItem.title} (${topItem.url})` : "   Link: n/a"
    ].join("\n");
  });

  return [...header, "", ...lines].join("\n");
}

export function ActionCenter({ topics, timeWindow, generatedAt, lifecycle, onInspectTopic }: ActionCenterProps) {
  const [watchlistIds, setWatchlistIds] = useState<string[]>([]);
  const [copyState, setCopyState] = useState<"idle" | "copied" | "failed">("idle");

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    try {
      const saved = window.localStorage.getItem(WATCHLIST_STORAGE_KEY);
      if (!saved) return;
      const parsed = JSON.parse(saved);
      if (Array.isArray(parsed)) {
        setWatchlistIds(parsed.filter((value): value is string => typeof value === "string"));
      }
    } catch {
      setWatchlistIds([]);
    }
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }
    window.localStorage.setItem(WATCHLIST_STORAGE_KEY, JSON.stringify(watchlistIds));
  }, [watchlistIds]);

  const rankedTopics = useMemo(() => {
    return [...topics].sort((left, right) => actionScore(right) - actionScore(left)).slice(0, 40);
  }, [topics]);

  const watchlistTopics = useMemo(() => {
    const byId = new Map(topics.map((topic) => [topic.topic_id, topic]));
    return watchlistIds.map((topicId) => byId.get(topicId)).filter((topic): topic is Topic => Boolean(topic));
  }, [topics, watchlistIds]);

  const toggleWatchlist = (topicId: string) => {
    setWatchlistIds((previous) => {
      if (previous.includes(topicId)) {
        return previous.filter((id) => id !== topicId);
      }
      return [...previous, topicId];
    });
  };

  const copyBriefing = async () => {
    const payload = buildBriefing(watchlistTopics, timeWindow, generatedAt);
    if (typeof navigator === "undefined" || !navigator.clipboard?.writeText) {
      setCopyState("failed");
      return;
    }
    try {
      await navigator.clipboard.writeText(payload);
      setCopyState("copied");
      window.setTimeout(() => setCopyState("idle"), 2200);
    } catch {
      setCopyState("failed");
      window.setTimeout(() => setCopyState("idle"), 2200);
    }
  };

  return (
    <section className="action-center">
      <header className="action-header">
        <div>
          <p className="hero-kicker">Action Center</p>
          <h2>Turn narratives into decisions</h2>
          <p>
            Prioritized queue, persistent watchlist, and one-click briefing export. Use this panel to decide what to
            investigate now versus monitor later.
          </p>
        </div>
        <div className="action-header-meta">
          <p>Tracked: {watchlistTopics.length}</p>
          <p>Queue: {rankedTopics.length} of {topics.length}</p>
        </div>
      </header>
      {lifecycle ? (
        <p className="decision-item-next">
          Lifecycle: {lifecycle.candidates} candidates → {lifecycle.considered} considered → {lifecycle.recommended}{" "}
          recommended → {lifecycle.approved} approved
        </p>
      ) : null}
      {lifecycle?.explanations?.length ? (
        <p className="decision-item-next">
          {lifecycle.explanations[0]} {lifecycle.corrective_ctas?.[0] ? `Corrective action: ${lifecycle.corrective_ctas[0]}` : ""}
        </p>
      ) : null}
      {rankedTopics.length === 0 ? (
        <p className="decision-item-next">
          Queue is empty for this filter scope. Corrective action: expand sources or clear search to repopulate candidates.
        </p>
      ) : null}

      <div className="action-grid">
        <section className="action-panel">
          <div className="action-panel-head">
            <h3>Priority queue</h3>
            <p>Top 40 ranked by momentum, novelty, source quality, and cross-source spread.</p>
          </div>
          <ul className="action-list">
            {rankedTopics.map((topic) => {
              const action = suggestedAction(topic);
              const tracked = watchlistIds.includes(topic.topic_id);
              const trustWarnings = topic.trust_contract?.warnings ?? [];
              const unresolved = isUnresolvedNarrative(topic);
              return (
                <li key={topic.topic_id} className="action-item">
                  <div className="action-item-main">
                    <button type="button" className="action-topic-link" onClick={() => onInspectTopic(topic)}>
                      {topic.label}
                    </button>
                    <p className="action-item-meta">
                      {action.label} · {action.rationale}
                    </p>
                    <div className="action-item-metrics">
                      <span>{topic.volume_now} stories</span>
                      <span>{formatPercent(topic.momentum)}</span>
                      <span>{formatPercent(topic.novelty)} novelty</span>
                      <span>{topic.diversity} sources</span>
                    </div>
                    {trustWarnings.length > 0 ? (
                      <p className="decision-item-next">Trust warnings: {trustWarnings.slice(0, 2).join(" | ")}</p>
                    ) : null}
                    {unresolved ? (
                      <p className="decision-item-next">
                        Evidence links:{" "}
                        {topic.representative_items.slice(0, 3).map((item, index) => (
                          <span key={`${topic.topic_id}-${item.url}`}>
                            {index > 0 ? " · " : ""}
                            <a href={item.url} target="_blank" rel="noreferrer">
                              {item.source_name}
                            </a>
                          </span>
                        ))}
                      </p>
                    ) : null}
                  </div>
                  <div className="action-item-controls">
                    <button type="button" onClick={() => toggleWatchlist(topic.topic_id)}>
                      {tracked ? "Untrack" : "Track"}
                    </button>
                    {!unresolved && topic.representative_items[0] ? (
                      <a href={topic.representative_items[0].url} target="_blank" rel="noreferrer">
                        Open
                      </a>
                    ) : null}
                  </div>
                </li>
              );
            })}
            {rankedTopics.length === 0 ? <li className="decision-empty">No candidates in the current queue.</li> : null}
          </ul>
        </section>

        <aside className="action-panel action-watchlist">
          <div className="action-panel-head">
            <h3>Watchlist</h3>
            <p>Persisted locally in this browser.</p>
          </div>

          <div className="watchlist-controls">
            <button type="button" onClick={copyBriefing} disabled={watchlistTopics.length === 0}>
              Copy briefing
            </button>
            <button type="button" onClick={() => setWatchlistIds([])} disabled={watchlistTopics.length === 0}>
              Clear watchlist
            </button>
            <span className="watchlist-feedback">
              {copyState === "copied" ? "Copied" : copyState === "failed" ? "Copy failed" : ""}
            </span>
          </div>

          {watchlistTopics.length === 0 ? (
            <p className="watchlist-empty">Track narratives from the queue to build a focused monitoring list.</p>
          ) : (
            <ul className="watchlist-list">
              {watchlistTopics.map((topic) => {
                const topItem = topic.representative_items[0];
                return (
                  <li key={topic.topic_id}>
                    <button type="button" className="action-topic-link" onClick={() => onInspectTopic(topic)}>
                      {topic.label}
                    </button>
                    <p>
                      {suggestedAction(topic).label} · {formatPercent(topic.momentum)} momentum · {topic.diversity}{" "}
                      sources
                    </p>
                    {topItem ? (
                      <p className="watchlist-link">
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
        </aside>
      </div>
    </section>
  );
}
