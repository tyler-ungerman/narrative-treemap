"use client";

import { formatGeneratedTime, formatPercent } from "@/lib/format";

export interface RefreshDeltaItem {
  topic_id: string;
  label: string;
  momentum: number;
  delta_momentum: number;
  volume_now: number;
}

export interface RefreshChangesSummary {
  previous_generated_at: string | null;
  generated_at: string | null;
  new_topics: number;
  dropped_topics: number;
  rising: RefreshDeltaItem[];
  cooling: RefreshDeltaItem[];
}

interface RefreshChangesPanelProps {
  summary: RefreshChangesSummary | null;
  onInspectTopic: (topicId: string) => void;
}

export function RefreshChangesPanel({ summary, onInspectTopic }: RefreshChangesPanelProps) {
  if (!summary) {
    return (
      <section className="refresh-panel">
        <header>
          <p className="hero-kicker">Since Last Refresh</p>
          <h2>Waiting for second snapshot</h2>
          <p>Once a new refresh lands, this panel will show what changed in momentum and topic composition.</p>
        </header>
      </section>
    );
  }

  return (
    <section className="refresh-panel">
      <header>
        <p className="hero-kicker">Since Last Refresh</p>
        <h2>What changed</h2>
        <p>
          Compared to {formatGeneratedTime(summary.previous_generated_at)} → {formatGeneratedTime(summary.generated_at)}
        </p>
      </header>
      <div className="refresh-summary">
        <span>New topics: {summary.new_topics}</span>
        <span>Dropped topics: {summary.dropped_topics}</span>
      </div>
      <div className="refresh-grid">
        <article>
          <h3>Rising fastest</h3>
          {summary.rising.length === 0 ? <p className="refresh-empty">No strong risers this cycle.</p> : null}
          <ul>
            {summary.rising.slice(0, 5).map((item) => (
              <li key={`rise-${item.topic_id}`}>
                <button type="button" onClick={() => onInspectTopic(item.topic_id)}>
                  {item.label}
                </button>
                <span>
                  {formatPercent(item.delta_momentum)} delta · {formatPercent(item.momentum)} now · {item.volume_now} stories
                </span>
              </li>
            ))}
          </ul>
        </article>
        <article>
          <h3>Cooling fastest</h3>
          {summary.cooling.length === 0 ? <p className="refresh-empty">No strong cool-downs this cycle.</p> : null}
          <ul>
            {summary.cooling.slice(0, 5).map((item) => (
              <li key={`cool-${item.topic_id}`}>
                <button type="button" onClick={() => onInspectTopic(item.topic_id)}>
                  {item.label}
                </button>
                <span>
                  {formatPercent(item.delta_momentum)} delta · {formatPercent(item.momentum)} now · {item.volume_now} stories
                </span>
              </li>
            ))}
          </ul>
        </article>
      </div>
    </section>
  );
}

