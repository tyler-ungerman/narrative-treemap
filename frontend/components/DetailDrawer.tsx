"use client";

import { formatPercent, formatTime } from "@/lib/format";
import { Topic } from "@/lib/types";
import { Sparkline } from "./Sparkline";

interface DetailDrawerProps {
  topic: Topic | null;
  allTopics: Topic[];
  onClose: () => void;
  onSelectTopic: (topic: Topic) => void;
}

export function DetailDrawer({ topic, allTopics, onClose, onSelectTopic }: DetailDrawerProps) {
  const relatedTopics = topic
    ? topic.related_topic_ids
        .map((relatedTopicId) => allTopics.find((candidate) => candidate.topic_id === relatedTopicId))
        .filter((candidate): candidate is Topic => Boolean(candidate))
    : [];

  return (
    <aside className={`detail-drawer ${topic ? "is-open" : ""}`} aria-hidden={!topic}>
      <div className="drawer-header">
        <div>
          <p className="drawer-kicker">Narrative Detail</p>
          <h2>{topic?.label ?? "Select a topic"}</h2>
        </div>
        <button type="button" onClick={onClose}>
          Close
        </button>
      </div>

      {topic ? (
        <div className="drawer-content">
          <div className="drawer-metrics">
            <article>
              <span>Volume</span>
              <strong>{topic.volume_now}</strong>
            </article>
            <article>
              <span>Momentum</span>
              <strong>{formatPercent(topic.momentum)}</strong>
            </article>
            <article>
              <span>Novelty</span>
              <strong>{formatPercent(topic.novelty)}</strong>
            </article>
            <article>
              <span>Diversity</span>
              <strong>{topic.diversity}</strong>
            </article>
          </div>

          <section className="drawer-section">
            <h3>Summary</h3>
            <p>{topic.summary}</p>
            <Sparkline values={topic.sparkline} className="drawer-sparkline" />
          </section>

          <section className="drawer-section">
            <h3>Top links</h3>
            <ul className="drawer-links">
              {topic.representative_items.map((item) => (
                <li key={item.url}>
                  <a href={item.url} target="_blank" rel="noreferrer">
                    {item.title}
                  </a>
                  <div>
                    <span>{item.source_name}</span>
                    <time>
                      {formatTime(item.published_at)}
                      {item.source_quality ? ` · q${item.source_quality.toFixed(2)}` : ""}
                    </time>
                  </div>
                </li>
              ))}
            </ul>
          </section>

          <section className="drawer-section">
            <h3>Keywords</h3>
            <div className="chip-wrap">
              {topic.keywords.map((keyword) => (
                <span key={keyword} className="token-chip">
                  {keyword}
                </span>
              ))}
            </div>
          </section>

          <section className="drawer-section">
            <h3>Entities</h3>
            <div className="chip-wrap">
              {topic.entities.map((entity) => (
                <span key={entity} className="token-chip entity-chip">
                  {entity}
                </span>
              ))}
            </div>
          </section>

          <section className="drawer-section">
            <h3>Related topics</h3>
            <div className="related-list">
              {relatedTopics.length === 0 ? <p>No related topics available.</p> : null}
              {relatedTopics.map((relatedTopic) => (
                <button key={relatedTopic.topic_id} type="button" onClick={() => onSelectTopic(relatedTopic)}>
                  {relatedTopic.label}
                </button>
              ))}
            </div>
          </section>
        </div>
      ) : (
        <div className="drawer-placeholder">
          <p>Click any tile to inspect representative links, entities, keywords, and nearby clusters.</p>
        </div>
      )}
    </aside>
  );
}
