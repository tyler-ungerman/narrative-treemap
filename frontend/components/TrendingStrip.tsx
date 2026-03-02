"use client";

import { Topic } from "@/lib/types";

interface TrendingStripProps {
  topics: Topic[];
  onSelectTopic: (topic: Topic) => void;
}

function trendingScore(topic: Topic): number {
  const weightedVolume = topic.weighted_volume_now ?? topic.volume_now;
  const qualityScore = topic.source_quality_score ?? 1;
  return topic.momentum * Math.log(weightedVolume + 1) * qualityScore;
}

export function TrendingStrip({ topics, onSelectTopic }: TrendingStripProps) {
  const topTopics = [...topics]
    .sort((left, right) => trendingScore(right) - trendingScore(left))
    .slice(0, 5);

  return (
    <section className="trending-strip" aria-label="Trending narratives">
      <h2>Trending narratives</h2>
      <div className="trending-items">
        {topTopics.length === 0 ? <p>No active trends yet.</p> : null}
        {topTopics.map((topic, index) => (
          <button key={topic.topic_id} type="button" onClick={() => onSelectTopic(topic)}>
            <span className="trend-rank">{index + 1}</span>
            <span className="trend-label">{topic.label}</span>
            <span className="trend-momentum">{(topic.momentum * 100).toFixed(0)}%</span>
          </button>
        ))}
      </div>
    </section>
  );
}
