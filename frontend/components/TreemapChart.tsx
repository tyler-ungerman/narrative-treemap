"use client";

import { hierarchy, treemap } from "d3-hierarchy";
import { CSSProperties, useEffect, useMemo, useRef, useState } from "react";

import { formatPercent } from "@/lib/format";
import { Topic, TreemapDensityOption } from "@/lib/types";
import { Sparkline } from "./Sparkline";

interface TreemapChartProps {
  topics: Topic[];
  selectedTopicId: string | null;
  density: TreemapDensityOption;
  renderMode?: "2d" | "3d";
  legendNote?: string | null;
  onSelectTopic: (topic: Topic) => void;
}

interface TreemapSize {
  width: number;
  height: number;
}

interface TileDatum {
  topic: Topic;
  x0: number;
  x1: number;
  y0: number;
  y1: number;
}

type TreemapLeaf = Topic & { value: number };
type TreemapNode = { name: string; children: TreemapLeaf[] } | TreemapLeaf;
type MomentumScale = {
  low: number;
  median: number;
  high: number;
};

const DENSITY_LIMITS: Record<TreemapDensityOption, number> = {
  readable: 120,
  expanded: 220,
  all: Number.POSITIVE_INFINITY
};
const MAX_PRUNE_ITERATIONS = 7;
const MIN_VISIBLE_TOPICS = 10;
const TREEMAP_TOP_INSET = 52;

function clamp(value: number, min: number, max: number): number {
  return Math.max(min, Math.min(value, max));
}

function blendHex(start: string, end: string, ratio: number): string {
  const toRgb = (hex: string) => {
    const normalized = hex.replace("#", "");
    return {
      r: Number.parseInt(normalized.slice(0, 2), 16),
      g: Number.parseInt(normalized.slice(2, 4), 16),
      b: Number.parseInt(normalized.slice(4, 6), 16)
    };
  };
  const startRgb = toRgb(start);
  const endRgb = toRgb(end);
  const mix = (a: number, b: number) => Math.round(a + (b - a) * ratio);
  const r = mix(startRgb.r, endRgb.r).toString(16).padStart(2, "0");
  const g = mix(startRgb.g, endRgb.g).toString(16).padStart(2, "0");
  const b = mix(startRgb.b, endRgb.b).toString(16).padStart(2, "0");
  return `#${r}${g}${b}`;
}

function momentumColor(normalizedMomentum: number): string {
  const clamped = clamp(normalizedMomentum, -1, 1);
  const normalized = (clamped + 1) / 2;
  if (normalized < 0.5) {
    return blendHex("#1d4ed8", "#94a3b8", normalized / 0.5);
  }
  return blendHex("#94a3b8", "#ea580c", (normalized - 0.5) / 0.5);
}

function buildMomentumScale(topics: Topic[]): MomentumScale {
  const sorted = [...topics]
    .map((topic) => topic.momentum)
    .sort((left, right) => left - right);
  if (sorted.length === 0) {
    return { low: -1, median: 0, high: 1 };
  }

  const at = (ratio: number) => {
    const index = Math.floor((sorted.length - 1) * ratio);
    return sorted[index];
  };
  return {
    low: at(0.15),
    median: at(0.5),
    high: at(0.85)
  };
}

function normalizeMomentum(momentum: number, scale: MomentumScale): number {
  const upperSpread = Math.max(scale.high - scale.median, 0.25);
  const lowerSpread = Math.max(scale.median - scale.low, 0.25);
  if (momentum >= scale.median) {
    return clamp((momentum - scale.median) / upperSpread, -1, 1);
  }
  return clamp((momentum - scale.median) / lowerSpread, -1, 1);
}

function heatLabel(relativeMomentum: number): string {
  const clamped = clamp(relativeMomentum, -1, 1);
  if (clamped > 0.7) {
    return "Hot";
  }
  if (clamped > 0.25) {
    return "Warming";
  }
  if (clamped < -0.7) {
    return "Cool";
  }
  if (clamped < -0.25) {
    return "Cooling";
  }
  return "Stable";
}

function shortLabel(label: string, maxWords = 4): string {
  const compact = label.includes(":") ? label.split(":").slice(1).join(":").trim() : label;
  const words = compact.split(/\s+/).filter(Boolean);
  if (words.length <= maxWords) {
    return compact;
  }
  return `${words.slice(0, maxWords).join(" ")}...`;
}

function topicPriority(topic: Topic): number {
  const weightedVolume = topic.weighted_volume_now ?? topic.volume_now;
  const quality = topic.source_quality_score ?? 1;
  return (
    Math.log(weightedVolume + 1) * 0.75 +
    topic.momentum * 1.45 +
    topic.novelty * 0.8 +
    topic.diversity * 0.08 +
    quality * 0.45
  );
}

function minimumReadableArea(size: TreemapSize, density: TreemapDensityOption): number {
  const totalArea = Math.max(size.width * size.height, 1);
  const baseRatio = density === "all" ? 0.0024 : density === "expanded" ? 0.0029 : 0.0034;
  const maxArea = density === "all" ? 3000 : density === "expanded" ? 3600 : 4400;
  return clamp(Math.floor(totalArea * baseRatio), 1700, maxArea);
}

function minimumReadableWidth(density: TreemapDensityOption): number {
  if (density === "all") {
    return 105;
  }
  if (density === "expanded") {
    return 118;
  }
  return 130;
}

function minimumReadableHeight(density: TreemapDensityOption): number {
  if (density === "all") {
    return 72;
  }
  if (density === "expanded") {
    return 78;
  }
  return 84;
}

function layoutTiles(topics: Topic[], size: TreemapSize, topInset: number): TileDatum[] {
  if (!topics.length) {
    return [];
  }

  const layoutHeight = Math.max(size.height - topInset, 220);

  const root = hierarchy<TreemapNode>({
    name: "root",
    children: topics.map((topic) => ({
      ...topic,
      value: Math.max(topic.weighted_volume_now ?? topic.volume_now, 1)
    }))
  })
    .sum((node: TreemapNode) => {
      return "value" in node ? node.value : 0;
    })
    .sort((left, right) => (right.value ?? 0) - (left.value ?? 0));

  const layout = treemap<TreemapNode>()
    .size([size.width, layoutHeight])
    .paddingOuter(10)
    .paddingInner(6)
    .round(true);
  const layoutRoot = layout(root);

  return layoutRoot
    .leaves()
    .map((leaf) => ({
      topic: leaf.data as Topic,
      x0: leaf.x0,
      x1: leaf.x1,
      y0: leaf.y0 + topInset,
      y1: leaf.y1 + topInset
    }))
    .filter((tile) => tile.x1 - tile.x0 > 8 && tile.y1 - tile.y0 > 8);
}

function isUnreadableTile(
  tile: TileDatum,
  density: TreemapDensityOption,
  minimumArea: number
): boolean {
  const width = tile.x1 - tile.x0;
  const height = tile.y1 - tile.y0;
  const area = width * height;
  return (
    area < minimumArea ||
    width < minimumReadableWidth(density) ||
    height < minimumReadableHeight(density)
  );
}

function dedupeTopicsById(topics: Topic[]): Topic[] {
  const seen = new Set<string>();
  const result: Topic[] = [];
  topics.forEach((topic) => {
    if (seen.has(topic.topic_id)) {
      return;
    }
    seen.add(topic.topic_id);
    result.push(topic);
  });
  return result;
}

export function TreemapChart({
  topics,
  selectedTopicId,
  density,
  renderMode = "2d",
  legendNote,
  onSelectTopic
}: TreemapChartProps) {
  const containerRef = useRef<HTMLDivElement | null>(null);
  const [size, setSize] = useState<TreemapSize>({ width: 800, height: 540 });
  const [hoveredTopicId, setHoveredTopicId] = useState<string | null>(null);

  useEffect(() => {
    if (!containerRef.current) return;
    const element = containerRef.current;
    const observer = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (!entry) return;
      setSize({
        width: Math.max(320, Math.floor(entry.contentRect.width)),
        height: Math.max(420, Math.floor(entry.contentRect.height))
      });
    });
    observer.observe(element);
    return () => observer.disconnect();
  }, []);

  const { visibleTopics, tiles, hiddenTopicCount } = useMemo(() => {
    const limit = DENSITY_LIMITS[density];
    const rankedTopics = [...topics].sort((left, right) => topicPriority(right) - topicPriority(left));
    let limitedTopics =
      Number.isFinite(limit) && rankedTopics.length > limit ? rankedTopics.slice(0, limit) : rankedTopics;

    if (selectedTopicId) {
      const selectedTopic = topics.find((topic) => topic.topic_id === selectedTopicId);
      if (selectedTopic && !limitedTopics.some((topic) => topic.topic_id === selectedTopic.topic_id)) {
        limitedTopics = [...limitedTopics.slice(0, Math.max(limitedTopics.length - 1, 0)), selectedTopic];
      }
    }
    limitedTopics = dedupeTopicsById(limitedTopics);

    const hiddenByDensity = Math.max(topics.length - limitedTopics.length, 0);
    const chartAreaSize = {
      width: size.width,
      height: Math.max(size.height - TREEMAP_TOP_INSET, 220)
    };
    const minimumArea = minimumReadableArea(chartAreaSize, density);

    let workingTopics = [...limitedTopics];

    for (let iteration = 0; iteration < MAX_PRUNE_ITERATIONS; iteration += 1) {
      if (workingTopics.length <= MIN_VISIBLE_TOPICS) {
        break;
      }
      const provisionalTiles = layoutTiles(workingTopics, size, TREEMAP_TOP_INSET);
      if (!provisionalTiles.length) {
        break;
      }

      const unreadableTopicIds = new Set<string>(
        provisionalTiles
          .filter((tile) => isUnreadableTile(tile, density, minimumArea))
          .map((tile) => tile.topic.topic_id)
      );

      if (unreadableTopicIds.size === 0) {
        break;
      }

      const candidates = workingTopics
        .filter((topic) => unreadableTopicIds.has(topic.topic_id))
        .sort((left, right) => topicPriority(left) - topicPriority(right));
      const maxRemovable = Math.max(workingTopics.length - MIN_VISIBLE_TOPICS, 0);
      const removable = candidates.slice(0, maxRemovable);
      if (removable.length === 0) {
        break;
      }

      const removeIds = new Set(removable.map((topic) => topic.topic_id));
      workingTopics = workingTopics.filter((topic) => !removeIds.has(topic.topic_id));
    }

    const finalTiles = layoutTiles(workingTopics, size, TREEMAP_TOP_INSET).filter(
      (tile) => !isUnreadableTile(tile, density, minimumArea)
    );
    const finalTopicIds = new Set(finalTiles.map((tile) => tile.topic.topic_id));
    const finalTopics = workingTopics.filter((topic) => finalTopicIds.has(topic.topic_id));
    const hiddenByReadability = Math.max(topics.length - hiddenByDensity - finalTopics.length, 0);

    return {
      visibleTopics: finalTopics,
      tiles: finalTiles,
      hiddenTopicCount: hiddenByDensity + hiddenByReadability
    };
  }, [density, selectedTopicId, size, topics]);

  const momentumScale = useMemo(() => buildMomentumScale(visibleTopics), [visibleTopics]);

  if (!topics.length) {
    return (
      <div className="treemap-empty" ref={containerRef}>
        <p>No topics found for the current filter selection.</p>
      </div>
    );
  }

  const hoveredTopic = visibleTopics.find((topic) => topic.topic_id === hoveredTopicId) ?? null;

  return (
    <div className={`treemap-shell ${renderMode === "3d" ? "mode-3d" : ""}`} ref={containerRef}>
      <div className="treemap-legend">
        <span>Momentum</span>
        <div className="legend-bar" />
        <span>Cool</span>
        <span>Hot</span>
        <span className="legend-note">
          {legendNote ?? (hiddenTopicCount > 0 ? `${hiddenTopicCount} hidden` : "Relative view")}
        </span>
      </div>

      {tiles.map((tile) => {
        const width = tile.x1 - tile.x0;
        const height = tile.y1 - tile.y0;
        const area = width * height;
        const relativeMomentum = normalizeMomentum(tile.topic.momentum, momentumScale);
        const tileMode =
          area < 9200 || width < 210 || height < 138
            ? "small"
            : area < 14500 || width < 250 || height < 175
              ? "medium"
              : "large";

        const tileDepth = renderMode === "3d" ? clamp(Math.round(6 + (relativeMomentum + 1) * 7), 6, 20) : 0;
        const tileStyle: CSSProperties & { [key: string]: string | number } = {
          left: tile.x0,
          top: tile.y0,
          width,
          height,
          backgroundColor: momentumColor(relativeMomentum)
        };
        if (renderMode === "3d") {
          tileStyle["--tile-depth"] = `${tileDepth}px`;
        }

        return (
          <button
            key={tile.topic.topic_id}
            type="button"
            className={`treemap-tile tile-${tileMode} ${renderMode === "3d" ? "is-3d" : ""} ${selectedTopicId === tile.topic.topic_id ? "is-active" : ""}`}
            style={tileStyle}
            onClick={() => onSelectTopic(tile.topic)}
            onMouseEnter={() => setHoveredTopicId(tile.topic.topic_id)}
            onMouseLeave={() => setHoveredTopicId((prev) => (prev === tile.topic.topic_id ? null : prev))}
          >
            <div className="tile-header">
              <p className="tile-label">
                {tileMode === "small" ? shortLabel(tile.topic.label, 3) : shortLabel(tile.topic.label, 5)}
              </p>
              {tileMode === "large" ? <span className="tile-heat">{heatLabel(relativeMomentum)}</span> : null}
            </div>

            {tileMode === "large" ? (
              <>
                <div className="tile-meta">
                  <span>{tile.topic.volume_now} stories</span>
                  <span>{formatPercent(tile.topic.momentum)}</span>
                </div>
                <Sparkline values={tile.topic.sparkline} className="tile-sparkline" />
              </>
            ) : (
              <div className="tile-meta tile-meta-compact">
                <span>{tile.topic.volume_now} stories</span>
                {tileMode === "medium" ? <span>{formatPercent(tile.topic.momentum)}</span> : null}
              </div>
            )}
          </button>
        );
      })}

      {hoveredTopic ? (
        <aside className="treemap-tooltip" role="status" aria-live="polite">
          <strong>{hoveredTopic.label}</strong>
          <p>Volume: {hoveredTopic.volume_now}</p>
          <p>Weighted volume: {(hoveredTopic.weighted_volume_now ?? hoveredTopic.volume_now).toFixed(1)}</p>
          <p>Momentum: {formatPercent(hoveredTopic.momentum)}</p>
          <p>Novelty: {formatPercent(hoveredTopic.novelty)}</p>
          <p>Sources: {hoveredTopic.diversity}</p>
        </aside>
      ) : null}
    </div>
  );
}
