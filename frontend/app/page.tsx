"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";

import { AboutModal } from "@/components/AboutModal";
import { DetailDrawer } from "@/components/DetailDrawer";
import { FiltersBar } from "@/components/FiltersBar";
import { RefreshChangesPanel, RefreshChangesSummary } from "@/components/RefreshChangesPanel";
import { SourceHealthBar } from "@/components/SourceHealthBar";
import { TrendingStrip } from "@/components/TrendingStrip";
import { fetchDecisionBriefing, fetchSeedTopics, fetchSourceCatalog, fetchTopics } from "@/lib/api";
import { formatGeneratedTime } from "@/lib/format";
import {
  DecisionItem,
  Topic,
  TopicFilters,
  TopicsResponse,
  TreemapDensityOption,
  ViewTab,
  WindowOption
} from "@/lib/types";

const DEFAULT_FILTERS: TopicFilters = {
  window: "24h",
  verticals: [],
  sources: [],
  onlyRising: false,
  search: ""
};

const VALID_WINDOWS: WindowOption[] = ["1h", "6h", "24h", "7d"];

type TreemapRenderMode = "2d" | "3d";

interface SnapshotRecord {
  generated_at: string | null;
  topics: Topic[];
}

function parseListParam(value: string | null): string[] {
  if (!value) return [];
  return value
    .split(",")
    .map((entry) => entry.trim())
    .filter(Boolean);
}

function aggregateSparkline(topics: Topic[]): number[] {
  const maxLength = Math.max(...topics.map((topic) => topic.sparkline.length), 0);
  if (maxLength <= 0) {
    return [];
  }
  const summed = new Array<number>(maxLength).fill(0);
  topics.forEach((topic) => {
    topic.sparkline.forEach((value, index) => {
      summed[index] += value;
    });
  });
  return summed;
}

function topTerms(topics: Topic[], kind: "keywords" | "entities", maxTerms = 8): string[] {
  const counts = new Map<string, number>();
  topics.forEach((topic) => {
    const values = kind === "keywords" ? topic.keywords : topic.entities;
    values.forEach((value) => {
      counts.set(value, (counts.get(value) ?? 0) + 1);
    });
  });
  return [...counts.entries()]
    .sort((left, right) => right[1] - left[1])
    .slice(0, maxTerms)
    .map(([term]) => term);
}

function buildVerticalRollups(topics: Topic[]): Topic[] {
  const byVertical = new Map<string, Topic[]>();
  topics.forEach((topic) => {
    const bucket = byVertical.get(topic.vertical) ?? [];
    bucket.push(topic);
    byVertical.set(topic.vertical, bucket);
  });

  const rollups = [...byVertical.entries()].map(([vertical, group]) => {
    const weightedVolumeNow = group.reduce(
      (total, topic) => total + (topic.weighted_volume_now ?? topic.volume_now),
      0
    );
    const weightedVolumePrev = group.reduce(
      (total, topic) => total + (topic.weighted_volume_prev ?? topic.volume_prev),
      0
    );
    const weight = group.reduce(
      (total, topic) => total + Math.max(topic.weighted_volume_now ?? topic.volume_now, 1),
      0
    );
    const weightedMomentum = group.reduce((total, topic) => {
      const topicWeight = Math.max(topic.weighted_volume_now ?? topic.volume_now, 1);
      return total + topic.momentum * topicWeight;
    }, 0);
    const weightedNovelty = group.reduce((total, topic) => {
      const topicWeight = Math.max(topic.weighted_volume_now ?? topic.volume_now, 1);
      return total + topic.novelty * topicWeight;
    }, 0);

    const representativeItems = group
      .flatMap((topic) => topic.representative_items)
      .slice(0, 8);
    const uniqueSources = new Set(representativeItems.map((item) => item.source_name));

    return {
      topic_id: `vertical_rollup:${vertical}`,
      label: `${vertical[0]?.toUpperCase() ?? ""}${vertical.slice(1)} overview`,
      vertical,
      volume_now: group.reduce((total, topic) => total + topic.volume_now, 0),
      volume_prev: group.reduce((total, topic) => total + topic.volume_prev, 0),
      momentum: weight > 0 ? weightedMomentum / weight : 0,
      novelty: weight > 0 ? weightedNovelty / weight : 0,
      diversity: uniqueSources.size,
      sparkline: aggregateSparkline(group),
      representative_items: representativeItems,
      keywords: topTerms(group, "keywords", 10),
      entities: topTerms(group, "entities", 8),
      related_topic_ids: group.slice(0, 12).map((topic) => topic.topic_id),
      summary: `Drill into ${vertical} to inspect ${group.length} narratives and ${Math.round(weightedVolumeNow)} weighted stories.`,
      weighted_volume_now: weightedVolumeNow,
      weighted_volume_prev: weightedVolumePrev,
      source_quality_score:
        group.reduce((total, topic) => total + (topic.source_quality_score ?? 1), 0) /
        Math.max(group.length, 1),
      label_confidence: 1
    } satisfies Topic;
  });

  return rollups.sort(
    (left, right) =>
      (right.weighted_volume_now ?? right.volume_now) - (left.weighted_volume_now ?? left.volume_now)
  );
}

function buildRefreshSummary(snapshots: SnapshotRecord[]): RefreshChangesSummary | null {
  if (snapshots.length < 2) {
    return null;
  }

  const previous = snapshots[snapshots.length - 2];
  const current = snapshots[snapshots.length - 1];
  const previousById = new Map(previous.topics.map((topic) => [topic.topic_id, topic]));
  const currentById = new Map(current.topics.map((topic) => [topic.topic_id, topic]));

  const newTopics = current.topics.filter((topic) => !previousById.has(topic.topic_id));
  const droppedTopics = previous.topics.filter((topic) => !currentById.has(topic.topic_id));

  const deltas = current.topics
    .map((topic) => {
      const previousTopic = previousById.get(topic.topic_id);
      if (!previousTopic) return null;
      const deltaMomentum = topic.momentum - previousTopic.momentum;
      return {
        topic_id: topic.topic_id,
        label: topic.label,
        momentum: topic.momentum,
        delta_momentum: deltaMomentum,
        volume_now: topic.volume_now
      };
    })
    .filter((item): item is NonNullable<typeof item> => Boolean(item));

  const rising = [...deltas]
    .filter((item) => item.delta_momentum > 0)
    .sort((left, right) => right.delta_momentum - left.delta_momentum);
  const cooling = [...deltas]
    .filter((item) => item.delta_momentum < 0)
    .sort((left, right) => left.delta_momentum - right.delta_momentum);

  return {
    previous_generated_at: previous.generated_at,
    generated_at: current.generated_at,
    new_topics: newTopics.length,
    dropped_topics: droppedTopics.length,
    rising,
    cooling
  };
}

const TreemapChart = dynamic(
  () => import("@/components/TreemapChart").then((module) => module.TreemapChart),
  { ssr: false }
);
const NarrativeLandscape3D = dynamic(
  () => import("@/components/NarrativeLandscape3D").then((module) => module.NarrativeLandscape3D),
  { ssr: false }
);
const ActionCenter = dynamic(
  () => import("@/components/ActionCenter").then((module) => module.ActionCenter),
  { ssr: false }
);
const DecisionBriefing = dynamic(
  () => import("@/components/DecisionBriefing").then((module) => module.DecisionBriefing),
  { ssr: false }
);
const BacktestPanel = dynamic(
  () => import("@/components/BacktestPanel").then((module) => module.BacktestPanel),
  { ssr: false }
);
const PaperPortfolioPanel = dynamic(
  () => import("@/components/PaperPortfolioPanel").then((module) => module.PaperPortfolioPanel),
  { ssr: false }
);
const AlertRulesPanel = dynamic(
  () => import("@/components/AlertRulesPanel").then((module) => module.AlertRulesPanel),
  { ssr: false }
);

export default function Page() {
  const [filters, setFilters] = useState<TopicFilters>(DEFAULT_FILTERS);
  const [response, setResponse] = useState<TopicsResponse | null>(null);
  const [selectedTopic, setSelectedTopic] = useState<Topic | null>(null);
  const [evidenceByTopicId, setEvidenceByTopicId] = useState<Record<string, DecisionItem>>({});
  const [recentSnapshots, setRecentSnapshots] = useState<SnapshotRecord[]>([]);
  const [autoSelectEnabled, setAutoSelectEnabled] = useState(true);
  const [catalogVerticals, setCatalogVerticals] = useState<string[]>([]);
  const [catalogSources, setCatalogSources] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState<ViewTab>("map");
  const [treemapDensity, setTreemapDensity] = useState<TreemapDensityOption>("readable");
  const [drilldownMode, setDrilldownMode] = useState(false);
  const [drilldownVertical, setDrilldownVertical] = useState<string | null>(null);
  const [treemapRenderMode, setTreemapRenderMode] = useState<TreemapRenderMode>("2d");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [aboutOpen, setAboutOpen] = useState(false);
  const [urlHydrated, setUrlHydrated] = useState(false);

  const loadTopics = useCallback(async (activeFilters: TopicFilters, initial = false) => {
    if (initial) {
      setLoading(true);
    } else {
      setRefreshing(true);
    }
    setError(null);

    try {
      const payload = await fetchTopics(activeFilters);
      setResponse(payload);
      setSelectedTopic((previousTopic) => {
        if (!previousTopic) return null;
        return payload.topics.find((topic) => topic.topic_id === previousTopic.topic_id) ?? null;
      });
    } catch (fetchError) {
      const seed = await fetchSeedTopics(activeFilters.window);
      if (seed) {
        setResponse(seed);
        const detail =
          fetchError instanceof Error ? fetchError.message.split("Attempts:")[0].trim() : "Fetch failed";
        setError(`Live backend unreachable (${detail}). Displaying seeded demo cache.`);
      } else {
        setError(fetchError instanceof Error ? fetchError.message : "Unable to load topics");
      }
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    if (!urlHydrated) return;
    void loadTopics(filters, true);
  }, [filters, loadTopics, urlHydrated]);

  useEffect(() => {
    const loadCatalog = async () => {
      const catalog = await fetchSourceCatalog();
      if (!catalog) return;

      const sourceNames = new Set<string>();
      const verticalNames = new Set<string>();
      for (const source of catalog.sources) {
        sourceNames.add(source.name);
        verticalNames.add(source.vertical);
      }
      setCatalogSources([...sourceNames].sort());
      setCatalogVerticals([...verticalNames].sort());
    };

    void loadCatalog();
  }, []);

  useEffect(() => {
    if (typeof window === "undefined") {
      return;
    }

    const searchParams = new URLSearchParams(window.location.search);
    const view = searchParams.get("view");
    const parsedFilters: TopicFilters = { ...DEFAULT_FILTERS };

    const windowParam = searchParams.get("window");
    if (windowParam && VALID_WINDOWS.includes(windowParam as WindowOption)) {
      parsedFilters.window = windowParam as WindowOption;
    }
    parsedFilters.verticals = parseListParam(searchParams.get("verticals"));
    parsedFilters.sources = parseListParam(searchParams.get("sources"));
    parsedFilters.onlyRising = searchParams.get("only_rising") === "true";
    parsedFilters.search = searchParams.get("search") ?? "";

    const densityParam = searchParams.get("density");
    if (densityParam === "readable" || densityParam === "expanded" || densityParam === "all") {
      setTreemapDensity(densityParam);
    }

    const mapDimParam = searchParams.get("map_dim");
    if (mapDimParam === "3d") {
      setTreemapRenderMode("3d");
    }

    const drilldownParam = searchParams.get("drilldown");
    if (drilldownParam === "on") {
      setDrilldownMode(true);
      const drillVerticalParam = searchParams.get("drill_vertical");
      if (drillVerticalParam) {
        setDrilldownVertical(drillVerticalParam);
      }
    }

    setFilters(parsedFilters);

    if (
      view === "map" ||
      view === "action" ||
      view === "briefing" ||
      view === "backtest" ||
      view === "portfolio" ||
      view === "alerts"
    ) {
      setActiveTab(view);
    }

    setUrlHydrated(true);
  }, []);

  useEffect(() => {
    if (!urlHydrated) {
      return;
    }
    const interval = window.setInterval(() => {
      void loadTopics(filters, false);
    }, 45000);

    return () => window.clearInterval(interval);
  }, [filters, loadTopics, urlHydrated]);

  useEffect(() => {
    if (!urlHydrated || typeof window === "undefined") {
      return;
    }
    const url = new URL(window.location.href);
    if (activeTab === "map") {
      url.searchParams.delete("view");
    } else {
      url.searchParams.set("view", activeTab);
    }
    url.searchParams.set("window", filters.window);
    if (filters.verticals.length > 0) {
      url.searchParams.set("verticals", filters.verticals.join(","));
    } else {
      url.searchParams.delete("verticals");
    }
    if (filters.sources.length > 0) {
      url.searchParams.set("sources", filters.sources.join(","));
    } else {
      url.searchParams.delete("sources");
    }
    if (filters.onlyRising) {
      url.searchParams.set("only_rising", "true");
    } else {
      url.searchParams.delete("only_rising");
    }
    if (filters.search.trim()) {
      url.searchParams.set("search", filters.search.trim());
    } else {
      url.searchParams.delete("search");
    }
    url.searchParams.set("density", treemapDensity);
    url.searchParams.set("map_dim", treemapRenderMode);
    if (drilldownMode) {
      url.searchParams.set("drilldown", "on");
      if (drilldownVertical) {
        url.searchParams.set("drill_vertical", drilldownVertical);
      } else {
        url.searchParams.delete("drill_vertical");
      }
    } else {
      url.searchParams.delete("drilldown");
      url.searchParams.delete("drill_vertical");
    }

    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, [activeTab, drilldownMode, drilldownVertical, filters, treemapDensity, treemapRenderMode, urlHydrated]);

  useEffect(() => {
    if (!response) return;
    setRecentSnapshots((previous) => {
      const next: SnapshotRecord = {
        generated_at: response.metadata?.generated_at ?? null,
        topics: response.topics
      };
      const last = previous[previous.length - 1];
      if (last && last.generated_at === next.generated_at) {
        return previous;
      }
      return [...previous, next].slice(-2);
    });
  }, [response]);

  const topics = response?.topics ?? [];
  const metadata = response?.metadata;
  const refreshSummary = useMemo(() => buildRefreshSummary(recentSnapshots), [recentSnapshots]);
  const verticalRollupTopics = useMemo(() => buildVerticalRollups(topics), [topics]);

  const treemapTopics = useMemo(() => {
    if (!drilldownMode) {
      return topics;
    }
    if (!drilldownVertical) {
      return verticalRollupTopics;
    }
    return topics.filter((topic) => topic.vertical === drilldownVertical);
  }, [drilldownMode, drilldownVertical, topics, verticalRollupTopics]);
  const landscapeTopics = useMemo(() => {
    if (drilldownMode && drilldownVertical) {
      return topics.filter((topic) => topic.vertical === drilldownVertical);
    }
    return topics;
  }, [drilldownMode, drilldownVertical, topics]);
  const autoSelectTopics = treemapRenderMode === "3d" ? landscapeTopics : treemapTopics;

  const isVerticalOverview = drilldownMode && !drilldownVertical;
  const selectedTopicForDrawer =
    selectedTopic && !selectedTopic.topic_id.startsWith("vertical_rollup:") ? selectedTopic : null;

  useEffect(() => {
    if (!drilldownMode) {
      setDrilldownVertical(null);
      return;
    }
    if (drilldownVertical && !topics.some((topic) => topic.vertical === drilldownVertical)) {
      setDrilldownVertical(null);
    }
  }, [drilldownMode, drilldownVertical, topics]);

  useEffect(() => {
    if (!urlHydrated) return;
    let cancelled = false;
    const loadEvidence = async () => {
      try {
        const briefing = await fetchDecisionBriefing(filters, "investor", 50);
        if (cancelled) return;
        const nextMap: Record<string, DecisionItem> = {};
        briefing.decisions.forEach((decision) => {
          nextMap[decision.topic_id] = decision;
        });
        setEvidenceByTopicId(nextMap);
      } catch {
        if (!cancelled) {
          setEvidenceByTopicId({});
        }
      }
    };
    void loadEvidence();
    return () => {
      cancelled = true;
    };
  }, [filters, urlHydrated]);

  const handleSelectTopic = useCallback((topic: Topic) => {
    setSelectedTopic(topic);
    setAutoSelectEnabled(false);
  }, []);

  const handleTreemapSelect = useCallback(
    (topic: Topic) => {
      if (isVerticalOverview) {
        setDrilldownVertical(topic.vertical);
        setSelectedTopic(null);
        return;
      }
      handleSelectTopic(topic);
    },
    [handleSelectTopic, isVerticalOverview]
  );

  const handleTabChange = useCallback((tab: ViewTab) => {
    setActiveTab(tab);
  }, []);

  useEffect(() => {
    setAutoSelectEnabled(true);
  }, [filters]);

  useEffect(() => {
    if (isVerticalOverview && treemapRenderMode !== "3d") {
      return;
    }
    if (!autoSelectEnabled || selectedTopic || autoSelectTopics.length === 0) {
      return;
    }
    const nextTopic = [...autoSelectTopics].sort((left, right) => {
      const leftVolume = left.weighted_volume_now ?? left.volume_now;
      const rightVolume = right.weighted_volume_now ?? right.volume_now;
      const leftScore = left.momentum * Math.log(leftVolume + 1);
      const rightScore = right.momentum * Math.log(rightVolume + 1);
      return rightScore - leftScore;
    })[0];
    if (nextTopic) {
      setSelectedTopic(nextTopic);
      setAutoSelectEnabled(false);
    }
  }, [autoSelectEnabled, autoSelectTopics, isVerticalOverview, selectedTopic, treemapRenderMode]);

  const availableVerticals = useMemo(() => {
    const values = new Set<string>();
    catalogVerticals.forEach((value) => values.add(value));
    filters.verticals.forEach((value) => values.add(value));
    topics.forEach((topic) => values.add(topic.vertical));
    return [...values].sort();
  }, [catalogVerticals, filters.verticals, topics]);

  const availableSources = useMemo(() => {
    const values = new Set<string>();
    catalogSources.forEach((value) => values.add(value));
    filters.sources.forEach((value) => values.add(value));
    topics.forEach((topic) => {
      topic.representative_items.forEach((item) => values.add(item.source_name));
    });
    return [...values].sort();
  }, [catalogSources, filters.sources, topics]);

  return (
    <main className="page-shell">
      <header className="hero">
        <div>
          <p className="hero-kicker">Narrative Treemap</p>
          <h1>Multi-vertical narrative heatmap</h1>
          <p className="hero-subtitle">
            Tile size tracks topic volume and color tracks momentum. Click any narrative for sources,
            entities, and related clusters.
          </p>
        </div>
        <div className="hero-meta">
          <p>Window: {filters.window}</p>
          <p>Generated: {metadata ? formatGeneratedTime(metadata.generated_at) : "n/a"}</p>
          <p>Status: {refreshing ? "Refreshing..." : "Live"}</p>
          <p>Items: {metadata?.item_count ?? 0}</p>
          <p>Algorithm: {metadata?.algorithm ?? "seed"}</p>
        </div>
      </header>

      <FiltersBar
        filters={filters}
        treemapDensity={treemapDensity}
        availableVerticals={availableVerticals}
        availableSources={availableSources}
        visibleTopicCount={topics.length}
        onChange={(nextFilters) => {
          const normalized: TopicFilters = {
            ...nextFilters,
            window: nextFilters.window as WindowOption
          };
          setFilters(normalized);
        }}
        onDensityChange={setTreemapDensity}
        onOpenAbout={() => setAboutOpen(true)}
      />

      <section className="view-tabs">
        <button
          type="button"
          className={activeTab === "map" ? "is-selected" : ""}
          onClick={() => handleTabChange("map")}
        >
          Treemap
        </button>
        <button
          type="button"
          className={activeTab === "action" ? "is-selected" : ""}
          onClick={() => handleTabChange("action")}
        >
          Action center
        </button>
        <button
          type="button"
          className={activeTab === "briefing" ? "is-selected" : ""}
          onClick={() => handleTabChange("briefing")}
        >
          Decision briefing
        </button>
        <button
          type="button"
          className={activeTab === "backtest" ? "is-selected" : ""}
          onClick={() => handleTabChange("backtest")}
        >
          Backtest
        </button>
        <button
          type="button"
          className={activeTab === "portfolio" ? "is-selected" : ""}
          onClick={() => handleTabChange("portfolio")}
        >
          Paper portfolio
        </button>
        <button
          type="button"
          className={activeTab === "alerts" ? "is-selected" : ""}
          onClick={() => handleTabChange("alerts")}
        >
          Alerts
        </button>
      </section>

      <TrendingStrip topics={topics} onSelectTopic={handleSelectTopic} />

      {metadata ? <SourceHealthBar health={metadata.source_health} /> : null}

      {error ? <p className="error-banner">{error}</p> : null}

      {activeTab === "map" ? (
        <>
          <section className="map-controls">
            <div className="mode-toggle">
              <span>Mode</span>
              <button
                type="button"
                className={!drilldownMode ? "is-selected" : ""}
                onClick={() => {
                  setDrilldownMode(false);
                  setDrilldownVertical(null);
                }}
              >
                Standard
              </button>
              <button
                type="button"
                className={drilldownMode ? "is-selected" : ""}
                onClick={() => setDrilldownMode(true)}
              >
                Drilldown
              </button>
            </div>
            <div className="mode-toggle">
              <span>Render</span>
              <button
                type="button"
                className={treemapRenderMode === "2d" ? "is-selected" : ""}
                onClick={() => setTreemapRenderMode("2d")}
              >
                Treemap
              </button>
              <button
                type="button"
                className={treemapRenderMode === "3d" ? "is-selected" : ""}
                onClick={() => setTreemapRenderMode("3d")}
              >
                3D landscape
              </button>
            </div>
            {drilldownMode ? (
              <div className="map-drilldown-state">
                {drilldownVertical ? (
                  <>
                    <span>Drilldown: {drilldownVertical}</span>
                    <button
                      type="button"
                      onClick={() => {
                        setDrilldownVertical(null);
                        setSelectedTopic(null);
                      }}
                    >
                      Back to verticals
                    </button>
                  </>
                ) : (
                  <span>
                    {treemapRenderMode === "3d"
                      ? "Landscape shows topic-level graph. Set a vertical to focus the scene."
                      : "Select a vertical tile to inspect topic-level clusters."}
                  </span>
                )}
              </div>
            ) : null}
          </section>

          <RefreshChangesPanel
            summary={refreshSummary}
            onInspectTopic={(topicId) => {
              const topic = topics.find((candidate) => candidate.topic_id === topicId);
              if (!topic) return;
              setDrilldownMode(true);
              setDrilldownVertical(topic.vertical);
              handleSelectTopic(topic);
            }}
          />

          <section className="workspace">
            <div className="treemap-column">
              {loading ? (
                <div className="loading-state">
                  <p>Loading narrative map...</p>
                </div>
              ) : treemapRenderMode === "3d" ? (
                <NarrativeLandscape3D
                  topics={landscapeTopics}
                  selectedTopicId={selectedTopicForDrawer?.topic_id ?? null}
                  onSelectTopic={handleSelectTopic}
                />
              ) : (
                <TreemapChart
                  topics={treemapTopics}
                  density={treemapDensity}
                  renderMode="2d"
                  selectedTopicId={selectedTopicForDrawer?.topic_id ?? null}
                  legendNote={
                    isVerticalOverview
                      ? "Click a vertical tile to drill in"
                      : drilldownMode
                        ? `${drilldownVertical ?? "Filtered"} drilldown`
                        : null
                  }
                  onSelectTopic={handleTreemapSelect}
                />
              )}
            </div>

            <DetailDrawer
              topic={selectedTopicForDrawer}
              allTopics={topics}
              decisionEvidence={
                selectedTopicForDrawer ? evidenceByTopicId[selectedTopicForDrawer.topic_id] ?? null : null
              }
              onClose={() => {
                setSelectedTopic(null);
                setAutoSelectEnabled(false);
              }}
              onSelectTopic={handleSelectTopic}
            />
          </section>
        </>
      ) : activeTab === "action" ? (
        <ActionCenter
          topics={topics}
          timeWindow={filters.window}
          generatedAt={metadata?.generated_at ?? null}
          lifecycle={metadata?.lifecycle ?? null}
          onInspectTopic={(topic) => {
            handleSelectTopic(topic);
            handleTabChange("map");
          }}
        />
      ) : activeTab === "briefing" ? (
        <DecisionBriefing
          filters={filters}
          topics={topics}
          onInspectTopic={(topic) => {
            handleSelectTopic(topic);
            handleTabChange("map");
          }}
        />
      ) : activeTab === "backtest" ? (
        <BacktestPanel
          filters={filters}
          topics={topics}
          onInspectTopic={(topic) => {
            handleSelectTopic(topic);
            handleTabChange("map");
          }}
        />
      ) : activeTab === "portfolio" ? (
        <PaperPortfolioPanel filters={filters} />
      ) : (
        <AlertRulesPanel
          filters={filters}
          availableVerticals={availableVerticals}
          availableSources={availableSources}
        />
      )}

      <AboutModal open={aboutOpen} onClose={() => setAboutOpen(false)} />
    </main>
  );
}
