"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import dynamic from "next/dynamic";

import { AboutModal } from "@/components/AboutModal";
import { DetailDrawer } from "@/components/DetailDrawer";
import { FiltersBar } from "@/components/FiltersBar";
import { SourceHealthBar } from "@/components/SourceHealthBar";
import { TrendingStrip } from "@/components/TrendingStrip";
import { fetchSeedTopics, fetchSourceCatalog, fetchTopics } from "@/lib/api";
import { formatGeneratedTime } from "@/lib/format";
import {
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

const TreemapChart = dynamic(
  () => import("@/components/TreemapChart").then((module) => module.TreemapChart),
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
  const [autoSelectEnabled, setAutoSelectEnabled] = useState(true);
  const [catalogVerticals, setCatalogVerticals] = useState<string[]>([]);
  const [catalogSources, setCatalogSources] = useState<string[]>([]);
  const [activeTab, setActiveTab] = useState<ViewTab>("map");
  const [treemapDensity, setTreemapDensity] = useState<TreemapDensityOption>("readable");
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [aboutOpen, setAboutOpen] = useState(false);

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
    void loadTopics(filters, true);
  }, [filters, loadTopics]);

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
    const view = new URLSearchParams(window.location.search).get("view");
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
  }, []);

  useEffect(() => {
    const interval = window.setInterval(() => {
      void loadTopics(filters, false);
    }, 45000);

    return () => window.clearInterval(interval);
  }, [filters, loadTopics]);

  const topics = response?.topics ?? [];
  const metadata = response?.metadata;

  const handleSelectTopic = useCallback((topic: Topic) => {
    setSelectedTopic(topic);
    setAutoSelectEnabled(false);
  }, []);

  const handleTabChange = useCallback((tab: ViewTab) => {
    setActiveTab(tab);
    if (typeof window === "undefined") {
      return;
    }
    const url = new URL(window.location.href);
    if (tab === "map") {
      url.searchParams.delete("view");
    } else {
      url.searchParams.set("view", tab);
    }
    window.history.replaceState({}, "", `${url.pathname}${url.search}${url.hash}`);
  }, []);

  useEffect(() => {
    setAutoSelectEnabled(true);
  }, [filters]);

  useEffect(() => {
    if (!autoSelectEnabled || selectedTopic || topics.length === 0) {
      return;
    }
    const nextTopic = [...topics].sort((left, right) => {
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
  }, [autoSelectEnabled, selectedTopic, topics]);

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
        <section className="workspace">
          <div className="treemap-column">
            {loading ? (
              <div className="loading-state">
                <p>Loading narrative map...</p>
              </div>
            ) : (
              <TreemapChart
                topics={topics}
                density={treemapDensity}
                selectedTopicId={selectedTopic?.topic_id ?? null}
                onSelectTopic={handleSelectTopic}
              />
            )}
          </div>

          <DetailDrawer
            topic={selectedTopic}
            allTopics={topics}
            onClose={() => {
              setSelectedTopic(null);
              setAutoSelectEnabled(false);
            }}
            onSelectTopic={handleSelectTopic}
          />
        </section>
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
