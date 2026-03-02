"use client";

import { useEffect, useMemo, useRef, useState } from "react";

import { TopicFilters, TreemapDensityOption, WindowOption } from "@/lib/types";

const WINDOWS: WindowOption[] = ["1h", "6h", "24h", "7d"];

interface FiltersBarProps {
  filters: TopicFilters;
  treemapDensity: TreemapDensityOption;
  availableVerticals: string[];
  availableSources: string[];
  visibleTopicCount: number;
  onChange: (nextFilters: TopicFilters) => void;
  onDensityChange: (density: TreemapDensityOption) => void;
  onOpenAbout: () => void;
}

function toggleListEntry(entries: string[], entry: string): string[] {
  return entries.includes(entry) ? entries.filter((value) => value !== entry) : [...entries, entry];
}

export function FiltersBar({
  filters,
  treemapDensity,
  availableVerticals,
  availableSources,
  visibleTopicCount,
  onChange,
  onDensityChange,
  onOpenAbout
}: FiltersBarProps) {
  const [sourceMenuOpen, setSourceMenuOpen] = useState(false);
  const [sourceQuery, setSourceQuery] = useState("");
  const sourceDropdownRef = useRef<HTMLDivElement | null>(null);

  const filteredSources = useMemo(() => {
    const query = sourceQuery.trim().toLowerCase();
    if (!query) {
      return availableSources;
    }
    return availableSources.filter((source) => source.toLowerCase().includes(query));
  }, [availableSources, sourceQuery]);

  useEffect(() => {
    const onWindowClick = (event: MouseEvent) => {
      if (!sourceMenuOpen) {
        return;
      }
      const container = sourceDropdownRef.current;
      if (!container) {
        return;
      }
      if (container.contains(event.target as Node)) {
        return;
      }
      setSourceMenuOpen(false);
    };

    window.addEventListener("mousedown", onWindowClick);
    return () => window.removeEventListener("mousedown", onWindowClick);
  }, [sourceMenuOpen]);

  const selectedSourceCount = filters.sources.length;

  return (
    <section className="filters-panel">
      <div className="filters-row">
        <div className="window-switcher">
          {WINDOWS.map((windowOption) => (
            <button
              key={windowOption}
              type="button"
              className={filters.window === windowOption ? "is-selected" : ""}
              onClick={() => onChange({ ...filters, window: windowOption })}
            >
              {windowOption}
            </button>
          ))}
        </div>

        <label className="search-box">
          <input
            type="search"
            placeholder="Search narratives, entities, keywords..."
            value={filters.search}
            onChange={(event) => onChange({ ...filters, search: event.target.value })}
          />
        </label>

        <label className="toggle-box">
          <input
            type="checkbox"
            checked={filters.onlyRising}
            onChange={(event) => onChange({ ...filters, onlyRising: event.target.checked })}
          />
          <span>Only rising</span>
        </label>

        <span className="filter-stat">
          {filters.onlyRising
            ? `Showing ${visibleTopicCount} rising narratives`
            : `Showing ${visibleTopicCount} narratives`}
        </span>

        <label className="density-control">
          <span>Density</span>
          <select
            value={treemapDensity}
            onChange={(event) => onDensityChange(event.target.value as TreemapDensityOption)}
          >
            <option value="readable">Readable</option>
            <option value="expanded">Expanded</option>
            <option value="all">All</option>
          </select>
        </label>

        <button type="button" className="about-trigger" onClick={onOpenAbout}>
          About
        </button>
      </div>

      <div className="filters-row filters-wrap">
        <div className="chip-group">
          <span className="chip-group-label">Verticals</span>
          {availableVerticals.map((vertical) => (
            <button
              key={vertical}
              type="button"
              className={filters.verticals.includes(vertical) ? "chip is-selected" : "chip"}
              onClick={() =>
                onChange({
                  ...filters,
                  verticals: toggleListEntry(filters.verticals, vertical)
                })
              }
            >
              {vertical}
            </button>
          ))}
        </div>

        <div className="source-filter">
          <span className="chip-group-label">Sources</span>
          <div
            className={`source-dropdown ${sourceMenuOpen ? "is-open" : ""}`}
            ref={sourceDropdownRef}
          >
            <button
              type="button"
              className="source-dropdown-trigger"
              onClick={() => setSourceMenuOpen((open) => !open)}
            >
              <span>{selectedSourceCount > 0 ? `${selectedSourceCount} selected` : "All sources"}</span>
              <span className="source-dropdown-caret" aria-hidden="true">
                {sourceMenuOpen ? "▲" : "▼"}
              </span>
            </button>

            {sourceMenuOpen ? (
              <div className="source-dropdown-panel">
                <div className="source-dropdown-controls">
                  <input
                    type="search"
                    className="source-dropdown-search"
                    placeholder="Search sources"
                    value={sourceQuery}
                    onChange={(event) => setSourceQuery(event.target.value)}
                  />
                  <div className="source-dropdown-actions">
                    <button
                      type="button"
                      onClick={() => {
                        const merged = new Set(filters.sources);
                        filteredSources.forEach((source) => merged.add(source));
                        onChange({
                          ...filters,
                          sources: [...merged].sort()
                        });
                      }}
                    >
                      Select filtered
                    </button>
                    <button
                      type="button"
                      onClick={() =>
                        onChange({
                          ...filters,
                          sources: []
                        })
                      }
                    >
                      Clear
                    </button>
                  </div>
                </div>

                <div className="source-dropdown-list">
                  {filteredSources.length ? (
                    filteredSources.map((source) => (
                      <label key={source} className="source-option">
                        <input
                          type="checkbox"
                          checked={filters.sources.includes(source)}
                          onChange={() =>
                            onChange({
                              ...filters,
                              sources: toggleListEntry(filters.sources, source)
                            })
                          }
                        />
                        <span>{source}</span>
                      </label>
                    ))
                  ) : (
                    <p className="source-dropdown-empty">No matching sources.</p>
                  )}
                </div>
              </div>
            ) : null}
          </div>
        </div>
      </div>
    </section>
  );
}
