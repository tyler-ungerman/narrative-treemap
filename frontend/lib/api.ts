import {
  AlertEvaluationResponse,
  AlertRule,
  AlertRulesResponse,
  BacktestResponse,
  DecisionBriefingResponse,
  DecisionProfile,
  PortfolioResponse,
  TopicFilters,
  TopicsResponse,
  WindowOption
} from "@/lib/types";

const CONFIGURED_API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL?.trim() ?? "";
const DEFAULT_API_BASE_URL = "http://localhost:8001";

function dedupe(values: string[]): string[] {
  return [...new Set(values.filter((value) => value.trim().length > 0))];
}

function candidateApiBaseUrls(): string[] {
  const candidates: string[] = [];

  if (CONFIGURED_API_BASE_URL) {
    candidates.push(CONFIGURED_API_BASE_URL);
  }
  candidates.push(DEFAULT_API_BASE_URL, "http://localhost:8000");

  if (typeof window !== "undefined") {
    const hostname = window.location.hostname;
    candidates.push(
      `http://${hostname}:8001`,
      `http://${hostname}:8000`,
      "http://127.0.0.1:8001",
      "http://127.0.0.1:8000"
    );
  }

  return dedupe(candidates);
}

async function fetchWithFallback(pathWithQuery: string, cacheMode: RequestCache = "no-store"): Promise<Response> {
  const errors: string[] = [];

  for (const baseUrl of candidateApiBaseUrls()) {
    try {
      const response = await fetch(`${baseUrl}${pathWithQuery}`, { cache: cacheMode });
      if (response.ok) {
        return response;
      }
      errors.push(`${baseUrl} -> HTTP ${response.status}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`${baseUrl} -> ${message}`);
    }
  }

  throw new Error(`Backend fetch failed. Attempts: ${errors.join(" | ")}`);
}

async function fetchWriteWithFallback(path: string, options: RequestInit): Promise<Response> {
  const errors: string[] = [];
  for (const baseUrl of candidateApiBaseUrls()) {
    try {
      const response = await fetch(`${baseUrl}${path}`, options);
      if (response.ok) {
        return response;
      }
      errors.push(`${baseUrl} -> HTTP ${response.status}`);
    } catch (error) {
      const message = error instanceof Error ? error.message : String(error);
      errors.push(`${baseUrl} -> ${message}`);
    }
  }
  throw new Error(`Backend write failed. Attempts: ${errors.join(" | ")}`);
}

function buildQuery(filters: TopicFilters): string {
  const query = new URLSearchParams();
  query.set("window", filters.window);
  if (filters.verticals.length > 0) {
    query.set("verticals", filters.verticals.join(","));
  }
  if (filters.sources.length > 0) {
    query.set("sources", filters.sources.join(","));
  }
  if (filters.onlyRising) {
    query.set("only_rising", "true");
  }
  if (filters.search.trim()) {
    query.set("search", filters.search.trim());
  }
  return query.toString();
}

function buildBriefingQuery(filters: TopicFilters, profile: DecisionProfile, topN: number): string {
  const query = new URLSearchParams(buildQuery(filters));
  query.set("profile", profile);
  query.set("top_n", String(topN));
  return query.toString();
}

export async function fetchTopics(filters: TopicFilters): Promise<TopicsResponse> {
  const query = buildQuery(filters);
  const response = await fetchWithFallback(`/api/topics?${query}`);
  return (await response.json()) as TopicsResponse;
}

export async function fetchDecisionBriefing(
  filters: TopicFilters,
  profile: DecisionProfile,
  topN: number
): Promise<DecisionBriefingResponse> {
  const query = buildBriefingQuery(filters, profile, topN);
  const response = await fetchWithFallback(`/api/briefing?${query}`);
  return (await response.json()) as DecisionBriefingResponse;
}

export async function fetchBacktest(
  filters: TopicFilters,
  profile: DecisionProfile
): Promise<BacktestResponse> {
  const query = new URLSearchParams(buildQuery(filters));
  query.set("profile", profile);
  const response = await fetchWithFallback(`/api/backtest?${query.toString()}`);
  return (await response.json()) as BacktestResponse;
}

export async function fetchPortfolio(
  filters: TopicFilters,
  profile: DecisionProfile
): Promise<PortfolioResponse> {
  const query = new URLSearchParams(buildQuery(filters));
  query.set("profile", profile);
  const response = await fetchWithFallback(`/api/portfolio?${query.toString()}`);
  return (await response.json()) as PortfolioResponse;
}

export async function fetchSeedTopics(window: WindowOption): Promise<TopicsResponse | null> {
  try {
    const response = await fetch(`/seed-topics-${window}.json`, {
      cache: "force-cache"
    });
    if (!response.ok) {
      return null;
    }
    return (await response.json()) as TopicsResponse;
  } catch {
    return null;
  }
}

export interface SourceCatalogResponse {
  sources: Array<{
    name: string;
    vertical: string;
    category: string;
    cadence_minutes: number;
    max_items: number;
    failover_behavior: string;
    quality_score: number;
  }>;
}

export async function fetchSourceCatalog(): Promise<SourceCatalogResponse | null> {
  try {
    const response = await fetchWithFallback("/api/sources");
    return (await response.json()) as SourceCatalogResponse;
  } catch {
    return null;
  }
}

export async function fetchAlertRules(eventsLimit = 50): Promise<AlertRulesResponse> {
  const response = await fetchWithFallback(`/api/alerts?events_limit=${eventsLimit}`);
  return (await response.json()) as AlertRulesResponse;
}

export async function saveAlertRule(
  payload: Omit<AlertRule, "rule_id" | "created_at" | "updated_at" | "last_triggered_at">
): Promise<AlertRule> {
  const created = await fetchWriteWithFallback("/api/alerts/rules", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  return (await created.json()) as AlertRule;
}

export async function updateAlertRule(
  ruleId: string,
  payload: Omit<AlertRule, "rule_id" | "created_at" | "updated_at" | "last_triggered_at">
): Promise<AlertRule> {
  const updated = await fetchWriteWithFallback(`/api/alerts/rules/${ruleId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload)
  });
  return (await updated.json()) as AlertRule;
}

export async function deleteAlertRule(ruleId: string): Promise<void> {
  await fetchWriteWithFallback(`/api/alerts/rules/${ruleId}`, {
    method: "DELETE"
  });
}

export async function evaluateAlertRules(ruleId?: string): Promise<AlertEvaluationResponse> {
  const path = ruleId ? `/api/alerts/evaluate?rule_id=${encodeURIComponent(ruleId)}` : "/api/alerts/evaluate";
  const response = await fetchWriteWithFallback(path, { method: "POST" });
  return (await response.json()) as AlertEvaluationResponse;
}
