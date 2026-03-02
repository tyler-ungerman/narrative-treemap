"use client";

import { FormEvent, useEffect, useMemo, useState } from "react";

import {
  deleteAlertRule,
  evaluateAlertRules,
  fetchAlertRules,
  saveAlertRule,
  updateAlertRule
} from "@/lib/api";
import { formatGeneratedTime, formatPercent } from "@/lib/format";
import { AlertChannelType, AlertRule, TopicFilters, WindowOption } from "@/lib/types";

interface AlertRulesPanelProps {
  filters: TopicFilters;
  availableVerticals: string[];
  availableSources: string[];
}

interface AlertFormState {
  name: string;
  channelType: AlertChannelType;
  endpointUrl: string;
  window: WindowOption;
  momentumThreshold: string;
  diversityThreshold: string;
  minQualityScore: string;
  verticals: string[];
  sources: string[];
  enabled: boolean;
}

const INITIAL_FORM: AlertFormState = {
  name: "Rising narratives",
  channelType: "webhook",
  endpointUrl: "",
  window: "24h",
  momentumThreshold: "0.25",
  diversityThreshold: "3",
  minQualityScore: "0.80",
  verticals: [],
  sources: [],
  enabled: true
};

export function AlertRulesPanel({ filters, availableVerticals, availableSources }: AlertRulesPanelProps) {
  const [rules, setRules] = useState<AlertRule[]>([]);
  const [events, setEvents] = useState<
    Array<{
      event_id: string;
      rule_id: string;
      topic_id: string;
      topic_label: string;
      channel_type: string;
      delivery_status: string;
      delivery_error: string | null;
      triggered_at: string;
    }>
  >([]);
  const [form, setForm] = useState<AlertFormState>({ ...INITIAL_FORM, window: filters.window });
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [evaluationStatus, setEvaluationStatus] = useState<string>("");

  const loadRules = async () => {
    setLoading(true);
    setError(null);
    try {
      const payload = await fetchAlertRules(80);
      setRules(payload.rules);
      setEvents(payload.events.map((event) => ({ ...event })));
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : "Unable to load alerts");
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadRules();
  }, []);

  const verticalOptions = useMemo(() => availableVerticals.slice(0, 20), [availableVerticals]);
  const sourceOptions = useMemo(() => availableSources.slice(0, 40), [availableSources]);

  const submitRule = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setSaving(true);
    setError(null);
    try {
      await saveAlertRule({
        name: form.name,
        channel_type: form.channelType,
        endpoint_url: form.endpointUrl,
        window: form.window,
        momentum_threshold: Number(form.momentumThreshold),
        diversity_threshold: Number(form.diversityThreshold),
        min_quality_score: Number(form.minQualityScore),
        verticals: form.verticals,
        sources: form.sources,
        enabled: form.enabled
      });
      setForm({ ...INITIAL_FORM, window: filters.window });
      await loadRules();
    } catch (saveError) {
      setError(saveError instanceof Error ? saveError.message : "Unable to save alert rule");
    } finally {
      setSaving(false);
    }
  };

  const toggleRule = async (rule: AlertRule) => {
    try {
      await updateAlertRule(rule.rule_id, {
        name: rule.name,
        channel_type: rule.channel_type,
        endpoint_url: rule.endpoint_url,
        window: rule.window,
        momentum_threshold: rule.momentum_threshold,
        diversity_threshold: rule.diversity_threshold,
        min_quality_score: rule.min_quality_score,
        verticals: rule.verticals,
        sources: rule.sources,
        enabled: !rule.enabled
      });
      await loadRules();
    } catch (updateError) {
      setError(updateError instanceof Error ? updateError.message : "Unable to update alert rule");
    }
  };

  const removeRule = async (ruleId: string) => {
    try {
      await deleteAlertRule(ruleId);
      await loadRules();
    } catch (deleteError) {
      setError(deleteError instanceof Error ? deleteError.message : "Unable to delete alert rule");
    }
  };

  const runEvaluation = async (ruleId?: string) => {
    setEvaluationStatus("Evaluating...");
    try {
      const payload = await evaluateAlertRules(ruleId);
      setEvaluationStatus(
        `Sent ${payload.total_sent}, failed ${payload.total_failed}, candidates ${payload.total_candidates} (${formatGeneratedTime(payload.generated_at)})`
      );
      await loadRules();
    } catch (evalError) {
      setEvaluationStatus("Evaluation failed");
      setError(evalError instanceof Error ? evalError.message : "Unable to evaluate alert rules");
    }
  };

  return (
    <section className="analytics-panel">
      <header className="analytics-header">
        <div>
          <p className="hero-kicker">Alert Rules</p>
          <h2>Notify when narratives cross thresholds</h2>
          <p>Define momentum/diversity triggers and deliver to webhook, Discord, or Slack endpoints.</p>
        </div>
        <div className="decision-export-controls">
          <button type="button" onClick={() => runEvaluation()} disabled={loading}>
            Evaluate now
          </button>
        </div>
      </header>

      {error ? <p className="error-banner">{error}</p> : null}
      {evaluationStatus ? <p className="decision-export-note">{evaluationStatus}</p> : null}

      {loading ? (
        <div className="decision-loading">
          <p>Loading alert rules...</p>
        </div>
      ) : (
        <div className="analytics-grid">
          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Create rule</h3>
              <p>New triggers use your current filter window by default.</p>
            </div>
            <form className="alert-form" onSubmit={submitRule}>
              <label>
                Name
                <input value={form.name} onChange={(event) => setForm((prev) => ({ ...prev, name: event.target.value }))} />
              </label>
              <label>
                Channel
                <select
                  value={form.channelType}
                  onChange={(event) => setForm((prev) => ({ ...prev, channelType: event.target.value as AlertChannelType }))}
                >
                  <option value="webhook">Webhook</option>
                  <option value="discord">Discord</option>
                  <option value="slack">Slack</option>
                </select>
              </label>
              <label>
                Endpoint URL
                <input
                  value={form.endpointUrl}
                  placeholder="https://..."
                  onChange={(event) => setForm((prev) => ({ ...prev, endpointUrl: event.target.value }))}
                />
              </label>
              <div className="alert-form-grid">
                <label>
                  Window
                  <select value={form.window} onChange={(event) => setForm((prev) => ({ ...prev, window: event.target.value as WindowOption }))}>
                    <option value="1h">1h</option>
                    <option value="6h">6h</option>
                    <option value="24h">24h</option>
                    <option value="7d">7d</option>
                  </select>
                </label>
                <label>
                  Momentum ≥
                  <input
                    value={form.momentumThreshold}
                    onChange={(event) => setForm((prev) => ({ ...prev, momentumThreshold: event.target.value }))}
                  />
                </label>
                <label>
                  Diversity ≥
                  <input
                    value={form.diversityThreshold}
                    onChange={(event) => setForm((prev) => ({ ...prev, diversityThreshold: event.target.value }))}
                  />
                </label>
                <label>
                  Min quality ≥
                  <input value={form.minQualityScore} onChange={(event) => setForm((prev) => ({ ...prev, minQualityScore: event.target.value }))} />
                </label>
              </div>
              <label>
                Verticals (optional)
                <select
                  multiple
                  value={form.verticals}
                  onChange={(event) =>
                    setForm((prev) => ({
                      ...prev,
                      verticals: Array.from(event.target.selectedOptions).map((option) => option.value)
                    }))
                  }
                >
                  {verticalOptions.map((vertical) => (
                    <option key={vertical} value={vertical}>
                      {vertical}
                    </option>
                  ))}
                </select>
              </label>
              <label>
                Sources (optional)
                <select
                  multiple
                  value={form.sources}
                  onChange={(event) =>
                    setForm((prev) => ({
                      ...prev,
                      sources: Array.from(event.target.selectedOptions).map((option) => option.value)
                    }))
                  }
                >
                  {sourceOptions.map((source) => (
                    <option key={source} value={source}>
                      {source}
                    </option>
                  ))}
                </select>
              </label>
              <label className="alert-form-toggle">
                <input
                  type="checkbox"
                  checked={form.enabled}
                  onChange={(event) => setForm((prev) => ({ ...prev, enabled: event.target.checked }))}
                />
                Enabled
              </label>
              <button type="submit" disabled={saving}>
                {saving ? "Saving..." : "Create alert rule"}
              </button>
            </form>
          </section>

          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Rules</h3>
              <p>{rules.length} configured</p>
            </div>
            <ul className="decision-list">
              {rules.map((rule) => (
                <li key={rule.rule_id} className="decision-item">
                  <div className="decision-item-head">
                    <span>{rule.name}</span>
                    <span className={rule.enabled ? "decision-badge act-now" : "decision-badge ignore"}>
                      {rule.enabled ? "Enabled" : "Disabled"}
                    </span>
                  </div>
                  <p className="decision-item-metrics">
                    {rule.channel_type} · {rule.window} · momentum {formatPercent(rule.momentum_threshold)} · diversity{" "}
                    {rule.diversity_threshold} · quality {rule.min_quality_score.toFixed(2)}
                  </p>
                  <p className="decision-item-next">
                    Last trigger: {rule.last_triggered_at ? formatGeneratedTime(rule.last_triggered_at) : "never"}
                  </p>
                  <div className="decision-export-controls">
                    <button type="button" onClick={() => toggleRule(rule)}>
                      {rule.enabled ? "Disable" : "Enable"}
                    </button>
                    <button type="button" onClick={() => runEvaluation(rule.rule_id)}>
                      Test now
                    </button>
                    <button type="button" onClick={() => removeRule(rule.rule_id)}>
                      Delete
                    </button>
                  </div>
                </li>
              ))}
              {rules.length === 0 ? <li className="decision-empty">No alert rules yet.</li> : null}
            </ul>
          </section>

          <section className="decision-panel">
            <div className="decision-panel-head">
              <h3>Recent alert events</h3>
              <p>Delivery logs with status.</p>
            </div>
            <ul className="decision-list">
              {events.slice(0, 40).map((event) => (
                <li key={event.event_id} className="decision-item">
                  <div className="decision-item-head">
                    <span>{event.topic_label}</span>
                    <span className={event.delivery_status === "sent" ? "decision-badge act-now" : "decision-badge ignore"}>
                      {event.delivery_status}
                    </span>
                  </div>
                  <p className="decision-item-metrics">
                    Rule {event.rule_id} · {event.channel_type} · {formatGeneratedTime(event.triggered_at)}
                  </p>
                  {event.delivery_error ? <p className="decision-item-risk">{event.delivery_error}</p> : null}
                </li>
              ))}
              {events.length === 0 ? <li className="decision-empty">No alert events yet.</li> : null}
            </ul>
          </section>
        </div>
      )}
    </section>
  );
}
