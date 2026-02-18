import { newId } from "./state.js";
import { queryMetric } from "./metrics.js";
import { evaluateActionPolicy, canAutopilot } from "./policy.js";

function chooseProvider(tenant, task) {
  const byo = tenant.modelConfig?.byoProviders ?? [];
  if (task.provider && (task.provider === "managed" || byo.includes(task.provider))) {
    return task.provider;
  }
  if (byo.length > 0 && task.preferByo) {
    return byo[0];
  }
  return "managed";
}

function linearForecast(series, horizon = 7) {
  if (series.length < 2) {
    return { points: [], quality: "insufficient_data" };
  }
  const n = series.length;
  const first = series[0].value;
  const last = series[n - 1].value;
  const slope = (last - first) / Math.max(1, n - 1);

  const points = [];
  for (let i = 1; i <= horizon; i += 1) {
    points.push({ step: i, value: Number((last + slope * i).toFixed(3)) });
  }
  return { points, quality: "ok" };
}

function detectAnomalies(series) {
  if (series.length < 10) return [];
  const values = series.map((p) => p.value);
  const mean = values.reduce((s, v) => s + v, 0) / values.length;
  const variance = values.reduce((s, v) => s + (v - mean) ** 2, 0) / values.length;
  const stdev = Math.sqrt(variance);
  return series
    .filter((p) => Math.abs(p.value - mean) > stdev * 1.8)
    .map((p) => ({ bucket: p.bucket, value: p.value, zHint: Number(((p.value - mean) / (stdev || 1)).toFixed(2)) }));
}

function proposeActions(task, confidence) {
  if (task.objective === "forecast") {
    return [
      {
        id: newId("action"),
        actionType: "adjust_budget",
        targetSystem: "google_ads",
        requiresApproval: true,
        estimatedBudgetImpactUsd: 2500,
        confidence
      },
      {
        id: newId("action"),
        actionType: "create_report",
        targetSystem: "reporting",
        requiresApproval: false,
        estimatedBudgetImpactUsd: 0,
        confidence
      }
    ];
  }

  return [
    {
      id: newId("action"),
      actionType: "notify_owner",
      targetSystem: "slack",
      requiresApproval: false,
      estimatedBudgetImpactUsd: 0,
      confidence
    }
  ];
}

export function runModelTask(state, tenant, task) {
  const runId = newId("model");
  const metricId = task.outputMetricIds?.[0] ?? task.inputs?.[0] ?? "revenue";
  const history = queryMetric(state, tenant.id, { metricId, grain: "day" });
  const points = history.series;
  const qualityWarnings = [];

  if (points.length < 14) {
    qualityWarnings.push("insufficient_history_for_reliable_modeling");
  }

  const provider = chooseProvider(tenant, task);
  const forecast = linearForecast(points, Number(task.horizonDays ?? 7));
  const anomalies = task.objective === "anomaly" ? detectAnomalies(points) : [];

  const confidenceBase = points.length >= 30 ? 0.84 : points.length >= 14 ? 0.72 : 0.54;
  const confidence = Number((confidenceBase - qualityWarnings.length * 0.1).toFixed(2));

  const proposed = proposeActions(task, confidence);
  const recommendedActions = proposed.map((action) => {
    const policy = evaluateActionPolicy(tenant, action);
    return {
      id: action.id,
      actionType: action.actionType,
      targetSystem: action.targetSystem,
      requiresApproval: action.requiresApproval,
      policyDecision: policy.decision,
      policyReason: policy.reason,
      confidence: action.confidence,
      estimatedBudgetImpactUsd: action.estimatedBudgetImpactUsd,
      executionState: canAutopilot(tenant, policy) ? "executed" : "pending"
    };
  });

  const insight = {
    id: newId("insight"),
    tenantId: tenant.id,
    modelRunId: runId,
    severity: confidence >= 0.8 ? "low" : confidence >= 0.65 ? "medium" : "high",
    confidence,
    summary:
      task.objective === "forecast"
        ? `Forecast for ${metricId} produced ${forecast.points.length} forward points.`
        : `Anomaly scan found ${anomalies.length} anomaly candidates for ${metricId}.`,
    objective: task.objective,
    metricId,
    recommendedActions,
    forecast,
    anomalies,
    qualityWarnings,
    createdAt: new Date().toISOString()
  };

  const run = {
    id: runId,
    tenantId: tenant.id,
    objective: task.objective,
    provider,
    metricId,
    status: qualityWarnings.length ? "completed_with_warnings" : "completed",
    createdAt: new Date().toISOString(),
    qualityWarnings
  };

  state.modelRuns.push(run);
  state.insights.push(insight);

  return {
    run,
    insight
  };
}
