import { newId } from "./state.js";
import { queryMetric } from "./metrics.js";
import { evaluateActionPolicy, canAutopilot } from "./policy.js";

function providerHealthKey(tenantId, provider) {
  return `${tenantId}:${provider}`;
}

function providerHealthRecord(state, tenantId, provider) {
  return state.modelProviderHealth.get(providerHealthKey(tenantId, provider)) ?? null;
}

function isProviderCoolingDown(state, tenantId, provider) {
  const record = providerHealthRecord(state, tenantId, provider);
  if (!record?.cooldownUntil) return false;
  return Date.parse(record.cooldownUntil) > Date.now();
}

function markProviderFailure(state, tenantId, provider, cooldownMinutes, reason) {
  const key = providerHealthKey(tenantId, provider);
  const prev = state.modelProviderHealth.get(key) ?? {
    tenantId,
    provider,
    failCount: 0,
    successCount: 0,
    lastError: null,
    cooldownUntil: null
  };
  const failCount = Number(prev.failCount ?? 0) + 1;
  const cooldown = new Date(Date.now() + Math.max(1, Number(cooldownMinutes ?? 10)) * 60_000).toISOString();
  state.modelProviderHealth.set(key, {
    ...prev,
    failCount,
    lastError: String(reason ?? "provider_failure"),
    cooldownUntil: cooldown,
    updatedAt: new Date().toISOString()
  });
}

function markProviderSuccess(state, tenantId, provider) {
  const key = providerHealthKey(tenantId, provider);
  const prev = state.modelProviderHealth.get(key) ?? {
    tenantId,
    provider,
    failCount: 0,
    successCount: 0,
    lastError: null,
    cooldownUntil: null
  };
  state.modelProviderHealth.set(key, {
    ...prev,
    successCount: Number(prev.successCount ?? 0) + 1,
    lastError: null,
    cooldownUntil: null,
    updatedAt: new Date().toISOString()
  });
}

function resolveProviderChain(tenant, task) {
  const chain = [];
  const append = (value) => {
    const provider = String(value ?? "").trim();
    if (!provider) return;
    if (!chain.includes(provider)) chain.push(provider);
  };
  const byo = tenant.modelConfig?.byoProviders ?? [];
  if (task.provider) append(task.provider);
  if (task.preferByo) {
    byo.forEach(append);
  }
  append(tenant.modelConfig?.defaultProvider);
  const configuredChain = Array.isArray(tenant.modelConfig?.failoverChain) ? tenant.modelConfig.failoverChain : [];
  configuredChain.forEach(append);
  append("managed");
  return chain;
}

function chooseProvider(state, tenant, task) {
  const chain = resolveProviderChain(tenant, task);
  const available = chain.find((provider) => !isProviderCoolingDown(state, tenant.id, provider));
  return {
    selected: available ?? "managed",
    chain,
    skippedCooldown: chain.filter((provider) => isProviderCoolingDown(state, tenant.id, provider))
  };
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

  const cooldownMinutes = Number(tenant.modelConfig?.providerCooldownMinutes ?? 10);
  const providerSelection = chooseProvider(state, tenant, task);
  const failoverTrace = [];
  const simulatedFailures = new Set((task.simulateProviderFailures ?? []).map((entry) => String(entry)));
  let provider = providerSelection.selected;
  let providerResolved = false;
  for (const candidate of providerSelection.chain) {
    if (isProviderCoolingDown(state, tenant.id, candidate)) {
      failoverTrace.push({ provider: candidate, outcome: "skipped_cooldown" });
      continue;
    }
    const shouldFail = simulatedFailures.has(candidate) || String(candidate).includes("down");
    if (shouldFail) {
      failoverTrace.push({ provider: candidate, outcome: "failed", reason: "simulated_provider_failure" });
      markProviderFailure(state, tenant.id, candidate, cooldownMinutes, "simulated_provider_failure");
      continue;
    }
    provider = candidate;
    providerResolved = true;
    failoverTrace.push({ provider: candidate, outcome: "selected" });
    markProviderSuccess(state, tenant.id, candidate);
    break;
  }
  if (!providerResolved) {
    provider = "managed";
    failoverTrace.push({ provider: "managed", outcome: "forced_fallback" });
    markProviderSuccess(state, tenant.id, "managed");
    qualityWarnings.push("provider_failover_exhausted_using_managed");
  } else if (failoverTrace.some((entry) => entry.outcome === "failed" || entry.outcome === "skipped_cooldown")) {
    qualityWarnings.push("provider_failover_used");
  }

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
    providerTrace: {
      chain: providerSelection.chain,
      skippedCooldown: providerSelection.skippedCooldown,
      failoverTrace
    },
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
