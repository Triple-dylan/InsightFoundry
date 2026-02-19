import crypto from "node:crypto";
import { newId } from "./state.js";
import { listSkillToolCatalog, runDeterministicSkillTool } from "./skill-compute.js";

const SKILL_CATALOG = [
  {
    id: "marketing-optimizer",
    version: "1.0.0",
    name: "Marketing Optimizer",
    description: "Optimize spend, ROAS, and lead volume with budget suggestions.",
    triggers: { intents: ["marketing_optimization", "campaign_analysis"], channels: ["web", "slack", "api"] },
    tools: [
      { id: "compute.data_quality_snapshot", allow: true },
      { id: "compute.finance_snapshot", allow: true },
      { id: "model.run", allow: true },
      { id: "reports.generate", allow: true },
      { id: "sources.sync", allow: true }
    ],
    guardrails: { confidenceMin: 0.7, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 12000, tokenBudget: 3000, timeBudgetMs: 8000, killSwitch: false },
    prompts: { system: "Focus on marketing ROI and efficiency. Provide actionable recommendations." },
    schedules: [{ name: "Daily Marketing Digest", intervalMinutes: 1440 }]
  },
  {
    id: "finance-forecast-analyst",
    version: "1.0.0",
    name: "Finance Forecast Analyst",
    description: "Generate cash-flow and profit forecasts with variance notes.",
    triggers: { intents: ["finance_forecast", "cashflow_analysis"], channels: ["web", "email", "api"] },
    tools: [
      { id: "compute.finance_snapshot", allow: true },
      { id: "compute.data_quality_snapshot", allow: true },
      { id: "model.run", allow: true },
      { id: "reports.generate", allow: true }
    ],
    guardrails: { confidenceMin: 0.75, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 25000, tokenBudget: 3000, timeBudgetMs: 10000, killSwitch: false },
    prompts: { system: "Prioritize financial clarity, risks, and near-term cash implications." },
    schedules: [{ name: "Daily Finance Pulse", intervalMinutes: 1440 }]
  },
  {
    id: "deal-desk-analyst",
    version: "1.0.0",
    name: "Deal Desk Analyst",
    description: "Run deterministic deal desk policy checks before recommendations.",
    triggers: { intents: ["deal_review", "deal_desk", "pricing_approval"], channels: ["web", "slack", "api"] },
    tools: [
      { id: "compute.deal_desk_snapshot", allow: true },
      { id: "compute.finance_snapshot", allow: true },
      { id: "model.run", allow: true },
      { id: "reports.generate", allow: true }
    ],
    guardrails: { confidenceMin: 0.72, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 30000, tokenBudget: 2500, timeBudgetMs: 9000, killSwitch: false },
    prompts: { system: "Prioritize pricing policy, margin quality, discount governance, and approval readiness." },
    schedules: [{ name: "Deal Desk Policy Scan", intervalMinutes: 360 }]
  },
  {
    id: "data-quality-auditor",
    version: "1.0.0",
    name: "Data Quality Auditor",
    description: "Score data quality and flag reliability risks before modeling.",
    triggers: { intents: ["data_quality", "quality_audit", "reliability_check"], channels: ["web", "api"] },
    tools: [
      { id: "compute.data_quality_snapshot", allow: true },
      { id: "compute.finance_snapshot", allow: true },
      { id: "reports.generate", allow: true }
    ],
    guardrails: { confidenceMin: 0.7, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 5000, tokenBudget: 1800, timeBudgetMs: 6000, killSwitch: false },
    prompts: { system: "Prefer deterministic data quality analysis. Only recommend actions with clear evidence." },
    schedules: [{ name: "Daily Quality Audit", intervalMinutes: 1440 }]
  },
  {
    id: "crm-pipeline-nudger",
    version: "1.0.0",
    name: "CRM Pipeline Nudger",
    description: "Detect funnel bottlenecks and recommend follow-up actions.",
    triggers: { intents: ["pipeline_health", "crm_anomaly"], channels: ["web", "slack", "telegram", "api"] },
    tools: [
      { id: "compute.data_quality_snapshot", allow: true },
      { id: "model.run", allow: true },
      { id: "notify.owner", allow: true }
    ],
    guardrails: { confidenceMin: 0.65, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 5000, tokenBudget: 2000, timeBudgetMs: 6000, killSwitch: false },
    prompts: { system: "Focus on stage conversion risks and operator tasks." },
    schedules: [{ name: "Pipeline Alert Check", intervalMinutes: 240 }]
  }
];

const SOURCE_PRECEDENCE = {
  workspace: 3,
  local: 2,
  bundled: 1
};

function manifestSignature(manifest) {
  const canonical = JSON.stringify(manifest, Object.keys(manifest).sort());
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
}

function precedenceForSource(source) {
  return SOURCE_PRECEDENCE[String(source ?? "bundled")] ?? SOURCE_PRECEDENCE.bundled;
}

function registryEntryFromCatalog(item) {
  return {
    registryId: `bundled:${item.id}@${item.version}`,
    id: item.id,
    version: item.version,
    source: "bundled",
    riskLevel: "medium",
    verified: true,
    tags: ["built-in"],
    manifest: clone(item),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
}

export function ensureSkillRegistry(state) {
  if (!Array.isArray(state.skillRegistry)) {
    state.skillRegistry = [];
  }
  for (const item of SKILL_CATALOG) {
    const exists = state.skillRegistry.some(
      (entry) => entry.id === item.id && entry.version === item.version && entry.source === "bundled"
    );
    if (!exists) {
      state.skillRegistry.push(registryEntryFromCatalog(item));
    }
  }
  return state.skillRegistry;
}

export function listSkillRegistry(state, options = {}) {
  ensureSkillRegistry(state);
  return state.skillRegistry
    .filter((entry) => (options.source ? entry.source === options.source : true))
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function registerSkillInRegistry(state, tenantId, payload = {}) {
  ensureSkillRegistry(state);
  const manifest = clone(payload.manifest ?? {});
  validateSkillManifest(manifest);
  const entry = {
    registryId: newId("skill_registry"),
    id: manifest.id,
    version: manifest.version,
    source: payload.source ?? "workspace",
    riskLevel: payload.riskLevel ?? "medium",
    verified: Boolean(payload.verified ?? false),
    tags: Array.isArray(payload.tags) ? payload.tags.map((tag) => String(tag)) : [],
    tenantScope: tenantId,
    manifest,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  state.skillRegistry.push(entry);
  return entry;
}

function isKnownToolId(toolId) {
  const known = new Set(listSkillToolCatalog().map((item) => item.id));
  return known.has(toolId) || String(toolId).startsWith("custom.");
}

export function validateSkillManifest(manifest) {
  const required = ["id", "version", "name", "description", "triggers", "tools", "guardrails", "prompts", "schedules"];
  for (const key of required) {
    if (!manifest[key]) {
      const err = new Error(`Skill manifest is missing '${key}'`);
      err.statusCode = 400;
      throw err;
    }
  }

  if (!Array.isArray(manifest.tools) || !manifest.tools.length) {
    const err = new Error("Skill manifest must include at least one tool");
    err.statusCode = 400;
    throw err;
  }

  if (!Array.isArray(manifest.triggers?.intents) || !manifest.triggers.intents.length) {
    const err = new Error("Skill manifest must include trigger intents");
    err.statusCode = 400;
    throw err;
  }

  if (!/^[a-z0-9-]{2,80}$/.test(String(manifest.id ?? ""))) {
    const err = new Error("Skill id must match /^[a-z0-9-]{2,80}$/");
    err.statusCode = 400;
    throw err;
  }

  if (!/^\d+\.\d+\.\d+(-[a-z0-9.-]+)?$/i.test(String(manifest.version ?? ""))) {
    const err = new Error("Skill version must be semver-like (x.y.z)");
    err.statusCode = 400;
    throw err;
  }

  const unknownTool = manifest.tools.find((tool) => !tool?.id || !isKnownToolId(tool.id));
  if (unknownTool) {
    const err = new Error(`Skill manifest references unsupported tool '${unknownTool.id}'`);
    err.statusCode = 400;
    throw err;
  }

  const riskLevel = String(manifest.riskLevel ?? "medium");
  if (!["low", "medium", "high"].includes(riskLevel)) {
    const err = new Error("Skill riskLevel must be one of: low, medium, high");
    err.statusCode = 400;
    throw err;
  }

  manifest.riskLevel = riskLevel;
  manifest.guardrails = {
    ...manifest.guardrails,
    tokenBudget: Number(manifest.guardrails?.tokenBudget ?? 2500),
    timeBudgetMs: Number(manifest.guardrails?.timeBudgetMs ?? 8000),
    contextTokenBudget: Number(manifest.guardrails?.contextTokenBudget ?? 1400)
  };
}

function catalogSkillById(skillId) {
  return SKILL_CATALOG.find((item) => item.id === skillId);
}

export function listSkillCatalog() {
  return SKILL_CATALOG;
}

export function listSkillTools() {
  return listSkillToolCatalog();
}

export function installSkillPack(state, tenant, payload = {}) {
  ensureSkillRegistry(state);
  let manifest = null;
  let source = payload.source ?? "bundled";
  let riskLevel = payload.riskLevel ?? "medium";
  if (payload.registryId) {
    const registryItem = state.skillRegistry.find((entry) => entry.registryId === payload.registryId);
    if (!registryItem) {
      const err = new Error(`Registry skill '${payload.registryId}' not found`);
      err.statusCode = 404;
      throw err;
    }
    manifest = clone(registryItem.manifest);
    source = registryItem.source ?? source;
    riskLevel = registryItem.riskLevel ?? riskLevel;
  } else {
    manifest = payload.manifest ? clone(payload.manifest) : clone(catalogSkillById(payload.skillId));
  }
  if (!manifest) {
    const err = new Error(`Skill '${payload.skillId}' not found in catalog`);
    err.statusCode = 404;
    throw err;
  }

  validateSkillManifest(manifest);
  const signature = manifestSignature(manifest);

  const install = {
    installId: newId("skill_install"),
    id: `${manifest.id}@${manifest.version}`,
    baseId: manifest.id,
    version: manifest.version,
    tenantId: tenant.id,
    manifest,
    source,
    riskLevel,
    precedence: precedenceForSource(source),
    signature,
    active: Boolean(payload.active ?? true),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  state.installedSkills.push(install);

  if (install.active) {
    for (const candidate of state.installedSkills) {
      if (candidate.tenantId === tenant.id && candidate.baseId === install.baseId && candidate.id !== install.id) {
        candidate.active = false;
        candidate.updatedAt = new Date().toISOString();
      }
    }
  }

  return install;
}

export function listInstalledSkillPacks(state, tenantId) {
  return state.installedSkills.filter((item) => item.tenantId === tenantId);
}

export function setSkillActivation(state, tenantId, skillId, active) {
  const skill = state.installedSkills.find((item) => item.tenantId === tenantId && item.id === skillId);
  if (!skill) {
    const err = new Error(`Installed skill '${skillId}' not found`);
    err.statusCode = 404;
    throw err;
  }

  skill.active = active;
  skill.updatedAt = new Date().toISOString();
  if (active) {
    for (const candidate of state.installedSkills) {
      if (candidate.tenantId === tenantId && candidate.baseId === skill.baseId && candidate.id !== skill.id) {
        candidate.active = false;
        candidate.updatedAt = new Date().toISOString();
      }
    }
  }

  return skill;
}

function mergePatch(target, patch = {}) {
  for (const [key, value] of Object.entries(patch)) {
    if (value && typeof value === "object" && !Array.isArray(value) && target[key] && typeof target[key] === "object") {
      mergePatch(target[key], value);
    } else {
      target[key] = value;
    }
  }
  return target;
}

export function patchInstalledSkillPack(state, tenantId, skillId, payload = {}) {
  const skill = state.installedSkills.find((item) => item.tenantId === tenantId && item.id === skillId);
  if (!skill) {
    const err = new Error(`Installed skill '${skillId}' not found`);
    err.statusCode = 404;
    throw err;
  }

  if (payload.manifest && typeof payload.manifest === "object") {
    const manifest = clone(skill.manifest);
    mergePatch(manifest, payload.manifest);
    validateSkillManifest(manifest);
    skill.manifest = manifest;
    skill.version = manifest.version;
    skill.baseId = manifest.id;
    skill.signature = manifestSignature(manifest);
  }

  if (typeof payload.active === "boolean") {
    skill.active = payload.active;
  }
  skill.updatedAt = new Date().toISOString();

  if (skill.active) {
    for (const candidate of state.installedSkills) {
      if (candidate.tenantId === tenantId && candidate.baseId === skill.baseId && candidate.id !== skill.id) {
        candidate.active = false;
        candidate.updatedAt = new Date().toISOString();
      }
    }
  }

  return skill;
}

function classifySkill(installedSkills, payload = {}) {
  if (payload.skillId) {
    const exact = installedSkills.find((item) => item.id === payload.skillId || item.baseId === payload.skillId);
    if (exact) return exact;
  }

  const channel = payload.channel ?? "web";
  const intent = String(payload.intent ?? "").toLowerCase();
  const input = String(payload.input ?? "").toLowerCase();

  const candidates = installedSkills
    .filter((item) => item.active)
    .sort((a, b) => Number(b.precedence ?? 0) - Number(a.precedence ?? 0));
  let best = null;
  let bestScore = -1;

  for (const skill of candidates) {
    let score = 0;
    if (skill.manifest.triggers.channels.includes(channel)) score += 1;
    for (const declaredIntent of skill.manifest.triggers.intents) {
      const token = String(declaredIntent).toLowerCase();
      if (intent.includes(token) || input.includes(token.replaceAll("_", " "))) score += 3;
    }

    if (score > bestScore) {
      best = skill;
      bestScore = score;
    }
  }

  return best ?? null;
}

function assertSkillSafety(tenant, skill, payload = {}) {
  const checks = [];
  if (tenant.autonomyPolicy.killSwitch) {
    checks.push({ check: "tenant_kill_switch", status: "fail", detail: "Tenant kill switch is enabled" });
    const err = new Error("Tenant kill switch is enabled");
    err.statusCode = 403;
    err.checks = checks;
    throw err;
  }
  checks.push({ check: "tenant_kill_switch", status: "pass", detail: "Tenant kill switch is off" });

  if (skill.manifest.guardrails.killSwitch) {
    checks.push({ check: "skill_kill_switch", status: "fail", detail: "Skill kill switch is enabled" });
    const err = new Error("Skill kill switch is enabled");
    err.statusCode = 403;
    err.checks = checks;
    throw err;
  }
  checks.push({ check: "skill_kill_switch", status: "pass", detail: "Skill kill switch is off" });

  const requestedTools = payload.requestedTools ?? [];
  const allowedTools = new Set(skill.manifest.tools.filter((t) => t.allow).map((t) => t.id));
  for (const tool of requestedTools) {
    if (!allowedTools.has(tool)) {
      checks.push({ check: "tool_allowlist", status: "fail", detail: `Tool '${tool}' is not allowed` });
      const err = new Error(`Skill tool '${tool}' is not allowed by manifest policy`);
      err.statusCode = 403;
      err.checks = checks;
      throw err;
    }
  }
  checks.push({ check: "tool_allowlist", status: "pass", detail: "All requested tools are allowlisted" });

  const tokenEstimate = Number(payload.estimatedTokens ?? 0);
  if (tokenEstimate > Number(skill.manifest.guardrails.tokenBudget ?? 0)) {
    checks.push({
      check: "token_budget",
      status: "fail",
      detail: `${tokenEstimate} exceeds ${Number(skill.manifest.guardrails.tokenBudget ?? 0)}`
    });
    const err = new Error("Skill token budget exceeded");
    err.statusCode = 400;
    err.checks = checks;
    throw err;
  }
  checks.push({ check: "token_budget", status: "pass", detail: `${tokenEstimate} within budget` });

  const contextTokenEstimate = Number(payload.contextTokensEstimate ?? 0);
  const contextBudget = Number(skill.manifest.guardrails.contextTokenBudget ?? 1400);
  if (contextTokenEstimate > contextBudget) {
    checks.push({
      check: "context_token_budget",
      status: "fail",
      detail: `${contextTokenEstimate} exceeds ${contextBudget}`
    });
    const err = new Error("Skill context token budget exceeded");
    err.statusCode = 400;
    err.checks = checks;
    throw err;
  }
  checks.push({ check: "context_token_budget", status: "pass", detail: `${contextTokenEstimate} within budget` });

  const timeoutMs = Number(payload.timeoutMs ?? 0);
  if (timeoutMs > Number(skill.manifest.guardrails.timeBudgetMs ?? 0)) {
    checks.push({
      check: "time_budget",
      status: "fail",
      detail: `${timeoutMs} exceeds ${Number(skill.manifest.guardrails.timeBudgetMs ?? 0)}`
    });
    const err = new Error("Skill time budget exceeded");
    err.statusCode = 400;
    err.checks = checks;
    throw err;
  }
  checks.push({ check: "time_budget", status: "pass", detail: `${timeoutMs} within budget` });
  return checks;
}

export function runSkillPack(state, tenant, payload = {}, adapters = {}) {
  ensureSkillRegistry(state);
  const installed = listInstalledSkillPacks(state, tenant.id);
  const selected = classifySkill(installed, payload);

  if (!selected) {
    const err = new Error("No installed active skill matched this request");
    err.statusCode = 404;
    throw err;
  }

  if (!selected.active) {
    const err = new Error(`Skill '${selected.id}' is not active`);
    err.statusCode = 400;
    throw err;
  }

  const signature = manifestSignature(selected.manifest);
  if (signature !== selected.signature) {
    const err = new Error("Skill manifest signature verification failed");
    err.statusCode = 403;
    throw err;
  }

  const guardrailChecks = assertSkillSafety(tenant, selected, payload);

  const runtimeEnv = Object.fromEntries(
    Object.entries(payload.runtimeEnv ?? {})
      .filter(([key]) => /^((SKILL|TENANT|MODEL)_|OPENAI_)/.test(String(key)))
      .slice(0, 24)
      .map(([key, value]) => [String(key), String(value).slice(0, 320)])
  );

  const allowedTools = selected.manifest.tools.filter((tool) => tool.allow).map((tool) => tool.id);
  const requestedTools = (payload.requestedTools ?? []).length ? payload.requestedTools : allowedTools;
  const deterministicOutputs = {};
  const modelRuns = [];
  const reportRuns = [];

  for (const toolId of requestedTools) {
    if (!allowedTools.includes(toolId)) continue;
    const deterministic = runDeterministicSkillTool(state, tenant, toolId, payload);
    if (deterministic) {
      deterministicOutputs[toolId] = deterministic;
      continue;
    }
    if (toolId === "model.run" && adapters.runModelTask) {
      const objective = /anomaly|quality/i.test(String(payload.intent ?? "")) ? "anomaly" : "forecast";
      const metricId = selected.baseId === "finance-forecast-analyst"
        ? "profit"
        : selected.baseId === "deal-desk-analyst"
          ? "revenue"
          : "revenue";
      const model = adapters.runModelTask(state, tenant, {
        objective,
        outputMetricIds: [metricId],
        horizonDays: Number(payload.horizonDays ?? 7),
        provider: payload.provider
      });
      modelRuns.push(model);
    }
    if (toolId === "reports.generate" && adapters.generateReport && payload.generateReport) {
      const report = adapters.generateReport(state, tenant, {
        title: payload.reportTitle ?? `${selected.manifest.name} Auto Report`,
        channels: payload.channels ?? ["email"],
        metricIds: payload.metricIds ?? ["revenue", "profit", "spend"],
        summaryPreface: payload.summaryPreface
      });
      reportRuns.push(report);
    }
  }

  const primaryModel = modelRuns[0] ?? null;
  const qualitySnapshot = deterministicOutputs["compute.data_quality_snapshot"]?.summary ?? null;
  const artifacts = {
    deterministicOutputs,
    model: primaryModel,
    models: modelRuns,
    report: reportRuns[0] ?? null,
    reports: reportRuns
  };

  const run = {
    id: newId("skill_run"),
    tenantId: tenant.id,
    skillId: selected.id,
    baseId: selected.baseId,
    channel: payload.channel ?? "web",
    intent: payload.intent ?? "unspecified",
    status: "completed",
    confidence: primaryModel?.insight?.confidence ?? 0.7,
    artifacts,
    trace: {
      routing: {
        requestedSkillId: payload.skillId ?? null,
        selectedSkillId: selected.id,
        selectedSkillSource: selected.source ?? "bundled",
        selectedSkillRiskLevel: selected.riskLevel ?? selected.manifest?.riskLevel ?? "medium",
        channel: payload.channel ?? "web",
        intent: payload.intent ?? "unspecified"
      },
      tools: {
        requested: requestedTools,
        allowed: allowedTools,
        deterministicExecuted: Object.keys(deterministicOutputs)
      },
      guardrails: guardrailChecks,
      runtimeEnv: {
        injected: Object.keys(runtimeEnv),
        masked: true
      }
    },
    reasoningHints: {
      deterministicFirst: true,
      tokenEfficiency: {
        rawRowsAvoided: qualitySnapshot ? qualitySnapshot.totalRows : 0,
        precomputedBlocks: Object.keys(deterministicOutputs).length
      }
    },
    createdAt: new Date().toISOString()
  };

  if (run.confidence < Number(selected.manifest.guardrails.confidenceMin ?? 0)) {
    run.status = "completed_with_warning";
    run.warning = "confidence_below_skill_threshold";
    run.trace.guardrails.push({
      check: "confidence_threshold",
      status: "warn",
      detail: `${run.confidence} below ${Number(selected.manifest.guardrails.confidenceMin ?? 0)}`
    });
  } else {
    run.trace.guardrails.push({
      check: "confidence_threshold",
      status: "pass",
      detail: `${run.confidence} meets threshold`
    });
  }

  if (qualitySnapshot && Number(qualitySnapshot.qualityScore ?? 1) < 0.7) {
    run.status = "completed_with_warning";
    run.warning = run.warning ?? "low_data_quality";
    run.trace.guardrails.push({
      check: "data_quality",
      status: "warn",
      detail: `quality score ${Number(qualitySnapshot.qualityScore).toFixed(2)} below 0.70`
    });
  }

  state.skillRuns.push(run);
  return run;
}

export function listSkillRuns(state, tenantId) {
  return state.skillRuns.filter((item) => item.tenantId === tenantId);
}
