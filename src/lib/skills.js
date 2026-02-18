import crypto from "node:crypto";
import { newId } from "./state.js";

const SKILL_CATALOG = [
  {
    id: "marketing-optimizer",
    version: "1.0.0",
    name: "Marketing Optimizer",
    description: "Optimize spend, ROAS, and lead volume with budget suggestions.",
    triggers: { intents: ["marketing_optimization", "campaign_analysis"], channels: ["web", "slack", "api"] },
    tools: [{ id: "model.run", allow: true }, { id: "reports.generate", allow: true }, { id: "sources.sync", allow: true }],
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
    tools: [{ id: "model.run", allow: true }, { id: "reports.generate", allow: true }],
    guardrails: { confidenceMin: 0.75, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 25000, tokenBudget: 3000, timeBudgetMs: 10000, killSwitch: false },
    prompts: { system: "Prioritize financial clarity, risks, and near-term cash implications." },
    schedules: [{ name: "Daily Finance Pulse", intervalMinutes: 1440 }]
  },
  {
    id: "crm-pipeline-nudger",
    version: "1.0.0",
    name: "CRM Pipeline Nudger",
    description: "Detect funnel bottlenecks and recommend follow-up actions.",
    triggers: { intents: ["pipeline_health", "crm_anomaly"], channels: ["web", "slack", "telegram", "api"] },
    tools: [{ id: "model.run", allow: true }, { id: "notify.owner", allow: true }],
    guardrails: { confidenceMin: 0.65, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 5000, tokenBudget: 2000, timeBudgetMs: 6000, killSwitch: false },
    prompts: { system: "Focus on stage conversion risks and operator tasks." },
    schedules: [{ name: "Pipeline Alert Check", intervalMinutes: 240 }]
  }
];

function manifestSignature(manifest) {
  const canonical = JSON.stringify(manifest, Object.keys(manifest).sort());
  return crypto.createHash("sha256").update(canonical).digest("hex");
}

function clone(value) {
  return JSON.parse(JSON.stringify(value));
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
}

function catalogSkillById(skillId) {
  return SKILL_CATALOG.find((item) => item.id === skillId);
}

export function listSkillCatalog() {
  return SKILL_CATALOG;
}

export function installSkillPack(state, tenant, payload = {}) {
  let manifest = payload.manifest ? clone(payload.manifest) : clone(catalogSkillById(payload.skillId));
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

function classifySkill(installedSkills, payload = {}) {
  if (payload.skillId) {
    const exact = installedSkills.find((item) => item.id === payload.skillId || item.baseId === payload.skillId);
    if (exact) return exact;
  }

  const channel = payload.channel ?? "web";
  const intent = String(payload.intent ?? "").toLowerCase();
  const input = String(payload.input ?? "").toLowerCase();

  const candidates = installedSkills.filter((item) => item.active);
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

  const baseId = selected.baseId;
  const artifacts = {};
  if (adapters.runModelTask) {
    if (baseId === "marketing-optimizer") {
      artifacts.model = adapters.runModelTask(state, tenant, { objective: "forecast", outputMetricIds: ["revenue"], horizonDays: 7 });
    } else if (baseId === "finance-forecast-analyst") {
      artifacts.model = adapters.runModelTask(state, tenant, { objective: "forecast", outputMetricIds: ["profit"], horizonDays: 7 });
    } else {
      artifacts.model = adapters.runModelTask(state, tenant, { objective: "anomaly", outputMetricIds: ["leads"], horizonDays: 7 });
    }
  }

  if (adapters.generateReport && payload.generateReport) {
    artifacts.report = adapters.generateReport(state, tenant, {
      title: payload.reportTitle ?? `${selected.manifest.name} Auto Report`,
      channels: payload.channels ?? ["email"],
      metricIds: payload.metricIds ?? ["revenue", "profit", "spend"]
    });
  }

  const run = {
    id: newId("skill_run"),
    tenantId: tenant.id,
    skillId: selected.id,
    baseId: selected.baseId,
    channel: payload.channel ?? "web",
    intent: payload.intent ?? "unspecified",
    status: "completed",
    confidence: artifacts.model?.insight?.confidence ?? 0.7,
    artifacts,
    trace: {
      routing: {
        requestedSkillId: payload.skillId ?? null,
        selectedSkillId: selected.id,
        channel: payload.channel ?? "web",
        intent: payload.intent ?? "unspecified"
      },
      tools: {
        requested: payload.requestedTools ?? [],
        allowed: selected.manifest.tools.filter((tool) => tool.allow).map((tool) => tool.id)
      },
      guardrails: guardrailChecks
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

  state.skillRuns.push(run);
  return run;
}

export function listSkillRuns(state, tenantId) {
  return state.skillRuns.filter((item) => item.tenantId === tenantId);
}
