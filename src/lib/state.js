import crypto from "node:crypto";
import { getBlueprint } from "./blueprints.js";

function id(prefix) {
  return `${prefix}_${crypto.randomUUID()}`;
}

function defaultPolicy() {
  return {
    autonomyMode: "policy-gated",
    autopilotEnabled: true,
    confidenceThreshold: 0.76,
    actionAllowlist: ["adjust_budget", "notify_owner", "create_report"],
    highImpactActions: ["adjust_budget"],
    budgetGuardrailUsd: 10000,
    killSwitch: false
  };
}

function defaultDataPolicy() {
  return {
    maxLiveQueryRows: 500,
    maxLiveQueryTimeoutMs: 10000,
    maxLiveQueryCostUnits: 1000
  };
}

function defaultBranding(name) {
  return {
    companyName: name,
    logoUrl: "",
    theme: {
      primary: "#0c8f6b",
      background: "#f4f8f7",
      panel: "#ffffff",
      text: "#13201d"
    },
    customDomain: ""
  };
}

export function createState() {
  return {
    tenants: new Map(),
    metricsByTenant: new Map(),
    facts: [],
    factKeys: new Set(),
    connectorRuns: [],
    sourceConnections: [],
    sourceConnectionRuns: [],
    secretRefs: new Map(),
    liveQueryCache: new Map(),
    materializationRuns: [],
    installedSkills: [],
    skillDrafts: [],
    skillRuns: [],
    modelProfiles: [],
    reportTypes: [],
    analysisRuns: [],
    settingsByTenant: new Map(),
    featureFlags: {
      ui_rehaul_enabled: true,
      runs_engine_enabled: true,
      skills_builder_v2_enabled: true
    },
    modelRuns: [],
    insights: [],
    reports: [],
    reportSchedules: [],
    agentJobs: [],
    actionApprovals: [],
    auditEvents: [],
    channelEvents: [],
    sentReportRuns: new Set()
  };
}

export function createTenant(state, payload) {
  const now = new Date().toISOString();
  const blueprint = getBlueprint(payload.blueprintId);
  const tenant = {
    id: id("tenant"),
    name: payload.name,
    status: "active",
    blueprintId: blueprint.id,
    domains: blueprint.domains,
    branding: payload.branding ?? defaultBranding(payload.name),
    trainingOptIn: Boolean(payload.trainingOptIn),
    modelConfig: {
      mode: "managed",
      byoProviders: [],
      ...(payload.modelConfig ?? {})
    },
    autonomyPolicy: {
      ...defaultPolicy(),
      ...(payload.autonomyPolicy ?? {})
    },
    dataPolicy: {
      ...defaultDataPolicy(),
      ...(payload.dataPolicy ?? {})
    },
    createdAt: now,
    updatedAt: now
  };

  state.tenants.set(tenant.id, tenant);
  state.metricsByTenant.set(tenant.id, blueprint.metrics);
  return tenant;
}

export function getTenant(state, tenantId) {
  return state.tenants.get(tenantId);
}

export function requireTenant(state, tenantId) {
  const tenant = getTenant(state, tenantId);
  if (!tenant) {
    const err = new Error(`Unknown tenant '${tenantId}'`);
    err.statusCode = 404;
    throw err;
  }
  return tenant;
}

export function newId(prefix) {
  return id(prefix);
}
