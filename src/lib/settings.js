import { newId } from "./state.js";

function defaultGeneral(tenant) {
  return {
    name: tenant.name,
    timezone: "America/New_York",
    locale: "en-US",
    onboardingMode: "quickstart",
    branding: {
      logoUrl: tenant.branding?.logoUrl ?? "",
      primary: tenant.branding?.theme?.primary ?? "#0c8f6b"
    }
  };
}

function defaultModelPreferences(tenant) {
  return {
    llmMode: tenant.modelConfig?.mode ?? "managed",
    defaultProvider: "managed",
    defaultProfileId: null,
    byoKeyRefs: [],
    failoverChain: ["managed"],
    providerCooldownMinutes: 10,
    authProfiles: []
  };
}

function defaultTraining(tenant) {
  return {
    optIn: Boolean(tenant.trainingOptIn),
    allowTenantFineTuning: false,
    schedule: { intervalHours: 24 }
  };
}

function defaultChannels() {
  return {
    slack: { enabled: false, webhookRef: "", template: "Insight summary: {{summary}}" },
    telegram: { enabled: false, botTokenRef: "", chatId: "", template: "Insight summary: {{summary}}" },
    reliability: {
      email: { maxAttempts: 2, baseDelayMs: 15000 },
      slack: { maxAttempts: 3, baseDelayMs: 20000 },
      telegram: { maxAttempts: 4, baseDelayMs: 30000 }
    }
  };
}

function defaultPolicies(tenant) {
  return {
    autonomyMode: tenant.autonomyPolicy?.autonomyMode ?? "policy-gated",
    confidenceThreshold: tenant.autonomyPolicy?.confidenceThreshold ?? 0.76,
    budgetGuardrailUsd: tenant.autonomyPolicy?.budgetGuardrailUsd ?? 10000,
    killSwitch: Boolean(tenant.autonomyPolicy?.killSwitch),
    securityAuditIntervalHours: 24,
    doctorAutoRun: false
  };
}

function defaultChecklist(state, tenant) {
  const hasConnection = state.sourceConnections.some((item) => item.tenantId === tenant.id);
  const hasModelProfile = state.modelProfiles.some((item) => item.tenantId === tenant.id);
  const hasReportType = state.reportTypes.some((item) => item.tenantId === tenant.id);
  return {
    connectionsConfigured: hasConnection,
    modelProfileConfigured: hasModelProfile,
    reportTypeConfigured: hasReportType,
    channelsConfigured: false
  };
}

export function ensureTenantSettings(state, tenant) {
  const existing = state.settingsByTenant.get(tenant.id);
  if (existing) {
    existing.checklist = {
      ...existing.checklist,
      connectionsConfigured: state.sourceConnections.some((item) => item.tenantId === tenant.id),
      modelProfileConfigured: state.modelProfiles.some((item) => item.tenantId === tenant.id),
      reportTypeConfigured: state.reportTypes.some((item) => item.tenantId === tenant.id)
    };
    return existing;
  }

  const settings = {
    id: newId("settings"),
    tenantId: tenant.id,
    general: defaultGeneral(tenant),
    modelPreferences: defaultModelPreferences(tenant),
    training: defaultTraining(tenant),
    channels: defaultChannels(),
    policies: defaultPolicies(tenant),
    checklist: defaultChecklist(state, tenant),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  state.settingsByTenant.set(tenant.id, settings);
  return settings;
}

function patch(target, payload) {
  if (!payload || typeof payload !== "object") return target;
  for (const [key, value] of Object.entries(payload)) {
    if (value && typeof value === "object" && !Array.isArray(value) && target[key] && typeof target[key] === "object") {
      patch(target[key], value);
    } else {
      target[key] = value;
    }
  }
  return target;
}

export function getTenantSettings(state, tenant) {
  return ensureTenantSettings(state, tenant);
}

export function patchSettingsGeneral(state, tenant, payload) {
  const settings = ensureTenantSettings(state, tenant);
  patch(settings.general, payload);
  if (payload?.name) {
    tenant.name = payload.name;
  }
  settings.updatedAt = new Date().toISOString();
  return settings;
}

export function patchSettingsModelPreferences(state, tenant, payload) {
  const settings = ensureTenantSettings(state, tenant);
  patch(settings.modelPreferences, payload);
  tenant.modelConfig.mode = settings.modelPreferences.llmMode;
  tenant.modelConfig.defaultProvider = settings.modelPreferences.defaultProvider;
  tenant.modelConfig.failoverChain = Array.isArray(settings.modelPreferences.failoverChain)
    ? settings.modelPreferences.failoverChain
    : ["managed"];
  tenant.modelConfig.providerCooldownMinutes = Number(settings.modelPreferences.providerCooldownMinutes ?? 10);
  tenant.modelConfig.authProfiles = Array.isArray(settings.modelPreferences.authProfiles)
    ? settings.modelPreferences.authProfiles
    : [];
  tenant.modelConfig.byoProviders = Array.isArray(settings.modelPreferences.byoKeyRefs)
    ? settings.modelPreferences.byoKeyRefs.map((item) => String(item).split(":")[0]).filter(Boolean)
    : tenant.modelConfig.byoProviders;
  settings.updatedAt = new Date().toISOString();
  return settings;
}

export function patchSettingsTraining(state, tenant, payload) {
  const settings = ensureTenantSettings(state, tenant);
  patch(settings.training, payload);
  tenant.trainingOptIn = Boolean(settings.training.optIn);
  settings.updatedAt = new Date().toISOString();
  return settings;
}

export function patchSettingsPolicies(state, tenant, payload) {
  const settings = ensureTenantSettings(state, tenant);
  patch(settings.policies, payload);
  tenant.autonomyPolicy.autonomyMode = settings.policies.autonomyMode;
  tenant.autonomyPolicy.confidenceThreshold = settings.policies.confidenceThreshold;
  tenant.autonomyPolicy.budgetGuardrailUsd = settings.policies.budgetGuardrailUsd;
  tenant.autonomyPolicy.killSwitch = settings.policies.killSwitch;
  tenant.autonomyPolicy.securityAuditIntervalHours = Number(settings.policies.securityAuditIntervalHours ?? 24);
  tenant.autonomyPolicy.doctorAutoRun = Boolean(settings.policies.doctorAutoRun);
  settings.updatedAt = new Date().toISOString();
  return settings;
}

export function getSettingsChannels(state, tenant) {
  const settings = ensureTenantSettings(state, tenant);
  return settings.channels;
}

export function patchSettingsChannels(state, tenant, payload) {
  const settings = ensureTenantSettings(state, tenant);
  patch(settings.channels, payload);
  settings.checklist.channelsConfigured = Boolean(
    settings.channels.slack?.enabled || settings.channels.telegram?.enabled
  );
  settings.updatedAt = new Date().toISOString();
  return settings.channels;
}
