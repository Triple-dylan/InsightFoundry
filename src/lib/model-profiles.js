import { newId } from "./state.js";
import { ensureTenantSettings } from "./settings.js";

const PRESET_PROFILES = [
  {
    name: "Revenue Forecast",
    objective: "forecast",
    targetMetricId: "revenue",
    horizonDays: 14,
    provider: "managed",
    params: { confidenceTarget: 0.78, seasonality: true }
  },
  {
    name: "Profit Forecast",
    objective: "forecast",
    targetMetricId: "profit",
    horizonDays: 14,
    provider: "managed",
    params: { confidenceTarget: 0.75, seasonality: true }
  },
  {
    name: "Funnel Anomaly",
    objective: "anomaly",
    targetMetricId: "leads",
    horizonDays: 7,
    provider: "managed",
    params: { sensitivity: 1.8 }
  },
  {
    name: "Pipeline Risk",
    objective: "anomaly",
    targetMetricId: "deals",
    horizonDays: 7,
    provider: "managed",
    params: { sensitivity: 1.6 }
  }
];

export function listPresetProfiles() {
  return PRESET_PROFILES;
}

function normalizeProfile(tenant, payload = {}) {
  return {
    id: newId("profile"),
    tenantId: tenant.id,
    name: payload.name ?? "Custom Profile",
    objective: payload.objective ?? "forecast",
    targetMetricId: payload.targetMetricId ?? "revenue",
    horizonDays: Number(payload.horizonDays ?? 7),
    provider: payload.provider ?? "managed",
    params: payload.params ?? {},
    active: Boolean(payload.active),
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
}

export function ensureDefaultModelProfiles(state, tenant) {
  const existing = state.modelProfiles.filter((item) => item.tenantId === tenant.id);
  if (existing.length > 0) return existing;

  const created = PRESET_PROFILES.map((preset, index) =>
    normalizeProfile(tenant, { ...preset, active: index === 0 })
  );
  state.modelProfiles.push(...created);

  const settings = ensureTenantSettings(state, tenant);
  settings.modelPreferences.defaultProfileId = created[0]?.id ?? null;
  settings.updatedAt = new Date().toISOString();

  return created;
}

export function listModelProfiles(state, tenantId) {
  return state.modelProfiles.filter((item) => item.tenantId === tenantId);
}

export function createModelProfile(state, tenant, payload = {}) {
  const profile = normalizeProfile(tenant, payload);
  if (profile.active) {
    for (const existing of state.modelProfiles) {
      if (existing.tenantId === tenant.id) existing.active = false;
    }
  }

  state.modelProfiles.push(profile);

  const settings = ensureTenantSettings(state, tenant);
  if (profile.active || !settings.modelPreferences.defaultProfileId) {
    settings.modelPreferences.defaultProfileId = profile.id;
    settings.updatedAt = new Date().toISOString();
  }

  return profile;
}

export function requireModelProfile(state, tenantId, profileId) {
  const profile = state.modelProfiles.find((item) => item.tenantId === tenantId && item.id === profileId);
  if (!profile) {
    const err = new Error(`Model profile '${profileId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return profile;
}

export function patchModelProfile(state, tenantId, profileId, payload = {}) {
  const profile = requireModelProfile(state, tenantId, profileId);

  for (const [key, value] of Object.entries(payload)) {
    if (key === "id" || key === "tenantId") continue;
    profile[key] = value;
  }
  profile.updatedAt = new Date().toISOString();

  if (payload.active === true) {
    for (const item of state.modelProfiles) {
      if (item.tenantId === tenantId && item.id !== profile.id) item.active = false;
    }
  }

  return profile;
}

export function activateModelProfile(state, tenant, profileId) {
  const target = requireModelProfile(state, tenant.id, profileId);
  for (const item of state.modelProfiles) {
    if (item.tenantId === tenant.id) item.active = item.id === target.id;
  }

  const settings = ensureTenantSettings(state, tenant);
  settings.modelPreferences.defaultProfileId = target.id;
  settings.updatedAt = new Date().toISOString();

  target.updatedAt = new Date().toISOString();
  return target;
}
