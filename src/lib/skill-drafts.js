import { newId } from "./state.js";
import { validateSkillManifest, installSkillPack } from "./skills.js";

function defaultDraft() {
  return {
    id: "custom-skill",
    version: "1.0.0",
    name: "Custom Skill",
    description: "Tenant custom orchestration skill",
    triggers: { intents: ["custom_intent"], channels: ["web", "api"] },
    tools: [{ id: "model.run", allow: true }],
    guardrails: {
      confidenceMin: 0.7,
      humanApprovalFor: ["adjust_budget"],
      budgetCapUsd: 10000,
      tokenBudget: 2500,
      timeBudgetMs: 8000,
      killSwitch: false
    },
    prompts: { system: "Use available tenant data to generate actionable outcomes." },
    schedules: []
  };
}

export function createSkillDraft(state, tenant, payload = {}) {
  const draft = {
    draftId: newId("skill_draft"),
    tenantId: tenant.id,
    manifest: {
      ...defaultDraft(),
      ...(payload.manifest ?? {})
    },
    status: "draft",
    validationErrors: [],
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
  state.skillDrafts.push(draft);
  return draft;
}

export function requireSkillDraft(state, tenantId, draftId) {
  const draft = state.skillDrafts.find((item) => item.tenantId === tenantId && item.draftId === draftId);
  if (!draft) {
    const err = new Error(`Skill draft '${draftId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return draft;
}

export function patchSkillDraft(state, tenantId, draftId, payload = {}) {
  const draft = requireSkillDraft(state, tenantId, draftId);
  if (payload.manifest && typeof payload.manifest === "object") {
    draft.manifest = {
      ...draft.manifest,
      ...payload.manifest
    };
  }
  draft.updatedAt = new Date().toISOString();
  return draft;
}

export function validateSkillDraft(state, tenantId, draftId) {
  const draft = requireSkillDraft(state, tenantId, draftId);
  const errors = [];
  try {
    validateSkillManifest(draft.manifest);
  } catch (error) {
    errors.push(error.message);
  }

  if (!Array.isArray(draft.manifest.triggers?.channels) || draft.manifest.triggers.channels.length === 0) {
    errors.push("Manifest requires at least one channel trigger");
  }

  if (!Array.isArray(draft.manifest.schedules)) {
    errors.push("Manifest schedules must be an array");
  }

  draft.validationErrors = errors;
  draft.status = errors.length ? "invalid" : "valid";
  draft.updatedAt = new Date().toISOString();

  return {
    draftId: draft.draftId,
    status: draft.status,
    errors
  };
}

export function publishSkillDraft(state, tenant, draftId, options = {}) {
  const draft = requireSkillDraft(state, tenant.id, draftId);
  const validation = validateSkillDraft(state, tenant.id, draftId);
  if (validation.errors.length) {
    const err = new Error("Draft validation failed; cannot publish");
    err.statusCode = 400;
    throw err;
  }

  const install = installSkillPack(state, tenant, {
    manifest: draft.manifest,
    active: options.active ?? true
  });

  draft.status = "published";
  draft.publishedSkillId = install.id;
  draft.updatedAt = new Date().toISOString();

  return {
    draft,
    install
  };
}
