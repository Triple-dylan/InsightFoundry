import { newId } from "./state.js";

function nowIso() {
  return new Date().toISOString();
}

function extractVariables(body = "") {
  const set = new Set();
  const re = /{{\s*([a-zA-Z0-9_.-]+)\s*}}/g;
  let match;
  while ((match = re.exec(String(body)))) {
    set.add(match[1]);
  }
  return [...set];
}

function renderTemplate(body, context = {}) {
  return String(body).replace(/{{\s*([a-zA-Z0-9_.-]+)\s*}}/g, (_, key) => {
    const value = context[key];
    return value == null ? "" : String(value);
  });
}

export function listReportTemplates(state, tenantId, options = {}) {
  return state.reportTemplates
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (options.reportTypeId ? item.reportTypeId === options.reportTypeId : true))
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function requireReportTemplate(state, tenantId, templateId) {
  const template = state.reportTemplates.find((item) => item.tenantId === tenantId && item.id === templateId);
  if (!template) {
    const err = new Error(`Report template '${templateId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return template;
}

export function uploadReportTemplate(state, tenant, payload = {}) {
  const name = String(payload.name || "").trim();
  const body = String(payload.body || "");
  if (!name || !body) {
    const err = new Error("Template name and body are required");
    err.statusCode = 400;
    throw err;
  }

  const createdAt = nowIso();
  const versions = state.reportTemplates
    .filter((item) => item.tenantId === tenant.id && item.name === name)
    .map((item) => Number(item.version || 1));
  const version = (versions.length ? Math.max(...versions) : 0) + 1;

  const template = {
    id: newId("report_template"),
    tenantId: tenant.id,
    reportTypeId: payload.reportTypeId ? String(payload.reportTypeId) : null,
    objective: payload.objective ? String(payload.objective) : null,
    domain: payload.domain ? String(payload.domain) : null,
    name,
    format: "markdown",
    body,
    variables: extractVariables(body),
    defaults: payload.defaults && typeof payload.defaults === "object" ? payload.defaults : {},
    version,
    active: Boolean(payload.active),
    createdAt,
    updatedAt: createdAt
  };

  if (template.active) {
    state.reportTemplates.forEach((item) => {
      if (item.tenantId !== tenant.id) return;
      if (template.reportTypeId && item.reportTypeId === template.reportTypeId) item.active = false;
      if (!template.reportTypeId && item.name === template.name) item.active = false;
    });
  }

  state.reportTemplates.push(template);
  return template;
}

export function patchReportTemplate(state, tenantId, templateId, payload = {}) {
  const template = requireReportTemplate(state, tenantId, templateId);
  if (payload.name != null) template.name = String(payload.name);
  if (payload.reportTypeId !== undefined) template.reportTypeId = payload.reportTypeId ? String(payload.reportTypeId) : null;
  if (payload.objective !== undefined) template.objective = payload.objective ? String(payload.objective) : null;
  if (payload.domain !== undefined) template.domain = payload.domain ? String(payload.domain) : null;
  if (payload.body != null) {
    template.body = String(payload.body);
    template.variables = extractVariables(template.body);
  }
  if (payload.defaults && typeof payload.defaults === "object") {
    template.defaults = {
      ...(template.defaults || {}),
      ...payload.defaults
    };
  }
  if (typeof payload.active === "boolean") template.active = payload.active;
  template.updatedAt = nowIso();
  return template;
}

export function activateReportTemplate(state, tenantId, templateId) {
  const template = requireReportTemplate(state, tenantId, templateId);
  state.reportTemplates.forEach((item) => {
    if (item.tenantId !== tenantId) return;
    if (template.reportTypeId) {
      if (item.reportTypeId === template.reportTypeId) item.active = false;
    } else if (item.name === template.name) {
      item.active = false;
    }
  });
  template.active = true;
  template.updatedAt = nowIso();
  return template;
}

export function previewReportTemplate(state, tenantId, templateId, payload = {}) {
  const template = requireReportTemplate(state, tenantId, templateId);
  const context = {
    ...(template.defaults || {}),
    ...(payload.context || {})
  };
  const missing = (template.variables || []).filter((key) => context[key] == null || context[key] === "");
  return {
    template,
    missing,
    compiled: renderTemplate(template.body, context)
  };
}

export function resolveActiveTemplate(state, tenantId, selectors = {}) {
  const templates = state.reportTemplates.filter((item) => item.tenantId === tenantId && item.active);
  const byReportType = selectors.reportTypeId
    ? templates.find((item) => item.reportTypeId === selectors.reportTypeId)
    : null;
  if (byReportType) return byReportType;
  const byObjective = selectors.objective
    ? templates.find((item) => item.objective === selectors.objective)
    : null;
  if (byObjective) return byObjective;
  const byDomain = selectors.domain
    ? templates.find((item) => item.domain === selectors.domain)
    : null;
  return byDomain ?? templates[0] ?? null;
}

export function compileReportTemplateBody(template, context = {}) {
  if (!template) return null;
  return renderTemplate(template.body, {
    ...(template.defaults || {}),
    ...context
  });
}
