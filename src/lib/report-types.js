import { newId } from "./state.js";

function defaultDeliveryTemplates() {
  return {
    email: "[{{channel}}] {{reportTitle}}\n{{reportSummary}}\nRun={{runId}}",
    slack: "[{{channel}}] {{reportTitle}} | {{reportSummary}} | confidence={{confidence}}",
    telegram: "[{{channel}}] {{reportTitle}} | {{reportSummary}}"
  };
}

function normalizeReportType(type) {
  type.deliveryTemplates = {
    ...defaultDeliveryTemplates(),
    ...(type.deliveryTemplates ?? {})
  };
  return type;
}

const PRESET_REPORT_TYPES = [
  {
    name: "Executive Overview",
    sections: ["kpi_snapshot", "trend_chart", "narrative", "actions"],
    defaultChannels: ["email", "slack"],
    defaultFormat: "pdf",
    schedule: { intervalMinutes: 1440 },
    deliveryTemplates: defaultDeliveryTemplates()
  },
  {
    name: "Marketing Performance",
    sections: ["kpi_snapshot", "trend_chart", "actions", "appendix"],
    defaultChannels: ["email", "slack", "telegram"],
    defaultFormat: "pdf",
    schedule: { intervalMinutes: 720 },
    deliveryTemplates: defaultDeliveryTemplates()
  },
  {
    name: "Founder Growth and Cash Cockpit",
    sections: ["kpi_snapshot", "trend_chart", "narrative", "actions", "appendix"],
    defaultChannels: ["slack", "email"],
    defaultFormat: "html",
    schedule: { intervalMinutes: 1440 },
    deliveryTemplates: defaultDeliveryTemplates()
  }
];

export function ensureDefaultReportTypes(state, tenant) {
  const existing = state.reportTypes.filter((item) => item.tenantId === tenant.id);
  if (existing.length > 0) return existing.map(normalizeReportType);

  const created = PRESET_REPORT_TYPES.map((preset) => ({
    id: newId("report_type"),
    tenantId: tenant.id,
    ...preset,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  }));

  state.reportTypes.push(...created);
  return created.map(normalizeReportType);
}

export function listReportTypes(state, tenantId) {
  return state.reportTypes
    .filter((item) => item.tenantId === tenantId)
    .map(normalizeReportType);
}

export function createReportType(state, tenant, payload = {}) {
  const type = {
    id: newId("report_type"),
    tenantId: tenant.id,
    name: payload.name ?? "Custom Report",
    sections: payload.sections ?? ["kpi_snapshot", "narrative"],
    defaultChannels: payload.defaultChannels ?? ["email"],
    defaultFormat: payload.defaultFormat ?? "pdf",
    schedule: payload.schedule ?? null,
    deliveryTemplates: {
      ...defaultDeliveryTemplates(),
      ...(payload.deliveryTemplates ?? {})
    },
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  state.reportTypes.push(type);
  return normalizeReportType(type);
}

export function requireReportType(state, tenantId, typeId) {
  const reportType = state.reportTypes.find((item) => item.tenantId === tenantId && item.id === typeId);
  if (!reportType) {
    const err = new Error(`Report type '${typeId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return normalizeReportType(reportType);
}

export function patchReportType(state, tenantId, typeId, payload = {}) {
  const reportType = requireReportType(state, tenantId, typeId);
  for (const [key, value] of Object.entries(payload)) {
    if (key === "id" || key === "tenantId") continue;
    if (key === "deliveryTemplates" && value && typeof value === "object") {
      reportType.deliveryTemplates = {
        ...(reportType.deliveryTemplates ?? defaultDeliveryTemplates()),
        ...value
      };
      continue;
    }
    reportType[key] = value;
  }
  reportType.updatedAt = new Date().toISOString();
  return reportType;
}

export function previewReportType(reportType, context = {}) {
  const lines = [];
  lines.push(`# Preview: ${reportType.name}`);
  lines.push("");
  lines.push(`Format: ${reportType.defaultFormat}`);
  lines.push(`Channels: ${reportType.defaultChannels.join(", ")}`);
  lines.push("");
  lines.push("Sections:");
  for (const section of reportType.sections) {
    if (section === "kpi_snapshot") lines.push("- KPI Snapshot: spend, revenue, profit trends");
    if (section === "trend_chart") lines.push("- Trend Chart: weekly trajectory and variance notes");
    if (section === "narrative") lines.push("- Narrative: concise executive explanation");
    if (section === "actions") lines.push("- Actions: prioritized recommendations with confidence");
    if (section === "appendix") lines.push("- Appendix: raw metrics and query provenance");
  }
  if (context.latestInsightSummary) {
    lines.push("");
    lines.push(`Latest insight context: ${context.latestInsightSummary}`);
  }
  lines.push("");
  lines.push("Channel Templates:");
  const templates = reportType.deliveryTemplates ?? defaultDeliveryTemplates();
  for (const [channel, template] of Object.entries(templates)) {
    lines.push(`- ${channel}: ${template}`);
  }
  return lines.join("\n");
}
