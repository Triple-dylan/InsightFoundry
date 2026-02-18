import { newId } from "./state.js";
import { queryMetric } from "./metrics.js";
import { notifyReportDelivery } from "./channels.js";

function formatLines(title, metricBlocks, insight) {
  const lines = [];
  lines.push(`# ${title}`);
  lines.push("");
  lines.push(`Generated: ${new Date().toISOString()}`);
  lines.push("");
  lines.push("## KPI Snapshot");
  lines.push("");

  for (const block of metricBlocks) {
    lines.push(`- ${block.metricId}: total=${block.summary.total}, avg=${block.summary.average}`);
  }

  lines.push("");
  lines.push("## AI Insight");
  lines.push("");
  if (insight) {
    lines.push(`- ${insight.summary}`);
    lines.push(`- Confidence: ${insight.confidence}`);
    lines.push(`- Actions: ${insight.recommendedActions.length}`);
  } else {
    lines.push("- No insight generated yet.");
  }

  return lines.join("\n");
}

export function generateReport(state, tenant, payload = {}) {
  const metricIds = payload.metricIds?.length ? payload.metricIds : ["revenue", "profit", "spend"];
  const blocks = metricIds.map((metricId) => queryMetric(state, tenant.id, { metricId, grain: payload.grain ?? "week" }));
  const latestInsight = [...state.insights]
    .filter((item) => item.tenantId === tenant.id)
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))[0];

  const report = {
    id: newId("report"),
    tenantId: tenant.id,
    title: payload.title ?? `${tenant.name} Executive Report`,
    format: payload.format ?? "pdf",
    summary: `Report with ${blocks.length} metric blocks`,
    metricIds,
    generatedAt: new Date().toISOString(),
    body: formatLines(payload.title ?? `${tenant.name} Executive Report`, blocks, latestInsight)
  };

  state.reports.push(report);

  const deliveryChannels = payload.channels ?? ["email"];
  const deliveryEvents = notifyReportDelivery(state, tenant.id, deliveryChannels, report, {
    templates: payload.channelTemplates,
    context: payload.channelTemplateContext
  });

  return { report, deliveryEvents };
}

export function createReportSchedule(state, tenant, payload = {}) {
  const intervalMinutes = Math.max(5, Math.min(24 * 60, Number(payload.intervalMinutes ?? 60)));
  const now = Date.now();
  const schedule = {
    id: newId("schedule"),
    tenantId: tenant.id,
    name: payload.name ?? "Executive Digest",
    metricIds: payload.metricIds ?? ["revenue", "profit"],
    channels: payload.channels ?? ["email"],
    format: payload.format ?? "pdf",
    intervalMinutes,
    active: payload.active ?? true,
    lastRunAt: null,
    nextRunAt: new Date(now + intervalMinutes * 60_000).toISOString(),
    createdAt: new Date().toISOString()
  };

  state.reportSchedules.push(schedule);
  return schedule;
}
