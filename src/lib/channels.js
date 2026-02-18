import { newId } from "./state.js";

export function deliverChannelEvent(state, event) {
  const saved = {
    id: newId("channel"),
    at: new Date().toISOString(),
    ...event
  };
  state.channelEvents.push(saved);
  return saved;
}

function renderTemplate(template, context = {}) {
  return String(template ?? "")
    .replaceAll("{{reportTitle}}", String(context.reportTitle ?? ""))
    .replaceAll("{{reportSummary}}", String(context.reportSummary ?? ""))
    .replaceAll("{{tenantId}}", String(context.tenantId ?? ""))
    .replaceAll("{{channel}}", String(context.channel ?? ""))
    .replaceAll("{{runId}}", String(context.runId ?? ""))
    .replaceAll("{{insightId}}", String(context.insightId ?? ""))
    .replaceAll("{{confidence}}", context.confidence == null ? "n/a" : String(context.confidence))
    .replaceAll("{{actionsCount}}", String(context.actionsCount ?? 0));
}

function defaultTemplate(channel) {
  if (channel === "slack") return "[{{channel}}] {{reportTitle}} | {{reportSummary}} | confidence={{confidence}}";
  if (channel === "telegram") return "[{{channel}}] {{reportTitle}} | {{reportSummary}}";
  return "[{{channel}}] {{reportTitle}}\n{{reportSummary}}\nRun={{runId}}";
}

function readinessForChannel(state, tenantId, channel) {
  const settings = state.settingsByTenant.get(tenantId);
  if (!settings?.channels) {
    return { ready: channel === "email", reason: channel === "email" ? null : "channel_settings_missing" };
  }

  if (channel === "email") {
    return { ready: true, reason: null };
  }
  if (channel === "slack") {
    if (!settings.channels.slack?.enabled) return { ready: false, reason: "slack_disabled" };
    if (!settings.channels.slack?.webhookRef) return { ready: false, reason: "slack_webhook_missing" };
    return { ready: true, reason: null };
  }
  if (channel === "telegram") {
    if (!settings.channels.telegram?.enabled) return { ready: false, reason: "telegram_disabled" };
    if (!settings.channels.telegram?.botTokenRef || !settings.channels.telegram?.chatId) {
      return { ready: false, reason: "telegram_credentials_missing" };
    }
    return { ready: true, reason: null };
  }
  return { ready: true, reason: null };
}

export function previewReportDelivery(state, tenantId, channels, report, options = {}) {
  const templates = options.templates ?? {};
  const extraContext = options.context ?? {};
  return channels.map((channel) => {
    const template = templates[channel] ?? defaultTemplate(channel);
    const readiness = readinessForChannel(state, tenantId, channel);
    const message = renderTemplate(template, {
      reportTitle: report.title,
      reportSummary: report.summary,
      tenantId,
      channel,
      ...extraContext
    });
    return {
      channel,
      ready: readiness.ready,
      reason: readiness.reason,
      template,
      message
    };
  });
}

export function notifyReportDelivery(state, tenantId, channels, report, options = {}) {
  const outputs = [];
  const previews = previewReportDelivery(state, tenantId, channels, report, options);
  for (const preview of previews) {
    const priorAttempts = Number(options.attemptCount ?? 0);
    const attemptCount = priorAttempts + 1;
    const maxAttempts = Number(options.maxAttempts ?? 3);
    const delivered = preview.ready && !options.forceFailChannels?.includes(preview.channel);
    const status = delivered ? "delivered" : (attemptCount >= maxAttempts ? "failed_permanent" : "failed");
    const lastError = delivered ? null : (preview.reason ?? "delivery_failed");
    outputs.push(
      deliverChannelEvent(state, {
        tenantId,
        channel: preview.channel,
        eventType: "report_delivery",
        status,
        attemptCount,
        maxAttempts,
        lastError,
        responseMetadata: {
          provider: preview.channel,
          httpStatus: delivered ? 200 : 503,
          deliveredAt: delivered ? new Date().toISOString() : null
        },
        payload: {
          reportId: report.id,
          title: report.title,
          summary: report.summary,
          message: preview.message
        }
      })
    );
  }
  return outputs;
}

export function retryChannelEvent(state, tenantId, eventId, options = {}) {
  const event = state.channelEvents.find(
    (item) => item.tenantId === tenantId && item.id === eventId && item.eventType === "report_delivery"
  );
  if (!event) {
    const err = new Error(`Channel event '${eventId}' not found`);
    err.statusCode = 404;
    throw err;
  }

  const report = state.reports.find((item) => item.tenantId === tenantId && item.id === event.payload?.reportId);
  if (!report) {
    const err = new Error(`Report '${event.payload?.reportId}' not found for retry`);
    err.statusCode = 404;
    throw err;
  }

  const templates = options.templates ?? {};
  const preview = previewReportDelivery(state, tenantId, [event.channel], report, {
    templates,
    context: options.context
  })[0];
  const nextAttempt = Number(event.attemptCount ?? 1) + 1;
  const maxAttempts = Number(event.maxAttempts ?? 3);
  const delivered = preview.ready && !options.forceFailChannels?.includes(event.channel);
  event.status = delivered ? "delivered" : (nextAttempt >= maxAttempts ? "failed_permanent" : "failed");
  event.attemptCount = nextAttempt;
  event.lastError = delivered ? null : (preview.reason ?? "delivery_failed");
  event.responseMetadata = {
    provider: event.channel,
    httpStatus: delivered ? 200 : 503,
    deliveredAt: delivered ? new Date().toISOString() : null
  };
  event.payload.message = preview.message;
  event.at = new Date().toISOString();
  return event;
}

export function notifyInsight(state, tenantId, channels, insight) {
  const outputs = [];
  for (const channel of channels) {
    outputs.push(
      deliverChannelEvent(state, {
        tenantId,
        channel,
        eventType: "insight_alert",
        payload: {
          insightId: insight.id,
          summary: insight.summary,
          confidence: insight.confidence
        }
      })
    );
  }
  return outputs;
}
