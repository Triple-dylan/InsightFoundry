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

function liveDeliveryEnabled() {
  return String(process.env.CHANNEL_DELIVERY_MODE || "").toLowerCase() === "live";
}

function channelConfig(state, tenantId, channel) {
  const settings = state.settingsByTenant.get(tenantId);
  if (!settings?.channels) return null;
  if (channel === "slack") return settings.channels.slack || null;
  if (channel === "telegram") return settings.channels.telegram || null;
  if (channel === "discord") return settings.channels.discord || null;
  return null;
}

async function deliverLiveChannel(event, config) {
  if (event.channel === "slack") {
    const webhook = String(config?.webhookRef || "").trim();
    if (!/^https?:\/\//i.test(webhook)) {
      throw new Error("slack_webhook_ref_not_http_url");
    }
    const res = await fetch(webhook, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: event.payload?.message || "" })
    });
    if (!res.ok) {
      throw new Error(`slack_http_${res.status}`);
    }
    return { provider: "slack", httpStatus: res.status };
  }
  if (event.channel === "telegram") {
    const token = String(config?.botTokenRef || "").trim();
    const chatId = String(config?.chatId || "").trim();
    if (!token || !chatId) throw new Error("telegram_credentials_missing");
    const endpoint = `https://api.telegram.org/bot${encodeURIComponent(token)}/sendMessage`;
    const res = await fetch(endpoint, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text: event.payload?.message || ""
      })
    });
    if (!res.ok) {
      throw new Error(`telegram_http_${res.status}`);
    }
    return { provider: "telegram", httpStatus: res.status };
  }
  if (event.channel === "discord") {
    const webhook = String(config?.webhookRef || "").trim();
    if (!/^https?:\/\//i.test(webhook)) {
      throw new Error("discord_webhook_ref_not_http_url");
    }
    const res = await fetch(webhook, {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ content: event.payload?.message || "" })
    });
    if (!res.ok) {
      throw new Error(`discord_http_${res.status}`);
    }
    return { provider: "discord", httpStatus: res.status };
  }
  return { provider: event.channel, httpStatus: 200 };
}

function maybeDispatchLiveDelivery(state, event) {
  if (!liveDeliveryEnabled()) return;
  if (!event || event.status !== "delivered") return;
  if (!["slack", "telegram", "discord"].includes(event.channel)) return;
  const cfg = channelConfig(state, event.tenantId, event.channel);
  if (!cfg?.enabled) return;
  event.status = "queued";
  event.lastError = null;
  event.at = new Date().toISOString();
  deliverLiveChannel(event, cfg)
    .then((meta) => {
      event.status = "delivered";
      event.responseMetadata = {
        ...event.responseMetadata,
        ...meta,
        deliveredAt: new Date().toISOString()
      };
      event.lastError = null;
      event.at = new Date().toISOString();
    })
    .catch((error) => {
      event.status = "failed";
      event.lastError = String(error?.message || "live_delivery_failed");
      event.responseMetadata = {
        ...event.responseMetadata,
        httpStatus: 503,
        deliveredAt: null
      };
      event.at = new Date().toISOString();
    });
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
  if (channel === "discord") return "**{{reportTitle}}**\n{{reportSummary}}\nconfidence={{confidence}}";
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
  if (channel === "discord") {
    if (!settings.channels.discord?.enabled) return { ready: false, reason: "discord_disabled" };
    if (!settings.channels.discord?.webhookRef) return { ready: false, reason: "discord_webhook_missing" };
    return { ready: true, reason: null };
  }
  return { ready: true, reason: null };
}

function reliabilityForChannel(state, tenantId, channel) {
  const settings = state.settingsByTenant.get(tenantId);
  const defaults = {
    email: { maxAttempts: 2, baseDelayMs: 15000 },
    slack: { maxAttempts: 3, baseDelayMs: 20000 },
    telegram: { maxAttempts: 4, baseDelayMs: 30000 },
    discord: { maxAttempts: 3, baseDelayMs: 20000 }
  };
  const configured = settings?.channels?.reliability?.[channel] ?? defaults[channel] ?? { maxAttempts: 3, baseDelayMs: 20000 };
  return {
    maxAttempts: Math.max(1, Number(configured.maxAttempts ?? defaults[channel]?.maxAttempts ?? 3)),
    baseDelayMs: Math.max(1000, Number(configured.baseDelayMs ?? defaults[channel]?.baseDelayMs ?? 20000))
  };
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
    const reliability = reliabilityForChannel(state, tenantId, preview.channel);
    const maxAttempts = Number(options.maxAttempts ?? reliability.maxAttempts);
    const delivered = preview.ready && !options.forceFailChannels?.includes(preview.channel);
    const status = delivered ? "delivered" : (attemptCount >= maxAttempts ? "failed_permanent" : "failed");
    const lastError = delivered ? null : (preview.reason ?? "delivery_failed");
    const nextRetryAt = delivered || status === "failed_permanent"
      ? null
      : new Date(Date.now() + reliability.baseDelayMs * Math.max(1, attemptCount)).toISOString();
    outputs.push(
      deliverChannelEvent(state, {
        tenantId,
        channel: preview.channel,
        eventType: "report_delivery",
        status,
        attemptCount,
        maxAttempts,
        nextRetryAt,
        retryPolicy: reliability,
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
    maybeDispatchLiveDelivery(state, outputs[outputs.length - 1]);
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
  const reliability = reliabilityForChannel(state, tenantId, event.channel);
  const nextAttempt = Number(event.attemptCount ?? 1) + 1;
  const maxAttempts = Number(event.maxAttempts ?? reliability.maxAttempts);
  const delivered = preview.ready && !options.forceFailChannels?.includes(event.channel);
  event.status = delivered ? "delivered" : (nextAttempt >= maxAttempts ? "failed_permanent" : "failed");
  event.attemptCount = nextAttempt;
  event.maxAttempts = maxAttempts;
  event.nextRetryAt = delivered || event.status === "failed_permanent"
    ? null
    : new Date(Date.now() + reliability.baseDelayMs * Math.max(1, nextAttempt)).toISOString();
  event.retryPolicy = reliability;
  event.lastError = delivered ? null : (preview.reason ?? "delivery_failed");
  event.responseMetadata = {
    provider: event.channel,
    httpStatus: delivered ? 200 : 503,
    deliveredAt: delivered ? new Date().toISOString() : null
  };
  event.payload.message = preview.message;
  event.at = new Date().toISOString();
  maybeDispatchLiveDelivery(state, event);
  return event;
}

function normalizeBridgeConfig(channelConfig) {
  const bridge = channelConfig?.bridge && typeof channelConfig.bridge === "object"
    ? channelConfig.bridge
    : {};
  return {
    enabled: Boolean(bridge.enabled),
    threadId: String(bridge.threadId || "").trim()
  };
}

export function notifyChatBridge(state, tenantId, message, options = {}) {
  const channels = Array.isArray(options.channels) && options.channels.length
    ? options.channels
    : ["slack", "discord"];
  const outputs = [];
  const settings = state.settingsByTenant.get(tenantId);
  for (const channel of channels) {
    if (!["slack", "discord"].includes(channel)) continue;
    const cfg = settings?.channels?.[channel];
    const bridge = normalizeBridgeConfig(cfg);
    const channelEnabled = Boolean(cfg?.enabled);
    const webhookReady = Boolean(String(cfg?.webhookRef || "").trim());
    const mappedThread = Array.isArray(state.channelThreadLinks)
      && state.channelThreadLinks.some((item) =>
        item.tenantId === tenantId
        && item.channel === channel
        && item.threadId === message.threadId
      );
    const threadMatches = !bridge.threadId || bridge.threadId === message.threadId || mappedThread;
    const ready = channelEnabled && webhookReady && bridge.enabled && threadMatches;
    const status = ready ? "delivered" : "failed";
    const reason = ready
      ? null
      : (!channelEnabled
          ? `${channel}_disabled`
          : (!webhookReady
              ? `${channel}_webhook_missing`
              : (!bridge.enabled
                  ? `${channel}_bridge_disabled`
                  : "bridge_thread_mismatch")));
    const delivered = deliverChannelEvent(state, {
      tenantId,
      channel,
      eventType: "chat_bridge_delivery",
      status,
      attemptCount: 1,
      maxAttempts: 1,
      nextRetryAt: null,
      retryPolicy: { maxAttempts: 1, baseDelayMs: 0 },
      lastError: reason,
      responseMetadata: {
        provider: channel,
        httpStatus: ready ? 200 : 400,
        deliveredAt: ready ? new Date().toISOString() : null
      },
      payload: {
        threadId: message.threadId,
        messageId: message.id,
        authorName: message.authorName,
        body: message.body,
        message: `#${message.threadId.slice(0, 8)} ${message.authorName}: ${message.body}`
      }
    });
    outputs.push(delivered);
    maybeDispatchLiveDelivery(state, delivered);
  }
  return outputs;
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
