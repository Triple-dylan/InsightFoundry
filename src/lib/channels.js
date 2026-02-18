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

export function notifyReportDelivery(state, tenantId, channels, report) {
  const outputs = [];
  for (const channel of channels) {
    outputs.push(
      deliverChannelEvent(state, {
        tenantId,
        channel,
        eventType: "report_delivery",
        payload: {
          reportId: report.id,
          title: report.title,
          summary: report.summary
        }
      })
    );
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
