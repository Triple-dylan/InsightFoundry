import { newId } from "./state.js";

export function pushAudit(state, event) {
  const record = {
    id: newId("audit"),
    at: new Date().toISOString(),
    ...event
  };
  state.auditEvents.push(record);
  return record;
}

export function listAudit(state, { tenantId, since } = {}) {
  const sinceTs = since ? Date.parse(since) : 0;
  return state.auditEvents.filter((event) => {
    if (tenantId && event.tenantId !== tenantId) return false;
    if (sinceTs && Date.parse(event.at) < sinceTs) return false;
    return true;
  });
}
