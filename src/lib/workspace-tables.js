import { newId } from "./state.js";

function nowIso() {
  return new Date().toISOString();
}

function inferType(value) {
  if (typeof value === "number") return "number";
  if (typeof value === "boolean") return "boolean";
  return "string";
}

function inferSchemaFromRows(rows = []) {
  const map = new Map();
  for (const row of rows) {
    for (const [key, value] of Object.entries(row || {})) {
      if (!map.has(key)) map.set(key, inferType(value));
    }
  }
  return [...map.entries()].map(([key, type]) => ({ key, type }));
}

export function listWorkspaceTables(state, tenantId) {
  return state.workspaceTables
    .filter((item) => item.tenantId === tenantId)
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function requireWorkspaceTable(state, tenantId, tableId) {
  const table = state.workspaceTables.find((item) => item.tenantId === tenantId && item.id === tableId);
  if (!table) {
    const err = new Error(`Workspace table '${tableId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return table;
}

export function listWorkspaceTableRows(state, tenantId, tableId) {
  requireWorkspaceTable(state, tenantId, tableId);
  return state.workspaceTableRows
    .filter((item) => item.tenantId === tenantId && item.tableId === tableId)
    .sort((a, b) => (a.createdAt > b.createdAt ? 1 : -1));
}

export function createWorkspaceTable(state, tenant, payload = {}) {
  const name = String(payload.name || "").trim();
  if (!name) {
    const err = new Error("Table name is required");
    err.statusCode = 400;
    throw err;
  }
  const schema = Array.isArray(payload.schema)
    ? payload.schema
        .filter((item) => item?.key)
        .map((item) => ({ key: String(item.key), type: String(item.type || "string") }))
    : [];

  const createdAt = nowIso();
  const table = {
    id: newId("workspace_table"),
    tenantId: tenant.id,
    name,
    schema,
    sourceRef: payload.sourceRef ? String(payload.sourceRef) : "manual",
    createdBy: payload.createdBy ?? "system",
    linkedThreadIds: Array.isArray(payload.linkedThreadIds) ? payload.linkedThreadIds.map((id) => String(id)) : [],
    linkedRunIds: Array.isArray(payload.linkedRunIds) ? payload.linkedRunIds.map((id) => String(id)) : [],
    createdAt,
    updatedAt: createdAt
  };
  state.workspaceTables.push(table);
  return table;
}

export function patchWorkspaceTable(state, tenantId, tableId, payload = {}) {
  const table = requireWorkspaceTable(state, tenantId, tableId);
  if (payload.name != null) table.name = String(payload.name);
  if (Array.isArray(payload.schema)) {
    table.schema = payload.schema
      .filter((item) => item?.key)
      .map((item) => ({ key: String(item.key), type: String(item.type || "string") }));
  }
  if (payload.sourceRef != null) table.sourceRef = String(payload.sourceRef);
  if (Array.isArray(payload.linkedThreadIds)) {
    table.linkedThreadIds = payload.linkedThreadIds.map((id) => String(id));
  }
  if (Array.isArray(payload.linkedRunIds)) {
    table.linkedRunIds = payload.linkedRunIds.map((id) => String(id));
  }
  table.updatedAt = nowIso();
  return table;
}

export function addWorkspaceTableRows(state, tenantId, tableId, rows = [], actor = "system") {
  const table = requireWorkspaceTable(state, tenantId, tableId);
  if (!Array.isArray(rows) || !rows.length) {
    const err = new Error("rows array is required");
    err.statusCode = 400;
    throw err;
  }

  const now = nowIso();
  const created = rows.map((values) => ({
    id: newId("workspace_row"),
    tenantId,
    tableId,
    values: values && typeof values === "object" ? values : {},
    createdBy: actor,
    createdAt: now,
    updatedAt: now
  }));
  state.workspaceTableRows.push(...created);

  if (!table.schema.length) {
    table.schema = inferSchemaFromRows(created.map((item) => item.values));
  }
  table.updatedAt = nowIso();
  return created;
}

export function importLiveQueryToWorkspaceTable(state, tenant, payload = {}, adapters = {}) {
  if (!payload.connection) {
    const err = new Error("connection is required");
    err.statusCode = 400;
    throw err;
  }
  if (!adapters.runLiveQuery) {
    const err = new Error("runLiveQuery adapter unavailable");
    err.statusCode = 500;
    throw err;
  }

  const live = adapters.runLiveQuery(state, tenant, payload.connection, payload.queryPayload || {});
  const rows = live.rows || [];

  const table = payload.tableId
    ? requireWorkspaceTable(state, tenant.id, payload.tableId)
    : createWorkspaceTable(state, tenant, {
        name: payload.tableName || `${payload.connection.sourceType} live dataset`,
        sourceRef: `live:${payload.connection.id}`,
        schema: inferSchemaFromRows(rows),
        createdBy: payload.createdBy || "system"
      });

  const createdRows = addWorkspaceTableRows(state, tenant.id, table.id, rows, payload.createdBy || "system");
  return {
    table,
    insertedRows: createdRows.length,
    queryMetadata: live.queryMetadata,
    resultId: live.resultId
  };
}
