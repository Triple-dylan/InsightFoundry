import { newId } from "./state.js";
import { ingestCanonicalFact } from "./connectors.js";

function normalizeLiveQueryPayload(payload = {}) {
  const q = payload.query ?? {};
  const table = q.table ?? "metrics_daily";
  const columns = q.columns?.length ? q.columns : ["date", "domain", "metricId", "value", "source"];
  const limit = Math.max(1, Math.min(1000, Number(q.limit ?? 100)));
  const filters = q.filters ?? {};
  return { table, columns, limit, filters };
}

function assertReadOnly(payload) {
  if (!payload.sql) return;
  const sql = String(payload.sql).trim().toLowerCase();
  const forbidden = ["insert", "update", "delete", "drop", "alter", "truncate", "create", "grant"];
  if (!sql.startsWith("select") || forbidden.some((token) => sql.includes(token))) {
    const err = new Error("Only read-only SELECT queries are allowed");
    err.statusCode = 400;
    throw err;
  }
}

function enforceQueryPolicy(tenant, connection, normalized, payload) {
  const timeoutMs = Number(payload.timeoutMs ?? 3000);
  const costLimit = Number(payload.costLimit ?? 100);

  if (timeoutMs > tenant.dataPolicy.maxLiveQueryTimeoutMs) {
    const err = new Error("Requested query timeout exceeds tenant policy");
    err.statusCode = 400;
    throw err;
  }

  if (costLimit > tenant.dataPolicy.maxLiveQueryCostUnits) {
    const err = new Error("Requested cost limit exceeds tenant policy");
    err.statusCode = 400;
    throw err;
  }

  if (normalized.limit > tenant.dataPolicy.maxLiveQueryRows) {
    const err = new Error("Requested row limit exceeds tenant policy");
    err.statusCode = 400;
    throw err;
  }

  if (!connection.queryPolicy.allowedTables.includes(normalized.table)) {
    const err = new Error(`Table '${normalized.table}' is not allowed for this connection`);
    err.statusCode = 403;
    throw err;
  }

  const allowedColumns =
    connection.queryPolicy.allowedColumnsByTable?.[normalized.table] ??
    connection.queryPolicy.allowedColumnsByTable?.default ??
    [];

  for (const column of normalized.columns) {
    if (!allowedColumns.includes(column)) {
      const err = new Error(`Column '${column}' is not allowed on table '${normalized.table}'`);
      err.statusCode = 403;
      throw err;
    }
  }

  return { timeoutMs, costLimit };
}

function withFilters(rows, filters) {
  return rows.filter((row) => {
    for (const [key, value] of Object.entries(filters)) {
      if (String(row[key]) !== String(value)) return false;
    }
    return true;
  });
}

function rowsFromFacts(state, tenantId, table) {
  const facts = state.facts.filter((item) => item.tenantId === tenantId);

  if (table === "campaign_performance") {
    return facts
      .filter((item) => item.domain === "marketing")
      .map((item) => ({
        date: item.date,
        campaign: item.source,
        spend: item.metricId === "spend" ? item.value : 0,
        leads: item.metricId === "leads" ? item.value : 0,
        revenue: item.metricId === "revenue" ? item.value : 0
      }));
  }

  if (table === "finance_ledger") {
    const byDate = new Map();
    for (const fact of facts.filter((item) => item.domain === "finance")) {
      const row = byDate.get(fact.date) ?? { date: fact.date, account: "default", cash_in: 0, cash_out: 0, profit: 0 };
      if (fact.metricId === "cash_in") row.cash_in += fact.value;
      if (fact.metricId === "cash_out") row.cash_out += fact.value;
      row.profit = row.cash_in - row.cash_out;
      byDate.set(fact.date, row);
    }
    return [...byDate.values()];
  }

  if (table === "crm_pipeline") {
    return facts
      .filter((item) => item.metricId === "leads")
      .map((item) => ({
        date: item.date,
        stage: "qualified",
        deals: Math.max(1, Math.round(item.value / 5)),
        amount: Math.round(item.value * 180)
      }));
  }

  return facts.map((item) => ({
    date: item.date,
    domain: item.domain,
    metricId: item.metricId,
    value: item.value,
    source: item.source
  }));
}

function pickColumns(rows, columns, limit) {
  return rows.slice(0, limit).map((row) => {
    const trimmed = {};
    for (const column of columns) {
      trimmed[column] = row[column] ?? null;
    }
    return trimmed;
  });
}

export function runLiveQuery(state, tenant, connection, payload = {}) {
  if (!connection.mode || !["live", "hybrid"].includes(connection.mode)) {
    const err = new Error("Connection is not enabled for live queries");
    err.statusCode = 400;
    throw err;
  }

  assertReadOnly(payload);
  const normalized = normalizeLiveQueryPayload(payload);
  const policy = enforceQueryPolicy(tenant, connection, normalized, payload);

  const key = `${tenant.id}:${connection.id}:${JSON.stringify(normalized)}`;
  const cacheHit = state.liveQueryCache.get(key);
  if (cacheHit && Date.parse(cacheHit.expiresAt) > Date.now()) {
    return {
      resultId: cacheHit.resultId,
      rows: cacheHit.rows,
      cached: true,
      queryMetadata: cacheHit.queryMetadata
    };
  }

  const sourceRows = rowsFromFacts(state, tenant.id, normalized.table);
  const filtered = withFilters(sourceRows, normalized.filters);
  const rows = pickColumns(filtered, normalized.columns, normalized.limit);

  const resultId = newId("live_result");
  const queryMetadata = {
    table: normalized.table,
    rowCount: rows.length,
    timeoutMs: policy.timeoutMs,
    costUnits: Math.max(1, rows.length),
    connectionId: connection.id
  };

  state.liveQueryCache.set(key, {
    resultId,
    tenantId: tenant.id,
    rows,
    queryMetadata,
    expiresAt: new Date(Date.now() + 60_000).toISOString()
  });

  return {
    resultId,
    rows,
    cached: false,
    queryMetadata
  };
}

function findCachedByResultId(state, tenantId, resultId) {
  for (const value of state.liveQueryCache.values()) {
    if (value.tenantId === tenantId && value.resultId === resultId) return value;
  }
  return null;
}

export function materializeQueryResult(state, tenant, connection, payload = {}) {
  let cached = null;
  if (payload.resultId) {
    cached = findCachedByResultId(state, tenant.id, payload.resultId);
    if (!cached) {
      const err = new Error(`Live query result '${payload.resultId}' was not found`);
      err.statusCode = 404;
      throw err;
    }
  } else {
    const live = runLiveQuery(state, tenant, connection, payload);
    cached = {
      resultId: live.resultId,
      rows: live.rows,
      queryMetadata: live.queryMetadata
    };
  }

  const mapping = {
    domain: payload.mapping?.domain ?? "ops",
    metricColumn: payload.mapping?.metricColumn ?? "metricId",
    valueColumn: payload.mapping?.valueColumn ?? "value",
    dateColumn: payload.mapping?.dateColumn ?? "date",
    fixedMetricId: payload.mapping?.fixedMetricId
  };

  const materializedSource = `materialized:${payload.datasetName ?? "live_query_dataset"}`;
  let inserted = 0;

  for (const row of cached.rows) {
    const metricId = mapping.fixedMetricId ?? row[mapping.metricColumn] ?? "events";
    const value = Number(row[mapping.valueColumn] ?? 0);
    const date = String(row[mapping.dateColumn] ?? new Date().toISOString().slice(0, 10));
    if (!Number.isFinite(value)) continue;

    const added = ingestCanonicalFact(state, {
      tenantId: tenant.id,
      domain: mapping.domain,
      metricId,
      date,
      value,
      source: materializedSource,
      lineage: {
        provider: connection.sourceType,
        connectorRunId: cached.resultId,
        extractedAt: new Date().toISOString()
      }
    });

    if (added) inserted += 1;
  }

  const run = {
    id: newId("materialize"),
    tenantId: tenant.id,
    connectionId: connection.id,
    sourceResultId: cached.resultId,
    datasetName: payload.datasetName ?? "live_query_dataset",
    insertedRecords: inserted,
    totalRows: cached.rows.length,
    createdAt: new Date().toISOString()
  };

  state.materializationRuns.push(run);
  return run;
}
