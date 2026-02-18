import crypto from "node:crypto";
import { newId } from "./state.js";
import { runConnectorSync } from "./connectors.js";

const SOURCE_CATALOG = [
  { sourceType: "postgres", family: "database", domains: ["finance", "ops", "sales"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "mysql", family: "database", domains: ["finance", "ops", "sales"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "sqlserver", family: "database", domains: ["finance", "ops", "sales"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "snowflake", family: "warehouse", domains: ["marketing", "finance", "sales", "ops"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "bigquery", family: "warehouse", domains: ["marketing", "finance", "sales", "ops"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "redshift", family: "warehouse", domains: ["marketing", "finance", "sales", "ops"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "databricks", family: "warehouse", domains: ["marketing", "finance", "sales", "ops"], modes: ["ingest", "live", "hybrid"] },
  { sourceType: "google_ads", family: "saas", domains: ["marketing"], modes: ["ingest", "hybrid"] },
  { sourceType: "meta_ads", family: "saas", domains: ["marketing"], modes: ["ingest", "hybrid"] },
  { sourceType: "salesforce", family: "saas", domains: ["sales", "ops"], modes: ["ingest", "hybrid"] },
  { sourceType: "hubspot", family: "saas", domains: ["sales", "marketing"], modes: ["ingest", "hybrid"] },
  { sourceType: "stripe", family: "saas", domains: ["finance"], modes: ["ingest", "hybrid"] },
  { sourceType: "quickbooks", family: "saas", domains: ["finance"], modes: ["ingest", "hybrid"] }
];

const DEFAULT_QUERY_POLICY = {
  allowedTables: ["metrics_daily", "campaign_performance", "finance_ledger", "crm_pipeline"],
  allowedColumnsByTable: {
    metrics_daily: ["date", "domain", "metricId", "value", "source"],
    campaign_performance: ["date", "campaign", "spend", "leads", "revenue"],
    finance_ledger: ["date", "account", "cash_in", "cash_out", "profit"],
    crm_pipeline: ["date", "stage", "deals", "amount"],
    default: ["date", "value"]
  }
};

function normalizeMode(entry, requested) {
  const mode = requested ?? "hybrid";
  if (!entry.modes.includes(mode)) {
    const err = new Error(`Source '${entry.sourceType}' does not support mode '${mode}'`);
    err.statusCode = 400;
    throw err;
  }
  return mode;
}

function asDigest(value) {
  return crypto.createHash("sha256").update(value).digest("hex");
}

function storeSecretRef(state, tenantId, auth = {}) {
  const authString = JSON.stringify(auth);
  const hasCredentials = Object.keys(auth).length > 0;
  const authRef = `secret_${asDigest(`${tenantId}:${authString}`).slice(0, 20)}`;
  state.secretRefs.set(authRef, {
    tenantId,
    hasCredentials,
    fingerprint: asDigest(authString),
    createdAt: new Date().toISOString(),
    lastValidatedAt: null
  });
  return authRef;
}

function sourceCatalogEntry(sourceType) {
  const entry = SOURCE_CATALOG.find((item) => item.sourceType === sourceType);
  if (!entry) {
    const err = new Error(`Unsupported sourceType '${sourceType}'`);
    err.statusCode = 400;
    throw err;
  }
  return entry;
}

export function listSourceCatalog() {
  return SOURCE_CATALOG;
}

export function createSourceConnection(state, tenant, payload) {
  const sourceType = payload.sourceType;
  if (!sourceType) {
    const err = new Error("sourceType is required");
    err.statusCode = 400;
    throw err;
  }

  const entry = sourceCatalogEntry(sourceType);
  const mode = normalizeMode(entry, payload.mode);
  const authRef = storeSecretRef(state, tenant.id, payload.auth ?? {});

  const connection = {
    id: newId("source"),
    tenantId: tenant.id,
    sourceType,
    mode,
    authRef,
    status: "active",
    syncPolicy: {
      intervalMinutes: Number(payload.syncPolicy?.intervalMinutes ?? 60),
      backfillDays: Number(payload.syncPolicy?.backfillDays ?? 30)
    },
    queryPolicy: {
      ...DEFAULT_QUERY_POLICY,
      ...(payload.queryPolicy ?? {})
    },
    metadata: {
      label: payload.label ?? `${sourceType}-${tenant.name}`,
      extractionSpec: payload.extractionSpec ?? {}
    },
    checkpoint: payload.checkpoint ?? null,
    createdAt: new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  const secret = state.secretRefs.get(authRef);
  if (!secret?.hasCredentials) {
    connection.status = "error";
  }

  state.sourceConnections.push(connection);
  return connection;
}

export function listSourceConnections(state, tenantId) {
  return state.sourceConnections.filter((item) => item.tenantId === tenantId);
}

export function requireSourceConnection(state, tenantId, connectionId) {
  const connection = state.sourceConnections.find((item) => item.id === connectionId && item.tenantId === tenantId);
  if (!connection) {
    const err = new Error(`Source connection '${connectionId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return connection;
}

export function testSourceConnection(state, connection) {
  const secret = state.secretRefs.get(connection.authRef);
  const ok = Boolean(secret?.hasCredentials);
  const diagnostics = ok
    ? { check: "auth", message: "Connection credentials present" }
    : { check: "auth", message: "Missing credentials payload for source connection" };

  connection.status = ok ? "active" : "error";
  connection.updatedAt = new Date().toISOString();
  if (secret) {
    secret.lastValidatedAt = new Date().toISOString();
  }

  return {
    connectionId: connection.id,
    sourceType: connection.sourceType,
    status: ok ? "success" : "failed",
    diagnostics
  };
}

export function runSourceSync(state, tenant, connection, payload = {}) {
  if (connection.mode === "live") {
    const err = new Error("Live-only connections do not support ingest sync");
    err.statusCode = 400;
    throw err;
  }

  const backfillDays = Number(payload.periodDays ?? connection.syncPolicy.backfillDays ?? 30);
  const connectorResult = runConnectorSync(state, tenant, connection.sourceType, {
    domain: payload.domain,
    periodDays: backfillDays,
    extractionSpec: connection.metadata.extractionSpec,
    schedule: payload.schedule ?? "manual"
  });

  const run = {
    id: newId("source_run"),
    tenantId: tenant.id,
    connectionId: connection.id,
    sourceType: connection.sourceType,
    mode: connection.mode,
    status: payload.simulateFailure ? "error" : "success",
    diagnostics: {
      generatedRecords: connectorResult.canonicalRecords.length,
      insertedRecords: connectorResult.lineageMetadata.insertedRecords,
      qualityScore: connectorResult.qualityScore,
      retries: Number(payload.retries ?? 0)
    },
    checkpoint: {
      cursor: connectorResult.canonicalRecords.at(-1)?.date ?? null
    },
    createdAt: new Date().toISOString()
  };

  connection.checkpoint = run.checkpoint;
  connection.status = run.status === "success" ? "active" : "error";
  connection.updatedAt = new Date().toISOString();
  state.sourceConnectionRuns.push(run);

  return {
    syncStatus: run.status,
    sourceRunId: run.id,
    connectionId: connection.id,
    diagnostics: run.diagnostics,
    checkpoint: run.checkpoint,
    lineageMetadata: connectorResult.lineageMetadata
  };
}

export function listSourceConnectionRuns(state, tenantId, connectionId) {
  return state.sourceConnectionRuns.filter(
    (run) => run.tenantId === tenantId && run.connectionId === connectionId
  );
}
