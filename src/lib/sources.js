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
  { sourceType: "quickbooks", family: "saas", domains: ["finance"], modes: ["ingest", "hybrid"] },
  { sourceType: "google_sheets", family: "saas", domains: ["marketing", "finance", "sales", "ops"], modes: ["ingest", "hybrid"] },
  { sourceType: "excel_365", family: "saas", domains: ["marketing", "finance", "sales", "ops"], modes: ["ingest", "hybrid"] }
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

const QUALITY_CHECK_PRESETS = new Set([
  "null_check",
  "duplicate_guard",
  "spike_check",
  "schema_drift"
]);

function normalizeConnection(connection) {
  const rawQuality = Number(connection.qualityPolicy?.minQualityScore ?? 0.75);
  connection.syncPolicy = {
    intervalMinutes: Math.max(5, Number(connection.syncPolicy?.intervalMinutes ?? 60)),
    backfillDays: Math.max(1, Number(connection.syncPolicy?.backfillDays ?? 30)),
    freshnessSlaHours: Math.max(1, Number(connection.syncPolicy?.freshnessSlaHours ?? 24))
  };
  connection.qualityPolicy = {
    minQualityScore: Math.max(0, Math.min(1, rawQuality)),
    blockModelRun: Boolean(connection.qualityPolicy?.blockModelRun ?? false)
  };
  connection.metadata = {
    label: connection.metadata?.label ?? `${connection.sourceType}-connection`,
    owner: connection.metadata?.owner ?? "unassigned",
    qualityChecks: Array.isArray(connection.metadata?.qualityChecks)
      ? connection.metadata.qualityChecks.filter((check) => QUALITY_CHECK_PRESETS.has(check))
      : ["null_check", "duplicate_guard", "spike_check"],
    extractionSpec: connection.metadata?.extractionSpec ?? {}
  };
  if (!connection.metadata.qualityChecks.length) {
    connection.metadata.qualityChecks = ["null_check", "duplicate_guard", "spike_check"];
  }
  return connection;
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
      backfillDays: Number(payload.syncPolicy?.backfillDays ?? 30),
      freshnessSlaHours: Number(payload.syncPolicy?.freshnessSlaHours ?? 24)
    },
    qualityPolicy: {
      minQualityScore: Number(payload.qualityPolicy?.minQualityScore ?? 0.75),
      blockModelRun: Boolean(payload.qualityPolicy?.blockModelRun ?? false)
    },
    queryPolicy: {
      ...DEFAULT_QUERY_POLICY,
      ...(payload.queryPolicy ?? {})
    },
    metadata: {
      label: payload.label ?? `${sourceType}-${tenant.name}`,
      owner: payload.metadata?.owner ?? "unassigned",
      qualityChecks: payload.metadata?.qualityChecks ?? ["null_check", "duplicate_guard"],
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
  return normalizeConnection(connection);
}

export function listSourceConnections(state, tenantId) {
  return state.sourceConnections
    .filter((item) => item.tenantId === tenantId)
    .map(normalizeConnection);
}

export function requireSourceConnection(state, tenantId, connectionId) {
  const connection = state.sourceConnections.find((item) => item.id === connectionId && item.tenantId === tenantId);
  if (!connection) {
    const err = new Error(`Source connection '${connectionId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return normalizeConnection(connection);
}

function normalizePatch(connection, payload = {}) {
  if (payload.mode) {
    const entry = sourceCatalogEntry(connection.sourceType);
    connection.mode = normalizeMode(entry, payload.mode);
  }

  if (payload.syncPolicy) {
    connection.syncPolicy = {
      ...connection.syncPolicy,
      ...payload.syncPolicy
    };
    connection.syncPolicy.intervalMinutes = Number(connection.syncPolicy.intervalMinutes ?? 60);
    connection.syncPolicy.backfillDays = Number(connection.syncPolicy.backfillDays ?? 30);
    connection.syncPolicy.freshnessSlaHours = Number(connection.syncPolicy.freshnessSlaHours ?? 24);
  }

  if (payload.qualityPolicy) {
    connection.qualityPolicy = {
      ...connection.qualityPolicy,
      ...payload.qualityPolicy
    };
    connection.qualityPolicy.minQualityScore = Number(connection.qualityPolicy.minQualityScore ?? 0.75);
    connection.qualityPolicy.blockModelRun = Boolean(connection.qualityPolicy.blockModelRun);
  }

  if (payload.queryPolicy) {
    connection.queryPolicy = {
      ...connection.queryPolicy,
      ...payload.queryPolicy
    };
  }

  if (payload.metadata || payload.label || payload.extractionSpec) {
    connection.metadata = {
      ...connection.metadata,
      ...(payload.metadata ?? {})
    };
    if (payload.label) connection.metadata.label = payload.label;
    if (payload.extractionSpec) connection.metadata.extractionSpec = payload.extractionSpec;
  }
}

export function patchSourceConnection(state, tenantId, connectionId, payload = {}) {
  const connection = requireSourceConnection(state, tenantId, connectionId);
  normalizePatch(connection, payload);
  connection.updatedAt = new Date().toISOString();
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
  normalizeConnection(connection);
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

  const qualityChecks = connection.metadata.qualityChecks.map((checkName) => {
    if (checkName === "null_check") {
      const pass = connectorResult.qualityScore >= 0.6;
      return {
        check: checkName,
        status: pass ? "pass" : "fail",
        detail: pass ? "Null-rate within threshold" : "Null-rate exceeded threshold"
      };
    }
    if (checkName === "duplicate_guard") {
      const pass = connectorResult.lineageMetadata.insertedRecords <= connectorResult.canonicalRecords.length;
      return {
        check: checkName,
        status: pass ? "pass" : "fail",
        detail: pass ? "Duplicate guard passed" : "Duplicate ratio too high"
      };
    }
    if (checkName === "spike_check") {
      const pass = connectorResult.qualityScore >= 0.7;
      return {
        check: checkName,
        status: pass ? "pass" : "warn",
        detail: pass ? "No suspicious spike behavior" : "Potential spike detected in source series"
      };
    }
    if (checkName === "schema_drift") {
      const pass = !payload.simulateSchemaDrift;
      return {
        check: checkName,
        status: pass ? "pass" : "fail",
        detail: pass ? "Schema matches expected mapping" : "Schema drift detected in extraction payload"
      };
    }
    return {
      check: checkName,
      status: "warn",
      detail: "Unknown quality check preset"
    };
  });
  const checksPassed = qualityChecks.every((check) => check.status !== "fail");

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
      retries: Number(payload.retries ?? 0),
      qualityPassed: connectorResult.qualityScore >= Number(connection.qualityPolicy?.minQualityScore ?? 0.75) && checksPassed,
      qualityChecks
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
