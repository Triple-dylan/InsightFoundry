import { newId } from "./state.js";

const PROVIDER_DOMAINS = {
  postgres: ["finance", "sales", "ops"],
  mysql: ["finance", "sales", "ops"],
  sqlserver: ["finance", "sales", "ops"],
  snowflake: ["marketing", "finance", "sales", "ops"],
  bigquery: ["marketing", "finance", "sales", "ops"],
  redshift: ["marketing", "finance", "sales", "ops"],
  databricks: ["marketing", "finance", "sales", "ops"],
  google_ads: ["marketing"],
  meta_ads: ["marketing"],
  salesforce: ["sales", "ops"],
  hubspot: ["sales", "marketing"],
  stripe: ["finance"],
  quickbooks: ["finance"],
  google_sheets: ["marketing", "finance", "sales", "ops"],
  excel_365: ["marketing", "finance", "sales", "ops"],
  csv_upload: ["marketing", "finance", "sales", "ops"]
};

function metricShape(domain) {
  if (domain === "marketing") {
    return ["spend", "leads", "revenue"];
  }
  if (domain === "finance") {
    return ["cash_in", "cash_out"];
  }
  return ["events"];
}

function randomValue(metricId, dayOffset) {
  if (metricId === "spend") return 1200 + dayOffset * 18 + Math.floor(Math.random() * 90);
  if (metricId === "leads") return 120 + dayOffset * 3 + Math.floor(Math.random() * 20);
  if (metricId === "revenue") return 2800 + dayOffset * 29 + Math.floor(Math.random() * 210);
  if (metricId === "cash_in") return 4200 + dayOffset * 31 + Math.floor(Math.random() * 180);
  if (metricId === "cash_out") return 2100 + dayOffset * 16 + Math.floor(Math.random() * 145);
  return 1 + Math.floor(Math.random() * 10);
}

function daysAgo(offset) {
  const d = new Date();
  d.setUTCDate(d.getUTCDate() - offset);
  return d.toISOString().slice(0, 10);
}

function buildRecords({ tenantId, provider, domain, count, connectorRunId }) {
  const records = [];
  const metrics = metricShape(domain);
  for (let i = count - 1; i >= 0; i -= 1) {
    const date = daysAgo(i);
    for (const metricId of metrics) {
      records.push({
        tenantId,
        domain,
        metricId,
        date,
        value: randomValue(metricId, count - i),
        source: provider,
        lineage: {
          provider,
          extractedAt: new Date().toISOString(),
          connectorRunId
        }
      });
    }
  }
  return records;
}

function normalizeDomain(tenant, provider, requestedDomain) {
  if (requestedDomain) return requestedDomain;
  const supported = PROVIDER_DOMAINS[provider] ?? [];
  if (!supported.length) return tenant.domains[0] ?? "marketing";
  const valid = supported.find((domain) => tenant.domains.includes(domain));
  return valid ?? supported[0];
}

export function ingestCanonicalFact(state, fact) {
  const key = `${fact.tenantId}:${fact.date}:${fact.domain}:${fact.metricId}:${fact.source}`;
  if (state.factKeys.has(key)) return false;
  state.factKeys.add(key);
  state.facts.push(fact);
  return true;
}

export function runConnectorSync(state, tenant, provider, payload = {}) {
  const connectorRunId = newId("sync");
  const domain = normalizeDomain(tenant, provider, payload.domain);
  const periodDays = Math.max(7, Math.min(90, Number(payload.periodDays ?? 30)));
  const records = buildRecords({
    tenantId: tenant.id,
    provider,
    domain,
    count: periodDays,
    connectorRunId
  });

  let inserted = 0;
  for (const record of records) {
    if (ingestCanonicalFact(state, record)) inserted += 1;
  }

  const qualityScore = Math.min(0.99, 0.8 + inserted / Math.max(1, records.length) * 0.2);
  const run = {
    id: connectorRunId,
    tenantId: tenant.id,
    provider,
    domain,
    extractionSpec: payload.extractionSpec ?? {},
    schedule: payload.schedule ?? "manual",
    generatedRecords: records.length,
    insertedRecords: inserted,
    qualityScore: Number(qualityScore.toFixed(3)),
    status: "success",
    createdAt: new Date().toISOString()
  };

  state.connectorRuns.push(run);
  return {
    syncStatus: run.status,
    connectorRunId: run.id,
    qualityScore: run.qualityScore,
    canonicalRecords: records.slice(0, 25),
    lineageMetadata: {
      provider,
      domain,
      insertedRecords: run.insertedRecords
    }
  };
}
