import test from "node:test";
import assert from "node:assert/strict";
import { createPlatform } from "../src/app.js";

async function startServer() {
  const platform = createPlatform({ seedDemo: false, startBackground: false });
  await new Promise((resolve) => platform.server.listen(0, "127.0.0.1", resolve));
  const addr = platform.server.address();
  const baseUrl = `http://127.0.0.1:${addr.port}`;
  return {
    platform,
    baseUrl,
    stop: async () => {
      platform.close();
      await new Promise((resolve) => platform.server.close(() => resolve()));
    }
  };
}

function tenantHeaders(tenantId, role = "owner") {
  return {
    "content-type": "application/json",
    "x-tenant-id": tenantId,
    "x-user-id": "tester",
    "x-user-role": role
  };
}

async function createTenant(baseUrl, name, extra = {}) {
  const res = await fetch(`${baseUrl}/v1/tenants`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ name, ...extra })
  });
  assert.equal(res.status, 201);
  const data = await res.json();
  return data.tenant;
}

test("tenant isolation blocks cross-tenant audit access", async () => {
  const ctx = await startServer();
  try {
    const tenantA = await createTenant(ctx.baseUrl, "Tenant A");
    const tenantB = await createTenant(ctx.baseUrl, "Tenant B");

    await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenantA.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 14 })
    });

    await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenantB.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 14 })
    });

    const metricARes = await fetch(`${ctx.baseUrl}/v1/metrics/query?metricId=revenue&grain=day`, {
      headers: tenantHeaders(tenantA.id)
    });
    assert.equal(metricARes.status, 200);
    const metricA = await metricARes.json();
    assert.ok(metricA.points > 0);

    const forbiddenAudit = await fetch(`${ctx.baseUrl}/v1/audit/events?tenantId=${tenantB.id}`, {
      headers: tenantHeaders(tenantA.id)
    });
    assert.equal(forbiddenAudit.status, 403);
  } finally {
    await ctx.stop();
  }
});

test("connector retries do not duplicate canonical facts", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Retry Safe Co");

    const first = await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 10 })
    });
    const firstBody = await first.json();
    assert.ok(firstBody.lineageMetadata.insertedRecords > 0);

    const second = await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 10 })
    });
    const secondBody = await second.json();
    assert.equal(secondBody.lineageMetadata.insertedRecords, 0);
  } finally {
    await ctx.stop();
  }
});

test("model run adds quality warnings on low history and keeps actions policy-gated", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Guardrail Co", {
      autonomyPolicy: {
        confidenceThreshold: 0.95,
        actionAllowlist: ["notify_owner", "create_report"]
      }
    });

    await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 7 })
    });

    const model = await fetch(`${ctx.baseUrl}/v1/models/run`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ objective: "forecast", outputMetricIds: ["revenue"], horizonDays: 5 })
    });
    assert.equal(model.status, 200);
    const modelBody = await model.json();

    assert.equal(modelBody.run.status, "completed_with_warnings");
    assert.ok(modelBody.insight.qualityWarnings.includes("insufficient_history_for_reliable_modeling"));
    assert.ok(modelBody.insight.recommendedActions.every((a) => ["review", "deny"].includes(a.policyDecision)));

    const pending = await fetch(`${ctx.baseUrl}/v1/agents/actions/pending`, {
      headers: tenantHeaders(tenant.id)
    });
    const pendingBody = await pending.json();
    assert.ok(pendingBody.actions.length > 0);
  } finally {
    await ctx.stop();
  }
});

test("report generation returns deliveries and is queryable", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Reporting Co");

    await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 20 })
    });

    await fetch(`${ctx.baseUrl}/v1/connectors/quickbooks/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ domain: "finance", periodDays: 20 })
    });

    await fetch(`${ctx.baseUrl}/v1/models/run`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ objective: "forecast", outputMetricIds: ["revenue"], horizonDays: 5 })
    });

    const reportRes = await fetch(`${ctx.baseUrl}/v1/reports/generate`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ channels: ["email", "slack", "telegram"], metricIds: ["revenue", "profit"] })
    });
    assert.equal(reportRes.status, 201);
    const reportBody = await reportRes.json();
    assert.equal(reportBody.deliveryEvents.length, 3);

    const channelsRes = await fetch(`${ctx.baseUrl}/v1/channels/events`, { headers: tenantHeaders(tenant.id) });
    const channelsBody = await channelsRes.json();
    assert.ok(channelsBody.events.length >= 3);

    const reportsRes = await fetch(`${ctx.baseUrl}/v1/reports`, { headers: tenantHeaders(tenant.id) });
    const reportsBody = await reportsRes.json();
    assert.equal(reportsBody.reports.length, 1);
  } finally {
    await ctx.stop();
  }
});

test("source connection lifecycle supports create, test, sync, and run history", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Source Co");

    const connectionRes = await fetch(`${ctx.baseUrl}/v1/sources/connections`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        sourceType: "postgres",
        mode: "hybrid",
        auth: { host: "db.example", token: "secure" },
        syncPolicy: { intervalMinutes: 30, backfillDays: 14 }
      })
    });
    assert.equal(connectionRes.status, 201);
    const connectionBody = await connectionRes.json();
    const connection = connectionBody.connection;
    assert.equal(connection.sourceType, "postgres");
    assert.ok(connection.authRef.startsWith("secret_"));

    const testRes = await fetch(`${ctx.baseUrl}/v1/sources/connections/${connection.id}/test`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    const testBody = await testRes.json();
    assert.equal(testRes.status, 200);
    assert.equal(testBody.status, "success");

    const syncRes = await fetch(`${ctx.baseUrl}/v1/sources/connections/${connection.id}/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ periodDays: 10 })
    });
    assert.equal(syncRes.status, 200);
    const syncBody = await syncRes.json();
    assert.equal(syncBody.syncStatus, "success");
    assert.ok(syncBody.checkpoint.cursor);

    const runsRes = await fetch(`${ctx.baseUrl}/v1/sources/connections/${connection.id}/runs`, {
      headers: tenantHeaders(tenant.id)
    });
    const runsBody = await runsRes.json();
    assert.equal(runsRes.status, 200);
    assert.equal(runsBody.runs.length, 1);

    const badConnectionRes = await fetch(`${ctx.baseUrl}/v1/sources/connections`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        sourceType: "mysql",
        mode: "hybrid",
        auth: {}
      })
    });
    const badConnection = (await badConnectionRes.json()).connection;

    const badTestRes = await fetch(`${ctx.baseUrl}/v1/sources/connections/${badConnection.id}/test`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    const badTestBody = await badTestRes.json();
    assert.equal(badTestRes.status, 200);
    assert.equal(badTestBody.status, "failed");
    assert.match(badTestBody.diagnostics.message, /Missing credentials/i);
  } finally {
    await ctx.stop();
  }
});

test("live query enforces table policy and materialization writes tenant facts", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Live Query Co");

    const connectionRes = await fetch(`${ctx.baseUrl}/v1/sources/connections`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        sourceType: "bigquery",
        mode: "hybrid",
        auth: { serviceAccount: "x" },
        queryPolicy: {
          allowedTables: ["metrics_daily"],
          allowedColumnsByTable: {
            metrics_daily: ["date", "metricId", "value", "domain", "source"],
            default: ["date", "value"]
          }
        }
      })
    });
    const connection = (await connectionRes.json()).connection;

    await fetch(`${ctx.baseUrl}/v1/sources/connections/${connection.id}/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ periodDays: 15 })
    });

    const denyRes = await fetch(`${ctx.baseUrl}/v1/query/live`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        connectionId: connection.id,
        query: { table: "finance_ledger", columns: ["date", "profit"], limit: 20 }
      })
    });
    assert.equal(denyRes.status, 403);

    const liveRes = await fetch(`${ctx.baseUrl}/v1/query/live`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        connectionId: connection.id,
        query: { table: "metrics_daily", columns: ["date", "metricId", "value"], limit: 20 }
      })
    });
    assert.equal(liveRes.status, 200);
    const liveBody = await liveRes.json();
    assert.ok(liveBody.resultId);
    assert.ok(liveBody.rows.length > 0);

    const materializeRes = await fetch(`${ctx.baseUrl}/v1/query/materialize`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        connectionId: connection.id,
        resultId: liveBody.resultId,
        datasetName: "materialized_metrics",
        mapping: {
          domain: "ops",
          metricColumn: "metricId",
          valueColumn: "value",
          dateColumn: "date"
        }
      })
    });
    assert.equal(materializeRes.status, 201);
    const materializeBody = await materializeRes.json();
    assert.ok(materializeBody.run.insertedRecords > 0);
  } finally {
    await ctx.stop();
  }
});

test("skill pack install and run supports activation state and guardrails", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Skill Co");

    await fetch(`${ctx.baseUrl}/v1/connectors/google_ads/sync`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ domain: "marketing", periodDays: 20 })
    });

    const installRes = await fetch(`${ctx.baseUrl}/v1/skills/install`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ skillId: "marketing-optimizer", active: true })
    });
    assert.equal(installRes.status, 201);
    const installBody = await installRes.json();
    const skillId = installBody.install.id;

    const runRes = await fetch(`${ctx.baseUrl}/v1/skills/run`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        skillId,
        intent: "marketing_optimization",
        channel: "web",
        requestedTools: ["model.run"],
        estimatedTokens: 1000,
        timeoutMs: 2000
      })
    });
    assert.equal(runRes.status, 200);
    const runBody = await runRes.json();
    assert.equal(runBody.run.skillId, skillId);

    const deactivateRes = await fetch(`${ctx.baseUrl}/v1/skills/${encodeURIComponent(skillId)}/deactivate`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(deactivateRes.status, 200);

    const runAfterDeactivate = await fetch(`${ctx.baseUrl}/v1/skills/run`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        skillId,
        intent: "marketing_optimization",
        channel: "web",
        requestedTools: ["model.run"],
        estimatedTokens: 1000,
        timeoutMs: 2000
      })
    });
    assert.equal(runAfterDeactivate.status, 400);
  } finally {
    await ctx.stop();
  }
});
