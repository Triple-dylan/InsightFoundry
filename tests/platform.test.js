import test from "node:test";
import assert from "node:assert/strict";
import { createPlatform } from "../src/app.js";

async function startServer() {
  const platform = await createPlatform({ seedDemo: false, startBackground: false });
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
      body: JSON.stringify({
        channels: ["email", "slack", "telegram"],
        metricIds: ["revenue", "profit"],
        channelTemplates: {
          slack: "[{{channel}}] {{reportTitle}} | {{reportSummary}} | confidence={{confidence}}"
        },
        channelTemplateContext: {
          runId: "manual_report",
          confidence: 0.88
        }
      })
    });
    assert.equal(reportRes.status, 201);
    const reportBody = await reportRes.json();
    assert.equal(reportBody.deliveryEvents.length, 3);
    const slackEvent = reportBody.deliveryEvents.find((event) => event.channel === "slack");
    assert.ok(slackEvent.payload.message.includes("confidence=0.88"));

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
    assert.equal(connection.syncPolicy.freshnessSlaHours, 24);
    assert.equal(connection.qualityPolicy.minQualityScore, 0.75);

    const patchRes = await fetch(`${ctx.baseUrl}/v1/sources/connections/${connection.id}`, {
      method: "PATCH",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        metadata: { owner: "ops-team" },
        syncPolicy: { freshnessSlaHours: 12 },
        qualityPolicy: { minQualityScore: 0.9, blockModelRun: true }
      })
    });
    assert.equal(patchRes.status, 200);
    const patched = (await patchRes.json()).connection;
    assert.equal(patched.metadata.owner, "ops-team");
    assert.equal(patched.syncPolicy.freshnessSlaHours, 12);
    assert.equal(patched.qualityPolicy.minQualityScore, 0.9);
    assert.equal(patched.qualityPolicy.blockModelRun, true);

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
    assert.ok(Array.isArray(syncBody.diagnostics.qualityChecks));
    assert.ok(syncBody.diagnostics.qualityChecks.length > 0);

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

    const toolsRes = await fetch(`${ctx.baseUrl}/v1/skills/tools`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(toolsRes.status, 200);
    const toolsBody = await toolsRes.json();
    assert.ok(toolsBody.tools.some((tool) => tool.id === "compute.finance_snapshot"));

    const runRes = await fetch(`${ctx.baseUrl}/v1/skills/run`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        skillId,
        intent: "marketing_optimization",
        channel: "web",
        requestedTools: ["compute.data_quality_snapshot", "model.run"],
        estimatedTokens: 1000,
        timeoutMs: 2000
      })
    });
    assert.equal(runRes.status, 200);
    const runBody = await runRes.json();
    assert.equal(runBody.run.skillId, skillId);
    assert.ok(Array.isArray(runBody.run.trace.guardrails));
    assert.ok(runBody.run.trace.guardrails.length > 0);
    assert.ok(runBody.run.artifacts.deterministicOutputs["compute.data_quality_snapshot"]);

    const patchInstalledSkill = await fetch(`${ctx.baseUrl}/v1/skills/installed/${encodeURIComponent(skillId)}`, {
      method: "PATCH",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        manifest: {
          guardrails: { confidenceMin: 0.66, tokenBudget: 3200 },
          prompts: { system: "Use deterministic snapshots before any model inference." },
          tools: [
            { id: "compute.data_quality_snapshot", allow: true },
            { id: "model.run", allow: true }
          ]
        }
      })
    });
    assert.equal(patchInstalledSkill.status, 200);
    const patchBody = await patchInstalledSkill.json();
    assert.equal(patchBody.skill.manifest.guardrails.confidenceMin, 0.66);
    assert.equal(patchBody.skill.manifest.tools.length, 2);

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

test("settings and profile/report endpoints are configurable", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Configurable Co");

    const settingsRes = await fetch(`${ctx.baseUrl}/v1/settings`, { headers: tenantHeaders(tenant.id) });
    assert.equal(settingsRes.status, 200);
    const settingsBody = await settingsRes.json();
    assert.equal(settingsBody.settings.general.name, "Configurable Co");

    const patchGeneral = await fetch(`${ctx.baseUrl}/v1/settings/general`, {
      method: "PATCH",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ name: "Configurable Co 2", timezone: "UTC", locale: "en-US" })
    });
    assert.equal(patchGeneral.status, 200);

    const profileCreate = await fetch(`${ctx.baseUrl}/v1/models/profiles`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ name: "Ops Forecast", objective: "forecast", targetMetricId: "revenue", horizonDays: 12 })
    });
    assert.equal(profileCreate.status, 201);
    const profileBody = await profileCreate.json();

    const activateProfile = await fetch(`${ctx.baseUrl}/v1/models/profiles/${profileBody.profile.id}/activate`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(activateProfile.status, 200);

    const reportTypeCreate = await fetch(`${ctx.baseUrl}/v1/reports/types`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        name: "Ops Digest",
        sections: ["kpi_snapshot", "narrative", "actions"],
        defaultChannels: ["email", "slack"],
        defaultFormat: "pdf"
      })
    });
    assert.equal(reportTypeCreate.status, 201);
    const reportType = (await reportTypeCreate.json()).reportType;

    const reportTypesRes = await fetch(`${ctx.baseUrl}/v1/reports/types`, { headers: tenantHeaders(tenant.id) });
    const reportTypesBody = await reportTypesRes.json();
    assert.ok(reportTypesBody.types.some((item) => item.name === "Founder Growth and Cash Cockpit"));

    const preview = await fetch(`${ctx.baseUrl}/v1/reports/types/${reportType.id}/preview`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(preview.status, 200);
    const previewBody = await preview.json();
    assert.match(previewBody.preview, /Preview/);

    const deliveryPreview = await fetch(`${ctx.baseUrl}/v1/reports/types/${reportType.id}/delivery-preview`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        channels: ["email", "slack"],
        reportTitle: "Ops Digest",
        reportSummary: "Summary",
        context: { runId: "run_x", confidence: 0.8 }
      })
    });
    assert.equal(deliveryPreview.status, 200);
    const deliveryPreviewBody = await deliveryPreview.json();
    assert.equal(deliveryPreviewBody.previews.length, 2);
  } finally {
    await ctx.stop();
  }
});

test("skill draft workflow validates and publishes to installed skills", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Skill Draft Co");

    const createDraft = await fetch(`${ctx.baseUrl}/v1/skills/drafts`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        manifest: {
          id: "tenant-custom-skill",
          version: "1.0.0",
          name: "Tenant Custom Skill",
          description: "Custom tenant skill",
          triggers: { intents: ["custom_intent"], channels: ["web", "api"] },
          tools: [{ id: "model.run", allow: true }],
          guardrails: { confidenceMin: 0.7, humanApprovalFor: ["adjust_budget"], budgetCapUsd: 5000, tokenBudget: 2000, timeBudgetMs: 8000, killSwitch: false },
          prompts: { system: "Act as tenant custom operator." },
          schedules: []
        }
      })
    });
    assert.equal(createDraft.status, 201);
    const draftId = (await createDraft.json()).draft.draftId;

    const validate = await fetch(`${ctx.baseUrl}/v1/skills/drafts/${draftId}/validate`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(validate.status, 200);
    const validateBody = await validate.json();
    assert.equal(validateBody.result.status, "valid");

    const publish = await fetch(`${ctx.baseUrl}/v1/skills/drafts/${draftId}/publish`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ active: true })
    });
    assert.equal(publish.status, 201);
    const publishBody = await publish.json();
    assert.ok(publishBody.install.id.includes("tenant-custom-skill@1.0.0"));
  } finally {
    await ctx.stop();
  }
});

test("analysis run quality gate can block model execution", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Quality Gate Co");

    const connectionRes = await fetch(`${ctx.baseUrl}/v1/sources/connections`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        sourceType: "google_ads",
        mode: "hybrid",
        auth: { token: "t" },
        qualityPolicy: { minQualityScore: 1, blockModelRun: true }
      })
    });
    const connectionId = (await connectionRes.json()).connection.id;

    const profilesRes = await fetch(`${ctx.baseUrl}/v1/models/profiles`, { headers: tenantHeaders(tenant.id) });
    const modelProfileId = (await profilesRes.json()).profiles[0].id;

    const reportTypesRes = await fetch(`${ctx.baseUrl}/v1/reports/types`, { headers: tenantHeaders(tenant.id) });
    const reportTypeId = (await reportTypesRes.json()).types[0].id;

    const runCreate = await fetch(`${ctx.baseUrl}/v1/analysis-runs`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        sourceConnectionId: connectionId,
        modelProfileId,
        reportTypeId
      })
    });
    const runId = (await runCreate.json()).run.id;

    const runExecute = await fetch(`${ctx.baseUrl}/v1/analysis-runs/${runId}/execute`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ forceSync: true })
    });
    assert.equal(runExecute.status, 400);
    const errorBody = await runExecute.json();
    assert.match(errorBody.error, /quality gate failed/i);
  } finally {
    await ctx.stop();
  }
});

test("guided analysis run executes end-to-end with artifacts", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Runflow Co");

    const connectionRes = await fetch(`${ctx.baseUrl}/v1/sources/connections`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ sourceType: "google_ads", mode: "hybrid", auth: { token: "t" } })
    });
    const connectionId = (await connectionRes.json()).connection.id;

    const profilesRes = await fetch(`${ctx.baseUrl}/v1/models/profiles`, { headers: tenantHeaders(tenant.id) });
    const modelProfileId = (await profilesRes.json()).profiles[0].id;

    const reportTypesRes = await fetch(`${ctx.baseUrl}/v1/reports/types`, { headers: tenantHeaders(tenant.id) });
    const reportTypeId = (await reportTypesRes.json()).types[0].id;

    const runCreate = await fetch(`${ctx.baseUrl}/v1/analysis-runs`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        sourceConnectionId: connectionId,
        modelProfileId,
        reportTypeId,
        channels: ["email", "slack"]
      })
    });
    assert.equal(runCreate.status, 201);
    const runId = (await runCreate.json()).run.id;

    const runExecute = await fetch(`${ctx.baseUrl}/v1/analysis-runs/${runId}/execute`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ forceSync: true })
    });
    assert.equal(runExecute.status, 200);
    const executed = (await runExecute.json()).run;
    assert.equal(executed.status, "completed");
    assert.ok(executed.artifacts.insightId);
    assert.ok(executed.artifacts.reportId);

    const insightRes = await fetch(`${ctx.baseUrl}/v1/insights/${executed.artifacts.insightId}`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(insightRes.status, 200);
    const insightBody = await insightRes.json();
    assert.equal(insightBody.insight.id, executed.artifacts.insightId);

    const reportRes = await fetch(`${ctx.baseUrl}/v1/reports/${executed.artifacts.reportId}`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(reportRes.status, 200);
    const reportBody = await reportRes.json();
    assert.equal(reportBody.report.id, executed.artifacts.reportId);

    const runDeliver = await fetch(`${ctx.baseUrl}/v1/analysis-runs/${runId}/deliver`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({ channels: ["email", "telegram"] })
    });
    assert.equal(runDeliver.status, 200);
    const deliveredBody = await runDeliver.json();
    assert.equal(deliveredBody.events.length, 2);
    const telegramEvent = deliveredBody.events.find((event) => event.channel === "telegram");
    assert.equal(telegramEvent.status, "failed");

    await fetch(`${ctx.baseUrl}/v1/settings/channels`, {
      method: "PATCH",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        telegram: {
          enabled: true,
          botTokenRef: "bot_ref",
          chatId: "chat_1"
        }
      })
    });

    const retry = await fetch(`${ctx.baseUrl}/v1/channels/events/${telegramEvent.id}/retry`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({})
    });
    assert.equal(retry.status, 200);
    const retryBody = await retry.json();
    assert.equal(retryBody.event.status, "delivered");
    assert.ok(Number(retryBody.event.attemptCount) >= 2);
  } finally {
    await ctx.stop();
  }
});

test("team settings and shared workspace threads stay tenant-scoped", async () => {
  const ctx = await startServer();
  try {
    const tenantA = await createTenant(ctx.baseUrl, "Collab Tenant A");
    const tenantB = await createTenant(ctx.baseUrl, "Collab Tenant B");

    const teamRes = await fetch(`${ctx.baseUrl}/v1/settings/team`, {
      headers: tenantHeaders(tenantA.id)
    });
    assert.equal(teamRes.status, 200);
    const teamBody = await teamRes.json();
    assert.ok(teamBody.team.length >= 1);

    const addMemberRes = await fetch(`${ctx.baseUrl}/v1/settings/team`, {
      method: "POST",
      headers: tenantHeaders(tenantA.id),
      body: JSON.stringify({
        name: "Analyst One",
        email: "analyst.one@example.com",
        role: "analyst"
      })
    });
    assert.equal(addMemberRes.status, 201);

    const folderCreateRes = await fetch(`${ctx.baseUrl}/v1/workspace/folders`, {
      method: "POST",
      headers: tenantHeaders(tenantA.id),
      body: JSON.stringify({ name: "Deal Desk" })
    });
    assert.equal(folderCreateRes.status, 201);
    const folderId = (await folderCreateRes.json()).folder.id;

    const threadCreateRes = await fetch(`${ctx.baseUrl}/v1/workspace/threads`, {
      method: "POST",
      headers: tenantHeaders(tenantA.id),
      body: JSON.stringify({
        folderId,
        title: "Large Renewal Escalation"
      })
    });
    assert.equal(threadCreateRes.status, 201);
    const threadId = (await threadCreateRes.json()).thread.id;

    const commentCreateRes = await fetch(`${ctx.baseUrl}/v1/workspace/threads/${threadId}/comments`, {
      method: "POST",
      headers: tenantHeaders(tenantA.id),
      body: JSON.stringify({
        authorName: "analyst.one",
        role: "comment",
        body: "Need pricing approval before EOD."
      })
    });
    assert.equal(commentCreateRes.status, 201);

    const commentsRes = await fetch(`${ctx.baseUrl}/v1/workspace/threads/${threadId}/comments`, {
      headers: tenantHeaders(tenantA.id)
    });
    assert.equal(commentsRes.status, 200);
    const commentsBody = await commentsRes.json();
    assert.ok(commentsBody.comments.some((item) => /pricing approval/i.test(item.body)));

    const crossTenantRead = await fetch(`${ctx.baseUrl}/v1/workspace/threads/${threadId}/comments`, {
      headers: tenantHeaders(tenantB.id)
    });
    assert.equal(crossTenantRead.status, 404);
  } finally {
    await ctx.stop();
  }
});

test("mcp settings and folder-scoped agent command approvals work end-to-end", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "MCP Agent Co");

    const foldersRes = await fetch(`${ctx.baseUrl}/v1/workspace/folders`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(foldersRes.status, 200);
    const folders = (await foldersRes.json()).folders;
    assert.ok(folders.length > 0);
    const folderId = folders[0].id;

    const threadsRes = await fetch(`${ctx.baseUrl}/v1/workspace/threads?folderId=${encodeURIComponent(folderId)}`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(threadsRes.status, 200);
    const threadId = (await threadsRes.json()).threads[0].id;

    const mcpCatalog = await fetch(`${ctx.baseUrl}/v1/settings/mcp/catalog`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(mcpCatalog.status, 200);
    const mcpCatalogBody = await mcpCatalog.json();
    assert.ok(mcpCatalogBody.providers.some((provider) => provider.provider === "filesystem"));

    const createMcp = await fetch(`${ctx.baseUrl}/v1/settings/mcp/servers`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        provider: "filesystem",
        name: "Workspace FS",
        endpoint: "",
        authRef: "",
        allowedFolderIds: [folderId]
      })
    });
    assert.equal(createMcp.status, 201);
    const mcpServerId = (await createMcp.json()).server.id;

    const testMcp = await fetch(`${ctx.baseUrl}/v1/settings/mcp/servers/${mcpServerId}/test`, {
      method: "POST",
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(testMcp.status, 200);
    const testMcpBody = await testMcp.json();
    assert.equal(testMcpBody.status, "success");

    const coworkJob = await fetch(`${ctx.baseUrl}/v1/agents/jobs`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        jobType: "cowork_thread",
        folderId,
        threadId,
        input: "Summarize blockers and next actions."
      })
    });
    assert.equal(coworkJob.status, 201);

    const commentsAfterJob = await fetch(`${ctx.baseUrl}/v1/workspace/threads/${threadId}/comments`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(commentsAfterJob.status, 200);
    const commentsBody = await commentsAfterJob.json();
    assert.ok(commentsBody.comments.some((comment) => /Agent cowork completed/i.test(comment.body)));

    const commandReq = await fetch(`${ctx.baseUrl}/v1/agents/device-commands`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        folderId,
        threadId,
        command: "pwd",
        args: []
      })
    });
    assert.equal(commandReq.status, 201);
    const requestId = (await commandReq.json()).request.id;

    const commandApprove = await fetch(`${ctx.baseUrl}/v1/agents/device-commands/${requestId}/approve`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        decision: "approve",
        executeNow: true
      })
    });
    assert.equal(commandApprove.status, 200);
    const commandApproveBody = await commandApprove.json();
    assert.equal(commandApproveBody.request.status, "executed");
    assert.equal(commandApproveBody.request.exitCode, 0);

    const deniedReq = await fetch(`${ctx.baseUrl}/v1/agents/device-commands`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        folderId,
        threadId,
        command: "rm",
        args: ["-rf", "/tmp/unsafe"]
      })
    });
    assert.equal(deniedReq.status, 201);
    const deniedReqBody = await deniedReq.json();
    assert.equal(deniedReqBody.request.status, "denied");
  } finally {
    await ctx.stop();
  }
});

test("quick add integrations connects source, channels, and mcp in settings flows", async () => {
  const ctx = await startServer();
  try {
    const tenant = await createTenant(ctx.baseUrl, "Integrations Quick Add Co");

    const catalogRes = await fetch(`${ctx.baseUrl}/v1/integrations/catalog`, {
      headers: tenantHeaders(tenant.id)
    });
    assert.equal(catalogRes.status, 200);
    const catalogBody = await catalogRes.json();
    assert.ok(catalogBody.integrations.some((item) => item.key === "google_ads"));
    assert.ok(catalogBody.integrations.some((item) => item.key === "slack_channel"));

    const quickSource = await fetch(`${ctx.baseUrl}/v1/integrations/quick-add`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        integrationKey: "google_ads",
        authRef: "ga_secret_ref",
        runInitialSync: true,
        periodDays: 10
      })
    });
    assert.equal(quickSource.status, 201);
    const quickSourceBody = await quickSource.json();
    assert.equal(quickSourceBody.result.connection.sourceType, "google_ads");
    assert.equal(quickSourceBody.result.initialSync.syncStatus, "success");

    const sourceConnections = await fetch(`${ctx.baseUrl}/v1/sources/connections`, {
      headers: tenantHeaders(tenant.id)
    });
    const sourceConnectionsBody = await sourceConnections.json();
    assert.ok(sourceConnectionsBody.connections.some((item) => item.sourceType === "google_ads"));

    const quickSlack = await fetch(`${ctx.baseUrl}/v1/integrations/quick-add`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        integrationKey: "slack_channel",
        webhookRef: "slack_ref_123"
      })
    });
    assert.equal(quickSlack.status, 201);

    const channelsRes = await fetch(`${ctx.baseUrl}/v1/settings/channels`, {
      headers: tenantHeaders(tenant.id)
    });
    const channelsBody = await channelsRes.json();
    assert.equal(channelsBody.channels.slack.enabled, true);
    assert.equal(channelsBody.channels.slack.webhookRef, "slack_ref_123");

    const foldersRes = await fetch(`${ctx.baseUrl}/v1/workspace/folders`, {
      headers: tenantHeaders(tenant.id)
    });
    const folderId = (await foldersRes.json()).folders[0].id;
    const quickMcp = await fetch(`${ctx.baseUrl}/v1/integrations/quick-add`, {
      method: "POST",
      headers: tenantHeaders(tenant.id),
      body: JSON.stringify({
        integrationKey: "google_drive_mcp",
        authRef: "gdrive_ref_123",
        allowedFolderIds: [folderId]
      })
    });
    assert.equal(quickMcp.status, 201);
    const quickMcpBody = await quickMcp.json();
    assert.equal(quickMcpBody.result.server.provider, "google-drive");
    assert.equal(quickMcpBody.result.test.status, "success");
  } finally {
    await ctx.stop();
  }
});
