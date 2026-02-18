import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import { fileURLToPath } from "node:url";
import { createState, createTenant, requireTenant } from "./lib/state.js";
import { runConnectorSync } from "./lib/connectors.js";
import { queryMetric } from "./lib/metrics.js";
import { runModelTask } from "./lib/models.js";
import { createReportSchedule, generateReport } from "./lib/reports.js";
import { createAgentJob, approveAction, listPendingActions } from "./lib/agents.js";
import { authContextFromHeaders, requireRole, requireTenantHeader } from "./lib/auth.js";
import { pushAudit, listAudit } from "./lib/audit.js";
import { listBlueprints } from "./lib/blueprints.js";
import { startScheduler } from "./lib/scheduler.js";
import {
  listSourceCatalog,
  createSourceConnection,
  listSourceConnections,
  requireSourceConnection,
  testSourceConnection,
  runSourceSync,
  listSourceConnectionRuns
} from "./lib/sources.js";
import { runLiveQuery, materializeQueryResult } from "./lib/query-broker.js";
import {
  listSkillCatalog,
  installSkillPack,
  listInstalledSkillPacks,
  runSkillPack,
  listSkillRuns,
  setSkillActivation
} from "./lib/skills.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = path.join(__dirname, "public", "index.html");

function parseJsonBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > 1024 * 1024) {
        const err = new Error("Request body too large");
        err.statusCode = 413;
        reject(err);
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (!chunks.length) {
        resolve({});
        return;
      }
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch (e) {
        const err = new Error("Invalid JSON body");
        err.statusCode = 400;
        reject(err);
      }
    });
    req.on("error", reject);
  });
}

function respondJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, null, 2));
}

function respondHtml(res, html) {
  res.writeHead(200, { "content-type": "text/html; charset=utf-8" });
  res.end(html);
}

function pathMatcher(pathname, pattern) {
  const route = pattern.split("/").filter(Boolean);
  const candidate = pathname.split("/").filter(Boolean);
  if (route.length !== candidate.length) return null;
  const params = {};
  for (let i = 0; i < route.length; i += 1) {
    if (route[i].startsWith(":")) {
      params[route[i].slice(1)] = decodeURIComponent(candidate[i]);
    } else if (route[i] !== candidate[i]) {
      return null;
    }
  }
  return params;
}

function demoSeed(state) {
  const tenant = createTenant(state, {
    name: "Acme Pilot",
    blueprintId: "cross-domain",
    trainingOptIn: false,
    autonomyPolicy: {
      autonomyMode: "policy-gated",
      autopilotEnabled: true
    }
  });

  const adsConnection = createSourceConnection(state, tenant, {
    sourceType: "google_ads",
    mode: "hybrid",
    auth: { token: "demo-token" },
    syncPolicy: { intervalMinutes: 60, backfillDays: 35 }
  });
  runSourceSync(state, tenant, adsConnection, { domain: "marketing", periodDays: 35 });

  const financeConnection = createSourceConnection(state, tenant, {
    sourceType: "quickbooks",
    mode: "hybrid",
    auth: { token: "demo-token" },
    syncPolicy: { intervalMinutes: 60, backfillDays: 35 }
  });
  runSourceSync(state, tenant, financeConnection, { domain: "finance", periodDays: 35 });

  installSkillPack(state, tenant, { skillId: "marketing-optimizer", active: true });
  installSkillPack(state, tenant, { skillId: "finance-forecast-analyst", active: true });

  return tenant;
}

export function createPlatform({ seedDemo = true, startBackground = true } = {}) {
  const state = createState();
  const demoTenant = seedDemo ? demoSeed(state) : null;

  const stopScheduler = startBackground
    ? startScheduler(state, (schedule) => {
        const tenant = requireTenant(state, schedule.tenantId);
        const result = generateReport(state, tenant, {
          title: `${schedule.name} (${tenant.name})`,
          metricIds: schedule.metricIds,
          channels: schedule.channels,
          format: schedule.format
        });

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: "scheduler",
          action: "report_scheduled_run",
          details: {
            scheduleId: schedule.id,
            reportId: result.report.id
          }
        });
      })
    : () => {};

  const server = http.createServer(async (req, res) => {
    try {
      const method = req.method ?? "GET";
      const base = new URL(req.url ?? "/", "http://localhost");
      const pathname = base.pathname;

      if (method === "GET" && pathname === "/") {
        const template = fs.readFileSync(INDEX_PATH, "utf8");
        const html = template.replaceAll("__DEMO_TENANT_ID__", demoTenant?.id ?? "");
        respondHtml(res, html);
        return;
      }

      if (method === "GET" && pathname === "/healthz") {
        respondJson(res, 200, { ok: true, uptimeSec: process.uptime() });
        return;
      }

      if (method === "GET" && pathname === "/v1/blueprints") {
        respondJson(res, 200, { blueprints: listBlueprints() });
        return;
      }

      if (method === "GET" && pathname === "/v1/tenants") {
        respondJson(res, 200, {
          tenants: [...state.tenants.values()].map((tenant) => ({
            id: tenant.id,
            name: tenant.name,
            blueprintId: tenant.blueprintId,
            domains: tenant.domains,
            trainingOptIn: tenant.trainingOptIn
          }))
        });
        return;
      }

      if (method === "POST" && pathname === "/v1/tenants") {
        const body = await parseJsonBody(req);
        if (!body.name) {
          respondJson(res, 400, { error: "name is required" });
          return;
        }

        const tenant = createTenant(state, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: "control_plane",
          action: "tenant_created",
          details: { blueprintId: tenant.blueprintId }
        });

        respondJson(res, 201, { tenant });
        return;
      }

      if (method === "GET" && pathname === "/v1/sources/catalog") {
        respondJson(res, 200, { sources: listSourceCatalog() });
        return;
      }

      if (method === "POST" && pathname === "/v1/sources/connections") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);

        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const connection = createSourceConnection(state, tenant, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "source_connection_created",
          details: {
            connectionId: connection.id,
            sourceType: connection.sourceType,
            mode: connection.mode
          }
        });

        respondJson(res, 201, { connection });
        return;
      }

      if (method === "GET" && pathname === "/v1/sources/connections") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const connections = listSourceConnections(state, ctx.tenantId);
        respondJson(res, 200, { connections });
        return;
      }

      const sourceConnectionMatch = pathMatcher(pathname, "/v1/sources/connections/:connectionId/test");
      if (method === "POST" && sourceConnectionMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);

        const connection = requireSourceConnection(state, ctx.tenantId, sourceConnectionMatch.connectionId);
        const result = testSourceConnection(state, connection);

        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "source_connection_tested",
          details: {
            connectionId: connection.id,
            status: result.status
          }
        });

        respondJson(res, 200, result);
        return;
      }

      const sourceSyncMatch = pathMatcher(pathname, "/v1/sources/connections/:connectionId/sync");
      if (method === "POST" && sourceSyncMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);

        const tenant = requireTenant(state, ctx.tenantId);
        const connection = requireSourceConnection(state, ctx.tenantId, sourceSyncMatch.connectionId);
        const body = await parseJsonBody(req);
        const result = runSourceSync(state, tenant, connection, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "source_connection_synced",
          details: {
            connectionId: connection.id,
            sourceRunId: result.sourceRunId,
            status: result.syncStatus
          }
        });

        respondJson(res, 200, result);
        return;
      }

      const sourceRunsMatch = pathMatcher(pathname, "/v1/sources/connections/:connectionId/runs");
      if (method === "GET" && sourceRunsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);

        requireSourceConnection(state, ctx.tenantId, sourceRunsMatch.connectionId);
        const runs = listSourceConnectionRuns(state, ctx.tenantId, sourceRunsMatch.connectionId);
        respondJson(res, 200, { runs });
        return;
      }

      const connectorMatch = pathMatcher(pathname, "/v1/connectors/:provider/sync");
      if (method === "POST" && connectorMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);

        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = runConnectorSync(state, tenant, connectorMatch.provider, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "connector_sync",
          details: {
            provider: connectorMatch.provider,
            qualityScore: result.qualityScore,
            status: result.syncStatus
          }
        });

        respondJson(res, 200, result);
        return;
      }

      if (method === "GET" && pathname === "/v1/metrics/query") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);

        const result = queryMetric(state, ctx.tenantId, {
          metricId: base.searchParams.get("metricId"),
          grain: base.searchParams.get("grain") ?? "day",
          startDate: base.searchParams.get("startDate") ?? undefined,
          endDate: base.searchParams.get("endDate") ?? undefined
        });

        respondJson(res, 200, result);
        return;
      }

      if (method === "POST" && pathname === "/v1/query/live") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const connection = requireSourceConnection(state, ctx.tenantId, body.connectionId);
        const result = runLiveQuery(state, tenant, connection, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "live_query_executed",
          details: {
            connectionId: connection.id,
            resultId: result.resultId,
            rowCount: result.queryMetadata.rowCount
          }
        });

        respondJson(res, 200, result);
        return;
      }

      if (method === "POST" && pathname === "/v1/query/materialize") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);

        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const connection = requireSourceConnection(state, ctx.tenantId, body.connectionId);
        const run = materializeQueryResult(state, tenant, connection, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "live_query_materialized",
          details: {
            connectionId: connection.id,
            materializationId: run.id,
            insertedRecords: run.insertedRecords
          }
        });

        respondJson(res, 201, { run });
        return;
      }

      if (method === "POST" && pathname === "/v1/models/run") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);

        const body = await parseJsonBody(req);
        const tenant = requireTenant(state, ctx.tenantId);
        const result = runModelTask(state, tenant, {
          objective: body.objective ?? "forecast",
          inputs: body.inputs ?? [],
          outputMetricIds: body.outputMetricIds ?? ["revenue"],
          horizonDays: body.horizonDays ?? 7,
          provider: body.provider,
          preferByo: body.preferByo
        });

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "model_run",
          details: {
            modelRunId: result.run.id,
            objective: result.run.objective,
            status: result.run.status
          }
        });

        respondJson(res, 200, result);
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/catalog") {
        respondJson(res, 200, { skills: listSkillCatalog() });
        return;
      }

      if (method === "POST" && pathname === "/v1/skills/install") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);

        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const install = installSkillPack(state, tenant, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "skill_installed",
          details: {
            skillId: install.id
          }
        });

        respondJson(res, 201, { install });
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/installed") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const installed = listInstalledSkillPacks(state, ctx.tenantId);
        respondJson(res, 200, { installed });
        return;
      }

      if (method === "POST" && pathname === "/v1/skills/run") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);

        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const run = runSkillPack(state, tenant, body, {
          runModelTask,
          generateReport
        });

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "skill_run",
          details: {
            skillId: run.skillId,
            runId: run.id,
            status: run.status
          }
        });

        respondJson(res, 200, { run });
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/runs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const runs = listSkillRuns(state, ctx.tenantId);
        respondJson(res, 200, { runs });
        return;
      }

      const skillActivateMatch = pathMatcher(pathname, "/v1/skills/:skillId/activate");
      if (method === "POST" && skillActivateMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);

        const skill = setSkillActivation(state, ctx.tenantId, decodeURIComponent(skillActivateMatch.skillId), true);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "skill_activated",
          details: {
            skillId: skill.id
          }
        });

        respondJson(res, 200, { skill });
        return;
      }

      const skillDeactivateMatch = pathMatcher(pathname, "/v1/skills/:skillId/deactivate");
      if (method === "POST" && skillDeactivateMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);

        const skill = setSkillActivation(state, ctx.tenantId, decodeURIComponent(skillDeactivateMatch.skillId), false);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "skill_deactivated",
          details: {
            skillId: skill.id
          }
        });

        respondJson(res, 200, { skill });
        return;
      }

      if (method === "POST" && pathname === "/v1/reports/generate") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);

        const body = await parseJsonBody(req);
        const tenant = requireTenant(state, ctx.tenantId);
        const result = generateReport(state, tenant, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "report_generated",
          details: {
            reportId: result.report.id,
            channels: body.channels ?? ["email"]
          }
        });

        respondJson(res, 201, result);
        return;
      }

      if (method === "POST" && pathname === "/v1/reports/schedules") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);

        const body = await parseJsonBody(req);
        const tenant = requireTenant(state, ctx.tenantId);
        const schedule = createReportSchedule(state, tenant, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "report_schedule_created",
          details: { scheduleId: schedule.id }
        });

        respondJson(res, 201, { schedule });
        return;
      }

      if (method === "GET" && pathname === "/v1/reports") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const reports = state.reports.filter((item) => item.tenantId === ctx.tenantId);
        respondJson(res, 200, { reports });
        return;
      }

      if (method === "GET" && pathname === "/v1/channels/events") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const events = state.channelEvents.filter((item) => item.tenantId === ctx.tenantId);
        respondJson(res, 200, { events });
        return;
      }

      if (method === "POST" && pathname === "/v1/agents/jobs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);

        const body = await parseJsonBody(req);
        const tenant = requireTenant(state, ctx.tenantId);
        const job = createAgentJob(state, tenant, body);

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "agent_job_executed",
          details: {
            jobId: job.id,
            jobType: job.jobType,
            status: job.status
          }
        });

        respondJson(res, 201, { job });
        return;
      }

      if (method === "POST" && pathname === "/v1/agents/actions/approve") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);

        const body = await parseJsonBody(req);
        const approval = approveAction(state, ctx.tenantId, body);

        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "agent_action_reviewed",
          details: {
            actionId: body.actionId,
            decision: body.decision
          }
        });

        respondJson(res, 200, { approval });
        return;
      }

      if (method === "GET" && pathname === "/v1/agents/actions/pending") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const actions = listPendingActions(state, ctx.tenantId);
        respondJson(res, 200, { actions });
        return;
      }

      if (method === "GET" && pathname === "/v1/insights/latest") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const latest = [...state.insights]
          .filter((item) => item.tenantId === ctx.tenantId)
          .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))[0] ?? null;
        respondJson(res, 200, { insight: latest });
        return;
      }

      if (method === "GET" && pathname === "/v1/audit/events") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenantId = base.searchParams.get("tenantId") ?? ctx.tenantId;
        if (tenantId !== ctx.tenantId) {
          respondJson(res, 403, { error: "Cross-tenant audit access denied" });
          return;
        }
        const since = base.searchParams.get("since") ?? undefined;
        const events = listAudit(state, { tenantId, since });
        respondJson(res, 200, { events });
        return;
      }

      respondJson(res, 404, { error: `No route for ${method} ${pathname}` });
    } catch (error) {
      const statusCode = Number(error.statusCode ?? 500);
      respondJson(res, statusCode, {
        error: error.message,
        statusCode
      });
    }
  });

  return {
    state,
    server,
    demoTenant,
    close: () => {
      stopScheduler();
      server.close();
    }
  };
}
