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
import { notifyReportDelivery, previewReportDelivery, retryChannelEvent } from "./lib/channels.js";
import {
  listSourceCatalog,
  createSourceConnection,
  listSourceConnections,
  requireSourceConnection,
  patchSourceConnection,
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
import {
  getTenantSettings,
  patchSettingsGeneral,
  patchSettingsModelPreferences,
  patchSettingsTraining,
  patchSettingsPolicies,
  getSettingsChannels,
  patchSettingsChannels,
  ensureTenantSettings
} from "./lib/settings.js";
import {
  listPresetProfiles,
  ensureDefaultModelProfiles,
  listModelProfiles,
  createModelProfile,
  requireModelProfile,
  patchModelProfile,
  activateModelProfile
} from "./lib/model-profiles.js";
import {
  ensureDefaultReportTypes,
  listReportTypes,
  createReportType,
  requireReportType,
  patchReportType,
  previewReportType
} from "./lib/report-types.js";
import {
  createSkillDraft,
  patchSkillDraft,
  requireSkillDraft,
  validateSkillDraft,
  publishSkillDraft
} from "./lib/skill-drafts.js";
import {
  createAnalysisRun,
  listAnalysisRuns,
  requireAnalysisRun,
  executeAnalysisRun,
  deliverAnalysisRun
} from "./lib/analysis-runs.js";
import { createPersistence, loadStateFromPersistence, saveStateToPersistence } from "./lib/persistence.js";

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
  ensureTenantSettings(state, tenant);
  ensureDefaultModelProfiles(state, tenant);
  ensureDefaultReportTypes(state, tenant);

  return tenant;
}

export async function createPlatform({ seedDemo = true, startBackground = true } = {}) {
  const state = createState();
  const persistence = await createPersistence();
  const hydrated = await loadStateFromPersistence(state, persistence);
  let demoTenant = null;
  if (seedDemo && !hydrated && state.tenants.size === 0) {
    demoTenant = demoSeed(state);
    await saveStateToPersistence(state, persistence);
  } else if (state.tenants.size > 0) {
    demoTenant = [...state.tenants.values()][0];
  }

  const persistState = async () => {
    await saveStateToPersistence(state, persistence);
  };

  const stopScheduler = startBackground
    ? startScheduler(state, async (schedule) => {
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
        await persistState();
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

      if (method === "GET" && pathname === "/v1/feature-flags") {
        respondJson(res, 200, { flags: state.featureFlags });
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
        ensureTenantSettings(state, tenant);
        ensureDefaultModelProfiles(state, tenant);
        ensureDefaultReportTypes(state, tenant);
        await persistState();

        respondJson(res, 201, { tenant });
        return;
      }

      if (method === "GET" && pathname === "/v1/settings") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureDefaultModelProfiles(state, tenant);
        ensureDefaultReportTypes(state, tenant);
        const settings = getTenantSettings(state, tenant);
        respondJson(res, 200, { settings });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/general") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const settings = patchSettingsGeneral(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "settings_general_updated", details: {} });
        await persistState();
        respondJson(res, 200, { settings });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/model-preferences") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const settings = patchSettingsModelPreferences(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "settings_model_preferences_updated", details: {} });
        await persistState();
        respondJson(res, 200, { settings });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/training") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const settings = patchSettingsTraining(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "settings_training_updated", details: {} });
        await persistState();
        respondJson(res, 200, { settings });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/policies") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const settings = patchSettingsPolicies(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "settings_policies_updated", details: {} });
        await persistState();
        respondJson(res, 200, { settings });
        return;
      }

      if (method === "GET" && pathname === "/v1/settings/channels") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        const channels = getSettingsChannels(state, tenant);
        respondJson(res, 200, { channels });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/channels") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const channels = patchSettingsChannels(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "settings_channels_updated", details: {} });
        await persistState();
        respondJson(res, 200, { channels });
        return;
      }

      if (method === "GET" && pathname === "/v1/models/profiles") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureDefaultModelProfiles(state, tenant);
        respondJson(res, 200, { presets: listPresetProfiles(), profiles: listModelProfiles(state, tenant.id) });
        return;
      }

      if (method === "POST" && pathname === "/v1/models/profiles") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const profile = createModelProfile(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "model_profile_created", details: { profileId: profile.id } });
        await persistState();
        respondJson(res, 201, { profile });
        return;
      }

      const modelProfilePatchMatch = pathMatcher(pathname, "/v1/models/profiles/:profileId");
      if (method === "PATCH" && modelProfilePatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const profile = patchModelProfile(state, ctx.tenantId, modelProfilePatchMatch.profileId, body);
        pushAudit(state, { tenantId: ctx.tenantId, actorId: ctx.userId, action: "model_profile_updated", details: { profileId: profile.id } });
        await persistState();
        respondJson(res, 200, { profile });
        return;
      }

      const modelProfileActivateMatch = pathMatcher(pathname, "/v1/models/profiles/:profileId/activate");
      if (method === "POST" && modelProfileActivateMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const profile = activateModelProfile(state, tenant, modelProfileActivateMatch.profileId);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "model_profile_activated", details: { profileId: profile.id } });
        await persistState();
        respondJson(res, 200, { profile });
        return;
      }

      if (method === "GET" && pathname === "/v1/reports/types") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureDefaultReportTypes(state, tenant);
        respondJson(res, 200, { types: listReportTypes(state, tenant.id) });
        return;
      }

      if (method === "POST" && pathname === "/v1/reports/types") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const reportType = createReportType(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "report_type_created", details: { typeId: reportType.id } });
        await persistState();
        respondJson(res, 201, { reportType });
        return;
      }

      const reportTypePatchMatch = pathMatcher(pathname, "/v1/reports/types/:typeId");
      if (method === "PATCH" && reportTypePatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const reportType = patchReportType(state, ctx.tenantId, reportTypePatchMatch.typeId, body);
        pushAudit(state, { tenantId: ctx.tenantId, actorId: ctx.userId, action: "report_type_updated", details: { typeId: reportType.id } });
        await persistState();
        respondJson(res, 200, { reportType });
        return;
      }

      const reportTypePreviewMatch = pathMatcher(pathname, "/v1/reports/types/:typeId/preview");
      if (method === "POST" && reportTypePreviewMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const type = requireReportType(state, ctx.tenantId, reportTypePreviewMatch.typeId);
        const latestInsight = [...state.insights]
          .filter((item) => item.tenantId === ctx.tenantId)
          .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))[0];
        const preview = previewReportType(type, { latestInsightSummary: latestInsight?.summary });
        respondJson(res, 200, { preview });
        return;
      }

      const reportTypeDeliveryPreviewMatch = pathMatcher(pathname, "/v1/reports/types/:typeId/delivery-preview");
      if (method === "POST" && reportTypeDeliveryPreviewMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const type = requireReportType(state, ctx.tenantId, reportTypeDeliveryPreviewMatch.typeId);
        const body = await parseJsonBody(req);
        const channels = Array.isArray(body.channels) && body.channels.length
          ? body.channels
          : type.defaultChannels;
        const report = {
          id: "preview_report",
          title: body.reportTitle ?? type.name,
          summary: body.reportSummary ?? "Preview summary"
        };
        const previews = previewReportDelivery(state, ctx.tenantId, channels, report, {
          templates: {
            ...(type.deliveryTemplates ?? {}),
            ...(body.deliveryTemplates ?? {})
          },
          context: body.context
        });
        respondJson(res, 200, { previews });
        return;
      }

      if (method === "POST" && pathname === "/v1/skills/drafts") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const draft = createSkillDraft(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "skill_draft_created", details: { draftId: draft.draftId } });
        await persistState();
        respondJson(res, 201, { draft });
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/drafts") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const drafts = state.skillDrafts.filter((item) => item.tenantId === ctx.tenantId);
        respondJson(res, 200, { drafts });
        return;
      }

      const skillDraftPatchMatch = pathMatcher(pathname, "/v1/skills/drafts/:draftId");
      if (method === "PATCH" && skillDraftPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const draft = patchSkillDraft(state, ctx.tenantId, skillDraftPatchMatch.draftId, body);
        pushAudit(state, { tenantId: ctx.tenantId, actorId: ctx.userId, action: "skill_draft_updated", details: { draftId: draft.draftId } });
        await persistState();
        respondJson(res, 200, { draft });
        return;
      }

      const skillDraftValidateMatch = pathMatcher(pathname, "/v1/skills/drafts/:draftId/validate");
      if (method === "POST" && skillDraftValidateMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const result = validateSkillDraft(state, ctx.tenantId, skillDraftValidateMatch.draftId);
        await persistState();
        respondJson(res, 200, { result });
        return;
      }

      const skillDraftPublishMatch = pathMatcher(pathname, "/v1/skills/drafts/:draftId/publish");
      if (method === "POST" && skillDraftPublishMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = publishSkillDraft(state, tenant, skillDraftPublishMatch.draftId, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "skill_draft_published", details: { draftId: skillDraftPublishMatch.draftId, skillId: result.install.id } });
        await persistState();
        respondJson(res, 201, result);
        return;
      }

      if (method === "POST" && pathname === "/v1/analysis-runs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const run = createAnalysisRun(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "analysis_run_created", details: { runId: run.id } });
        await persistState();
        respondJson(res, 201, { run });
        return;
      }

      if (method === "GET" && pathname === "/v1/analysis-runs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        respondJson(res, 200, { runs: listAnalysisRuns(state, ctx.tenantId) });
        return;
      }

      const analysisRunMatch = pathMatcher(pathname, "/v1/analysis-runs/:runId");
      if (method === "GET" && analysisRunMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const run = requireAnalysisRun(state, ctx.tenantId, analysisRunMatch.runId);
        respondJson(res, 200, { run });
        return;
      }

      const analysisRunExecuteMatch = pathMatcher(pathname, "/v1/analysis-runs/:runId/execute");
      if (method === "POST" && analysisRunExecuteMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const run = requireAnalysisRun(state, ctx.tenantId, analysisRunExecuteMatch.runId);
        const executed = executeAnalysisRun(state, tenant, run, {
          requireSourceConnection,
          runSourceSync,
          requireModelProfile,
          runModelTask,
          requireReportType,
          generateReport,
          runSkillPack
        }, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "analysis_run_executed", details: { runId: executed.id, status: executed.status } });
        await persistState();
        respondJson(res, 200, { run: executed });
        return;
      }

      const analysisRunDeliverMatch = pathMatcher(pathname, "/v1/analysis-runs/:runId/deliver");
      if (method === "POST" && analysisRunDeliverMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const run = requireAnalysisRun(state, ctx.tenantId, analysisRunDeliverMatch.runId);
        const delivered = deliverAnalysisRun(state, tenant, run, { notifyReportDelivery }, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "analysis_run_delivered", details: { runId: run.id, channelEvents: delivered.events.length } });
        await persistState();
        respondJson(res, 200, delivered);
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
        ensureTenantSettings(state, tenant);
        await persistState();

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

      const sourceConnectionPatchMatch = pathMatcher(pathname, "/v1/sources/connections/:connectionId");
      if (method === "PATCH" && sourceConnectionPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);

        const body = await parseJsonBody(req);
        const connection = patchSourceConnection(state, ctx.tenantId, sourceConnectionPatchMatch.connectionId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "source_connection_updated",
          details: {
            connectionId: connection.id
          }
        });
        await persistState();
        respondJson(res, 200, { connection });
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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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
        await persistState();

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

      const channelRetryMatch = pathMatcher(pathname, "/v1/channels/events/:eventId/retry");
      if (method === "POST" && channelRetryMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const body = await parseJsonBody(req);
        const event = retryChannelEvent(state, ctx.tenantId, channelRetryMatch.eventId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "channel_event_retried",
          details: {
            eventId: event.id,
            status: event.status,
            attemptCount: event.attemptCount
          }
        });
        await persistState();
        respondJson(res, 200, { event });
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
        await persistState();

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
        await persistState();

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

      const insightMatch = pathMatcher(pathname, "/v1/insights/:insightId");
      if (method === "GET" && insightMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const insight = state.insights.find(
          (item) => item.tenantId === ctx.tenantId && item.id === insightMatch.insightId
        );
        if (!insight) {
          respondJson(res, 404, { error: `Insight '${insightMatch.insightId}' not found` });
          return;
        }
        respondJson(res, 200, { insight });
        return;
      }

      const reportMatch = pathMatcher(pathname, "/v1/reports/:reportId");
      if (method === "GET" && reportMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const report = state.reports.find(
          (item) => item.tenantId === ctx.tenantId && item.id === reportMatch.reportId
        );
        if (!report) {
          respondJson(res, 404, { error: `Report '${reportMatch.reportId}' not found` });
          return;
        }
        respondJson(res, 200, { report });
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
        statusCode,
        checks: error.checks,
        details: error.details
      });
    }
  });

  return {
    state,
    server,
    demoTenant,
    close: () => {
      stopScheduler();
    }
  };
}
