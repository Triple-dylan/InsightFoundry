import fs from "node:fs";
import path from "node:path";
import http from "node:http";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { createState, createTenant, requireTenant, newId } from "./lib/state.js";
import { runConnectorSync } from "./lib/connectors.js";
import { queryMetric } from "./lib/metrics.js";
import { runModelTask } from "./lib/models.js";
import { createReportSchedule, generateReport } from "./lib/reports.js";
import {
  createAgentJob,
  listAgentJobs,
  createDeviceCommandRequest,
  listDeviceCommandRequests,
  approveDeviceCommandRequest,
  approveAction,
  listPendingActions
} from "./lib/agents.js";
import { authContextFromHeaders, requireRole, requireTenantHeader } from "./lib/auth.js";
import { pushAudit, listAudit } from "./lib/audit.js";
import { listBlueprints } from "./lib/blueprints.js";
import { startScheduler } from "./lib/scheduler.js";
import { notifyReportDelivery, previewReportDelivery, retryChannelEvent, notifyChatBridge } from "./lib/channels.js";
import {
  ensureCollaborationDefaults,
  listTeamMembers,
  addTeamMember,
  patchTeamMember,
  listWorkspaceFolders,
  requireWorkspaceFolder,
  createWorkspaceFolder,
  patchWorkspaceFolder,
  listWorkspaceThreads,
  createWorkspaceThread,
  patchWorkspaceThread,
  requireWorkspaceThread,
  listThreadComments,
  createThreadComment
} from "./lib/collaboration.js";
import {
  ensureWorkspaceCoreDefaults,
  listThreadMessages,
  createChatMessage,
  voteOnMessagePoll,
  listMessageReplies,
  getMiniThread,
  createAiReplyForMessage,
  listThreadAttachments,
  listFolderAttachments,
  listNotifications,
  markNotificationRead,
  getWorkspaceAgentProfile,
  patchWorkspaceAgentProfile,
  patchTeamMemberAppearance,
  listTeamMemberAppearance,
  listFolderAutomations,
  createFolderAutomation,
  patchFolderAutomation,
  runFolderAutomation,
  listAutomationRuns,
  parseHeartbeatContent,
  validateHeartbeatContent,
  processFolderAutomations
} from "./lib/workspace-core.js";
import {
  listProjectMemories,
  createProjectMemory,
  patchProjectMemory,
  listUserMemories,
  createUserMemory,
  patchUserMemory,
  buildMemoryContext,
  snapshotMemoryContext,
  listMemorySnapshots,
  ingestRememberCommand
} from "./lib/memory.js";
import {
  runDoctor,
  listDoctorRuns,
  listSecurityAuditRuns,
  loadThreatModel
} from "./lib/system-health.js";
import {
  listMcpProviderCatalog,
  listMcpServers,
  createMcpServer,
  patchMcpServer,
  testMcpServer
} from "./lib/mcp.js";
import { listIntegrationsCatalog, quickAddIntegration } from "./lib/integrations.js";
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
  ensureSkillRegistry,
  listSkillCatalog,
  listSkillRegistry,
  registerSkillInRegistry,
  listSkillTools,
  installSkillPack,
  listInstalledSkillPacks,
  runSkillPack,
  listSkillRuns,
  setSkillActivation,
  patchInstalledSkillPack
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
  normalizeProviderName,
  parseProviderCredentialEntry,
  maskApiKey,
  buildSkillManifestFromAnswers,
  buildSkillMarkdown,
  generateSkillArtifactsWithLlm,
  generateWebToolFromPrompt,
  generateChatReplyWithLlm
} from "./lib/skill-builder.js";
import {
  createAnalysisRun,
  listAnalysisRuns,
  requireAnalysisRun,
  executeAnalysisRun,
  deliverAnalysisRun
} from "./lib/analysis-runs.js";
import {
  createRealtimeHub,
  issueRealtimeToken,
  handleRealtimeUpgrade,
  publishRealtimeEvent,
  listRealtimeEvents
} from "./lib/realtime.js";
import {
  addMessageReaction,
  removeMessageReaction,
  listMessageReactions
} from "./lib/reactions.js";
import {
  startGoogleWorkspaceAuth,
  completeGoogleWorkspaceAuth,
  listWorkspaceDocFiles,
  openWorkspaceDocFile,
  linkWorkspaceDocToThread
} from "./lib/workspace-docs.js";
import {
  listWorkspaceTables,
  createWorkspaceTable,
  requireWorkspaceTable,
  patchWorkspaceTable,
  listWorkspaceTableRows,
  addWorkspaceTableRows,
  importLiveQueryToWorkspaceTable
} from "./lib/workspace-tables.js";
import {
  listReportTemplates,
  uploadReportTemplate,
  patchReportTemplate,
  activateReportTemplate,
  previewReportTemplate,
  requireReportTemplate,
  resolveActiveTemplate,
  compileReportTemplateBody
} from "./lib/report-templates.js";
import { createPersistence, loadStateFromPersistence, saveStateToPersistence } from "./lib/persistence.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const INDEX_PATH = path.join(__dirname, "public", "index.html");
const ALLOW_ORIGIN = process.env.ALLOW_ORIGIN ?? "";

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

function parseRawBody(req, limitBytes = 8 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (chunk) => {
      size += chunk.length;
      if (size > limitBytes) {
        const err = new Error("Request body too large");
        err.statusCode = 413;
        reject(err);
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => resolve(Buffer.concat(chunks)));
    req.on("error", reject);
  });
}

function parseMultipartForm(req) {
  return new Promise(async (resolve, reject) => {
    try {
      const contentType = String(req.headers["content-type"] ?? "");
      const boundaryMatch = contentType.match(/boundary=(?:"([^"]+)"|([^;]+))/i);
      if (!boundaryMatch) {
        const err = new Error("Missing multipart boundary");
        err.statusCode = 400;
        reject(err);
        return;
      }
      const boundary = boundaryMatch[1] || boundaryMatch[2];
      const body = await parseRawBody(req, 8 * 1024 * 1024);
      const marker = `--${boundary}`;
      const raw = body.toString("latin1");
      const blocks = raw.split(marker).slice(1, -1);
      const fields = {};
      let file = null;

      for (const block of blocks) {
        const trimmed = block.replace(/^\r\n/, "").replace(/\r\n$/, "");
        if (!trimmed) continue;
        const splitIndex = trimmed.indexOf("\r\n\r\n");
        if (splitIndex < 0) continue;
        const headerRaw = trimmed.slice(0, splitIndex);
        const contentRaw = trimmed.slice(splitIndex + 4).replace(/\r\n$/, "");
        const headers = {};
        for (const line of headerRaw.split("\r\n")) {
          const idx = line.indexOf(":");
          if (idx < 0) continue;
          const key = line.slice(0, idx).trim().toLowerCase();
          const value = line.slice(idx + 1).trim();
          headers[key] = value;
        }
        const disposition = headers["content-disposition"] || "";
        const nameMatch = disposition.match(/name="([^"]+)"/i);
        const fileMatch = disposition.match(/filename="([^"]*)"/i);
        const fieldName = nameMatch?.[1];
        if (!fieldName) continue;

        if (fileMatch && file == null) {
          const filename = fileMatch[1] || "upload.bin";
          const mimeType = headers["content-type"] || "application/octet-stream";
          const binary = Buffer.from(contentRaw, "latin1");
          file = {
            fieldName,
            filename,
            mimeType,
            size: binary.length,
            data: binary
          };
          continue;
        }

        fields[fieldName] = Buffer.from(contentRaw, "latin1").toString("utf8");
      }

      resolve({ fields, file });
    } catch (error) {
      reject(error);
    }
  });
}

function sanitizeAttachmentName(value = "") {
  const normalized = String(value).replaceAll("\\", "/").split("/").pop() ?? "file";
  const clean = normalized.trim().replace(/[^\w.\- ()]/g, "_").slice(0, 240);
  return clean || "file";
}

function buildAttachmentAsset(state, tenantId, userId, payload = {}) {
  const name = sanitizeAttachmentName(payload.name ?? payload.filename ?? "file");
  const mimeType = String(payload.mimeType ?? payload.type ?? "application/octet-stream").slice(0, 140);
  const rawBuffer = Buffer.isBuffer(payload.data)
    ? payload.data
    : payload.contentBase64
      ? Buffer.from(String(payload.contentBase64), "base64")
      : Buffer.alloc(0);
  const size = Number(payload.size ?? rawBuffer.length ?? 0);
  if (size > 2 * 1024 * 1024) {
    const err = new Error("Attachment exceeds 2MB limit");
    err.statusCode = 413;
    throw err;
  }
  const previewable = /^text\/|^application\/json$|^application\/pdf$|^image\//i.test(mimeType) && rawBuffer.length > 0;
  const contentBase64 = rawBuffer.length ? rawBuffer.toString("base64") : "";
  const createdAt = new Date().toISOString();
  const id = `asset_${crypto.randomUUID()}`;
  const asset = {
    id,
    tenantId,
    name,
    mimeType,
    size,
    previewable,
    contentBase64,
    createdBy: userId,
    createdAt
  };
  state.workspaceAttachments.push(asset);
  return asset;
}

function requireAttachmentAsset(state, tenantId, attachmentId) {
  const asset = state.workspaceAttachments.find((item) => item.tenantId === tenantId && item.id === attachmentId);
  if (!asset) {
    const err = new Error(`Attachment '${attachmentId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return asset;
}

function attachmentAssetView(asset) {
  let preview = null;
  if (asset.previewable && asset.contentBase64) {
    if (asset.mimeType.startsWith("text/") || asset.mimeType === "application/json") {
      preview = {
        kind: "text",
        text: Buffer.from(asset.contentBase64, "base64").toString("utf8").slice(0, 20000)
      };
    } else {
      preview = {
        kind: "dataUrl",
        dataUrl: `data:${asset.mimeType};base64,${asset.contentBase64}`
      };
    }
  }
  return {
    attachment: {
      id: asset.id,
      tenantId: asset.tenantId,
      name: asset.name,
      mimeType: asset.mimeType,
      size: asset.size,
      previewable: asset.previewable,
      createdBy: asset.createdBy,
      createdAt: asset.createdAt
    },
    refs: {
      previewUrl: `/v1/workspace/attachments/${encodeURIComponent(asset.id)}`,
      downloadUrl: `/v1/workspace/attachments/${encodeURIComponent(asset.id)}`
    },
    preview
  };
}

function mergeMessageAttachments(state, tenantId, attachments, attachmentIds) {
  const merged = [];
  if (Array.isArray(attachments)) {
    merged.push(...attachments);
  }
  if (Array.isArray(attachmentIds)) {
    for (const rawId of attachmentIds) {
      const attachmentId = String(rawId ?? "").trim();
      if (!attachmentId) continue;
      const asset = requireAttachmentAsset(state, tenantId, attachmentId);
      merged.push({
        id: attachmentId,
        attachmentId,
        name: asset.name,
        type: asset.mimeType,
        mimeType: asset.mimeType,
        size: asset.size,
        previewable: asset.previewable,
        previewUrl: `/v1/workspace/attachments/${encodeURIComponent(asset.id)}`
      });
    }
  }
  const dedup = new Map();
  for (const item of merged) {
    const key = String(item?.attachmentId ?? item?.id ?? "");
    if (!key) continue;
    dedup.set(key, item);
  }
  return [...dedup.values()];
}

function parseMentionTokens(body = "") {
  return [...String(body).matchAll(/@([a-zA-Z0-9._-]{2,64})/g)]
    .map((match) => String(match[1] || "").toLowerCase());
}

function providerFromModelPreset(value = "") {
  const preset = String(value || "").toLowerCase();
  if (!preset || preset === "auto") return "managed";
  if (preset.includes("gpt") || preset.includes("openai")) return "openai";
  if (preset.includes("claude") || preset.includes("anthropic")) return "anthropic";
  if (preset.includes("gemini") || preset.includes("google")) return "gemini";
  return normalizeProviderName(preset);
}

function shouldInvokeAiForMessage(messageBody, invokeAiMode, agentProfile) {
  if (invokeAiMode === "explicit") return true;
  if (invokeAiMode === "none") return false;
  const tokens = parseMentionTokens(messageBody);
  const aliases = new Set(
    [
      String(agentProfile?.name ?? "titus").toLowerCase().replaceAll(/\s+/g, ""),
      ...(Array.isArray(agentProfile?.mentionAliases) ? agentProfile.mentionAliases : [])
    ]
      .map((item) => String(item || "").toLowerCase().replaceAll(/\s+/g, ""))
      .filter(Boolean)
  );
  return tokens.some((token) => aliases.has(token.replaceAll(/\s+/g, "")));
}

function listActiveTeamMemberIds(state, tenantId) {
  return state.teamMembers
    .filter((member) => member.tenantId === tenantId)
    .filter((member) => member.status !== "inactive")
    .map((member) => member.id);
}

function createChatAiApprovalRecord(state, tenantId, payload = {}) {
  const requiredUserIds = [...new Set((payload.requiredUserIds || []).map((item) => String(item).trim()).filter(Boolean))];
  const approvedUserIds = [...new Set((payload.approvedUserIds || []).map((item) => String(item).trim()).filter(Boolean))];
  const now = new Date().toISOString();
  const record = {
    id: newId("chat_ai_approval"),
    tenantId,
    threadId: String(payload.threadId || ""),
    messageId: String(payload.messageId || ""),
    requestedBy: String(payload.requestedBy || ""),
    requiredUserIds,
    approvedUserIds,
    status: approvedUserIds.length >= Math.max(1, requiredUserIds.length) ? "approved" : "pending",
    aiOptions: {
      visibility: payload.aiOptions?.visibility || "shared",
      provider: payload.aiOptions?.provider || "",
      modelPreset: payload.aiOptions?.modelPreset || "",
      effort: payload.aiOptions?.effort || "high",
      planMode: Boolean(payload.aiOptions?.planMode),
      apiKey: payload.aiOptions?.apiKey || "",
      explicitResponseText: payload.aiOptions?.explicitResponseText || "",
      messageBody: payload.aiOptions?.messageBody || ""
    },
    createdAt: now,
    updatedAt: now,
    executedAt: null,
    aiMessageId: null
  };
  state.chatAiApprovals.push(record);
  return record;
}

function requireChatAiApproval(state, tenantId, approvalId) {
  const record = state.chatAiApprovals.find((item) => item.tenantId === tenantId && item.id === approvalId);
  if (!record) {
    const err = new Error(`chat_ai_approval '${approvalId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return record;
}

function listThreadChatAiApprovals(state, tenantId, threadId) {
  return state.chatAiApprovals
    .filter((item) => item.tenantId === tenantId && item.threadId === threadId)
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

function requireChatMessageById(state, tenantId, messageId) {
  const message = state.chatMessages.find((item) => item.tenantId === tenantId && item.id === messageId);
  if (!message) {
    const err = new Error(`Chat message '${messageId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return message;
}

function normalizeExternalThreadKey(channel, value = "") {
  const key = String(value || "").trim();
  if (!key) return "";
  return `${String(channel || "").toLowerCase()}:${key}`;
}

function listChannelThreadLinks(state, tenantId, channel, options = {}) {
  const scopedChannel = String(channel || "").toLowerCase();
  return state.channelThreadLinks
    .filter((item) => item.tenantId === tenantId && item.channel === scopedChannel)
    .filter((item) => (options.threadId ? item.threadId === options.threadId : true))
    .filter((item) => (options.externalThreadKey ? item.externalThreadKey === normalizeExternalThreadKey(scopedChannel, options.externalThreadKey) : true))
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

function upsertChannelThreadLink(state, tenantId, channel, externalThreadKey, threadId, metadata = {}) {
  const scopedChannel = String(channel || "").toLowerCase();
  const normalizedExternalKey = normalizeExternalThreadKey(scopedChannel, externalThreadKey);
  if (!scopedChannel || !normalizedExternalKey || !threadId) {
    const err = new Error("channel, externalThreadKey, and threadId are required");
    err.statusCode = 400;
    throw err;
  }
  const now = new Date().toISOString();
  const existing = state.channelThreadLinks.find(
    (item) =>
      item.tenantId === tenantId
      && item.channel === scopedChannel
      && item.externalThreadKey === normalizedExternalKey
  );
  if (existing) {
    existing.threadId = String(threadId);
    existing.metadata = {
      ...(existing.metadata || {}),
      ...(metadata && typeof metadata === "object" ? metadata : {})
    };
    existing.updatedAt = now;
    return existing;
  }
  const link = {
    id: newId("channel_thread_link"),
    tenantId,
    channel: scopedChannel,
    externalThreadKey: normalizedExternalKey,
    threadId: String(threadId),
    metadata: metadata && typeof metadata === "object" ? metadata : {},
    createdAt: now,
    updatedAt: now
  };
  state.channelThreadLinks.push(link);
  return link;
}

function findChannelThreadLink(state, tenantId, channel, externalThreadKey) {
  const scopedChannel = String(channel || "").toLowerCase();
  const normalizedExternalKey = normalizeExternalThreadKey(scopedChannel, externalThreadKey);
  if (!normalizedExternalKey) return null;
  return state.channelThreadLinks.find(
    (item) =>
      item.tenantId === tenantId
      && item.channel === scopedChannel
      && item.externalThreadKey === normalizedExternalKey
  ) || null;
}

function requireChannelThreadLink(state, tenantId, channel, linkId) {
  const scopedChannel = String(channel || "").toLowerCase();
  const link = state.channelThreadLinks.find(
    (item) => item.tenantId === tenantId && item.channel === scopedChannel && item.id === String(linkId)
  );
  if (!link) {
    const err = new Error(`channel thread link '${linkId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return link;
}

function extractInboundChannelPayload(channel, body = {}) {
  if (channel === "slack") {
    if (body.type === "url_verification" && body.challenge) {
      return { challenge: String(body.challenge), ignore: true };
    }
    const event = body.event && typeof body.event === "object" ? body.event : {};
    const channelId = String(event.channel || body.channel || "").trim();
    const threadTs = String(event.thread_ts || body.thread_ts || "").trim();
    return {
      text: String(event.text || body.text || "").trim(),
      externalUserId: String(event.user || body.user_id || "").trim(),
      authorName: String(event.user_name || body.user_name || "Slack User").trim(),
      externalThreadKey: threadTs && channelId ? `${channelId}:${threadTs}` : channelId
    };
  }
  if (channel === "discord") {
    const author = body.author && typeof body.author === "object" ? body.author : {};
    const channelId = String(body.channel_id || body.channel || "").trim();
    const threadId = String(body.thread_id || "").trim();
    return {
      text: String(body.content || body.text || "").trim(),
      externalUserId: String(author.id || body.user_id || "").trim(),
      authorName: String(author.username || body.username || "Discord User").trim(),
      externalThreadKey: threadId || channelId
    };
  }
  return {
    text: String(body.text || "").trim(),
    externalUserId: "",
    authorName: "Channel User",
    externalThreadKey: ""
  };
}

function resolveInboundBridgeThreadId(state, tenantId, channel, channelConfig, requestedThreadId = "", externalThreadKey = "") {
  const explicit = String(requestedThreadId || "").trim();
  if (explicit) return explicit;
  const linked = findChannelThreadLink(state, tenantId, channel, externalThreadKey);
  if (linked?.threadId) return linked.threadId;
  const bridge = channelConfig?.bridge && typeof channelConfig.bridge === "object" ? channelConfig.bridge : {};
  const configured = String(bridge.threadId || "").trim();
  return configured;
}

function verifyInboundChannelToken(channelConfig, providedToken = "") {
  const inbound = channelConfig?.inbound && typeof channelConfig.inbound === "object" ? channelConfig.inbound : {};
  const requiredToken = String(inbound.token || "").trim();
  if (!requiredToken) return true;
  return requiredToken === String(providedToken || "").trim();
}

async function executeChatAiApproval(state, tenant, ctx, approval, emitRealtime) {
  if (approval.status !== "approved") return { approval, aiMessage: null, chatResponse: null };
  if (approval.executedAt && approval.aiMessageId) {
    const existing = state.chatMessages.find((item) => item.tenantId === tenant.id && item.id === approval.aiMessageId) || null;
    return { approval, aiMessage: existing, chatResponse: null };
  }
  const sourceMessage = requireChatMessageById(state, tenant.id, approval.messageId);
  const thread = requireWorkspaceThread(state, tenant.id, approval.threadId);
  const agentProfile = getWorkspaceAgentProfile(state, tenant.id);
  emitRealtime({
    tenantId: tenant.id,
    threadId: approval.threadId,
    type: "chat.ai_working",
    payload: { messageId: sourceMessage.id, agentName: agentProfile.name }
  });
  const chatResponse = await buildChatResponse(state, tenant, ctx, {
    messageBody: approval.aiOptions?.messageBody || sourceMessage.body || "",
    explicitResponseText: approval.aiOptions?.explicitResponseText,
    provider: approval.aiOptions?.provider,
    modelPreset: approval.aiOptions?.modelPreset,
    effort: approval.aiOptions?.effort,
    apiKey: approval.aiOptions?.apiKey,
    planMode: Boolean(approval.aiOptions?.planMode),
    threadTitle: thread.title,
    agentProfile
  });
  const aiMessage = createAiReplyForMessage(state, tenant, {
    threadId: approval.threadId,
    messageId: sourceMessage.id,
    visibility: approval.aiOptions?.visibility || "shared",
    responseText: chatResponse.text,
    attachments: [],
    authorName: agentProfile.name
  }, ctx);
  const bridgeChannels = ["slack", "discord"].filter((channel) => channel !== String(sourceMessage.channel || "").toLowerCase());
  const bridgeEvents = aiMessage.visibility === "shared"
    ? notifyChatBridge(state, tenant.id, aiMessage, { channels: bridgeChannels })
    : [];
  approval.executedAt = new Date().toISOString();
  approval.aiMessageId = aiMessage.id;
  approval.status = "completed";
  approval.updatedAt = new Date().toISOString();
  emitRealtime({
    tenantId: tenant.id,
    threadId: approval.threadId,
    type: "chat.ai_reply_created",
    payload: {
      parentMessageId: aiMessage.parentMessageId || sourceMessage.id,
      aiMessageId: aiMessage.id,
      visibility: aiMessage.visibility
    }
  });
  emitRealtime({
    tenantId: tenant.id,
    threadId: approval.threadId,
    type: "chat.ai_approval_updated",
    payload: {
      approvalId: approval.id,
      status: approval.status,
      approvedCount: approval.approvedUserIds.length,
      requiredCount: approval.requiredUserIds.length,
      aiMessageId: aiMessage.id,
      bridgeEventCount: bridgeEvents.length
    }
  });
  return { approval, aiMessage, chatResponse, bridgeEvents };
}

function upsertModelApiKeyRef(state, tenantId, provider, apiKey) {
  const normalizedProvider = normalizeProviderName(provider);
  const value = String(apiKey ?? "").trim();
  if (!normalizedProvider || normalizedProvider === "managed" || !value) return null;
  const fingerprint = crypto.createHash("sha256").update(`${tenantId}:${normalizedProvider}:${value}`).digest("hex");
  const ref = `llmkey_${normalizedProvider}_${fingerprint.slice(0, 16)}`;
  const existing = state.secretRefs.get(ref);
  const now = new Date().toISOString();
  state.secretRefs.set(ref, {
    tenantId,
    kind: "llm_api_key",
    provider: normalizedProvider,
    hasCredentials: true,
    fingerprint,
    keyValue: value,
    masked: maskApiKey(value),
    createdAt: existing?.createdAt ?? now,
    updatedAt: now,
    lastValidatedAt: existing?.lastValidatedAt ?? null
  });
  return `${normalizedProvider}:${ref}`;
}

function normalizeByoKeyRefs(state, tenantId, refs = []) {
  const normalized = [];
  const byProvider = new Map();
  for (const entry of refs) {
    const parsed = parseProviderCredentialEntry(entry);
    if (!parsed?.provider || parsed.provider === "managed") continue;
    const token = String(parsed.token ?? "").trim();
    if (!token) {
      byProvider.set(parsed.provider, parsed.provider);
      continue;
    }
    if (token.startsWith("secret_") || token.startsWith("llmkey_")) {
      byProvider.set(parsed.provider, `${parsed.provider}:${token}`);
      continue;
    }
    const storedRef = upsertModelApiKeyRef(state, tenantId, parsed.provider, token);
    if (storedRef) byProvider.set(parsed.provider, storedRef);
  }
  for (const value of byProvider.values()) normalized.push(value);
  return normalized;
}

function listModelKeySummaries(state, tenantId, settings) {
  const refs = Array.isArray(settings?.modelPreferences?.byoKeyRefs) ? settings.modelPreferences.byoKeyRefs : [];
  const rows = refs
    .map((entry) => parseProviderCredentialEntry(entry))
    .filter(Boolean)
    .map((parsed) => {
      const provider = normalizeProviderName(parsed.provider);
      const token = String(parsed.token ?? "").trim();
      if (!token) {
        return {
          provider,
          ref: "",
          status: "missing",
          maskedKey: "",
          updatedAt: null
        };
      }
      if (token.startsWith("secret_") || token.startsWith("llmkey_")) {
        const secret = state.secretRefs.get(token);
        const valid = Boolean(secret && secret.tenantId === tenantId && secret.kind === "llm_api_key" && secret.hasCredentials);
        return {
          provider,
          ref: token,
          status: valid ? "configured" : "missing",
          maskedKey: valid ? (secret.masked || "") : "",
          updatedAt: valid ? (secret.updatedAt || null) : null
        };
      }
      return {
        provider,
        ref: "",
        status: "configured",
        maskedKey: maskApiKey(token),
        updatedAt: null
      };
    });
  const dedup = new Map();
  for (const row of rows) dedup.set(row.provider, row);
  return [...dedup.values()].sort((a, b) => (a.provider > b.provider ? 1 : -1));
}

function resolveSkillGenerationCredential(state, tenantId, settings, preferredProvider, explicitApiKey = "") {
  const requested = normalizeProviderName(preferredProvider || settings?.modelPreferences?.defaultProvider || "managed");
  if (String(explicitApiKey || "").trim()) {
    return {
      provider: requested === "managed" ? "openai" : requested,
      apiKey: String(explicitApiKey).trim(),
      source: "request"
    };
  }
  const refs = Array.isArray(settings?.modelPreferences?.byoKeyRefs) ? settings.modelPreferences.byoKeyRefs : [];
  const candidates = refs
    .map((entry) => parseProviderCredentialEntry(entry))
    .filter(Boolean)
    .map((parsed) => {
      const provider = normalizeProviderName(parsed.provider);
      const token = String(parsed.token ?? "").trim();
      if (!token) return { provider, apiKey: "", source: "provider_only" };
      if (token.startsWith("secret_") || token.startsWith("llmkey_")) {
        const secret = state.secretRefs.get(token);
        if (secret && secret.tenantId === tenantId && secret.kind === "llm_api_key" && secret.hasCredentials) {
          return { provider, apiKey: String(secret.keyValue ?? "").trim(), source: "secret_ref" };
        }
        return { provider, apiKey: "", source: "secret_ref_missing" };
      }
      return { provider, apiKey: token, source: "inline" };
    })
    .filter((item) => item.provider && item.provider !== "managed");

  const byProvider = new Map();
  for (const candidate of candidates) {
    if (!byProvider.has(candidate.provider) || (!byProvider.get(candidate.provider).apiKey && candidate.apiKey)) {
      byProvider.set(candidate.provider, candidate);
    }
  }

  const requestedCandidate = requested !== "managed" ? byProvider.get(requested) : null;
  if (requestedCandidate?.apiKey) return requestedCandidate;

  const defaultProvider = normalizeProviderName(settings?.modelPreferences?.defaultProvider || "");
  const defaultCandidate = defaultProvider && defaultProvider !== "managed" ? byProvider.get(defaultProvider) : null;
  if (defaultCandidate?.apiKey) return defaultCandidate;

  const firstWithKey = [...byProvider.values()].find((item) => item.apiKey);
  if (firstWithKey) return firstWithKey;

  return {
    provider: requested === "managed" ? defaultProvider || "managed" : requested,
    apiKey: "",
    source: "none"
  };
}

function buildOnboardingState(state, tenant) {
  const settings = ensureTenantSettings(state, tenant);
  const checklist = settings.checklist ?? {};
  const hasConnections = Boolean(checklist.connectionsConfigured);
  const hasProvider = Boolean(settings.modelPreferences?.defaultProvider);
  const hasProfiles = Boolean(checklist.modelProfileConfigured);
  const hasRuns = state.analysisRuns.some((run) => run.tenantId === tenant.id && run.status === "completed");
  const hasDeliveries = state.channelEvents.some((event) => event.tenantId === tenant.id);
  const steps = [
    { id: "connect_source", title: "Connect data source", status: hasConnections ? "done" : "pending", actionId: "connect_source" },
    { id: "configure_llm", title: "Configure LLM model", status: hasProvider ? "done" : "pending", actionId: "configure_llm" },
    { id: "choose_profile", title: "Choose analysis profile", status: hasProfiles ? "done" : "pending", actionId: "choose_profile" },
    { id: "run_first_analysis", title: "Run first analysis", status: hasRuns ? "done" : "pending", actionId: "run_first_analysis" },
    { id: "deliver_report", title: "Deliver report", status: hasDeliveries ? "done" : "pending", actionId: "deliver_report" }
  ];
  const next = steps.find((step) => step.status === "pending") ?? null;
  return {
    tenantId: tenant.id,
    completed: Boolean(!next),
    steps,
    nextActionId: next?.actionId ?? null
  };
}

function ensureSoulDoc(state, tenantId, updatedBy = "system") {
  let soul = state.workspaceSoulDocs.find((item) => item.tenantId === tenantId);
  if (soul) return soul;
  soul = {
    tenantId,
    content: [
      "# soul.md",
      "",
      "## Identity",
      "- You are Titus, the collaborative operating copilot for this workspace.",
      "- Be concise, practical, and precise.",
      "",
      "## Core Principles",
      "- Truth over tone: never fabricate data or certainty.",
      "- Deterministic-first: use built-in metrics, checks, and skills before freeform reasoning.",
      "- Actionable outputs: each recommendation should map to a clear next step.",
      "",
      "## Collaboration Rules",
      "- Shared workspace by default.",
      "- Respect private user context and private AI exchanges.",
      "- Keep thread continuity and reference prior decisions.",
      "",
      "## Safety Constraints",
      "- Never leak cross-tenant data.",
      "- Respect policy and budget guardrails.",
      "- High-impact actions require approval.",
      "",
      "## Artifacts",
      "- heartbeat.md drives folder automation checks and triggers.",
      "- me.md provides per-user interaction preferences."
    ].join("\n"),
    updatedAt: new Date().toISOString(),
    updatedBy,
    exportPath: ""
  };
  state.workspaceSoulDocs.push(soul);
  return soul;
}

function ensureMeDoc(state, tenantId, userId = "ui-user", updatedBy = "system") {
  const scopedUser = String(userId || "ui-user");
  let me = state.workspaceMeDocs.find((item) => item.tenantId === tenantId && item.userId === scopedUser);
  if (me) return me;
  me = {
    tenantId,
    userId: scopedUser,
    content: [
      "# me.md",
      "",
      "## Working Style",
      "- Preferred response density: concise.",
      "- Prefer numbered action plans for execution.",
      "",
      "## Decision Preferences",
      "- Surface tradeoffs and risks before recommendation.",
      "- Highlight assumptions explicitly.",
      "",
      "## Collaboration Preferences",
      "- Ask clarifying questions only when blocked.",
      "- Default to implementation-first behavior."
    ].join("\n"),
    updatedAt: new Date().toISOString(),
    updatedBy,
    exportPath: ""
  };
  state.workspaceMeDocs.push(me);
  return me;
}

function safeToolName(value = "") {
  const clean = String(value || "Quick Tool")
    .trim()
    .replace(/[^\w.\- ]/g, "")
    .slice(0, 120);
  return clean || "Quick Tool";
}

function resolveToolGenerationCredential(state, tenantId, settings, preferredProvider, explicitApiKey = "") {
  return resolveSkillGenerationCredential(
    state,
    tenantId,
    settings,
    preferredProvider,
    explicitApiKey
  );
}

function buildFallbackWebTool(title, prompt) {
  const safeTitle = safeToolName(title || "Quick Tool");
  const safePrompt = String(prompt || "").slice(0, 2000).replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  return [
    "<!doctype html>",
    "<html>",
    "<head>",
    "<meta charset=\"utf-8\"/>",
    "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>",
    `<title>${safeTitle}</title>`,
    "<style>",
    "body{margin:0;font-family:ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto,sans-serif;background:#f5f7fb;color:#111827}",
    ".wrap{max-width:960px;margin:0 auto;padding:24px}",
    ".card{background:#fff;border:1px solid #e5e7eb;border-radius:14px;padding:16px;box-shadow:0 8px 28px rgba(15,23,42,.08)}",
    "textarea{width:100%;min-height:180px;border:1px solid #d1d5db;border-radius:10px;padding:10px}",
    "button{margin-top:10px;border:0;background:#0f172a;color:#fff;padding:10px 14px;border-radius:10px;cursor:pointer}",
    "</style>",
    "</head>",
    "<body>",
    "<div class=\"wrap\">",
    `<h1>${safeTitle}</h1>`,
    "<div class=\"card\">",
    "<p>Fallback tool scaffold generated because live model output was unavailable.</p>",
    `<p><strong>Original prompt:</strong> ${safePrompt}</p>`,
    "<textarea id=\"notes\" placeholder=\"Use this area to continue building...\"></textarea>",
    "<button onclick=\"alert('Scaffold ready. Replace with generated logic when model key is configured.')\">Run</button>",
    "</div>",
    "</div>",
    "</body>",
    "</html>"
  ].join("");
}

function buildPersonaResponse(state, tenantId, userId, messageBody, agentProfile, explicit = null, options = {}) {
  if (explicit != null && String(explicit).trim()) return String(explicit);
  const text = String(messageBody || "").trim();
  const me = ensureMeDoc(state, tenantId, userId, userId);
  const soul = ensureSoulDoc(state, tenantId, userId);
  const concisePreferred = /concise|brief|short/i.test(me.content);
  const numberedPreferred = /numbered|action plan|step/i.test(me.content);
  const safetyFirst = /approval|guardrail|safety/i.test(soul.content);

  let core = "";
  if (/forecast|projection|horizon/i.test(text)) {
    core = "Forecast path ready: I can run the horizon model, summarize confidence, and propose next actions.";
  } else if (/anomaly|risk|variance/i.test(text)) {
    core = "Anomaly path ready: I can run checks, score severity, and post evidence-backed actions.";
  } else if (/deal|quote|discount|margin/i.test(text)) {
    core = "Deal-desk path ready: I can run margin, discount, and approval checks with policy notes.";
  } else {
    core = "I can convert this into a structured run with evidence and delivery outputs.";
  }

  const suffix = [];
  if (numberedPreferred) suffix.push("I will answer in numbered steps.");
  if (safetyFirst) suffix.push("Approval and policy guardrails will be enforced.");
  const response = concisePreferred
    ? `${agentProfile.name}: ${core}`
    : `${agentProfile.name}: ${core} ${suffix.join(" ").trim()}`.trim();
  if (options.planMode) {
    return [
      `${agentProfile.name}: Plan mode`,
      "1. Clarify objective and constraints from the message.",
      "2. Select data scope and validation checks.",
      "3. Run the best-fit model/profile path.",
      "4. Summarize evidence with confidence and risks.",
      "5. Propose approval-gated next actions."
    ].join("\n");
  }
  return response;
}

async function buildChatResponse(state, tenant, ctx, options = {}) {
  const agentProfile = options.agentProfile || getWorkspaceAgentProfile(state, tenant.id);
  const explicit = options.explicitResponseText;
  if (explicit != null && String(explicit).trim()) {
    return {
      text: String(explicit),
      mode: "explicit",
      provider: "local",
      model: null,
      source: "request"
    };
  }

  const fallback = buildPersonaResponse(
    state,
    tenant.id,
    ctx.userId,
    options.messageBody,
    agentProfile,
    null,
    { planMode: Boolean(options.planMode) }
  );
  const settings = ensureTenantSettings(state, tenant);
  const requestedProvider = normalizeProviderName(
    options.provider
    || providerFromModelPreset(options.modelPreset)
    || settings.modelPreferences?.defaultProvider
    || "managed"
  );
  const resolved = resolveSkillGenerationCredential(
    state,
    tenant.id,
    settings,
    requestedProvider,
    options.apiKey
  );

  if (!resolved.apiKey || normalizeProviderName(resolved.provider) === "managed") {
    return {
      text: fallback,
      mode: "fallback",
      provider: normalizeProviderName(resolved.provider || requestedProvider || "managed"),
      model: null,
      source: resolved.source
    };
  }

  try {
    const me = ensureMeDoc(state, tenant.id, ctx.userId, ctx.userId);
    const soul = ensureSoulDoc(state, tenant.id, ctx.userId);
    const generated = await generateChatReplyWithLlm({
      provider: resolved.provider,
      apiKey: resolved.apiKey,
      agentName: agentProfile.name,
      userMessage: options.messageBody,
      meContent: me.content,
      soulContent: soul.content,
      effort: options.effort,
      planMode: Boolean(options.planMode),
      threadTitle: options.threadTitle || "",
      timeoutMs: 16000
    });
    if (!String(generated.text || "").trim()) {
      return {
        text: fallback,
        mode: "fallback",
        provider: generated.provider || normalizeProviderName(resolved.provider),
        model: generated.model || null,
        source: resolved.source
      };
    }
    return {
      text: generated.text,
      mode: "llm",
      provider: generated.provider || normalizeProviderName(resolved.provider),
      model: generated.model || null,
      source: resolved.source
    };
  } catch (error) {
    return {
      text: fallback,
      mode: "fallback",
      provider: normalizeProviderName(resolved.provider || requestedProvider || "managed"),
      model: null,
      source: resolved.source,
      warning: String(error?.message || "chat_llm_failed")
    };
  }
}

function respondJson(res, statusCode, payload) {
  res.writeHead(statusCode, { "content-type": "application/json; charset=utf-8" });
  res.end(JSON.stringify(payload, null, 2));
}

function respondHtml(res, html, statusCode = 200) {
  res.writeHead(statusCode, { "content-type": "text/html; charset=utf-8" });
  res.end(html);
}

function applyCors(req, res) {
  if (!ALLOW_ORIGIN) {
    if (req.method === "OPTIONS") {
      res.writeHead(204);
      res.end();
    }
    return;
  }
  res.setHeader("access-control-allow-origin", ALLOW_ORIGIN);
  res.setHeader("access-control-allow-methods", "GET,POST,PATCH,DELETE,OPTIONS");
  res.setHeader("access-control-allow-headers", "content-type,x-tenant-id,x-user-id,x-user-role,x-channel-id");
  res.setHeader("vary", "origin");
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
  }
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
  ensureSkillRegistry(state);
  ensureTenantSettings(state, tenant);
  ensureDefaultModelProfiles(state, tenant);
  ensureDefaultReportTypes(state, tenant);
  ensureCollaborationDefaults(state, tenant);
  ensureWorkspaceCoreDefaults(state, tenant);

  return tenant;
}

export async function createPlatform({ seedDemo = true, startBackground = true } = {}) {
  const state = createState();
  const persistence = await createPersistence();
  const startedAt = new Date().toISOString();
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

  const automationTimer = startBackground
    ? setInterval(async () => {
        const runs = processFolderAutomations(state, {
          runSkillPack,
          runModelTask,
          generateReport,
          createAgentJob: (localState, tenant, payload) => createAgentJob(localState, tenant, payload, {
            requireWorkspaceFolder,
            requireWorkspaceThread,
            createThreadComment,
            runSkillPack,
            runModelTask,
            generateReport
          }),
          createDeviceCommandRequest: (localState, tenant, payload) => createDeviceCommandRequest(localState, tenant, payload, {
            requireWorkspaceFolder,
            requireWorkspaceThread
          })
        });
        if (!runs.length) return;
        runs.forEach((run) => {
          pushAudit(state, {
            tenantId: run.tenantId,
            actorId: "automation_scheduler",
            action: "folder_automation_run",
            details: {
              automationId: run.automationId,
              runId: run.id,
              status: run.status,
              triggerType: run.triggerType
            }
          });
        });
        await persistState();
      }, 5_000)
    : null;

  const doctorTimer = startBackground
    ? setInterval(async () => {
        for (const tenant of state.tenants.values()) {
          if (!tenant.autonomyPolicy?.doctorAutoRun) continue;
          const intervalHours = Number(tenant.autonomyPolicy?.securityAuditIntervalHours ?? 24);
          const latest = state.doctorRuns
            .filter((item) => item.tenantId === tenant.id)
            .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))[0];
          const due = !latest
            || (Date.now() - Date.parse(latest.createdAt)) > intervalHours * 60 * 60 * 1000;
          if (!due) continue;
          const result = runDoctor(state, tenant, { applyFixes: false });
          pushAudit(state, {
            tenantId: tenant.id,
            actorId: "doctor_scheduler",
            action: "system_doctor_run",
            details: { runId: result.run.id, status: result.run.status, auto: true }
          });
        }
        await persistState();
      }, 60_000)
    : null;

  const realtimeHub = createRealtimeHub();
  const emitRealtime = (payload) => publishRealtimeEvent(state, realtimeHub, payload);

  const server = http.createServer(async (req, res) => {
    const requestId = crypto.randomUUID();
    res.setHeader("x-request-id", requestId);
    applyCors(req, res);
    if (req.method === "OPTIONS") return;
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
        respondJson(res, 200, {
          ok: true,
          uptimeSec: process.uptime(),
          startedAt,
          persistence: persistence.kind ?? "unknown",
          tenants: state.tenants.size,
          version: process.env.APP_VERSION ?? process.env.npm_package_version ?? "0.1.0"
        });
        return;
      }

      if (method === "GET" && pathname === "/v1/feature-flags") {
        respondJson(res, 200, { flags: state.featureFlags });
        return;
      }

      const channelInboundMatch = pathMatcher(pathname, "/v1/channels/:channel/inbound");
      if (method === "POST" && channelInboundMatch) {
        const channel = String(channelInboundMatch.channel || "").toLowerCase();
        if (!["slack", "discord"].includes(channel)) {
          respondJson(res, 400, { error: `Unsupported inbound channel '${channel}'` });
          return;
        }
        const body = await parseJsonBody(req);
        const tenantId = String(req.headers["x-tenant-id"] || body.tenantId || "").trim();
        if (!tenantId) {
          respondJson(res, 400, { error: "tenantId is required (header x-tenant-id or body.tenantId)" });
          return;
        }
        const tenant = requireTenant(state, tenantId);
        ensureCollaborationDefaults(state, tenant);
        ensureWorkspaceCoreDefaults(state, tenant);
        const settings = ensureTenantSettings(state, tenant);
        const channelConfig = settings.channels?.[channel] || {};
        const providedToken = String(
          req.headers["x-channel-token"]
          || base.searchParams.get("token")
          || body.token
          || ""
        ).trim();
        if (!verifyInboundChannelToken(channelConfig, providedToken)) {
          respondJson(res, 403, { error: "invalid_channel_token" });
          return;
        }
        const inbound = extractInboundChannelPayload(channel, body);
        if (inbound.ignore && inbound.challenge) {
          respondJson(res, 200, { challenge: inbound.challenge });
          return;
        }
        if (!inbound.text) {
          respondJson(res, 202, { accepted: false, reason: "empty_message" });
          return;
        }
        const resolvedThreadId = resolveInboundBridgeThreadId(
          state,
          tenant.id,
          channel,
          channelConfig,
          base.searchParams.get("threadId") || body.threadId,
          inbound.externalThreadKey
        );
        if (!resolvedThreadId) {
          respondJson(res, 400, { error: "No bridge thread configured. Set settings.channels.<channel>.bridge.threadId or pass threadId." });
          return;
        }
        const thread = requireWorkspaceThread(state, tenant.id, resolvedThreadId);
        let linkedThread = null;
        if (inbound.externalThreadKey) {
          linkedThread = upsertChannelThreadLink(
            state,
            tenant.id,
            channel,
            inbound.externalThreadKey,
            thread.id,
            {
              source: "inbound",
              authorName: inbound.authorName || "",
              externalUserId: inbound.externalUserId || ""
            }
          );
        }
        const authorName = `${channel[0].toUpperCase()}${channel.slice(1)} • ${inbound.authorName || "User"}`;
        const authorId = `${channel}:${inbound.externalUserId || crypto.randomUUID()}`;
        const actor = {
          tenantId: tenant.id,
          userId: authorId,
          userRole: "analyst",
          channel
        };
        const message = createChatMessage(state, tenant, {
          threadId: thread.id,
          folderId: thread.folderId,
          visibility: "shared",
          body: inbound.text,
          channel,
          authorType: "user",
          authorId,
          authorName
        }, actor);
        emitRealtime({
          tenantId: tenant.id,
          threadId: message.threadId,
          type: "chat.message_created",
          payload: {
            messageId: message.id,
            authorId: message.authorId,
            visibility: message.visibility
          }
        });

        let aiMessage = null;
        const agentProfile = getWorkspaceAgentProfile(state, tenant.id);
        const invokeAi = shouldInvokeAiForMessage(inbound.text, "auto", agentProfile);
        if (invokeAi) {
          emitRealtime({
            tenantId: tenant.id,
            threadId: message.threadId,
            type: "chat.ai_working",
            payload: { messageId: message.id, agentName: agentProfile.name }
          });
          const chatResponse = await buildChatResponse(state, tenant, actor, {
            messageBody: inbound.text,
            threadTitle: thread.title,
            agentProfile
          });
          aiMessage = createAiReplyForMessage(state, tenant, {
            threadId: message.threadId,
            messageId: message.id,
            visibility: "shared",
            responseText: chatResponse.text,
            attachments: [],
            authorName: agentProfile.name
          }, actor);
          emitRealtime({
            tenantId: tenant.id,
            threadId: message.threadId,
            type: "chat.ai_reply_created",
            payload: {
              parentMessageId: aiMessage.parentMessageId || message.id,
              aiMessageId: aiMessage.id,
              visibility: aiMessage.visibility
            }
          });
        }
        const bridgeTargets = ["slack", "discord"].filter((item) => item !== channel);
        const channelEvents = notifyChatBridge(state, tenant.id, message, { channels: bridgeTargets });
        if (aiMessage?.visibility === "shared") {
          channelEvents.push(...notifyChatBridge(state, tenant.id, aiMessage, { channels: bridgeTargets }));
        }
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: authorId,
          action: "channel_inbound_message_received",
          details: {
            channel,
            threadId: thread.id,
            messageId: message.id,
            aiTriggered: invokeAi,
            channelEventCount: channelEvents.length,
            externalThreadKey: inbound.externalThreadKey || null,
            linkedThreadLinkId: linkedThread?.id || null
          }
        });
        await persistState();
        respondJson(res, 201, { message, aiMessage, channelEvents, threadLink: linkedThread });
        return;
      }

      const channelThreadLinksMatch = pathMatcher(pathname, "/v1/channels/:channel/thread-links");
      if (channelThreadLinksMatch && method === "GET") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const channel = String(channelThreadLinksMatch.channel || "").toLowerCase();
        if (!["slack", "discord"].includes(channel)) {
          respondJson(res, 400, { error: `Unsupported channel '${channel}'` });
          return;
        }
        const threadId = String(base.searchParams.get("threadId") || "").trim();
        const externalThreadKey = String(base.searchParams.get("externalThreadKey") || "").trim();
        const links = listChannelThreadLinks(state, ctx.tenantId, channel, {
          threadId: threadId || undefined,
          externalThreadKey: externalThreadKey || undefined
        });
        respondJson(res, 200, { links });
        return;
      }

      if (channelThreadLinksMatch && method === "POST") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const channel = String(channelThreadLinksMatch.channel || "").toLowerCase();
        if (!["slack", "discord"].includes(channel)) {
          respondJson(res, 400, { error: `Unsupported channel '${channel}'` });
          return;
        }
        const body = await parseJsonBody(req);
        const threadId = String(body.threadId || "").trim();
        const externalThreadKey = String(body.externalThreadKey || "").trim();
        if (!threadId || !externalThreadKey) {
          respondJson(res, 400, { error: "threadId and externalThreadKey are required" });
          return;
        }
        requireWorkspaceThread(state, ctx.tenantId, threadId);
        const link = upsertChannelThreadLink(
          state,
          ctx.tenantId,
          channel,
          externalThreadKey,
          threadId,
          {
            source: "manual",
            updatedBy: ctx.userId
          }
        );
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "channel_thread_link_upserted",
          details: {
            linkId: link.id,
            channel: link.channel,
            threadId: link.threadId,
            externalThreadKey: link.externalThreadKey
          }
        });
        await persistState();
        respondJson(res, 201, { link });
        return;
      }

      const channelThreadLinkDeleteMatch = pathMatcher(pathname, "/v1/channels/:channel/thread-links/:linkId");
      if (channelThreadLinkDeleteMatch && method === "DELETE") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const channel = String(channelThreadLinkDeleteMatch.channel || "").toLowerCase();
        if (!["slack", "discord"].includes(channel)) {
          respondJson(res, 400, { error: `Unsupported channel '${channel}'` });
          return;
        }
        const link = requireChannelThreadLink(state, ctx.tenantId, channel, channelThreadLinkDeleteMatch.linkId);
        state.channelThreadLinks = state.channelThreadLinks.filter((item) => item.id !== link.id);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "channel_thread_link_deleted",
          details: {
            linkId: link.id,
            channel: link.channel,
            threadId: link.threadId,
            externalThreadKey: link.externalThreadKey
          }
        });
        await persistState();
        respondJson(res, 200, { removed: link });
        return;
      }

      if (method === "GET" && pathname === "/v1/realtime/token") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const token = issueRealtimeToken(ctx.tenantId, ctx.userId, ctx.channel);
        respondJson(res, 200, {
          token,
          wsPath: `/v1/realtime/ws?token=${encodeURIComponent(token)}`,
          expiresInSec: 1800
        });
        return;
      }

      if (method === "GET" && pathname === "/v1/realtime/ws") {
        respondJson(res, 426, { error: "Use WebSocket upgrade for /v1/realtime/ws" });
        return;
      }

      if (method === "GET" && pathname === "/v1/realtime/events") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const since = base.searchParams.get("since") ?? undefined;
        const events = listRealtimeEvents(state, ctx.tenantId, since);
        respondJson(res, 200, { events });
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
        ensureCollaborationDefaults(state, tenant);
        ensureSkillRegistry(state);
        ensureWorkspaceCoreDefaults(state, tenant);
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
        ensureCollaborationDefaults(state, tenant);
        ensureSkillRegistry(state);
        ensureWorkspaceCoreDefaults(state, tenant);
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
        if (Array.isArray(body.byoKeyRefs)) {
          body.byoKeyRefs = normalizeByoKeyRefs(state, tenant.id, body.byoKeyRefs);
        }
        const settings = patchSettingsModelPreferences(state, tenant, body);
        pushAudit(state, { tenantId: tenant.id, actorId: ctx.userId, action: "settings_model_preferences_updated", details: {} });
        await persistState();
        respondJson(res, 200, { settings });
        return;
      }

      if (method === "GET" && pathname === "/v1/settings/model-keys") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        const settings = ensureTenantSettings(state, tenant);
        const keys = listModelKeySummaries(state, tenant.id, settings);
        respondJson(res, 200, { keys });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/model-keys") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const settings = ensureTenantSettings(state, tenant);
        const incoming = Array.isArray(body.keys) ? body.keys : [];
        const existing = Array.isArray(settings.modelPreferences?.byoKeyRefs) ? [...settings.modelPreferences.byoKeyRefs] : [];
        const mapByProvider = new Map(existing
          .map((item) => parseProviderCredentialEntry(item))
          .filter(Boolean)
          .map((entry) => [normalizeProviderName(entry.provider), entry.token ? `${normalizeProviderName(entry.provider)}:${entry.token}` : normalizeProviderName(entry.provider)]));
        for (const row of incoming) {
          const provider = normalizeProviderName(row?.provider || "");
          const apiKey = String(row?.apiKey ?? "").trim();
          if (!provider || provider === "managed" || !apiKey) continue;
          const ref = upsertModelApiKeyRef(state, tenant.id, provider, apiKey);
          if (ref) mapByProvider.set(provider, ref);
        }
        settings.modelPreferences.byoKeyRefs = [...mapByProvider.values()];
        if (settings.modelPreferences.byoKeyRefs.length && settings.modelPreferences.defaultProvider === "managed") {
          const firstConfigured = parseProviderCredentialEntry(settings.modelPreferences.byoKeyRefs[0]);
          settings.modelPreferences.defaultProvider = normalizeProviderName(firstConfigured?.provider || "openai");
        }
        settings.updatedAt = new Date().toISOString();
        tenant.modelConfig.byoProviders = settings.modelPreferences.byoKeyRefs
          .map((item) => parseProviderCredentialEntry(item))
          .filter(Boolean)
          .map((item) => normalizeProviderName(item.provider))
          .filter((value) => value && value !== "managed");
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "settings_model_keys_updated",
          details: {
            providers: tenant.modelConfig.byoProviders
          }
        });
        await persistState();
        respondJson(res, 200, { keys: listModelKeySummaries(state, tenant.id, settings) });
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

      if (method === "GET" && pathname === "/v1/settings/mcp/catalog") {
        respondJson(res, 200, { providers: listMcpProviderCatalog() });
        return;
      }

      if (method === "GET" && pathname === "/v1/settings/mcp/servers") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        respondJson(res, 200, { servers: listMcpServers(state, ctx.tenantId) });
        return;
      }

      if (method === "POST" && pathname === "/v1/settings/mcp/servers") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const server = createMcpServer(state, tenant, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "mcp_server_created",
          details: { serverId: server.id, provider: server.provider }
        });
        await persistState();
        respondJson(res, 201, { server });
        return;
      }

      const mcpServerPatchMatch = pathMatcher(pathname, "/v1/settings/mcp/servers/:serverId");
      if (method === "PATCH" && mcpServerPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const server = patchMcpServer(state, ctx.tenantId, mcpServerPatchMatch.serverId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "mcp_server_updated",
          details: { serverId: server.id, status: server.status }
        });
        await persistState();
        respondJson(res, 200, { server });
        return;
      }

      const mcpServerTestMatch = pathMatcher(pathname, "/v1/settings/mcp/servers/:serverId/test");
      if (method === "POST" && mcpServerTestMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);
        const result = testMcpServer(state, ctx.tenantId, mcpServerTestMatch.serverId);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "mcp_server_tested",
          details: { serverId: result.serverId, status: result.status }
        });
        await persistState();
        respondJson(res, 200, result);
        return;
      }

      if (method === "GET" && pathname === "/v1/integrations/catalog") {
        respondJson(res, 200, { integrations: listIntegrationsCatalog() });
        return;
      }

      if (method === "POST" && pathname === "/v1/integrations/quick-add") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = quickAddIntegration(state, tenant, body, {
          createSourceConnection,
          runSourceSync,
          patchSettingsChannels,
          createMcpServer,
          testMcpServer
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "integration_quick_added",
          details: {
            integrationKey: result.integrationKey,
            kind: result.kind
          }
        });
        await persistState();
        respondJson(res, 201, { result });
        return;
      }

      if (method === "GET" && pathname === "/v1/integrations/google/workspace/auth/start") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const auth = startGoogleWorkspaceAuth(state, ctx.tenantId, ctx.userId);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "google_workspace_auth_started",
          details: { oauthState: auth.oauthState }
        });
        await persistState();
        respondJson(res, 200, auth);
        return;
      }

      if (method === "GET" && pathname === "/v1/integrations/google/workspace/auth/callback") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const auth = await completeGoogleWorkspaceAuth(state, ctx.tenantId, ctx.userId, {
          state: base.searchParams.get("state"),
          code: base.searchParams.get("code")
        });
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "google_workspace_auth_completed",
          details: { authId: auth.id }
        });
        await persistState();
        respondJson(res, 200, { auth });
        return;
      }

      if (method === "GET" && pathname === "/v1/settings/team") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureCollaborationDefaults(state, tenant);
        ensureWorkspaceCoreDefaults(state, tenant);
        respondJson(res, 200, {
          team: listTeamMembers(state, tenant.id),
          appearance: listTeamMemberAppearance(state, tenant.id)
        });
        return;
      }

      if (method === "POST" && pathname === "/v1/settings/team") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const member = addTeamMember(state, tenant, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "team_member_added",
          details: { memberId: member.id, role: member.role }
        });
        await persistState();
        respondJson(res, 201, { member });
        return;
      }

      const teamMemberPatchMatch = pathMatcher(pathname, "/v1/settings/team/:memberId");
      if (method === "PATCH" && teamMemberPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const member = patchTeamMember(state, ctx.tenantId, teamMemberPatchMatch.memberId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "team_member_updated",
          details: { memberId: member.id, role: member.role, status: member.status }
        });
        await persistState();
        respondJson(res, 200, { member });
        return;
      }

      const teamMemberAppearanceMatch = pathMatcher(pathname, "/v1/settings/team/:memberId/appearance");
      if (method === "PATCH" && teamMemberAppearanceMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const appearance = patchTeamMemberAppearance(state, ctx.tenantId, teamMemberAppearanceMatch.memberId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "team_member_appearance_updated",
          details: { memberId: teamMemberAppearanceMatch.memberId, colorMode: appearance.colorMode }
        });
        await persistState();
        respondJson(res, 200, { appearance });
        return;
      }

      if (method === "GET" && pathname === "/v1/settings/workspace-agent") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const profile = getWorkspaceAgentProfile(state, tenant.id);
        respondJson(res, 200, { profile });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/settings/workspace-agent") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const body = await parseJsonBody(req);
        const profile = patchWorkspaceAgentProfile(state, tenant.id, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_agent_profile_updated",
          details: { name: profile.name, tonePreset: profile.tonePreset }
        });
        await persistState();
        respondJson(res, 200, { profile });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/soul") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const soul = ensureSoulDoc(state, ctx.tenantId, ctx.userId);
        respondJson(res, 200, { soul });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/workspace/soul") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const soul = ensureSoulDoc(state, ctx.tenantId, ctx.userId);
        soul.content = String(body.content ?? soul.content);
        soul.updatedAt = new Date().toISOString();
        soul.updatedBy = ctx.userId;
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_soul_updated",
          details: {}
        });
        await persistState();
        respondJson(res, 200, { soul });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/soul/export") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const soul = ensureSoulDoc(state, ctx.tenantId, ctx.userId);
        const content = String(body.content ?? soul.content);
        const runtimeDir = path.join(process.cwd(), ".runtime", "soul", ctx.tenantId);
        fs.mkdirSync(runtimeDir, { recursive: true });
        const exportPath = path.join(runtimeDir, "soul.md");
        fs.writeFileSync(exportPath, content, "utf8");
        soul.content = content;
        soul.exportPath = exportPath;
        soul.updatedAt = new Date().toISOString();
        soul.updatedBy = ctx.userId;
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_soul_exported",
          details: { exportPath }
        });
        await persistState();
        respondJson(res, 200, { soul, path: exportPath });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/me") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const me = ensureMeDoc(state, ctx.tenantId, ctx.userId, ctx.userId);
        respondJson(res, 200, { me });
        return;
      }

      if (method === "PATCH" && pathname === "/v1/workspace/me") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const me = ensureMeDoc(state, ctx.tenantId, ctx.userId, ctx.userId);
        me.content = String(body.content ?? me.content);
        me.updatedAt = new Date().toISOString();
        me.updatedBy = ctx.userId;
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_me_updated",
          details: {}
        });
        await persistState();
        respondJson(res, 200, { me });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/me/export") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const me = ensureMeDoc(state, ctx.tenantId, ctx.userId, ctx.userId);
        const content = String(body.content ?? me.content);
        const runtimeDir = path.join(process.cwd(), ".runtime", "me", ctx.tenantId, ctx.userId);
        fs.mkdirSync(runtimeDir, { recursive: true });
        const exportPath = path.join(runtimeDir, "me.md");
        fs.writeFileSync(exportPath, content, "utf8");
        me.content = content;
        me.exportPath = exportPath;
        me.updatedAt = new Date().toISOString();
        me.updatedBy = ctx.userId;
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_me_exported",
          details: { exportPath }
        });
        await persistState();
        respondJson(res, 200, { me, path: exportPath });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/tools") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const tools = state.workspaceTools
          .filter((item) => item.tenantId === ctx.tenantId)
          .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1))
          .map((item) => ({
            id: item.id,
            tenantId: item.tenantId,
            threadId: item.threadId,
            name: item.name,
            prompt: item.prompt,
            html: item.html,
            provider: item.provider,
            model: item.model,
            mode: item.mode,
            warning: item.warning || null,
            createdBy: item.createdBy,
            createdAt: item.createdAt,
            updatedAt: item.updatedAt
          }));
        respondJson(res, 200, { tools });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/tools/generate") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const settings = ensureTenantSettings(state, tenant);
        const name = safeToolName(body.name || "Quick Tool");
        const prompt = String(body.prompt || "").trim();
        if (!prompt) {
          respondJson(res, 400, { error: "prompt is required" });
          return;
        }
        const resolved = resolveToolGenerationCredential(
          state,
          tenant.id,
          settings,
          body.provider,
          body.apiKey
        );

        let mode = "fallback";
        let provider = normalizeProviderName(resolved.provider || body.provider || settings.modelPreferences?.defaultProvider || "managed");
        let model = null;
        let warning = "";
        let html = "";
        if (resolved.apiKey && provider !== "managed") {
          try {
            const generated = await generateWebToolFromPrompt({
              provider,
              apiKey: resolved.apiKey,
              prompt,
              title: name
            });
            mode = "llm";
            provider = generated.provider;
            model = generated.model;
            html = generated.html;
          } catch (error) {
            warning = String(error.message || "tool_generation_failed");
          }
        } else {
          warning = "no_valid_api_key_configured_fallback_used";
        }
        if (!html) {
          html = buildFallbackWebTool(name, prompt);
        }

        const tool = {
          id: newId("workspace_tool"),
          tenantId: tenant.id,
          threadId: body.threadId ? String(body.threadId) : null,
          name,
          prompt,
          html,
          mode,
          provider,
          model,
          warning,
          createdBy: ctx.userId,
          createdAt: new Date().toISOString(),
          updatedAt: new Date().toISOString()
        };
        state.workspaceTools.push(tool);
        emitRealtime({
          tenantId: tenant.id,
          threadId: tool.threadId || undefined,
          type: "workspace.tool_generated",
          payload: {
            toolId: tool.id,
            mode: tool.mode,
            provider: tool.provider
          }
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_tool_generated",
          details: {
            toolId: tool.id,
            mode: tool.mode,
            provider: tool.provider
          }
        });
        await persistState();
        respondJson(res, 201, { tool });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/folders") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureCollaborationDefaults(state, tenant);
        ensureWorkspaceCoreDefaults(state, tenant);
        respondJson(res, 200, { folders: listWorkspaceFolders(state, tenant.id) });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/folders") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const folder = createWorkspaceFolder(state, tenant, { ...body, createdBy: ctx.userId });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_folder_created",
          details: { folderId: folder.id }
        });
        await persistState();
        respondJson(res, 201, { folder });
        return;
      }

      const workspaceFolderPatchMatch = pathMatcher(pathname, "/v1/workspace/folders/:folderId");
      if (method === "PATCH" && workspaceFolderPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const folder = patchWorkspaceFolder(state, ctx.tenantId, workspaceFolderPatchMatch.folderId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_folder_updated",
          details: { folderId: folder.id }
        });
        await persistState();
        respondJson(res, 200, { folder });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/threads") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureCollaborationDefaults(state, tenant);
        ensureWorkspaceCoreDefaults(state, tenant);
        const folderId = base.searchParams.get("folderId") ?? undefined;
        respondJson(res, 200, { threads: listWorkspaceThreads(state, tenant.id, { folderId }) });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/threads") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const thread = createWorkspaceThread(state, tenant, { ...body, createdBy: ctx.userId });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_thread_created",
          details: { threadId: thread.id, folderId: thread.folderId }
        });
        await persistState();
        respondJson(res, 201, { thread });
        return;
      }

      const workspaceThreadMatch = pathMatcher(pathname, "/v1/workspace/threads/:threadId");
      if (method === "GET" && workspaceThreadMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const thread = requireWorkspaceThread(state, ctx.tenantId, workspaceThreadMatch.threadId);
        respondJson(res, 200, { thread });
        return;
      }

      if (method === "PATCH" && workspaceThreadMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const thread = patchWorkspaceThread(state, ctx.tenantId, workspaceThreadMatch.threadId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_thread_updated",
          details: { threadId: thread.id, contextMode: thread.contextMode }
        });
        await persistState();
        respondJson(res, 200, { thread });
        return;
      }

      const workspaceChatMessagesMatch = pathMatcher(pathname, "/v1/workspace/chat/threads/:threadId/messages");
      if (method === "GET" && workspaceChatMessagesMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const thread = requireWorkspaceThread(state, ctx.tenantId, workspaceChatMessagesMatch.threadId);
        const messages = listThreadMessages(state, ctx.tenantId, workspaceChatMessagesMatch.threadId, ctx.userId, ctx.channel);
        const memoryContext = buildMemoryContext(state, ctx.tenantId, ctx.userId, {
          folderId: thread.folderId,
          threadId: thread.id,
          limit: 8
        });
        respondJson(res, 200, { messages, memoryContext });
        return;
      }

      if (method === "POST" && workspaceChatMessagesMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const body = await parseJsonBody(req);
        const thread = requireWorkspaceThread(state, ctx.tenantId, workspaceChatMessagesMatch.threadId);
        const messageAttachments = mergeMessageAttachments(state, ctx.tenantId, body.attachments, body.attachmentIds);
        const message = createChatMessage(state, tenant, {
          threadId: workspaceChatMessagesMatch.threadId,
          folderId: body.folderId,
          parentMessageId: body.parentMessageId,
          visibility: body.visibility,
          privateRecipientUserId: body.privateRecipientUserId,
          body: body.body,
          attachments: messageAttachments,
          poll: body.poll,
          channel: ctx.channel,
          authorType: body.authorType ?? "user",
          authorId: body.authorId ?? ctx.userId,
          authorName: body.authorName ?? ctx.userId
        }, ctx);
        emitRealtime({
          tenantId: tenant.id,
          threadId: message.threadId,
          type: "chat.message_created",
          payload: {
            messageId: message.id,
            authorId: message.authorId,
            visibility: message.visibility
          }
        });
        const bridgeChannels = ["slack", "discord"].filter((channel) => channel !== String(message.channel || "").toLowerCase());
        const bridgeEvents = message.visibility === "shared"
          ? notifyChatBridge(state, tenant.id, message, { channels: bridgeChannels })
          : [];
        const memoryCapture = ingestRememberCommand(state, tenant, ctx.userId, {
          body: body.body,
          folderId: message.folderId,
          threadId: message.threadId,
          tags: body.tags
        });
        let aiMessage = null;
        let chatMeta = null;
        const invokeAiMode = String(body.invokeAiMode ?? "none");
        const planMode = Boolean(body.planMode);
        const agentProfile = getWorkspaceAgentProfile(state, tenant.id);
        const invokeAi = shouldInvokeAiForMessage(body.body, invokeAiMode, agentProfile);
        const requireGroupApproval = Boolean(body.requireGroupApproval);
        let aiApproval = null;
        if (invokeAi && message.visibility === "shared" && requireGroupApproval) {
          let requiredUserIds = listActiveTeamMemberIds(state, tenant.id);
          if (!requiredUserIds.length) requiredUserIds = [ctx.userId];
          if (!requiredUserIds.includes(ctx.userId)) requiredUserIds.push(ctx.userId);
          aiApproval = createChatAiApprovalRecord(state, tenant.id, {
            threadId: message.threadId,
            messageId: message.id,
            requestedBy: ctx.userId,
            requiredUserIds,
            approvedUserIds: [ctx.userId],
            aiOptions: {
              visibility: body.aiVisibility ?? "shared",
              provider: body.provider || "",
              modelPreset: body.modelPreset || "",
              effort: body.effort || "high",
              planMode,
              apiKey: body.apiKey || "",
              explicitResponseText: body.aiResponseText || "",
              messageBody: body.body || ""
            }
          });
          emitRealtime({
            tenantId: tenant.id,
            threadId: message.threadId,
            type: "chat.ai_approval_requested",
            payload: {
              approvalId: aiApproval.id,
              messageId: message.id,
              requestedBy: ctx.userId,
              approvedCount: aiApproval.approvedUserIds.length,
              requiredCount: aiApproval.requiredUserIds.length
            }
          });
          if (aiApproval.status === "approved") {
            const executed = await executeChatAiApproval(state, tenant, ctx, aiApproval, emitRealtime);
            aiMessage = executed.aiMessage;
            chatMeta = executed.chatResponse;
          }
        }
        if (!aiApproval && invokeAi && message.visibility === "shared") {
          emitRealtime({
            tenantId: tenant.id,
            threadId: message.threadId,
            type: "chat.ai_working",
            payload: { messageId: message.id, agentName: agentProfile.name }
          });
          const chatResponse = await buildChatResponse(state, tenant, ctx, {
            messageBody: body.body,
            explicitResponseText: body.aiResponseText,
            provider: body.provider,
            modelPreset: body.modelPreset,
            effort: body.effort,
            apiKey: body.apiKey,
            planMode,
            threadTitle: thread.title,
            agentProfile
          });
          chatMeta = chatResponse;
          aiMessage = createAiReplyForMessage(state, tenant, {
            threadId: message.threadId,
            messageId: message.id,
            visibility: body.aiVisibility ?? "shared",
            responseText: chatResponse.text,
            attachments: [],
            authorName: agentProfile.name
          }, ctx);
          emitRealtime({
            tenantId: tenant.id,
            threadId: message.threadId,
            type: "chat.ai_reply_created",
            payload: {
              parentMessageId: aiMessage.parentMessageId || message.id,
              aiMessageId: aiMessage.id,
              visibility: aiMessage.visibility
            }
          });
          if (aiMessage.visibility === "shared") {
            bridgeEvents.push(...notifyChatBridge(state, tenant.id, aiMessage, { channels: bridgeChannels }));
          }
        }
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_message_created",
          details: {
            threadId: workspaceChatMessagesMatch.threadId,
            messageId: message.id,
            visibility: message.visibility,
            memoryCaptured: Boolean(memoryCapture),
            invokeAi,
            aiApprovalRequired: Boolean(aiApproval),
            aiApprovalStatus: aiApproval?.status || null,
            bridgeEventCount: bridgeEvents.length,
            chatMode: aiMessage ? (chatMeta?.mode || "llm") : "none",
            chatProvider: chatMeta?.provider || null,
            chatModel: chatMeta?.model || null
          }
        });
        await persistState();
        respondJson(res, 201, { message, memoryCapture, aiMessage, aiApproval, channelEvents: bridgeEvents });
        return;
      }

      const workspaceChatAiMatch = pathMatcher(pathname, "/v1/workspace/chat/threads/:threadId/messages/:messageId/ai");
      if (method === "POST" && workspaceChatAiMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const body = await parseJsonBody(req);
        const aiAttachments = mergeMessageAttachments(state, ctx.tenantId, body.attachments, body.attachmentIds);
        const agentProfile = getWorkspaceAgentProfile(state, tenant.id);
        const thread = requireWorkspaceThread(state, tenant.id, workspaceChatAiMatch.threadId);
        const sourceMessage = state.chatMessages.find(
          (item) => item.tenantId === tenant.id && item.id === workspaceChatAiMatch.messageId
        );
        if (Boolean(body.requireGroupApproval)) {
          let requiredUserIds = listActiveTeamMemberIds(state, tenant.id);
          if (!requiredUserIds.length) requiredUserIds = [ctx.userId];
          if (!requiredUserIds.includes(ctx.userId)) requiredUserIds.push(ctx.userId);
          const aiApproval = createChatAiApprovalRecord(state, tenant.id, {
            threadId: workspaceChatAiMatch.threadId,
            messageId: workspaceChatAiMatch.messageId,
            requestedBy: ctx.userId,
            requiredUserIds,
            approvedUserIds: [ctx.userId],
            aiOptions: {
              visibility: body.visibility || "shared",
              provider: body.provider || "",
              modelPreset: body.modelPreset || "",
              effort: body.effort || "high",
              planMode: Boolean(body.planMode),
              apiKey: body.apiKey || "",
              explicitResponseText: body.responseText || "",
              messageBody: String(body.sourceMessage || sourceMessage?.body || "")
            }
          });
          emitRealtime({
            tenantId: tenant.id,
            threadId: workspaceChatAiMatch.threadId,
            type: "chat.ai_approval_requested",
            payload: {
              approvalId: aiApproval.id,
              messageId: workspaceChatAiMatch.messageId,
              requestedBy: ctx.userId,
              approvedCount: aiApproval.approvedUserIds.length,
              requiredCount: aiApproval.requiredUserIds.length
            }
          });
          let aiMessage = null;
          if (aiApproval.status === "approved") {
            const executed = await executeChatAiApproval(state, tenant, ctx, aiApproval, emitRealtime);
            aiMessage = executed.aiMessage;
          }
          pushAudit(state, {
            tenantId: tenant.id,
            actorId: ctx.userId,
            action: "workspace_ai_approval_requested",
            details: {
              approvalId: aiApproval.id,
              threadId: workspaceChatAiMatch.threadId,
              messageId: workspaceChatAiMatch.messageId,
              requiredCount: aiApproval.requiredUserIds.length
            }
          });
          await persistState();
          respondJson(res, 201, { approval: aiApproval, message: aiMessage });
          return;
        }
        const chatResponse = await buildChatResponse(state, tenant, ctx, {
          messageBody: String(body.sourceMessage || sourceMessage?.body || ""),
          explicitResponseText: body.responseText,
          provider: body.provider,
          modelPreset: body.modelPreset,
          effort: body.effort,
          apiKey: body.apiKey,
          planMode: Boolean(body.planMode),
          threadTitle: thread.title,
          agentProfile
        });
        const message = createAiReplyForMessage(state, tenant, {
          threadId: workspaceChatAiMatch.threadId,
          messageId: workspaceChatAiMatch.messageId,
          visibility: body.visibility,
          responseText: chatResponse.text,
          attachments: aiAttachments,
          authorName: agentProfile.name
        }, ctx);
        const bridgeChannels = ["slack", "discord"].filter((channel) => channel !== String(sourceMessage?.channel || "").toLowerCase());
        const bridgeEvents = message.visibility === "shared"
          ? notifyChatBridge(state, tenant.id, message, { channels: bridgeChannels })
          : [];
        emitRealtime({
          tenantId: tenant.id,
          threadId: workspaceChatAiMatch.threadId,
          type: "chat.ai_reply_created",
          payload: {
            parentMessageId: message.parentMessageId || workspaceChatAiMatch.messageId,
            aiMessageId: message.id,
            visibility: message.visibility
          }
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_ai_reply_created",
          details: {
            threadId: workspaceChatAiMatch.threadId,
            messageId: workspaceChatAiMatch.messageId,
            aiMessageId: message.id,
            visibility: message.visibility,
            chatMode: chatResponse.mode,
            chatProvider: chatResponse.provider,
            chatModel: chatResponse.model,
            bridgeEventCount: bridgeEvents.length
          }
        });
        await persistState();
        respondJson(res, 201, { message, channelEvents: bridgeEvents });
        return;
      }

      const workspaceChatRepliesMatch = pathMatcher(pathname, "/v1/workspace/chat/threads/:threadId/messages/:messageId/replies");
      if (method === "GET" && workspaceChatRepliesMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const replies = listMessageReplies(
          state,
          ctx.tenantId,
          workspaceChatRepliesMatch.threadId,
          workspaceChatRepliesMatch.messageId,
          ctx.userId,
          ctx.channel
        );
        respondJson(res, 200, { replies });
        return;
      }

      if (method === "POST" && workspaceChatRepliesMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const body = await parseJsonBody(req);
        const replyAttachments = mergeMessageAttachments(state, ctx.tenantId, body.attachments, body.attachmentIds);
        const reply = createChatMessage(state, tenant, {
          threadId: workspaceChatRepliesMatch.threadId,
          parentMessageId: workspaceChatRepliesMatch.messageId,
          visibility: body.visibility,
          privateRecipientUserId: body.privateRecipientUserId,
          body: body.body,
          attachments: replyAttachments,
          channel: ctx.channel,
          authorType: body.authorType ?? "user",
          authorId: body.authorId ?? ctx.userId,
          authorName: body.authorName ?? ctx.userId
        }, ctx);
        const bridgeChannels = ["slack", "discord"].filter((channel) => channel !== String(reply.channel || "").toLowerCase());
        const bridgeEvents = reply.visibility === "shared"
          ? notifyChatBridge(state, tenant.id, reply, { channels: bridgeChannels })
          : [];
        emitRealtime({
          tenantId: tenant.id,
          threadId: reply.threadId,
          type: "chat.reply_created",
          payload: {
            messageId: reply.id,
            parentMessageId: workspaceChatRepliesMatch.messageId,
            authorId: reply.authorId,
            visibility: reply.visibility
          }
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_message_reply_created",
          details: {
            threadId: workspaceChatRepliesMatch.threadId,
            parentMessageId: workspaceChatRepliesMatch.messageId,
            messageId: reply.id,
            bridgeEventCount: bridgeEvents.length
          }
        });
        await persistState();
        respondJson(res, 201, { reply, channelEvents: bridgeEvents });
        return;
      }

      const workspaceMiniThreadMatch = pathMatcher(pathname, "/v1/workspace/chat/threads/:threadId/mini-threads/:parentMessageId");
      if (method === "GET" && workspaceMiniThreadMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const miniThread = getMiniThread(
          state,
          ctx.tenantId,
          workspaceMiniThreadMatch.threadId,
          workspaceMiniThreadMatch.parentMessageId,
          ctx.userId,
          ctx.channel
        );
        respondJson(res, 200, { miniThread });
        return;
      }

      const workspaceAiApprovalsMatch = pathMatcher(pathname, "/v1/workspace/chat/threads/:threadId/ai-approvals");
      if (method === "GET" && workspaceAiApprovalsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const thread = requireWorkspaceThread(state, ctx.tenantId, workspaceAiApprovalsMatch.threadId);
        const approvals = listThreadChatAiApprovals(state, ctx.tenantId, thread.id);
        respondJson(res, 200, { approvals });
        return;
      }

      const workspaceAiApprovalApproveMatch = pathMatcher(pathname, "/v1/workspace/chat/approvals/:approvalId/approve");
      if (method === "POST" && workspaceAiApprovalApproveMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const approval = requireChatAiApproval(state, tenant.id, workspaceAiApprovalApproveMatch.approvalId);
        if (approval.status === "completed") {
          respondJson(res, 200, { approval, aiMessageId: approval.aiMessageId || null });
          return;
        }
        if (!approval.requiredUserIds.includes(ctx.userId)) {
          const err = new Error("user_not_required_for_approval");
          err.statusCode = 403;
          throw err;
        }
        if (!approval.approvedUserIds.includes(ctx.userId)) {
          approval.approvedUserIds.push(ctx.userId);
        }
        const requiredCount = Math.max(1, approval.requiredUserIds.length);
        approval.status = approval.approvedUserIds.length >= requiredCount ? "approved" : "pending";
        approval.updatedAt = new Date().toISOString();
        emitRealtime({
          tenantId: tenant.id,
          threadId: approval.threadId,
          type: "chat.ai_approval_updated",
          payload: {
            approvalId: approval.id,
            status: approval.status,
            approvedCount: approval.approvedUserIds.length,
            requiredCount: approval.requiredUserIds.length
          }
        });
        let aiMessage = null;
        let chatMeta = null;
        if (approval.status === "approved") {
          const executed = await executeChatAiApproval(state, tenant, ctx, approval, emitRealtime);
          aiMessage = executed.aiMessage;
          chatMeta = executed.chatResponse;
        }
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_ai_approval_approved",
          details: {
            approvalId: approval.id,
            threadId: approval.threadId,
            approvedCount: approval.approvedUserIds.length,
            requiredCount: approval.requiredUserIds.length,
            chatMode: chatMeta?.mode || null
          }
        });
        await persistState();
        respondJson(res, 200, { approval, aiMessage });
        return;
      }

      const messagePollVoteMatch = pathMatcher(pathname, "/v1/workspace/chat/messages/:messageId/poll-vote");
      if (method === "POST" && messagePollVoteMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const optionIds = Array.isArray(body.optionIds) ? body.optionIds : [body.optionId].filter(Boolean);
        const poll = voteOnMessagePoll(state, tenant.id, messagePollVoteMatch.messageId, ctx.userId, optionIds);
        const message = state.chatMessages.find((item) => item.tenantId === tenant.id && item.id === messagePollVoteMatch.messageId);
        emitRealtime({
          tenantId: tenant.id,
          threadId: message?.threadId || null,
          type: "chat.poll_voted",
          payload: {
            messageId: messagePollVoteMatch.messageId,
            userId: ctx.userId,
            optionIds
          }
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_poll_voted",
          details: {
            messageId: messagePollVoteMatch.messageId,
            optionIds
          }
        });
        await persistState();
        respondJson(res, 200, { poll });
        return;
      }

      const messageReactionsMatch = pathMatcher(pathname, "/v1/workspace/chat/messages/:messageId/reactions");
      if (method === "GET" && messageReactionsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const reactions = listMessageReactions(state, ctx.tenantId, messageReactionsMatch.messageId, ctx.userId);
        respondJson(res, 200, { reactions });
        return;
      }

      if (method === "POST" && messageReactionsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const reaction = addMessageReaction(state, tenant.id, messageReactionsMatch.messageId, ctx.userId, body.emoji);
        const message = state.chatMessages.find((item) => item.tenantId === tenant.id && item.id === messageReactionsMatch.messageId);
        emitRealtime({
          tenantId: tenant.id,
          threadId: message?.threadId,
          type: "chat.reaction_added",
          payload: {
            messageId: messageReactionsMatch.messageId,
            reactionId: reaction.id,
            emoji: reaction.emoji,
            userId: reaction.userId
          }
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_reaction_added",
          details: { messageId: messageReactionsMatch.messageId, reactionId: reaction.id, emoji: reaction.emoji }
        });
        await persistState();
        respondJson(res, 201, { reaction, reactions: listMessageReactions(state, tenant.id, messageReactionsMatch.messageId, ctx.userId) });
        return;
      }

      const messageReactionDeleteMatch = pathMatcher(pathname, "/v1/workspace/chat/messages/:messageId/reactions/:reactionId");
      if (method === "DELETE" && messageReactionDeleteMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const removed = removeMessageReaction(
          state,
          tenant.id,
          messageReactionDeleteMatch.messageId,
          messageReactionDeleteMatch.reactionId,
          ctx.userId,
          ctx.userRole
        );
        const message = state.chatMessages.find((item) => item.tenantId === tenant.id && item.id === messageReactionDeleteMatch.messageId);
        emitRealtime({
          tenantId: tenant.id,
          threadId: message?.threadId,
          type: "chat.reaction_removed",
          payload: {
            messageId: messageReactionDeleteMatch.messageId,
            reactionId: removed.id,
            emoji: removed.emoji,
            userId: removed.userId
          }
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_reaction_removed",
          details: { messageId: messageReactionDeleteMatch.messageId, reactionId: removed.id, emoji: removed.emoji }
        });
        await persistState();
        respondJson(res, 200, { removed, reactions: listMessageReactions(state, tenant.id, messageReactionDeleteMatch.messageId, ctx.userId) });
        return;
      }

      const workspaceAttachmentsMatch = pathMatcher(pathname, "/v1/workspace/chat/threads/:threadId/attachments");
      if (method === "GET" && workspaceAttachmentsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const attachments = listThreadAttachments(state, ctx.tenantId, workspaceAttachmentsMatch.threadId, ctx.userId, ctx.channel);
        respondJson(res, 200, { attachments });
        return;
      }

      const workspaceFolderAttachmentsMatch = pathMatcher(pathname, "/v1/workspace/folders/:folderId/attachments");
      if (method === "GET" && workspaceFolderAttachmentsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureCollaborationDefaults(state, tenant);
        ensureWorkspaceCoreDefaults(state, tenant);
        let attachments = [];
        try {
          attachments = listFolderAttachments(
            state,
            ctx.tenantId,
            workspaceFolderAttachmentsMatch.folderId,
            ctx.userId,
            ctx.channel
          );
        } catch (error) {
          if (error?.statusCode !== 404) throw error;
        }
        respondJson(res, 200, { attachments });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/docs/files") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const q = base.searchParams.get("q") ?? "";
        const files = await listWorkspaceDocFiles(state, ctx.tenantId, { q });
        respondJson(res, 200, { files });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/docs/open") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const opened = openWorkspaceDocFile(state, ctx.tenantId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_doc_opened",
          details: { fileId: opened.file.id, sessionId: opened.session.id }
        });
        await persistState();
        respondJson(res, 200, opened);
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/docs/link-thread") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const file = linkWorkspaceDocToThread(state, ctx.tenantId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_doc_linked_thread",
          details: { fileId: file.id, threadId: body.threadId }
        });
        await persistState();
        respondJson(res, 200, { file });
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/tables") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const tables = listWorkspaceTables(state, ctx.tenantId);
        respondJson(res, 200, { tables });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/tables") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const table = createWorkspaceTable(state, tenant, { ...body, createdBy: ctx.userId });
        if (Array.isArray(body.rows) && body.rows.length) {
          addWorkspaceTableRows(state, tenant.id, table.id, body.rows, ctx.userId);
        }
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_table_created",
          details: { tableId: table.id }
        });
        await persistState();
        respondJson(res, 201, { table, rows: listWorkspaceTableRows(state, tenant.id, table.id) });
        return;
      }

      const workspaceTableMatch = pathMatcher(pathname, "/v1/workspace/tables/:tableId");
      if (method === "GET" && workspaceTableMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const table = requireWorkspaceTable(state, ctx.tenantId, workspaceTableMatch.tableId);
        const rows = listWorkspaceTableRows(state, ctx.tenantId, table.id);
        respondJson(res, 200, { table, rows });
        return;
      }

      if (method === "PATCH" && workspaceTableMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const table = patchWorkspaceTable(state, ctx.tenantId, workspaceTableMatch.tableId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_table_updated",
          details: { tableId: table.id }
        });
        await persistState();
        respondJson(res, 200, { table, rows: listWorkspaceTableRows(state, ctx.tenantId, table.id) });
        return;
      }

      const workspaceTableRowsMatch = pathMatcher(pathname, "/v1/workspace/tables/:tableId/rows");
      if (method === "POST" && workspaceTableRowsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const rows = addWorkspaceTableRows(
          state,
          ctx.tenantId,
          workspaceTableRowsMatch.tableId,
          Array.isArray(body.rows) ? body.rows : [body.row || {}],
          ctx.userId
        );
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "workspace_table_rows_added",
          details: { tableId: workspaceTableRowsMatch.tableId, count: rows.length }
        });
        await persistState();
        respondJson(res, 201, { rows, table: requireWorkspaceTable(state, ctx.tenantId, workspaceTableRowsMatch.tableId) });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/tables/import-live-query") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const connection = requireSourceConnection(state, tenant.id, body.connectionId);
        const result = importLiveQueryToWorkspaceTable(state, tenant, {
          tableId: body.tableId,
          tableName: body.tableName,
          createdBy: ctx.userId,
          connection,
          queryPayload: {
            query: body.query,
            timeoutMs: body.timeoutMs,
            costLimit: body.costLimit
          }
        }, { runLiveQuery });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_table_import_live_query",
          details: {
            tableId: result.table.id,
            resultId: result.resultId,
            insertedRows: result.insertedRows
          }
        });
        await persistState();
        respondJson(res, 201, result);
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/onboarding") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureTenantSettings(state, tenant);
        ensureDefaultModelProfiles(state, tenant);
        ensureDefaultReportTypes(state, tenant);
        const onboarding = buildOnboardingState(state, tenant);
        respondJson(res, 200, { onboarding });
        return;
      }

      const onboardingActionMatch = pathMatcher(pathname, "/v1/workspace/onboarding/actions/:actionId");
      if (method === "POST" && onboardingActionMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureTenantSettings(state, tenant);
        ensureDefaultModelProfiles(state, tenant);
        ensureDefaultReportTypes(state, tenant);
        ensureCollaborationDefaults(state, tenant);
        ensureWorkspaceCoreDefaults(state, tenant);

        const actionId = onboardingActionMatch.actionId;
        const result = { actionId, status: "completed" };

        if (actionId === "connect_source") {
          if (!state.sourceConnections.some((item) => item.tenantId === tenant.id)) {
            result.integration = quickAddIntegration(state, tenant, {
              integrationKey: "google_ads",
              authRef: "onboarding_seed_token",
              runInitialSync: true,
              periodDays: 21
            }, {
              createSourceConnection,
              runSourceSync,
              patchSettingsChannels,
              createMcpServer,
              testMcpServer
            });
          } else {
            result.status = "noop";
          }
        } else if (actionId === "configure_llm") {
          patchSettingsModelPreferences(state, tenant, {
            llmMode: "managed",
            defaultProvider: "managed"
          });
          result.modelPreferences = getTenantSettings(state, tenant).modelPreferences;
        } else if (actionId === "choose_profile") {
          const profiles = listModelProfiles(state, tenant.id);
          const target = profiles.find((item) => item.active) || profiles[0];
          if (target) {
            result.profile = activateModelProfile(state, tenant, target.id);
          } else {
            result.status = "noop";
          }
        } else if (actionId === "run_first_analysis") {
          let source = state.sourceConnections.find((item) => item.tenantId === tenant.id);
          if (!source) {
            quickAddIntegration(state, tenant, {
              integrationKey: "google_ads",
              authRef: "onboarding_seed_token",
              runInitialSync: true,
              periodDays: 21
            }, {
              createSourceConnection,
              runSourceSync,
              patchSettingsChannels,
              createMcpServer,
              testMcpServer
            });
            source = state.sourceConnections.find((item) => item.tenantId === tenant.id);
          }
          const profile = listModelProfiles(state, tenant.id).find((item) => item.active) || listModelProfiles(state, tenant.id)[0];
          const reportType = listReportTypes(state, tenant.id)[0];
          const folder = listWorkspaceFolders(state, tenant.id)[0];
          const thread = listWorkspaceThreads(state, tenant.id, { folderId: folder?.id })[0];
          const run = createAnalysisRun(state, tenant, {
            sourceConnectionId: source?.id ?? null,
            modelProfileId: profile?.id ?? null,
            reportTypeId: reportType?.id ?? null,
            folderId: folder?.id ?? null,
            threadId: thread?.id ?? null,
            channels: ["email"]
          });
          result.run = executeAnalysisRun(state, tenant, run, {
            requireSourceConnection,
            runSourceSync,
            requireModelProfile,
            runModelTask,
            requireReportType,
            generateReport,
            runSkillPack,
            buildMemoryContext,
            snapshotMemoryContext
          }, {
            forceSync: true,
            userId: ctx.userId
          });
        } else if (actionId === "deliver_report") {
          let completed = listAnalysisRuns(state, tenant.id)
            .filter((run) => run.status === "completed")
            .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1))[0] ?? null;
          if (!completed) {
            const source = state.sourceConnections.find((item) => item.tenantId === tenant.id);
            const profile = listModelProfiles(state, tenant.id).find((item) => item.active) || listModelProfiles(state, tenant.id)[0];
            const reportType = listReportTypes(state, tenant.id)[0];
            const run = createAnalysisRun(state, tenant, {
              sourceConnectionId: source?.id ?? null,
              modelProfileId: profile?.id ?? null,
              reportTypeId: reportType?.id ?? null,
              channels: ["email"]
            });
            completed = executeAnalysisRun(state, tenant, run, {
              requireSourceConnection,
              runSourceSync,
              requireModelProfile,
              runModelTask,
              requireReportType,
              generateReport,
              runSkillPack,
              buildMemoryContext,
              snapshotMemoryContext
            }, {
              forceSync: true,
              userId: ctx.userId
            });
          }
          const settings = getTenantSettings(state, tenant);
          const channels = [];
          if (settings.channels?.slack?.enabled) channels.push("slack");
          if (settings.channels?.telegram?.enabled) channels.push("telegram");
          if (settings.channels?.discord?.enabled) channels.push("discord");
          if (!channels.length) channels.push("email");
          result.delivery = deliverAnalysisRun(state, tenant, completed, { notifyReportDelivery }, { channels });
        } else {
          const err = new Error(`Unknown onboarding action '${actionId}'`);
          err.statusCode = 404;
          throw err;
        }

        const onboarding = buildOnboardingState(state, tenant);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_onboarding_action",
          details: { actionId, status: result.status }
        });
        await persistState();
        respondJson(res, 200, { onboarding, result });
        return;
      }

      if (method === "POST" && pathname === "/v1/workspace/attachments/upload") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const contentType = String(req.headers["content-type"] ?? "").toLowerCase();
        let attachmentPayload = {};
        if (contentType.startsWith("multipart/form-data")) {
          const multipart = await parseMultipartForm(req);
          if (!multipart.file) {
            respondJson(res, 400, { error: "multipart file is required" });
            return;
          }
          attachmentPayload = {
            filename: multipart.file.filename,
            mimeType: multipart.file.mimeType,
            size: multipart.file.size,
            data: multipart.file.data
          };
        } else {
          const body = await parseJsonBody(req);
          attachmentPayload = {
            name: body.name,
            mimeType: body.mimeType,
            size: body.size,
            contentBase64: body.contentBase64
          };
        }
        const asset = buildAttachmentAsset(state, tenant.id, ctx.userId, attachmentPayload);
        const view = attachmentAssetView(asset);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_attachment_uploaded",
          details: { attachmentId: asset.id, mimeType: asset.mimeType, size: asset.size }
        });
        await persistState();
        respondJson(res, 201, view);
        return;
      }

      const workspaceAttachmentMatch = pathMatcher(pathname, "/v1/workspace/attachments/:attachmentId");
      if (method === "GET" && workspaceAttachmentMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const asset = requireAttachmentAsset(state, ctx.tenantId, workspaceAttachmentMatch.attachmentId);
        respondJson(res, 200, attachmentAssetView(asset));
        return;
      }

      if (method === "GET" && pathname === "/v1/workspace/notifications") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const notifications = listNotifications(state, ctx.tenantId, ctx.userId);
        respondJson(res, 200, { notifications });
        return;
      }

      const workspaceNotificationReadMatch = pathMatcher(pathname, "/v1/workspace/notifications/:notificationId/read");
      if (method === "POST" && workspaceNotificationReadMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const notification = markNotificationRead(state, ctx.tenantId, ctx.userId, workspaceNotificationReadMatch.notificationId);
        emitRealtime({
          tenantId: ctx.tenantId,
          threadId: notification.threadId,
          type: "notification.read",
          audienceUserIds: [ctx.userId],
          payload: { notificationId: notification.id }
        });
        await persistState();
        respondJson(res, 200, { notification });
        return;
      }

      if (method === "GET" && pathname === "/v1/memory/projects") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const memories = listProjectMemories(state, ctx.tenantId, {
          folderId: base.searchParams.get("folderId") ?? undefined,
          threadId: base.searchParams.get("threadId") ?? undefined,
          domain: base.searchParams.get("domain") ?? undefined
        });
        respondJson(res, 200, { memories });
        return;
      }

      if (method === "POST" && pathname === "/v1/memory/projects") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const memory = createProjectMemory(state, tenant, body, ctx.userId);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "project_memory_created",
          details: { memoryId: memory.id, folderId: memory.folderId, threadId: memory.threadId }
        });
        await persistState();
        respondJson(res, 201, { memory });
        return;
      }

      const projectMemoryPatchMatch = pathMatcher(pathname, "/v1/memory/projects/:memoryId");
      if (method === "PATCH" && projectMemoryPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const memory = patchProjectMemory(state, ctx.tenantId, projectMemoryPatchMatch.memoryId, body, ctx.userId);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "project_memory_updated",
          details: { memoryId: memory.id }
        });
        await persistState();
        respondJson(res, 200, { memory });
        return;
      }

      if (method === "GET" && pathname === "/v1/memory/users") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const targetUserId = base.searchParams.get("userId") || ctx.userId;
        if (targetUserId !== ctx.userId && !["owner", "admin", "operator"].includes(ctx.role)) {
          respondJson(res, 403, { error: "Cannot read another user's memory" });
          return;
        }
        const memories = listUserMemories(state, ctx.tenantId, targetUserId, {
          folderId: base.searchParams.get("folderId") ?? undefined,
          threadId: base.searchParams.get("threadId") ?? undefined
        });
        respondJson(res, 200, { memories });
        return;
      }

      if (method === "POST" && pathname === "/v1/memory/users") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const targetUserId = body.userId && ["owner", "admin", "operator"].includes(ctx.role)
          ? body.userId
          : ctx.userId;
        const memory = createUserMemory(state, tenant, targetUserId, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "user_memory_created",
          details: { memoryId: memory.id, userId: targetUserId }
        });
        await persistState();
        respondJson(res, 201, { memory });
        return;
      }

      const userMemoryPatchMatch = pathMatcher(pathname, "/v1/memory/users/:memoryId");
      if (method === "PATCH" && userMemoryPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const targetUserId = body.userId && ["owner", "admin", "operator"].includes(ctx.role)
          ? body.userId
          : ctx.userId;
        if (targetUserId !== ctx.userId && !["owner", "admin", "operator"].includes(ctx.role)) {
          respondJson(res, 403, { error: "Cannot update another user's memory" });
          return;
        }
        const memory = patchUserMemory(state, ctx.tenantId, targetUserId, userMemoryPatchMatch.memoryId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "user_memory_updated",
          details: { memoryId: memory.id, userId: targetUserId }
        });
        await persistState();
        respondJson(res, 200, { memory });
        return;
      }

      if (method === "GET" && pathname === "/v1/memory/context") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const context = buildMemoryContext(state, ctx.tenantId, ctx.userId, {
          folderId: base.searchParams.get("folderId") ?? undefined,
          threadId: base.searchParams.get("threadId") ?? undefined,
          tags: (base.searchParams.get("tags") ?? "").split(",").map((item) => item.trim()).filter(Boolean),
          limit: Number(base.searchParams.get("limit") ?? 12)
        });
        respondJson(res, 200, { context });
        return;
      }

      if (method === "POST" && pathname === "/v1/memory/snapshots") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = snapshotMemoryContext(state, tenant, ctx.userId, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "memory_snapshot_created",
          details: { snapshotId: result.snapshot.id, contextCount: result.snapshot.contextCount }
        });
        await persistState();
        respondJson(res, 201, result);
        return;
      }

      if (method === "GET" && pathname === "/v1/memory/snapshots") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const snapshots = listMemorySnapshots(state, ctx.tenantId, ctx.userId, {
          folderId: base.searchParams.get("folderId") ?? undefined,
          threadId: base.searchParams.get("threadId") ?? undefined
        });
        respondJson(res, 200, { snapshots });
        return;
      }

      if (method === "POST" && pathname === "/v1/system/doctor") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = runDoctor(state, tenant, { applyFixes: Boolean(body.applyFixes) });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "system_doctor_run",
          details: { runId: result.run.id, status: result.run.status }
        });
        await persistState();
        respondJson(res, 200, result);
        return;
      }

      if (method === "GET" && pathname === "/v1/system/doctor/runs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        respondJson(res, 200, { runs: listDoctorRuns(state, ctx.tenantId) });
        return;
      }

      if (method === "GET" && pathname === "/v1/system/security-audits") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        respondJson(res, 200, { runs: listSecurityAuditRuns(state, ctx.tenantId) });
        return;
      }

      if (method === "GET" && pathname === "/v1/system/threat-model") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        respondJson(res, 200, { threatModel: loadThreatModel() });
        return;
      }

      const workspaceThreadCommentsMatch = pathMatcher(pathname, "/v1/workspace/threads/:threadId/comments");
      if (method === "GET" && workspaceThreadCommentsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const comments = listThreadComments(state, ctx.tenantId, workspaceThreadCommentsMatch.threadId);
        respondJson(res, 200, { comments });
        return;
      }

      if (method === "POST" && workspaceThreadCommentsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst", "viewer"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const comment = createThreadComment(state, tenant, {
          threadId: workspaceThreadCommentsMatch.threadId,
          body: body.body,
          role: body.role,
          authorId: body.authorId ?? ctx.userId,
          authorName: body.authorName ?? ctx.userId
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "workspace_thread_commented",
          details: { threadId: workspaceThreadCommentsMatch.threadId, commentId: comment.id }
        });
        await persistState();
        respondJson(res, 201, { comment });
        return;
      }

      const folderAutomationsMatch = pathMatcher(pathname, "/v1/automations/folders/:folderId");
      if (method === "GET" && folderAutomationsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        requireWorkspaceFolder(state, tenant.id, folderAutomationsMatch.folderId);
        const automations = listFolderAutomations(state, tenant.id, folderAutomationsMatch.folderId);
        respondJson(res, 200, { automations });
        return;
      }

      if (method === "POST" && folderAutomationsMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const body = await parseJsonBody(req);
        const automation = createFolderAutomation(state, tenant, folderAutomationsMatch.folderId, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "folder_automation_created",
          details: { automationId: automation.id, folderId: folderAutomationsMatch.folderId, triggerType: automation.triggerType }
        });
        await persistState();
        respondJson(res, 201, { automation });
        return;
      }

      const automationPatchMatch = pathMatcher(pathname, "/v1/automations/:automationId");
      if (method === "PATCH" && automationPatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const automation = patchFolderAutomation(state, ctx.tenantId, automationPatchMatch.automationId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "folder_automation_updated",
          details: { automationId: automation.id, enabled: automation.enabled }
        });
        await persistState();
        respondJson(res, 200, { automation });
        return;
      }

      const automationRunMatch = pathMatcher(pathname, "/v1/automations/:automationId/run");
      if (method === "POST" && automationRunMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        ensureWorkspaceCoreDefaults(state, tenant);
        const body = await parseJsonBody(req);
        const run = runFolderAutomation(state, tenant, automationRunMatch.automationId, {
          runSkillPack,
          runModelTask,
          generateReport,
          createAgentJob: (localState, localTenant, payload) => createAgentJob(localState, localTenant, payload, {
            requireWorkspaceFolder,
            requireWorkspaceThread,
            createThreadComment,
            runSkillPack,
            runModelTask,
            generateReport
          }),
          createDeviceCommandRequest: (localState, localTenant, payload) => createDeviceCommandRequest(localState, localTenant, payload, {
            requireWorkspaceFolder,
            requireWorkspaceThread
          })
        }, {
          actionType: body.actionType,
          actionPayload: body.actionPayload,
          targetThreadId: body.targetThreadId
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "folder_automation_run",
          details: { automationId: automationRunMatch.automationId, runId: run.id, status: run.status }
        });
        await persistState();
        respondJson(res, 200, { run });
        return;
      }

      const automationHeartbeatMatch = pathMatcher(pathname, "/v1/automations/:automationId/heartbeat");
      if (method === "GET" && automationHeartbeatMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const automation = listFolderAutomations(state, ctx.tenantId).find((item) => item.id === automationHeartbeatMatch.automationId);
        if (!automation) {
          respondJson(res, 404, { error: `Folder automation '${automationHeartbeatMatch.automationId}' not found` });
          return;
        }
        const content = String(automation.heartbeatContent || "");
        const validated = validateHeartbeatContent(content);
        respondJson(res, 200, {
          heartbeat: {
            automationId: automation.id,
            tenantId: ctx.tenantId,
            content,
            parsed: validated.config,
            valid: validated.ok,
            errors: validated.errors,
            updatedAt: automation.updatedAt
          }
        });
        return;
      }

      if (method === "PATCH" && automationHeartbeatMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const content = String(body.content ?? "");
        const validated = validateHeartbeatContent(content);
        const automation = patchFolderAutomation(state, ctx.tenantId, automationHeartbeatMatch.automationId, {
          triggerType: "heartbeat",
          heartbeatContent: content,
          heartbeatPath: body.heartbeatPath,
          targetThreadId: body.targetThreadId
        });
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "automation_heartbeat_updated",
          details: { automationId: automation.id, valid: validated.ok, errors: validated.errors.length }
        });
        await persistState();
        respondJson(res, 200, {
          heartbeat: {
            automationId: automation.id,
            tenantId: ctx.tenantId,
            content,
            parsed: validated.config,
            valid: validated.ok,
            errors: validated.errors,
            updatedAt: automation.updatedAt
          }
        });
        return;
      }

      const automationHeartbeatExportMatch = pathMatcher(pathname, "/v1/automations/:automationId/heartbeat/export");
      if (method === "POST" && automationHeartbeatExportMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const automation = listFolderAutomations(state, ctx.tenantId).find((item) => item.id === automationHeartbeatExportMatch.automationId);
        if (!automation) {
          respondJson(res, 404, { error: `Folder automation '${automationHeartbeatExportMatch.automationId}' not found` });
          return;
        }
        const content = String(body.content ?? automation.heartbeatContent ?? "");
        const validated = validateHeartbeatContent(content);
        if (!validated.ok) {
          respondJson(res, 400, { error: "Heartbeat content is invalid", errors: validated.errors });
          return;
        }
        const runtimeDir = path.join(process.cwd(), ".runtime", "heartbeats", ctx.tenantId, automation.folderId);
        fs.mkdirSync(runtimeDir, { recursive: true });
        const exportPath = path.join(runtimeDir, "heartbeat.md");
        fs.writeFileSync(exportPath, content, "utf8");
        patchFolderAutomation(state, ctx.tenantId, automation.id, {
          triggerType: "heartbeat",
          heartbeatContent: content,
          heartbeatPath: exportPath
        });
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "automation_heartbeat_exported",
          details: { automationId: automation.id, exportPath }
        });
        await persistState();
        respondJson(res, 200, {
          heartbeat: {
            automationId: automation.id,
            tenantId: ctx.tenantId,
            valid: true,
            exportPath
          }
        });
        return;
      }

      if (method === "GET" && pathname === "/v1/automations/runs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const runs = listAutomationRuns(state, ctx.tenantId);
        respondJson(res, 200, { runs });
        return;
      }

      if (method === "POST" && pathname === "/v1/automations/heartbeat/validate") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = validateHeartbeatContent(body.content ?? "");
        respondJson(res, 200, result);
        return;
      }

      if (method === "POST" && pathname === "/v1/automations/heartbeat/parse") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const result = parseHeartbeatContent(body.content ?? "");
        respondJson(res, 200, result);
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

      if (method === "GET" && pathname === "/v1/reports/templates") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const reportTypeId = base.searchParams.get("reportTypeId") ?? undefined;
        const templates = listReportTemplates(state, ctx.tenantId, { reportTypeId });
        respondJson(res, 200, { templates });
        return;
      }

      if (method === "POST" && pathname === "/v1/reports/templates/upload") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);

        let body = {};
        const contentType = String(req.headers["content-type"] ?? "");
        if (contentType.includes("multipart/form-data")) {
          const multipart = await parseMultipartForm(req);
          if (!multipart.file) {
            respondJson(res, 400, { error: "multipart file is required" });
            return;
          }
          const fileBody = Buffer.from(multipart.file.data).toString("utf8");
          body = {
            name: multipart.fields.name || multipart.file.filename.replace(/\\.md$/i, ""),
            body: fileBody,
            reportTypeId: multipart.fields.reportTypeId || null,
            objective: multipart.fields.objective || null,
            domain: multipart.fields.domain || null,
            active: String(multipart.fields.active || "").toLowerCase() === "true"
          };
        } else {
          body = await parseJsonBody(req);
        }

        const template = uploadReportTemplate(state, tenant, body);
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "report_template_uploaded",
          details: { templateId: template.id, version: template.version, reportTypeId: template.reportTypeId }
        });
        await persistState();
        respondJson(res, 201, { template });
        return;
      }

      const reportTemplatePatchMatch = pathMatcher(pathname, "/v1/reports/templates/:templateId");
      if (method === "PATCH" && reportTemplatePatchMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const template = patchReportTemplate(state, ctx.tenantId, reportTemplatePatchMatch.templateId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "report_template_updated",
          details: { templateId: template.id, version: template.version }
        });
        await persistState();
        respondJson(res, 200, { template });
        return;
      }

      const reportTemplateActivateMatch = pathMatcher(pathname, "/v1/reports/templates/:templateId/activate");
      if (method === "POST" && reportTemplateActivateMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        requireTenant(state, ctx.tenantId);
        const template = activateReportTemplate(state, ctx.tenantId, reportTemplateActivateMatch.templateId);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "report_template_activated",
          details: { templateId: template.id, reportTypeId: template.reportTypeId }
        });
        await persistState();
        respondJson(res, 200, { template });
        return;
      }

      const reportTemplatePreviewMatch = pathMatcher(pathname, "/v1/reports/templates/:templateId/preview");
      if (method === "POST" && reportTemplatePreviewMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const preview = previewReportTemplate(state, ctx.tenantId, reportTemplatePreviewMatch.templateId, body);
        respondJson(res, 200, preview);
        return;
      }

      if (method === "POST" && pathname === "/v1/skills/drafts/generate") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const answers = body.answers && typeof body.answers === "object" ? body.answers : {};
        const settings = ensureTenantSettings(state, tenant);
        const resolved = resolveSkillGenerationCredential(
          state,
          tenant.id,
          settings,
          body.provider,
          body.apiKey
        );

        const warnings = [];
        let generationMode = "fallback";
        let providerUsed = normalizeProviderName(resolved.provider || body.provider || settings.modelPreferences?.defaultProvider || "managed");
        let modelUsed = null;
        let manifest = buildSkillManifestFromAnswers(answers);
        let skillMd = buildSkillMarkdown(manifest);

        if (resolved.apiKey && providerUsed !== "managed") {
          try {
            const generated = await generateSkillArtifactsWithLlm({
              provider: providerUsed,
              apiKey: resolved.apiKey,
              answers,
              workspaceAgentName: getWorkspaceAgentProfile(state, tenant.id)?.name || "Titus"
            });
            generationMode = "llm";
            providerUsed = generated.provider;
            modelUsed = generated.model;
            manifest = generated.manifest;
            skillMd = generated.skillMarkdown;
          } catch (error) {
            warnings.push(error.message || "llm_generation_failed");
          }
        } else {
          warnings.push("no_valid_api_key_configured_fallback_used");
        }

        const draft = createSkillDraft(state, tenant, { manifest });
        const validation = validateSkillDraft(state, tenant.id, draft.draftId);
        if (validation.errors.length) {
          warnings.push("generated_manifest_invalid_using_safe_defaults");
          draft.manifest = buildSkillManifestFromAnswers(answers);
          skillMd = buildSkillMarkdown(draft.manifest);
          validateSkillDraft(state, tenant.id, draft.draftId);
          generationMode = "fallback";
          modelUsed = null;
        }

        draft.generation = {
          mode: generationMode,
          provider: providerUsed,
          model: modelUsed,
          source: resolved.source,
          generatedAt: new Date().toISOString(),
          warnings
        };

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "skill_draft_generated",
          details: {
            draftId: draft.draftId,
            mode: draft.generation.mode,
            provider: draft.generation.provider
          }
        });
        await persistState();
        respondJson(res, 201, {
          draft,
          result: {
            draftId: draft.draftId,
            status: draft.status,
            errors: draft.validationErrors
          },
          generation: draft.generation,
          skillMd
        });
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
        const run = createAnalysisRun(state, tenant, {
          ...body,
          createdBy: ctx.userId
        });
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
          runSkillPack,
          buildMemoryContext,
          snapshotMemoryContext
        }, {
          ...body,
          userId: ctx.userId
        });
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

      if (method === "GET" && pathname === "/v1/models/providers/health") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const health = [...state.modelProviderHealth.entries()]
          .map(([key, value]) => ({ key, ...value }))
          .filter((item) => item.tenantId === ctx.tenantId)
          .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
        respondJson(res, 200, { health });
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/catalog") {
        ensureSkillRegistry(state);
        respondJson(res, 200, { skills: listSkillCatalog() });
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/registry") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const source = base.searchParams.get("source") ?? undefined;
        const skills = listSkillRegistry(state, { source })
          .filter((entry) => !entry.tenantScope || entry.tenantScope === ctx.tenantId);
        respondJson(res, 200, { skills });
        return;
      }

      if (method === "POST" && pathname === "/v1/skills/registry") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const entry = registerSkillInRegistry(state, ctx.tenantId, body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "skill_registry_entry_created",
          details: {
            registryId: entry.registryId,
            source: entry.source,
            riskLevel: entry.riskLevel
          }
        });
        await persistState();
        respondJson(res, 201, { entry });
        return;
      }

      if (method === "GET" && pathname === "/v1/skills/tools") {
        respondJson(res, 200, { tools: listSkillTools() });
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

      const patchInstalledSkillMatch = pathMatcher(pathname, "/v1/skills/installed/:skillId");
      if (method === "PATCH" && patchInstalledSkillMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const skill = patchInstalledSkillPack(state, ctx.tenantId, decodeURIComponent(patchInstalledSkillMatch.skillId), body);
        pushAudit(state, {
          tenantId: ctx.tenantId,
          actorId: ctx.userId,
          action: "skill_manifest_updated",
          details: {
            skillId: skill.id,
            active: skill.active
          }
        });
        await persistState();
        respondJson(res, 200, { skill });
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
        const resolvedTemplate = body.templateId
          ? requireReportTemplate(state, tenant.id, body.templateId)
          : resolveActiveTemplate(state, tenant.id, {
              reportTypeId: body.reportTypeId,
              objective: body.objective,
              domain: body.domain
            });
        const templateBody = compileReportTemplateBody(resolvedTemplate, {
          reportTitle: body.title ?? `${tenant.name} Executive Report`,
          reportSummary: body.summary ?? "",
          channel: "email",
          runId: body.runId ?? "",
          confidence: body.confidence ?? ""
        });
        const result = generateReport(state, tenant, body);
        if (resolvedTemplate) {
          result.report.templateId = resolvedTemplate.id;
          result.report.templateVersion = resolvedTemplate.version;
          if (templateBody) {
            result.report.body = templateBody;
            result.report.summary = result.report.summary || `Rendered from template ${resolvedTemplate.name}`;
          }
        }

        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "report_generated",
          details: {
            reportId: result.report.id,
            channels: body.channels ?? ["email"],
            templateId: resolvedTemplate?.id ?? null
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
        const job = createAgentJob(state, tenant, body, {
          requireWorkspaceFolder,
          requireWorkspaceThread,
          createThreadComment,
          runSkillPack,
          runModelTask,
          generateReport
        });

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

      if (method === "GET" && pathname === "/v1/agents/jobs") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const jobs = listAgentJobs(state, ctx.tenantId, {
          folderId: base.searchParams.get("folderId") ?? undefined,
          threadId: base.searchParams.get("threadId") ?? undefined
        });
        respondJson(res, 200, { jobs });
        return;
      }

      if (method === "POST" && pathname === "/v1/agents/device-commands") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator", "analyst"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const request = createDeviceCommandRequest(state, tenant, {
          ...body,
          requestedBy: ctx.userId
        }, {
          requireWorkspaceFolder,
          requireWorkspaceThread
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "device_command_requested",
          details: {
            requestId: request.id,
            command: request.command,
            status: request.status
          }
        });
        await persistState();
        respondJson(res, 201, { request });
        return;
      }

      if (method === "GET" && pathname === "/v1/agents/device-commands") {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireTenant(state, ctx.tenantId);
        const requests = listDeviceCommandRequests(state, ctx.tenantId, {
          status: base.searchParams.get("status") ?? undefined
        });
        respondJson(res, 200, { requests });
        return;
      }

      const deviceCommandApproveMatch = pathMatcher(pathname, "/v1/agents/device-commands/:requestId/approve");
      if (method === "POST" && deviceCommandApproveMatch) {
        const ctx = authContextFromHeaders(req.headers);
        requireTenantHeader(ctx);
        requireRole(ctx, ["owner", "admin", "operator"]);
        const tenant = requireTenant(state, ctx.tenantId);
        const body = await parseJsonBody(req);
        const request = approveDeviceCommandRequest(state, tenant, {
          requestId: deviceCommandApproveMatch.requestId,
          decision: body.decision,
          reason: body.reason,
          executeNow: body.executeNow,
          reviewedBy: ctx.userId
        }, {
          createThreadComment
        });
        pushAudit(state, {
          tenantId: tenant.id,
          actorId: ctx.userId,
          action: "device_command_reviewed",
          details: {
            requestId: request.id,
            status: request.status,
            decision: body.decision ?? "approve"
          }
        });
        await persistState();
        respondJson(res, 200, { request });
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
        requestId,
        checks: error.checks,
        details: error.details
      });
    }
  });

  server.on("upgrade", (req, socket, head) => {
    const enabled = state.featureFlags?.realtime_ws_enabled !== false;
    if (!enabled) {
      socket.write("HTTP/1.1 503 Service Unavailable\r\n\r\n");
      socket.destroy();
      return;
    }
    const handled = handleRealtimeUpgrade(req, socket, head, realtimeHub);
    if (!handled) {
      socket.write("HTTP/1.1 404 Not Found\r\n\r\n");
      socket.destroy();
    }
  });

  return {
    state,
    server,
    demoTenant,
    close: () => {
      stopScheduler();
      if (automationTimer) clearInterval(automationTimer);
      if (doctorTimer) clearInterval(doctorTimer);
      for (const client of realtimeHub.clients.values()) {
        try {
          client.socket.end();
        } catch {}
      }
      realtimeHub.clients.clear();
    }
  };
}
