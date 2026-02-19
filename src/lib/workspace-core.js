import fs from "node:fs";
import path from "node:path";
import { newId } from "./state.js";

const TEAM_COLOR_PALETTE = [
  "#0f766e",
  "#0b57d0",
  "#a61e4d",
  "#7c2d12",
  "#3f6212",
  "#5b21b6",
  "#155e75",
  "#831843",
  "#1f2937",
  "#b45309"
];

const HEARTBEAT_SUPPORTED_OPERANDS = new Set([
  "source_freshness_hours",
  "quality_score",
  "unread_mentions_count",
  "pending_approvals_count"
]);

function nowIso() {
  return new Date().toISOString();
}

function requireWorkspaceFolder(state, tenantId, folderId) {
  const folder = state.workspaceFolders.find((item) => item.tenantId === tenantId && item.id === folderId);
  if (!folder) {
    const err = new Error(`Workspace folder '${folderId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return folder;
}

function requireWorkspaceThread(state, tenantId, threadId) {
  const thread = state.workspaceThreads.find((item) => item.tenantId === tenantId && item.id === threadId);
  if (!thread) {
    const err = new Error(`Workspace thread '${threadId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return thread;
}

function requireChatMessage(state, tenantId, messageId) {
  const message = state.chatMessages.find((item) => item.tenantId === tenantId && item.id === messageId);
  if (!message) {
    const err = new Error(`Chat message '${messageId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return message;
}

function compareByCreatedAtAsc(a, b) {
  if (a.createdAt === b.createdAt) return a.id < b.id ? -1 : 1;
  return a.createdAt < b.createdAt ? -1 : 1;
}

function normalizeVisibility(value) {
  return value === "private" ? "private" : "shared";
}

function normalizeAttachments(input) {
  if (!Array.isArray(input)) return [];
  return input
    .slice(0, 20)
    .map((item, idx) => {
      const id = String(item?.id ?? `attachment_${idx + 1}`);
      const name = String(item?.name ?? "file");
      return {
        id,
        name: name.slice(0, 240),
        type: String(item?.type ?? ""),
        size: Number(item?.size ?? 0)
      };
    });
}

function projectMessageForViewer(message, viewerUserId) {
  if (message.visibility !== "private") return { ...message };
  const canSeePrivate = viewerUserId && (viewerUserId === message.privateRecipientUserId || viewerUserId === message.authorId);
  if (canSeePrivate) return { ...message };
  return {
    ...message,
    body: "[Private AI exchange occurred]",
    attachments: [],
    privateHidden: true
  };
}

function normalizeMemberAlias(input = "") {
  return input.toLowerCase().replace(/[^a-z0-9]+/g, "");
}

function parseMentionTokens(body = "") {
  return [...String(body).matchAll(/@([a-zA-Z0-9._-]{2,64})/g)].map((match) => normalizeMemberAlias(match[1]));
}

function resolveMentionMemberIds(state, tenantId, body) {
  const tokens = new Set(parseMentionTokens(body));
  if (!tokens.size) return [];
  return state.teamMembers
    .filter((member) => member.tenantId === tenantId)
    .filter((member) => {
      const aliases = [
        member.id,
        member.name,
        member.email?.split("@")?.[0] ?? ""
      ].map((entry) => normalizeMemberAlias(entry));
      return aliases.some((alias) => tokens.has(alias));
    })
    .map((member) => member.id);
}

function createNotification(state, payload = {}) {
  const record = {
    id: newId("notification"),
    tenantId: payload.tenantId,
    userId: payload.userId,
    kind: payload.kind ?? "mention",
    folderId: payload.folderId ?? "",
    threadId: payload.threadId ?? "",
    messageId: payload.messageId ?? null,
    title: payload.title ?? "New workspace event",
    read: false,
    createdAt: nowIso()
  };
  state.notifications.push(record);
  return record;
}

function findTeamMember(state, tenantId, memberId) {
  return state.teamMembers.find((item) => item.tenantId === tenantId && item.id === memberId) ?? null;
}

function normalizeMessageAuthor(state, tenantId, payload = {}, actor = {}) {
  const authorType = payload.authorType === "agent" ? "agent" : "user";
  if (authorType === "agent") {
    return {
      authorType,
      authorId: payload.authorId ?? "workspace_agent",
      authorName: payload.authorName ?? "InsightFoundry Agent"
    };
  }
  const actorId = payload.authorId ?? actor.userId ?? "user";
  const member = findTeamMember(state, tenantId, actorId);
  return {
    authorType,
    authorId: actorId,
    authorName: payload.authorName ?? member?.name ?? actorId
  };
}

function ensureThreadFolderMatch(thread, folderId) {
  if (folderId && thread.folderId !== folderId) {
    const err = new Error(`Thread '${thread.id}' does not belong to folder '${folderId}'`);
    err.statusCode = 400;
    throw err;
  }
}

function assertMessagePermissions(visibility, payload = {}, actor = {}) {
  if (visibility === "private" && !payload.privateRecipientUserId && !actor.userId) {
    const err = new Error("privateRecipientUserId is required for private messages");
    err.statusCode = 400;
    throw err;
  }
}

function listTenantTeamMemberIds(state, tenantId) {
  return state.teamMembers
    .filter((item) => item.tenantId === tenantId && item.status !== "inactive")
    .map((item) => item.id);
}

function ensureThreadTimestamps(thread, createdAt) {
  thread.lastMessageAt = createdAt;
  thread.updatedAt = createdAt;
}

function touchParentThreadState(state, message) {
  if (!message.parentMessageId) return;
  const parent = state.chatMessages.find((item) => item.tenantId === message.tenantId && item.id === message.parentMessageId);
  if (!parent) return;
  parent.replyCount = Number(parent.replyCount ?? 0) + 1;
  parent.hasMiniThread = parent.replyCount > 1;
  parent.updatedAt = nowIso();
}

function baseAgentResponse(sourceMessage, agentProfile, promptOverride) {
  if (promptOverride) return String(promptOverride);
  const trimmed = String(sourceMessage.body ?? "").trim();
  if (!trimmed) {
    return `${agentProfile.name}: I can help break this down into a run plan and recommendations.`;
  }
  if (/forecast|projection|horizon/i.test(trimmed)) {
    return `${agentProfile.name}: Forecast plan drafted. I can run a horizon model, summarize confidence, and list approval-gated actions.`;
  }
  if (/anomaly|risk|variance/i.test(trimmed)) {
    return `${agentProfile.name}: Anomaly workflow drafted. I can run checks, score severity, and post evidence-backed actions.`;
  }
  return `${agentProfile.name}: Received. I can turn this into a structured analysis run and report delivery path.`;
}

function toNumber(value, fallback = 0) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function parseScalar(raw) {
  const value = String(raw).trim();
  if (value === "true") return true;
  if (value === "false") return false;
  if (value === "null") return null;
  if (/^-?\d+(\.\d+)?$/.test(value)) return Number(value);
  if ((value.startsWith("\"") && value.endsWith("\"")) || (value.startsWith("'") && value.endsWith("'"))) {
    return value.slice(1, -1);
  }
  return value;
}

function parseSimpleYaml(frontmatter) {
  const lines = String(frontmatter).split(/\r?\n/);
  const result = {};
  const errors = [];
  let inTriggers = false;
  let currentTrigger = null;
  let inPayload = false;
  let payloadIndent = 0;

  lines.forEach((line, index) => {
    const lineNo = index + 1;
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#")) return;
    const indent = line.length - line.trimStart().length;

    if (!inTriggers && indent === 0) {
      const match = trimmed.match(/^([a-zA-Z0-9_]+):\s*(.*)$/);
      if (!match) {
        errors.push({ line: lineNo, message: "Invalid root key syntax" });
        return;
      }
      const [, key, rawValue] = match;
      if (key === "triggers") {
        result.triggers = [];
        inTriggers = true;
        currentTrigger = null;
        inPayload = false;
        return;
      }
      if (!rawValue.length) {
        errors.push({ line: lineNo, message: `Missing value for key '${key}'` });
        return;
      }
      result[key] = parseScalar(rawValue);
      return;
    }

    if (!inTriggers) {
      errors.push({ line: lineNo, message: "Unexpected indentation before triggers section" });
      return;
    }

    if (trimmed.startsWith("- ")) {
      currentTrigger = {};
      result.triggers.push(currentTrigger);
      inPayload = false;
      const remainder = trimmed.slice(2).trim();
      if (remainder) {
        const inline = remainder.match(/^([a-zA-Z0-9_]+):\s*(.*)$/);
        if (!inline) {
          errors.push({ line: lineNo, message: "Invalid trigger item syntax" });
          return;
        }
        currentTrigger[inline[1]] = parseScalar(inline[2]);
      }
      return;
    }

    if (!currentTrigger) {
      errors.push({ line: lineNo, message: "Trigger field must follow '-'" });
      return;
    }

    const field = trimmed.match(/^([a-zA-Z0-9_]+):\s*(.*)$/);
    if (!field) {
      errors.push({ line: lineNo, message: "Invalid trigger field syntax" });
      return;
    }

    const [, key, rawValue] = field;
    if (key === "payload") {
      currentTrigger.payload = {};
      inPayload = true;
      payloadIndent = indent;
      return;
    }

    if (inPayload && indent > payloadIndent) {
      currentTrigger.payload[key] = parseScalar(rawValue);
      return;
    }

    inPayload = false;
    currentTrigger[key] = parseScalar(rawValue);
  });

  return { parsed: result, errors };
}

function parseHeartbeatContentInternal(rawContent = "") {
  const content = String(rawContent);
  const blockMatch = content.match(/^---\s*\n([\s\S]*?)\n---\s*(?:\n([\s\S]*))?$/);
  if (!blockMatch) {
    return {
      config: null,
      notes: "",
      errors: [{ line: 1, message: "Expected YAML frontmatter wrapped by --- markers" }]
    };
  }
  const frontmatter = blockMatch[1];
  const notes = blockMatch[2] ?? "";
  const yaml = parseSimpleYaml(frontmatter);
  if (yaml.errors.length) {
    return {
      config: null,
      notes,
      errors: yaml.errors
    };
  }
  return {
    config: {
      enabled: Boolean(yaml.parsed.enabled ?? true),
      intervalMinutes: Math.max(1, toNumber(yaml.parsed.intervalMinutes, 5)),
      targetThreadId: String(yaml.parsed.targetThreadId ?? ""),
      triggers: Array.isArray(yaml.parsed.triggers) ? yaml.parsed.triggers.map((trigger, idx) => ({
        id: String(trigger.id ?? `trigger_${idx + 1}`),
        when: String(trigger.when ?? ""),
        action: String(trigger.action ?? "run_agent_job"),
        payload: typeof trigger.payload === "object" && trigger.payload ? trigger.payload : {},
        cooldownMinutes: trigger.cooldownMinutes != null ? Math.max(0, toNumber(trigger.cooldownMinutes, 0)) : undefined
      })) : []
    },
    notes,
    errors: []
  };
}

function validateHeartbeatDsl(whenExpr) {
  const expr = String(whenExpr ?? "").trim();
  const match = expr.match(/^([a-z_]+)\(([^)]*)\)\s*(==|!=|>=|<=|>|<)\s*(-?\d+(?:\.\d+)?)$/i);
  if (!match) {
    return {
      ok: false,
      message: "Expected expression like source_freshness_hours(connection_id) > 24"
    };
  }
  const operand = match[1];
  if (!HEARTBEAT_SUPPORTED_OPERANDS.has(operand)) {
    return {
      ok: false,
      message: `Unsupported operand '${operand}'`
    };
  }
  return {
    ok: true,
    operand: match[1],
    arg: String(match[2] ?? "").trim(),
    operator: match[3],
    threshold: Number(match[4])
  };
}

function readHeartbeatContent(automation) {
  if (automation.heartbeatContent) return String(automation.heartbeatContent);
  if (!automation.heartbeatPath) return "";
  const absolutePath = path.isAbsolute(automation.heartbeatPath)
    ? automation.heartbeatPath
    : path.join(process.cwd(), automation.heartbeatPath);
  if (!fs.existsSync(absolutePath)) return "";
  return fs.readFileSync(absolutePath, "utf8");
}

function findLatestSourceRun(state, tenantId, identifier) {
  const key = String(identifier ?? "").trim();
  const runs = state.sourceConnectionRuns
    .filter((item) => item.tenantId === tenantId)
    .filter((run) => {
      if (!key) return true;
      if (run.connectionId === key) return true;
      const connection = state.sourceConnections.find((conn) => conn.tenantId === tenantId && conn.id === run.connectionId);
      return connection?.sourceType === key;
    })
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
  return runs[0] ?? null;
}

function evaluateOperandValue(state, tenantId, automation, parsedDsl) {
  const arg = parsedDsl.arg;
  if (parsedDsl.operand === "source_freshness_hours") {
    const latest = findLatestSourceRun(state, tenantId, arg);
    if (!latest) return Number.POSITIVE_INFINITY;
    const ageMs = Date.now() - Date.parse(latest.createdAt);
    return Math.max(0, ageMs / 3_600_000);
  }
  if (parsedDsl.operand === "quality_score") {
    const latest = findLatestSourceRun(state, tenantId, arg);
    if (!latest) return 0;
    return toNumber(latest.diagnostics?.qualityScore, 0);
  }
  if (parsedDsl.operand === "unread_mentions_count") {
    const targetThreadId = automation.targetThreadId ?? null;
    const unread = state.notifications.filter((item) => item.tenantId === tenantId && item.kind === "mention" && !item.read);
    if (!targetThreadId) return unread.length;
    return unread.filter((item) => item.threadId === targetThreadId).length;
  }
  if (parsedDsl.operand === "pending_approvals_count") {
    const pendingDevice = state.deviceCommandRequests.filter((item) => item.tenantId === tenantId && item.status === "pending_approval").length;
    const pendingActions = state.insights
      .filter((insight) => insight.tenantId === tenantId)
      .flatMap((insight) => insight.recommendedActions || [])
      .filter((action) => action.executionState === "pending").length;
    return pendingDevice + pendingActions;
  }
  return 0;
}

function compareDslValue(value, operator, threshold) {
  if (operator === ">") return value > threshold;
  if (operator === ">=") return value >= threshold;
  if (operator === "<") return value < threshold;
  if (operator === "<=") return value <= threshold;
  if (operator === "==") return value === threshold;
  if (operator === "!=") return value !== threshold;
  return false;
}

function heartbeatStateKey(automationId, triggerId) {
  return `${automationId}:${triggerId}`;
}

function getHeartbeatStateEntry(state, tenantId, automationId, triggerId) {
  const key = heartbeatStateKey(automationId, triggerId);
  return state.heartbeatState.find((item) => item.tenantId === tenantId && item.key === key) ?? null;
}

function setHeartbeatStateEntry(state, tenantId, automationId, triggerId, patch = {}) {
  const key = heartbeatStateKey(automationId, triggerId);
  let entry = state.heartbeatState.find((item) => item.tenantId === tenantId && item.key === key);
  if (!entry) {
    entry = {
      id: newId("heartbeat_state"),
      key,
      tenantId,
      automationId,
      triggerId,
      lastTriggeredAt: null,
      updatedAt: nowIso()
    };
    state.heartbeatState.push(entry);
  }
  Object.assign(entry, patch, { updatedAt: nowIso() });
  return entry;
}

export function deterministicTeamColor(memberId) {
  const input = String(memberId ?? "");
  let hash = 0;
  for (let i = 0; i < input.length; i += 1) {
    hash = (hash * 31 + input.charCodeAt(i)) >>> 0;
  }
  return TEAM_COLOR_PALETTE[hash % TEAM_COLOR_PALETTE.length];
}

export function ensureWorkspaceAgentProfile(state, tenant) {
  const existing = state.workspaceAgentProfiles.find((item) => item.tenantId === tenant.id);
  if (existing) return existing;
  const profile = {
    tenantId: tenant.id,
    name: "InsightFoundry Agent",
    avatarStyle: "orb",
    tonePreset: "operator",
    defaultModelProfileId: null,
    updatedAt: nowIso()
  };
  state.workspaceAgentProfiles.push(profile);
  return profile;
}

export function getWorkspaceAgentProfile(state, tenantId) {
  const profile = state.workspaceAgentProfiles.find((item) => item.tenantId === tenantId);
  if (!profile) {
    const err = new Error("Workspace agent profile not found");
    err.statusCode = 404;
    throw err;
  }
  return profile;
}

export function patchWorkspaceAgentProfile(state, tenantId, payload = {}) {
  const profile = getWorkspaceAgentProfile(state, tenantId);
  if (payload.name != null) profile.name = String(payload.name);
  if (payload.avatarStyle != null) profile.avatarStyle = String(payload.avatarStyle);
  if (payload.tonePreset != null) profile.tonePreset = String(payload.tonePreset);
  if (payload.defaultModelProfileId !== undefined) profile.defaultModelProfileId = payload.defaultModelProfileId;
  profile.updatedAt = nowIso();
  return profile;
}

export function getTeamMemberAppearance(state, tenantId, memberId) {
  const existing = state.teamMemberAppearance.find((item) => item.tenantId === tenantId && item.memberId === memberId);
  if (existing) {
    if (existing.colorMode === "manual" && existing.colorHex) return existing;
    return {
      ...existing,
      colorHex: deterministicTeamColor(memberId)
    };
  }
  return {
    memberId,
    tenantId,
    colorMode: "deterministic",
    colorHex: deterministicTeamColor(memberId),
    updatedAt: nowIso()
  };
}

export function listTeamMemberAppearance(state, tenantId) {
  return state.teamMembers
    .filter((member) => member.tenantId === tenantId)
    .map((member) => getTeamMemberAppearance(state, tenantId, member.id));
}

export function patchTeamMemberAppearance(state, tenantId, memberId, payload = {}) {
  const member = findTeamMember(state, tenantId, memberId);
  if (!member) {
    const err = new Error(`Team member '${memberId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  let record = state.teamMemberAppearance.find((item) => item.tenantId === tenantId && item.memberId === memberId);
  if (!record) {
    record = {
      tenantId,
      memberId,
      colorMode: "deterministic",
      colorHex: null,
      updatedAt: nowIso()
    };
    state.teamMemberAppearance.push(record);
  }
  if (payload.colorMode != null) {
    record.colorMode = payload.colorMode === "manual" ? "manual" : "deterministic";
  }
  if (record.colorMode === "manual" && payload.colorHex != null) {
    record.colorHex = String(payload.colorHex);
  }
  if (record.colorMode === "deterministic") {
    record.colorHex = null;
  }
  record.updatedAt = nowIso();
  return getTeamMemberAppearance(state, tenantId, memberId);
}

export function createChatMessage(state, tenant, payload = {}, actor = {}) {
  const threadId = String(payload.threadId ?? "");
  if (!threadId) {
    const err = new Error("threadId is required");
    err.statusCode = 400;
    throw err;
  }
  const thread = requireWorkspaceThread(state, tenant.id, threadId);
  ensureThreadFolderMatch(thread, payload.folderId ?? null);
  requireWorkspaceFolder(state, tenant.id, thread.folderId);

  const body = String(payload.body ?? "").trim();
  const attachments = normalizeAttachments(payload.attachments);
  if (!body && attachments.length === 0) {
    const err = new Error("body or attachments is required");
    err.statusCode = 400;
    throw err;
  }

  const visibility = normalizeVisibility(payload.visibility);
  assertMessagePermissions(visibility, payload, actor);
  const parentMessageId = payload.parentMessageId ? String(payload.parentMessageId) : null;
  if (parentMessageId) {
    const parent = requireChatMessage(state, tenant.id, parentMessageId);
    if (parent.threadId !== thread.id) {
      const err = new Error(`Parent message '${parentMessageId}' is not in thread '${thread.id}'`);
      err.statusCode = 400;
      throw err;
    }
  }

  const author = normalizeMessageAuthor(state, tenant.id, payload, actor);
  const createdAt = nowIso();
  const message = {
    id: newId("chat_msg"),
    tenantId: tenant.id,
    folderId: thread.folderId,
    threadId: thread.id,
    parentMessageId,
    authorType: author.authorType,
    authorId: author.authorId,
    authorName: author.authorName,
    visibility,
    privateRecipientUserId: visibility === "private"
      ? String(payload.privateRecipientUserId ?? actor.userId ?? author.authorId)
      : null,
    body,
    attachments,
    hasMiniThread: false,
    replyCount: 0,
    isPrivateMarker: Boolean(payload.isPrivateMarker),
    createdAt,
    updatedAt: createdAt
  };
  state.chatMessages.push(message);
  touchParentThreadState(state, message);
  ensureThreadTimestamps(thread, createdAt);

  const mentionTargets = visibility === "shared" && author.authorType === "user"
    ? resolveMentionMemberIds(state, tenant.id, body).filter((memberId) => memberId !== author.authorId)
    : [];
  mentionTargets.forEach((memberId) => {
    createNotification(state, {
      tenantId: tenant.id,
      userId: memberId,
      kind: "mention",
      folderId: thread.folderId,
      threadId: thread.id,
      messageId: message.id,
      title: `${author.authorName} mentioned you in ${thread.title}`
    });
  });

  return message;
}

export function listThreadMessages(state, tenantId, threadId, viewerUserId) {
  requireWorkspaceThread(state, tenantId, threadId);
  return state.chatMessages
    .filter((item) => item.tenantId === tenantId && item.threadId === threadId && item.parentMessageId === null)
    .sort(compareByCreatedAtAsc)
    .map((message) => projectMessageForViewer(message, viewerUserId));
}

export function listMessageReplies(state, tenantId, threadId, parentMessageId, viewerUserId) {
  requireWorkspaceThread(state, tenantId, threadId);
  const parent = requireChatMessage(state, tenantId, parentMessageId);
  if (parent.threadId !== threadId) {
    const err = new Error(`Parent message '${parentMessageId}' is not in thread '${threadId}'`);
    err.statusCode = 400;
    throw err;
  }
  return state.chatMessages
    .filter((item) => item.tenantId === tenantId && item.threadId === threadId && item.parentMessageId === parentMessageId)
    .sort(compareByCreatedAtAsc)
    .map((reply) => projectMessageForViewer(reply, viewerUserId));
}

export function getMiniThread(state, tenantId, threadId, parentMessageId, viewerUserId) {
  const parent = requireChatMessage(state, tenantId, parentMessageId);
  if (parent.threadId !== threadId) {
    const err = new Error(`Parent message '${parentMessageId}' is not in thread '${threadId}'`);
    err.statusCode = 400;
    throw err;
  }
  const replies = listMessageReplies(state, tenantId, threadId, parentMessageId, viewerUserId);
  return {
    parent: projectMessageForViewer(parent, viewerUserId),
    replies,
    mode: replies.length > 1 ? "mini-thread" : "inline"
  };
}

export function createAiReplyForMessage(state, tenant, payload = {}, actor = {}) {
  const thread = requireWorkspaceThread(state, tenant.id, payload.threadId);
  const parent = requireChatMessage(state, tenant.id, payload.messageId);
  if (parent.threadId !== thread.id) {
    const err = new Error(`Message '${parent.id}' is not in thread '${thread.id}'`);
    err.statusCode = 400;
    throw err;
  }
  const profile = getWorkspaceAgentProfile(state, tenant.id);
  const visibility = normalizeVisibility(payload.visibility);
  const responseBody = baseAgentResponse(parent, profile, payload.responseText);
  const aiMessage = createChatMessage(state, tenant, {
    threadId: thread.id,
    folderId: thread.folderId,
    parentMessageId: parent.id,
    authorType: "agent",
    authorId: "workspace_agent",
    authorName: profile.name,
    visibility,
    privateRecipientUserId: visibility === "private" ? actor.userId : null,
    body: responseBody,
    attachments: payload.attachments
  }, actor);

  if (visibility === "shared") {
    listTenantTeamMemberIds(state, tenant.id)
      .filter((memberId) => memberId !== actor.userId)
      .forEach((memberId) => {
        createNotification(state, {
          tenantId: tenant.id,
          userId: memberId,
          kind: "ai_reply",
          folderId: thread.folderId,
          threadId: thread.id,
          messageId: aiMessage.id,
          title: `${profile.name} replied in ${thread.title}`
        });
      });
  } else {
    listTenantTeamMemberIds(state, tenant.id)
      .filter((memberId) => memberId !== actor.userId)
      .forEach((memberId) => {
        createNotification(state, {
          tenantId: tenant.id,
          userId: memberId,
          kind: "ai_reply",
          folderId: thread.folderId,
          threadId: thread.id,
          messageId: aiMessage.id,
          title: `Private AI exchange occurred in ${thread.title}`
        });
      });
    createChatMessage(state, tenant, {
      threadId: thread.id,
      folderId: thread.folderId,
      parentMessageId: parent.id,
      authorType: "agent",
      authorId: "workspace_agent",
      authorName: profile.name,
      visibility: "shared",
      body: `[Private AI exchange occurred for ${actor.userId ?? "requester"}]`,
      isPrivateMarker: true
    }, actor);
  }

  return aiMessage;
}

export function listThreadAttachments(state, tenantId, threadId, viewerUserId) {
  requireWorkspaceThread(state, tenantId, threadId);
  return state.chatMessages
    .filter((message) => message.tenantId === tenantId && message.threadId === threadId)
    .map((message) => projectMessageForViewer(message, viewerUserId))
    .flatMap((message) => (message.attachments || []).map((attachment) => ({
      ...attachment,
      messageId: message.id,
      createdAt: message.createdAt,
      authorName: message.authorName
    })))
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function listNotifications(state, tenantId, userId) {
  return state.notifications
    .filter((item) => item.tenantId === tenantId && item.userId === userId)
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function markNotificationRead(state, tenantId, userId, notificationId) {
  const notification = state.notifications.find(
    (item) => item.tenantId === tenantId && item.userId === userId && item.id === notificationId
  );
  if (!notification) {
    const err = new Error(`Notification '${notificationId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  notification.read = true;
  notification.readAt = nowIso();
  return notification;
}

export function listFolderAutomations(state, tenantId, folderId) {
  return state.folderAutomations
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (folderId ? item.folderId === folderId : true))
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

function computeNextRunAt(intervalMinutes) {
  if (!intervalMinutes) return null;
  return new Date(Date.now() + intervalMinutes * 60_000).toISOString();
}

export function createFolderAutomation(state, tenant, folderId, payload = {}) {
  requireWorkspaceFolder(state, tenant.id, folderId);
  const triggerType = payload.triggerType === "heartbeat" ? "heartbeat" : "cron";
  const intervalMinutes = triggerType === "cron"
    ? Math.max(1, toNumber(payload.intervalMinutes, 30))
    : null;
  const automation = {
    id: newId("folder_auto"),
    tenantId: tenant.id,
    folderId,
    name: String(payload.name ?? "Folder Automation"),
    enabled: payload.enabled !== false,
    triggerType,
    intervalMinutes,
    heartbeatPath: payload.heartbeatPath ? String(payload.heartbeatPath) : null,
    heartbeatContent: payload.heartbeatContent ? String(payload.heartbeatContent) : "",
    actionType: payload.actionType ?? "run_agent_job",
    actionPayload: typeof payload.actionPayload === "object" && payload.actionPayload ? payload.actionPayload : {},
    targetThreadId: payload.targetThreadId ? String(payload.targetThreadId) : null,
    createdAt: nowIso(),
    updatedAt: nowIso(),
    lastRunAt: null,
    nextRunAt: triggerType === "cron" ? computeNextRunAt(intervalMinutes) : null
  };
  if (automation.targetThreadId) {
    const thread = requireWorkspaceThread(state, tenant.id, automation.targetThreadId);
    if (thread.folderId !== folderId) {
      const err = new Error("targetThreadId must belong to the same folder");
      err.statusCode = 400;
      throw err;
    }
  }
  state.folderAutomations.push(automation);
  return automation;
}

export function patchFolderAutomation(state, tenantId, automationId, payload = {}) {
  const automation = state.folderAutomations.find((item) => item.tenantId === tenantId && item.id === automationId);
  if (!automation) {
    const err = new Error(`Folder automation '${automationId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (payload.name != null) automation.name = String(payload.name);
  if (payload.enabled != null) automation.enabled = Boolean(payload.enabled);
  if (payload.intervalMinutes != null && automation.triggerType === "cron") {
    automation.intervalMinutes = Math.max(1, toNumber(payload.intervalMinutes, automation.intervalMinutes || 30));
    automation.nextRunAt = computeNextRunAt(automation.intervalMinutes);
  }
  if (payload.triggerType != null) {
    automation.triggerType = payload.triggerType === "heartbeat" ? "heartbeat" : "cron";
    automation.intervalMinutes = automation.triggerType === "cron"
      ? Math.max(1, toNumber(payload.intervalMinutes, automation.intervalMinutes || 30))
      : null;
    automation.nextRunAt = automation.triggerType === "cron" ? computeNextRunAt(automation.intervalMinutes) : null;
  }
  if (payload.heartbeatPath !== undefined) automation.heartbeatPath = payload.heartbeatPath ? String(payload.heartbeatPath) : null;
  if (payload.heartbeatContent !== undefined) automation.heartbeatContent = payload.heartbeatContent ? String(payload.heartbeatContent) : "";
  if (payload.actionType != null) automation.actionType = String(payload.actionType);
  if (payload.actionPayload != null && typeof payload.actionPayload === "object") automation.actionPayload = payload.actionPayload;
  if (payload.targetThreadId !== undefined) automation.targetThreadId = payload.targetThreadId ? String(payload.targetThreadId) : null;
  automation.updatedAt = nowIso();
  return automation;
}

export function listAutomationRuns(state, tenantId) {
  return state.automationRuns
    .filter((item) => item.tenantId === tenantId)
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function runFolderAutomation(state, tenant, automationId, adapters = {}, runtime = {}) {
  const automation = state.folderAutomations.find((item) => item.tenantId === tenant.id && item.id === automationId);
  if (!automation) {
    const err = new Error(`Folder automation '${automationId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (!automation.enabled) {
    const err = new Error(`Folder automation '${automation.id}' is disabled`);
    err.statusCode = 400;
    throw err;
  }

  const run = {
    id: newId("auto_run"),
    tenantId: tenant.id,
    automationId: automation.id,
    folderId: automation.folderId,
    triggerType: runtime.triggerType ?? automation.triggerType,
    triggerId: runtime.triggerId ?? null,
    actionType: runtime.actionType ?? automation.actionType,
    status: "completed",
    correlationId: newId("corr"),
    details: {},
    createdAt: nowIso()
  };

  const actionPayload = runtime.actionPayload ?? automation.actionPayload ?? {};
  const targetThreadId = runtime.targetThreadId ?? automation.targetThreadId;

  try {
    if (run.actionType === "run_skill" && adapters.runSkillPack) {
      const skillRun = adapters.runSkillPack(state, tenant, {
        skillId: actionPayload.skillId,
        intent: actionPayload.intent ?? "automation_run",
        channel: actionPayload.channel ?? "web",
        requestedTools: actionPayload.requestedTools ?? [],
        estimatedTokens: toNumber(actionPayload.estimatedTokens, 700),
        timeoutMs: toNumber(actionPayload.timeoutMs, 2500),
        generateReport: Boolean(actionPayload.generateReport)
      }, {
        runModelTask: adapters.runModelTask,
        generateReport: adapters.generateReport
      });
      run.details.skillRunId = skillRun.id;
      run.details.skillId = skillRun.skillId;
    } else if (run.actionType === "run_agent_job" && adapters.createAgentJob) {
      const job = adapters.createAgentJob(state, tenant, {
        jobType: actionPayload.jobType ?? "cowork_thread",
        folderId: automation.folderId,
        threadId: targetThreadId,
        input: actionPayload.input ?? "Heartbeat-triggered automation run."
      }, adapters);
      run.details.jobId = job.id;
      run.details.jobStatus = job.status;
    } else if (run.actionType === "request_device_command" && adapters.createDeviceCommandRequest) {
      const request = adapters.createDeviceCommandRequest(state, tenant, {
        folderId: automation.folderId,
        threadId: targetThreadId,
        command: actionPayload.command ?? "pwd",
        args: Array.isArray(actionPayload.args) ? actionPayload.args : [],
        requestedBy: "automation"
      }, adapters);
      run.details.deviceCommandRequestId = request.id;
      run.details.deviceCommandStatus = request.status;
      listTenantTeamMemberIds(state, tenant.id).forEach((memberId) => {
        createNotification(state, {
          tenantId: tenant.id,
          userId: memberId,
          kind: "approval_required",
          folderId: automation.folderId,
          threadId: targetThreadId ?? "",
          messageId: null,
          title: `Approval required for command '${request.command}'`
        });
      });
    } else {
      run.details.note = "No adapter configured for action type";
    }
  } catch (error) {
    run.status = "failed";
    run.details.error = error.message;
  }

  state.automationRuns.push(run);
  automation.lastRunAt = run.createdAt;
  if (automation.triggerType === "cron" && automation.intervalMinutes) {
    automation.nextRunAt = computeNextRunAt(automation.intervalMinutes);
  }
  automation.updatedAt = nowIso();

  if (targetThreadId) {
    const summary = run.status === "completed"
      ? `Automation '${automation.name}' completed (${run.actionType}).`
      : `Automation '${automation.name}' failed: ${run.details.error}`;
    createChatMessage(state, tenant, {
      threadId: targetThreadId,
      folderId: automation.folderId,
      authorType: "agent",
      authorId: "workspace_agent",
      authorName: getWorkspaceAgentProfile(state, tenant.id).name,
      visibility: "shared",
      body: summary
    }, { userId: "automation" });
  }

  listTenantTeamMemberIds(state, tenant.id).forEach((memberId) => {
    createNotification(state, {
      tenantId: tenant.id,
      userId: memberId,
      kind: "automation_run",
      folderId: automation.folderId,
      threadId: targetThreadId ?? "",
      messageId: null,
      title: `Automation '${automation.name}' ${run.status}`
    });
  });

  return run;
}

export function parseHeartbeatContent(content) {
  const parsed = parseHeartbeatContentInternal(content);
  return {
    config: parsed.config,
    notes: parsed.notes,
    errors: parsed.errors
  };
}

export function validateHeartbeatContent(content) {
  const parsed = parseHeartbeatContentInternal(content);
  const errors = [...parsed.errors];
  if (parsed.config) {
    if (!parsed.config.targetThreadId) {
      errors.push({ line: 1, message: "targetThreadId is required" });
    }
    if (!Array.isArray(parsed.config.triggers) || parsed.config.triggers.length === 0) {
      errors.push({ line: 1, message: "At least one trigger is required" });
    } else {
      const ids = new Set();
      parsed.config.triggers.forEach((trigger, idx) => {
        if (!trigger.id) {
          errors.push({ line: idx + 1, message: "Trigger id is required" });
        } else if (ids.has(trigger.id)) {
          errors.push({ line: idx + 1, message: `Duplicate trigger id '${trigger.id}'` });
        }
        ids.add(trigger.id);
        const dsl = validateHeartbeatDsl(trigger.when);
        if (!dsl.ok) {
          errors.push({ line: idx + 1, message: `Invalid trigger.when for '${trigger.id}': ${dsl.message}` });
        }
      });
    }
  }
  return {
    ok: errors.length === 0,
    config: parsed.config,
    errors
  };
}

function evaluateHeartbeatTriggers(state, tenantId, automation, config) {
  const matches = [];
  const warnings = [];
  for (const trigger of config.triggers) {
    const parsedDsl = validateHeartbeatDsl(trigger.when);
    if (!parsedDsl.ok) {
      warnings.push({ triggerId: trigger.id, message: parsedDsl.message });
      continue;
    }
    const value = evaluateOperandValue(state, tenantId, automation, parsedDsl);
    const matched = compareDslValue(value, parsedDsl.operator, parsedDsl.threshold);
    if (!matched) continue;

    const cooldownMinutes = Math.max(0, toNumber(trigger.cooldownMinutes, 0));
    const stateEntry = getHeartbeatStateEntry(state, tenantId, automation.id, trigger.id);
    if (stateEntry?.lastTriggeredAt && cooldownMinutes > 0) {
      const elapsedMinutes = (Date.now() - Date.parse(stateEntry.lastTriggeredAt)) / 60_000;
      if (elapsedMinutes < cooldownMinutes) {
        continue;
      }
    }
    setHeartbeatStateEntry(state, tenantId, automation.id, trigger.id, {
      lastTriggeredAt: nowIso(),
      lastValue: value
    });
    matches.push({
      triggerId: trigger.id,
      value,
      action: trigger.action,
      payload: trigger.payload || {},
      targetThreadId: config.targetThreadId
    });
  }
  return { matches, warnings };
}

export function processFolderAutomations(state, adapters = {}) {
  const runs = [];
  const now = Date.now();

  for (const automation of state.folderAutomations) {
    if (!automation.enabled) continue;
    const tenant = state.tenants.get(automation.tenantId);
    if (!tenant) continue;
    if (tenant.autonomyPolicy?.killSwitch) continue;

    if (automation.triggerType === "cron") {
      const dueAt = automation.nextRunAt ? Date.parse(automation.nextRunAt) : Number.NaN;
      if (!Number.isFinite(dueAt) || dueAt > now) continue;
      const run = runFolderAutomation(state, tenant, automation.id, adapters, { triggerType: "cron" });
      runs.push(run);
      continue;
    }

    if (automation.triggerType !== "heartbeat") continue;
    const heartbeatContent = readHeartbeatContent(automation);
    if (!heartbeatContent) continue;
    const validated = validateHeartbeatContent(heartbeatContent);
    if (!validated.ok || !validated.config?.enabled) continue;

    const { matches } = evaluateHeartbeatTriggers(state, tenant.id, automation, validated.config);
    for (const match of matches) {
      const run = runFolderAutomation(state, tenant, automation.id, adapters, {
        triggerType: "heartbeat",
        triggerId: match.triggerId,
        actionType: match.action,
        actionPayload: match.payload,
        targetThreadId: match.targetThreadId
      });
      runs.push(run);
    }
  }

  return runs;
}

export function ensureWorkspaceCoreDefaults(state, tenant) {
  ensureWorkspaceAgentProfile(state, tenant);
  const legacyComments = state.workspaceComments.filter((item) => item.tenantId === tenant.id);
  if (legacyComments.length && !state.chatMessages.some((item) => item.tenantId === tenant.id)) {
    legacyComments.forEach((comment) => {
      const thread = state.workspaceThreads.find((item) => item.tenantId === tenant.id && item.id === comment.threadId);
      if (!thread) return;
      state.chatMessages.push({
        id: comment.id.replace(/^workspace_comment_/, "chat_msg_"),
        tenantId: tenant.id,
        folderId: thread.folderId,
        threadId: comment.threadId,
        parentMessageId: null,
        authorType: comment.role === "assistant" ? "agent" : "user",
        authorId: comment.authorId,
        authorName: comment.authorName,
        visibility: "shared",
        privateRecipientUserId: null,
        body: comment.body,
        attachments: [],
        hasMiniThread: false,
        replyCount: 0,
        isPrivateMarker: false,
        createdAt: comment.createdAt,
        updatedAt: comment.createdAt
      });
    });
  }
}

