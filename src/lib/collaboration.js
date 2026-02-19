import { newId } from "./state.js";
import { createChatMessage } from "./workspace-core.js";

function nowIso() {
  return new Date().toISOString();
}

function defaultTeamMember(tenant, payload = {}) {
  return {
    id: newId("team_member"),
    tenantId: tenant.id,
    name: payload.name ?? `${tenant.name} Admin`,
    email: payload.email ?? `${tenant.name.toLowerCase().replaceAll(/\s+/g, ".")}@example.com`,
    role: payload.role ?? "owner",
    title: payload.title ?? "Tenant Admin",
    status: payload.status ?? "active",
    createdAt: nowIso(),
    updatedAt: nowIso()
  };
}

export function listTeamMembers(state, tenantId) {
  return state.teamMembers.filter((item) => item.tenantId === tenantId);
}

export function addTeamMember(state, tenant, payload = {}) {
  if (!payload.name || !payload.email) {
    const err = new Error("name and email are required");
    err.statusCode = 400;
    throw err;
  }
  const member = defaultTeamMember(tenant, payload);
  state.teamMembers.push(member);
  return member;
}

export function patchTeamMember(state, tenantId, memberId, payload = {}) {
  const member = state.teamMembers.find((item) => item.tenantId === tenantId && item.id === memberId);
  if (!member) {
    const err = new Error(`Team member '${memberId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (payload.name != null) member.name = payload.name;
  if (payload.email != null) member.email = payload.email;
  if (payload.role != null) member.role = payload.role;
  if (payload.title != null) member.title = payload.title;
  if (payload.status != null) member.status = payload.status;
  member.updatedAt = nowIso();
  return member;
}

export function listWorkspaceFolders(state, tenantId) {
  return state.workspaceFolders
    .filter((item) => item.tenantId === tenantId)
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function requireWorkspaceFolder(state, tenantId, folderId) {
  const folder = state.workspaceFolders.find((item) => item.tenantId === tenantId && item.id === folderId);
  if (!folder) {
    const err = new Error(`Workspace folder '${folderId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return folder;
}

export function createWorkspaceFolder(state, tenant, payload = {}) {
  if (!payload.name) {
    const err = new Error("Folder name is required");
    err.statusCode = 400;
    throw err;
  }
  const folder = {
    id: newId("workspace_folder"),
    tenantId: tenant.id,
    name: payload.name,
    description: payload.description ?? "",
    createdBy: payload.createdBy ?? "system",
    createdAt: nowIso(),
    updatedAt: nowIso()
  };
  state.workspaceFolders.push(folder);
  return folder;
}

export function patchWorkspaceFolder(state, tenantId, folderId, payload = {}) {
  const folder = state.workspaceFolders.find((item) => item.tenantId === tenantId && item.id === folderId);
  if (!folder) {
    const err = new Error(`Workspace folder '${folderId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (payload.name != null) folder.name = payload.name;
  if (payload.description != null) folder.description = payload.description;
  folder.updatedAt = nowIso();
  return folder;
}

export function listWorkspaceThreads(state, tenantId, options = {}) {
  return state.workspaceThreads
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (options.folderId ? item.folderId === options.folderId : true))
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function createWorkspaceThread(state, tenant, payload = {}) {
  if (!payload.folderId || !payload.title) {
    const err = new Error("folderId and title are required");
    err.statusCode = 400;
    throw err;
  }
  const folder = requireWorkspaceFolder(state, tenant.id, payload.folderId);

  const thread = {
    id: newId("workspace_thread"),
    tenantId: tenant.id,
    folderId: folder.id,
    title: payload.title,
    summary: payload.summary ?? "",
    contextMode: payload.contextMode === "per-user" || payload.contextMode === "per-channel-user"
      ? payload.contextMode
      : "shared",
    modelProfileId: payload.modelProfileId ?? null,
    createdBy: payload.createdBy ?? "system",
    createdAt: nowIso(),
    updatedAt: nowIso(),
    lastMessageAt: null
  };
  state.workspaceThreads.push(thread);
  folder.updatedAt = nowIso();
  return thread;
}

export function patchWorkspaceThread(state, tenantId, threadId, payload = {}) {
  const thread = state.workspaceThreads.find((item) => item.tenantId === tenantId && item.id === threadId);
  if (!thread) {
    const err = new Error(`Workspace thread '${threadId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (payload.title != null) thread.title = String(payload.title);
  if (payload.summary != null) thread.summary = String(payload.summary);
  if (payload.modelProfileId !== undefined) thread.modelProfileId = payload.modelProfileId ?? null;
  if (payload.contextMode != null) {
    thread.contextMode = payload.contextMode === "per-user" || payload.contextMode === "per-channel-user"
      ? payload.contextMode
      : "shared";
  }
  thread.updatedAt = nowIso();
  return thread;
}

export function requireWorkspaceThread(state, tenantId, threadId) {
  const thread = state.workspaceThreads.find((item) => item.tenantId === tenantId && item.id === threadId);
  if (!thread) {
    const err = new Error(`Workspace thread '${threadId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return thread;
}

export function listThreadComments(state, tenantId, threadId) {
  requireWorkspaceThread(state, tenantId, threadId);
  const mapped = state.chatMessages
    .filter((item) => item.tenantId === tenantId && item.threadId === threadId)
    .filter((item) => item.visibility === "shared")
    .sort((a, b) => (a.createdAt > b.createdAt ? 1 : -1))
    .map((item) => ({
      id: item.id,
      tenantId: item.tenantId,
      threadId: item.threadId,
      authorId: item.authorId,
      authorName: item.authorName,
      role: item.authorType === "agent" ? "assistant" : "comment",
      body: item.body,
      createdAt: item.createdAt
    }));
  if (mapped.length) return mapped;
  return state.workspaceComments
    .filter((item) => item.tenantId === tenantId && item.threadId === threadId)
    .sort((a, b) => (a.createdAt > b.createdAt ? 1 : -1));
}

export function createThreadComment(state, tenant, payload = {}) {
  if (!payload.threadId || !payload.body) {
    const err = new Error("threadId and body are required");
    err.statusCode = 400;
    throw err;
  }
  const thread = requireWorkspaceThread(state, tenant.id, payload.threadId);
  const authorType = payload.role === "assistant" ? "agent" : "user";
  const message = createChatMessage(state, tenant, {
    threadId: thread.id,
    folderId: thread.folderId,
    authorType,
    authorId: payload.authorId ?? "system",
    authorName: payload.authorName ?? "System",
    visibility: "shared",
    body: payload.body
  }, { userId: payload.authorId ?? "system" });
  const comment = {
    id: newId("workspace_comment"),
    tenantId: tenant.id,
    threadId: thread.id,
    authorId: message.authorId,
    authorName: message.authorName,
    role: payload.role ?? "comment",
    body: message.body,
    createdAt: message.createdAt
  };
  state.workspaceComments.push(comment);
  return comment;
}

export function ensureCollaborationDefaults(state, tenant) {
  if (!listTeamMembers(state, tenant.id).length) {
    state.teamMembers.push(defaultTeamMember(tenant));
  }
  let folders = listWorkspaceFolders(state, tenant.id);
  if (!folders.length) {
    createWorkspaceFolder(state, tenant, { name: "General", description: "Shared cross-functional analysis threads", createdBy: "system" });
    createWorkspaceFolder(state, tenant, { name: "Deal Desk", description: "Pricing, approval, and deal support threads", createdBy: "system" });
    folders = listWorkspaceFolders(state, tenant.id);
  }
  const threads = listWorkspaceThreads(state, tenant.id);
  if (!threads.length) {
    const general = folders.find((item) => item.name === "General") ?? folders[0];
    const dealDesk = folders.find((item) => item.name === "Deal Desk") ?? folders[0];
    const t1 = createWorkspaceThread(state, tenant, {
      folderId: general.id,
      title: "Weekly Executive Insights",
      summary: "Cross-domain summary and action priorities",
      createdBy: "system"
    });
    const t2 = createWorkspaceThread(state, tenant, {
      folderId: dealDesk.id,
      title: "Enterprise Renewal Risk Review",
      summary: "Track approvals, margin impact, and legal blockers",
      createdBy: "system"
    });
    createThreadComment(state, tenant, {
      threadId: t1.id,
      authorId: "system",
      authorName: "InsightFoundry",
      role: "assistant",
      body: "Use this shared thread to align on priorities before running reports."
    });
    createThreadComment(state, tenant, {
      threadId: t2.id,
      authorId: "system",
      authorName: "InsightFoundry",
      role: "assistant",
      body: "Deal desk workflow starts here: attach quote context, then run risk and approval checks."
    });
  }
}
