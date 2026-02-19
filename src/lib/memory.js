import { newId } from "./state.js";

function nowIso() {
  return new Date().toISOString();
}

function normalizeTags(value) {
  if (!Array.isArray(value)) return [];
  return value
    .map((item) => String(item ?? "").trim().toLowerCase())
    .filter(Boolean)
    .slice(0, 20);
}

function normalizeText(value, max = 3000) {
  return String(value ?? "").trim().slice(0, max);
}

function requireBody(body, label = "body") {
  const text = normalizeText(body);
  if (!text) {
    const err = new Error(`${label} is required`);
    err.statusCode = 400;
    throw err;
  }
  return text;
}

function scoreMemory(memory, options = {}) {
  let score = 0;
  if (options.folderId && memory.folderId === options.folderId) score += 4;
  if (options.threadId && memory.threadId === options.threadId) score += 5;
  if (options.domain && memory.domain === options.domain) score += 2;
  if (options.tags?.length) {
    const tagSet = new Set(options.tags);
    const overlap = (memory.tags ?? []).filter((tag) => tagSet.has(tag)).length;
    score += overlap * 1.5;
  }
  const ageMs = Date.now() - Date.parse(memory.updatedAt || memory.createdAt || nowIso());
  const ageDays = Math.max(0, ageMs / (24 * 60 * 60 * 1000));
  score -= Math.min(3, ageDays * 0.08);
  return score;
}

function byUpdatedDesc(a, b) {
  if (a.updatedAt === b.updatedAt) return a.id < b.id ? 1 : -1;
  return a.updatedAt < b.updatedAt ? 1 : -1;
}

function byScoreDesc(a, b) {
  if (a.score === b.score) return byUpdatedDesc(a.memory, b.memory);
  return a.score < b.score ? 1 : -1;
}

export function listProjectMemories(state, tenantId, options = {}) {
  return state.projectMemories
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (options.folderId ? item.folderId === options.folderId : true))
    .filter((item) => (options.threadId ? item.threadId === options.threadId : true))
    .filter((item) => (options.domain ? item.domain === options.domain : true))
    .sort(byUpdatedDesc);
}

export function createProjectMemory(state, tenant, payload = {}, actorId = "system") {
  const memory = {
    id: newId("project_memory"),
    tenantId: tenant.id,
    folderId: payload.folderId ? String(payload.folderId) : null,
    threadId: payload.threadId ? String(payload.threadId) : null,
    domain: payload.domain ? String(payload.domain) : "general",
    title: normalizeText(payload.title ?? "Project memory", 180),
    body: requireBody(payload.body),
    tags: normalizeTags(payload.tags),
    visibility: "shared",
    createdBy: actorId,
    createdAt: nowIso(),
    updatedAt: nowIso()
  };
  state.projectMemories.push(memory);
  return memory;
}

export function patchProjectMemory(state, tenantId, memoryId, payload = {}, actorId = "system") {
  const memory = state.projectMemories.find((item) => item.tenantId === tenantId && item.id === memoryId);
  if (!memory) {
    const err = new Error(`Project memory '${memoryId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (payload.title != null) memory.title = normalizeText(payload.title, 180);
  if (payload.body != null) memory.body = requireBody(payload.body);
  if (payload.tags != null) memory.tags = normalizeTags(payload.tags);
  if (payload.domain != null) memory.domain = normalizeText(payload.domain, 64);
  if (payload.folderId !== undefined) memory.folderId = payload.folderId ? String(payload.folderId) : null;
  if (payload.threadId !== undefined) memory.threadId = payload.threadId ? String(payload.threadId) : null;
  memory.updatedBy = actorId;
  memory.updatedAt = nowIso();
  return memory;
}

export function listUserMemories(state, tenantId, userId, options = {}) {
  return state.userMemories
    .filter((item) => item.tenantId === tenantId && item.userId === userId)
    .filter((item) => (options.folderId ? item.folderId === options.folderId : true))
    .filter((item) => (options.threadId ? item.threadId === options.threadId : true))
    .sort(byUpdatedDesc);
}

export function createUserMemory(state, tenant, userId, payload = {}) {
  const memory = {
    id: newId("user_memory"),
    tenantId: tenant.id,
    userId: String(userId),
    scope: payload.scope === "thread" ? "thread" : payload.scope === "folder" ? "folder" : "global",
    folderId: payload.folderId ? String(payload.folderId) : null,
    threadId: payload.threadId ? String(payload.threadId) : null,
    title: normalizeText(payload.title ?? "User memory", 180),
    body: requireBody(payload.body),
    tags: normalizeTags(payload.tags),
    createdAt: nowIso(),
    updatedAt: nowIso()
  };
  state.userMemories.push(memory);
  return memory;
}

export function patchUserMemory(state, tenantId, userId, memoryId, payload = {}) {
  const memory = state.userMemories.find(
    (item) => item.tenantId === tenantId && item.userId === userId && item.id === memoryId
  );
  if (!memory) {
    const err = new Error(`User memory '${memoryId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (payload.title != null) memory.title = normalizeText(payload.title, 180);
  if (payload.body != null) memory.body = requireBody(payload.body);
  if (payload.tags != null) memory.tags = normalizeTags(payload.tags);
  if (payload.scope != null) {
    memory.scope = payload.scope === "thread" ? "thread" : payload.scope === "folder" ? "folder" : "global";
  }
  if (payload.folderId !== undefined) memory.folderId = payload.folderId ? String(payload.folderId) : null;
  if (payload.threadId !== undefined) memory.threadId = payload.threadId ? String(payload.threadId) : null;
  memory.updatedAt = nowIso();
  return memory;
}

export function buildMemoryContext(state, tenantId, userId, options = {}) {
  const limit = Math.max(1, Math.min(50, Number(options.limit ?? 12)));
  const tags = normalizeTags(options.tags ?? []);
  const projectRanked = listProjectMemories(state, tenantId, options)
    .map((memory) => ({ memory, score: scoreMemory(memory, { ...options, tags }) }))
    .sort(byScoreDesc)
    .slice(0, limit);
  const userRanked = listUserMemories(state, tenantId, userId, options)
    .map((memory) => ({ memory, score: scoreMemory(memory, { ...options, tags }) + 1 }))
    .sort(byScoreDesc)
    .slice(0, limit);

  const context = {
    project: projectRanked.map((entry) => ({ ...entry.memory, relevanceScore: Number(entry.score.toFixed(2)) })),
    user: userRanked.map((entry) => ({ ...entry.memory, relevanceScore: Number(entry.score.toFixed(2)) })),
    merged: [...projectRanked, ...userRanked]
      .sort((a, b) => byScoreDesc(a, b))
      .slice(0, limit)
      .map((entry) => ({
        id: entry.memory.id,
        memoryType: entry.memory.userId ? "user" : "project",
        title: entry.memory.title,
        body: entry.memory.body,
        tags: entry.memory.tags ?? [],
        relevanceScore: Number(entry.score.toFixed(2)),
        updatedAt: entry.memory.updatedAt
      }))
  };
  return context;
}

export function snapshotMemoryContext(state, tenant, userId, payload = {}) {
  const context = buildMemoryContext(state, tenant.id, userId, payload);
  const snapshot = {
    id: newId("memory_snapshot"),
    tenantId: tenant.id,
    userId: String(userId),
    folderId: payload.folderId ? String(payload.folderId) : null,
    threadId: payload.threadId ? String(payload.threadId) : null,
    tags: normalizeTags(payload.tags ?? []),
    contextCount: context.merged.length,
    summary: context.merged
      .slice(0, 4)
      .map((item) => `${item.title}: ${item.body.slice(0, 120)}`)
      .join(" | "),
    createdAt: nowIso()
  };
  state.memorySnapshots.push(snapshot);
  return {
    snapshot,
    context
  };
}

export function listMemorySnapshots(state, tenantId, userId, options = {}) {
  return state.memorySnapshots
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (userId ? item.userId === userId : true))
    .filter((item) => (options.folderId ? item.folderId === options.folderId : true))
    .filter((item) => (options.threadId ? item.threadId === options.threadId : true))
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function ingestRememberCommand(state, tenant, userId, payload = {}) {
  const body = String(payload.body ?? "").trim();
  if (!body.startsWith("/remember") && !body.startsWith("/remember-project")) return null;
  if (body.startsWith("/remember-project")) {
    const content = body.replace(/^\/remember-project\s*/i, "");
    if (!content) return null;
    return {
      type: "project",
      memory: createProjectMemory(state, tenant, {
        folderId: payload.folderId,
        threadId: payload.threadId,
        title: payload.title ?? "Remembered project note",
        body: content,
        tags: payload.tags ?? []
      }, userId)
    };
  }
  const content = body.replace(/^\/remember\s*/i, "");
  if (!content) return null;
  return {
    type: "user",
    memory: createUserMemory(state, tenant, userId, {
      scope: payload.threadId ? "thread" : payload.folderId ? "folder" : "global",
      folderId: payload.folderId,
      threadId: payload.threadId,
      title: payload.title ?? "Remembered note",
      body: content,
      tags: payload.tags ?? []
    })
  };
}

