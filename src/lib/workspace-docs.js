import { newId } from "./state.js";

function nowIso() {
  return new Date().toISOString();
}

function ensureTenantDocs(state, tenantId) {
  const existing = state.workspaceDocFiles.filter((item) => item.tenantId === tenantId);
  if (existing.length) return existing;

  const seeded = [
    {
      id: newId("doc_file"),
      tenantId,
      provider: "google_docs",
      externalFileId: "mock-doc-001",
      title: "Board Update Draft",
      embedUrl: "https://docs.google.com/document/d/1jvB2p2oX0mock/edit",
      editable: true,
      linkedThreadIds: [],
      createdAt: nowIso(),
      updatedAt: nowIso()
    },
    {
      id: newId("doc_file"),
      tenantId,
      provider: "google_sheets",
      externalFileId: "mock-sheet-001",
      title: "Pipeline Forecast Sheet",
      embedUrl: "https://docs.google.com/spreadsheets/d/1Qhsmock/edit",
      editable: true,
      linkedThreadIds: [],
      createdAt: nowIso(),
      updatedAt: nowIso()
    }
  ];
  state.workspaceDocFiles.push(...seeded);
  return seeded;
}

export function startGoogleWorkspaceAuth(state, tenantId, userId) {
  const oauthState = newId("oauth_state");
  state.workspaceDocAuthStates.push({
    id: oauthState,
    tenantId,
    userId,
    createdAt: nowIso(),
    expiresAt: new Date(Date.now() + 10 * 60 * 1000).toISOString()
  });

  const authorizeUrl = `/v1/integrations/google/workspace/auth/callback?state=${encodeURIComponent(oauthState)}&code=mock-code`;
  return {
    provider: "google_workspace",
    oauthState,
    authorizeUrl,
    expiresInSec: 600
  };
}

export function completeGoogleWorkspaceAuth(state, tenantId, userId, payload = {}) {
  const match = state.workspaceDocAuthStates.find(
    (item) => item.id === payload.state && item.tenantId === tenantId && item.userId === userId
  );
  if (!match) {
    const err = new Error("Invalid oauth state");
    err.statusCode = 400;
    throw err;
  }

  let auth = state.workspaceDocAuth.find((item) => item.tenantId === tenantId && item.userId === userId);
  if (!auth) {
    auth = {
      id: newId("doc_auth"),
      tenantId,
      userId,
      provider: "google_workspace",
      status: "connected",
      tokenRef: `gws_${newId("token")}`,
      connectedAt: nowIso(),
      updatedAt: nowIso()
    };
    state.workspaceDocAuth.push(auth);
  } else {
    auth.status = "connected";
    auth.updatedAt = nowIso();
  }

  state.workspaceDocAuthStates = state.workspaceDocAuthStates.filter((item) => item.id !== payload.state);
  ensureTenantDocs(state, tenantId);
  return auth;
}

export function listWorkspaceDocFiles(state, tenantId, options = {}) {
  ensureTenantDocs(state, tenantId);
  const q = String(options.q ?? "").trim().toLowerCase();
  return state.workspaceDocFiles
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (q ? item.title.toLowerCase().includes(q) : true))
    .sort((a, b) => (a.updatedAt < b.updatedAt ? 1 : -1));
}

export function openWorkspaceDocFile(state, tenantId, payload = {}) {
  ensureTenantDocs(state, tenantId);
  const file = state.workspaceDocFiles.find(
    (item) => item.tenantId === tenantId && (item.id === payload.fileId || item.externalFileId === payload.externalFileId)
  );
  if (!file) {
    const err = new Error("Doc/Sheet file not found");
    err.statusCode = 404;
    throw err;
  }
  const session = {
    id: newId("doc_session"),
    tenantId,
    fileId: file.id,
    embedUrl: file.embedUrl,
    mode: payload.mode === "view" ? "view" : "edit",
    createdAt: nowIso()
  };
  state.workspaceDocSessions.push(session);
  return { session, file };
}

export function linkWorkspaceDocToThread(state, tenantId, payload = {}) {
  const file = state.workspaceDocFiles.find((item) => item.tenantId === tenantId && item.id === payload.fileId);
  if (!file) {
    const err = new Error("Doc/Sheet file not found");
    err.statusCode = 404;
    throw err;
  }
  const threadId = String(payload.threadId || "").trim();
  if (!threadId) {
    const err = new Error("threadId is required");
    err.statusCode = 400;
    throw err;
  }
  if (!file.linkedThreadIds.includes(threadId)) file.linkedThreadIds.push(threadId);
  file.updatedAt = nowIso();
  return file;
}
