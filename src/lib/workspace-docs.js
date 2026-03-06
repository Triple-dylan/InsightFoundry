import { newId } from "./state.js";

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_REDIRECT_URI = process.env.GOOGLE_REDIRECT_URI || "";
const GOOGLE_SCOPES = process.env.GOOGLE_SCOPES || [
  "https://www.googleapis.com/auth/drive.readonly",
  "https://www.googleapis.com/auth/documents.readonly",
  "https://www.googleapis.com/auth/spreadsheets.readonly"
].join(" ");

function googleLiveEnabled() {
  return Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET && GOOGLE_REDIRECT_URI);
}

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

async function tryLoadGoogleDriveFiles(state, tenantId) {
  const auth = state.workspaceDocAuth.find((item) => item.tenantId === tenantId && item.status === "connected");
  if (!auth?.tokenRef) return null;
  const secret = state.secretRefs.get(auth.tokenRef);
  const accessToken = String(secret?.accessToken || "").trim();
  if (!accessToken) return null;
  const query = encodeURIComponent("mimeType='application/vnd.google-apps.document' or mimeType='application/vnd.google-apps.spreadsheet'");
  const endpoint = `https://www.googleapis.com/drive/v3/files?q=${query}&pageSize=25&fields=files(id,name,mimeType,modifiedTime)`;
  const res = await fetch(endpoint, {
    headers: {
      authorization: `Bearer ${accessToken}`
    }
  });
  if (!res.ok) return null;
  const payload = await res.json().catch(() => ({}));
  const files = Array.isArray(payload.files) ? payload.files : [];
  if (!files.length) return [];
  return files.map((file) => {
    const isSheet = String(file.mimeType || "").includes("spreadsheet");
    return {
      id: `drive_${file.id}`,
      tenantId,
      provider: isSheet ? "google_sheets" : "google_docs",
      externalFileId: String(file.id),
      title: String(file.name || "Untitled"),
      embedUrl: isSheet
        ? `https://docs.google.com/spreadsheets/d/${encodeURIComponent(file.id)}/edit`
        : `https://docs.google.com/document/d/${encodeURIComponent(file.id)}/edit`,
      editable: true,
      linkedThreadIds: [],
      createdAt: nowIso(),
      updatedAt: String(file.modifiedTime || nowIso())
    };
  });
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

  const authorizeUrl = googleLiveEnabled()
    ? `https://accounts.google.com/o/oauth2/v2/auth?client_id=${encodeURIComponent(GOOGLE_CLIENT_ID)}&redirect_uri=${encodeURIComponent(GOOGLE_REDIRECT_URI)}&response_type=code&scope=${encodeURIComponent(GOOGLE_SCOPES)}&access_type=offline&prompt=consent&state=${encodeURIComponent(oauthState)}`
    : `/v1/integrations/google/workspace/auth/callback?state=${encodeURIComponent(oauthState)}&code=mock-code`;
  return {
    provider: "google_workspace",
    oauthState,
    authorizeUrl,
    expiresInSec: 600
  };
}

export async function completeGoogleWorkspaceAuth(state, tenantId, userId, payload = {}) {
  const match = state.workspaceDocAuthStates.find(
    (item) => item.id === payload.state && item.tenantId === tenantId && item.userId === userId
  );
  if (!match) {
    const err = new Error("Invalid oauth state");
    err.statusCode = 400;
    throw err;
  }

  let tokenRef = `gws_${newId("token")}`;
  if (googleLiveEnabled() && payload.code && payload.code !== "mock-code") {
    const tokenRes = await fetch("https://oauth2.googleapis.com/token", {
      method: "POST",
      headers: { "content-type": "application/x-www-form-urlencoded" },
      body: new URLSearchParams({
        code: String(payload.code),
        client_id: GOOGLE_CLIENT_ID,
        client_secret: GOOGLE_CLIENT_SECRET,
        redirect_uri: GOOGLE_REDIRECT_URI,
        grant_type: "authorization_code"
      })
    });
    const tokenPayload = await tokenRes.json().catch(() => ({}));
    if (!tokenRes.ok || !tokenPayload.access_token) {
      const err = new Error(tokenPayload.error_description || tokenPayload.error || "Google token exchange failed");
      err.statusCode = 400;
      throw err;
    }
    tokenRef = `gws_${newId("token")}`;
    state.secretRefs.set(tokenRef, {
      tenantId,
      kind: "google_workspace_oauth",
      hasCredentials: true,
      createdAt: nowIso(),
      updatedAt: nowIso(),
      accessToken: String(tokenPayload.access_token),
      refreshToken: tokenPayload.refresh_token ? String(tokenPayload.refresh_token) : "",
      expiresInSec: Number(tokenPayload.expires_in || 0)
    });
  }

  let auth = state.workspaceDocAuth.find((item) => item.tenantId === tenantId && item.userId === userId);
  if (!auth) {
    auth = {
      id: newId("doc_auth"),
      tenantId,
      userId,
      provider: "google_workspace",
      status: "connected",
      tokenRef,
      connectedAt: nowIso(),
      updatedAt: nowIso()
    };
    state.workspaceDocAuth.push(auth);
  } else {
    auth.status = "connected";
    auth.tokenRef = tokenRef;
    auth.updatedAt = nowIso();
  }

  state.workspaceDocAuthStates = state.workspaceDocAuthStates.filter((item) => item.id !== payload.state);
  ensureTenantDocs(state, tenantId);
  return auth;
}

export async function listWorkspaceDocFiles(state, tenantId, options = {}) {
  const fromDrive = await tryLoadGoogleDriveFiles(state, tenantId).catch(() => null);
  if (Array.isArray(fromDrive) && fromDrive.length) {
    const existingByExternal = new Map(
      state.workspaceDocFiles
        .filter((item) => item.tenantId === tenantId)
        .map((item) => [item.externalFileId, item])
    );
    for (const file of fromDrive) {
      const existing = existingByExternal.get(file.externalFileId);
      if (existing) {
        existing.title = file.title;
        existing.provider = file.provider;
        existing.embedUrl = file.embedUrl;
        existing.updatedAt = file.updatedAt;
      } else {
        state.workspaceDocFiles.push(file);
      }
    }
  } else {
    ensureTenantDocs(state, tenantId);
  }
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
