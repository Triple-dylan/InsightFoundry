import crypto from "node:crypto";
import { newId } from "./state.js";

const MCP_PROVIDER_CATALOG = [
  {
    provider: "filesystem",
    displayName: "Filesystem MCP",
    description: "Read/write bounded workspace files for agent workflows.",
    authMode: "none"
  },
  {
    provider: "postgres",
    displayName: "Postgres MCP",
    description: "Expose database resources through MCP for controlled access.",
    authMode: "secret_ref"
  },
  {
    provider: "slack",
    displayName: "Slack MCP",
    description: "Expose Slack channel context and posting via MCP bridge.",
    authMode: "secret_ref"
  },
  {
    provider: "google-drive",
    displayName: "Google Drive MCP",
    description: "Access docs/sheets resources with tenant-scoped permissions.",
    authMode: "oauth_ref"
  }
];

function nowIso() {
  return new Date().toISOString();
}

function secretFingerprint(value) {
  return crypto.createHash("sha256").update(String(value)).digest("hex").slice(0, 20);
}

function requireProvider(provider) {
  const entry = MCP_PROVIDER_CATALOG.find((item) => item.provider === provider);
  if (!entry) {
    const err = new Error(`Unsupported MCP provider '${provider}'`);
    err.statusCode = 400;
    throw err;
  }
  return entry;
}

export function listMcpProviderCatalog() {
  return MCP_PROVIDER_CATALOG;
}

export function listMcpServers(state, tenantId) {
  return state.mcpServers.filter((item) => item.tenantId === tenantId);
}

export function createMcpServer(state, tenant, payload = {}) {
  if (!payload.provider || !payload.name) {
    const err = new Error("provider and name are required");
    err.statusCode = 400;
    throw err;
  }
  const provider = requireProvider(payload.provider);

  const server = {
    id: newId("mcp_server"),
    tenantId: tenant.id,
    provider: provider.provider,
    name: payload.name,
    status: "active",
    endpoint: payload.endpoint ?? "",
    authRef: payload.authRef ?? "",
    authFingerprint: payload.authRef ? secretFingerprint(payload.authRef) : "",
    allowedFolderIds: Array.isArray(payload.allowedFolderIds) ? payload.allowedFolderIds : [],
    capabilities: Array.isArray(payload.capabilities) ? payload.capabilities : ["resources.list", "resources.read"],
    createdAt: nowIso(),
    updatedAt: nowIso(),
    lastTestedAt: null
  };

  state.mcpServers.push(server);
  return server;
}

export function patchMcpServer(state, tenantId, serverId, payload = {}) {
  const server = state.mcpServers.find((item) => item.tenantId === tenantId && item.id === serverId);
  if (!server) {
    const err = new Error(`MCP server '${serverId}' not found`);
    err.statusCode = 404;
    throw err;
  }

  if (payload.name != null) server.name = payload.name;
  if (payload.endpoint != null) server.endpoint = payload.endpoint;
  if (payload.status != null) server.status = payload.status;
  if (payload.authRef != null) {
    server.authRef = payload.authRef;
    server.authFingerprint = payload.authRef ? secretFingerprint(payload.authRef) : "";
  }
  if (Array.isArray(payload.allowedFolderIds)) {
    server.allowedFolderIds = payload.allowedFolderIds;
  }
  if (Array.isArray(payload.capabilities)) {
    server.capabilities = payload.capabilities;
  }
  server.updatedAt = nowIso();
  return server;
}

export function testMcpServer(state, tenantId, serverId) {
  const server = state.mcpServers.find((item) => item.tenantId === tenantId && item.id === serverId);
  if (!server) {
    const err = new Error(`MCP server '${serverId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  const hasEndpoint = Boolean(server.endpoint || server.provider === "filesystem");
  const hasAuth = Boolean(server.authRef || server.provider === "filesystem");
  const ok = hasEndpoint && hasAuth;
  server.lastTestedAt = nowIso();
  server.status = ok ? "active" : "error";
  server.updatedAt = nowIso();

  return {
    serverId: server.id,
    status: ok ? "success" : "failed",
    diagnostics: {
      endpoint: hasEndpoint ? "ok" : "missing_endpoint",
      auth: hasAuth ? "ok" : "missing_auth_ref",
      provider: server.provider
    }
  };
}
