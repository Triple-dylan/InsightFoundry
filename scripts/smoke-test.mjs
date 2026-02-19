import { createPlatform } from "../src/app.js";

let BASE_URL = process.env.BASE_URL ?? "";
let embeddedPlatform = null;

async function ensureBaseUrl() {
  if (BASE_URL) return;
  embeddedPlatform = await createPlatform({ seedDemo: false, startBackground: false });
  try {
    await new Promise((resolve, reject) => {
      embeddedPlatform.server.once("error", reject);
      embeddedPlatform.server.listen(0, "127.0.0.1", resolve);
    });
  } catch (error) {
    await teardownBaseUrl();
    const err = new Error(
      `Unable to self-host smoke server (${error.code ?? "unknown_error"}). ` +
      "Start InsightFoundry separately and set BASE_URL."
    );
    err.cause = error;
    throw err;
  }
  const addr = embeddedPlatform.server.address();
  BASE_URL = `http://127.0.0.1:${addr.port}`;
}

async function teardownBaseUrl() {
  if (!embeddedPlatform) return;
  embeddedPlatform.close();
  await new Promise((resolve) => embeddedPlatform.server.close(() => resolve()));
}

async function api(path, init = {}) {
  const res = await fetch(`${BASE_URL}${path}`, init);
  let body = null;
  try {
    body = await res.json();
  } catch {
    body = null;
  }
  return { res, body };
}

function requireStatus(status, expected, label, body) {
  if (status !== expected) {
    const detail = body ? JSON.stringify(body) : "";
    throw new Error(`${label} failed: expected ${expected}, got ${status}. ${detail}`);
  }
}

async function main() {
  await ensureBaseUrl();
  const health = await api("/healthz");
  requireStatus(health.res.status, 200, "health check", health.body);
  console.log(`[smoke] health ok, persistence=${health.body?.persistence ?? "unknown"}`);

  const createTenant = await api("/v1/tenants", {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({
      name: `Smoke Tenant ${new Date().toISOString()}`
    })
  });
  requireStatus(createTenant.res.status, 201, "tenant create", createTenant.body);
  const tenantId = createTenant.body.tenant.id;
  const headers = {
    "content-type": "application/json",
    "x-tenant-id": tenantId,
    "x-user-id": "smoke-tester",
    "x-user-role": "owner"
  };
  console.log(`[smoke] tenant created: ${tenantId}`);

  const settings = await api("/v1/settings", { headers });
  requireStatus(settings.res.status, 200, "settings fetch", settings.body);

  const folders = await api("/v1/workspace/folders", { headers });
  requireStatus(folders.res.status, 200, "workspace folders", folders.body);
  const folderId = folders.body.folders[0].id;

  const threads = await api(`/v1/workspace/threads?folderId=${encodeURIComponent(folderId)}`, { headers });
  requireStatus(threads.res.status, 200, "workspace threads", threads.body);
  const threadId = threads.body.threads[0].id;

  const remember = await api(`/v1/workspace/chat/threads/${threadId}/messages`, {
    method: "POST",
    headers,
    body: JSON.stringify({
      body: "/remember Smoke test remembers this preference.",
      visibility: "shared"
    })
  });
  requireStatus(remember.res.status, 201, "remember message", remember.body);
  if (!remember.body.memoryCapture) {
    throw new Error("remember command did not create a memory entry");
  }

  const memoryContext = await api(`/v1/memory/context?threadId=${encodeURIComponent(threadId)}&limit=5`, { headers });
  requireStatus(memoryContext.res.status, 200, "memory context", memoryContext.body);
  if (!Array.isArray(memoryContext.body.context?.merged) || memoryContext.body.context.merged.length === 0) {
    throw new Error("memory context is empty after remember command");
  }

  const doctor = await api("/v1/system/doctor", {
    method: "POST",
    headers,
    body: JSON.stringify({ applyFixes: false })
  });
  requireStatus(doctor.res.status, 200, "doctor run", doctor.body);
  console.log(`[smoke] doctor status=${doctor.body.run.status}`);

  const providersHealth = await api("/v1/models/providers/health", { headers });
  requireStatus(providersHealth.res.status, 200, "provider health", providersHealth.body);

  const registry = await api("/v1/skills/registry", { headers });
  requireStatus(registry.res.status, 200, "skills registry", registry.body);
  if (!Array.isArray(registry.body.skills) || registry.body.skills.length === 0) {
    throw new Error("skills registry is empty");
  }

  console.log("[smoke] all checks passed");
}

main().catch((error) => {
  console.error(`[smoke] failed: ${error.message}`);
  process.exitCode = 1;
}).finally(async () => {
  await teardownBaseUrl();
});
