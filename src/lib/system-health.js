import fs from "node:fs";
import path from "node:path";
import { newId } from "./state.js";
import { createWorkspaceFolder } from "./collaboration.js";
import { ensureTenantSettings } from "./settings.js";

function nowIso() {
  return new Date().toISOString();
}

const THREAT_MODEL_PATH = path.join(process.cwd(), "docs", "threat-model.yaml");

function addCheck(checks, key, status, detail) {
  checks.push({ key, status, detail });
}

function hasActiveOwner(state, tenantId) {
  return state.teamMembers.some(
    (member) => member.tenantId === tenantId && member.role === "owner" && member.status !== "inactive"
  );
}

function sourceCoverage(state, tenantId) {
  const connections = state.sourceConnections.filter((item) => item.tenantId === tenantId);
  return {
    count: connections.length,
    withCredentials: connections.filter((item) => item.authRef).length
  };
}

function validateAutomations(state, tenantId) {
  const automations = state.folderAutomations.filter((item) => item.tenantId === tenantId);
  const invalid = automations.filter((automation) => {
    const folderOk = state.workspaceFolders.some((folder) => folder.tenantId === tenantId && folder.id === automation.folderId);
    const threadOk = !automation.targetThreadId
      || state.workspaceThreads.some((thread) => thread.tenantId === tenantId && thread.id === automation.targetThreadId);
    return !folderOk || !threadOk;
  });
  return {
    total: automations.length,
    invalidCount: invalid.length
  };
}

function runSecurityAudit(state, tenant) {
  const checks = [];
  const settings = ensureTenantSettings(state, tenant);
  const privateMessageCount = state.chatMessages.filter(
    (message) => message.tenantId === tenant.id && message.visibility === "private"
  ).length;
  const mcpExternal = state.mcpServers
    .filter((server) => server.tenantId === tenant.id)
    .filter((server) => server.endpoint && !String(server.endpoint).includes("127.0.0.1") && !String(server.endpoint).includes("localhost"))
    .length;
  const webhookConfigured = Boolean(settings.channels?.slack?.webhookRef || settings.channels?.telegram?.botTokenRef);

  addCheck(checks, "kill_switch_default", tenant.autonomyPolicy?.killSwitch ? "warn" : "pass", tenant.autonomyPolicy?.killSwitch
    ? "Tenant kill switch is enabled (safe but blocks autonomous value)"
    : "Kill switch disabled");
  addCheck(checks, "private_ai_visibility", privateMessageCount > 0 ? "pass" : "warn", privateMessageCount > 0
    ? `Private message pathways exercised (${privateMessageCount})`
    : "No private AI exchanges yet; test privacy workflows");
  addCheck(checks, "mcp_endpoint_scope", mcpExternal > 0 ? "warn" : "pass", mcpExternal > 0
    ? `${mcpExternal} MCP servers use non-local endpoints`
    : "MCP endpoints local-only or unset");
  addCheck(checks, "channel_credentials", webhookConfigured ? "pass" : "warn", webhookConfigured
    ? "At least one channel credential configured"
    : "No outbound channel credentials configured");

  const run = {
    id: newId("security_audit"),
    tenantId: tenant.id,
    checks,
    status: checks.some((item) => item.status === "warn" || item.status === "fail") ? "warn" : "pass",
    createdAt: nowIso()
  };
  state.securityAuditRuns.push(run);
  return run;
}

export function runDoctor(state, tenant, options = {}) {
  const applyFixes = Boolean(options.applyFixes);
  const checks = [];
  const fixes = [];

  const settings = ensureTenantSettings(state, tenant);
  addCheck(checks, "settings_present", settings ? "pass" : "fail", settings ? "Tenant settings found" : "Tenant settings missing");

  if (!hasActiveOwner(state, tenant.id)) {
    addCheck(checks, "active_owner", "fail", "No active owner user found");
  } else {
    addCheck(checks, "active_owner", "pass", "Active owner present");
  }

  const sourceStats = sourceCoverage(state, tenant.id);
  if (sourceStats.count === 0) {
    addCheck(checks, "sources_connected", "warn", "No source connections configured");
  } else if (sourceStats.withCredentials < sourceStats.count) {
    addCheck(checks, "source_credentials", "warn", "Some source connections missing credential refs");
  } else {
    addCheck(checks, "source_credentials", "pass", `${sourceStats.count} source connections credentialed`);
  }

  const automationStats = validateAutomations(state, tenant.id);
  if (automationStats.invalidCount > 0) {
    addCheck(checks, "automation_integrity", "warn", `${automationStats.invalidCount} automations reference missing folder/thread`);
  } else {
    addCheck(checks, "automation_integrity", "pass", "Folder automations are linked to valid workspace entities");
  }

  const profileCount = state.modelProfiles.filter((item) => item.tenantId === tenant.id).length;
  addCheck(checks, "model_profiles", profileCount > 0 ? "pass" : "warn", profileCount > 0
    ? `${profileCount} model profiles configured`
    : "No model profiles configured");

  const memoryCount = state.projectMemories.filter((item) => item.tenantId === tenant.id).length;
  const userMemoryCount = state.userMemories.filter((item) => item.tenantId === tenant.id).length;
  addCheck(checks, "memory_coverage", (memoryCount + userMemoryCount) > 0 ? "pass" : "warn", (memoryCount + userMemoryCount) > 0
    ? `Memory entries present (project=${memoryCount}, user=${userMemoryCount})`
    : "No memory entries captured yet");

  const threatModelExists = fs.existsSync(THREAT_MODEL_PATH);
  addCheck(checks, "threat_model", threatModelExists ? "pass" : "warn", threatModelExists
    ? "Threat model file present"
    : `Threat model file missing at ${THREAT_MODEL_PATH}`);

  if (applyFixes) {
    if (!sourceStats.count) {
      fixes.push("No sources to auto-fix.");
    }
    if (state.workspaceFolders.filter((folder) => folder.tenantId === tenant.id).length === 0) {
      createWorkspaceFolder(state, tenant, {
        name: "General",
        description: "Created by doctor",
        createdBy: "doctor"
      });
      fixes.push("Created default 'General' folder.");
    }
  }

  const audit = runSecurityAudit(state, tenant);
  const run = {
    id: newId("doctor"),
    tenantId: tenant.id,
    checks,
    fixes,
    securityAuditRunId: audit.id,
    status: checks.some((item) => item.status === "fail")
      ? "fail"
      : checks.some((item) => item.status === "warn")
        ? "warn"
        : "pass",
    createdAt: nowIso()
  };
  state.doctorRuns.push(run);
  return { run, securityAudit: audit };
}

export function listDoctorRuns(state, tenantId) {
  return state.doctorRuns
    .filter((item) => item.tenantId === tenantId)
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function listSecurityAuditRuns(state, tenantId) {
  return state.securityAuditRuns
    .filter((item) => item.tenantId === tenantId)
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function loadThreatModel() {
  if (!fs.existsSync(THREAT_MODEL_PATH)) {
    return {
      path: THREAT_MODEL_PATH,
      exists: false,
      content: ""
    };
  }
  return {
    path: THREAT_MODEL_PATH,
    exists: true,
    content: fs.readFileSync(THREAT_MODEL_PATH, "utf8")
  };
}

