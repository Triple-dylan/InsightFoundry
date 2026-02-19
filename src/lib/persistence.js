import fs from "node:fs";
import path from "node:path";

const SNAPSHOT_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS insightfoundry_state_snapshots (
  id INTEGER PRIMARY KEY,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

function serializeState(state) {
  return {
    tenants: [...state.tenants.values()],
    metricsByTenant: [...state.metricsByTenant.entries()],
    facts: state.facts,
    connectorRuns: state.connectorRuns,
    sourceConnections: state.sourceConnections,
    sourceConnectionRuns: state.sourceConnectionRuns,
    secretRefs: [...state.secretRefs.entries()],
    materializationRuns: state.materializationRuns,
    mcpServers: state.mcpServers,
    teamMembers: state.teamMembers,
    teamMemberAppearance: state.teamMemberAppearance,
    workspaceFolders: state.workspaceFolders,
    workspaceThreads: state.workspaceThreads,
    workspaceComments: state.workspaceComments,
    workspaceAgentProfiles: state.workspaceAgentProfiles,
    chatMessages: state.chatMessages,
    messageReactions: state.messageReactions,
    realtimeEvents: state.realtimeEvents,
    workspaceAttachments: state.workspaceAttachments,
    workspaceDocAuth: state.workspaceDocAuth,
    workspaceDocAuthStates: state.workspaceDocAuthStates,
    workspaceDocFiles: state.workspaceDocFiles,
    workspaceDocSessions: state.workspaceDocSessions,
    workspaceTables: state.workspaceTables,
    workspaceTableRows: state.workspaceTableRows,
    notifications: state.notifications,
    projectMemories: state.projectMemories,
    userMemories: state.userMemories,
    memorySnapshots: state.memorySnapshots,
    folderAutomations: state.folderAutomations,
    automationRuns: state.automationRuns,
    heartbeatState: state.heartbeatState,
    installedSkills: state.installedSkills,
    skillDrafts: state.skillDrafts,
    skillRuns: state.skillRuns,
    modelProfiles: state.modelProfiles,
    reportTypes: state.reportTypes,
    reportTemplates: state.reportTemplates,
    analysisRuns: state.analysisRuns,
    modelProviderHealth: [...state.modelProviderHealth.entries()],
    skillRegistry: state.skillRegistry,
    securityAuditRuns: state.securityAuditRuns,
    doctorRuns: state.doctorRuns,
    modelRuns: state.modelRuns,
    insights: state.insights,
    reports: state.reports,
    reportSchedules: state.reportSchedules,
    agentJobs: state.agentJobs,
    deviceCommandRequests: state.deviceCommandRequests,
    actionApprovals: state.actionApprovals,
    auditEvents: state.auditEvents,
    channelEvents: state.channelEvents,
    settingsByTenant: [...state.settingsByTenant.entries()],
    featureFlags: state.featureFlags
  };
}

function hydrateState(state, payload = {}) {
  if (payload.tenants) {
    state.tenants = new Map(payload.tenants.map((item) => [item.id, item]));
  }
  if (payload.metricsByTenant) {
    state.metricsByTenant = new Map(payload.metricsByTenant);
  }

  state.facts = payload.facts ?? state.facts;
  state.connectorRuns = payload.connectorRuns ?? state.connectorRuns;
  state.sourceConnections = payload.sourceConnections ?? state.sourceConnections;
  state.sourceConnectionRuns = payload.sourceConnectionRuns ?? state.sourceConnectionRuns;
  state.secretRefs = new Map(payload.secretRefs ?? []);
  state.materializationRuns = payload.materializationRuns ?? state.materializationRuns;
  state.mcpServers = payload.mcpServers ?? state.mcpServers;
  state.teamMembers = payload.teamMembers ?? state.teamMembers;
  state.teamMemberAppearance = payload.teamMemberAppearance ?? state.teamMemberAppearance;
  state.workspaceFolders = payload.workspaceFolders ?? state.workspaceFolders;
  state.workspaceThreads = payload.workspaceThreads ?? state.workspaceThreads;
  state.workspaceComments = payload.workspaceComments ?? state.workspaceComments;
  state.workspaceAgentProfiles = payload.workspaceAgentProfiles ?? state.workspaceAgentProfiles;
  state.chatMessages = payload.chatMessages ?? state.chatMessages;
  state.messageReactions = payload.messageReactions ?? state.messageReactions;
  state.realtimeEvents = payload.realtimeEvents ?? state.realtimeEvents;
  state.workspaceAttachments = payload.workspaceAttachments ?? state.workspaceAttachments;
  state.workspaceDocAuth = payload.workspaceDocAuth ?? state.workspaceDocAuth;
  state.workspaceDocAuthStates = payload.workspaceDocAuthStates ?? state.workspaceDocAuthStates;
  state.workspaceDocFiles = payload.workspaceDocFiles ?? state.workspaceDocFiles;
  state.workspaceDocSessions = payload.workspaceDocSessions ?? state.workspaceDocSessions;
  state.workspaceTables = payload.workspaceTables ?? state.workspaceTables;
  state.workspaceTableRows = payload.workspaceTableRows ?? state.workspaceTableRows;
  state.notifications = payload.notifications ?? state.notifications;
  state.projectMemories = payload.projectMemories ?? state.projectMemories;
  state.userMemories = payload.userMemories ?? state.userMemories;
  state.memorySnapshots = payload.memorySnapshots ?? state.memorySnapshots;
  state.folderAutomations = payload.folderAutomations ?? state.folderAutomations;
  state.automationRuns = payload.automationRuns ?? state.automationRuns;
  state.heartbeatState = payload.heartbeatState ?? state.heartbeatState;
  state.installedSkills = payload.installedSkills ?? state.installedSkills;
  state.skillDrafts = payload.skillDrafts ?? state.skillDrafts;
  state.skillRuns = payload.skillRuns ?? state.skillRuns;
  state.modelProfiles = payload.modelProfiles ?? state.modelProfiles;
  state.reportTypes = payload.reportTypes ?? state.reportTypes;
  state.reportTemplates = payload.reportTemplates ?? state.reportTemplates;
  state.analysisRuns = payload.analysisRuns ?? state.analysisRuns;
  state.modelProviderHealth = new Map(payload.modelProviderHealth ?? []);
  state.skillRegistry = payload.skillRegistry ?? state.skillRegistry;
  state.securityAuditRuns = payload.securityAuditRuns ?? state.securityAuditRuns;
  state.doctorRuns = payload.doctorRuns ?? state.doctorRuns;
  state.modelRuns = payload.modelRuns ?? state.modelRuns;
  state.insights = payload.insights ?? state.insights;
  state.reports = payload.reports ?? state.reports;
  state.reportSchedules = payload.reportSchedules ?? state.reportSchedules;
  state.agentJobs = payload.agentJobs ?? state.agentJobs;
  state.deviceCommandRequests = payload.deviceCommandRequests ?? state.deviceCommandRequests;
  state.actionApprovals = payload.actionApprovals ?? state.actionApprovals;
  state.auditEvents = payload.auditEvents ?? state.auditEvents;
  state.channelEvents = payload.channelEvents ?? state.channelEvents;
  state.settingsByTenant = new Map(payload.settingsByTenant ?? []);
  state.featureFlags = payload.featureFlags ?? state.featureFlags;

  state.factKeys = new Set(
    state.facts.map((fact) => `${fact.tenantId}:${fact.date}:${fact.domain}:${fact.metricId}:${fact.source}`)
  );
}

class InMemoryPersistence {
  constructor() {
    this.kind = "memory";
  }
  async init() {}
  async load() { return null; }
  async save() {}
}

class FilePersistence {
  constructor(snapshotPath) {
    this.kind = "file";
    this.snapshotPath = snapshotPath;
  }

  async init() {
    const dir = path.dirname(this.snapshotPath);
    fs.mkdirSync(dir, { recursive: true });
  }

  async load() {
    if (!fs.existsSync(this.snapshotPath)) return null;
    return JSON.parse(fs.readFileSync(this.snapshotPath, "utf8"));
  }

  async save(payload) {
    fs.writeFileSync(this.snapshotPath, JSON.stringify(payload), "utf8");
  }
}

class PostgresPersistence {
  constructor(databaseUrl) {
    this.kind = "postgres";
    this.databaseUrl = databaseUrl;
    this.client = null;
  }

  async init() {
    const { Client } = await import("pg");
    this.client = new Client({ connectionString: this.databaseUrl });
    await this.client.connect();
    await this.client.query(SNAPSHOT_TABLE_SQL);
  }

  async load() {
    const result = await this.client.query("SELECT payload FROM insightfoundry_state_snapshots WHERE id = 1");
    if (!result.rows.length) return null;
    return result.rows[0].payload;
  }

  async save(payload) {
    await this.client.query(
      `INSERT INTO insightfoundry_state_snapshots (id, payload, updated_at)
       VALUES (1, $1::jsonb, NOW())
       ON CONFLICT (id)
       DO UPDATE SET payload = EXCLUDED.payload, updated_at = NOW()`,
      [JSON.stringify(payload)]
    );
  }
}

export async function createPersistence() {
  const isTestMode =
    process.env.NODE_ENV === "test" ||
    process.argv.includes("--test") ||
    Boolean(process.env.JEST_WORKER_ID) ||
    Boolean(process.env.VITEST);

  if (isTestMode) {
    const p = new InMemoryPersistence();
    await p.init();
    return p;
  }

  if (process.env.DATABASE_URL) {
    try {
      const p = new PostgresPersistence(process.env.DATABASE_URL);
      await p.init();
      return p;
    } catch (error) {
      console.warn("Postgres persistence unavailable, falling back to file persistence:", error.message);
    }
  }

  const snapshotPath = process.env.STATE_SNAPSHOT_PATH ?? path.join(process.cwd(), ".runtime", "state-snapshot.json");
  const p = new FilePersistence(snapshotPath);
  await p.init();
  return p;
}

export async function loadStateFromPersistence(state, persistence) {
  const payload = await persistence.load();
  if (!payload) return false;
  hydrateState(state, payload);
  return true;
}

export async function saveStateToPersistence(state, persistence) {
  await persistence.save(serializeState(state));
}
