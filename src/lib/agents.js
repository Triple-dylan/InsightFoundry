import { execFileSync } from "node:child_process";
import { newId } from "./state.js";
import { runModelTask } from "./models.js";

const SAFE_DEVICE_COMMANDS = new Set(["pwd", "ls", "date", "whoami", "echo"]);

function nowIso() {
  return new Date().toISOString();
}

function findAction(state, tenantId, actionId) {
  for (const insight of state.insights) {
    if (insight.tenantId !== tenantId) continue;
    const action = insight.recommendedActions.find((candidate) => candidate.id === actionId);
    if (action) return { insight, action };
  }
  return null;
}

function normalizeArgs(args) {
  if (!Array.isArray(args)) return [];
  return args.map((value) => String(value).slice(0, 256)).slice(0, 24);
}

function canRunDeviceCommand(command) {
  return SAFE_DEVICE_COMMANDS.has(String(command ?? ""));
}

function runSafeDeviceCommand(payload = {}) {
  const command = String(payload.command ?? "").trim();
  if (!canRunDeviceCommand(command)) {
    return {
      status: "denied",
      error: `Command '${command}' is not allowlisted`,
      output: "",
      exitCode: 126
    };
  }

  try {
    const output = execFileSync(command, normalizeArgs(payload.args), {
      cwd: payload.cwd ? String(payload.cwd) : process.cwd(),
      timeout: Math.max(500, Number(payload.timeoutMs ?? 2500)),
      encoding: "utf8",
      stdio: ["ignore", "pipe", "pipe"]
    });
    return {
      status: "executed",
      error: "",
      output: String(output ?? "").slice(0, 8000),
      exitCode: 0
    };
  } catch (error) {
    return {
      status: "failed",
      error: error.message,
      output: String(error.stdout ?? error.stderr ?? "").slice(0, 8000),
      exitCode: Number(error.status ?? 1)
    };
  }
}

export function createAgentJob(state, tenant, payload = {}, adapters = {}) {
  if (payload.folderId && adapters.requireWorkspaceFolder) {
    adapters.requireWorkspaceFolder(state, tenant.id, payload.folderId);
  }
  if (payload.threadId && adapters.requireWorkspaceThread) {
    adapters.requireWorkspaceThread(state, tenant.id, payload.threadId);
  }

  const job = {
    id: newId("job"),
    tenantId: tenant.id,
    folderId: payload.folderId ?? null,
    threadId: payload.threadId ?? null,
    jobType: payload.jobType ?? "run_model",
    status: "queued",
    payload,
    createdAt: nowIso(),
    completedAt: null
  };

  if (job.jobType === "run_model") {
    const task = {
      objective: payload.objective ?? "forecast",
      outputMetricIds: payload.outputMetricIds ?? ["revenue"],
      inputs: payload.inputs ?? [],
      horizonDays: payload.horizonDays ?? 7
    };
    const run = runModelTask(state, tenant, task);
    job.status = "completed";
    job.completedAt = nowIso();
    job.result = run;
  } else if (job.jobType === "run_skill" && adapters.runSkillPack) {
    const run = adapters.runSkillPack(state, tenant, {
      skillId: payload.skillId,
      intent: payload.intent ?? "agent_cowork",
      channel: payload.channel ?? "web",
      requestedTools: payload.requestedTools ?? [],
      estimatedTokens: Number(payload.estimatedTokens ?? 900),
      timeoutMs: Number(payload.timeoutMs ?? 3000),
      generateReport: Boolean(payload.generateReport)
    }, {
      runModelTask: adapters.runModelTask,
      generateReport: adapters.generateReport
    });
    job.status = "completed";
    job.completedAt = nowIso();
    job.result = run;
  } else if (job.jobType === "cowork_thread") {
    const input = String(payload.input ?? "Summarize latest run context and suggest next action.");
    const summary = `Agent cowork completed in folder ${payload.folderId ?? "n/a"}: ${input}`;
    if (payload.threadId && adapters.createThreadComment) {
      adapters.createThreadComment(state, tenant, {
        threadId: payload.threadId,
        authorId: "agent",
        authorName: "InsightFoundry Agent",
        role: "assistant",
        body: summary
      });
    }
    job.status = "completed";
    job.completedAt = nowIso();
    job.result = { summary };
  } else {
    job.status = "completed";
    job.completedAt = nowIso();
    job.result = { note: "No-op job type" };
  }

  state.agentJobs.push(job);
  return job;
}

export function listAgentJobs(state, tenantId, options = {}) {
  return state.agentJobs
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (options.folderId ? item.folderId === options.folderId : true))
    .filter((item) => (options.threadId ? item.threadId === options.threadId : true))
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function createDeviceCommandRequest(state, tenant, payload = {}, adapters = {}) {
  if (!payload.command) {
    const err = new Error("command is required");
    err.statusCode = 400;
    throw err;
  }
  if (payload.folderId && adapters.requireWorkspaceFolder) {
    adapters.requireWorkspaceFolder(state, tenant.id, payload.folderId);
  }
  if (payload.threadId && adapters.requireWorkspaceThread) {
    adapters.requireWorkspaceThread(state, tenant.id, payload.threadId);
  }

  const command = String(payload.command);
  const request = {
    id: newId("device_cmd"),
    tenantId: tenant.id,
    folderId: payload.folderId ?? null,
    threadId: payload.threadId ?? null,
    command,
    args: normalizeArgs(payload.args),
    cwd: payload.cwd ? String(payload.cwd) : process.cwd(),
    requestedBy: payload.requestedBy ?? "agent",
    requiresApproval: true,
    policyDecision: canRunDeviceCommand(command) ? "review" : "deny",
    policyReason: canRunDeviceCommand(command) ? "device_command_requires_approval" : "command_not_allowlisted",
    status: canRunDeviceCommand(command) ? "pending_approval" : "denied",
    output: "",
    error: canRunDeviceCommand(command) ? "" : `Command '${command}' is not allowlisted`,
    createdAt: nowIso(),
    reviewedAt: null,
    executedAt: null
  };

  state.deviceCommandRequests.push(request);
  return request;
}

export function listDeviceCommandRequests(state, tenantId, options = {}) {
  return state.deviceCommandRequests
    .filter((item) => item.tenantId === tenantId)
    .filter((item) => (options.status ? item.status === options.status : true))
    .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1));
}

export function approveDeviceCommandRequest(state, tenant, payload = {}, adapters = {}) {
  const request = state.deviceCommandRequests.find(
    (item) => item.tenantId === tenant.id && item.id === payload.requestId
  );
  if (!request) {
    const err = new Error(`Device command request '${payload.requestId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  if (!["pending_approval", "approved"].includes(request.status)) {
    const err = new Error(`Device command request '${request.id}' cannot be reviewed from status '${request.status}'`);
    err.statusCode = 400;
    throw err;
  }

  if (tenant.autonomyPolicy.killSwitch) {
    request.status = "denied";
    request.policyDecision = "deny";
    request.policyReason = "kill_switch_enabled";
    request.reviewedAt = nowIso();
    return request;
  }

  const decision = payload.decision === "approve" ? "approve" : "reject";
  request.reviewedAt = nowIso();
  request.reviewedBy = payload.reviewedBy ?? "reviewer";
  request.reviewNote = payload.reason ?? "manual_review";

  if (decision === "reject") {
    request.status = "rejected";
    request.policyDecision = "deny";
    request.policyReason = "manual_rejection";
    return request;
  }

  request.status = "approved";
  request.policyDecision = "allow";
  request.policyReason = "manual_approval";

  const executeNow = Boolean(payload.executeNow ?? true);
  if (!executeNow) return request;

  const result = runSafeDeviceCommand(request);
  request.status = result.status;
  request.output = result.output;
  request.error = result.error;
  request.exitCode = result.exitCode;
  request.executedAt = nowIso();

  if (request.threadId && adapters.createThreadComment) {
    const body = request.status === "executed"
      ? `Command executed: ${request.command} ${request.args.join(" ")}\n\n${request.output || "(no output)"}`
      : `Command execution ${request.status}: ${request.command}\n\n${request.error || request.output || ""}`;
    adapters.createThreadComment(state, tenant, {
      threadId: request.threadId,
      authorId: "agent",
      authorName: "InsightFoundry Agent",
      role: "assistant",
      body
    });
  }
  return request;
}

export function approveAction(state, tenantId, payload) {
  const actionId = payload.actionId;
  const decision = payload.decision;
  const located = findAction(state, tenantId, actionId);

  if (!located) {
    const err = new Error(`Action '${actionId}' not found`);
    err.statusCode = 404;
    throw err;
  }

  located.action.executionState = decision === "approve" ? "executed" : "rejected";
  located.action.approvedAt = nowIso();

  const approval = {
    id: newId("approval"),
    tenantId,
    actionId,
    decision,
    reason: payload.reason ?? "manual_review",
    createdAt: nowIso()
  };

  state.actionApprovals.push(approval);
  return approval;
}

export function listPendingActions(state, tenantId) {
  return state.insights
    .filter((item) => item.tenantId === tenantId)
    .flatMap((insight) => insight.recommendedActions)
    .filter((action) => action.executionState === "pending");
}
