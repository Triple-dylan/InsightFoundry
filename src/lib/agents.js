import { newId } from "./state.js";
import { runModelTask } from "./models.js";

function findAction(state, tenantId, actionId) {
  for (const insight of state.insights) {
    if (insight.tenantId !== tenantId) continue;
    const action = insight.recommendedActions.find((candidate) => candidate.id === actionId);
    if (action) return { insight, action };
  }
  return null;
}

export function createAgentJob(state, tenant, payload = {}) {
  const job = {
    id: newId("job"),
    tenantId: tenant.id,
    jobType: payload.jobType ?? "run_model",
    status: "queued",
    payload,
    createdAt: new Date().toISOString(),
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
    job.completedAt = new Date().toISOString();
    job.result = run;
  } else {
    job.status = "completed";
    job.completedAt = new Date().toISOString();
    job.result = { note: "No-op job type" };
  }

  state.agentJobs.push(job);
  return job;
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
  located.action.approvedAt = new Date().toISOString();

  const approval = {
    id: newId("approval"),
    tenantId,
    actionId,
    decision,
    reason: payload.reason ?? "manual_review",
    createdAt: new Date().toISOString()
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
