import { newId } from "./state.js";

function nowIso() {
  return new Date().toISOString();
}

function baseSteps() {
  return [
    { name: "source", status: "pending", detail: "Source freshness check pending" },
    { name: "model", status: "pending", detail: "Model execution pending" },
    { name: "skill", status: "pending", detail: "Skill orchestration optional" },
    { name: "report", status: "pending", detail: "Report generation pending" },
    { name: "delivery", status: "pending", detail: "Channel delivery pending" }
  ];
}

function setStep(run, stepName, status, detail) {
  const step = run.steps.find((item) => item.name === stepName);
  if (!step) return;
  step.status = status;
  if (detail) step.detail = detail;
  step.updatedAt = nowIso();
}

export function createAnalysisRun(state, tenant, payload = {}) {
  const run = {
    id: newId("analysis_run"),
    tenantId: tenant.id,
    status: "draft",
    sourceConnectionId: payload.sourceConnectionId ?? null,
    modelProfileId: payload.modelProfileId ?? null,
    reportTypeId: payload.reportTypeId ?? null,
    skillId: payload.skillId ?? null,
    channels: payload.channels ?? ["email"],
    steps: baseSteps(),
    artifacts: {
      insightId: null,
      reportId: null,
      channelEventIds: []
    },
    timeline: [{ at: nowIso(), event: "run_created", detail: "Draft run created" }],
    createdAt: nowIso(),
    updatedAt: nowIso()
  };

  state.analysisRuns.push(run);
  return run;
}

export function listAnalysisRuns(state, tenantId) {
  return state.analysisRuns.filter((item) => item.tenantId === tenantId);
}

export function requireAnalysisRun(state, tenantId, runId) {
  const run = state.analysisRuns.find((item) => item.tenantId === tenantId && item.id === runId);
  if (!run) {
    const err = new Error(`Analysis run '${runId}' not found`);
    err.statusCode = 404;
    throw err;
  }
  return run;
}

export function executeAnalysisRun(state, tenant, run, adapters = {}, options = {}) {
  if (!run.sourceConnectionId || !run.modelProfileId || !run.reportTypeId) {
    const err = new Error("Run requires sourceConnectionId, modelProfileId, and reportTypeId before execution");
    err.statusCode = 400;
    throw err;
  }

  run.status = "running";
  run.updatedAt = nowIso();
  run.timeline.push({ at: nowIso(), event: "execution_started", detail: "Execution started" });

  try {
    setStep(run, "source", "running", "Checking source freshness");
    const source = adapters.requireSourceConnection(state, tenant.id, run.sourceConnectionId);

    const recentSourceRun = state.sourceConnectionRuns
      .filter((item) => item.tenantId === tenant.id && item.connectionId === source.id)
      .sort((a, b) => (a.createdAt < b.createdAt ? 1 : -1))[0];

    let selectedSourceRun = recentSourceRun ?? null;
    const freshnessSlaHours = Number(source.syncPolicy?.freshnessSlaHours ?? 24);
    const isStale = selectedSourceRun
      ? (Date.now() - new Date(selectedSourceRun.createdAt).getTime()) > freshnessSlaHours * 60 * 60 * 1000
      : true;

    if (!selectedSourceRun || isStale || options.forceSync) {
      const sync = adapters.runSourceSync(state, tenant, source, {
        periodDays: options.periodDays ?? source.syncPolicy?.backfillDays ?? 30
      });
      run.timeline.push({ at: nowIso(), event: "source_synced", detail: `Source sync ${sync.sourceRunId}` });
      selectedSourceRun = state.sourceConnectionRuns.find((item) => item.id === sync.sourceRunId) ?? selectedSourceRun;
    }

    if (!selectedSourceRun) {
      const err = new Error("No source run available after freshness check");
      err.statusCode = 500;
      throw err;
    }

    const qualityScore = Number(selectedSourceRun.diagnostics?.qualityScore ?? 0);
    const minQualityScore = Number(source.qualityPolicy?.minQualityScore ?? 0.75);
    const qualityPassed = selectedSourceRun.diagnostics?.qualityPassed !== false;
    if (source.qualityPolicy?.blockModelRun && (!qualityPassed || qualityScore < minQualityScore)) {
      const err = new Error(
        !qualityPassed
          ? `Source quality gate failed due to quality checks (${qualityScore.toFixed(2)} score)`
          : `Source quality gate failed (${qualityScore.toFixed(2)} < ${minQualityScore.toFixed(2)})`
      );
      err.statusCode = 400;
      throw err;
    }
    setStep(run, "source", "done", `Source ready (quality ${qualityScore.toFixed(2)})`);

    setStep(run, "model", "running", "Running selected model profile");
    const profile = adapters.requireModelProfile(state, tenant.id, run.modelProfileId);
    const model = adapters.runModelTask(state, tenant, {
      objective: profile.objective,
      outputMetricIds: [profile.targetMetricId],
      horizonDays: profile.horizonDays,
      provider: profile.provider
    });
    run.artifacts.insightId = model.insight.id;
    run.timeline.push({ at: nowIso(), event: "model_completed", detail: `Model run ${model.run.id}` });
    setStep(run, "model", "done", `Insight ${model.insight.id}`);

    if (run.skillId) {
      setStep(run, "skill", "running", "Executing attached skill");
      const skillRun = adapters.runSkillPack(state, tenant, {
        skillId: run.skillId,
        intent: "analysis_run",
        channel: "web",
        requestedTools: ["model.run"],
        estimatedTokens: 1200,
        timeoutMs: 3000
      }, {
        runModelTask: adapters.runModelTask,
        generateReport: adapters.generateReport
      });
      run.timeline.push({ at: nowIso(), event: "skill_completed", detail: `Skill run ${skillRun.id}` });
      setStep(run, "skill", "done", `Skill run ${skillRun.id}`);
    } else {
      setStep(run, "skill", "done", "No skill attached");
    }

    setStep(run, "report", "running", "Generating report artifact");
    const reportType = adapters.requireReportType(state, tenant.id, run.reportTypeId);
    const reportResult = adapters.generateReport(state, tenant, {
      title: `${reportType.name} - ${tenant.name}`,
      channels: reportType.defaultChannels,
      format: reportType.defaultFormat,
      metricIds: ["revenue", "profit", "spend"],
      channelTemplates: reportType.deliveryTemplates,
      channelTemplateContext: {
        runId: run.id,
        insightId: run.artifacts.insightId,
        confidence: model.insight.confidence,
        actionsCount: model.insight.recommendedActions.length
      }
    });
    run.artifacts.reportId = reportResult.report.id;
    run.artifacts.channelEventIds.push(...reportResult.deliveryEvents.map((event) => event.id));
    run.timeline.push({ at: nowIso(), event: "report_generated", detail: `Report ${reportResult.report.id}` });
    setStep(run, "report", "done", `Report ${reportResult.report.id}`);

    setStep(run, "delivery", "done", `Delivered to ${reportResult.deliveryEvents.length} channels`);
    run.status = "completed";
    run.updatedAt = nowIso();
    run.timeline.push({ at: nowIso(), event: "execution_completed", detail: "Run completed" });

    return run;
  } catch (error) {
    const activeStep = run.steps.find((step) => step.status === "running");
    if (activeStep) {
      setStep(run, activeStep.name, "error", error.message);
    }
    run.status = "failed";
    run.updatedAt = nowIso();
    run.timeline.push({ at: nowIso(), event: "execution_failed", detail: error.message });
    throw error;
  }
}

export function deliverAnalysisRun(state, tenant, run, adapters = {}, payload = {}) {
  if (!run.artifacts.reportId) {
    const err = new Error("Run has no report artifact to deliver");
    err.statusCode = 400;
    throw err;
  }

  const report = state.reports.find((item) => item.tenantId === tenant.id && item.id === run.artifacts.reportId);
  if (!report) {
    const err = new Error(`Report '${run.artifacts.reportId}' not found`);
    err.statusCode = 404;
    throw err;
  }

  const channels = payload.channels?.length ? payload.channels : run.channels;
  const reportType = state.reportTypes.find((item) => item.tenantId === tenant.id && item.id === run.reportTypeId);
  const insight = run.artifacts?.insightId
    ? state.insights.find((item) => item.tenantId === tenant.id && item.id === run.artifacts.insightId)
    : null;
  const events = adapters.notifyReportDelivery(state, tenant.id, channels, report, {
    templates: reportType?.deliveryTemplates,
    context: {
      runId: run.id,
      insightId: run.artifacts?.insightId ?? null,
      confidence: insight?.confidence ?? null,
      actionsCount: insight?.recommendedActions?.length ?? 0
    }
  });
  run.artifacts.channelEventIds.push(...events.map((event) => event.id));
  run.updatedAt = nowIso();
  run.timeline.push({ at: nowIso(), event: "delivery_completed", detail: `Delivered to ${channels.join(",")}` });

  return {
    run,
    events
  };
}
