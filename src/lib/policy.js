export function evaluateActionPolicy(tenant, action) {
  const policy = tenant.autonomyPolicy;

  if (policy.killSwitch) {
    return { decision: "deny", reason: "kill_switch_enabled" };
  }

  if (!policy.actionAllowlist.includes(action.actionType)) {
    return { decision: "deny", reason: "action_not_allowlisted" };
  }

  if (action.estimatedBudgetImpactUsd > policy.budgetGuardrailUsd) {
    return { decision: "review", reason: "budget_guardrail" };
  }

  if (action.confidence < policy.confidenceThreshold) {
    return { decision: "review", reason: "low_confidence" };
  }

  if (policy.highImpactActions.includes(action.actionType)) {
    return { decision: "review", reason: "high_impact_requires_approval" };
  }

  return { decision: "allow", reason: "policy_allow" };
}

export function canAutopilot(tenant, policyResult) {
  return (
    tenant.autonomyPolicy.autopilotEnabled &&
    tenant.autonomyPolicy.autonomyMode === "policy-gated" &&
    policyResult.decision === "allow"
  );
}
