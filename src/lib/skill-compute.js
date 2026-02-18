function toNumber(value, fallback = 0) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function round(value, digits = 3) {
  const factor = 10 ** digits;
  return Math.round(toNumber(value) * factor) / factor;
}

function startDay(days) {
  const d = new Date();
  d.setUTCHours(0, 0, 0, 0);
  d.setUTCDate(d.getUTCDate() - (days - 1));
  return d.toISOString().slice(0, 10);
}

function factsForTenant(state, tenantId, sinceDate = null) {
  return state.facts.filter((item) => item.tenantId === tenantId && (!sinceDate || item.date >= sinceDate));
}

function sumMetric(facts, metricId) {
  return facts
    .filter((item) => item.metricId === metricId)
    .reduce((sum, item) => sum + toNumber(item.value), 0);
}

function seriesByMetricDay(facts, metricId) {
  const map = new Map();
  for (const fact of facts) {
    if (fact.metricId !== metricId) continue;
    map.set(fact.date, (map.get(fact.date) ?? 0) + toNumber(fact.value));
  }
  return [...map.entries()]
    .sort((a, b) => (a[0] > b[0] ? 1 : -1))
    .map(([date, value]) => ({ date, value }));
}

function average(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function slope(series) {
  if (series.length < 2) return 0;
  return (series[series.length - 1].value - series[0].value) / (series.length - 1);
}

function percentDelta(newValue, oldValue) {
  if (!oldValue) return 0;
  return (newValue - oldValue) / Math.abs(oldValue);
}

function financeSnapshot(state, tenantId, options = {}) {
  const horizonDays = Math.max(7, toNumber(options.horizonDays, 30));
  const sinceDate = startDay(horizonDays);
  const facts = factsForTenant(state, tenantId, sinceDate);
  const cashIn = sumMetric(facts, "cash_in");
  const cashOut = sumMetric(facts, "cash_out");
  const revenue = sumMetric(facts, "revenue") || cashIn;
  const spend = sumMetric(facts, "spend") || cashOut;
  const profit = revenue - spend;
  const grossMarginPct = revenue > 0 ? profit / revenue : 0;
  const burnRateDaily = cashOut > 0 ? cashOut / horizonDays : spend / horizonDays;
  const netCashDaily = (cashIn - cashOut) / horizonDays;
  const projectedRunwayDays = burnRateDaily <= 0 ? 999 : Math.max(0, (cashIn - cashOut) / burnRateDaily);
  const revenueSeries = seriesByMetricDay(facts, "revenue");
  const spendSeries = seriesByMetricDay(facts, "spend");

  return {
    scope: { horizonDays, sinceDate },
    kpis: {
      revenue: round(revenue, 2),
      spend: round(spend, 2),
      cashIn: round(cashIn, 2),
      cashOut: round(cashOut, 2),
      profit: round(profit, 2),
      grossMarginPct: round(grossMarginPct, 4),
      burnRateDaily: round(burnRateDaily, 2),
      netCashDaily: round(netCashDaily, 2),
      projectedRunwayDays: round(projectedRunwayDays, 1)
    },
    trends: {
      revenueSlopePerDay: round(slope(revenueSeries), 3),
      spendSlopePerDay: round(slope(spendSeries), 3)
    }
  };
}

function dataQualitySnapshot(state, tenantId, options = {}) {
  const horizonDays = Math.max(7, toNumber(options.horizonDays, 30));
  const sinceDate = startDay(horizonDays);
  const facts = factsForTenant(state, tenantId, sinceDate);
  const byMetric = new Map();
  for (const fact of facts) {
    if (!byMetric.has(fact.metricId)) byMetric.set(fact.metricId, []);
    byMetric.get(fact.metricId).push(fact);
  }

  const metrics = [...byMetric.entries()].map(([metricId, metricFacts]) => {
    const seenDays = new Set(metricFacts.map((item) => item.date));
    const nonNumeric = metricFacts.filter((item) => !Number.isFinite(Number(item.value))).length;
    const nullish = metricFacts.filter((item) => item.value == null).length;
    const completeness = horizonDays > 0 ? seenDays.size / horizonDays : 0;
    return {
      metricId,
      rows: metricFacts.length,
      daysCovered: seenDays.size,
      completenessPct: round(completeness, 4),
      nullishRows: nullish,
      nonNumericRows: nonNumeric
    };
  });

  const completenessAvg = average(metrics.map((item) => item.completenessPct));
  const nullishRows = metrics.reduce((sum, item) => sum + item.nullishRows, 0);
  const nonNumericRows = metrics.reduce((sum, item) => sum + item.nonNumericRows, 0);
  const score = Math.max(
    0,
    1 - (1 - completenessAvg) * 0.7 - Math.min(1, (nullishRows + nonNumericRows) / Math.max(1, facts.length)) * 0.3
  );

  return {
    scope: { horizonDays, sinceDate },
    summary: {
      totalRows: facts.length,
      metricCount: metrics.length,
      completenessAvg: round(completenessAvg, 4),
      nullishRows,
      nonNumericRows,
      qualityScore: round(score, 4)
    },
    metrics
  };
}

function dealDeskSnapshot(payload = {}, options = {}) {
  const deal = payload.deal ?? {};
  const policy = {
    maxDiscountPct: toNumber(options.maxDiscountPct, 0.2),
    minMarginPct: toNumber(options.minMarginPct, 0.35),
    maxPaymentTermsDays: toNumber(options.maxPaymentTermsDays, 60),
    minWinProbability: toNumber(options.minWinProbability, 0.55),
    ...(payload.policy ?? {})
  };

  const listPrice = toNumber(deal.listPrice ?? deal.amount, 0);
  const proposedPrice = toNumber(deal.proposedPrice ?? deal.amount, listPrice);
  const cogs = toNumber(deal.cogs, proposedPrice * 0.5);
  const paymentTermsDays = toNumber(deal.paymentTermsDays, 30);
  const winProbability = toNumber(deal.winProbability, 0.5);
  const legalRiskScore = toNumber(deal.legalRiskScore, 0.2);
  const termMonths = Math.max(1, toNumber(deal.termMonths, 12));

  const discountPct = listPrice > 0 ? (listPrice - proposedPrice) / listPrice : 0;
  const marginPct = proposedPrice > 0 ? (proposedPrice - cogs) / proposedPrice : 0;
  const annualValue = proposedPrice * (12 / termMonths);
  const weightedAnnualValue = annualValue * winProbability;
  const paymentTermRisk = Math.max(0, (paymentTermsDays - policy.maxPaymentTermsDays) / policy.maxPaymentTermsDays);

  const flags = [];
  if (discountPct > policy.maxDiscountPct) flags.push("discount_above_policy");
  if (marginPct < policy.minMarginPct) flags.push("margin_below_policy");
  if (paymentTermsDays > policy.maxPaymentTermsDays) flags.push("payment_terms_above_policy");
  if (winProbability < policy.minWinProbability) flags.push("low_win_probability");
  if (legalRiskScore >= 0.6) flags.push("high_legal_risk");

  return {
    policy,
    deal: {
      listPrice: round(listPrice, 2),
      proposedPrice: round(proposedPrice, 2),
      cogs: round(cogs, 2),
      termMonths,
      paymentTermsDays,
      winProbability: round(winProbability, 4),
      legalRiskScore: round(legalRiskScore, 4)
    },
    analysis: {
      discountPct: round(discountPct, 4),
      marginPct: round(marginPct, 4),
      annualValue: round(annualValue, 2),
      weightedAnnualValue: round(weightedAnnualValue, 2),
      paymentTermRisk: round(paymentTermRisk, 4),
      approvalRequired: flags.length > 0,
      riskFlags: flags
    }
  };
}

export const SKILL_TOOL_CATALOG = [
  { id: "compute.finance_snapshot", domain: "finance", description: "Deterministic finance KPI pack (margin, burn, runway, trend)." },
  { id: "compute.data_quality_snapshot", domain: "data", description: "Deterministic quality pack (coverage, nulls, quality score)." },
  { id: "compute.deal_desk_snapshot", domain: "deal_desk", description: "Deterministic deal desk checks (discount, margin, approvals)." },
  { id: "model.run", domain: "model", description: "Model execution for forecast/anomaly/segmentation tasks." },
  { id: "reports.generate", domain: "reporting", description: "Report artifact generation and optional channel delivery." },
  { id: "sources.sync", domain: "data", description: "Run source sync for freshness." },
  { id: "notify.owner", domain: "ops", description: "Notify owners through configured channels." }
];

export function listSkillToolCatalog() {
  return SKILL_TOOL_CATALOG;
}

export function runDeterministicSkillTool(state, tenant, toolId, payload = {}) {
  if (toolId === "compute.finance_snapshot") {
    return financeSnapshot(state, tenant.id, payload.options);
  }
  if (toolId === "compute.data_quality_snapshot") {
    return dataQualitySnapshot(state, tenant.id, payload.options);
  }
  if (toolId === "compute.deal_desk_snapshot") {
    return dealDeskSnapshot(payload, payload.options);
  }
  return null;
}
