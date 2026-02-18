function dateInRange(date, start, end) {
  if (start && date < start) return false;
  if (end && date > end) return false;
  return true;
}

function bucketDate(date, grain) {
  if (grain === "month") return date.slice(0, 7);
  if (grain === "week") {
    const d = new Date(`${date}T00:00:00Z`);
    const day = d.getUTCDay() || 7;
    d.setUTCDate(d.getUTCDate() - day + 1);
    return d.toISOString().slice(0, 10);
  }
  return date;
}

function aggregateByMetric(facts, grain) {
  const map = new Map();
  for (const fact of facts) {
    const bucket = bucketDate(fact.date, grain);
    const key = `${bucket}:${fact.metricId}`;
    map.set(key, (map.get(key) ?? 0) + fact.value);
  }
  return map;
}

function valueFromFormula(metricId, agg, bucket) {
  const get = (id) => agg.get(`${bucket}:${id}`) ?? 0;
  if (metricId === "roas") {
    const spend = get("spend");
    const rev = get("revenue");
    return spend === 0 ? 0 : rev / spend;
  }
  if (metricId === "profit") {
    return get("cash_in") - get("cash_out");
  }
  if (metricId === "runway_days") {
    const burn = get("cash_out");
    const balance = Math.max(0, get("cash_in") - burn);
    return burn === 0 ? 999 : balance / burn * 30;
  }
  return get(metricId);
}

export function queryMetric(state, tenantId, params) {
  const grain = params.grain ?? "day";
  const metricId = params.metricId;
  if (!metricId) {
    const err = new Error("metricId is required");
    err.statusCode = 400;
    throw err;
  }

  const facts = state.facts.filter(
    (item) => item.tenantId === tenantId && dateInRange(item.date, params.startDate, params.endDate)
  );
  const agg = aggregateByMetric(facts, grain);
  const buckets = [...new Set(facts.map((item) => bucketDate(item.date, grain)))].sort();

  const series = buckets.map((bucket) => ({
    bucket,
    value: Number(valueFromFormula(metricId, agg, bucket).toFixed(3))
  }));

  const values = series.map((item) => item.value);
  const total = values.reduce((sum, v) => sum + v, 0);
  const average = values.length ? total / values.length : 0;

  return {
    metricId,
    grain,
    points: series.length,
    series,
    summary: {
      total: Number(total.toFixed(3)),
      average: Number(average.toFixed(3)),
      max: values.length ? Math.max(...values) : 0,
      min: values.length ? Math.min(...values) : 0
    }
  };
}
