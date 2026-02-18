export function startScheduler(state, onRun) {
  const timer = setInterval(async () => {
    const now = Date.now();
    for (const schedule of state.reportSchedules) {
      if (!schedule.active) continue;
      const due = Date.parse(schedule.nextRunAt);
      if (Number.isNaN(due) || due > now) continue;

      const runKey = `${schedule.id}:${schedule.nextRunAt}`;
      if (state.sentReportRuns.has(runKey)) continue;
      state.sentReportRuns.add(runKey);

      await onRun(schedule);

      schedule.lastRunAt = schedule.nextRunAt;
      schedule.nextRunAt = new Date(now + schedule.intervalMinutes * 60_000).toISOString();
    }
  }, 4_000);

  return () => clearInterval(timer);
}
