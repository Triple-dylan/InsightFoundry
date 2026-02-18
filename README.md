# Firm Data Copilot MVP

A runnable, dependency-light implementation of a codex-style, multi-tenant AI agency platform for firm data.

## What is implemented

- Multi-tenant control plane with tenant creation, blueprint selection, branding config, and autonomy policy.
- Source catalog + source connection lifecycle for common firm systems (DB, warehouse, SaaS) with tenant-scoped secret references.
- Data plane connector sync contract with canonical ingestion, lineage metadata, quality score, run diagnostics, and retry-safe dedupe.
- Hybrid query path with policy-bounded live query broker plus materialization into canonical facts.
- Metric layer query API with day/week/month bucketing and derived metrics (`roas`, `profit`, `runway_days`).
- Intelligence plane model orchestration (`forecast`, `anomaly`) with provider routing (`managed` + BYO preference stub).
- Declarative skill-pack runtime with install/version/activate/deactivate/run lifecycle and guardrails.
- Policy-gated action engine with allowlist, confidence threshold, budget guardrail, kill switch, and approval endpoint.
- Report generation and scheduling APIs with channel delivery stubs for `email`, `slack`, and `telegram`.
- Audit logging across tenant actions.
- Clean web UI with copilot thread, dashboard cards, source setup actions, skill install/run actions, pending actions, and audit panel.

## Run

```bash
npm start
```

Server binds to `127.0.0.1` on a dynamic port by default. Set `PORT` to pin it:

```bash
PORT=8787 npm start
```

## Test

```bash
npm test
```

## API surface

- `POST /v1/tenants`
- `POST /v1/connectors/{provider}/sync`
- `GET /v1/sources/catalog`
- `POST /v1/sources/connections`
- `GET /v1/sources/connections`
- `POST /v1/sources/connections/{connectionId}/test`
- `POST /v1/sources/connections/{connectionId}/sync`
- `GET /v1/sources/connections/{connectionId}/runs`
- `GET /v1/metrics/query`
- `POST /v1/query/live`
- `POST /v1/query/materialize`
- `POST /v1/models/run`
- `GET /v1/skills/catalog`
- `POST /v1/skills/install`
- `GET /v1/skills/installed`
- `POST /v1/skills/run`
- `GET /v1/skills/runs`
- `POST /v1/skills/{skillId}/activate`
- `POST /v1/skills/{skillId}/deactivate`
- `POST /v1/reports/generate`
- `POST /v1/reports/schedules`
- `POST /v1/agents/jobs`
- `POST /v1/agents/actions/approve`
- `GET /v1/audit/events`

Additional operator/UI endpoints:

- `GET /v1/blueprints`
- `GET /v1/tenants`
- `GET /v1/insights/latest`
- `GET /v1/agents/actions/pending`
- `GET /v1/reports`
- `GET /v1/channels/events`

## Request header conventions

Tenant-scoped endpoints require:

- `x-tenant-id`
- `x-user-id`
- `x-user-role` (`owner`, `admin`, `operator`, `analyst`)

## Notes

- This MVP is in-memory (no persistent database yet).
- Credentials are represented as tenant-scoped secret references; raw credential payloads are not stored as plain connection fields.
- Slack/Telegram are implemented as channel event stubs for integration handoff.
- Scheduler is process-local and uses interval-based execution.
