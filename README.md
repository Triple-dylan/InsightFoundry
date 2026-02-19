# InsightFoundry

InsightFoundry is a multi-tenant AI analysis workspace for firm data with collaborative chat, settings-first operations, run orchestration, skills, automations, and policy-gated agent actions.

## Planning docs

- `docs/PLAN_ADDENDUM_GLASS_AND_RESEARCH_2026-02-18.md`
- `docs/UX_NAV_WORKFLOW_BENCHMARKS_2026-02-18.md`
- `docs/THIRD_PARTY_UI_LICENSES.md`
- `docs/threat-model.yaml`

## Core product surfaces

- `Workspace`: shared/private AI chat, mini-threads, attachments, folder/project context.
- `Settings`: team, connections, credentials, models, reports, skills, channels, policies, MCP.
- `Runs`: analysis runs, artifacts, delivery events, automation traces.

## Backend capabilities (current)

- Multi-tenant isolation and role checks.
- Source connection lifecycle (create, patch, test, sync, run history).
- Live query broker + materialization.
- Model profiles + provider routing with failover/cooldown health tracking.
- Report types + delivery preview and retries.
- Skill catalog/registry/install/run + draft validate/publish pipeline.
- Deterministic compute tools for token-efficient finance/deal-desk/data-quality workflows.
- Collaborative chat v2 with shared/private AI and mini-thread behavior.
- Team/workspace settings (agent profile, member appearance, permissions).
- Folder automations + `heartbeat.md` parse/validate/execute.
- Memory system (project memory, user memory, context build, snapshots, `/remember` capture).
- Doctor + security audit endpoints and threat-model access.

## Persistence behavior

- `NODE_ENV=test`: in-memory.
- `DATABASE_URL` set: Postgres snapshot table `insightfoundry_state_snapshots`.
- Default fallback: file snapshot at `.runtime/state-snapshot.json`.

## Local setup

```bash
npm install
cp .env.example .env
npm start
```

Defaults:
- Host: `127.0.0.1`
- Port: dynamic (`PORT=0`) unless overridden

Use a fixed port:

```bash
PORT=8787 npm start
```

## Health and smoke testing

Health endpoint:

```bash
curl -s http://127.0.0.1:8787/healthz
```

Deployment smoke test:

```bash
npm run smoke
# optionally:
# BASE_URL=http://127.0.0.1:8787 npm run smoke
```

`npm run smoke` self-hosts a temporary server when local socket permissions allow it.
If your environment blocks local binds, start the app separately and provide `BASE_URL`.

## Tests

```bash
npm test
```

Current suite status: 21 integration tests covering tenant isolation, runs, skills, chat v2, memory, doctor/security, automations, MCP, and team permissions.

## Deployment-oriented env vars

- `HOST` server bind host.
- `PORT` server bind port.
- `DATABASE_URL` enables Postgres-backed snapshots.
- `STATE_SNAPSHOT_PATH` file persistence path when Postgres is not used.
- `ALLOW_ORIGIN` optional CORS origin for split frontend/backend deployments.
- `APP_VERSION` optional app version surfaced by `/healthz`.

## High-use API groups

- Settings: `/v1/settings/*`, `/v1/settings/team*`, `/v1/settings/workspace-agent`, `/v1/settings/mcp/*`
- Workspace core: `/v1/workspace/folders*`, `/v1/workspace/threads*`, `/v1/workspace/chat/*`, `/v1/workspace/notifications*`
- Memory: `/v1/memory/projects*`, `/v1/memory/users*`, `/v1/memory/context`, `/v1/memory/snapshots*`
- Runs: `/v1/analysis-runs*`, `/v1/reports*`, `/v1/channels/events*`
- Skills: `/v1/skills/catalog`, `/v1/skills/registry*`, `/v1/skills/install`, `/v1/skills/run`, `/v1/skills/drafts*`
- Sources/query: `/v1/sources/*`, `/v1/query/live`, `/v1/query/materialize`
- System hardening: `/v1/system/doctor*`, `/v1/system/security-audits`, `/v1/system/threat-model`, `/v1/models/providers/health`
