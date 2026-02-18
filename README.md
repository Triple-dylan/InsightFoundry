# InsightFoundry

InsightFoundry is a multi-tenant AI analysis platform with a codex-style workspace and settings-first operations.

## Planning docs

- Core rehaul direction: `docs/PLAN_ADDENDUM_GLASS_AND_RESEARCH_2026-02-18.md`
- UX workflow benchmarks: `docs/UX_NAV_WORKFLOW_BENCHMARKS_2026-02-18.md`
- UI package/license registry: `docs/THIRD_PARTY_UI_LICENSES.md`

## Product surfaces

- `Workspace`: chat copilot, guided analysis run builder, insight context tabs.
- `Settings`: tenant configuration for connections, credentials, models, training, reports, skills, channels, and policies.
- `Runs`: analysis run timeline, step status, and artifact tracking.

## Implemented backend domains

- Tenant settings APIs (`general`, `model preferences`, `training`, `channels`, `policies`).
- Source connection lifecycle and sync orchestration.
- Live query + materialization APIs.
- Model profile APIs with presets and activation.
- Report type APIs with preview support.
- Skill install/run plus skill draft builder (`create`, `patch`, `validate`, `publish`).
- Deterministic skill compute tools for finance/data quality/deal desk to improve correctness and token efficiency.
- Editable installed skill manifests from Settings (prompt, guardrails, and tool allowlist).
- Guided analysis runs (`create`, `list`, `get`, `execute`, `deliver`).
- MCP server registry in Settings with catalog, tenant-scoped server config, and health testing.
- Quick-add integrations catalog to connect common source/channel/MCP platforms in Settings.
- Folder-scoped agent cowork jobs with shared-thread posting.
- Device command requests with mandatory approval and allowlisted execution.
- Audit trail updates for all major mutations.

## Persistence

- `NODE_ENV=test`: in-memory persistence.
- `DATABASE_URL` present and `pg` available: Postgres snapshot persistence (`insightfoundry_state_snapshots`).
- Otherwise: file persistence at `.runtime/state-snapshot.json`.

## Run

```bash
npm install
npm start
```

By default the server binds to `127.0.0.1` on a dynamic port.

Set a fixed port:

```bash
PORT=8787 npm start
```

## Test

```bash
npm test
```

## Primary APIs

- `GET /v1/settings`
- `PATCH /v1/settings/general`
- `PATCH /v1/settings/model-preferences`
- `PATCH /v1/settings/training`
- `PATCH /v1/settings/policies`
- `GET /v1/settings/channels`
- `PATCH /v1/settings/channels`
- `GET /v1/skills/tools`
- `GET /v1/settings/team`
- `POST /v1/settings/team`
- `PATCH /v1/settings/team/{memberId}`
- `GET /v1/workspace/folders`
- `POST /v1/workspace/folders`
- `PATCH /v1/workspace/folders/{folderId}`
- `GET /v1/workspace/threads`
- `POST /v1/workspace/threads`
- `GET /v1/workspace/threads/{threadId}`
- `GET /v1/workspace/threads/{threadId}/comments`
- `POST /v1/workspace/threads/{threadId}/comments`
- `PATCH /v1/skills/installed/{skillId}`
- `PATCH /v1/sources/connections/{connectionId}`
- `GET /v1/models/profiles`
- `POST /v1/models/profiles`
- `PATCH /v1/models/profiles/{profileId}`
- `POST /v1/models/profiles/{profileId}/activate`
- `GET /v1/reports/types`
- `POST /v1/reports/types`
- `PATCH /v1/reports/types/{typeId}`
- `POST /v1/reports/types/{typeId}/preview`
- `POST /v1/reports/types/{typeId}/delivery-preview`
- `POST /v1/skills/drafts`
- `PATCH /v1/skills/drafts/{draftId}`
- `POST /v1/skills/drafts/{draftId}/validate`
- `POST /v1/skills/drafts/{draftId}/publish`
- `POST /v1/analysis-runs`
- `GET /v1/analysis-runs`
- `GET /v1/analysis-runs/{runId}`
- `POST /v1/analysis-runs/{runId}/execute`
- `POST /v1/analysis-runs/{runId}/deliver`
- `GET /v1/settings/mcp/catalog`
- `GET /v1/settings/mcp/servers`
- `POST /v1/settings/mcp/servers`
- `PATCH /v1/settings/mcp/servers/{serverId}`
- `POST /v1/settings/mcp/servers/{serverId}/test`
- `GET /v1/integrations/catalog`
- `POST /v1/integrations/quick-add`
- `GET /v1/agents/jobs`
- `GET /v1/insights/{insightId}`
- `GET /v1/reports/{reportId}`
- `POST /v1/channels/events/{eventId}/retry`
- `POST /v1/agents/device-commands`
- `GET /v1/agents/device-commands`
- `POST /v1/agents/device-commands/{requestId}/approve`
- `GET /v1/feature-flags`

Legacy and existing APIs remain available for connectors, metrics, models, reports, skills, channels, agents, and audit events.
