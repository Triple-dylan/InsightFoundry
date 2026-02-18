# InsightFoundry

InsightFoundry is a multi-tenant AI analysis platform with a codex-style workspace and settings-first operations.

## Planning docs

- Core rehaul direction: `docs/PLAN_ADDENDUM_GLASS_AND_RESEARCH_2026-02-18.md`
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
- Guided analysis runs (`create`, `list`, `get`, `execute`, `deliver`).
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
- `GET /v1/insights/{insightId}`
- `GET /v1/reports/{reportId}`
- `POST /v1/channels/events/{eventId}/retry`
- `GET /v1/feature-flags`

Legacy and existing APIs remain available for connectors, metrics, models, reports, skills, channels, agents, and audit events.
