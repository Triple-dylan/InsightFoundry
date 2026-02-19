# Deployment Handoff Notes

## Runtime prerequisites

- Node.js `>=20`
- Optional Postgres (if using `DATABASE_URL`)

## Minimal startup

```bash
npm install
cp .env.example .env
PORT=8787 npm start
```

## Recommended environment strategy

- `development`: file snapshots (`.runtime/state-snapshot.json`)
- `staging`: Postgres snapshots (`DATABASE_URL`)
- `production`: Postgres snapshots + explicit `ALLOW_ORIGIN` + fixed `APP_VERSION`

## Fast validation flow after deploy

1. `GET /healthz` returns `ok=true`.
2. Run `npm run smoke` from a runner with `BASE_URL` set to deployed URL.
3. Confirm smoke output includes:
   - tenant creation
   - memory capture and memory context retrieval
   - doctor run completion
   - skill registry availability

## Operational checks

- Monitor `/v1/models/providers/health` for repeated failover and cooldown patterns.
- Monitor `/v1/system/doctor/runs` and `/v1/system/security-audits` for warning/fail trends.
- Keep `.runtime/` excluded from source control.

## Known current constraints

- Persistence is snapshot-style (single logical state payload), not normalized relational tables yet.
- Secrets are represented as references in app state; external secret manager wiring can be strengthened in next phase.
- CORS is single-origin via `ALLOW_ORIGIN` and is disabled by default.
