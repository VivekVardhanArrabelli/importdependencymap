# Build for India

Build for India helps identify domestic manufacturing opportunities by tracking imports, opportunity scores, and partner concentration for each HS code. The stack pairs a FastAPI backend, Postgres analytics tables, and a lightweight static client served from the same origin.

## Requirements
- Python 3.10+
- Postgres database (Railway recommended)
- Environment variables:
  - `DATABASE_URL` – Postgres URL (`?sslmode=require` for managed DBs)
  - `ADMIN_KEY` – bearer token for admin routes
  - `COMTRADE_BASE` (optional) – defaults to `https://comtradeapi.un.org/public/v1/preview`
  - `COMTRADE_FLOW` (default `import`), `COMTRADE_REPORTER` (default `India`), `COMTRADE_FREQ` (default `M`)
  - Optional observability keys: `SENTRY_DSN`, `GA_MEASUREMENT_ID`

Copy `.env.example`, fill the values, and never commit live secrets.

## Local Development
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
uvicorn server.main:app --host 0.0.0.0 --port 8000
```

The API serves the static client at `/` when `client/` exists. Alternatively run `python -m http.server --directory client 8080` during development.

### Seeding & Metrics
```bash
export ADMIN_KEY=dev-secret
./scripts/seed_local.sh http://localhost:8000
./scripts/recompute_local.sh http://localhost:8000
```
These scripts call the protected admin endpoints, populating sample HS data from `data/top100_hs.csv` and recomputing baselines/opportunity scores.

### Database Requirement
`DATABASE_URL` must be configured. Admin endpoints and API queries fail fast when the database is unreachable to prevent serving stale placeholder data.

## ETL & Automation
- `POST /admin/etl/comtrade?from=YYYY-MM&to=YYYY-MM` downloads monthly UN Comtrade data (HS6), upserts products/imports, and recomputes metrics.
- `.github/workflows/ci.yml` installs dependencies, runs `python -m compileall server`, and executes tests.
- `.github/workflows/nightly_etl.yml` triggers the Comtrade ETL and recompute every night at 03:00 UTC. Configure repository secrets `ADMIN_KEY` and `DEPLOY_URL`.

## Testing
- Add tests under `tests/` using `pytest` (fallback to `python -m unittest discover`).
- Run `pytest` locally before opening a PR.
- Ensure `python -m compileall server` passes to catch syntax issues.

## Deployment (Railway)
1. Provision a Railway service with Postgres.
2. Set environment variables above in **Project → Variables**.
3. Use the start command: `uvicorn server.main:app --host 0.0.0.0 --port $PORT`.
4. Optionally create a nightly job to hit `/admin/etl/comtrade` and `/admin/recompute` (see workflow for reference).

Seed or refresh data after deployment:
```bash
curl -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "$RAILWAY_URL/admin/seed"
```

## API Surface
- `GET /health`
- `POST /admin/seed`
- `POST /admin/etl/comtrade`
- `POST /admin/recompute`
- `GET /api/products`
- `GET /api/products/{hs}`
- `GET /api/leaderboard`
- `POST /api/domestic_capability`
- `GET /api/domestic_capability/{hs}`

Responses include `source` and `last_updated` metadata so the client can communicate freshness.

## Donations
Support future roadmap items (DGCI&S ETL, policy overlays, supplier registry) via:
- GitHub Sponsors – coming soon
- Open Collective – coming soon
