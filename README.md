# Build for India

FastAPI service and minimal client to explore India's import dependency opportunities. The project ships with Postgres schema management, seeding utilities, and heuristics to prioritise HS codes.

## Requirements

- Python 3.10+
- Postgres database (Railway, Supabase, local Docker, etc.)
- Environment variables: `DATABASE_URL`, `ADMIN_KEY`

Copy `.env.example` into `.env` and fill in your credentials for local development.

## Installation

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Running locally

1. Export `DATABASE_URL` and `ADMIN_KEY` (or load them from `.env` via `python-dotenv`).
2. Ensure the target Postgres database exists.
3. Start the API:

```bash
uvicorn server.main:app --host 0.0.0.0 --port 8000
```

4. Serve the minimal client in `client/index.html` with a lightweight static server (for example, `python -m http.server`) and proxy API calls to your FastAPI instance.

## Database seeding

Trigger the seed job after deploying or running locally (replace host and admin key):

```bash
curl -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  https://your-hostname/admin/seed
```

Running the seed endpoint is idempotent: products are upserted and monthly imports updated. The handler also recomputes baselines and progress metrics.

## API overview

- `GET /health` – readiness probe.
- `POST /admin/seed` – protected seed job. Creates tables, loads sample HS codes, recomputes metrics.
- `POST /admin/recompute` – protected recompute of baselines and opportunity scores.
- `GET /api/products` – filterable list of product cards with opportunity data.
- `GET /api/products/{hs}` – product detail, 36-month series, top partners, progress metrics.
- `GET /api/leaderboard` – top HS codes by opportunity, progress, or value.
- `POST /api/domestic_capability` – protected upsert of capability inputs (flagged as unverified).
- `GET /api/domestic_capability/{hs}` – public, verified capability entries.

All responses include `source` and `last_updated` metadata where applicable, and empty datasets return empty arrays instead of errors.

## Railway deployment

1. Create a new Railway project and add a Postgres plugin.
2. Set `DATABASE_URL` to the Railway Postgres connection string and choose a secure `ADMIN_KEY`.
3. Use the provided start command: `uvicorn server.main:app --host 0.0.0.0 --port $PORT`.
4. Deploy.

### Post-deploy checks

- Seed: `curl -X POST -H "Authorization: Bearer $ADMIN_KEY" https://<your-railway-url>/admin/seed`
- Verify products: `https://<your-railway-url>/api/products`
- Verify detail: `https://<your-railway-url>/api/products/850440`

## Donations

Support future iterations via:

- GitHub Sponsors (coming soon)
- Open Collective (coming soon)
