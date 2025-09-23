# Build for India

Build for India helps identify domestic manufacturing opportunities by tracking imports, opportunity scores, and partner concentration for each HS code. The stack pairs a FastAPI backend, Postgres analytics tables, and a lightweight static client served from the same origin. Live trade data is sourced from the UN Comtrade API (HS6 monthly series for India), and the roadmap includes direct DGCI&S/Tradestat, WITS, and data.gov.in integrations.

## Requirements
- Python 3.10+
- Postgres database (Railway recommended)
- Environment variables:
  - `DATABASE_URL` – Postgres URL (`?sslmode=require` for managed DBs)
  - `ADMIN_KEY` – bearer token for admin routes
  - `COMTRADE_BASE` (optional) – defaults to `https://comtradeapi.un.org/public/v1/preview`
  - `COMTRADE_FLOW` (default `import`), `COMTRADE_REPORTER` (default `India`), `COMTRADE_FREQ` (default `M`)
  - `FX_RATES_FILE` – path to monthly USD→INR CSV (defaults to `data/fx_rates.csv`)
  - `DGCIS_DEFAULT_PATH` – optional default path for DGCI&S CSV exports (`data/dgcis_latest.csv`)
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
./scripts/load_dgcis.sh data/dgcis_sample.csv http://localhost:8000
```
These scripts call the protected admin endpoints, populating sample HS data from `data/top100_hs.csv`, ingesting DGCI&S CSV exports, and recomputing baselines/opportunity scores.

### Database Requirement
`DATABASE_URL` must be configured. Admin endpoints and API queries fail fast when the database is unreachable to prevent serving stale placeholder data.

### Seed CSV (optional)
`data/top100_hs.csv` contains a curated starter list that can be ingested via `/admin/seed` after the database connection is working. It is purely a bootstrap aid and is not used automatically.

## Data Sources
- **DGCI&S / Tradestat** – official monthly import/export databank for India; scheduled integration will ingest HS-level CSV/API exports (`tradestat.commerce.gov.in`).
- **UN Comtrade** – currently integrated live source providing partner-country flows and historical HS trade series.
- **WITS / World Bank** – planned source for tariff, duty, and policy metrics (`wits.worldbank.org`).
- **data.gov.in / OGD India** – planned source for ITC/HS mappings, import policy notifications, and allied government datasets (`data.gov.in`).
- **Crowdsourced & vendor inputs** – CAPEX/OPEX templates, machine vendor directories, and skills taxonomies curated via the community.

## ETL & Automation
- `POST /admin/etl/comtrade?from=YYYY-MM&to=YYYY-MM` downloads monthly UN Comtrade data (HS6), upserts products/imports, and recomputes metrics.
- `POST /admin/etl/dgcis?file_path=...` ingests DGCI&S CSV exports (USD/INR values, partner mix) and recomputes analytics.
- `.github/workflows/ci.yml` installs dependencies, runs `python -m compileall server`, and executes tests.
- `.github/workflows/nightly_etl.yml` triggers the Comtrade ETL and recompute every night at 03:00 UTC. Configure repository secrets `ADMIN_KEY` and `DEPLOY_URL`.

### Pipeline Blueprint
**Source pulls**
- Monthly exports from Tradestat/DGCI&S (planned) for official HS-level commodity data.
- UN Comtrade API (implemented) for partner-country flows and historical series.
- WITS for tariffs and applied duty information (planned).
- data.gov.in datasets for ITC/HS mappings, policy circulars, and restrictions (planned).

**Transform & Normalize**
- Canonicalise HS codes (6/8/10 digit) using `server/etl/normalize.py`.
- Store monetary values in USD today; roadmap includes persisting INR alongside exchange rates.
- Compute rolling 12-month totals, YoY growth, partner concentration (HHI), and import share via `server/jobs.py`.

**Enrich**
- Attach tariff and policy flags using WITS/commerce ministry data (future work).
- Load crowdsourced CAPEX/OPEX templates and machine catalogs into `domestic_capability` entries.

**Serve**
- Persist analytics in Postgres tables (`products`, `monthly_imports`, `baseline_imports`, `import_progress`, `domestic_capability`).
- Expose REST endpoints (`/api/products`, `/api/products/{hs}`, `/api/leaderboard`, `/api/domestic_capability`, admin routes) for UI/API consumption.

### Data Model Snapshot
```sql
CREATE TABLE products (
  hs_code TEXT PRIMARY KEY,
  title TEXT,
  description TEXT,
  sectors TEXT[],
  granularity INT DEFAULT 6,
  capex_min NUMERIC,
  capex_max NUMERIC,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE monthly_imports (
  id SERIAL PRIMARY KEY,
  hs_code TEXT REFERENCES products(hs_code),
  year INT,
  month INT,
  value_usd NUMERIC,
  qty NUMERIC,
  partner_country TEXT,
  UNIQUE (hs_code, year, month, partner_country)
);

CREATE TABLE import_progress (
  hs_code TEXT PRIMARY KEY REFERENCES products(hs_code),
  baseline_12m_usd NUMERIC,
  current_12m_usd NUMERIC,
  reduction_abs NUMERIC,
  reduction_pct NUMERIC,
  hhi_baseline NUMERIC,
  hhi_current NUMERIC,
  concentration_shift NUMERIC,
  opportunity_score NUMERIC,
  last_updated timestamptz DEFAULT now()
);

CREATE TABLE domestic_capability (
  id SERIAL PRIMARY KEY,
  hs_code TEXT REFERENCES products(hs_code),
  capex_min NUMERIC,
  capex_max NUMERIC,
  machines JSONB,
  skills JSONB,
  notes TEXT,
  source TEXT,
  verified BOOLEAN DEFAULT false,
  created_at timestamptz DEFAULT now()
);



```
- `Normalized(ImportValue)`: log1p of the rolling 12-month USD import, min–max scaled (`server/util.py::norm_log`).
- `SupplierConcentration`: Herfindahl index of partner-country shares, using `1 - HHI` to reward diversified imports (`server/util.py::hhi_from_shares`).
- `TechFeasibilityScore`: heuristic per sector (`server/util.py::tech_feasibility_for`), to be refined with granular tags/ML in future.
- `PolicySupportFactor`: defaults to `1.0`; elevate to `>1.0` when PLI/Make-in-India incentives apply (via WITS/policy data).

## Product Vision (MVP)
- **Home dashboard** highlighting top imported items by value, trending surges, and a map of source-country dependence.
- **HS search & detail** surfaces monthly import charts, top partners, tariffs/policy flags, opportunity scores, CAPEX/OPEX ranges, required machinery, skill tiers, and localisation guidance.
- **Business case generator** estimates revenue, payback, and break-even given factory size, price, and import-replacement assumptions.
- **Compare view** juxtaposes HS items or supplier countries for strategy decisions.
- **Alerts & watchlist** trigger when YoY import growth spikes or single-country dependence exceeds thresholds.
- **Community registry** for machine vendors, tooling suppliers, and contract manufacturers (crowdsourced + verified).
- **APIs & downloads** provide CSV/JSON exports per HS item. Current REST endpoints ship today; richer download tooling is planned.

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

=======


### Opportunity Scoring
```
OpportunityScore = Normalized(ImportValue) * (1 - SupplierConcentration) * TechFeasibilityScore * PolicySupportFactor
```
- `Normalized(ImportValue)`: log1p of the rolling 12-month USD import, min–max scaled (`server/util.py::norm_log`).
- `SupplierConcentration`: Herfindahl index of partner-country shares, using `1 - HHI` to reward diversified imports (`server/util.py::hhi_from_shares`).
- `TechFeasibilityScore`: heuristic per sector (`server/util.py::tech_feasibility_for`), to be refined with granular tags/ML in future.
- `PolicySupportFactor`: defaults to `1.0`; elevate to `>1.0` when PLI/Make-in-India incentives apply (via WITS/policy data).

## Product Vision (MVP)
- **Home dashboard** highlighting top imported items by value, trending surges, and a map of source-country dependence.
- **HS search & detail** surfaces monthly import charts, top partners, tariffs/policy flags, opportunity scores, CAPEX/OPEX ranges, required machinery, skill tiers, and localisation guidance.
- **Business case generator** estimates revenue, payback, and break-even given factory size, price, and import-replacement assumptions.
- **Compare view** juxtaposes HS items or supplier countries for strategy decisions.
- **Alerts & watchlist** trigger when YoY import growth spikes or single-country dependence exceeds thresholds.
- **Community registry** for machine vendors, tooling suppliers, and contract manufacturers (crowdsourced + verified).
- **APIs & downloads** provide CSV/JSON exports per HS item. Current REST endpoints ship today; richer download tooling is planned.

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
- `POST /admin/etl/dgcis`
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
