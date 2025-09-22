# importdependencymap

Project: build-for-india — Autonomous V1 roadmap (Codex / Cursor)
Goal (one line)

Autonomously produce & deploy V1: Postgres-backed API + ETL that seeds HS-level import data, computes import_progress and opportunity_score, and serves product pages + CSV API. Deploy to managed Postgres (Railway/Neon) and a small web host.

Required env vars

DATABASE_URL — Postgres connection (postgres://USER:PASS@HOST:PORT/DB)

APP_HOST — host for app (default 0.0.0.0)

APP_PORT — port (default 8000)

ADMIN_KEY — simple admin token for /admin endpoints

Minimal file layout (agent should create)
/server
  ├─ app.py                  # FastAPI app
  ├─ db.py                   # DB connection / migrations (alembic optional)
  ├─ models.sql              # canonical SQL schema
  ├─ etl/
  │   ├─ fetcher.py          # DGCI&S / UNComtrade connectors (stubbed)
  │   └─ transform.py        # normalize HS, compute baseline/rolling sums
  ├─ jobs.py                 # compute import_progress & opportunity_score
  ├─ seed/
  │   └─ top100_hs.csv
  └─ requirements.txt
/client
  └─ static minimal SPA (HTML+JS) or Next.js minimal pages
/.github/workflows/deploy.yml
/CODEX_ROADMAP.md
/README.md

DB schema (create exactly)

Put this into server/models.sql — agent should run it on DATABASE_URL.

-- products (HS canonical)
CREATE TABLE IF NOT EXISTS products (
  hs_code TEXT PRIMARY KEY,
  title TEXT,
  description TEXT,
  sectors TEXT[],          -- array of industry tags
  granularity INT DEFAULT 6
);

-- sub_items (detailed variants)
CREATE TABLE IF NOT EXISTS sub_items (
  id SERIAL PRIMARY KEY,
  hs_code TEXT REFERENCES products(hs_code),
  name TEXT,
  attrs JSONB,             -- variant attributes, e.g. {age_group: '3-7', type:'plush'}
  typical_unit_cost NUMERIC,
  UNIQUE(hs_code, name)
);

-- monthly_imports
CREATE TABLE IF NOT EXISTS monthly_imports (
  id SERIAL PRIMARY KEY,
  hs_code TEXT REFERENCES products(hs_code),
  year INT,
  month INT,
  value_usd NUMERIC,
  qty NUMERIC,
  partner_country TEXT
);

-- baseline_imports (chosen baseline window)
CREATE TABLE IF NOT EXISTS baseline_imports (
  hs_code TEXT PRIMARY KEY,
  baseline_12m_usd NUMERIC,
  baseline_period TEXT,
  updated_at timestamptz DEFAULT now()
);

-- import_progress (materialized rolling)
CREATE TABLE IF NOT EXISTS import_progress (
  hs_code TEXT PRIMARY KEY,
  baseline_12m_usd NUMERIC,
  current_12m_usd NUMERIC,
  reduction_abs NUMERIC,
  reduction_pct NUMERIC,
  hhi_baseline NUMERIC,
  hhi_current NUMERIC,
  concentration_shift NUMERIC,
  last_updated timestamptz DEFAULT now()
);

-- domestic_capability
CREATE TABLE IF NOT EXISTS domestic_capability (
  id SERIAL PRIMARY KEY,
  hs_code TEXT REFERENCES products(hs_code),
  capex_min NUMERIC,
  capex_max NUMERIC,
  machines JSONB,
  skills JSONB,
  notes TEXT,
  source TEXT
);

Opportunity scoring (implement in jobs.py)

Formula (exact):

ImportValue = normalize_log(sum_last_12m_usd)  # log(1 + x) scaled 0..1
SupplierConcentration = HHI_current   # normalized 0..1
TechFeasibility = heuristic 0..1      # seed mapping by sector
PolicySupport = 1.2 if PLI/MakeInIndia else 1.0
OpportunityScore = ImportValue * (1 - SupplierConcentration) * TechFeasibility * PolicySupport


Agent must store opportunity_score in a read table or compute on-the-fly.

ETL & progress rules (agent tasks)

Fetch monthly HS-level import timeseries (DGCI&S or UN Comtrade). If API unavailable, use seed/top100_hs.csv.

Normalize HS codes to HS6 canonical (pad/truncate).

Compute rolling 12-month sums per HS.

Compute baseline (configurable: default = average of 2019-2021 or prior 24 months) and store in baseline_imports.

Compute HHI of partner_country shares for baseline and current.

Populate import_progress table with reduction_pct and concentration_shift.

Schedule job: daily or weekly (initially run once).

API endpoints (FastAPI style)

GET /api/products?sectors=&min_capex=&max_capex=&combine=AND|OR&sort=opportunity|progress&limit=100

GET /api/products/{hs} → product + timeseries (last 36mo) + import_progress + top partners

GET /api/products/{hs}/sub_items

POST /api/domestic_capability (admin, require ADMIN_KEY header)

POST /admin/recompute (admin: recompute import_progress & scores)

GET /api/leaderboard?metric=opportunity|progress&limit=50

Return JSON with provenance: source, last_updated.

Seed strategy (immediate)

Use data/top100_hs.csv (agent should include small list—HS6 + seed monthly value + top_country + sectors + capex_min/max).

Provide a /admin/seed endpoint to load CSV into DB.

Deploy (Railway/Neon recommended)

Steps for agent to automate:

Create Railway/Neon Postgres and populate DATABASE_URL (user will provide).

Add GitHub Actions workflow to push to Railway (or use Railway CLI).

On successful deploy, run /admin/seed endpoint (HTTP POST) via workflow.

Minimal GitHub Action snippet (put in .github/workflows/deploy.yml):

name: deploy
on:
  push:
    branches: [ main ]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Setup Python
        uses: actions/setup-python@v4
        with: {python-version: '3.10'}
      - name: Install deps
        run: pip install -r server/requirements.txt
      - name: Run migrations & seed
        env:
          DATABASE_URL: ${{ secrets.DATABASE_URL }}
          ADMIN_KEY: ${{ secrets.ADMIN_KEY }}
        run: |
          python -c "import server.db as db; db.create_all()"
          curl -X POST -H "Authorization: Bearer ${{ secrets.ADMIN_KEY }}" ${{ secrets.DEPLOY_URL }}/admin/seed


Agent: replace placeholders with actual host after deploy.

Cursor / Codex agent prompt (paste as single input; be explicit)
You are an autonomous coding agent. Repo root has CODEX_ROADMAP.md. Do these steps in order and commit changes:

1) Create server/models.sql exactly as specified.
2) Implement `server/db.py` to connect to `DATABASE_URL` with SQLAlchemy (async optional).
3) Implement `server/app.py` (FastAPI) with endpoints: /admin/seed, /admin/recompute, /api/products, /api/products/{hs}, /api/products/{hs}/timeseries, /api/domestic_capability, /api/leaderboard. Use `ADMIN_KEY` header for admin endpoints.
4) Implement `server/etl/fetcher.py` with stubbed connectors for DGCI&S and UN Comtrade; fallback to reading `data/top100_hs.csv`.
5) Implement `server/jobs.py` that computes rolling sums, HHI, baseline, import_progress, and opportunity_score (use exact formula).
6) Add `data/top100_hs.csv` with 50-100 HS6 rows (if unable to fetch real data, generate plausible mock numbers).
7) Add `client/index.html` minimal SPA to call the API and show product list + product page with Chart.js.
8) Add `.github/workflows/deploy.yml` that deploys and then calls /admin/seed.
9) Run unit tests for scoring and seed; fix failures.
10) Commit everything and push to the `main` branch.

When done, respond with: list of created files, how to set secrets, and the public deploy URL (if you deployed).
If any step errors, attempt fix autonomously, log error, and continue.

Minimal dev commands (agent should include these)
# create venv & install
python -m venv .venv
source .venv/bin/activate
pip install -r server/requirements.txt

# run locally (dev sqlite fallback)
uvicorn server.app:app --reload --host 0.0.0.0 --port 8000

# seed (after server up)
curl -X POST -H "Authorization: Bearer $ADMIN_KEY" http://localhost:8000/admin/seed

Governance & safety (brief)

Always include source and last_updated in API responses.

Moderate crowdsourced domestic_capability entries before publishing; add a verified flag.

Log data provenance for every HS item.
