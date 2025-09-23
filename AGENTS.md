AGENTS.md — Autonomous Build Plan for build-for-india
Mission
Ship an open-source website that helps Indians identify and build domestic manufacturing for every imported item. It must:
Track imports by HS code (HS6/HS8), values, quantities, partner concentration.
Score opportunities (replaceability) and show CAPEX/OPEX, machinery, tech needs, and business cases.
Track progress over time (import reduction, concentration shift) and guide focus to remaining high-impact items.
Support search/filters (industry tags, investment budget).
Provide open APIs and downloads; accept community submissions.
Run reliably on Railway (FastAPI + Postgres). Seed data exists; live ETL from UN Comtrade (no CSV fallback) + hooks for DGCI&S.
Runtime & Conventions
Python 3.10+; FastAPI; psycopg2; SQL only (no ORM).
Repo root contains requirements.txt and this file.
Start command (Railway):
uvicorn server.main:app --host 0.0.0.0 --port $PORT
Idempotent, retryable tasks. Clear logs. Type-annotate functions.
Environment & Secrets
Set on Railway (Project → Variables) and used as env vars:
DATABASE_URL — Postgres URL (append ?sslmode=require if needed)
ADMIN_KEY — token for admin endpoints
COMTRADE_BASE — default https://comtradeapi.un.org/public/v1/preview
COMTRADE_FLOW — import
COMTRADE_REPORTER — India
COMTRADE_FREQ — M
(optional) SENTRY_DSN, GA_MEASUREMENT_ID
Create .env.example (no values), and never commit secrets.
Target File Layout
/server
  __init__.py
  main.py
  db.py
  jobs.py
  etl/
    __init__.py
    comtrade.py
    dgcis.py            # stub + TODO
    normalize.py
  schemas.py
  util.py
/client
  index.html
  styles.css
  app.js
/data
  top100_hs.csv         # optional bootstrap seed (10–100 curated rows)
/scripts
  seed_local.sh
  recompute_local.sh
/tests
  test_health.py
  test_seed_and_list.py
  test_scoring.py
Procfile
README.md
CODE_OF_CONDUCT.md
CONTRIBUTING.md
SECURITY.md
LICENSE
.github/workflows/ci.yml
.github/workflows/nightly_etl.yml
Database Schema (execute via psycopg2 in db.init_db())
CREATE TABLE IF NOT EXISTS products (
  hs_code TEXT PRIMARY KEY,
  title TEXT,
  description TEXT,
  sectors TEXT[],                -- e.g. {'electronics','energy'}
  granularity INT DEFAULT 6,
  capex_min NUMERIC,
  capex_max NUMERIC,
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS monthly_imports (
  id SERIAL PRIMARY KEY,
  hs_code TEXT REFERENCES products(hs_code),
  year INT,
  month INT,
  value_usd NUMERIC,
  qty NUMERIC,
  partner_country TEXT
);

CREATE INDEX IF NOT EXISTS ix_monthly_imports_hs_ym
  ON monthly_imports (hs_code, year, month);

CREATE TABLE IF NOT EXISTS baseline_imports (
  hs_code TEXT PRIMARY KEY,
  baseline_12m_usd NUMERIC,
  baseline_period TEXT,
  updated_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS import_progress (
  hs_code TEXT PRIMARY KEY,
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

CREATE TABLE IF NOT EXISTS domestic_capability (
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
Core Logic
Normalization (etl/normalize.py)
HS canonicalization: normalize to HS6 (string, zero-pad/truncate).
Sectors mapping: map keywords to sectors list (heuristics).
Currency: values assumed USD; store as provided.
Progress & Scoring (jobs.py)
Rolling sums: last 12m per HS.
Baseline: choose earliest contiguous 12m window with data; otherwise last full calendar year; store in baseline_imports.
HHI: compute from partner shares for baseline and current.
OpportunityScore:
ImportValue = normalize_log(current_12m_usd)     # log1p, min-max across HS universe
TechFeasibility by sector: electronics=0.7, industrial=0.6, automotive=0.5,
                            metals=0.65, energy=0.6, instruments=0.65, default=0.6
PolicySupport = 1.0            # placeholder (hook for PLI flags later)
OpportunityScore = ImportValue * (1 - HHI_current) * TechFeasibility * PolicySupport
Write to import_progress (upsert). Guard div-by-zero and missing baselines.
ETL — UN Comtrade (etl/comtrade.py)
Admin route triggers: fetch by month range from=YYYY-MM&to=YYYY-MM.
Use COMTRADE_BASE, query HS6 monthly imports for India (reporter), parse:
cmdCode (HS), rt3ISO (reporter, ignore), pt3ISO (partner), rgCode (flow), yr, period (month), TradeValue, NetWeight or quantity if available.
Upsert products with HS title/description (if returned).
Upsert monthly_imports.
Handle 429/5xx with exponential backoff; partial writes allowed; log failures and bubble errors — no CSV fallback.
ETL — DGCI&S (etl/dgcis.py)
Stub with function signatures + TODOs; leave hooks for authenticated CSV uploads later.
API (FastAPI in server/main.py)
Add CORS *. All admin routes require header Authorization: Bearer <ADMIN_KEY>.
Public
GET /health → {ok: true}
GET /api/products?sectors=e1,e2&combine=AND|OR&min_capex=&max_capex=&sort=opportunity|progress|value&limit=100
Returns cards: hs_code,title,sectors,capex_min,max,last_12m_value_usd,reduction_pct,opportunity_score
GET /api/products/{hs}
Detail: metadata + last 36m series + top partners (share %) + progress block
GET /api/leaderboard?metric=opportunity|progress|value&limit=50
GET /api/domestic_capability/{hs} (verified only)
Admin (protected)
POST /admin/seed
Create tables, read /data/top100_hs.csv (if exists), upsert products, insert 12 months dummy per row, call recompute.
POST /admin/etl/comtrade?from=YYYY-MM&to=YYYY-MM
Live pull → upsert → recompute; returns counts.
POST /admin/recompute
Re-run baseline + progress globally; returns summary.
POST /api/domestic_capability
Upsert row (verified=false default). Body: hs_code, capex_min, capex_max, machines, skills, notes, source.
Response metadata: include { "source":"seed|comtrade|manual", "last_updated": <timestamp> } on relevant endpoints.
Frontend (/client)
index.html: Navbar, hero, search bar, filters (comma-separated sectors, capex min/max), sort dropdown, Load button.
app.js:
Fetch /api/products with query params; render cards.
Click card → fetch /api/products/{hs} → render Chart.js line chart (value_usd over time), partner shares, progress stats.
styles.css: simple responsive layout. Footer includes GitHub and Donate links.
CLI Scripts (/scripts)
seed_local.sh
set -e
: "${ADMIN_KEY:?Missing ADMIN_KEY}"
URL=${1:-http://localhost:8000}
curl -X POST -H "Authorization: Bearer $ADMIN_KEY" $URL/admin/seed
recompute_local.sh
set -e
: "${ADMIN_KEY:?Missing ADMIN_KEY}"
URL=${1:-http://localhost:8000}
curl -X POST -H "Authorization: Bearer $ADMIN_KEY" $URL/admin/recompute
CI & Nightly ETL
.github/workflows/ci.yml
Python setup, pip install -r requirements.txt
python -m compileall server
Run unit tests (pytest optional; default to python -m unittest discover)
.github/workflows/nightly_etl.yml
name: nightly-etl
on:
  schedule: [{ cron: "0 3 * * *" }]
jobs:
  run:
    runs-on: ubuntu-latest
    steps:
      - name: Trigger Comtrade ETL
        run: |
          curl -X POST \
            -H "Authorization: Bearer ${{ secrets.ADMIN_KEY }}" \
            "${{ secrets.DEPLOY_URL }}/admin/etl/comtrade?from=$(date -u -d '13 months ago' +%Y-%m)&to=$(date -u -d '1 month ago' +%Y-%m)"
      - name: Recompute
        run: |
          curl -X POST \
            -H "Authorization: Bearer ${{ secrets.ADMIN_KEY }}" \
            "${{ secrets.DEPLOY_URL }}/admin/recompute"
Repo secrets needed for workflow: ADMIN_KEY, DEPLOY_URL (your Railway URL).
Tests (light)
tests/test_health.py: /health 200.
tests/test_seed_and_list.py: assert admin + read endpoints refuse to operate without DATABASE_URL (no silent fallback).
tests/test_scoring.py: sanity on normalize_log, HHI, opportunity score monotonicity.
Security & Governance
Admin endpoints require exact Bearer match.
Validate CSV lines; cap request sizes.
SECURITY.md: report process; CODE_OF_CONDUCT.md + CONTRIBUTING.md.
All API responses include source + last_updated.
Future: moderation flow for domestic_capability (verified flag).
Donations
In README.md add sections for GitHub Sponsors / OpenCollective (links left as TODO).
Roadmap (post-V1)
DGCI&S authenticated ETL.
Policy flags (PLI, duties) + PolicySupport uplift > 1.0.
BOM/sub-items table; supplier registry; per-state incentives.
Export CSV/JSON endpoints; API keys & rate limits.
Observability (Sentry), caching, pagination.
TimescaleDB / analytical warehouse (BigQuery/Snowflake) for scale.
Agent Tasks (execute in order)
Ensure repo has requirements.txt at root with:
fastapi
uvicorn[standard]
psycopg2-binary
pydantic
python-dotenv
python-dateutil
Create all files/directories above. Implement DB init and helper functions.
Build ETL (Comtrade) with robust retry/backoff and fail-fast error handling (no CSV fallback).
Implement jobs.py recompute functions, normalization, HHI, scoring.
Implement all API routes (auth guard for admin).
Build minimal client (index/search/cards/detail chart).
Add scripts and GitHub Actions workflows.
Add README, CONTRIBUTING, CODE_OF_CONDUCT, SECURITY, LICENSE (MIT).
Run python -m compileall server and basic tests.
Commit: “feat: full V1 — API, ETL, scoring, progress, client, CI, cron” and push.
Print final checklist:
Railway start command (above)
Seed command (curl with ADMIN_KEY)
Public URLs to test: /health, /docs, /api/products, /api/products/<hs>
Acceptance Criteria
/health 200; /docs renders without 500.
/admin/seed ingests curated CSV when invoked (requires DATABASE_URL).
/admin/etl/comtrade succeeds and /admin/recompute writes import_progress.
Leaderboard returns sorted items by chosen metric.
Client loads, filters by sectors/capex, shows product page with chart.
CI passes; Nightly job configured (won’t fail if secrets missing).
