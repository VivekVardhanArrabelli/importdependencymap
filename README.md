# Build for India

Build for India is a capital-allocation operating system for domestic
capability. It maps India's imports by HS6 commodity, turns observed demand
into transparent opportunity signals, and helps a founder, operator, or
capital partner move candidates from screening to diligence to a hypothetical
capital plan. The app is a FastAPI service backed by Postgres with a
dependency-light static browser client.

The browser experience separates three kinds of information:

- **Observed facts:** import value, baseline change, monthly demand, partner
  mix, source, and period.
- **Derived signals:** country import signal and mandate match, with the formula shown
  in the product.
- **Planning assumptions:** entry route and import-substitution scenarios.

It does not execute investments, estimate returns, or treat import exposure as
revenue. Capital shapes the suggested diligence route but is excluded from fit
scoring because project-CAPEX coverage is not yet systematic. Isolated sourced
records appear as evidence, not as a ranking input.

## What populates the app

The data pipeline deliberately separates the product catalogue from monthly
trade facts:

- The official UN Comtrade HS 2022 reference supplies the complete HS6 product
  catalogue (currently 5,613 leaf commodities). It is synchronized on startup
  and at the beginning of each Comtrade refresh.
- Monthly India imports come from UN Comtrade. The current India reporter code
  is `699`; the retired code `356` does not return current data.
- A `COMTRADE_KEY` uses the fast authenticated endpoint. Without a key, the ETL
  uses the anonymous preview endpoint in complete one-month/exact-code batches
  so its 500-row ceiling cannot silently truncate the catalogue.
- World-total rows drive import-value time series. Country rows are kept
  separate so partner concentration cannot double-count World plus countries.
- Product-detail partner data is fetched from Comtrade for the latest loaded
  month when no partner breakdown is stored locally.

The ETL reads current Comtrade v1 fields such as `cmdDesc`, `primaryValue`,
`netWgt`, `qty`, and `partnerISO`, while retaining compatibility with older
field names.

## Requirements

- Python 3.10+
- Postgres
- `DATABASE_URL`
- `ADMIN_KEY` for protected refresh routes

Copy `.env.example` to `.env` and set the required values. Never commit live
keys.

Important configuration:

- `COMTRADE_KEY` — optional subscription key; strongly recommended for fast
  complete refreshes.
- `COMTRADE_BASE` — normally leave blank so the app selects the correct keyed
  or anonymous endpoint automatically.
- `COMTRADE_REPORTER_CODE` — defaults to current India code `699`.
- `COMTRADE_FLOW` — defaults to `import`.
- `COMTRADE_LOOKBACK_MONTHS` — nightly history window, at least 13 and default
  18 months.
- `COMTRADE_MIN_PRODUCTS_PER_PERIOD` — completeness guard, default 1,000
  distinct HS6 products per India month.
- `COMTRADE_SYNC_CATALOG_ON_STARTUP` — defaults to `true`; set `false` only if
  startup must avoid the reference download.
- `FX_RATES_FILE` — monthly USD-to-INR CSV, default
  `data/fx_rates.csv`.
- `DGCIS_DEFAULT_PATH` — optional DGCI&S export path, default
  `data/dgcis_latest.csv`.

## Local development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install pytest
uvicorn server.main:app --host 0.0.0.0 --port 8000
```

The service hosts the client at `/` and assets at `/static`.

Run validation:

```bash
pytest -q
python -m compileall server
node --check client/app.js
node --check client/opportunity-model.js
node --test tests/test_opportunity_model.js
```

## Refreshing data

Refresh an explicit inclusive range:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "http://localhost:8000/admin/etl/comtrade?from=2025-01&to=2026-04"
```

With `COMTRADE_KEY`, let the server determine an availability-aware rolling
window:

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $ADMIN_KEY" \
  "http://localhost:8000/admin/etl/nightly"
```

Requested dates are capped to the latest period reported by Comtrade. A
refresh fails loudly on API validation errors, count mismatches, or possible
record-limit truncation instead of persisting a partial response.

Optional admin helpers:

```bash
./scripts/seed_local.sh http://localhost:8000
./scripts/recompute_local.sh http://localhost:8000
./scripts/load_dgcis.sh data/dgcis_latest.csv http://localhost:8000
```

The seed file is demo-only. It is never a fallback for a failed live refresh.

## Data sources

### Primary source: UN Comtrade

- [API documentation](https://uncomtrade.org/docs/un-comtrade-api/)
- [API limits and subscription tiers](https://uncomtrade.org/docs/subscriptions/)
- [HS 2022 reference](https://comtradeapi.un.org/files/v1/app/reference/H6.json)
- [Reporter reference](https://comtradeapi.un.org/files/v1/app/reference/Reporters.json)

UN Comtrade is the best automated international source for this public app.
Review its [use and redistribution guidance](https://uncomtrade.org/docs/faqs-on-use-and-re-dissemination/)
before publishing bulk downloads or redistributing raw records.

### Better India-specific source: DGCI&S

For production-grade India analysis, the preferred source is the Department of
Commerce/DGCI&S:

- [TradeStat commodity-wise imports](https://tradestat.commerce.gov.in/meidb/commoditywise_import)
- [Trade Intelligence and Analytics portal](https://trade-analytics.commerce.gov.in/public/indiaTrade)

DGCI&S is more authoritative for India and provides HS8 detail, but it does
not expose a documented stable public API for this workflow. The safe
integration is a scheduled or manual official CSV export through
`POST /admin/etl/dgcis`, not brittle screen scraping. UN Comtrade remains the
automated fallback and cross-country comparison source.

## API

Public routes:

- `GET /health`
- `GET /api/products?limit=500&offset=0`
- `GET /api/products/{hs_code}`
- `GET /api/leaderboard`
- `GET /api/domestic_capability/{hs_code}`
- `POST /api/domestic_capability`

Protected routes:

- `POST /admin/etl/comtrade?from=YYYY-MM&to=YYYY-MM`
- `POST /admin/etl/comtrade/catalog`
- `GET /admin/etl/comtrade/availability`
- `POST /admin/etl/nightly`
- `POST /admin/etl/dgcis?file_path=...`
- `POST /admin/recompute`
- `POST /admin/seed`
- `GET /admin/diag`

Product listing responses include `total`, `offset`, `limit`, `source`,
`latest_period`, and `last_updated`. The client follows pagination until it
has received the complete filtered result.

## Data model and scoring

`monthly_imports` identifies each row by HS code, year, month, partner, and
source. `is_aggregate=true` marks World-total rows. This allows Comtrade and
DGCI&S records to coexist without treating the World aggregate as a supplier.

The current opportunity score is:

```text
normalized_import_value × (1 - HHI) × technical_feasibility × policy_factor
```

When stored partner detail is unavailable, HHI and the combined opportunity
score remain unavailable rather than treating an unknown supplier mix as
perfect diversification. Import values still populate, and a short anonymous
live partner snapshot is shown on product detail. Missing values are never
rendered as zero.

The browser does not substitute a made-up value for that unavailable backend
score. It presents two separate screening signals:

```text
CountryImportSignal = 70% import-value percentile + 30% import-value momentum
MandateMatch = sector edge + milestone/ambition alignment + selected role
CombinedFit = user-selected weighting of CountryImportSignal and MandateMatch
```

Sector groups are derived from official HS chapters rather than loose
substring matching. This prevents false classifications such as `coiled` or
`shelled` being treated as electronics because they contain `LED`. Energy is
also retained as a cross-cutting tag for products such as batteries.
Controlled chapter 93 goods are gated out of default founder results, and
aircraft and vessels carry a strategic-asset gate.

Gross import value is an exposure screen, not proof of dependence or a domestic
capability gap. Production, export, price/quantity, buyer, and project evidence
must be added during diligence before capital is committed.

## Railway deployment

1. Provision a Railway web service and Postgres database.
2. Set `DATABASE_URL`, `ADMIN_KEY`, and optionally `COMTRADE_KEY`.
3. Start with:

   ```bash
   uvicorn server.main:app --host 0.0.0.0 --port $PORT
   ```

4. Configure GitHub Actions secrets `DEPLOY_URL` and `ADMIN_KEY` for
   `.github/workflows/nightly_etl.yml`.
5. Manually dispatch `nightly-etl` once with `lookback_months=18` after the
   first deployment, then verify `/admin/diag`.

The scheduled workflow refreshes the two latest available Comtrade months.
Anonymous preview mode is partitioned into one-month HTTP requests to stay
inside Railway's request-duration limit, then analytics are recomputed once.
With `COMTRADE_KEY`, the direct `/admin/etl/nightly` endpoint remains the fast
single-request path.

## License and community

See `LICENSE`, `CONTRIBUTING.md`, `CODE_OF_CONDUCT.md`, and `SECURITY.md`.
