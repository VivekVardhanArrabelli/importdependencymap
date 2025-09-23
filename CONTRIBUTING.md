# Contributing to Build for India

We welcome contributions that improve data coverage, analytics, and user experience. Please follow the steps below to keep the workflow smooth.

## Getting Started
- Fork the repository and create a feature branch from `main` or the latest release branch.
- Use Python 3.11 (or 3.10+) and create a virtual environment: `python -m venv .venv && source .venv/bin/activate`.
- Install dependencies: `pip install -r requirements.txt`.
- Copy `.env.example` to `.env` and fill in local `DATABASE_URL` and `ADMIN_KEY`.

## Development Workflow
- Run the API with `uvicorn server.main:app --host 0.0.0.0 --port 8000`.
- Serve the static client with `python -m http.server --directory client 8080` or rely on FastAPI’s root route to serve `client/index.html` directly.
- Use the helper scripts:
  - Seed data: `./scripts/seed_local.sh http://localhost:8000`
  - Recompute metrics: `./scripts/recompute_local.sh http://localhost:8000`
- Keep code formatted with `black` and linted with `ruff` (optional but encouraged).

## Testing
- Add unit tests under `tests/` using `pytest`. Name files `test_<module>.py` and cover both Postgres-backed flows and failure handling when feasible.
- Run `pytest` (or `python -m unittest discover -s tests`) before submitting a PR.
- Ensure `python -m compileall server` passes.

## Commit & PR Guidelines
- Follow Conventional Commits (`feat:`, `fix:`, `docs:`, etc.). Use scopes like `feat(api):` when it clarifies the surface area.
- Squash fixup commits locally; keep history tidy.
- In PR descriptions include:
  - Problem statement and solution summary.
  - Manual verification steps (seed, recompute, API calls).
  - Screenshots or GIFs when changing the client.
- Link to any GitHub issues or roadmap items this PR addresses.

Thanks for helping build resilient domestic manufacturing intelligence for India!
