# 120-Day Closing High Materialization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Materialize an immutable daily SSE/SZSE 120-session closing-high snapshot at 21:30 and make the existing FastAPI endpoint read the latest ready snapshot without scanning `core.daily_bar`.

**Architecture:** PostgreSQL persistence loads one bounded per-symbol aggregate using the existing `(market, trade_date)` index. A pure Decimal calculator classifies candidates and hashes deterministic inputs/outputs; a service publishes calculation, snapshot, and members atomically. The Worker owns the fixed schedule, while the public RPC reads only the latest ready snapshot.

**Tech Stack:** Python 3.12, SQLAlchemy 2, PostgreSQL, APScheduler, FastAPI, Pydantic, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-08-16-close-price-new-highs-materialization-design.md`

## Global Constraints

- SSE/SZSE `stock` only; BSE remains excluded.
- Exactly 120 CN_A_SHARE trading sessions; current close must be strictly greater than the previous 119-session maximum.
- Prices and ratios use `Decimal`; missing values remain missing and zero is not treated as missing.
- Candidate universe is bounded at 10,000 and API statement timeout remains 10 seconds.
- Schedule is hard-coded Monday-Friday 21:30 Asia/Shanghai; configuration can only enable/disable it and defaults enabled.
- All schema changes use ordered `supabase/migrations/*.sql`; no ad-hoc production DDL.
- Worker uses APScheduler only; never add cron or Windows Task Scheduler.
- FastAPI calls only the bounded `api_v1` RPC and never reads internal schemas directly.

---

### Task 1: Pure Domain Model and Calculator

**Files:**
- Create: `src/market_data_center/domain/close_price_new_highs.py`
- Create: `src/market_data_center/close_price_new_highs_calculator.py`
- Create: `tests/test_close_price_new_highs.py`

**Interfaces:**
- Consumes: `Decimal`, `date`, sorted per-symbol aggregate inputs from Task 2.
- Produces: `ClosePriceNewHighCandidate`, `ClosePriceNewHighMember`, `ClosePriceNewHighCalculation`, `calculate_close_price_new_highs_120d(source)` and deterministic input/content hashes.

- [ ] **Step 1: Write failing calculator tests**

Create literal fixtures for one strict breakout, one equal close, one 119-row history, one explicit suspended bar, one nonpositive price, and one missing name. Assert exact omission counts, Decimal breakout percentage, ordering by percentage descending then symbol, and equal hashes for differently ordered inputs.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_close_price_new_highs.py -q`

Expected: collection fails because `market_data_center.domain.close_price_new_highs` does not exist.

- [ ] **Step 3: Implement the immutable records and pure calculation**

Use frozen slot dataclasses. Validate unique symbols, `session_count == 120`, and candidate bound. Classify every candidate independently; `eligible_history_count` includes complete valid histories before strict breakout filtering. Quantize `breakout_pct` to ten decimal places and hash canonical JSON containing ISO dates and Decimal strings.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_close_price_new_highs.py -q`

Expected: all calculator tests pass.

### Task 2: Versioned Tables, Persistence, and Service

**Files:**
- Create: `supabase/migrations/20260816000200_materialize_close_price_new_highs_120d.sql`
- Create: `src/market_data_center/persistence/close_price_new_highs_postgres.py`
- Create: `src/market_data_center/close_price_new_highs_service.py`
- Create: `tests/test_close_price_new_highs_service.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: Task 1 calculator records.
- Produces: `PostgreSQLClosePriceNewHighsPersistence.load_input(trade_date)`, `.publish(...)`, `.existing_snapshot(...)`, and `ClosePriceNewHighsService.build(trade_date)` returning a summary with fetched/accepted/rejected row counts.

- [ ] **Step 1: Write failing service tests**

Use an in-memory fake persistence to prove: same input reuses a snapshot, changed input publishes the next version, zero breakouts still publishes a ready header, and publish exceptions mark the calculation failed without returning success.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_close_price_new_highs_service.py -q`

Expected: imports fail because the service and persistence contract do not exist.

- [ ] **Step 3: Add migration and persistence**

Create snapshot/member tables, RLS policies, worker grants, ready lookup index, constraints, and the replacement `api_v1.query_close_price_new_highs_120d()` that reads only the latest ready snapshot. In `load_input`, select the exact 120 calendar sessions, derive `first_day/last_day`, scan bars with `b.trade_date between :first_day and :last_day`, aggregate by candidate symbol, and join the effective historical name. Reject missing same-day terminal `daily_market` runs and more than 10,000 candidates.

- [ ] **Step 4: Implement transactional service publication**

Acquire an advisory lock scoped to trade date. Create a `derived.calculation_run`, calculate, allocate version under the same transaction, insert snapshot and members, then finish the calculation. On failure update the calculation to failed in a separate safe transaction. Return `unchanged` for the same successful input hash.

- [ ] **Step 5: Add PostgreSQL integration coverage**

Extend the existing exact 120-session fixture to call the service, assert one ready snapshot plus only strict-breakout members, call twice to assert idempotence, revise an input close and assert version 2, and execute the RPC to verify it reads version 2 without accepting parameters.

- [ ] **Step 6: Verify GREEN**

Run: `uv run pytest tests/test_close_price_new_highs.py tests/test_close_price_new_highs_service.py tests/test_production_checks.py -q`

If `TEST_DATABASE_URL` is configured, also run: `uv run pytest tests/test_postgres_integration.py -k close_price_new_highs -q`

### Task 3: Worker Workflow, Fixed Schedule, and Manual CLI

**Files:**
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `supabase/migrations/20260816000200_materialize_close_price_new_highs_120d.sql`
- Modify: `tests/test_operations.py`
- Modify: `tests/test_cli.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: `ClosePriceNewHighsService.build(trade_date)` from Task 2.
- Produces: workflow code `close_price_new_highs_120d`, job ID `close-price-new-highs-120d-daily`, `run_close_price_new_highs_120d_job()`, and CLI `close-price-new-highs-120d-build --trade-date YYYY-MM-DD`.

- [ ] **Step 1: Write failing catalog and execution tests**

Assert the new workflow has exactly `build_close_price_new_highs_120d_snapshot`, the job is enabled by default at `(21, 30)`, disabling only removes registration, scheduler function mapping resolves, and execution records the service summary through Operations.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_operations.py tests/test_cli.py -q`

Expected: missing workflow enum/catalog/CLI assertions fail.

- [ ] **Step 3: Implement workflow and schedule**

Add `close_price_new_highs_120d_enabled: bool = True` with no time fields. Register the fixed cron definition and scheduler function. The scheduled function uses the scheduled fire date in Asia/Shanghai, starts Operations, runs one service step, records terminal status, and disposes its engine.

- [ ] **Step 4: Implement explicit-date CLI**

Add the parser and dispatch branch. Parse only an explicit ISO date, call the same service, and print a JSON-safe summary without credentials or database paths.

- [ ] **Step 5: Extend Operations migration constraint**

Replace the workflow-code check constraint in the same ordered migration so production accepts the new code before Worker deployment.

- [ ] **Step 6: Verify GREEN**

Run: `uv run pytest tests/test_operations.py tests/test_cli.py tests/test_worker_admin.py -q`

Expected: all focused Worker tests pass.

### Task 4: Public Contract and Documentation Synchronization

**Files:**
- Modify: `src/market_data_center/public_api/queries.py` only if response field mapping needs snapshot aliases.
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json` only if the response model changes.
- Modify: `scripts/check_fastapi_release.py`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/数据库导航.md`
- Create: `docs/领域详设-沪深120交易日收盘新高快照-2026-08-16.md`
- Modify: `tests/test_api_contracts.py`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: the no-argument RPC from Task 2.
- Produces: unchanged `GET /api/v1/close-price-new-highs-120d` consumer contract backed by the latest ready materialized snapshot.

- [ ] **Step 1: Write failing contract checks**

Assert the public route still has no query parameters, the RPC contract stays no-input, the release check requires it, and the latest migration RPC body reads `derived.close_price_new_high_120d_snapshot/member` without `core.daily_bar`.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_api_contracts.py tests/test_public_api.py tests/test_production_checks.py -q`

Expected: migration-source and documentation/contract expectations fail before synchronization.

- [ ] **Step 3: Synchronize contracts and docs**

Keep response schema compatible, update RPC descriptions to say “latest ready materialized snapshot,” document 21:30 freshness and not-found behavior, add the domain design, and ensure release preflight includes the route.

- [ ] **Step 4: Verify GREEN**

Run: `uv run pytest tests/test_api_contracts.py tests/test_public_api.py tests/test_production_checks.py -q`

Expected: focused API/contract tests pass.

### Task 5: Full Verification, Commit, Push, and Production Deployment

**Files:**
- Modify only files already listed if verification finds a defect.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: pushed `master`, one production migration, staged release, first ready snapshot, active Worker/API, and successful online endpoint response.

- [ ] **Step 1: Run the complete local gate**

Run:

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
git diff --check
```

Expected: all checks pass; PostgreSQL integration tests may skip only when `TEST_DATABASE_URL` is absent and that skip is reported.

- [ ] **Step 2: Commit and push**

Stage only planned files, commit with `feat: materialize daily 120-day closing highs`, and push `master`.

- [ ] **Step 3: Build and stage the Linux release**

Build with `uv run python scripts/build_release.py --platform linux`, verify SHA-256 locally/remotely, extract to a commit-suffixed directory, and run FastAPI/Worker release preflights before switching symlinks.

- [ ] **Step 4: Apply the single expected production migration**

Read-only compare repository and production migration histories first; proceed only when the sole pending version is `20260816000200`. Apply it transactionally and run `scripts/apply_migrations.py check`.

- [ ] **Step 5: Switch services and seed the first snapshot**

Atomically switch API and Worker releases, restart both systemd services, run the explicit CLI for the latest production trade date, and confirm a ready snapshot exists. Preserve the prior release directories for rollback and do not overwrite server environment files.

- [ ] **Step 6: Online verification**

Verify `/healthz`, `/readyz`, authenticated `/api/v1/close-price-new-highs-120d`, response counts, trade date, HTTP 200, duration below 10 seconds, both services active, and the task catalog next-run at 21:30.
