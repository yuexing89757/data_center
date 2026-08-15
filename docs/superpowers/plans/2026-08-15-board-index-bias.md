# THS 883423 Latest MA5 Bias API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish an authenticated read-only endpoint that calculates the latest stored `THS:883423` MA5 bias, prior-session direction, and latest-30-session extrema.

**Architecture:** An ordered PostgreSQL migration adds one fixed, bounded `api_v1` RPC over `core.board_index_daily_bar`; FastAPI calls only that RPC and validates the payload with Decimal-safe Pydantic models. The result is computed at read time, reads at most 34 rows, and creates no table, ingestion, or scheduled job.

**Tech Stack:** PostgreSQL 15 SQL/window functions, SQLAlchemy 2, FastAPI, Pydantic 2, pytest, JSON Schema/OpenAPI contracts

**Spec:** `docs/superpowers/specs/2026-08-15-board-index-bias.md`

## Global Constraints

- The only board identity is `THS:883423`; the HTTP route accepts no date or code input.
- `MA5` is the arithmetic mean of the current and four preceding available positive closes.
- `BIAS5 = (close - MA5) / MA5 * 100`; all numeric values remain PostgreSQL `numeric` and Python `Decimal`.
- Extrema use valid BIAS5 samples from the latest 30 stored board sessions; tied extrema select the latest date.
- FastAPI calls only `api_v1.query_board_index_bias_latest()` and never reads an internal schema.
- The RPC reads at most 34 rows, has a five-second statement timeout, and is executable only by `market_data_api`.
- No provider access, fallback date, forward fill, live fetch, persistence, or scheduler is added.

---

### Task 1: Bounded PostgreSQL calculation contract

**Files:**
- Create: `supabase/migrations/20260815000100_add_board_index_bias_api.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: `core.board_index_daily_bar(board_id, trade_date, close)` and `core.board_index(board_id, board_code, name)`.
- Produces: `api_v1.query_board_index_bias_latest() returns jsonb` with the exact fields in the design spec.

- [ ] **Step 1: Write failing integration and migration-presence tests**

Add an integration test that inserts `THS:883423` and 35 consecutive board-bar fixtures with Decimal closes, invokes `select api_v1.query_board_index_bias_latest()`, and asserts latest date, MA5, BIAS5, previous BIAS5, `up`, 30 samples, extrema values, and most-recent tie dates. Add separate assertions for four-row history returning null calculated fields and empty history raising SQLSTATE `P0002`. Extend the migration catalog assertion with:

```python
("20260815000100_add_board_index_bias_api.sql", "query_board_index_bias_latest")
```

- [ ] **Step 2: Run tests and verify RED**

Run:

```powershell
uv run pytest tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -k board_index_bias -q
```

Expected: migration-presence and RPC tests fail because the migration/function does not exist.

- [ ] **Step 3: Add the minimal RPC migration**

Implement a stable security-definer function with `search_path = pg_catalog, api_v1, core` and `statement_timeout = '5s'`. Select the latest 34 rows for `THS:883423`, restore ascending order, compute rolling `count(close)` and `avg(close)` over four preceding rows, restrict extrema to the latest 30 observation rows, and build the documented JSON. Use ordered scalar subqueries with `ORDER BY bias DESC/ASC, trade_date DESC LIMIT 1` for deterministic extrema. Raise `P0002` when no latest row exists, revoke all execution from `public`, `anon`, and `authenticated`, and grant only to `market_data_api`.

- [ ] **Step 4: Run tests and verify GREEN**

Run the two Step 2 commands. Expected: all selected tests pass with exact Decimal results and SQLSTATE behavior.

- [ ] **Step 5: Commit the database contract**

```powershell
git add supabase/migrations/20260815000100_add_board_index_bias_api.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: add board index bias RPC"
```

### Task 2: FastAPI model, query service, and route

**Files:**
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `tests/test_public_api.py`

**Interfaces:**
- Consumes: `api_v1.query_board_index_bias_latest()` JSON payload.
- Produces: `BoardIndexBiasResponse`, `PublicQueryService.board_index_bias_latest()`, and `GET /api/v1/board-indexes/883423/bias`.

- [ ] **Step 1: Write failing HTTP contract tests**

Extend `FakeQueryService` with a zero-argument `board_index_bias_latest()` method returning a typed response. Assert that an authenticated GET returns Decimal fields as strings, exact date fields, `up`, sample count, extrema and `board_index_bias_v1`; assert no API key returns 401; assert query parameters are not part of the OpenAPI operation.

- [ ] **Step 2: Run the focused test and verify RED**

```powershell
uv run pytest tests/test_public_api.py -k board_index_bias -q
```

Expected: failure because the response model, service method, and route are absent.

- [ ] **Step 3: Implement the minimal API path**

Add `BoardIndexBiasResponse` with `Literal["THS:883423"]`, `Literal["883423"]`, `Literal[30]`, `Literal["board_index_bias_v1"]`, Decimal/date nullable fields, and `Literal["up", "down", "flat"] | None`. Add:

```python
QUERY_BOARD_INDEX_BIAS_LATEST = text(
    "select api_v1.query_board_index_bias_latest() as payload"
)
```

Validate the first payload row into the model, using the existing database error mapper for `P0002`. Register the fixed authenticated GET route with no handler parameters other than dependencies.

- [ ] **Step 4: Run focused public API tests and verify GREEN**

```powershell
uv run pytest tests/test_public_api.py -k "board_index_bias or top_gainers_20d" -q
```

Expected: selected tests pass and existing nearby RPC behavior remains unchanged.

- [ ] **Step 5: Commit the HTTP implementation**

```powershell
git add src/market_data_center/public_api/models.py src/market_data_center/public_api/queries.py src/market_data_center/public_api/app.py tests/test_public_api.py
git commit -m "feat: expose board index bias API"
```

### Task 3: Checked-in contracts and operating documentation

**Files:**
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `tests/test_api_contracts.py`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/同花顺动态板块指数采集.md`

**Interfaces:**
- Consumes: the accepted RPC and FastAPI response shape from Tasks 1-2.
- Produces: synchronized machine-readable and human-readable public contracts.

- [ ] **Step 1: Write failing contract assertions**

Assert the FastAPI contract contains `/api/v1/board-indexes/883423/bias` as a key-protected GET with no request parameters and the `BoardIndexBiasResponse` schema. Assert the PostgREST and agent contracts contain `query_board_index_bias_latest`, no arguments, the fixed algorithm version, and the Decimal fields represented as strings.

- [ ] **Step 2: Run contract tests and verify RED**

```powershell
uv run pytest tests/test_api_contracts.py -k board_index_bias -q
```

Expected: failure because the checked-in contracts do not yet expose the operation.

- [ ] **Step 3: Synchronize contracts and docs**

Regenerate the FastAPI OpenAPI file with:

```powershell
uv run python scripts/export_fastapi_openapi.py
```

Add the no-argument PostgREST RPC and one no-input Agent tool definition using the same field names and nullability. Document latest-stored-date behavior, exact formulas, insufficient-history behavior, 34-row bound, API-key requirement, and the absence of live fallback.

- [ ] **Step 4: Run contract tests and verify GREEN**

```powershell
uv run pytest tests/test_api_contracts.py -k board_index_bias -q
uv run pytest tests/test_production_checks.py -q
```

Expected: contract synchronization and repository production checks pass.

- [ ] **Step 5: Commit contracts and docs**

```powershell
git add contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json tests/test_api_contracts.py docs/FastAPI外部接口.md docs/同花顺动态板块指数采集.md
git commit -m "docs: publish board index bias contract"
```

### Task 4: Final verification

**Files:**
- Verify only: all changed files from Tasks 1-3

**Interfaces:**
- Consumes: the complete feature.
- Produces: evidence that formatting, linting, types, unit tests, and isolated PostgreSQL behavior pass.

- [ ] **Step 1: Run the complete local gate**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: every command exits zero with no warnings introduced by the change.

- [ ] **Step 2: Run isolated PostgreSQL integration tests**

```powershell
uv run pytest -m integration
```

Expected: integration suite exits zero against an isolated disposable database, never production.

- [ ] **Step 3: Verify repository state and commits**

```powershell
git diff --check
git status --short
git log -4 --oneline
```

Expected: no uncommitted feature changes, no whitespace errors, and the design plus implementation commits are visible.
