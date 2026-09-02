# Regulation Warning Public API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish exact-date, calculation-version-coherent Regulation warnings through one bounded `api_v1` RPC and an API-key-protected read-only FastAPI route, with synchronized checked-in contracts.

**Architecture:** PostgreSQL selects one completed calculation and enforces filtering, keyset pagination, permissions, timeout, and error semantics. FastAPI validates HTTP inputs, calls only the RPC, and maps the JSON envelope into Decimal-safe Pydantic models without reading internal schemas or triggering calculation.

**Tech Stack:** PostgreSQL PL/pgSQL/JSONB, SQLAlchemy 2, FastAPI, Pydantic v2, OpenAPI 3.1, pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-regulation-warning-design.md`

## Global Constraints

- Complete the Regulation core and source/Worker plans before this plan.
- Require exact `trade_date >= 2026-07-06`; never fall back to another date or merge calculation IDs.
- Public reads expose objective rule conditions only, never internal tables, provider fields, Raw paths, subjective risk levels, or regulatory-action predictions.
- `limit` is 1–500 and pagination is stable keyset pagination over `(symbol, rule_code, scenario_code)`.
- PostgreSQL uses a fixed 5-second statement timeout and safe SQLSTATE values; FastAPI remains read-only and API-key protected.
- Decimal values serialize with the repository’s existing exact string semantics.
- Synchronize PostgREST, Agent Tools, and FastAPI contracts in the same reviewed change.
- Do not run production migrations or deploy the API.

---

### Task 1: Bounded `api_v1.query_regulation_warnings` RPC

**Files:**

- Create: `supabase/migrations/20260902000300_query_regulation_warnings.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Produces `api_v1.query_regulation_warnings(date,text,text,text,text,text,text,text,text,integer) -> jsonb`.
- Accepts exact trade date plus nullable exchange, segment, symbol, rule code, calculated state, announced state, reachability, opaque cursor, and limit.
- Returns envelope metadata and warning items from one calculation ID.

- [ ] **Step 1: Write failing RPC result tests**

Seed two completed calculations for one date plus one failed calculation. Assert the RPC selects only the newest completed SUCCEEDED/PARTIAL calculation, never combines rows from the older/failed IDs, and returns:

```json
{
  "trade_date": "2026-09-02",
  "calculation_id": "00000000-0000-0000-0000-000000000001",
  "algorithm_version": "1.0.0",
  "rule_set_version": "cn-a-share-regulation-2026-07-06.v1",
  "scenario_config_version": "regulation-scenarios.v1",
  "event_watermark": "2026-09-02T22:20:00+08:00",
  "calculation_status": "succeeded",
  "expected_count": 1,
  "complete_count": 1,
  "incomplete_count": 0,
  "not_applicable_count": 0,
  "returned_count": 1,
  "has_more": false,
  "next_cursor": null,
  "items": []
}
```

Items must contain every public field listed in the domain design and no ingestion ID, internal rule ID, Raw identity, provider code, or internal schema name.

- [ ] **Step 2: Write failing input, cursor, and permission tests**

Assert SQLSTATE `22023` for a null/pre-effective date, invalid enum/filter, blank symbol/rule, limit outside 1–500, malformed cursor, or cursor/filter mismatch. Assert `P0002` when no completed calculation exists on the exact date. Test two pages with no duplicates/skips and stable tuple ordering. Assert API roles can execute only the function and cannot select Regulation tables.

- [ ] **Step 3: Run RPC tests RED**

Run: `uv run pytest tests/test_production_checks.py -k regulation_warning -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_warning_rpc -q`

Expected: migration/function absent.

- [ ] **Step 4: Implement the locked read function**

Create a `stable security definer` function with a fixed safe `search_path`, `set statement_timeout='5s'`, explicit argument validation, and a single selected CalculationRun CTE. Revoke execute from public/anon/authenticated and grant only to configured read roles if present.

Encode cursor JSON containing version `1`, last symbol/rule/scenario, and an MD5 of normalized filters using PostgreSQL base64. Decode and validate before applying tuple `>` comparison. Cursor integrity is an input-consistency check, not authentication; database permissions remain the security boundary.

- [ ] **Step 5: Build deterministic envelope and Decimal-safe items**

Order by `(symbol, rule_code, scenario_code)`, fetch `limit+1`, emit only `limit`, set `has_more`, and derive the next cursor from the last emitted row. Use `jsonb_build_object` with numeric database values unchanged; clients parse them into Decimal.

- [ ] **Step 6: Run RPC tests GREEN**

Run: `uv run pytest tests/test_production_checks.py -k regulation_warning -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_warning_rpc -q`

- [ ] **Step 7: Commit the database read contract**

```powershell
git add supabase/migrations/20260902000300_query_regulation_warnings.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: query versioned regulation warnings"
```

---

### Task 2: FastAPI Pydantic Models and Query Service

**Files:**

- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `tests/test_public_api.py`

**Interfaces:**

- Produces `RegulationWarningItem` and `RegulationWarningPageResponse`.
- Adds `PublicQueryService.regulation_warnings(trade_date, exchange, segment, symbol, rule_code, calculated_state, announced_state, reachability, cursor, limit) -> RegulationWarningPageResponse`.
- SQLAlchemy calls only `api_v1.query_regulation_warnings`.

- [ ] **Step 1: Write failing model tests**

Create one complete payload with Decimal prices/percentages, nullable trigger fields for CURRENT/NOT_PRICE_CALCULABLE, timezone-aware event watermark, UUID calculation ID, all objective enums, and coverage counts. Assert exact JSON Decimal strings and rejection of an inconsistent item such as `REACHABLE_NEXT_SESSION` with null trigger price.

- [ ] **Step 2: Write failing query-service tests**

Use a fake SQLAlchemy engine/connection and assert parameter names and values match the RPC, statement timeout is 5,000 ms, payload validation occurs once, `22023` maps to `PublicQueryInvalid`, `P0002` to `PublicQueryNotFound`, `57014` to `PublicQueryTimeout`, and error text exposes no SQL/internal schema.

- [ ] **Step 3: Run model/query tests RED**

Run: `uv run pytest tests/test_public_api.py -k regulation -q`

Expected: models and service method absent.

- [ ] **Step 4: Implement exact models**

Use existing `ApiModel` conventions. Define Literal/StrEnum fields for segment, level, direction, states, scenarios, reachability, and completeness. Add model validators for CURRENT, price-calculable scenarios, NONE/NOT_PRICE_CALCULABLE, count confirmation, nonnegative distance, and count consistency.

- [ ] **Step 5: Implement the query method**

Add one SQLAlchemy `text()` statement selecting the JSON payload from the RPC. Pass all nullable filters without constructing SQL fragments. Validate the first row through `RegulationWarningPageResponse`; treat a missing/null payload as not found.

- [ ] **Step 6: Run tests and mypy GREEN**

Run: `uv run pytest tests/test_public_api.py -k regulation -q`

Run: `uv run mypy src/market_data_center/public_api/models.py src/market_data_center/public_api/queries.py`

- [ ] **Step 7: Commit models and query service**

```powershell
git add src/market_data_center/public_api/models.py src/market_data_center/public_api/queries.py tests/test_public_api.py
git commit -m "feat(api): model regulation warning reads"
```

---

### Task 3: Authenticated FastAPI Route and Chinese OpenAPI

**Files:**

- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/public_api/openapi_zh.py`
- Modify: `tests/test_public_api.py`

**Interfaces:**

- Produces `GET /api/v1/regulation/warnings`.
- Uses existing API-key dependency and public query service only.

- [ ] **Step 1: Write failing route tests**

Test an authenticated full query and every optional filter. Assert missing/invalid API key is 401, missing date and invalid enums/limit are 422, database `P0002` becomes safe 404, invalid cursor becomes safe 400, timeout becomes 504, and no request triggers network, Worker, or write methods.

- [ ] **Step 2: Run route tests RED**

Run: `uv run pytest tests/test_public_api.py -k regulation_warning_route -q`

- [ ] **Step 3: Implement route validation and delegation**

Add date, enum, symbol, rule, cursor, and limit query parameters. Require standard symbol syntax `^(SSE|SZSE):[0-9]{6}$`; do not accept ambiguous six-digit codes on this route. Delegate all parameters unchanged to `service.regulation_warnings()`.

- [ ] **Step 4: Add Chinese OpenAPI copy**

Describe the three index scenarios, exact-date/no-fallback behavior, calculation-vs-announcement separation, conditional count confirmation, price-limit reachability, Decimal strings, cursor semantics, and mandatory disclaimer. Do not use “预测”“一定停牌” or subjective risk grades as output semantics.

- [ ] **Step 5: Run route/OpenAPI tests GREEN**

Run: `uv run pytest tests/test_public_api.py -k regulation -q`

Run: `uv run ruff check src/market_data_center/public_api tests/test_public_api.py`

- [ ] **Step 6: Commit the route**

```powershell
git add src/market_data_center/public_api/app.py src/market_data_center/public_api/openapi_zh.py tests/test_public_api.py
git commit -m "feat(api): expose regulation warnings"
```

---

### Task 4: Synchronize Three Checked-In Contracts

**Files:**

- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `tests/test_api_contracts.py`
- Modify: `scripts/check_fastapi_release.py`

**Interfaces:**

- Publishes one PostgREST RPC/tool and one FastAPI path with matching bounds and response semantics.

- [ ] **Step 1: Extend contract tests RED**

Assert PostgREST and Agent sets both contain `query_regulation_warnings`, exact required/optional parameters, date floor, enum values, limit maximum 500, cursor, response metadata, Decimal fields, and disclaimer. Assert FastAPI contains `/api/v1/regulation/warnings` with API-key security and the same enum/bounds. Assert no internal schema/table, ingestion ID, provider field, Raw path, secret, `ts_code`, HIGH/MEDIUM/LOW, or regulatory-action prediction appears.

- [ ] **Step 2: Run contract tests RED**

Run: `uv run pytest tests/test_api_contracts.py -k regulation -q`

- [ ] **Step 3: Update PostgREST and Agent contracts manually and deterministically**

Add the exact RPC request and response schemas; keep the PostgREST/Agent endpoint sets synchronized. Agent Tools remains a read-only JSON schema, not MCP, and contains no base URL or credentials.

- [ ] **Step 4: Regenerate FastAPI OpenAPI**

Run: `uv run python scripts/export_fastapi_openapi.py`

Review the diff and verify only intended Regulation path/schema plus deterministic ordering changes.

- [ ] **Step 5: Run contract/release tests GREEN**

Run: `uv run pytest tests/test_api_contracts.py tests/test_production_checks.py -q`

Run: `uv run python scripts/check_fastapi_release.py`

- [ ] **Step 6: Commit contracts**

```powershell
git add contracts tests/test_api_contracts.py scripts/check_fastapi_release.py
git commit -m "docs: publish regulation warning contracts"
```

---

### Task 5: Consumer and Operator Documentation

**Files:**

- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/PostgREST-api_v1权限验证.md`
- Modify: `docs/数据库导航.md`
- Modify: `README.md`

**Interfaces:**

- Produces accurate documentation for implemented, read-only, exact-date contracts.

- [ ] **Step 1: Document request and response examples**

Add examples for a flat-index reachable price rule, an already-current trigger, a count rule requiring exchange confirmation, a turnover `NOT_PRICE_CALCULABLE` result, partial coverage, no exact-date calculation, and next-cursor pagination. Use standard symbol and Decimal strings.

- [ ] **Step 2: Document safety and semantics**

State that API cannot collect/recalculate/write, no date fallback exists, one response uses one calculation ID, official event state is independent, and trigger conditions are not price/regulatory-action predictions. Document the API role’s execute-only permission and five-second timeout.

- [ ] **Step 3: Check documentation against OpenAPI**

Run searches for every public field and enum in docs and contract; correct any spelling/unit mismatch. Do not document the endpoint as deployed until deployment actually occurs.

- [ ] **Step 4: Commit public documentation**

```powershell
git add docs/FastAPI外部接口.md docs/PostgREST-api_v1权限验证.md docs/数据库导航.md README.md
git commit -m "docs: explain regulation warning reads"
```

---

### Task 6: Complete Verification and Handoff

**Files:**

- Verify all files changed by this and the two prerequisite plans.
- Do not enable production migration, source collection, Worker schedules, or deployment.

**Interfaces:**

- Produces evidence-backed completion of Issue #69’s code and contract scope.

- [ ] **Step 1: Run focused public-contract tests**

```powershell
uv run pytest tests/test_public_api.py tests/test_api_contracts.py tests/test_production_checks.py -k "regulation or fastapi_openapi_contract" -q
```

- [ ] **Step 2: Run complete local gate**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all commands exit zero.

- [ ] **Step 3: Run isolated PostgreSQL integration gate**

Verify `TEST_DATABASE_URL` is an isolated disposable database and not production, then run:

```powershell
uv run pytest -m integration
```

- [ ] **Step 4: Inspect contract and safety boundaries**

```powershell
git diff --check origin/master...HEAD
git status --short
rg -n "regulation\." src/market_data_center/public_api contracts docs/FastAPI外部接口.md
rg -n "HIGH|MEDIUM|LOW|ts_code|一定停牌|必然监管" src contracts docs
rg -n "cron|Task Scheduler" docs README.md deploy src tests
```

Confirm FastAPI references only the `api_v1` RPC statement, contract files contain no internal schemas or secrets, warnings have no subjective grade, and no production action occurred.

- [ ] **Step 5: Commit verification-only fixes if required**

If scoped fixes were needed, commit them as `fix: complete regulation warning verification`. If no fix was needed, create no empty commit.

- [ ] **Step 6: Prepare final handoff**

Report all focused/full/integration command results, three migrations, route/RPC, contracts, default-disabled jobs, source-rights and protected-production blockers, and any command that could not run with its exact reason. Do not push, open a PR, run migrations, enable sources/jobs, or deploy unless separately authorized.
