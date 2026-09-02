# DragonTiger v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace TradingBillboard v1 with provider-neutral DragonTiger facts, bounded replacement APIs, deterministic objective analytics, and time-safe model features.

**Architecture:** Keep the proven bounded HTTP/Raw/ingestion/scheduler mechanisms, but replace the v1 aggregate with Event, Reason, TradingSeat/Alias and merged SeatTrade facts. Build analytics as pure Decimal calculators over facts; require explicit `as_of_date` for profiles and keep forward-return labels in a separate type and contract.

**Tech Stack:** Python 3.12, dataclasses, Decimal, SQLAlchemy Core/psycopg 3, PostgreSQL SQL migrations, PostgREST RPC, FastAPI/Pydantic, pytest, Ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-09-02-dragon-tiger-v2-design.md`

## Global Constraints

- Governing work item is GitHub Issue #70; ADR-0049 is Accepted.
- Do not modify `supabase/migrations/20260824000200_create_trading_billboard.sql`; add `20260902000300_replace_trading_billboard_with_dragon_tiger.sql`.
- Standard symbols, `CN_A_SHARE`, Decimal, missing-value and lineage rules follow `AGENTS.md`.
- One successful ingestion has exactly one provider; never combine EastMoney and Tushare facts.
- Do not persist calculable net amounts, concentration ratios, subjective scores or labels in Fact tables.
- Remove the three v1 RPCs and `/api/v1/trading-billboard/*` routes without compatibility aliases.
- Never run production migration, ingestion or replay from this plan.

---

### Task 1: Governance and executable deletion contract

**Files:**
- Create: `docs/adr/ADR-0049-DragonTiger事实与时点安全特征.md`
- Create: `docs/领域详设-DragonTiger-2026-09-02.md`
- Modify: `docs/adr/README.md`
- Test: `tests/test_api_contracts.py`

**Interfaces:**
- Consumes: Issue #70 and owner confirmation that old contracts are removed.
- Produces: authoritative v2 names and a failing contract test requiring new RPC/routes and rejecting v1 routes.

- [ ] **Step 1: Add the accepted ADR and effective domain design**

Record `DAY|THREE_DAY`, nullable unresolved `seat_id`, explicit unknown-versus-zero amount semantics,
new bounded APIs, v1 deletion and constitution-based exclusion of subjective scores.

- [ ] **Step 2: Write the failing public-contract test**

```python
def test_dragon_tiger_replaces_trading_billboard_contracts() -> None:
    assert "query_dragon_tiger_events_by_date" in POSTGREST_RPC_NAMES
    assert "/api/v1/dragon-tiger/events/by-date" in fastapi_paths
    assert "query_trading_billboard_by_date" not in serialized_contracts
    assert "/api/v1/trading-billboard/by-date" not in fastapi_paths
```

- [ ] **Step 3: Run the test and verify RED**

Run: `python -m pytest tests/test_api_contracts.py::test_dragon_tiger_replaces_trading_billboard_contracts -q`
Expected: FAIL because checked-in contracts still expose v1.

### Task 2: Provider-neutral fact domain

**Files:**
- Delete: `src/market_data_center/domain/trading_billboard.py`
- Create: `src/market_data_center/domain/dragon_tiger.py`
- Replace tests: `tests/test_trading_billboard.py` → `tests/test_dragon_tiger.py`
- Modify: `src/market_data_center/providers/contracts.py`

**Interfaces:**
- Produces: `DragonTigerPeriodType`, `DragonTigerReasonType`, `TradingSeatType`, `DragonTigerReason`, `TradingSeat`, `TradingSeatAlias`, `SeatTradeRecord`, `DragonTigerEventRecord`, `validate_dragon_tiger_events()`, and content/natural-key helpers.

- [ ] **Step 1: Write failing tests for DAY and THREE_DAY invariants**

```python
def test_three_day_event_requires_calendar_period() -> None:
    event = event_record(
        period_type=DragonTigerPeriodType.THREE_DAY,
        period_start_date=date(2026, 8, 18),
        period_end_date=date(2026, 8, 20),
    )
    assert event.period_end_date == event.trade_date
```

- [ ] **Step 2: Write failing tests for merged/missing seat behavior**

```python
def test_missing_opposing_amount_is_not_pure_buy() -> None:
    trade = seat_trade(buy_amount=Decimal("100"), sell_amount=None)
    assert trade.net_amount is None
    assert trade.is_pure_buy is False


def test_disclosed_zero_sell_is_pure_buy() -> None:
    trade = seat_trade(buy_amount=Decimal("100"), sell_amount=Decimal("0"))
    assert trade.net_amount == Decimal("100")
    assert trade.is_pure_buy is True
```

- [ ] **Step 3: Run tests and verify RED**

Run: `python -m pytest tests/test_dragon_tiger.py -q`
Expected: collection import failure because `domain.dragon_tiger` does not exist.

- [ ] **Step 4: Implement minimal immutable records and validation**

Require standard symbols, legal ranks, nonnegative base facts, source identity, period boundaries, parent
identity, source/semantic uniqueness and known security/calendar membership. Do not special-case BSE.

- [ ] **Step 5: Run tests and verify GREEN**

Run: `python -m pytest tests/test_dragon_tiger.py -q`
Expected: PASS.

### Task 3: Pure analytics, profiles, Feature and Label separation

**Files:**
- Create: `src/market_data_center/dragon_tiger_analytics.py`
- Create: `src/market_data_center/dragon_tiger_features.py`
- Create: `tests/test_dragon_tiger_analytics.py`
- Create: `tests/test_dragon_tiger_features.py`

**Interfaces:**
- Produces: `calculate_dragon_tiger_capital_metrics(event)`, `build_trading_seat_profile(...)`,
  `build_dragon_tiger_feature(...)`, and `build_dragon_tiger_labels(...)`.

- [ ] **Step 1: Write failing literal-value metrics tests**

```python
assert metrics.top1_buy_concentration == Decimal("0.5")
assert metrics.top3_sell_concentration == Decimal("0.9")
assert metrics.buy_sell_overlap_count == 1
```

- [ ] **Step 2: Run metrics tests and verify RED**

Run: `python -m pytest tests/test_dragon_tiger_analytics.py -q`
Expected: import failure because analytics module is missing.

- [ ] **Step 3: Implement Decimal-only metrics**

Return `None` for missing/zero denominators; count pure sides only when the opposing side is explicitly zero;
sum institution/northbound amounts without converting missing values to zero facts.

- [ ] **Step 4: Write failing as-of leakage tests**

```python
profile = build_trading_seat_profile(history, labels, as_of_date=date(2026, 8, 20))
assert profile.t1_sample_count == 1  # excludes label available on 2026-08-21
assert "t1_return" not in DragonTigerFeature.__dataclass_fields__
```

- [ ] **Step 5: Implement profiles, features and separate labels**

Filter facts strictly before the feature event for historical profile inputs and include outcomes only when
`label_available_date <= as_of_date`. Feature accepts objective market inputs but never a Label object.

- [ ] **Step 6: Run analytics/feature tests and verify GREEN**

Run: `python -m pytest tests/test_dragon_tiger_analytics.py tests/test_dragon_tiger_features.py -q`
Expected: PASS.

### Task 4: EastMoney v2 Adapter and v1 Raw replay

**Files:**
- Delete: `src/market_data_center/providers/eastmoney_trading_billboard.py`
- Create: `src/market_data_center/providers/eastmoney_dragon_tiger.py`
- Replace: `tests/test_eastmoney_trading_billboard_provider.py` → `tests/test_eastmoney_dragon_tiger_provider.py`
- Modify: `src/market_data_center/reliability.py`
- Modify: `tests/test_reliability.py`

**Interfaces:**
- Produces: `EastmoneyDragonTigerAdapter.fetch_dragon_tiger(trade_date)`,
  `normalize_eastmoney_dragon_tiger_raw(rows, schema_version)` supporting v1 replay and v2 collection.

- [ ] **Step 1: Write failing normalizer tests**

Cover ordinary events, three-day classification, reliable-code cross-side merge, anonymous institutions kept
separate, missing opposing amounts, institution/northbound flags, Decimal parsing and deterministic replay.

- [ ] **Step 2: Run provider tests and verify RED**

Run: `python -m pytest tests/test_eastmoney_dragon_tiger_provider.py -q`
Expected: import failure because the new adapter is absent.

- [ ] **Step 3: Extract and implement the minimal reusable transport**

Carry over fixed endpoint, retry, pagination and byte-bound behavior unchanged; replace v1 model creation and
derived NET use. Emit `eastmoney.dragon_tiger.v2`; accept immutable `eastmoney.trading_billboard.v1` in replay.

- [ ] **Step 4: Run provider and replay tests and verify GREEN**

Run: `python -m pytest tests/test_eastmoney_dragon_tiger_provider.py tests/test_reliability.py -q`
Expected: PASS.

### Task 5: Tushare DragonTiger Adapter

**Files:**
- Create: `src/market_data_center/providers/tushare_dragon_tiger.py`
- Modify: `src/market_data_center/providers/tushare.py`
- Create: `tests/test_tushare_dragon_tiger_provider.py`

**Interfaces:**
- Produces: `TushareDragonTigerAdapter.fetch_dragon_tiger(trade_date)` using injected `top_list` and
  `top_inst` client calls and `tushare.dragon_tiger.v1` Raw rows.

- [ ] **Step 1: Write failing complete-shape mocked adapter tests**

Assert deterministic event identity, amounts in CNY, DAY/THREE_DAY mapping, anonymous seat preservation,
two-call atomic failure and absence of EastMoney fields from output.

- [ ] **Step 2: Run and verify RED**

Run: `python -m pytest tests/test_tushare_dragon_tiger_provider.py -q`
Expected: import failure because the adapter is missing.

- [ ] **Step 3: Implement minimal adapter without routing**

Call both APIs for one exact `YYYYMMDD`; convert values from their original string/Decimal representation;
derive source event IDs from canonical source facts; reject ambiguous detail-to-event joins.

- [ ] **Step 4: Run and verify GREEN**

Run: `python -m pytest tests/test_tushare_dragon_tiger_provider.py -q`
Expected: PASS.

### Task 6: Application service and PostgreSQL persistence replacement

**Files:**
- Delete: `src/market_data_center/trading_billboard_service.py`
- Delete: `src/market_data_center/persistence/trading_billboard_postgres.py`
- Create: `src/market_data_center/dragon_tiger_service.py`
- Create: `src/market_data_center/persistence/dragon_tiger_postgres.py`
- Replace: `tests/test_trading_billboard_service.py` → `tests/test_dragon_tiger_service.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `src/market_data_center/persistence/postgres.py`

**Interfaces:**
- Produces: `DragonTigerService.collect(trade_date)`, bounded `backfill(start_date, end_date)`, period-calendar
  resolution, reason/seat identity resolution and idempotent atomic event/trade persistence.

- [ ] **Step 1: Write failing service tests**

Test one-provider lineage, three-trading-day period lookup, raw-before-normalization, no partial persistence,
idempotent rerun and per-date backfill stop semantics.

- [ ] **Step 2: Run and verify RED**

Run: `python -m pytest tests/test_dragon_tiger_service.py -q`
Expected: import failure because service is missing.

- [ ] **Step 3: Implement application orchestration**

Construct `ProviderCode` from the selected adapter source, preserve batch request params/schema, resolve the
period with persistence-provided trading dates, validate once and commit one aggregate transaction.

- [ ] **Step 4: Implement SQLAlchemy persistence after service GREEN**

Upsert reason source aliases and reliable seats, assign UUIDs, insert/update event content hashes, replace
seat trades only for revised aggregates, and commit ingestion/manifest/quality atomically.

- [ ] **Step 5: Run focused tests**

Run: `python -m pytest tests/test_dragon_tiger_service.py tests/test_postgres_integration.py -q`
Expected: unit tests PASS; integration tests SKIP without `TEST_DATABASE_URL` or PASS against an isolated DB.

### Task 7: Ordered migration and new PostgREST RPCs

**Files:**
- Create: `supabase/migrations/20260902000300_replace_trading_billboard_with_dragon_tiger.sql`
- Modify: `tests/test_postgres_integration.py`

**Interfaces:**
- Produces: v2 internal tables and four bounded `api_v1` RPCs listed in ADR-0049; removes all v1 tables/RPCs.

- [ ] **Step 1: Add failing migration integration assertions**

Assert v2 constraints/RLS/grants/indexes, idempotent revision behavior, nested ordering, bounded validation,
and `to_regprocedure('api_v1.query_trading_billboard_by_date(...)') IS NULL`.

- [ ] **Step 2: Write the forward-only migration**

Revoke/drop old RPCs first, create v2 tables and functions with locked `search_path`, 5-second local timeout,
hard ranges and page bounds, then drop old seat/entry tables. Do not touch Raw or ingestion history.

- [ ] **Step 3: Run migration tests**

Run: `python -m pytest -m integration tests/test_postgres_integration.py -q`
Expected: SKIP only when `TEST_DATABASE_URL` is absent; otherwise PASS from an empty disposable database.

### Task 8: Replace FastAPI and checked-in contracts

**Files:**
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/public_api/openapi_zh.py`
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_api_contracts.py`

**Interfaces:**
- Produces: four `/api/v1/dragon-tiger/...` reads backed only by v2 `api_v1` RPCs.

- [ ] **Step 1: Extend failing API behavior tests**

Assert six-digit symbol normalization, UUID seat/event inputs, period enum validation, hard pagination/range
bounds, Decimal strings and absence of all old paths/models.

- [ ] **Step 2: Implement models/query service/routes**

Use Pydantic Decimal fields and typed nested seats; SQL text may call only `api_v1.query_dragon_tiger_*`.

- [ ] **Step 3: Regenerate/update all three contracts**

Derive FastAPI OpenAPI from the app and update PostgREST/Agent schemas to exact new RPC parameters and
results. Remove all `trading_billboard` identifiers.

- [ ] **Step 4: Run contract/API tests and verify GREEN**

Run: `python -m pytest tests/test_public_api.py tests/test_api_contracts.py -q`
Expected: PASS.

### Task 9: CLI, scheduler, operations and obsolete-code removal

**Files:**
- Modify: `src/market_data_center/cli.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/operations_service.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_operations.py`
- Modify: `README.md`

**Interfaces:**
- Produces: `dragon-tiger-collect` CLI, `dragon-tiger-daily` Worker catalog entry at 20:30 Shanghai, and v2
  operations statistics. Removes runtime imports and commands tied only to v1.

- [ ] **Step 1: Write failing CLI/scheduler/catalog tests**

Assert the new names and execution wiring, same opt-in/default-disabled behavior, trading-day skip and absence
of v1 command/job IDs.

- [ ] **Step 2: Implement minimal rewiring and delete dead imports**

Keep APScheduler ownership and existing source-rights opt-in gate. Rename dataset/workflow only through code
and the new migration constraint update; do not add OS scheduling instructions.

- [ ] **Step 3: Run focused tests**

Run: `python -m pytest tests/test_scheduler.py tests/test_operations.py tests/test_cli.py -q`
Expected: PASS.

### Task 10: Documentation consistency and complete verification

**Files:**
- Modify: `docs/adr/README.md`
- Modify: `README.md`
- Modify: v1 design documents to show `Superseded by ADR-0049` without rewriting history.

**Interfaces:**
- Produces: accurate current-state documentation and complete Issue #70 evidence.

- [ ] **Step 1: Scan for stale runtime/contract claims**

Run: `rg -n "query_trading_billboard|/api/v1/trading-billboard|TradingBillboardRecord|TradingBillboardSeatRecord" src tests contracts README.md docs`
Expected: matches only in explicitly superseded historical documents or negative migration tests.

- [ ] **Step 2: Run formatting and static checks**

Run: `python -m ruff format --check . && python -m ruff check . && python -m mypy src`
Expected: all commands exit 0.

- [ ] **Step 3: Run the complete local suite**

Run: `python -m pytest -q --basetemp=.tmp/pytest-final`
Expected: all available tests PASS; database tests may SKIP only for missing `TEST_DATABASE_URL`.

- [ ] **Step 4: Review diff and migration safety**

Run: `git diff --check && git status --short && git diff --stat`
Expected: no whitespace errors, no secrets/Raw data, and only Issue #70 files changed.
