# Regulation Rule and Calculation Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the accepted Regulation domain model, 26-row official rule catalog, six-table PostgreSQL boundary, pure Decimal calculators, T+1 projections, and atomic result persistence.

**Architecture:** Store official applicability and thresholds in `regulation.rule`, implement only three typed formula families in pure Python, and persist every logical result under one immutable calculation ID. Reuse the accepted price-limit catalog and corporate-action semantics instead of duplicating market rules inside Regulation.

**Tech Stack:** Python 3.12, dataclasses, Decimal, SQLAlchemy 2, PostgreSQL ordered migrations, pytest, Ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-09-02-regulation-warning-design.md`

## Global Constraints

- Follow Accepted `docs/adr/ADR-0048-沪深主板与创业板监管异动规则测算.md` and `docs/领域详设-Regulation-2026-09-02.md`.
- V1 evaluation begins on 2026-07-06 and supports only `SSE_MAIN`, `SZSE_MAIN`, and `GEM` ordinary stocks.
- Use standard `symbol`, `Decimal`, `date`, and timezone-aware `datetime`; never use `ts_code` or `float`.
- Calculators perform no database, network, filesystem, clock, UUID, locking, or transaction work.
- Thresholds and official source metadata live in ordered migration data; Python contains formula families but no 20/30/50/70/100/200 regulation constants.
- Calculated triggers never create official Regulation events or regulatory actions.
- Schema changes use ordered migrations only; do not run production migrations.
- Preserve unrelated user changes and do not enable production collection or Worker schedules.

---

### Task 1: Provider-Neutral Regulation Domain Contracts

**Files:**

- Create: `src/market_data_center/domain/regulation.py`
- Modify: `src/market_data_center/domain/__init__.py`
- Test: `tests/test_regulation.py`

**Interfaces:**

- Produces enums `RegulationSegment`, `RegulationRuleLevel`, `RegulationRuleKind`, `RegulationDirection`, `RegulationResetLevel`, `CalculatedRegulationState`, `AnnouncedRegulationState`, `RegulationApplicability`, `RegulationEvaluationState`, `RegulationReachability`, `RegulationScenarioCode`, and `RegulationRunStatus`.
- Produces immutable records `RegulationRule`, `RegulationEventRecord`, `RegulationDailyReturn`, `RegulationCandidate`, `RegulationCalculationInput`, `RegulationStatusResult`, `RegulationRuleResult`, `RegulationWarningResult`, `RegulationCoverage`, and `RegulationCalculationOutput`.
- Produces `validate_regulation_rules(rules, trade_date)` and `regulation_event_natural_key(record)`.

- [ ] **Step 1: Write failing enum and record validation tests**

Add tests constructing one cumulative-deviation rule, one turnover rule, and one count rule. Assert kind-specific fields are required and mutually exclusive, UP thresholds are positive, DOWN thresholds are negative, dates are ordered, hashes are lowercase SHA-256, event periods are ordered, and `Direction.NONE` is accepted only for turnover rules.

Use exact values:

```python
rule = RegulationRule(
    rule_code="SSE_MAIN_ABNORMAL_3D_DEV_UP",
    exchange=Exchange.SSE,
    segment=RegulationSegment.SSE_MAIN,
    level=RegulationRuleLevel.ABNORMAL,
    kind=RegulationRuleKind.CUMULATIVE_DEVIATION,
    direction=RegulationDirection.UP,
    window_days=3,
    threshold_pct=Decimal("20"),
    comparison_window_days=None,
    ratio_threshold=None,
    secondary_threshold_pct=None,
    count_window_days=None,
    required_count=None,
    counted_event_kind=None,
    reset_level=RegulationResetLevel.ABNORMAL,
    benchmark_symbol="SSE:000002",
    rule_set_version="cn-a-share-regulation-2026-07-06.v1",
    effective_date=date(2026, 7, 6),
    expire_date=None,
    source_document="上海证券交易所交易规则（2026年修订）",
    source_clause="5.4.2(1)",
    source_url="https://www.sse.com.cn/",
    enabled=True,
)
```

- [ ] **Step 2: Run domain tests RED**

Run: `uv run pytest tests/test_regulation.py -q`

Expected: import failure because `domain.regulation` does not exist.

- [ ] **Step 3: Implement enums, immutable records, and invariants**

Use `StrEnum` and `@dataclass(frozen=True, slots=True)`. Keep database IDs and lineage out of Domain records. Validate all nonblank strings, positive prices/factors, unique rules by code, unique candidate symbols, sorted/unique trading dates, and timezone-aware event timestamps.

`validate_regulation_rules()` must reject:

- no enabled rule covering the requested date;
- duplicate rule codes;
- duplicate `(segment, level, kind, direction, rule window, comparison window)` for the
  same date; distinct official 10-day and 30-day windows must coexist;
- benchmark missing on cumulative-deviation rules;
- any rule before 2026-07-06;
- turnover or event-count fields present on the wrong kind.

- [ ] **Step 4: Export the public Domain names**

Add explicit imports and `__all__` entries in `domain/__init__.py`; do not wildcard-import the module.

- [ ] **Step 5: Run domain tests and static checks GREEN**

Run: `uv run pytest tests/test_regulation.py -q`

Run: `uv run ruff check src/market_data_center/domain/regulation.py tests/test_regulation.py`

Run: `uv run mypy src/market_data_center/domain/regulation.py`

Expected: all pass.

- [ ] **Step 6: Commit the Domain contracts**

```powershell
git add src/market_data_center/domain/regulation.py src/market_data_center/domain/__init__.py tests/test_regulation.py
git commit -m "feat: define regulation domain contracts"
```

---

### Task 2: Reusable Mainboard and GEM Price-Limit Inputs

**Files:**

- Modify: `src/market_data_center/domain/stock_pool.py`
- Modify: `src/market_data_center/stock_pool_calculator.py`
- Modify: `tests/test_stock_pool.py`

**Interfaces:**

- Extends `price_limit_rule(exchange, trade_date, *, board="mainboard")` with board-aware lookup.
- Produces `calculate_price_limit_range(previous_close, ratio, price_tick)` returning `(lower_limit, upper_limit)` as Decimal.
- Keeps existing mainboard stock-pool behavior byte-for-byte compatible.

- [ ] **Step 1: Write failing GEM and compatibility tests**

Assert:

```python
gem = price_limit_rule(Exchange.SZSE, date(2026, 9, 3), board="gem")
assert gem.regular_ratio == Decimal("0.20")
assert gem.price_tick == Decimal("0.01")
assert gem.initial_no_limit_trading_days == 5

main = price_limit_rule(Exchange.SSE, date(2026, 9, 3))
assert main.regular_ratio == Decimal("0.10")
```

Also assert SSE+GEM and dates before 2026-07-06 fail, and `calculate_price_limit_range()` preserves the ADR-0021 half-up/minimum-one-tick semantics.

- [ ] **Step 2: Run stock-pool tests RED**

Run: `uv run pytest tests/test_stock_pool.py -q`

Expected: GEM lookup and public range helper are absent.

- [ ] **Step 3: Add the versioned GEM rule and extract the pure helper**

Add rule version `CN_GEM_2026_07_06` for SZSE GEM, regular/ST ratio `Decimal("0.20")`, tick `Decimal("0.01")`, and five no-limit trading days. Move the existing `_round_limit` pair into `calculate_price_limit_range()` and call it from `calculate_mainboard_stock_pools()` so Regulation and stock pools share one implementation.

- [ ] **Step 4: Run focused regression tests GREEN**

Run: `uv run pytest tests/test_stock_pool.py tests/test_calculators.py -q`

Run: `uv run mypy src/market_data_center/domain/stock_pool.py src/market_data_center/stock_pool_calculator.py`

Expected: existing mainboard and new GEM tests pass.

- [ ] **Step 5: Commit price-limit reuse**

```powershell
git add src/market_data_center/domain/stock_pool.py src/market_data_center/stock_pool_calculator.py tests/test_stock_pool.py
git commit -m "feat: add versioned GEM price-limit inputs"
```

---

### Task 3: Regulation Schema and 26-Row Official Rule Catalog

**Files:**

- Create: `supabase/migrations/20260902000100_create_regulation.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Produces internal tables `regulation.rule`, `regulation.event`, `regulation.calculation_run`, `regulation.status`, `regulation.rule_result`, and `regulation.warning`.
- Produces exactly 26 rules for rule set `cn-a-share-regulation-2026-07-06.v1`.
- Grants internal read/write only to `market_data_worker`; grants no public/API table reads.

- [ ] **Step 1: Write failing migration structure tests**

In `test_production_checks.py`, read the exact migration and assert it creates all six tables, enables RLS, contains 26 explicit rule inserts, records both official document URLs and clauses, and contains no `HIGH`, `MEDIUM`, `LOW`, `ts_code`, executable JSON expression, or public/internal-table grant.

- [ ] **Step 2: Write failing PostgreSQL constraints tests**

In an isolated migrated database, assert:

- catalog counts are 9 SSE_MAIN, 9 SZSE_MAIN, and 8 GEM;
- the active date is 2026-07-06 and all 26 rows share the accepted rule-set version;
- duplicate/overlapping rules, wrong-sign thresholds, invalid kind-specific parameter combinations, invalid event periods, mixed calculation IDs, negative distances, and invalid scenario/reachability pairs are rejected;
- API roles cannot select any Regulation table;
- worker can select/insert required rows but cannot mutate referenced rule semantics.

- [ ] **Step 3: Run migration tests RED**

Run: `uv run pytest tests/test_production_checks.py -k regulation -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_schema -q`

Expected: failures because the migration is absent.

- [ ] **Step 4: Implement schema, constraints, indexes, and grants**

Use PostgreSQL `numeric`, `date`, `timestamptz`, enums-as-checked-text, UUID primary keys, explicit foreign keys, and `btree_gist` date-range exclusion for active rule dimensions. Add lookup indexes for:

```text
rule(exchange, segment, effective_date, expire_date)
event(symbol, period_end_date desc, event_level, direction)
calculation_run(trade_date, completed_at desc)
status(calculation_id, symbol)
rule_result(calculation_id, symbol, rule_id)
warning(calculation_id, symbol, rule_id, scenario_code)
```

Create all 26 inserts exactly as listed in the domain design. Do not store scenario -2/0/+2 in `regulation.rule`.

- [ ] **Step 5: Run schema tests GREEN**

Run: `uv run pytest tests/test_production_checks.py -k regulation -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_schema -q`

Expected: all pass against `TEST_DATABASE_URL` only.

- [ ] **Step 6: Commit schema and official rules**

```powershell
git add supabase/migrations/20260902000100_create_regulation.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: persist official regulation rule catalog"
```

---

### Task 4: Pure Cumulative-Deviation and Window Selection Calculator

**Files:**

- Create: `src/market_data_center/regulation_calculator.py`
- Create: `tests/test_regulation_calculator.py`

**Interfaces:**

- Produces `calculate_regulation(source: RegulationCalculationInput) -> RegulationCalculationOutput`.
- Internally exposes no I/O ports; tests call only the public calculator and pure price-limit helper.

- [ ] **Step 1: Write Golden Tests for compounding and maximum windows**

Create fixtures with exact Decimal factors and assert:

- a two-session window can trigger a three-session rule;
- a six-session window can trigger a ten-session rule;
- compounded stock/index returns differ from summing daily deviations;
- UP selects the maximum legal window and DOWN the minimum;
- equality triggers;
- no selected window crosses its reset date;
- a gap in required exchange trading dates yields `INSUFFICIENT_DATA` instead of compressing dates.

One exact equality fixture should use stock factor `1.21`, index factor `1.01`, and threshold `20`, producing exactly `20.00` percentage points.

- [ ] **Step 2: Run calculator tests RED**

Run: `uv run pytest tests/test_regulation_calculator.py -k "deviation or window" -q`

Expected: import failure because the calculator is absent.

- [ ] **Step 3: Implement deterministic window enumeration**

Sort by trading calendar position, require one stock and benchmark return for every date in a candidate window, enumerate lengths `1..window_days`, reject starts before the selected reset boundary, compound with `Decimal(1)`, and store the selected start/end/day count. Distance is zero when triggered; otherwise use `threshold-current` for UP and `current-threshold` for DOWN.

- [ ] **Step 4: Run deviation tests GREEN**

Run: `uv run pytest tests/test_regulation_calculator.py -k "deviation or window" -q`

Run: `uv run ruff check src/market_data_center/regulation_calculator.py tests/test_regulation_calculator.py`

Expected: all pass.

- [ ] **Step 5: Commit cumulative-deviation calculation**

```powershell
git add src/market_data_center/regulation_calculator.py tests/test_regulation_calculator.py
git commit -m "feat: calculate regulation deviation windows"
```

---

### Task 5: Turnover, Official Event Count, and Independent States

**Files:**

- Modify: `src/market_data_center/regulation_calculator.py`
- Modify: `tests/test_regulation_calculator.py`

**Interfaces:**

- Extends `calculate_regulation()` to all three accepted rule kinds.
- Produces independent `calculated_state` and `announced_state` per symbol.

- [ ] **Step 1: Write failing turnover and count tests**

Assert the turnover rule triggers only when both `latest_average/prior_average >= 30` and latest-three cumulative turnover `>= 20`; a zero prior average or any missing value is incomplete. Assert count rules include only official price-deviation events matching direction and the ten-session calendar, exclude turnover/unknown-direction events, and respect the serious reset date.

Assert calculated and announced states can differ:

```text
SERIOUS_TRIGGERED + NONE
NORMAL + ABNORMAL
ABNORMAL_TRIGGERED + SERIOUS_ABNORMAL
```

- [ ] **Step 2: Run focused tests RED**

Run: `uv run pytest tests/test_regulation_calculator.py -k "turnover or event_count or state" -q`

Expected: missing rule-kind behavior.

- [ ] **Step 3: Implement the two typed evaluators and state reducer**

Require exactly eight consecutive turnover observations. Count distinct official event natural keys by `period_end_date`; never inspect calculated abnormal rule results when computing current official count. Reduce calculated state by serious-before-abnormal precedence and announced state exclusively from events at or below the input watermark.

- [ ] **Step 4: Run full calculator tests GREEN**

Run: `uv run pytest tests/test_regulation_calculator.py -q`

Run: `uv run mypy src/market_data_center/regulation_calculator.py`

Expected: all pass.

- [ ] **Step 5: Commit typed rule families**

```powershell
git add src/market_data_center/regulation_calculator.py tests/test_regulation_calculator.py
git commit -m "feat: evaluate turnover and official event counts"
```

---

### Task 6: T+1 Scenario Solver and Objective Warning Selection

**Files:**

- Modify: `src/market_data_center/regulation_calculator.py`
- Modify: `tests/test_regulation_calculator.py`

**Interfaces:**

- Adds fixed scenario configuration version `regulation-scenarios.v1` with Decimal `-0.02`, `0`, `0.02`.
- Produces one dominant warning per `(symbol, direction, level, scenario)` plus all current triggers.

- [ ] **Step 1: Write failing T+1 Golden Tests**

Cover all three index scenarios and assert:

```text
x = (tau + B) / A - 1
raw_trigger_price = next_day_reference_price * (1 + x)
```

Also assert the oldest day rolls out, the T+1 best start can differ from the T-day best start, UP uses ceiling-to-tick, DOWN uses floor-to-tick, the rounded value is re-evaluated, mainboard 10% and GEM 20% produce different reachability, already-triggered rules use CURRENT, turnover uses NONE/NOT_PRICE_CALCULABLE, and one-short count rules set `requires_official_event_confirmation`.

- [ ] **Step 2: Run T+1 tests RED**

Run: `uv run pytest tests/test_regulation_calculator.py -k "scenario or trigger_price or reachability or warning" -q`

Expected: projection fields are absent.

- [ ] **Step 3: Implement solver, rounding, reachability, and dominance**

Enumerate tomorrow-valid starts separately from current windows. Use `ROUND_CEILING` for UP and `ROUND_FLOOR` for DOWN at the supplied tick, then recompute deviation and assert the inclusive threshold. Compare with the supplied `DailyPriceLimit`. Sort warning candidates by required absolute price move then rule code; retain every current trigger regardless of dominance.

- [ ] **Step 4: Implement deterministic messages**

Use stable template codes, Decimal string formatting, and the required disclaimer. Count-rule messages append the exchange-confirmation condition. Do not accept model-generated or caller-provided free text.

- [ ] **Step 5: Run calculator and stock-pool regression tests GREEN**

Run: `uv run pytest tests/test_regulation_calculator.py tests/test_stock_pool.py -q`

Run: `uv run ruff format --check src/market_data_center/regulation_calculator.py tests/test_regulation_calculator.py`

Expected: all pass.

- [ ] **Step 6: Commit T+1 projections**

```powershell
git add src/market_data_center/regulation_calculator.py tests/test_regulation_calculator.py
git commit -m "feat: solve next-session regulation triggers"
```

---

### Task 7: Rule Loading and Atomic Calculation Persistence

**Files:**

- Create: `src/market_data_center/persistence/regulation_postgres.py`
- Modify: `src/market_data_center/persistence/__init__.py`
- Create: `tests/test_regulation_persistence.py`
- Modify: `tests/test_postgres_integration.py`

**Interfaces:**

- Produces `PostgreSQLRegulationPersistence.load_active_rules(trade_date)`.
- Produces `find_calculation(trade_date, input_hash)` for idempotency.
- Produces `publish_calculation(run, output)` that writes one calculation and its status/rule-result/warning rows atomically.
- Produces `mark_calculation_failed(calculation_id, completed_at)` without publishing child rows.

- [ ] **Step 1: Write failing fake-port and integration tests**

Assert rule rows map to Domain without database IDs leaking into calculators; active-date lookup returns one rule-set version and fails on mixed versions. Assert identical `(trade_date,input_hash)` reuses the completed calculation, changed input creates a new ID, child rows never mix IDs, PARTIAL publishes coverage metadata, and an injected warning insert error rolls back the calculation and every child row.

- [ ] **Step 2: Run persistence tests RED**

Run: `uv run pytest tests/test_regulation_persistence.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_persistence -q`

Expected: persistence module is absent.

- [ ] **Step 3: Implement strict row mappers and transaction methods**

Use SQLAlchemy `Engine.begin()`, explicit column lists, Decimal-preserving bindings, stable row ordering, and bulk inserts. Compute no regulation values in persistence. Reject publish if output trade date, next trade date, counts, rule IDs, or input hash disagree with the run.

- [ ] **Step 4: Run persistence tests GREEN**

Run: `uv run pytest tests/test_regulation_persistence.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_persistence -q`

Expected: all pass.

- [ ] **Step 5: Commit calculation persistence**

```powershell
git add src/market_data_center/persistence/regulation_postgres.py src/market_data_center/persistence/__init__.py tests/test_regulation_persistence.py tests/test_postgres_integration.py
git commit -m "feat: persist versioned regulation calculations"
```

---

### Task 8: Core Verification and Handoff

**Files:**

- Verify all files changed by Tasks 1–7.
- Do not modify source-ingestion, Worker, FastAPI, or contract files in this plan.

**Interfaces:**

- Produces a tested core consumed by `2026-09-02-regulation-ingestion-worker.md`.

- [ ] **Step 1: Run focused core tests**

```powershell
uv run pytest tests/test_regulation.py tests/test_regulation_calculator.py tests/test_regulation_persistence.py tests/test_stock_pool.py tests/test_production_checks.py -q
```

Expected: all pass.

- [ ] **Step 2: Run static gates**

```powershell
uv run ruff format --check src/market_data_center/domain/regulation.py src/market_data_center/regulation_calculator.py src/market_data_center/persistence/regulation_postgres.py tests/test_regulation.py tests/test_regulation_calculator.py tests/test_regulation_persistence.py
uv run ruff check src tests
uv run mypy src
```

Expected: all exit zero.

- [ ] **Step 3: Run isolated PostgreSQL tests**

Verify `TEST_DATABASE_URL` is an isolated disposable database and not the production URL, then run:

```powershell
uv run pytest -m integration tests/test_postgres_integration.py -k regulation -q
```

Expected: all Regulation integration tests pass.

- [ ] **Step 4: Inspect scope and safety**

Run:

```powershell
git diff --check origin/master...HEAD
git status --short
rg -n "ts_code|HIGH|MEDIUM|LOW|if .*100|if .*200" src/market_data_center/domain/regulation.py src/market_data_center/regulation_calculator.py
```

Confirm no regulation threshold is hardcoded in Python, no official event is derived from bars, no production migration ran, and no unrelated file changed.

- [ ] **Step 5: Commit verification-only fixes if required**

Stage only files in this plan and commit `fix: complete regulation core verification`. If no fix was needed, do not create an empty commit.

- [ ] **Step 6: Prepare handoff evidence**

Report focused/static/integration commands, the exact migration, rule-row counts, algorithm/scenario versions, and any unavailable gate with its exact reason. Do not claim source collection or public API completion; those belong to the next two plans.
