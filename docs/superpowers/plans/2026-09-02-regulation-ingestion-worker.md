# Regulation Sources and Worker Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Collect the three benchmark indices and official SSE/SZSE Regulation events with immutable Raw lineage, assemble calculation inputs, publish core results, and run controlled post-close and pre-open Worker workflows.

**Architecture:** Keep each exchange and benchmark request in its own ingestion run, normalize provider fields at the adapter boundary, and let a service load a repeatable objective input snapshot for the pure calculator from the core plan. Scheduled jobs remain code-owned, opt-in, and observable through Operations.

**Tech Stack:** Python 3.12, BaoStock, HTTP client primitives already used by the repository, SQLAlchemy 2, immutable JSONL Raw Store, APScheduler, PostgreSQL, pytest.

**Spec:** `docs/superpowers/specs/2026-09-02-regulation-warning-design.md`

## Global Constraints

- Complete `docs/superpowers/plans/2026-09-02-regulation-core.md` first.
- Official events may come only from SSE/SZSE official public-trading-information pages or exchange-hosted documents that explicitly state the exchange conclusion.
- Never generate `RegulationEventRecord` from Daily Bars, calculated rule results, media, or a title alone.
- SSE, SZSE, and BaoStock source requests have separate IngestionRuns and immutable Raw objects; a successful run has one actual provider.
- Benchmark symbols are exactly `SSE:000002`, `SZSE:399107`, and `SZSE:399102`; never substitute another index.
- Ordinary stock Daily Bars remain remote-pytdx-only; the BaoStock exception is allowlist-only for the three Regulation indices.
- All scheduler triggers live in the Worker APScheduler catalog and default disabled; no OS scheduler instructions or artifacts.
- Production source access, migration execution, and schedule enablement require separate authorization.

---

### Task 1: Regulation Event Provider Capability and Validation

**Files:**

- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/providers/contracts.py`
- Modify: `src/market_data_center/providers/__init__.py`
- Modify: `src/market_data_center/domain/regulation.py`
- Create: `tests/test_regulation_event_provider_contract.py`

**Interfaces:**

- Adds provider codes `SSE_OFFICIAL="sse_official"` and `SZSE_OFFICIAL="szse_official"`.
- Adds dataset code `REGULATION_EVENT="regulation_event"`.
- Adds `RegulationEventProvider.fetch_events(observed_from, observed_to) -> ProviderBatch[RegulationEventRecord]`.
- Adds `validate_regulation_events(records, known_symbols, trading_days)` returning accepted records and deterministic findings.

- [ ] **Step 1: Write failing capability and validation tests**

Assert the Provider type accepts timezone-aware half-open observation bounds and the validator rejects unknown symbols, non-SSE/SZSE sources, periods outside known trading days, end-before-start, naive timestamps, unsupported event types, blank source identity, and duplicate/conflicting natural keys. Assert direction-unknown official events are accepted but carry a finding that excludes them from same-direction counts.

- [ ] **Step 2: Run tests RED**

Run: `uv run pytest tests/test_regulation_event_provider_contract.py -q`

Expected: capability, codes, and validator are absent.

- [ ] **Step 3: Implement the narrow capability and validator**

Extend `ProviderRecord` with `RegulationEventRecord`; do not add event methods to `MarketDataProvider`. Normalize findings under rule codes `regulation_event.*`. Reject unsupported data before `IngestionEnvelope` attachment.

- [ ] **Step 4: Run tests and mypy GREEN**

Run: `uv run pytest tests/test_regulation_event_provider_contract.py -q`

Run: `uv run mypy src/market_data_center/providers/contracts.py src/market_data_center/domain/regulation.py`

- [ ] **Step 5: Commit the capability**

```powershell
git add src/market_data_center/domain/ingestion.py src/market_data_center/domain/regulation.py src/market_data_center/providers/contracts.py src/market_data_center/providers/__init__.py tests/test_regulation_event_provider_contract.py
git commit -m "feat: define official regulation event capability"
```

---

### Task 2: SSE Official Event Adapter

**Files:**

- Create: `src/market_data_center/providers/sse_regulation.py`
- Create: `tests/test_sse_regulation_provider.py`

**Interfaces:**

- Produces `SSEOfficialRegulationEventProvider` with source code `sse_official`.
- Reads only official SSE public information rooted at `https://www.sse.com.cn/disclosure/diclosure/public/` and exchange-hosted listed-company documents under `https://www.sse.com.cn/disclosure/listedinfo/announcement/`.
- Emits Raw rows plus lazy normalized `RegulationEventRecord` values.

- [ ] **Step 1: Write mocked response tests RED**

Use minimal official-shaped fixture payloads for one abnormal price event, one turnover event, one serious event with multiple explicit reasons, one unknown-direction event, an unrelated risk announcement, duplicate pages, a changed document hash conflict, empty results, HTTP timeout, and malformed date/number text. Tests perform no live network calls.

- [ ] **Step 2: Run provider tests RED**

Run: `uv run pytest tests/test_sse_regulation_provider.py -q`

Expected: module absent.

- [ ] **Step 3: Implement bounded official-page fetch and normalization**

Use fixed official host allowlisting, explicit connect/read timeouts, bounded pages/documents, deterministic user agent, observation bounds, and response-size limits. Preserve source fields in Raw. Standardize SSE stock codes to `SSE:NNNNNN`; classify only explicit phrases equivalent to “属于股票交易异常波动” or “属于股票交易严重异常波动”. Extract direction only from signed official cumulative-deviation text; do not infer it from closing prices.

- [ ] **Step 4: Enforce document evidence**

Require source event ID, official URL, title, published timestamp, period dates, and normalized content hash. A title match without an explicit body conclusion stays Raw and produces no event. Multiple explicit serious reasons map to `explicit_rule_codes` without splitting one official event identity.

- [ ] **Step 5: Run provider tests GREEN**

Run: `uv run pytest tests/test_sse_regulation_provider.py -q`

Run: `uv run ruff check src/market_data_center/providers/sse_regulation.py tests/test_sse_regulation_provider.py`

- [ ] **Step 6: Commit the SSE adapter**

```powershell
git add src/market_data_center/providers/sse_regulation.py tests/test_sse_regulation_provider.py
git commit -m "feat: adapt SSE regulation events"
```

---

### Task 3: SZSE Official Event Adapter

**Files:**

- Create: `src/market_data_center/providers/szse_regulation.py`
- Create: `tests/test_szse_regulation_provider.py`

**Interfaces:**

- Produces `SZSEOfficialRegulationEventProvider` with source code `szse_official`.
- Reads only official SZSE public information rooted at `https://www.szse.cn/disclosure/deal/public/`, `https://www.szse.cn/disclosure/deal/inquiry/`, and exchange-hosted documents under `https://disc.static.szse.cn/`.

- [ ] **Step 1: Write mocked SZSE fixtures RED**

Cover SZSE mainboard abnormal, GEM abnormal at 30 percentage points, mainboard four-count serious, GEM three-count serious, 10/30-day serious, one six-session official period, unknown direction, non-stock disclosure, duplicate document, changed-hash conflict, truncated page, timeout, and malformed PDF/text extraction.

- [ ] **Step 2: Run provider tests RED**

Run: `uv run pytest tests/test_szse_regulation_provider.py -q`

Expected: module absent.

- [ ] **Step 3: Implement bounded adapter and evidence rules**

Apply the same host, timeout, size, pagination, Raw, Decimal, symbol, and explicit-body requirements as SSE. Distinguish mainboard and GEM only from standard Security facts supplied to validation; do not determine board from announcement prose alone. Keep every source-specific field inside the adapter/Raw boundary.

- [ ] **Step 4: Run provider tests GREEN**

Run: `uv run pytest tests/test_szse_regulation_provider.py -q`

Run: `uv run mypy src/market_data_center/providers/szse_regulation.py`

- [ ] **Step 5: Commit the SZSE adapter**

```powershell
git add src/market_data_center/providers/szse_regulation.py tests/test_szse_regulation_provider.py
git commit -m "feat: adapt SZSE regulation events"
```

---

### Task 4: Allowlisted BaoStock Benchmark Collection

**Files:**

- Create: `src/market_data_center/regulation_benchmark_service.py`
- Modify: `src/market_data_center/providers/baostock.py`
- Create: `tests/test_regulation_benchmark_service.py`
- Modify: `tests/test_baostock_provider.py`

**Interfaces:**

- Produces constant tuple `REGULATION_BENCHMARK_SYMBOLS` containing the three standard symbols.
- Produces `RegulationBenchmarkService.collect(trade_date) -> RegulationBenchmarkCollectionSummary`.
- Reuses existing `DailyBarRecord`, `DatasetCode.DAILY_BAR`, BaoStock Raw schema, and `core.daily_bar` persistence.

- [ ] **Step 1: Write failing whitelist and identity tests**

Assert the service requests exactly three source symbols, accepts only `SecurityType.INDEX`, requires exact trade date and positive OHLC/previous close, preserves Decimal, rejects duplicate or unexpected indices, commits one BaoStock ingestion run for the allowlisted index batch, and never calls provider routing for ordinary stock bars.

- [ ] **Step 2: Run tests RED**

Run: `uv run pytest tests/test_regulation_benchmark_service.py tests/test_baostock_provider.py -q`

Expected: benchmark service absent and index security validation incomplete.

- [ ] **Step 3: Implement allowlist-only collection**

Use the existing BaoStock Provider and Raw Store. Fetch exact-day daily bars one symbol at a time but publish only after all three requests and Raw manifests succeed; attach one BaoStock daily-bar IngestionRun and fail atomically on an invalid returned index. Do not add BaoStock to the ordinary stock Router.

- [ ] **Step 4: Run benchmark and routing regression tests GREEN**

Run: `uv run pytest tests/test_regulation_benchmark_service.py tests/test_baostock_provider.py tests/test_provider_router.py -q`

- [ ] **Step 5: Commit benchmark collection**

```powershell
git add src/market_data_center/regulation_benchmark_service.py src/market_data_center/providers/baostock.py tests/test_regulation_benchmark_service.py tests/test_baostock_provider.py
git commit -m "feat: collect regulation benchmark indices"
```

---

### Task 5: Ingestion Constraints, Event Persistence, and Raw Replay

**Files:**

- Create: `supabase/migrations/20260902000200_add_regulation_ingestion_and_workflows.sql`
- Modify: `src/market_data_center/persistence/regulation_postgres.py`
- Modify: `src/market_data_center/reliability.py`
- Create: `tests/test_regulation_event_service.py`
- Modify: `tests/test_raw_store.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Extends database constraints for provider codes, `regulation_event` dataset, quality rules, and two workflow codes.
- Produces atomic `publish_events(run, manifest, findings, events) -> RegulationEventCollectionSummary` and `commit_event_failure(run, manifest, findings) -> None`.
- Adds replay support for `sse.regulation_event.v1` and `szse.regulation_event.v1`.

- [ ] **Step 1: Write failing migration and persistence tests**

Assert provider/dataset/workflow checks retain every existing accepted value and add only the three new values. Assert identical event content is idempotent; the same source event ID with a changed hash preserves the new Raw, fails the ingestion, and leaves the existing event unchanged; a correcting event with a new official source ID appends normally; natural-key semantic conflicts fail; and partial standard events are never published after a hard error.

- [ ] **Step 2: Write Raw replay tests RED**

Assert replay validates path, byte size, SHA-256, row count, schema version, source host metadata, and observation bounds; creates a new IngestionRun referencing the old manifest; never opens HTTP; and passes through the same normalizer/validator/persistence path.

- [ ] **Step 3: Run tests RED**

Run: `uv run pytest tests/test_regulation_event_service.py tests/test_raw_store.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_event -q`

- [ ] **Step 4: Implement ordered constraints and event transaction methods**

Rebuild existing text check constraints with all previous values plus `sse_official`, `szse_official`, `regulation_event`, `regulation_daily_calculation`, and `regulation_event_reconciliation`; validate every rebuilt constraint. Use one transaction to register manifest/findings/events and finish the run.

- [ ] **Step 5: Implement the two replay dispatches**

Dispatch on exact provider+schema pairs. Reject cross-pair replay such as SSE provider with SZSE schema. Reuse the adapter normalization functions without constructing network clients.

- [ ] **Step 6: Run persistence/replay tests GREEN**

Run: `uv run pytest tests/test_regulation_event_service.py tests/test_raw_store.py tests/test_production_checks.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_event -q`

- [ ] **Step 7: Commit source persistence**

```powershell
git add supabase/migrations/20260902000200_add_regulation_ingestion_and_workflows.sql src/market_data_center/persistence/regulation_postgres.py src/market_data_center/reliability.py tests/test_regulation_event_service.py tests/test_raw_store.py tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: persist and replay official regulation events"
```

---

### Task 6: Calculation Input Assembly and Daily Service

**Files:**

- Create: `src/market_data_center/regulation_service.py`
- Modify: `src/market_data_center/persistence/regulation_postgres.py`
- Create: `tests/test_regulation_service.py`
- Modify: `tests/test_postgres_integration.py`

**Interfaces:**

- Produces `RegulationService.calculate(trade_date) -> RegulationCalculationSummary`.
- Produces `RegulationService.reconcile_events(observed_from, observed_to) -> RegulationReconciliationSummary`.
- Persistence loads an exact repeatable-read snapshot and resolves official reference previous closes with accepted Capital semantics before calling the pure calculator.

- [ ] **Step 1: Write fake-port service tests RED**

Assert exact calendar T/T+1 selection, active rule loading, segment mapping, SecurityNameHistory ST exclusion, no-limit/listing-stage exclusion, benchmark isolation, turnover window loading, event watermark, independent reset boundaries, input-hash idempotency, and one atomic publish. Assert missing one benchmark marks only its segment incomplete and missing one stock input blocks only that symbol.

- [ ] **Step 2: Write corporate-action tests RED**

Cover no-action previous-close derivation, source-provided previous close, ADR-0009 distribution/rights reference-price construction, T+1 known ex-date, and missing required event fields. Missing inputs must produce `INSUFFICIENT_DATA`; no service path may silently use an unadjusted prior close across an ex-date.

- [ ] **Step 3: Run service tests RED**

Run: `uv run pytest tests/test_regulation_service.py -q`

- [ ] **Step 4: Implement snapshot loading and pure calculation orchestration**

Use a read transaction to load exact calendar dates, candidates, bars, index bars, indicators, Capital events, rules, and official events plus watermarks. Close the read transaction before calling `calculate_regulation()`. Open a separate write transaction through persistence to publish. Build hashes from canonical Decimal strings and sorted identities.

- [ ] **Step 5: Implement bounded reconciliation**

When a newly committed official event or a correcting event with a new source ID arrives, determine affected `period_end_date..current_trade_date` dates using the calendar, cap recomputation to 30 trading sessions, and call exact-date calculation in ascending order. A changed hash under an existing source ID never reaches reconciliation because ingestion fails. Do not rewrite old CalculationRuns.

- [ ] **Step 6: Run service/integration tests GREEN**

Run: `uv run pytest tests/test_regulation_service.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k regulation_service -q`

- [ ] **Step 7: Commit the daily service**

```powershell
git add src/market_data_center/regulation_service.py src/market_data_center/persistence/regulation_postgres.py tests/test_regulation_service.py tests/test_postgres_integration.py
git commit -m "feat: orchestrate daily regulation calculation"
```

---

### Task 7: CLI, Settings, Worker Catalog, and Scheduled Execution

**Files:**

- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `.env.example`
- Modify: `deploy/linux/market-data-center.env.example`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_cli.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_worker_admin.py`

**Interfaces:**

- Adds settings `regulation_daily_enabled=False` and `regulation_reconciliation_enabled=False`.
- Adds manual exact-date command `regulation-calculate --trade-date YYYY-MM-DD`.
- Adds jobs `regulation-daily-calculation` at 22:30 and `regulation-event-reconciliation` at 08:30 Asia/Shanghai.

- [ ] **Step 1: Write CLI and settings tests RED**

Assert exact date is required, cannot precede 2026-07-06 or be future, no unbounded historical mode exists, JSON summary contains calculation/coverage/version fields but no source exception or secret, and both schedule settings default false.

- [ ] **Step 2: Write catalog and execution tests RED**

Assert the exact workflow steps and order from the domain design, Monday-Friday Shanghai cron times, one registration each, disabled settings omit jobs without removing catalog entries, non-trading dates skip normally, event/benchmark failures mark the correct JobExecution and WorkflowRun, and partial calculation ends `partial`.

- [ ] **Step 3: Run CLI/scheduler tests RED**

Run: `uv run pytest tests/test_settings.py tests/test_cli.py tests/test_scheduler.py tests/test_worker_admin.py -k regulation -q`

- [ ] **Step 4: Implement opt-in catalog and commands**

Add code-owned constants and definitions; do not make hour/minute configurable. Manual and scheduled paths create Operations runs, execute named steps in order, dispose engines, and never swallow exceptions. Reconciliation derives a bounded overlap from the last successful event watermark.

- [ ] **Step 5: Run scheduler tests GREEN**

Run: `uv run pytest tests/test_settings.py tests/test_cli.py tests/test_scheduler.py tests/test_worker_admin.py -k regulation -q`

Run: `uv run mypy src/market_data_center/scheduler.py src/market_data_center/regulation_service.py`

- [ ] **Step 6: Commit Worker integration**

```powershell
git add src/market_data_center/settings.py src/market_data_center/domain/operations.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py src/market_data_center/cli.py .env.example deploy/linux/market-data-center.env.example tests/test_settings.py tests/test_cli.py tests/test_scheduler.py tests/test_worker_admin.py
git commit -m "feat: schedule regulation calculation workflows"
```

---

### Task 8: Source and Worker Documentation, Verification, and Handoff

**Files:**

- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/Raw重放与运行恢复.md`
- Modify: `docs/数据库导航.md`
- Modify: `README.md`

**Interfaces:**

- Produces operator documentation that describes an implemented but default-disabled capability.
- Produces a verified source/Worker boundary consumed by the public API plan.

- [ ] **Step 1: Document exact operations and source gates**

Document workflow codes, 22:30/08:30 times, default-disabled environment flags, benchmark allowlist, official source roots, Raw schemas, replay commands, partial semantics, event watermark, and the source-rights/production-approval gate. State explicitly that no OS task is created.

- [ ] **Step 2: Run focused tests**

```powershell
uv run pytest tests/test_sse_regulation_provider.py tests/test_szse_regulation_provider.py tests/test_regulation_benchmark_service.py tests/test_regulation_event_service.py tests/test_regulation_service.py tests/test_scheduler.py tests/test_cli.py tests/test_worker_admin.py -q
```

- [ ] **Step 3: Run local static and full unit gates**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all exit zero.

- [ ] **Step 4: Run isolated integration tests**

After verifying `TEST_DATABASE_URL` is disposable and not production:

```powershell
uv run pytest -m integration
```

- [ ] **Step 5: Inspect safety**

```powershell
git diff --check origin/master...HEAD
git status --short
rg -n "cron|Task Scheduler" docs README.md deploy src tests
rg -n "sse_official|szse_official" .env.example deploy src tests
```

Confirm no credential, Raw market data, live-network unit test, provider fallback, default-enabled job, or OS scheduling instruction was added.

- [ ] **Step 6: Commit docs and verification fixes**

Commit scoped documentation as `docs: document regulation source workflows`. If verification requires code fixes, commit them separately as `fix: complete regulation source verification`.

- [ ] **Step 7: Prepare handoff**

Report all commands and results, migrations, Raw schemas, schedule defaults, source rights blocker, and any unavailable check. Do not claim the public read contract is complete; it belongs to `2026-09-02-regulation-public-api.md`.
