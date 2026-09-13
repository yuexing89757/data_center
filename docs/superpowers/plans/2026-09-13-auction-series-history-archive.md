# Auction Series History Archive Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Archive all persisted call-auction market-series facts into a six-calendar-month history table before the three-trading-day online cleanup can delete them.

**Architecture:** A new 02:30 Worker workflow performs an idempotent `INSERT ... SELECT` into a minimally indexed monthly-partitioned history table. The existing 03:00 cleanup proves every target row exists in history before deleting online rows, then applies a separate six-calendar-month history retention delete.

**Tech Stack:** Python 3.12, SQLAlchemy 2, PostgreSQL, APScheduler, ordered SQL migrations, pytest, Ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-09-13-auction-series-history-archive-design.md`

## Global Constraints

- Persist all 32 rounds, five levels, price/volume/amount facts, value semantics, Session/Round identity, and ingestion lineage without transforming values.
- Archive succeeded and partial sessions; missing rows remain missing.
- Online snapshots retain the latest three completed trading days; history retains six calendar months.
- 03:00 online deletion fails closed if any target natural key is absent from history.
- Worker never performs DDL, partition detach/drop, or VACUUM.
- No public RPC, FastAPI route, or contract change belongs to this feature.
- Raw, Manifest, IngestionRun, QualityResult, Session, Round, and Operations facts remain untouched.
- Production migration, deployment, and first production archive require separate explicit authorization.

---

### Task 1: Define archive and retention domain services

**Files:**
- Create: `src/market_data_center/auction_series_archive_service.py`
- Modify: `src/market_data_center/data_cleanup_service.py`
- Modify: `src/market_data_center/operations_service.py`
- Create: `tests/test_auction_series_archive_service.py`
- Modify: `tests/test_data_cleanup_service.py`
- Modify: `tests/test_operations.py`

**Interfaces:**
- Produces: `AuctionSeriesArchivePersistence.archive_call_auction_market_series_snapshots(reference_date: date) -> tuple[int, int]` where values are scanned and inserted rows.
- Produces: `AuctionSeriesArchiveSummary(reference_date, scanned_rows, inserted_rows, existing_rows)`.
- Produces: `six_calendar_months_before(reference_date: date) -> date` with end-of-month clamping.
- Replaces: cleanup persistence delete with `verify_and_delete_archived_call_auction_market_series_snapshots_before(cutoff_date: date) -> tuple[int, int]` returning verified and deleted rows.
- Produces: `delete_call_auction_market_series_snapshot_history_before(cutoff_date: date) -> int`.

- [ ] **Step 1: Write failing archive summary and service tests**

Test that `(scanned=100, inserted=70)` yields `existing_rows=30`, negative or inserted-greater-than-scanned values fail, and the service passes the exact reference date to persistence.

- [ ] **Step 2: Run the focused archive tests and verify RED**

Run: `uv run pytest -q tests/test_auction_series_archive_service.py -p no:cacheprovider`

Expected: collection failure because the new module is absent.

- [ ] **Step 3: Implement the minimal archive service**

Use frozen slotted dataclasses and reject invalid counts in `__post_init__`. The service calls persistence once and derives `existing_rows = scanned_rows - inserted_rows`.

- [ ] **Step 4: Write failing six-month and protected-cleanup tests**

Cover ordinary dates, `2026-08-31 -> 2026-02-28`, leap-year `2028-08-31 -> 2028-02-29`, and cleanup call order: calendar lookup, atomic verify/delete, then history retention delete. Assert a verify/delete exception prevents history deletion.

- [ ] **Step 5: Implement cleanup domain changes**

Extend `DataCleanupSummary` with `verified_rows`, `history_cutoff_date`, and `history_deleted_rows`. Keep the three-trading-day cutoff function unchanged. Calculate the history cutoff without adding dependencies, using `calendar.monthrange` and integer year/month arithmetic.

- [ ] **Step 6: Teach Operations how to count both summaries**

Map archive summary to `(scanned_rows, inserted_rows, 0, succeeded)`. Map cleanup summary to fetched/accepted equal to `verified_rows + history_deleted_rows`, with no rejected rows. Add focused assertions to `tests/test_operations.py`.

- [ ] **Step 7: Run focused tests and commit**

Run: `uv run pytest -q tests/test_auction_series_archive_service.py tests/test_data_cleanup_service.py tests/test_operations.py -p no:cacheprovider`

Commit: `feat: define auction series archive workflow`

---

### Task 2: Add the partitioned history schema and least-privilege policy

**Files:**
- Create: `supabase/migrations/20260913000200_add_auction_series_history_archive.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Produces: `realtime.call_auction_market_series_snapshot_history`, partitioned by `trade_date`.
- Produces: partitions from `2026-03-01` through `2027-10-01` exclusive monthly boundaries.
- Produces: Worker-only SELECT/INSERT/DELETE RLS and grants; no UPDATE grant.
- Produces: Operations workflow code `call_auction_market_series_archive` in the controlled check constraint.

- [ ] **Step 1: Write failing migration contract tests**

Assert the migration contains the exact parent table, all current online columns, monthly partitioning, primary key `(trade_date, ingestion_id, symbol)`, index `(trade_date, symbol, sample_seq)`, RLS on every parent/child, Worker SELECT/INSERT/DELETE only, and no data-copy/delete statement executed by the migration.

- [ ] **Step 2: Write failing PostgreSQL schema and permission tests**

Assert all fields preserve Decimal/None and the source `created_at`, UPDATE fails for Worker, API roles cannot select, and a row cannot reference unknown Security, IngestionRun, Session, or Round facts.

- [ ] **Step 3: Run migration tests and verify RED**

Run: `uv run pytest -q tests/test_production_checks.py -p no:cacheprovider`

Expected: failure because migration and history relation are absent. Run integration only with a disposable `TEST_DATABASE_URL`.

- [ ] **Step 4: Implement the ordered migration**

Mirror the final online schema after migrations through `20260913000100`, including `batch_code`, all bid/ask levels, and `value_semantics`. Add explicit monthly partitions and policies. Replace the Operations workflow-code check constraint by preserving every existing code and adding only `call_auction_market_series_archive`.

- [ ] **Step 5: Run focused tests and commit**

Run: `uv run pytest -q tests/test_production_checks.py -p no:cacheprovider`

When an isolated database exists, run: `uv run pytest -q -m integration tests/test_postgres_integration.py -p no:cacheprovider`

Commit: `feat: add auction series history schema`

---

### Task 3: Implement archive and protected-cleanup persistence

**Files:**
- Modify: `src/market_data_center/persistence/postgres.py`
- Modify: `tests/test_postgres_integration.py`

**Interfaces:**
- Implements: `archive_call_auction_market_series_snapshots(reference_date) -> tuple[int, int]`.
- Implements: `verify_and_delete_archived_call_auction_market_series_snapshots_before(cutoff_date) -> tuple[int, int]` in one transaction.
- Implements: `delete_call_auction_market_series_snapshot_history_before(cutoff_date) -> int` in a later transaction.

- [ ] **Step 1: Add failing integration tests for exact full-field archive**

Seed a succeeded 32-round Session and a partial Session, including zero-price/nonzero-volume level semantics. Archive once and compare every history column to online with `EXCEPT`; archive again and assert zero inserted rows and unchanged history values.

- [ ] **Step 2: Add failing integration tests for fail-closed online cleanup**

Remove one corresponding history key, call protected cleanup, assert a stable application error and all online target rows remain. Restore the history row, rerun, and assert verified equals deleted while cutoff-day rows remain.

- [ ] **Step 3: Add failing integration tests for six-month history delete**

Seed rows immediately before, on, and after the cutoff. Assert only the earlier row is deleted and that online, Session, Round, IngestionRun, Raw Manifest, and quality tables are unchanged.

- [ ] **Step 4: Implement fixed SQL statements and transactions**

Use explicit column lists in `INSERT ... SELECT`; never use `SELECT *`. Obtain scanned and inserted counts in the archive transaction. For online cleanup, run the anti-join check and DELETE in the same `engine.begin()` block; raise `RuntimeError("online auction snapshots are not fully archived")` before DELETE when missing count is nonzero.

- [ ] **Step 5: Run focused integration tests and commit**

Run only against disposable PostgreSQL: `uv run pytest -q -m integration tests/test_postgres_integration.py -p no:cacheprovider`

Commit: `feat: persist auction series history archive`

---

### Task 4: Register the 02:30 Worker job and extend 03:00 orchestration

**Files:**
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `.env.example`
- Modify: `deploy/linux/market-data-center.env.example`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_scheduler.py`

**Interfaces:**
- Produces: `CALL_AUCTION_MARKET_SERIES_ARCHIVE_JOB_ID = "call-auction-market-series-archive-daily"`.
- Produces: `SchedulerSettings.call_auction_market_series_archive_enabled: bool = True`.
- Produces: `run_call_auction_market_series_archive_job()` at daily 02:30 Asia/Shanghai.
- Extends: `run_data_cleanup_job()` to protected online cleanup plus six-month history retention.

- [ ] **Step 1: Write failing settings and catalog tests**

Assert the archive job is enabled by default, only the boolean environment switch is accepted, schedule is exactly daily 02:30, executor is default single-threaded, timeout/misfire values use existing constants, and 03:00 cleanup remains fixed.

- [ ] **Step 2: Write failing scheduler workflow tests**

Assert archive fire time uses Shanghai date, workflow code and step name are controlled constants, failures record failed workflow, and the dispatch map contains the new job. Assert the cleanup summary includes protected online and history statistics.

- [ ] **Step 3: Implement catalog, settings, and scheduler wiring**

Add workflow definition `call_auction_market_series_archive` with step `archive_call_auction_market_series_snapshots`. Instantiate the archive service with `PostgreSQLPersistence`; keep both jobs inside the Worker and do not add OS scheduling.

- [ ] **Step 4: Update environment templates**

Add only `CALL_AUCTION_MARKET_SERIES_ARCHIVE_ENABLED=true`. Do not add hour, minute, retention, target table, batch size, or statement timeout settings.

- [ ] **Step 5: Run focused tests and commit**

Run: `uv run pytest -q tests/test_settings.py tests/test_scheduler.py tests/test_operations.py -p no:cacheprovider`

Commit: `feat: schedule auction series history archive`

---

### Task 5: Synchronize operations documentation and release checks

**Files:**
- Modify: `README.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/集合竞价五档采集运行手册.md`
- Modify: `docs/数据库导航.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Documents: 02:30 archive, 03:00 protected cleanup, 32 rounds, six calendar months, partial facts, first-deployment limitation, and no public API.

- [ ] **Step 1: Add failing production documentation assertions**

Assert active docs mention the exact job ID, 02:30, six months, the history table, fail-closed cleanup, and only the boolean switch. Assert active deployment files contain no cron, timer, dynamic retention, or history API declaration.

- [ ] **Step 2: Update active documentation**

Describe the feature as implemented locally but not deployed. Include the production live gate: archive row count, anti-join zero, online three-day count, history oldest/newest dates, partition sizes, role privileges, and both Worker job states.

- [ ] **Step 3: Run release checks and commit**

Run: `uv run pytest -q tests/test_production_checks.py tests/test_scheduler.py -p no:cacheprovider`

Commit: `docs: document auction series history archive`

---

### Task 6: Complete local verification and deployment handoff

**Files:**
- Verify all files changed by Tasks 1–5.

**Interfaces:**
- Produces: reviewed commits on local `master`, not pushed or deployed without a new explicit instruction.

- [ ] **Step 1: Run format and static gates**

Run:

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

- [ ] **Step 2: Run the full unit gate**

Run: `uv run pytest`

- [ ] **Step 3: Run the isolated PostgreSQL gate**

Run only when `TEST_DATABASE_URL` points to a disposable database: `uv run pytest -m integration`

- [ ] **Step 4: Audit scope and secrets**

Run `git diff --check`, inspect every changed path, verify no `.env`, credentials, Raw data, backup, generated database file, public contract change, or production mutation is included.

- [ ] **Step 5: Report handoff**

Report commit range, migration filename, exact verification results, skipped checks with reasons, estimated six-month capacity, and the separately authorized production sequence: push, protected migration, deploy same commit, restart Worker, read-only smoke, then explicitly run or wait for first 02:30 archive.
