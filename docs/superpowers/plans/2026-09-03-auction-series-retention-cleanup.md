# Auction Series Retention Cleanup Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a default-enabled Worker job named `数据清理任务` that runs every day at 03:00 Asia/Shanghai and deletes only `realtime.call_auction_market_series_snapshot` rows older than the most recent three completed `CN_A_SHARE` trading days.

**Architecture:** Put trading-day cutoff selection and fail-closed orchestration in a small cleanup service, keep SQL in `PostgreSQLPersistence`, and register the job through the existing controlled workflow/job catalog and APScheduler Worker. An ordered migration grants the Worker narrowly scoped DELETE permission and records the new workflow code; it does not delete production data. Existing session, round, Raw, lineage, quality, operations, and 09:25:30 snapshot records remain untouched.

**Tech Stack:** Python 3.12, SQLAlchemy, PostgreSQL/RLS, APScheduler, pytest, Ruff, mypy, `uv`.

**Spec:** `docs/superpowers/specs/2026-09-03-auction-series-retention-cleanup-design.md`; governing decision: `docs/adr/ADR-0050-竞价序列快照三交易日保留与清理.md`; issue: [#71](https://github.com/yuexing89757/data_center/issues/71).

## Global Constraints

- Do not add cron, Windows Task Scheduler, systemd timers, or another process. The job belongs only to the existing Worker APScheduler catalog.
- Do not make the 03:00 schedule, three-trading-day retention, or target table configurable. `.env` may expose only `DATA_CLEANUP_ENABLED`.
- Resolve dates from `core.trading_calendar` with `market = 'CN_A_SHARE'`, `is_trading_day`, and `trade_date <` the current Shanghai local date. If three completed trading dates cannot be found, raise before issuing DELETE.
- Delete with the exclusive predicate `trade_date < cutoff_date`; keep the cutoff date and the two newer completed trading dates.
- Never delete session, round, Raw, manifest, ingestion, quality, operations, or `realtime.call_auction_market_snapshot` data. Do not drop/detach partitions and do not run VACUUM/TRUNCATE.
- Preserve the current public API contract. A retained session/round for a cleaned date may return empty `items`; do not convert it to `not_found` merely because detail facts were cleaned.
- Run PostgreSQL integration tests only against an isolated disposable `TEST_DATABASE_URL`. Do not run the production migration, cleanup, deployment, or first production cleanup in this implementation session without a separate explicit authorization.

---

## Task 1: Implement the fail-closed retention service

**Files:**

- Create: `src/market_data_center/data_cleanup_service.py`
- Create: `tests/test_data_cleanup_service.py`

- [ ] Write service tests first for a normal trading week, a weekend reference date, insufficient calendar history, and delete orchestration.

```python
from datetime import date

import pytest

from market_data_center.data_cleanup_service import (
    DataCleanupService,
    DataCleanupSummary,
    retention_cutoff,
)


def test_retention_cutoff_keeps_latest_three_completed_trading_days() -> None:
    completed = (date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31))

    assert retention_cutoff(date(2026, 9, 3), completed) == date(2026, 8, 31)


def test_retention_cutoff_uses_completed_dates_on_weekend() -> None:
    completed = (date(2026, 9, 4), date(2026, 9, 3), date(2026, 9, 2))

    assert retention_cutoff(date(2026, 9, 6), completed) == date(2026, 9, 2)


def test_retention_cutoff_fails_closed_without_three_dates() -> None:
    with pytest.raises(RuntimeError, match="three completed trading dates"):
        retention_cutoff(date(2026, 9, 3), (date(2026, 9, 2), date(2026, 9, 1)))


def test_cleanup_service_deletes_only_after_cutoff_is_resolved() -> None:
    persistence = FakeCleanupPersistence(
        dates=(date(2026, 9, 2), date(2026, 9, 1), date(2026, 8, 31)),
        deleted_rows=123,
    )

    result = DataCleanupService(persistence).run(date(2026, 9, 3))

    assert result == DataCleanupSummary(
        cutoff_date=date(2026, 8, 31),
        retained_trading_days=3,
        deleted_rows=123,
    )
    assert persistence.deleted_before == date(2026, 8, 31)
```

Also assert that duplicate dates, dates on/after the reference date, and an insufficient result never invoke the delete method.

- [ ] Run the focused test and verify RED because the module does not exist yet.

Run: `uv run pytest tests/test_data_cleanup_service.py -q`

Expected: collection fails with `ModuleNotFoundError: market_data_center.data_cleanup_service`.

- [ ] Implement the minimal service contract and deterministic cutoff.

```python
from dataclasses import dataclass
from datetime import date
from typing import Protocol

RETAINED_COMPLETED_TRADING_DAYS = 3


class DataCleanupPersistence(Protocol):
    def latest_completed_trading_dates(
        self, reference_date: date, limit: int
    ) -> tuple[date, ...]: ...

    def delete_call_auction_market_series_snapshots_before(
        self, cutoff_date: date
    ) -> int: ...


@dataclass(frozen=True, slots=True)
class DataCleanupSummary:
    cutoff_date: date
    retained_trading_days: int
    deleted_rows: int


def retention_cutoff(reference_date: date, completed_dates: tuple[date, ...]) -> date:
    if len(completed_dates) != RETAINED_COMPLETED_TRADING_DAYS:
        raise RuntimeError("cleanup requires three completed trading dates")
    if len(set(completed_dates)) != len(completed_dates):
        raise RuntimeError("cleanup trading dates must be distinct")
    if any(item >= reference_date for item in completed_dates):
        raise RuntimeError("cleanup trading dates must precede reference date")
    return min(completed_dates)


class DataCleanupService:
    def __init__(self, persistence: DataCleanupPersistence) -> None:
        self._persistence = persistence

    def run(self, reference_date: date) -> DataCleanupSummary:
        dates = self._persistence.latest_completed_trading_dates(
            reference_date, RETAINED_COMPLETED_TRADING_DAYS
        )
        cutoff_date = retention_cutoff(reference_date, dates)
        deleted_rows = self._persistence.delete_call_auction_market_series_snapshots_before(
            cutoff_date
        )
        return DataCleanupSummary(cutoff_date, len(dates), deleted_rows)
```

Validate that `deleted_rows` cannot be negative. Keep the service free of SQLAlchemy, APScheduler, environment settings, and timezone lookup.

- [ ] Run the focused tests and verify GREEN.

Run: `uv run pytest tests/test_data_cleanup_service.py -q`

Expected: all cleanup service tests pass.

- [ ] Commit the service slice.

```bash
git add src/market_data_center/data_cleanup_service.py tests/test_data_cleanup_service.py
git commit -m "feat: add auction series cleanup service"
```

---

## Task 2: Add narrow PostgreSQL persistence and Worker DELETE permission

**Files:**

- Modify: `src/market_data_center/persistence/postgres.py`
- Modify: `tests/test_postgres_integration.py`
- Create: `supabase/migrations/20260903000100_add_auction_series_retention_cleanup.sql`

- [ ] Add integration tests before implementation.

Cover these cases in the isolated migrated database:

1. `latest_completed_trading_dates(reference_date=date(2026, 9, 7), limit=3)` returns the three latest `CN_A_SHARE` rows strictly before the reference date where `is_trading_day` is true, ordered newest first; it ignores weekends/non-trading rows and any row on the reference date.
2. `delete_call_auction_market_series_snapshots_before(date(2026, 9, 2))` deletes rows dated before 2026-09-02 and returns the exact row count; rows on 2026-09-02 and later remain.
3. The delete does not modify series session/round rows, Raw/manifest/ingestion/quality/operations rows, or `realtime.call_auction_market_snapshot`.
4. `market_data_worker` has DELETE only on the series snapshot parent and the DELETE RLS policy allows that role. `anon`, `authenticated`, and `market_data_api` do not gain DELETE.
5. Existing series-query RPC behavior remains unchanged after detail cleanup: retained round metadata is present, `items = []`, `returned_count = 0`, and requested codes appear in `missing_codes`.

- [ ] Run the focused integration tests and verify RED.

Run: `uv run pytest tests/test_postgres_integration.py -q -k "cleanup or auction_market_series"`

Expected: failures identify the missing migration, persistence methods, and DELETE permission.

- [ ] Add ordered migration `20260903000100`.

The migration must:

```sql
alter table operations.workflow_run
    drop constraint workflow_run_workflow_code_check;

alter table operations.workflow_run
    add constraint workflow_run_workflow_code_check
    check (workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily','dragon_tiger_daily','regulation_daily_calculation',
        'data_cleanup'
    )) not valid;

alter table operations.workflow_run
    validate constraint workflow_run_workflow_code_check;

create policy call_auction_market_series_snapshot_worker_delete
on realtime.call_auction_market_series_snapshot
for delete
to market_data_worker
using (true);

grant delete on realtime.call_auction_market_series_snapshot to market_data_worker;
```

Keep this complete workflow constraint list and append `data_cleanup` without removing legacy accepted codes such as `trading_billboard_daily`. Guard role-dependent statements consistently with nearby migrations if test fixtures do not always create the production role. Do not include `DELETE FROM`, partition DDL, VACUUM, TRUNCATE, or grants on any other table.

- [ ] Add bounded persistence methods.

```python
LATEST_COMPLETED_TRADING_DATES = text("""
select trade_date
from core.trading_calendar
where market = 'CN_A_SHARE'
  and is_trading_day
  and trade_date < :reference_date
order by trade_date desc
limit :limit
""")

DELETE_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS_BEFORE = text("""
delete from realtime.call_auction_market_series_snapshot
where trade_date < :cutoff_date
""")
```

Implement:

```python
def latest_completed_trading_dates(
    self, reference_date: date, limit: int
) -> tuple[date, ...]:
    if limit < 1:
        raise ValueError("limit must be positive")
    with self._engine.connect() as connection:
        rows = connection.execute(
            LATEST_COMPLETED_TRADING_DATES,
            {"reference_date": reference_date, "limit": limit},
        ).scalars()
        return tuple(rows)

def delete_call_auction_market_series_snapshots_before(
    self, cutoff_date: date
) -> int:
    with self._engine.begin() as connection:
        result = connection.execute(
            DELETE_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS_BEFORE,
            {"cutoff_date": cutoff_date},
        )
    return result.rowcount
```

Keep the delete in one transaction and use the parent partitioned table so PostgreSQL prunes matching partitions while preserving them.

- [ ] Run integration tests and verify GREEN.

Run: `uv run pytest tests/test_postgres_integration.py -q -k "cleanup or auction_market_series"`

Expected: all selected tests pass against disposable PostgreSQL.

- [ ] Commit the persistence/migration slice.

```bash
git add src/market_data_center/persistence/postgres.py tests/test_postgres_integration.py supabase/migrations/20260903000100_add_auction_series_retention_cleanup.sql
git commit -m "feat: persist auction series retention cleanup"
```

---

## Task 3: Register the controlled workflow and fixed schedule

**Files:**

- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/operations_service.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_operations.py`

- [ ] Extend tests first.

Assert all of the following:

- `SchedulerSettings(_env_file=None).data_cleanup_enabled is True`.
- `DATA_CLEANUP_ENABLED=false` disables only this job.
- no `data_cleanup_hour`, `data_cleanup_minute`, `data_cleanup_retained_days`, or cleanup-table setting exists.
- `WorkflowCode.DATA_CLEANUP.value == "data_cleanup"` and the exact workflow step is `cleanup_call_auction_market_series_snapshots`.
- the catalog contains `data-cleanup-daily`, display name `数据清理任务`, cron `hour=3`, `minute=0`, `day_of_week=None`, timezone `Asia/Shanghai`, and the setting-controlled enabled flag.
- a `DataCleanupSummary(deleted_rows=123, ...)` records expected/accepted rows as `123`, rejected rows as `0`, and succeeds.

- [ ] Run focused tests and verify RED.

Run: `uv run pytest tests/test_settings.py tests/test_operations.py -q`

Expected: failures for the missing setting, workflow enum/catalog entry, and result statistics mapping.

- [ ] Add the setting, enum, summary statistics, and catalog definitions.

Use these exact identifiers:

```python
# settings.py
data_cleanup_enabled: bool = True

# domain/operations.py
DATA_CLEANUP = "data_cleanup"

# scheduling_catalog.py
DATA_CLEANUP_JOB_ID = "data-cleanup-daily"
```

Catalog entries:

```python
WorkflowDefinition(
    "data_cleanup",
    "数据清理任务",
    "仅清理早盘竞价序列明细，保留最近三个已完成交易日。",
    ("cleanup_call_auction_market_series_snapshots",),
)

JobDefinition(
    DATA_CLEANUP_JOB_ID,
    "数据清理任务",
    "清理三个已完成交易日以前的沪深全市场开盘竞价序列快照。",
    "data_cleanup",
    "cron",
    "每天 03:00",
    timezone,
    settings.data_cleanup_enabled,
    timeout,
    "缺少三个已完成交易日时失败并保持数据不变；下一日自动重试。",
    hour=3,
    minute=0,
)
```

Add a `DataCleanupSummary` branch before the generic integer handling in `_result_statistics`:

```python
if isinstance(result, DataCleanupSummary):
    return (
        result.deleted_rows,
        result.deleted_rows,
        0,
        ExecutionStatus.SUCCEEDED,
    )
```

- [ ] Run focused tests and verify GREEN.

Run: `uv run pytest tests/test_settings.py tests/test_operations.py -q`

Expected: all selected tests pass.

- [ ] Commit the workflow/catalog slice.

```bash
git add src/market_data_center/settings.py src/market_data_center/domain/operations.py src/market_data_center/operations_service.py src/market_data_center/scheduling_catalog.py tests/test_settings.py tests/test_operations.py
git commit -m "feat: register daily data cleanup job"
```

---

## Task 4: Wire the job into the Worker and local read-only page

**Files:**

- Modify: `src/market_data_center/scheduler.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_worker_admin.py`

- [ ] Write scheduler/runner tests first.

Assert:

- the job is registered as `cron[hour='3', minute='0']`, default executor, coalesced, and `max_instances == 1`;
- it is absent when `data_cleanup_enabled=False`;
- the runner derives `reference_date` from the scheduled fire time converted to `Asia/Shanghai`, starts `WorkflowCode.DATA_CLEANUP`, executes step `cleanup_call_auction_market_series_snapshots` sequence 1, passes a `PostgreSQLPersistence` into `DataCleanupService`, calls `succeed()`, and disposes the engine;
- on service failure the runner calls `fail(error)`, re-raises, and still disposes the engine;
- the Worker admin page displays `数据清理任务`, `每天 03:00`, and its enabled/disabled state without adding mutation controls.

- [ ] Run focused tests and verify RED.

Run: `uv run pytest tests/test_scheduler.py tests/test_worker_admin.py -q -k "cleanup or register or catalog"`

Expected: failures for the missing runner/function mapping and page catalog entry.

- [ ] Implement `run_data_cleanup_job()` using existing runner patterns.

Core flow:

```python
def run_data_cleanup_job() -> None:
    worker = WorkerSettings()
    scheduling = SchedulerSettings()
    engine = create_engine(sqlalchemy_url(worker.database_url.get_secret_value()))
    fire_time = _scheduled_job_fire_time(
        DATA_CLEANUP_JOB_ID,
        scheduling,
        weekdays_only=False,
    )
    reference_date = fire_time.astimezone(ZoneInfo(SCHEDULER_TIMEZONE)).date()
    execution = WorkflowExecutionService(
        PostgreSQLOperationsPersistence(engine)
    ).start(
        WorkflowCode.DATA_CLEANUP,
        fire_time,
        TriggerSource.SCHEDULED,
    )
    try:
        service = DataCleanupService(PostgreSQLPersistence(engine))
        execution.step(
            "cleanup_call_auction_market_series_snapshots",
            1,
            lambda: service.run(reference_date),
        )
        execution.succeed()
    except Exception as error:
        execution.fail(error)
        raise
    finally:
        engine.dispose()
```

Match actual `create_engine`, URL, timezone, and execution helper conventions in adjacent runners rather than duplicating connection logic. Add the mapping:

```python
DATA_CLEANUP_JOB_ID: run_data_cleanup_job,
```

Do not assign the cleanup job to the dedicated `morning_auction` executor.

- [ ] Run focused tests and verify GREEN.

Run: `uv run pytest tests/test_scheduler.py tests/test_worker_admin.py -q -k "cleanup or register or catalog"`

Expected: all selected tests pass.

- [ ] Commit the Worker slice.

```bash
git add src/market_data_center/scheduler.py tests/test_scheduler.py tests/test_worker_admin.py
git commit -m "feat: run auction series cleanup in worker"
```

---

## Task 5: Add migration guards and update operations documentation

**Files:**

- Modify: `tests/test_production_checks.py`
- Modify: `docs/adr/README.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/集合竞价五档采集运行手册.md`
- Modify: `README.md`

- [ ] Add production-safety assertions before editing documentation.

The migration guard should verify:

- migration `20260903000100` is present in ordered sequence;
- it adds `data_cleanup` without losing any existing workflow code;
- it grants/policies DELETE only for `market_data_worker` on `realtime.call_auction_market_series_snapshot`;
- it contains no `delete from`, `truncate`, `vacuum`, `drop table`, `detach partition`, or grants on session, round, Raw, manifest, ingestion, quality, operations, or the 09:25:30 snapshot table.

- [ ] Run the guard and verify it passes with the completed migration.

Run: `uv run pytest tests/test_production_checks.py -q`

Expected: all production checks pass without contacting production.

- [ ] Update documentation to state the implemented behavior.

Document:

- Worker catalog count changes from 15 to 16 scheduled jobs.
- `数据清理任务` runs daily at 03:00 Asia/Shanghai and is enabled by default.
- only `DATA_CLEANUP_ENABLED` is configurable; schedule, retention, and table scope are code constants.
- “最近三个交易日” means the three latest completed `CN_A_SHARE` trading dates before the Shanghai reference date.
- cleanup deletes only series detail facts and preserves metadata/lineage/Raw/quality/operations and the separate 09:25:30 snapshot.
- the first actual deletion occurs only when a migrated/deployed Worker reaches its scheduled run; the migration itself performs no cleanup.
- cleaned historical series queries may return existing rounds with empty item arrays.
- partition space is reusable inside PostgreSQL but runtime cleanup does not promise immediate filesystem shrinkage.

Add ADR-0050 to `docs/adr/README.md`. Do not describe deployment or production migration as already completed.

- [ ] Run documentation/guard focused checks.

Run: `uv run pytest tests/test_production_checks.py tests/test_settings.py tests/test_operations.py tests/test_scheduler.py tests/test_worker_admin.py tests/test_data_cleanup_service.py -q`

Expected: all selected tests pass.

- [ ] Commit the documentation and guard slice.

```bash
git add tests/test_production_checks.py docs/adr/README.md docs/Worker调度系统.md docs/Worker日常采集与调度.md docs/集合竞价五档采集运行手册.md README.md
git commit -m "docs: document auction series retention cleanup"
```

---

## Task 6: Run the complete local gate and review the final diff

**Files:**

- Review: all files changed since `e378ebd`

- [ ] Verify migration ordering and that no secret or environment file is staged.

Run:

```bash
git diff --check e378ebd..HEAD
git status --short
git diff --name-only e378ebd..HEAD
```

Expected: no whitespace errors; no `.env`, credential, Raw data, scheduler SQLite file, or generated market data appears.

- [ ] Run formatting, lint, typing, and complete unit tests.

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: every command exits 0.

- [ ] Run the complete PostgreSQL integration suite only when an isolated disposable `TEST_DATABASE_URL` is available.

Run: `uv run pytest -m integration`

Expected: all integration tests pass. If no isolated database is available, record the exact skipped command/reason and do not substitute production.

- [ ] Inspect the final implementation against the fixed scope.

Confirm from the diff that:

- only series detail rows can be deleted;
- cutoff is exclusive and requires exactly three completed trading days;
- the schedule is daily 03:00 in code;
- disablement is the only cleanup environment control;
- JobExecution/WorkflowRun record the deleted count;
- no public contract file changed;
- no production migration, cleanup, deployment, or service restart was executed.

- [ ] Commit any verification-only corrections, then report the commit range and checks. Do not push, merge, migrate production, deploy, restart services, or run the production cleanup unless the user separately requests those actions.
