# Code-Owned Worker Schedules Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the controlled Worker catalog the sole owner of task times, keep only three optional-task enable switches in environment configuration, enable both five-level quote tasks by default, and—after ADR-0028—schedule only the 09:26 full-market call-auction source job under the existing call-auction switch.

**Architecture:** `scheduling_catalog.py` owns the fixed timezone, cron/interval values, cadence and timeout policy. `SchedulerSettings` retains only runtime paths/ports and three boolean task switches; scheduler execution resolves its intended fire time from `JobDefinition`, never from environment-provided hour/minute fields. ADR-0028 removes automatic call-auction finalization while retaining the morning source collection and internal database finalizer.

**Tech Stack:** Python 3.12, Pydantic Settings, APScheduler, SQLAlchemy, pytest, Ruff, mypy, uv.

## Global Constraints

- Follow `docs/项目宪法-MarketDataCenter-2026-07-24.md`, accepted ADRs and `docs/superpowers/specs/2026-08-11-code-owned-worker-schedules-design.md`.
- GitHub Issues remain the only task-planning system of record; do not use Linear.
- No cron, Windows Task Scheduler, systemd timer or other OS-level collection trigger may be added.
- `.env` may control only `AUCTION_COLLECTION_ENABLED`, `EOD_QUOTE_SNAPSHOT_ENABLED` and `CALL_AUCTION_SNAPSHOT_ENABLED` for optional scheduled tasks.
- The three optional-task switches default to `true`; all task times and scheduling policies are fixed in the code catalog.
- The call-auction switch registers only the weekday 09:26 full-market source collection; automatic finalization is retired without replacement.
- PYTDX pool refresh remains every 12 hours plus Worker startup refresh; auction cadence remains 5 seconds; misfire/timeout remains 21,600 seconds.
- `SCHEDULER_STORE_PATH`, `WORKER_ADMIN_PORT` and `PYTDX_POOL_PATH` remain environment-owned runtime configuration.
- Public PostgREST, FastAPI and agent-tools contracts do not change.
- Do not run a production migration, deploy to the server, edit production `.env`, restart Worker or trigger production ingestion without separate explicit authorization.
- Preserve user changes and never print or commit `.env`, credentials, Raw data or a generated endpoint pool.

---

### Task 0: Record the accepted scheduling configuration decision

**Files:**
- Modify: `docs/adr/ADR-0017-统一Worker进程内调度.md`
- Modify: `docs/superpowers/specs/2026-08-11-code-owned-worker-schedules-design.md`

**Interfaces:**
- Consumes: accepted design `docs/superpowers/specs/2026-08-11-code-owned-worker-schedules-design.md`
- Produces: one GitHub Issue URL and an ADR clarification that later code/docs cite

- [ ] **Step 1: Create the GitHub Issue**

Run and retain both values for Step 2:

```powershell
$issueUrl = gh issue create --title "Worker task times are code-owned" --body @"
## Goal
Make the controlled Worker catalog the sole owner of task times and scheduling policy.

## Requirements
- `.env` keeps only the three existing optional-task enable switches.
- Auction five-level and EOD five-level tasks default enabled.
- The existing call-auction switch controls only the weekday 09:26 full-market source collection.
- PYTDX refresh remains 12 hours; auction cadence remains 5 seconds.
- No OS-level scheduler, schema change, public contract change or production mutation.

## Design
`docs/superpowers/specs/2026-08-11-code-owned-worker-schedules-design.md`
"@
$issueNumber = [int](($issueUrl.TrimEnd('/') -split '/')[-1])
Write-Output "ISSUE_URL=$issueUrl ISSUE_NUMBER=$issueNumber"
```

Expected: a new Issue URL in `yuexing89757/data_center`.

- [ ] **Step 2: Clarify ADR-0017 and accept the design**

Add this decision item to ADR-0017, and add the numeric `$issueNumber` returned in Step 1 to its
`关联 Issue` line:

```markdown
6. Worker 的受控任务目录是任务时区、cron/interval、采样节奏、misfire 与 timeout 的唯一
   事实来源。运行环境只允许通过三个既有布尔开关启停可选任务，不允许覆盖任务执行时间。
```

In the design document change the status to `Accepted` and add a `关联 Issue` line containing the
same numeric `$issueNumber`.

```markdown
状态：Accepted
```

- [ ] **Step 3: Self-check governance changes**

Run:

```powershell
rg -n "状态：Accepted|关联 Issue|唯一.*事实来源|不允许覆盖任务执行时间" docs/adr/ADR-0017-统一Worker进程内调度.md docs/superpowers/specs/2026-08-11-code-owned-worker-schedules-design.md
git diff --check
```

Expected: both documents reference the same Issue and no whitespace errors are reported.

- [ ] **Step 4: Commit**

```powershell
git add docs/adr/ADR-0017-统一Worker进程内调度.md docs/superpowers/specs/2026-08-11-code-owned-worker-schedules-design.md
git commit -m "docs: accept code-owned worker schedules"
```

---

### Task 1: Reduce environment settings to runtime values and task switches

**Files:**
- Modify: `src/market_data_center/settings.py`
- Modify: `tests/test_settings.py`

**Interfaces:**
- Consumes: Pydantic `BaseSettings` environment loading
- Produces: `SchedulerSettings` with `scheduler_store_path`, `worker_admin_port`, and three boolean switches; `PytdxPoolSettings` with only `pytdx_pool_path`

- [ ] **Step 1: Write failing settings boundary tests**

Replace the pool refresh interval tests and add Scheduler settings tests:

```python
from market_data_center.settings import PytdxPoolSettings, SchedulerSettings


def test_optional_scheduled_tasks_default_enabled() -> None:
    settings = SchedulerSettings(_env_file=None)

    assert settings.auction_collection_enabled is True
    assert settings.eod_quote_snapshot_enabled is True
    assert settings.call_auction_snapshot_enabled is True


def test_optional_scheduled_tasks_can_be_disabled_by_environment(monkeypatch) -> None:
    monkeypatch.setenv("AUCTION_COLLECTION_ENABLED", "false")
    monkeypatch.setenv("EOD_QUOTE_SNAPSHOT_ENABLED", "false")
    monkeypatch.setenv("CALL_AUCTION_SNAPSHOT_ENABLED", "false")

    settings = SchedulerSettings(_env_file=None)

    assert settings.auction_collection_enabled is False
    assert settings.eod_quote_snapshot_enabled is False
    assert settings.call_auction_snapshot_enabled is False


def test_task_timing_is_not_part_of_environment_settings() -> None:
    scheduler = SchedulerSettings(_env_file=None)
    pool = PytdxPoolSettings(_env_file=None)

    removed_fields = (
        "scheduler_timezone",
        "daily_run_hour",
        "daily_run_minute",
        "stock_daily_indicator_hour",
        "stock_daily_indicator_minute",
        "stock_pool_hour",
        "stock_pool_minute",
        "deducted_profit_hour",
        "deducted_profit_minute",
        "scheduler_misfire_grace_seconds",
        "auction_collection_hour",
        "auction_collection_minute",
        "auction_collection_cadence_seconds",
        "eod_quote_hour",
        "eod_quote_minute",
        "call_auction_hour",
        "call_auction_minute",
    )
    assert all(not hasattr(scheduler, field) for field in removed_fields)
    assert not hasattr(pool, "pytdx_pool_refresh_hours")
```

- [ ] **Step 2: Run the tests and verify RED**

Run:

```powershell
uv run pytest tests/test_settings.py -q
```

Expected: FAIL because the two five-level switches still default to false and timing fields still exist.

- [ ] **Step 3: Implement the minimal settings boundary**

Make the settings classes exactly this shape:

```python
class SchedulerSettings(BaseSettings):
    """Runtime paths and optional-task switches for the collection Worker."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    scheduler_store_path: Path = Path("data/scheduler/jobs.sqlite")
    worker_admin_port: int = Field(default=8765, ge=1, le=65_535)
    auction_collection_enabled: bool = True
    eod_quote_snapshot_enabled: bool = True
    call_auction_snapshot_enabled: bool = True


class PytdxPoolSettings(BaseSettings):
    """Shared endpoint-pool runtime location."""

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    pytdx_pool_path: Path = Path("data/pytdx_pool.json")
```

Remove the now-unused `ValidationError` import and interval parametrization from `tests/test_settings.py`.

- [ ] **Step 4: Run focused tests and verify GREEN**

```powershell
uv run pytest tests/test_settings.py -q
uv run ruff check src/market_data_center/settings.py tests/test_settings.py
uv run mypy src/market_data_center/settings.py
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit**

```powershell
git add src/market_data_center/settings.py tests/test_settings.py
git commit -m "refactor: limit scheduler environment settings to switches"
```

---

### Task 2: Make the controlled catalog own every schedule value

**Files:**
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `tests/test_operations.py`
- Modify: `tests/test_scheduler.py`

**Interfaces:**
- Consumes: `SchedulerSettings` boolean fields from Task 1
- Produces: `SCHEDULER_TIMEZONE`, `JOB_TIMEOUT_SECONDS`, `AUCTION_COLLECTION_CADENCE_SECONDS`, `job_definitions(settings)`, and `job_definition(code, settings)`

- [ ] **Step 1: Write failing fixed-catalog tests**

Add to `tests/test_operations.py`:

```python
def test_job_catalog_owns_all_fixed_schedules() -> None:
    jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}

    assert (
        jobs["opening-auction-limit-up-quotes"].hour,
        jobs["opening-auction-limit-up-quotes"].minute,
    ) == (9, 15)
    assert jobs["opening-auction-limit-up-quotes"].cadence_seconds == 5
    assert (jobs["daily-run"].hour, jobs["daily-run"].minute) == (20, 0)
    assert (
        jobs["stock-daily-indicators-daily"].hour,
        jobs["stock-daily-indicators-daily"].minute,
    ) == (20, 30)
    assert (
        jobs["mainboard-price-limit-stock-pools-daily"].hour,
        jobs["mainboard-price-limit-stock-pools-daily"].minute,
    ) == (21, 0)
    assert (jobs["eod-quote-snapshot-daily"].hour, jobs["eod-quote-snapshot-daily"].minute) == (
        21,
        10,
    )
    assert (
        jobs["call-auction-market-snapshot-daily"].hour,
        jobs["call-auction-market-snapshot-daily"].minute,
    ) == (9, 26)
    assert "call-auction-snapshot-daily" not in jobs
    assert (jobs["deducted-profit-daily"].hour, jobs["deducted-profit-daily"].minute) == (20, 0)
    assert jobs["recover-stale-ingestion-runs"].interval_hours == 1
    assert jobs["pytdx-pool-refresh"].interval_hours == 12
    assert all(job.timezone == "Asia/Shanghai" for job in jobs.values())
    assert all(job.timeout_seconds == 21_600 for job in jobs.values())
```

Update the call-auction trigger assertions in `tests/test_scheduler.py` to:

```python
assert str(job.trigger) == "cron[day_of_week='mon-fri', hour='9', minute='26']"
assert scheduler.get_job("call-auction-snapshot-daily") is None
```

- [ ] **Step 2: Run tests and verify RED**

```powershell
uv run pytest tests/test_operations.py::test_job_catalog_owns_all_fixed_schedules tests/test_scheduler.py::test_call_auction_snapshot_job_registered_when_enabled -q
```

Expected: FAIL because `JobDefinition` has no `cadence_seconds`, the morning job/retired-job cleanup is absent, and catalog values still come from removed settings fields.

- [ ] **Step 3: Add fixed catalog constants and lookup**

At the top of `scheduling_catalog.py`, remove the `PytdxPoolSettings` import and add:

```python
SCHEDULER_TIMEZONE = "Asia/Shanghai"
JOB_TIMEOUT_SECONDS = 21_600
AUCTION_COLLECTION_CADENCE_SECONDS = 5
```

Add this field to `JobDefinition`:

```python
cadence_seconds: int | None = None
```

Change the catalog signature to accept only `SchedulerSettings`. Rewrite the existing nine-item
tuple in place with the exact values asserted by the Step 1 test. Add the exact lookup interface:

```python
def job_definition(code: str, settings: SchedulerSettings) -> JobDefinition:
    return next(item for item in job_definitions(settings) if item.code == code)
```

Use code literals for every schedule listed in the Task 2 test. Set the auction definition's
`cadence_seconds=AUCTION_COLLECTION_CADENCE_SECONDS`; set all definitions' timezone and timeout from
`SCHEDULER_TIMEZONE` and `JOB_TIMEOUT_SECONDS`. Preserve only these values from settings:

```python
settings.auction_collection_enabled
settings.eod_quote_snapshot_enabled
settings.call_auction_snapshot_enabled
```

Use the literal schedule descriptions `周一至周五 09:15`, `周一至周五 09:26`, `每 12 小时`, and the other times from the fixed schedule table.

- [ ] **Step 4: Update existing catalog consumers in tests**

Replace every two-argument test call:

```python
job_definitions(
    SchedulerSettings(_env_file=None),
    PytdxPoolSettings(pytdx_pool_refresh_hours=12, _env_file=None),
)
```

with:

```python
job_definitions(SchedulerSettings(_env_file=None))
```

Remove unused `PytdxPoolSettings` imports from those test modules.

- [ ] **Step 5: Run focused tests and verify GREEN**

```powershell
uv run pytest tests/test_operations.py tests/test_scheduler.py -q
uv run ruff check src/market_data_center/scheduling_catalog.py tests/test_operations.py tests/test_scheduler.py
uv run mypy src/market_data_center/scheduling_catalog.py
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit**

```powershell
git add src/market_data_center/scheduling_catalog.py tests/test_operations.py tests/test_scheduler.py
git commit -m "refactor: make worker catalog own task schedules"
```

---

### Task 3: Route scheduler execution through catalog definitions

**Files:**
- Modify: `src/market_data_center/scheduler.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_worker_admin.py`

**Interfaces:**
- Consumes: `job_definition(code, settings)`, `SCHEDULER_TIMEZONE`, and fixed `JobDefinition` fields from Task 2
- Produces: scheduler execution and display paths with no timing-field access on `SchedulerSettings`

- [ ] **Step 1: Add a failing legacy-environment isolation test**

Add to `tests/test_scheduler.py`:

```python
def test_legacy_time_environment_cannot_change_registered_jobs(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("CALL_AUCTION_HOUR", "1")
    monkeypatch.setenv("CALL_AUCTION_MINUTE", "2")
    monkeypatch.setenv("EOD_QUOTE_HOUR", "3")
    monkeypatch.setenv("PYTDX_POOL_REFRESH_HOURS", "4")
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "fixed.sqlite",
            eod_quote_snapshot_enabled=True,
            call_auction_snapshot_enabled=True,
            _env_file=None,
        )
    )

    assert str(scheduler.get_job(CALL_AUCTION_SNAPSHOT_JOB_ID).trigger) == (
        "cron[day_of_week='mon-fri', hour='21', minute='30']"
    )
    assert str(scheduler.get_job(EOD_QUOTE_SNAPSHOT_JOB_ID).trigger) == (
        "cron[day_of_week='mon-fri', hour='21', minute='10']"
    )
    assert str(scheduler.get_job(PYTDX_POOL_REFRESH_JOB_ID).trigger) == "interval[12:00:00]"
```

- [ ] **Step 2: Run scheduler tests and verify RED**

```powershell
uv run pytest tests/test_scheduler.py -q
```

Expected: ERROR or FAIL because `build_scheduler` and execution functions still access removed settings fields or require `PytdxPoolSettings`.

- [ ] **Step 3: Add one catalog-based fire-time helper**

In `scheduler.py`, import `SCHEDULER_TIMEZONE` and `job_definition`, then add:

```python
def _scheduled_job_fire_time(
    job_code: str,
    settings: SchedulerSettings,
    *,
    weekdays_only: bool = True,
) -> datetime:
    definition = job_definition(job_code, settings)
    if definition.hour is None or definition.minute is None:
        raise ValueError(f"job has no cron time: {job_code}")
    return _scheduled_fire_time(
        definition.hour,
        definition.minute,
        definition.timezone,
        weekdays_only=weekdays_only,
    )
```

- [ ] **Step 4: Replace every settings-based timing read**

Use `_scheduled_job_fire_time` in:

```text
run_stock_daily_indicator_job
run_daily_market_job
run_deducted_profit_job
run_stock_pool_job
run_eod_quote_snapshot_job
run_call_auction_market_snapshot_job
```

In `run_auction_collection_job`, get `definition = job_definition(AUCTION_COLLECTION_JOB_ID, scheduling)`, use `definition.timezone`, `definition.hour`, `definition.minute`, and require non-null `definition.cadence_seconds` before passing it to `AuctionCollectionService.collect`.

Use `SCHEDULER_TIMEZONE` in `read_job_store_snapshot`, `run_stock_pool_job` fallback date handling,
and `build_scheduler`. Change the function signature and catalog loop to these exact fragments:

```python
def build_scheduler(settings: SchedulerSettings) -> BlockingScheduler:

for definition in job_definitions(settings):
```

Remove `pool_settings` from `build_scheduler`, `prepare_locked_worker`, its `scheduler_factory` callable type, and `run_worker`. Keep `PytdxPoolSettings` only in `execute_pytdx_pool_refresh` and `run_pytdx_pool_refresh_job`, where the path is still runtime configuration.

- [ ] **Step 5: Update tests for simplified signatures**

Change the startup ordering test to call:

```python
prepare_locked_worker(
    SchedulerSettings(_env_file=None),
    cast(PostgreSQLPersistence, object()),
    cast(PostgreSQLOperationsPersistence, object()),
    startup_refresh=refresh,
    scheduler_factory=scheduler_factory,
    admin_factory=admin_factory,
)
```

Change its fake factory to accept one `SchedulerSettings` argument. Make the same update in the
startup-failure test. In worker-admin tests, continue passing
`SchedulerSettings(scheduler_store_path=store_path, _env_file=None)` so a developer's local task
switches cannot affect expected page rows.

- [ ] **Step 6: Run focused scheduler and admin tests**

```powershell
uv run pytest tests/test_scheduler.py tests/test_worker_admin.py -q
uv run ruff check src/market_data_center/scheduler.py tests/test_scheduler.py tests/test_worker_admin.py
uv run mypy src
```

Expected: all commands exit 0, including the legacy-environment isolation test.

- [ ] **Step 7: Commit**

```powershell
git add src/market_data_center/scheduler.py tests/test_scheduler.py tests/test_worker_admin.py
git commit -m "refactor: resolve worker fire times from job catalog"
```

---

### Task 4: Lock the call-auction symbol scope to the exact-date limit-up pool

**Files:**
- Modify: `tests/test_snapshot_collector.py`
- Verify: `src/market_data_center/snapshot_collector.py`

**Interfaces:**
- Consumes: `_limit_up_symbols(engine, trade_date)` and `LIMIT_UP_POOL_CODE`
- Produces: a characterization/regression test for exact-date, ready, limit-up-only membership

- [ ] **Step 1: Add a narrow fake database boundary**

Add these imports to `tests/test_snapshot_collector.py`:

```python
from dataclasses import dataclass
from typing import cast

from sqlalchemy.engine import Engine

from market_data_center.snapshot_collector import LIMIT_UP_POOL_CODE, _limit_up_symbols
```

Add these test-only result, connection and engine doubles. They implement only the real methods used
by `_limit_up_symbols`:

```python
@dataclass(frozen=True, slots=True)
class RecordedCall:
    statement: str
    parameters: dict[str, object]


class RecordingResult:
    def __init__(
        self,
        *,
        scalar: str | None = None,
        rows: tuple[tuple[str], ...] = (),
    ) -> None:
        self.scalar = scalar
        self.rows = rows

    def scalar_one_or_none(self) -> str | None:
        return self.scalar

    def all(self) -> list[tuple[str]]:
        return list(self.rows)


class RecordingConnection:
    def __init__(self, engine: "RecordingEngine") -> None:
        self.engine = engine

    def __enter__(self) -> "RecordingConnection":
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        return None

    def execute(self, statement, parameters) -> RecordingResult:
        self.engine.calls.append(RecordedCall(str(statement), dict(parameters)))
        if len(self.engine.calls) == 1:
            return RecordingResult(scalar=self.engine.snapshot_id)
        return RecordingResult(rows=tuple((symbol,) for symbol in self.engine.symbols))


class RecordingEngine:
    def __init__(self, *, snapshot_id: str, symbols: tuple[str, ...]) -> None:
        self.snapshot_id = snapshot_id
        self.symbols = symbols
        self.calls: list[RecordedCall] = []

    def connect(self) -> RecordingConnection:
        return RecordingConnection(self)
```

- [ ] **Step 2: Add the characterization test**

```python
def test_call_auction_symbols_use_exact_date_limit_up_pool_only() -> None:
    trade_date = date(2026, 8, 11)
    engine = RecordingEngine(snapshot_id="pool-up", symbols=("SSE:600000", "SZSE:000001"))

    symbols = _limit_up_symbols(cast(Engine, engine), trade_date)

    assert symbols == ["SSE:600000", "SZSE:000001"]
    assert engine.calls[0].parameters == {"code": LIMIT_UP_POOL_CODE, "d": trade_date}
    assert "basis_trade_date = :d" in engine.calls[0].statement
    assert "status = 'ready'" in engine.calls[0].statement
    assert engine.calls[1].parameters == {"snapshot_id": "pool-up"}
```

This is a characterization test for already-correct behavior, so it is expected to pass immediately. If it fails, stop and investigate the existing collector rather than changing the required scope.

- [ ] **Step 3: Run the regression test**

```powershell
uv run pytest tests/test_snapshot_collector.py::test_call_auction_symbols_use_exact_date_limit_up_pool_only -q
uv run ruff check tests/test_snapshot_collector.py
```

Expected: PASS and no network or PostgreSQL access.

- [ ] **Step 4: Commit**

```powershell
git add tests/test_snapshot_collector.py
git commit -m "test: lock call auction to limit-up pool"
```

---

### Task 5: Remove task-time configuration from release templates and active docs

**Files:**
- Modify: `.env.example`
- Modify: `deploy/linux/market-data-center.env.example`
- Modify: `README.md`
- Modify: `INSTALL-WINDOWS.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: fixed catalog from Tasks 2-3
- Produces: deployment contract containing runtime settings plus only the three optional-task switches

- [ ] **Step 1: Write a failing active-release configuration test**

Add to `tests/test_production_checks.py`:

```python
def test_release_templates_expose_task_switches_but_not_task_times() -> None:
    templates = "\n".join(
        path.read_text(encoding="utf-8")
        for path in (
            PROJECT_ROOT / ".env.example",
            PROJECT_ROOT / "deploy/linux/market-data-center.env.example",
        )
    )
    switches = (
        "AUCTION_COLLECTION_ENABLED=true",
        "EOD_QUOTE_SNAPSHOT_ENABLED=true",
        "CALL_AUCTION_SNAPSHOT_ENABLED=true",
    )
    forbidden = (
        "SCHEDULER_TIMEZONE",
        "DAILY_RUN_HOUR",
        "DAILY_RUN_MINUTE",
        "STOCK_DAILY_INDICATOR_HOUR",
        "STOCK_DAILY_INDICATOR_MINUTE",
        "STOCK_POOL_HOUR",
        "STOCK_POOL_MINUTE",
        "DEDUCTED_PROFIT_HOUR",
        "DEDUCTED_PROFIT_MINUTE",
        "SCHEDULER_MISFIRE_GRACE_SECONDS",
        "AUCTION_COLLECTION_HOUR",
        "AUCTION_COLLECTION_MINUTE",
        "AUCTION_COLLECTION_CADENCE_SECONDS",
        "EOD_QUOTE_HOUR",
        "EOD_QUOTE_MINUTE",
        "CALL_AUCTION_HOUR",
        "CALL_AUCTION_MINUTE",
        "PYTDX_POOL_REFRESH_HOURS",
    )

    assert all(templates.count(switch) == 2 for switch in switches)
    assert all(name not in templates for name in forbidden)
```

- [ ] **Step 2: Run the test and verify RED**

```powershell
uv run pytest tests/test_production_checks.py::test_release_templates_expose_task_switches_but_not_task_times -q
```

Expected: FAIL because both templates still contain task time variables and the Windows template lacks two switches.

- [ ] **Step 3: Update both environment templates**

Delete every `forbidden` variable from Step 1. Keep runtime paths, provider bounds, credentials placeholders and `WORKER_ADMIN_PORT`. Add exactly:

```dotenv
AUCTION_COLLECTION_ENABLED=true
EOD_QUOTE_SNAPSHOT_ENABLED=true
CALL_AUCTION_SNAPSHOT_ENABLED=true
```

Do not edit or stage the ignored real `.env` file.

- [ ] **Step 4: Update active documentation**

Make these statements consistent across the listed documents:

```text
任务执行时间由 scheduling_catalog.py 固定，.env 只控制三个可选任务的启用或停用。
集合竞价五档默认启用，工作日 09:15 开始。
收盘五档默认启用，工作日 21:10 执行。
全市场开盘竞价来源采集默认启用，工作日 09:26 执行；自动最终化已移除且无替代计划。
PYTDX 节点池由 Worker 启动时刷新，并每 12 小时刷新。
```

Remove instructions that tell operators to configure task hour/minute, cadence, timezone, misfire or refresh interval in `.env`. Preserve the rule that OS-level schedulers are forbidden.

- [ ] **Step 5: Run active-release checks**

```powershell
uv run pytest tests/test_production_checks.py -q
rg -n "DAILY_RUN_HOUR|STOCK_DAILY_INDICATOR_HOUR|STOCK_POOL_HOUR|DEDUCTED_PROFIT_HOUR|AUCTION_COLLECTION_HOUR|EOD_QUOTE_HOUR|CALL_AUCTION_HOUR|PYTDX_POOL_REFRESH_HOURS" .env.example deploy README.md INSTALL-WINDOWS.md docs/Worker日常采集与调度.md docs/Worker调度系统.md docs/最小生产发布运行手册.md
```

Expected: pytest passes; `rg` has no output and exits 1.

- [ ] **Step 6: Commit**

```powershell
git add .env.example deploy/linux/market-data-center.env.example README.md INSTALL-WINDOWS.md docs/Worker日常采集与调度.md docs/Worker调度系统.md docs/最小生产发布运行手册.md tests/test_production_checks.py
git commit -m "docs: keep task times out of environment config"
```

---

### Task 6: Run complete local verification and prepare handoff

**Files:**
- Verify: entire repository
- Verify: `contracts/postgrest-openapi-v1.json`
- Verify: `contracts/agent-tools-v1.json`
- Verify: `contracts/fastapi-openapi-v1.json`

**Interfaces:**
- Consumes: all prior tasks
- Produces: a clean, locally verified branch with no production mutation

- [ ] **Step 1: Format changed Python and Markdown code blocks**

```powershell
uv run ruff format src tests scripts docs/superpowers
git diff --check
```

Expected: formatter exits 0 and no whitespace errors remain.

- [ ] **Step 2: Run the complete local gate**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all commands exit 0. Record exact passed/skipped counts.

- [ ] **Step 3: Run PostgreSQL integration marker safely**

```powershell
uv run pytest -m integration
```

Expected: pass against a disposable `TEST_DATABASE_URL`. If it is absent, report the exact skips and do not substitute production PostgreSQL.

- [ ] **Step 4: Prove public contracts did not change**

```powershell
git diff --exit-code 9067352 -- contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json
```

Expected: exit 0 and no output.

- [ ] **Step 5: Check release configuration and repository state**

```powershell
rg -n "DAILY_RUN_HOUR|STOCK_DAILY_INDICATOR_HOUR|STOCK_POOL_HOUR|DEDUCTED_PROFIT_HOUR|AUCTION_COLLECTION_HOUR|EOD_QUOTE_HOUR|CALL_AUCTION_HOUR|PYTDX_POOL_REFRESH_HOURS" src .env.example deploy README.md INSTALL-WINDOWS.md docs/Worker日常采集与调度.md docs/Worker调度系统.md docs/最小生产发布运行手册.md
git status --short
git log --oneline --decorate -8
```

Expected: `rg` exits 1 with no output; worktree is clean; commits are grouped by governance, settings, catalog, scheduler, scope regression, and release docs.

- [ ] **Step 6: Report the production boundary**

The handoff must state explicitly:

```text
Local code and tests are complete. No server deployment, production migration, production .env edit,
Worker restart, or production ingestion was performed. Those operations require separate explicit authorization.
```
