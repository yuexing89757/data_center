# Shareholder Count Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a provider-neutral shareholder-count point-in-time domain backed by Tushare, with controlled historical backfill, atomic daily incremental collection, Raw replay, bounded PostgREST queries, and Worker-only scheduling.

**Architecture:** Each Tushare request produces one `IngestionRun`, one immutable Raw object, and one prepared batch. `ShareholderCountService` recursively splits responses that hit the 3000-row limit, then publishes all request batches for one daily execution in one PostgreSQL transaction; backfill uses the same mechanism one security at a time. Core stores append-only revisions with announcement and observation time, while three bounded RPCs separate strict as-of reads from current-known history.

**Tech Stack:** Python 3.12, dataclasses, Decimal, urllib-based Tushare JSON client, PostgreSQL, SQLAlchemy, APScheduler, pytest, Ruff, mypy, uv.

**Spec:** `docs/superpowers/specs/2026-08-24-shareholder-count-design.md`

## Global Constraints

- Create the GitHub Issue and Accepted ADR before implementation; GitHub Issues remain the only planning system of record.
- Use dataset `shareholder_count`, Raw schema `tushare.shareholder_count.v1`, and source `tushare`.
- Map source `end_date` to `statistics_date`; non-quarterly dates are valid.
- Reject counts less than one and never pass numbers through `float`.
- Revision SHA-256 inputs are symbol, statistics date, announcement date, and count joined by `\x1f`.
- Treat every 3000-row response as possibly truncated; split dates, then fall back to per-symbol single-day requests.
- Use `TUSHARE_SHAREHOLDER_COUNT_MAX_CALLS_PER_MINUTE`, range 1–200, default 180.
- Backfill listed and delisted SSE/SZSE/BSE stocks from `ipo_date`, falling back to `1990-12-19`.
- Run daily inside the Worker at 21:00 Asia/Shanghai over the current and previous 29 calendar days.
- Keep `SHAREHOLDER_COUNT_DAILY_ENABLED=false` until migration and a bounded probe succeed.
- Do not add FastAPI, MCP, another Provider, OS scheduled tasks, or execute production migration/backfill.
- Expose only bounded `api_v1` RPCs; never expose Core or ingestion/source/revision fields.

## File Map

- `domain/shareholder_count.py`: record, revision hash, validation.
- `shareholder_count_batch.py`: prepared request and sync summary.
- `shareholder_count_service.py`: truncation recursion, daily sync, backfill.
- `providers/tushare.py`: one-request adapter and replay normalizer.
- `pipeline.py`, `persistence/postgres.py`, `reliability.py`: prepare, atomically publish, replay.
- `cli.py`, `scheduling_catalog.py`, `scheduler.py`, `settings.py`: commands and Worker schedule.
- `supabase/migrations/20260824000100_create_shareholder_count.sql`: table, constraints, grants, RPCs.
- PostgREST/Agent contracts, focused tests, integration tests, and operational docs.

---

### Task 1: Establish accepted governance artifacts

**Files:**
- Create: `docs/adr/ADR-0047-股东人数点时事实与Tushare采集.md`
- Create: `docs/领域详设-ShareholderCount-2026-08-24.md`
- Modify: `docs/adr/README.md`

**Interfaces:**
- Consumes: approved spec.
- Produces: Accepted ADR with GitHub Issue number and authoritative domain design.

- [ ] **Step 1: Create the GitHub Issue**

~~~powershell
gh issue create --title "新增股东人数点时事实与 Tushare 采集" --body "实现已确认的 ShareholderCount 领域：Tushare stk_holdernumber、追加式修订、严格 as-of/current-known 查询、受控全历史回填、Worker 每日增量。设计见 docs/superpowers/specs/2026-08-24-shareholder-count-design.md。验收包含 migration、Raw 重放、Provider/Pipeline/PostgreSQL/契约/调度测试，且不执行生产迁移或生产回填。"
~~~

Expected: one issue URL. Record its numeric suffix in the ADR `关联 Issue` line.

- [ ] **Step 2: Write the ADR and domain design**

The ADR header is:

~~~markdown
# ADR-0047：股东人数点时事实与 Tushare 采集
- 状态：Accepted
- 日期：2026-08-24
- 关联 Issue：使用 Step 1 返回 URL 的实际数字编号
- 决策者：项目所有者
- 影响：ShareholderCount、Tushare Provider、Raw 重放、Worker 调度、api_v1
~~~

Copy every approved decision: append-only key, both time axes, backfill limitation, request-level Raw, 3000 guard, 30-day overlap, three RPCs, no FastAPI, no OS scheduler. The domain detail defines exact Python/SQL fields, split state machine, errors, and test matrix.

- [ ] **Step 3: Verify and commit**

~~~powershell
rg -n "状态：Accepted|关联 Issue|shareholder_count|stk_holdernumber|first_observed_at|3000|21:00|FastAPI" docs/adr/ADR-0047-股东人数点时事实与Tushare采集.md docs/领域详设-ShareholderCount-2026-08-24.md
rg -n "ADR-0047" docs/adr/README.md
git add docs/adr/ADR-0047-股东人数点时事实与Tushare采集.md docs/领域详设-ShareholderCount-2026-08-24.md docs/adr/README.md
git commit -m "docs: accept shareholder count domain"
~~~

Expected: documents contain no deployed-current claim.

### Task 2: Add the provider-neutral domain

**Files:**
- Create: `src/market_data_center/domain/shareholder_count.py`
- Modify: `src/market_data_center/domain/__init__.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/providers/contracts.py`
- Create: `tests/test_shareholder_count.py`

**Interfaces:**
- Produces: `ShareholderCountRecord`, `shareholder_count_revision_key`, `validate_shareholder_counts`, `DatasetCode.SHAREHOLDER_COUNT`, `ShareholderCountProvider`.

- [ ] **Step 1: Write failing tests**

~~~python
def test_revision_is_deterministic_and_zero_is_rejected() -> None:
    values = {
        "symbol": "SSE:600000",
        "statistics_date": date(2026, 6, 30),
        "announcement_date": date(2026, 7, 15),
        "shareholder_count": 12345,
    }
    revision = shareholder_count_revision_key(**values)
    record = ShareholderCountRecord(**values, revision_key=revision, source_code="tushare")
    assert validate_shareholder_counts((record,), known_symbols={record.symbol}) == (record,)
    with pytest.raises(ValueError, match="positive"):
        validate_shareholder_counts(
            (replace(record, shareholder_count=0),), known_symbols={record.symbol}
        )
~~~

Add separate assertions for date inversion, unknown symbol, non-Tushare source, hash mismatch, and duplicate natural key.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_shareholder_count.py -q`

Expected: missing module during collection.

- [ ] **Step 3: Implement minimal domain**

~~~python
@dataclass(frozen=True, slots=True)
class ShareholderCountRecord:
    symbol: str
    statistics_date: date
    announcement_date: date
    shareholder_count: int
    revision_key: str
    source_code: str


def shareholder_count_revision_key(
    *,
    symbol: str,
    statistics_date: date,
    announcement_date: date,
    shareholder_count: int,
) -> str:
    values = (
        symbol,
        statistics_date.isoformat(),
        announcement_date.isoformat(),
        str(shareholder_count),
    )
    return sha256("\x1f".join(values).encode()).hexdigest()


def validate_shareholder_counts(
    records: tuple[ShareholderCountRecord, ...],
    *,
    known_symbols: set[str],
) -> tuple[ShareholderCountRecord, ...]:
    seen: set[tuple[str, date, str]] = set()
    for record in records:
        if record.symbol not in known_symbols:
            raise ValueError("unknown shareholder-count symbol")
        if record.statistics_date > record.announcement_date:
            raise ValueError("shareholder-count announcement precedes statistics date")
        if record.shareholder_count <= 0:
            raise ValueError("shareholder count must be positive")
        if record.source_code != "tushare":
            raise ValueError("unsupported shareholder-count source")
        expected = shareholder_count_revision_key(
            symbol=record.symbol,
            statistics_date=record.statistics_date,
            announcement_date=record.announcement_date,
            shareholder_count=record.shareholder_count,
        )
        if record.revision_key != expected:
            raise ValueError("shareholder-count revision key mismatch")
        key = (record.symbol, record.statistics_date, record.revision_key)
        if key in seen:
            raise ValueError("duplicate shareholder-count revision")
        seen.add(key)
    return records
~~~

The validator enforces every invariant listed in Step 1. Export names; add enum value. Extend `ProviderRecord` and add:

~~~python
class ShareholderCountProvider(Protocol):
    source_code: str

    def fetch_shareholder_counts(
        self,
        source_symbol: str | None,
        start_date: date,
        end_date: date,
    ) -> ProviderBatch[ShareholderCountRecord]:
        raise NotImplementedError
~~~

- [ ] **Step 4: Verify GREEN and commit**

~~~powershell
uv run pytest tests/test_shareholder_count.py -q
uv run ruff check src/market_data_center/domain/shareholder_count.py tests/test_shareholder_count.py
uv run mypy src/market_data_center/domain/shareholder_count.py src/market_data_center/providers/contracts.py
git add src/market_data_center/domain/shareholder_count.py src/market_data_center/domain/__init__.py src/market_data_center/domain/ingestion.py src/market_data_center/providers/contracts.py tests/test_shareholder_count.py
git commit -m "feat: add shareholder count domain"
~~~

### Task 3: Implement Tushare request adaptation and Raw replay

**Files:**
- Modify: `src/market_data_center/providers/tushare.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `tests/test_tushare_provider.py`
- Modify: `tests/test_settings.py`

**Interfaces:**
- Consumes: Task 2 record/protocol.
- Produces: one-request adapter, strict integer mapping, pacing, `tushare.shareholder_count.v1` replay.

- [ ] **Step 1: Write failing tests**

Test mapping of `920000.BJ`, empty success, replay equality, missing/blank/`1.5`/zero counts, reversed input dates, and request pacing. The primary assertion is:

~~~python
batch = TushareProvider(client).fetch_shareholder_counts(
    "BSE:920000", date(2026, 8, 1), date(2026, 8, 24)
)
assert batch.request_params == {
    "source_symbol": "920000.BJ",
    "start_date": "20260801",
    "end_date": "20260824",
}
assert batch.schema_version == "tushare.shareholder_count.v1"
assert batch.records[0].shareholder_count == 12001
~~~

A two-call injected-sleeper test expects no sleep before the first call and one `pytest.approx(1/3)` delay before the second. Settings tests assert default 180 and rejection of 0/201.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_tushare_provider.py tests/test_settings.py -q`

Expected: missing method/settings/schema branch failures.

- [ ] **Step 3: Implement**

~~~python
SHAREHOLDER_COUNT_FIELDS = ("ts_code", "ann_date", "end_date", "holder_num")
SHAREHOLDER_COUNT_RESPONSE_LIMIT = 3_000


def _strict_positive_integer(value: str | None, field_name: str) -> int:
    if value is None or not value.strip() or not value.strip().isdigit():
        raise ProviderError(f"invalid Tushare integer: {field_name}")
    result = int(value)
    if result <= 0:
        raise ProviderError(f"Tushare {field_name} must be positive")
    return result
~~~

`fetch_shareholder_counts` calls only `stk_holdernumber`; it omits `ts_code` for all-market requests, uses announcement start/end dates, sorts by `(ts_code,end_date,ann_date,holder_num)`, and returns a lazy batch. Mapping uses `_normalize_symbol`, `_parse_date`, strict integer parsing, and Task 2 hash. Add the dataset/schema branch to `normalize_tushare_raw`; do not use `_optional_int`.

Add `tushare_shareholder_count_max_calls_per_minute: int = Field(default=180, ge=1, le=200)`. Inject `shareholder_count_request_interval_seconds` and `sleeper` into the provider; only this API is paced.

- [ ] **Step 4: Verify and commit**

~~~powershell
uv run pytest tests/test_tushare_provider.py tests/test_settings.py -q
uv run ruff check src/market_data_center/providers/tushare.py src/market_data_center/settings.py
uv run mypy src/market_data_center/providers/tushare.py
git add src/market_data_center/providers/tushare.py src/market_data_center/settings.py tests/test_tushare_provider.py tests/test_settings.py
git commit -m "feat: adapt Tushare shareholder counts"
~~~

### Task 4: Prepare and atomically persist request batches

**Files:**
- Create: `src/market_data_center/shareholder_count_batch.py`
- Modify: `src/market_data_center/pipeline.py`
- Modify: `src/market_data_center/persistence/postgres.py`
- Modify: `src/market_data_center/reliability.py`
- Modify: `src/market_data_center/recovery.py`
- Create: `tests/test_shareholder_count_batch.py`
- Modify: `tests/test_pipeline.py`
- Modify: `tests/test_reliability.py`

**Interfaces:**
- Produces: `PreparedShareholderCountBatch`, `ShareholderCountSyncSummary`, request preparation, aggregate commit/abort, replay.

- [ ] **Step 1: Write failing tests**

Model tests after `tests/test_daily_bar_batch.py`. Two prepared requests must commit using one `engine.begin()`. Assert duplicate aggregate natural keys and mismatched run/manifest/envelope IDs fail before transaction. Abort must insert manifests/ERROR quality and update failed runs without executing fact insert. Pipeline test asserts one request starts a run, writes Raw, validates known symbols, and returns without Core commit.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_shareholder_count_batch.py tests/test_pipeline.py -q`

- [ ] **Step 3: Add typed hand-offs**

~~~python
@dataclass(frozen=True, slots=True)
class PreparedShareholderCountBatch:
    run: IngestionRun
    manifest: RawManifest | None
    records: tuple[IngestionEnvelope[ShareholderCountRecord], ...]
    quality_results: tuple[QualityResult, ...] = ()


@dataclass(frozen=True, slots=True)
class ShareholderCountSyncSummary:
    request_count: int
    fetched_rows: int
    accepted_rows: int
    superseded_request_count: int
~~~

Validate all counts and relationships.

- [ ] **Step 4: Implement preparation and persistence**

~~~python
prepare_shareholder_count_request(
    source_symbol: str | None, start_date: date, end_date: date
) -> PreparedShareholderCountBatch
commit_shareholder_count_batches(
    batches: Sequence[PreparedShareholderCountBatch]
) -> None
abort_shareholder_count_batches(
    batches: Sequence[PreparedShareholderCountBatch], *, error_type: str
) -> None
~~~

Use task key `tushare:shareholder_count:<symbol-or-all>:<start>:<end>`. Live preparation stages one Raw and validates known symbols. Successful aggregate commit preflights all IDs/natural keys, requires a manifest for non-replay runs, then in one transaction inserts manifests, optional quality, `INSERT_SHAREHOLDER_COUNT ON CONFLICT DO NOTHING`, and updates every run. Abort converts prepared runs to failed with zero accepted, all fetched rejected, adds `shareholder_count.batch_aborted`, inserts no facts.

- [ ] **Step 5: Add replay**

Add the dataset branch to `reliability.py`, validate normalized records, and commit one prepared replay batch while preserving existing `replayed_from_raw_id`. Add `core.shareholder_count` to `recovery.COUNT_QUERIES` and its `ingestion_id` to the orphan-fact union so backup/restore verification covers the new fact.

- [ ] **Step 6: Verify and commit**

~~~powershell
uv run pytest tests/test_shareholder_count_batch.py tests/test_pipeline.py tests/test_reliability.py -q
uv run ruff check src/market_data_center/shareholder_count_batch.py src/market_data_center/pipeline.py src/market_data_center/persistence/postgres.py src/market_data_center/reliability.py src/market_data_center/recovery.py
uv run mypy src
git add src/market_data_center/shareholder_count_batch.py src/market_data_center/pipeline.py src/market_data_center/persistence/postgres.py src/market_data_center/reliability.py src/market_data_center/recovery.py tests/test_shareholder_count_batch.py tests/test_pipeline.py tests/test_reliability.py
git commit -m "feat: persist shareholder count request batches"
~~~

### Task 5: Add split orchestration and controlled CLI flows

**Files:**
- Create: `src/market_data_center/shareholder_count_service.py`
- Modify: `src/market_data_center/persistence/postgres.py`
- Modify: `src/market_data_center/operations_service.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/cli.py`
- Create: `tests/test_shareholder_count_service.py`
- Modify: `tests/test_cli.py`

**Interfaces:**
- Produces: `sync_daily`, `backfill`, target enumeration, daily/backfill commands.

- [ ] **Step 1: Write failing service tests**

Cover: 30-day range; 3000-row range split into non-overlapping halves; single-day all-market fallback to every known stock in symbol order; single-symbol/day 3000 hard failure; provider failure after one prepared child aborts it; successful split commits all prepared batches once and publishes no probe facts.

- [ ] **Step 2: Write failing CLI tests**

Parse:

~~~text
shareholder-count-daily --as-of-date 2026-08-24 --provider tushare
shareholder-count-backfill --cutoff-date 2026-08-24 --yes --provider tushare
shareholder-count-backfill --cutoff-date 2026-08-24 --symbols SSE:600000 BSE:920000 --resume-after-symbol SSE:600000 --yes --provider tushare
~~~

Assert non-Tushare, future cutoff, and non-interactive missing `--yes` fail before provider creation.

- [ ] **Step 3: Verify RED**

Run: `uv run pytest tests/test_shareholder_count_service.py tests/test_cli.py -q`

- [ ] **Step 4: Implement targets**

~~~python
@dataclass(frozen=True, slots=True)
class ShareholderCountBackfillTarget:
    symbol: str
    start_date: date

shareholder_count_backfill_targets(
    symbols: Collection[str] | None, resume_after_symbol: str | None
) -> tuple[ShareholderCountBackfillTarget, ...]
~~~

SQL selects `security_type='stock'`, exchanges SSE/SZSE/BSE, all statuses, orders symbol ascending, and uses `coalesce(ipo_date, date '1990-12-19')`. Reject any requested symbol absent from results.

- [ ] **Step 5: Implement service**

Split a multi-day range at `start + (end-start)//2`. A 3000-row probe becomes succeeded with zero published records plus INFO/PASSED `shareholder_count.response_split`. A one-day global probe falls back to all targets. A one-symbol/day 3000 result aborts and raises `ProviderError("Tushare shareholder count response remains truncated for one symbol-day")`.

`sync_daily(as_of)` uses `[as_of-29 days, as_of]` and commits all request batches once. `backfill` commits a full request tree per symbol before advancing. Run sequentially.

- [ ] **Step 6: Implement CLI and summary mapping**

Add `WorkflowCode.SHAREHOLDER_COUNT_DAILY = "shareholder_count_daily"` and `WorkflowCode.SHAREHOLDER_COUNT_BACKFILL = "shareholder_count_backfill"`. Map `ShareholderCountSyncSummary` to Operations statistics. Daily uses the daily workflow; backfill uses the backfill workflow, prints count/range, then requires interactive `yes` or `--yes`. Never print secrets, database URLs, Raw paths, or payloads.

- [ ] **Step 7: Verify and commit**

~~~powershell
uv run pytest tests/test_shareholder_count_service.py tests/test_cli.py -q
uv run ruff check src/market_data_center/shareholder_count_service.py src/market_data_center/cli.py
uv run mypy src
git add src/market_data_center/shareholder_count_service.py src/market_data_center/persistence/postgres.py src/market_data_center/operations_service.py src/market_data_center/domain/operations.py src/market_data_center/cli.py tests/test_shareholder_count_service.py tests/test_cli.py
git commit -m "feat: synchronize shareholder count history"
~~~

### Task 6: Add PostgreSQL facts and bounded read contracts

**Files:**
- Create: `supabase/migrations/20260824000100_create_shareholder_count.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Produces: Core table and three approved RPCs.

- [ ] **Step 1: Write failing migration and integration tests**

Production test asserts table, primary/check/source constraints, all RPC names, `language plpgsql stable security definer`, 5-second timeout, safe grants. Integration tests cover two revisions with controlled observation times; strict versus latest visibility; three-date lag/delta/Decimal ratio; narrowed-range first row null previous; `NULL` versus empty symbol-array behavior; 501-symbol and inverted-date errors; limit clamping; RLS/grants/no-update; and backup snapshot/orphan-lineage inclusion.

- [ ] **Step 2: Verify RED**

~~~powershell
uv run pytest tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -m integration -k shareholder_count -q
~~~

Expected: migration absent.

- [ ] **Step 3: Create table and controlled constraints**

~~~sql
create table core.shareholder_count (
    symbol text not null references core.security (symbol),
    statistics_date date not null,
    announcement_date date not null,
    shareholder_count bigint not null,
    revision_key text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    first_observed_at timestamptz not null default now(),
    primary key (symbol, statistics_date, revision_key),
    constraint shareholder_count_date_order check (statistics_date <= announcement_date),
    constraint shareholder_count_positive check (shareholder_count > 0),
    constraint shareholder_count_revision_key_check check (revision_key ~ '^[0-9a-f]{64}$'),
    constraint shareholder_count_source_check check (source_code = 'tushare')
);
~~~

Add approved indexes, RLS, worker SELECT/INSERT, dataset constraints, and both workflow codes without dropping existing enum values.

- [ ] **Step 4: Add three RPCs**

Each returns symbol, dates, current/previous counts, change count, numeric ratio. Use `plpgsql stable security definer`; raise SQLSTATE 22023 for over 500 symbols or inverted dates; clamp limit to 1–2000. Strict functions filter both announcement and Shanghai end-of-as-of observation time. Latest history omits knowledge cutoffs. Choose revisions with row_number ordered announcement/observation/revision descending, filter requested statistics range before `lag`, and apply limit last. Cross-section sorts symbol; histories sort statistics/announcement ascending. Revoke public and grant only existing PostgREST roles.

- [ ] **Step 5: Verify and commit**

~~~powershell
uv run pytest tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -m integration -k shareholder_count -q
git add supabase/migrations/20260824000100_create_shareholder_count.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat(db): store and query shareholder counts"
~~~

### Task 7: Register Worker-only daily scheduling

**Files:**
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `.env.example`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_operations.py`
- Modify: `tests/test_worker_admin.py`

**Interfaces:**
- Produces: disabled-by-default daily job at 21:00 every calendar day.

- [ ] **Step 1: Write failing tests**

~~~python
jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}
definition = jobs["shareholder-count-daily"]
assert definition.workflow_code == "shareholder_count_daily"
assert definition.day_of_week is None
assert (definition.hour, definition.minute) == (21, 0)
assert definition.timezone == "Asia/Shanghai"
assert definition.enabled is False
~~~

With the setting true, assert trigger `cron[hour='21', minute='0']`; admin page lists it without mutation controls.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_scheduler.py tests/test_operations.py tests/test_worker_admin.py -q`

- [ ] **Step 3: Implement codes, setting, catalog, handler**

Use the Task 5 WorkflowCode values; add setting `shareholder_count_daily_enabled: bool = False`, the env example line, job ID `shareholder-count-daily`, both workflow definitions, and the daily cron definition without weekday restriction.

`run_shareholder_count_daily_job` creates the Tushare provider, pipeline, service, and Operations execution, passes current Shanghai date, records failures, and disposes the engine in `finally`. Map the handler in scheduler registration. Add no hour/minute configuration and no OS scheduler.

- [ ] **Step 4: Verify and commit**

~~~powershell
uv run pytest tests/test_scheduler.py tests/test_operations.py tests/test_worker_admin.py -q
uv run ruff check src/market_data_center/domain/operations.py src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py
uv run mypy src
git add src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py .env.example tests/test_scheduler.py tests/test_operations.py tests/test_worker_admin.py
git commit -m "feat(worker): schedule shareholder count sync"
~~~

### Task 8: Publish contracts, docs, and complete verification

**Files:**
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `tests/test_api_contracts.py`
- Modify: `docs/领域模型总纲-DomainModelOverview-2026-07-24.md`
- Modify: `docs/数据库导航.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/Tushare-2000积分接口清单-2026-08-02.md`

**Interfaces:**
- Produces: synchronized public contracts and current-state docs; FastAPI remains unchanged.

- [ ] **Step 1: Write failing contract tests**

Add all three endpoint names to `EXPECTED_ENDPOINTS`. Assert cross-section `p_symbols.maxItems=500`, every limit maximum 2000, symbol/date formats, nullable previous/change fields, signed `change_count`, numeric ratio, and strict/current-known descriptions. Assert none appears in FastAPI OpenAPI.

- [ ] **Step 2: Verify RED**

Run: `uv run pytest tests/test_api_contracts.py -q`

- [ ] **Step 3: Update both contracts**

Add three PostgREST POST operations and matching Agent tools. Inputs use ISO dates and symbol pattern `^(SSE|SZSE|BSE):[0-9]{6}$`; tools are read-only with `additionalProperties:false`. Exclude internal schema names and lineage fields.

- [ ] **Step 4: Update docs**

Add domain/dependency, Core table/RPC navigation, disabled 21:00 Worker job, explicit backfill, and implemented Tushare status. Preserve dated-permission warnings and do not claim production migration/backfill occurred.

- [ ] **Step 5: Run focused and complete gates**

~~~powershell
uv run pytest tests/test_api_contracts.py tests/test_production_checks.py -q
git diff --check
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration
~~~

The integration command uses only disposable `TEST_DATABASE_URL`; if unset, report that exact reason and do not substitute another database.

- [ ] **Step 6: Safety scan and commit**

~~~powershell
rg -n "Windows Task Scheduler|cron" docs src scripts
rg -n "TUSHARE_TOKEN|DATABASE_URL" contracts docs/领域详设-ShareholderCount-2026-08-24.md docs/adr/ADR-0047-股东人数点时事实与Tushare采集.md
git status --short
git add contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json tests/test_api_contracts.py docs/领域模型总纲-DomainModelOverview-2026-07-24.md docs/数据库导航.md docs/Worker日常采集与调度.md docs/Tushare-2000积分接口清单-2026-08-02.md
git commit -m "docs: publish shareholder count contracts"
~~~

Expected: OS scheduler mentions are prohibitions only; no secret values or DB URLs; only planned files changed.

## Implementation Handoff Checklist

- Read this plan and the approved spec before Task 1.
- Do not execute production migration, production backfill, credential changes, or external collection for verification.
- Review each task's diff and focused test output before advancing.
- Run the complete Task 8 gate before claiming completion.
