# 09:25:20 Auction Final Series Batch Read Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make both existing auction snapshot APIs read the exact 09:25:20 final series batch and retire the duplicate 09:25:30 full-market scheduled job.

**Architecture:** Replace the bodies of the two stable `api_v1` RPCs with single-ingestion selectors over `realtime.call_auction_market_series_session`, `round`, and `snapshot`, while preserving their signatures and response schemas. Compute sealed funds deterministically from the final batch order book, remove only the scheduled-job surface of the legacy collector, and retain its historical table and non-scheduled domain/persistence implementation.

**Tech Stack:** Python 3.12, SQLAlchemy 2, psycopg 3, PostgreSQL 15, APScheduler, FastAPI/Pydantic, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-09-04-auction-final-series-batch-read-design.md`

## Global Constraints

- The only source for both APIs is `realtime.call_auction_market_series_snapshot` with `sample_seq=31` and `batch_code='092520'`.
- Prefer a succeeded selected attempt; if none exists for the exact date, allow the selected partial attempt. Never combine attempts, sessions, dates, or earlier rounds.
- Keep both FastAPI paths, RPC signatures, response field names, bounds, grants, and numeric serialization compatible.
- Compute `seal_amount` only when ask1/ask2/ask3 volume are each null or zero and bid1 price/volume are non-null; otherwise return null.
- Remove `call-auction-market-snapshot-daily` from the code-owned Worker catalog. Do not add cron, systemd timers, environment-based schedule times, or a replacement job.
- Preserve historical `realtime.call_auction_market_snapshot` rows, migrations, lineage, and non-scheduled collection/persistence code.
- Production schema changes use only an ordered file in `supabase/migrations`; tests never target production.
- Use `Decimal`/PostgreSQL numeric for prices and amounts; do not route market values through float.

---

### Task 1: Switch both database read contracts to the final series batch

**Files:**
- Create: `supabase/migrations/20260904000100_read_auction_apis_from_final_series_batch.sql`
- Modify: `tests/test_postgres_integration.py:2079-2385`
- Modify: `tests/test_production_checks.py:240-270`
- Modify: `scripts/check_fastapi_release.py:10-35`

**Interfaces:**
- Consumes: `realtime.call_auction_market_series_session`, `realtime.call_auction_market_series_round`, `realtime.call_auction_market_series_snapshot`, and `ingestion.ingestion_run`.
- Produces: unchanged functions `api_v1.query_call_auction_market_snapshots(date,text[]) returns jsonb` and `api_v1.query_auction_one_price_limits(date) returns jsonb`.

- [ ] **Step 1: Add failing integration fixtures for exact final-batch selection**

Add helpers in `tests/test_postgres_integration.py` that create one series session with round 30 (`092500`) and round 31 (`092520`), separate selected ingestion IDs, and conflicting legacy `call_auction_market_snapshot` facts. The returned item must match only round 31:

```python
payload = connection.scalar(
    text("""
        select api_v1.query_call_auction_market_snapshots(
            date '2026-09-04', array['600000']::text[]
        )
    """)
)
assert payload["ingestion_id"] == str(final_ingestion_id)
assert payload["ingestion_status"] == "succeeded"
assert payload["items"][0]["last_price"] == 11.00
assert payload["items"][0]["seal_amount"] == 11_000.00
```

Insert `bid1_price=11.00`, `bid1_volume=1000`, and null/zero ask1 through ask3 volumes in the final series row. Give the earlier series and legacy rows different prices so an incorrect source is observable.

- [ ] **Step 2: Add failing selection-boundary tests**

Add focused tests with these assertions:

```python
assert query(explicit_date_with_only_092500) raises DatabaseError with sqlstate "P0002"
assert query(date_with_succeeded_and_newer_partial_final_batches)["ingestion_id"] == succeeded_id
assert query(date_with_only_partial_final_batch)["ingestion_status"] == "partial"
assert query(explicit_missing_date) raises DatabaseError with sqlstate "P0002"
```

For the date-optional one-price RPC, insert a newer date containing only `092500` and verify it selects the most recent date containing `092520`, not merely the newest series date.

- [ ] **Step 3: Add failing sealed-funds and one-price tests**

Use final series rows covering both branches:

```python
assert limit_up_item["seal_amount"] == 11_000.00
assert row_with_nonzero_ask2["seal_amount"] is None
assert payload["calculation_mode"] == "realtime_read"
```

Keep the existing mainboard ranges, IPO age, prior-five-bars completeness, upper/lower limit formula, omission counts, and Shanghai timestamp formatting assertions unchanged. Make the old single-snapshot facts conflict so the test proves they are ignored.

- [ ] **Step 4: Run the new database tests and observe failure**

Run:

```powershell
$env:TEST_DATABASE_URL='postgresql://postgres:pg123456@117.72.105.65:5433/postgres'
uv run pytest tests/test_postgres_integration.py -k "call_auction_market_snapshot_rpc or auction_one_price_limits or final_series_batch" -q
```

Expected: FAIL because the installed test schema still reads `realtime.call_auction_market_snapshot`.

- [ ] **Step 5: Implement the ordered migration**

Start each RPC with an exact selected-ingestion CTE. For the explicit-date batch function, use this selection shape:

```sql
select run.ingestion_id, run.status
into selected_ingestion_id, selected_status
from realtime.call_auction_market_series_session session
join realtime.call_auction_market_series_round round
  on round.session_id = session.session_id
join ingestion.ingestion_run run
  on run.ingestion_id = round.selected_ingestion_id
where session.trade_date = p_trade_date
  and round.sample_seq = 31
  and round.status in ('succeeded', 'partial')
  and run.dataset_code = 'call_auction_market_series'
  and run.status in ('succeeded', 'partial')
  and exists (
      select 1
      from realtime.call_auction_market_series_snapshot snapshot
      where snapshot.trade_date = session.trade_date
        and snapshot.session_id = session.session_id
        and snapshot.sample_seq = 31
        and snapshot.batch_code = '092520'
        and snapshot.ingestion_id = run.ingestion_id
  )
order by case run.status when 'succeeded' then 0 else 1 end,
         session.started_at desc,
         run.finished_at desc,
         run.ingestion_id desc
limit 1;
```

Read items only with all of `trade_date`, `session_id`, `sample_seq=31`, `batch_code='092520'`, and selected `ingestion_id`. Compute the response field inline:

```sql
case
  when coalesce(snapshot.ask1_volume, 0) = 0
   and coalesce(snapshot.ask2_volume, 0) = 0
   and coalesce(snapshot.ask3_volume, 0) = 0
   and snapshot.bid1_price is not null
   and snapshot.bid1_volume is not null
  then snapshot.bid1_price * snapshot.bid1_volume
  else null
end as seal_amount
```

For `query_auction_one_price_limits`, first select the latest eligible date when `p_trade_date is null`, then apply the same single-ingestion ranking inside that date. Copy the current downstream calendar, mainboard, price-limit, omission, JSON, `security definer`, fixed `search_path`, five-second timeout, revoke, and grant logic unchanged; only replace source selection and sealed-funds source.

- [ ] **Step 6: Add static production checks for the migration**

Assert the new migration contains both unchanged signatures, `sample_seq = 31`, `batch_code = '092520'`, the series dataset code, the sealed-funds expression, and no source-table reference in either function body:

```python
assert "realtime.call_auction_market_series_snapshot" in migration
assert "batch_code = '092520'" in migration
assert "realtime.call_auction_market_snapshot snapshot" not in migration
```

Keep both signatures in `PUBLISHED_FUNCTIONS`; they are unchanged.

- [ ] **Step 7: Run focused database and production checks**

Run:

```powershell
$env:TEST_DATABASE_URL='postgresql://postgres:pg123456@117.72.105.65:5433/postgres'
uv run pytest tests/test_postgres_integration.py -k "call_auction_market_snapshot_rpc or auction_one_price_limits or final_series_batch" -q
uv run pytest tests/test_production_checks.py -q
```

Expected: PASS.

- [ ] **Step 8: Commit the database contract change**

```powershell
git add -- supabase/migrations/20260904000100_read_auction_apis_from_final_series_batch.sql tests/test_postgres_integration.py tests/test_production_checks.py scripts/check_fastapi_release.py
git commit -m "feat: read auction APIs from final series batch"
```

---

### Task 2: Retire the duplicate 09:25:30 Worker job

**Files:**
- Modify: `src/market_data_center/scheduling_catalog.py:1-290`
- Modify: `src/market_data_center/scheduler.py:60-90,546-585,845-865`
- Modify: `src/market_data_center/settings.py:24-45`
- Modify: `tests/test_operations.py:145-185`
- Modify: `tests/test_scheduler.py:1-390,800-860`
- Modify: `tests/test_settings.py:10-45`

**Interfaces:**
- Consumes: existing `CALL_AUCTION_MARKET_SERIES_JOB_ID` and `run_call_auction_market_series_job`.
- Produces: a code-owned catalog with no `call-auction-market-snapshot-daily` job and no `CALL_AUCTION_SNAPSHOT_ENABLED` switch.

- [ ] **Step 1: Rewrite scheduler tests to express retirement**

Replace assertions that expect the job with explicit absence checks:

```python
jobs = {job.code: job for job in job_definitions(SchedulerSettings(_env_file=None))}
assert "call-auction-market-snapshot-daily" not in jobs

scheduler = build_scheduler(SchedulerSettings(_env_file=None))
assert scheduler.get_job("call-auction-market-snapshot-daily") is None
assert scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID) is not None
assert scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID).executor == "morning_auction"
```

Delete tests for `_scheduled_job_fire_time(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID)` and for independently enabling/disabling the retired job. Update expected catalog/job counts from 16 to 15 where present.

- [ ] **Step 2: Rewrite settings tests to remove the obsolete switch**

Remove positive assertions for `call_auction_snapshot_enabled` and add it to the absent-field check:

```python
assert not hasattr(SchedulerSettings(_env_file=None), "call_auction_snapshot_enabled")
```

Set `CALL_AUCTION_SNAPSHOT_ENABLED=false` in the environment test and confirm it is ignored by `extra='ignore'`, rather than controlling a job.

- [ ] **Step 3: Run scheduler/settings tests and observe failure**

Run:

```powershell
uv run pytest tests/test_operations.py tests/test_scheduler.py tests/test_settings.py -q
```

Expected: FAIL while the job constant, catalog definition, handler, and settings field still exist.

- [ ] **Step 4: Remove the scheduled-job surface**

In `scheduling_catalog.py`, delete `CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID` and its `JobDefinition`; keep the `call_auction_market_snapshot` workflow definition for historical Operations lineage.

In `scheduler.py`, delete the constant import, `run_call_auction_market_snapshot_job`, and the handler-map entry:

```python
job_handlers = {
    # no call-auction-market-snapshot-daily entry
    CALL_AUCTION_MARKET_SERIES_JOB_ID: run_call_auction_market_series_job,
    # remaining handlers unchanged
}
```

Remove imports used only by the deleted runner. Do not remove `call_auction_market_service.py` or the legacy persistence methods.

In `settings.py`, delete:

```python
call_auction_snapshot_enabled: bool = True
```

- [ ] **Step 5: Run focused scheduler/settings tests**

Run:

```powershell
uv run pytest tests/test_operations.py tests/test_scheduler.py tests/test_settings.py -q
```

Expected: PASS with the series job still enabled and the 09:25:30 job absent.

- [ ] **Step 6: Commit job retirement**

```powershell
git add -- src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py src/market_data_center/settings.py tests/test_operations.py tests/test_scheduler.py tests/test_settings.py
git commit -m "refactor: retire duplicate auction snapshot job"
```

---

### Task 3: Synchronize API descriptions, contracts, and operations documentation

**Files:**
- Modify: `src/market_data_center/public_api/app.py:470-590`
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `tests/test_api_contracts.py`
- Modify: `docs/领域详设-RealtimeQuote-2026-08-02.md`
- Modify: `docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md`
- Modify: `docs/数据库导航.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/集合竞价五档采集运行手册.md`
- Modify: `docs/最小生产发布运行手册.md`

**Interfaces:**
- Consumes: unchanged FastAPI route signatures and response models.
- Produces: checked-in contracts and runbooks that consistently describe the 09:25:20 final series batch.

- [ ] **Step 1: Add contract-description assertions**

In `tests/test_api_contracts.py`, assert the two FastAPI operations mention `09:25:20` and do not claim to read the independent 09:25:30 snapshot. Assert the PostgREST and Agent descriptions for `query_call_auction_market_snapshots` mention the final series batch:

```python
assert "09:25:20" in fastapi["paths"][snapshot_path]["post"]["description"]
assert "09:25:20" in fastapi["paths"][limits_path]["get"]["description"]
assert "092520" in postgrest["paths"][rpc_path]["post"]["description"]
```

- [ ] **Step 2: Run the contract test and observe failure**

Run:

```powershell
uv run pytest tests/test_api_contracts.py -q
```

Expected: FAIL because checked-in descriptions still advertise the independent snapshot source.

- [ ] **Step 3: Update FastAPI route descriptions**

Keep paths, parameters, tags, and response models unchanged. Change only summary/description text, for example:

```python
description=(
    "固定读取指定交易日沪深全市场竞价序列的 09:25:20 最后一批；"
    "成功 attempt 优先，没有成功 attempt 时允许 partial，不回退到更早轮次。"
)
```

For one-price limits, explicitly state that it reads the persisted 09:25:20 final series batch and performs no Provider call or write.

- [ ] **Step 4: Regenerate FastAPI OpenAPI and update the other checked-in contracts**

Run:

```powershell
uv run python scripts/export_fastapi_openapi.py
```

Expected: `contracts/fastapi-openapi-v1.json` changes only in the two operation descriptions. Update the existing PostgREST RPC and Agent tool descriptions manually without changing schemas, paths, parameter bounds, or response fields.

- [ ] **Step 5: Update current operational/domain documentation**

Apply these exact facts consistently:

- Worker catalog has 15 jobs and no 09:25:30 full-market snapshot job.
- Both APIs read `call_auction_market_series_snapshot` at `batch_code=092520`.
- succeeded is preferred, partial is fallback, and no earlier round/date/legacy-table fallback exists.
- `seal_amount` is computed at read time from ask1/ask2/ask3 volume and bid1 price/volume.
- `call_auction_market_snapshot` is retained as historical internal facts and is not deleted by series retention cleanup.
- Availability starts only after the final series round is committed.

Do not rewrite historical ADRs or old plans as though they had always made the new decision; ADR-0052 records the change.

- [ ] **Step 6: Run contract and documentation guards**

Run:

```powershell
uv run pytest tests/test_api_contracts.py tests/test_production_checks.py -q
rg -n "call-auction-market-snapshot-daily|09:25:30" docs/Worker调度系统.md docs/Worker日常采集与调度.md docs/集合竞价五档采集运行手册.md docs/最小生产发布运行手册.md docs/领域详设-RealtimeQuote-2026-08-02.md docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md
```

Expected: tests PASS; any remaining grep matches describe historical facts or explicit retirement, not an active job.

- [ ] **Step 7: Commit contracts and documentation**

```powershell
git add -- src/market_data_center/public_api/app.py contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json tests/test_api_contracts.py docs/领域详设-RealtimeQuote-2026-08-02.md docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md docs/数据库导航.md docs/Worker调度系统.md docs/Worker日常采集与调度.md docs/集合竞价五档采集运行手册.md docs/最小生产发布运行手册.md
git commit -m "docs: publish final auction series batch semantics"
```

---

### Task 4: Run release gates and prepare deployment evidence

**Files:**
- Verify only: all files changed by Tasks 1-3

**Interfaces:**
- Consumes: migration `20260904000100`, retired Worker job catalog, synchronized API contracts.
- Produces: a clean commit series with reproducible local and isolated-database verification evidence.

- [ ] **Step 1: Run focused tests together**

Run:

```powershell
uv run pytest tests/test_operations.py tests/test_scheduler.py tests/test_settings.py tests/test_api_contracts.py tests/test_production_checks.py -q
$env:TEST_DATABASE_URL='postgresql://postgres:pg123456@117.72.105.65:5433/postgres'
uv run pytest tests/test_postgres_integration.py -k "call_auction_market_snapshot_rpc or auction_one_price_limits or final_series_batch" -q
```

Expected: PASS.

- [ ] **Step 2: Run the complete local gate**

Run:

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all commands PASS. If the full suite exceeds the available window, report the exact timeout and retain the completed focused/integration evidence; do not claim a full pass.

- [ ] **Step 3: Inspect the final diff and repository state**

Run:

```powershell
git diff 56c4038..HEAD --check
git status --short
git log --oneline -5
```

Expected: no whitespace errors, no uncommitted files, and separate commits for database behavior, job retirement, and contracts/docs.

- [ ] **Step 4: Prepare protected production steps without executing them**

Record the ordered release sequence for explicit user authorization:

```text
1. push the reviewed commits
2. apply migration 20260904000100 through the protected migration workflow
3. deploy the same commit to Worker and API
4. restart Worker and API
5. verify Worker catalog has no call-auction-market-snapshot-daily
6. smoke-test both FastAPI endpoints against an exact date with 092520 data
7. on the next trading day verify 32/32 rounds and no 09:25:30 job execution
```

Do not migrate production, deploy, restart services, or mutate production data without a separate explicit request.
