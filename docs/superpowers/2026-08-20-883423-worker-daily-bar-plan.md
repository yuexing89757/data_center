# THS:883423 Worker Daily Bar Implementation Plan

**Goal:** Move `THS:883423` daily-bar recovery from the FastAPI request path into a resilient,
code-scheduled Worker job while keeping the bias endpoint database-only.

**Architecture:** APScheduler registers one controlled catalog job with three code-owned close
slots. Its callback resolves the expected trading date, skips an already-complete target, and
uses the existing BoardIndex daily-bar pipeline with bounded Provider retries. A forward-only
migration changes the bounded query RPC to accept the latest persisted date and removes the API
write RPC; FastAPI then has no live Provider or writer path.

**Tech Stack:** Python 3.12, APScheduler, PostgreSQL migrations, FastAPI, pytest, uv.

---

## Task 1: Lock the catalog and domain contract with failing tests

**Files:**
- Modify: `tests/test_operations.py`
- Modify: `tests/test_scheduler.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/scheduling_catalog.py`

Add tests for the new workflow code, default-enabled switch, one logical job, and fixed weekday
slots 15:30/16:30/17:30. Run the focused tests and confirm they fail before extending the catalog
and trigger representation minimally.

## Task 2: Implement idempotent collection and bounded retries test-first

**Files:**
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_pipeline.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/pipeline.py` only if a small bounded query helper is required

Test the already-current no-op, missing-range selection, three Provider attempts, early success,
and propagated non-Provider failure. Implement the callback by composing existing calendar,
persistence, workflow execution, and `ingest_board_index_daily_bars` behavior; do not duplicate
Provider adaptation or persistence.

## Task 3: Make the FastAPI route database-only

**Files:**
- Modify: `tests/test_public_api.py`
- Delete or retire: `tests/test_board_index_bias_live.py`
- Delete or retire: `tests/test_board_index_bias_write.py`
- Modify: `src/market_data_center/public_api/app.py`
- Delete or retire: `src/market_data_center/public_api/board_index_bias_live.py`
- Delete or retire: `src/market_data_center/public_api/board_index_bias_write.py`
- Modify: `src/market_data_center/public_api/queries.py`

First change tests so stale-but-sufficient database data is returned and no Provider/writer is
constructed or called. Then remove the fallback exception branch, live error handlers, queue
lifecycle, and write plumbing. Preserve bounded RPC-only access and 404 mapping for insufficient
history.

## Task 4: Apply the forward-only database boundary migration

**Files:**
- Create: `supabase/migrations/20260820000100_board_index_daily_bar_worker_schedule.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `scripts/check_fastapi_release.py`

Test and implement: extend the controlled workflow code constraint, replace
`query_board_index_bias_latest()` without the current-date freshness rejection, and revoke/drop
the fixed live-persistence RPC. Keep the 34-row bound, statement timeout, API role grants, numeric
semantics, and `P0002` behavior for insufficient history.

## Task 5: Synchronize documentation and checked-in contracts

**Files:**
- Modify: `docs/同花顺动态板块指数采集.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `contracts/fastapi-openapi-v1.json` if generated output changes
- Modify: other checked-in contracts only if their public surface changes

Document the three fixed slots, enable/disable-only environment setting, retry/backfill semantics,
database-only API behavior, and actual-date response. Regenerate contract output using the
repository script instead of hand-editing generated JSON.

## Task 6: Verify locally

Run focused tests during each red-green loop, then execute:

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Run PostgreSQL integration tests only against an explicitly configured disposable
`TEST_DATABASE_URL`. Do not apply the migration to production or deploy without a separate explicit
user request.
