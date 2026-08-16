# THS 883423 Bias Live Fallback Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep `GET /api/v1/board-indexes/883423/bias` database-first, but return a freshly calculated THS result when the stored board history is missing, insufficient, or stale, then persist that fetched history asynchronously with complete Raw and ingestion lineage.

**Architecture:** The bounded read RPC remains the only normal read path and signals an expected cache miss with SQLSTATE `P0002`. A FastAPI live-fallback service then fetches fixed annual THS payloads through the existing `akshare_ths` adapter, calculates the same Decimal-safe result, writes a lossless JSONL Raw envelope before responding, and submits a prepared write to a dedicated one-running/one-waiting queue. A narrowly granted security-definer RPC performs idempotent lineage registration and Core upsert in one transaction; FastAPI never receives direct internal-table DML privileges.

**Tech Stack:** Python 3.12, FastAPI, Pydantic 2, SQLAlchemy 2, PostgreSQL 15, existing `akshare_ths` provider, `Decimal`, `ThreadPoolExecutor`, pytest

**Spec:** `docs/superpowers/specs/2026-08-15-board-index-bias-live-fallback-design.md`

## Global Constraints

- The endpoint is fixed to `THS:883423`; it accepts neither a board code nor a trading date.
- Database fallback is permitted only for SQLSTATE `P0002`. Authentication, connectivity, permission, timeout, and malformed-response errors propagate without provider access.
- The database RPC raises `P0002` when fewer than 34 bars exist or the latest bar predates the latest expected `CN_A_SHARE` trading date at request time.
- THS requests use the fixed URL `https://d.10jqka.com.cn/v4/line/bk_883423/01/{year}.js`, a five-second timeout, current year first, and previous year only when fewer than 34 unique bars were obtained.
- Provider field names stop at the adapter. Downstream code receives `BoardIndexDailyBarRecord` values and immutable Raw payload envelopes.
- Raw storage remains JSONL. Each row stores URL, year, encoding, and the exact HTTP response bytes as base64, so the source bytes are losslessly recoverable; the input hash is computed over length-prefixed original byte payloads.
- The live calculator uses the ADR-0035 formula unchanged: current five-session simple MA, Decimal BIAS5, prior-session direction, and latest-30 valid-sample extrema with latest-date tie breaking.
- A live response reports `data_origin="ths_live"`, `persistence_status="queued"`, and its fetch timestamp. A database response reports `database`, `persisted`, and request timestamp.
- Raw capture must succeed before a live response is returned. Raw or enqueue failure is HTTP 503, THS failure is HTTP 502, and a full live-fetch/write capacity gate is HTTP 429.
- The persistence executor has exactly one running and one waiting slot. Shutdown drains accepted writes. No Worker job, APScheduler catalog entry, cron task, or `.env` schedule setting is added.
- Only focused unit, migration-contract, API-contract, and narrowly selected PostgreSQL integration tests are run; the long full PostgreSQL suite is excluded.

---

### Task 1: Pure bias calculation and response metadata

**Files:**
- Create: `src/market_data_center/board_index_bias.py`
- Modify: `src/market_data_center/public_api/models.py`
- Create: `tests/test_board_index_bias.py`
- Modify: `tests/test_public_api.py`

**Interfaces:**
- `calculate_board_index_bias(records, *, fetched_at, data_origin, persistence_status) -> BoardIndexBiasResponse`
- `BoardIndexBiasResponse` adds `data_origin`, `persistence_status`, and `fetched_at` without changing existing price, bias, direction, or extrema fields.

- [ ] **Step 1: Write failing calculator and serialization tests**

Cover 34+ unordered records, duplicate-date rejection, fewer than five usable closes, exact Decimal math, unchanged latest-30 extrema/tie rules, and response metadata. Extend the HTTP fixture to assert an ISO timestamp serializes consistently.

- [ ] **Step 2: Run the focused tests and verify RED**

```powershell
uv run pytest tests/test_board_index_bias.py tests/test_public_api.py -k "board_index_bias" -q
```

Expected: imports/model construction fail because the calculator and metadata fields do not exist.

- [ ] **Step 3: Implement the pure calculator and typed metadata**

Keep the calculator free of database, HTTP, Raw Store, queue, clock, and environment access. Sort by `trade_date`, preserve `None`, reject duplicate natural keys, use only `Decimal`, and return the existing `board_index_bias_v1` calculation version.

- [ ] **Step 4: Run Step 2 and verify GREEN**

- [ ] **Step 5: Commit**

```powershell
git add src/market_data_center/board_index_bias.py src/market_data_center/public_api/models.py tests/test_board_index_bias.py tests/test_public_api.py
git commit -m "feat: add reusable board index bias calculator"
```

### Task 2: THS live payload boundary with lossless source capture

**Files:**
- Modify: `src/market_data_center/providers/akshare_ths.py`
- Modify: `tests/test_akshare_ths_provider.py`

**Interfaces:**
- Add immutable annual-payload and live-batch value objects containing request year, URL, exact response bytes, fetch timestamp, and standardized records.
- Add a bounded live history method for fixed board `883423` that requests the current annual payload, then the previous annual payload only when the deduplicated history is shorter than 34.
- Preserve the existing provider ingestion interface and its default timeout; API construction explicitly supplies five seconds.

- [ ] **Step 1: Write failing provider tests**

Mock HTTP bytes for the real `quotebridge_v4_line_bk_883423_01_2026(...)` wrapper. Assert exact bytes survive, field parsing yields standard records, current-year-only behavior at 34 rows, previous-year behavior below 34 rows, date deduplication, provider errors, and the configured timeout.

- [ ] **Step 2: Run and verify RED**

```powershell
uv run pytest tests/test_akshare_ths_provider.py -q
```

- [ ] **Step 3: Refactor the HTTP client at the provider boundary**

Make the low-level request return bytes before decoding; parse only inside `akshare_ths`. Do not leak THS keys or wrapper text into the calculator or persistence record payload. Keep URL construction fixed and validate board identity before any request.

- [ ] **Step 4: Run Step 2 and verify GREEN**

- [ ] **Step 5: Commit**

```powershell
git add src/market_data_center/providers/akshare_ths.py tests/test_akshare_ths_provider.py
git commit -m "feat: expose lossless THS board history fetch"
```

### Task 3: Database freshness contract and idempotent persistence RPC

**Files:**
- Create: `supabase/migrations/20260815000200_board_index_bias_live_fallback.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Replace `api_v1.query_board_index_bias_latest()` so its JSON includes response metadata and it raises SQLSTATE `P0002` for empty, fewer-than-34, or stale history.
- Add `api_v1.persist_board_index_daily_bars_live(...) returns jsonb`, granted only to `market_data_api`.
- The write RPC accepts caller-generated lineage IDs, Raw manifest metadata, request/input hashes, source years, and normalized records; it validates fixed provider/dataset/board identity and persists everything atomically.

- [ ] **Step 1: Write failing focused integration and static security tests**

Test ready database output, 33-row miss, stale-latest-date miss, non-trading-day freshness behavior, exact SQLSTATE, metadata, role grants, invalid payload rejection, successful lineage/Core write, repeated-input idempotency, and rollback on invalid records. Extend migration and release-function catalogs.

- [ ] **Step 2: Run and verify RED**

```powershell
uv run pytest tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -k "board_index_bias or persist_board_index_daily_bars_live" -q
```

If `TEST_DATABASE_URL` is not configured, record that exact reason and continue only with the static test; never substitute production.

- [ ] **Step 3: Implement one ordered migration**

Use `Asia/Shanghai` and `core.trading_calendar` to derive the latest expected trading date not later than today. Preserve the existing bounded 34-row SQL calculation. The strict security-definer persistence RPC must set a bounded statement timeout and safe search path, reject unexpected board/provider/dataset values, validate natural-key uniqueness and OHLC/nonnegative/calendar rules, register `ingestion.ingestion_run` and `ingestion.raw_manifest`, and upsert `core.board_index_daily_bar` in one transaction. Use the input hash under an advisory transaction lock to return the prior successful registration on retry rather than duplicate lineage. Revoke from `public`, `anon`, and `authenticated`; grant only `market_data_api`.

- [ ] **Step 4: Run Step 2 and verify GREEN**

- [ ] **Step 5: Commit**

```powershell
git add supabase/migrations/20260815000200_board_index_bias_live_fallback.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: add board index live persistence contract"
```

### Task 4: Raw preparation and bounded asynchronous writer

**Files:**
- Create: `src/market_data_center/public_api/board_index_bias_write.py`
- Create: `tests/test_board_index_bias_write.py`
- Modify: `src/market_data_center/public_api/queries.py`

**Interfaces:**
- `PreparedBoardIndexPersistence` is immutable and contains Raw manifest facts plus normalized records.
- `BoardIndexApiPersistence.prepare(live_batch)` synchronously writes JSONL Raw envelopes with base64 source bytes and returns prepared metadata.
- `BoardIndexPersistenceQueue.submit(prepared)` is nonblocking and permits one running plus one waiting write.
- `PublicQueryService.persist_board_index_daily_bars_live(...)` invokes only the new `api_v1` RPC.

- [ ] **Step 1: Write failing preparation, queue, and RPC tests**

Assert exact byte recovery after JSONL decoding, deterministic length-prefixed input hashing, manifest hash/size/count, Raw failure before enqueue, two accepted writes, third rejection, single-worker ordering, RPC argument shape, successful cleanup, and shutdown draining.

- [ ] **Step 2: Run and verify RED**

```powershell
uv run pytest tests/test_board_index_bias_write.py -q
```

- [ ] **Step 3: Implement using established auction persistence patterns**

Reuse `LocalRawStore.write_jsonl`, SQLAlchemy transaction handling, `ThreadPoolExecutor(max_workers=1)`, and `BoundedSemaphore(2)`. Do not add a second database abstraction, durable job state, retry daemon, or direct internal-schema DML.

- [ ] **Step 4: Run Step 2 and verify GREEN**

- [ ] **Step 5: Commit**

```powershell
git add src/market_data_center/public_api/board_index_bias_write.py src/market_data_center/public_api/queries.py tests/test_board_index_bias_write.py
git commit -m "feat: queue live board index persistence"
```

### Task 5: DB-first live service and FastAPI lifecycle wiring

**Files:**
- Create: `src/market_data_center/public_api/board_index_bias_live.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/settings.py`
- Create: `tests/test_board_index_bias_live.py`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_settings.py`

**Interfaces:**
- The route first calls `PublicQueryService.board_index_bias_latest()`.
- Only an extracted PostgreSQL SQLSTATE of `P0002` invokes `BoardIndexBiasLiveService.fetch_prepare_and_enqueue()`.
- Live service maps capacity to 429, provider errors to 502, and Raw/enqueue/persistence-setup errors to 503.
- App lifespan owns and drains the queue and disposes its write engine. Injected services remain externally owned for tests.

- [ ] **Step 1: Write failing orchestration and route tests**

Cover database hit with zero provider calls, `P0002` fallback, all other database exceptions without fallback, current/previous-year behavior, response-before-background-write completion, Raw-before-response ordering, calculator parity with stored output, 429/502/503 mappings, auth, and lifespan shutdown.

- [ ] **Step 2: Run and verify RED**

```powershell
uv run pytest tests/test_board_index_bias_live.py tests/test_public_api.py tests/test_settings.py -k "board_index_bias or live_raw_root" -q
```

- [ ] **Step 3: Implement the smallest orchestration layer**

Reuse the existing protected API Raw root rather than adding schedule settings. Add dependency injection for the live service, an owned single-worker queue in `create_app`, a strict SQLSTATE helper, and narrow exception handlers. Do not cache a live response or silently retry non-`P0002` database errors.

- [ ] **Step 4: Run Step 2 and verify GREEN**

- [ ] **Step 5: Commit**

```powershell
git add src/market_data_center/public_api/board_index_bias_live.py src/market_data_center/public_api/app.py src/market_data_center/settings.py tests/test_board_index_bias_live.py tests/test_public_api.py tests/test_settings.py
git commit -m "feat: add DB-first board index live fallback"
```

### Task 6: Contracts, operational documentation, and focused release gate

**Files:**
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/同花顺动态板块指数采集.md`
- Modify: `deploy/linux/market-data-center-api.env.example`
- Modify: `scripts/check_fastapi_release.py`
- Modify: `tests/test_api_contracts.py`

- [ ] **Step 1: Write failing contract/release assertions**

Assert the three response metadata fields, documented 429/502/503 responses, both read/write RPC contract entries, the new migration/function in the release preflight, and absence of scheduler configuration.

- [ ] **Step 2: Run and verify RED**

```powershell
uv run pytest tests/test_api_contracts.py tests/test_production_checks.py -q
uv run python scripts/check_fastapi_release.py --help
```

- [ ] **Step 3: Synchronize checked-in contracts and documentation**

Document database-first semantics, freshness definition, live source URL, Raw-before-response guarantee, asynchronous persistence, capacity/error codes, and that no new schedule setting exists. Describe the existing API Raw root as shared protected live-source storage without publishing credentials or local paths.

- [ ] **Step 4: Run the focused final gate**

```powershell
uv run ruff format --check src/market_data_center/board_index_bias.py src/market_data_center/providers/akshare_ths.py src/market_data_center/public_api tests/test_board_index_bias.py tests/test_board_index_bias_live.py tests/test_board_index_bias_write.py tests/test_akshare_ths_provider.py tests/test_public_api.py
uv run ruff check src/market_data_center/board_index_bias.py src/market_data_center/providers/akshare_ths.py src/market_data_center/public_api tests/test_board_index_bias.py tests/test_board_index_bias_live.py tests/test_board_index_bias_write.py tests/test_akshare_ths_provider.py tests/test_public_api.py tests/test_api_contracts.py tests/test_production_checks.py
uv run mypy src
uv run pytest tests/test_board_index_bias.py tests/test_board_index_bias_live.py tests/test_board_index_bias_write.py tests/test_akshare_ths_provider.py tests/test_public_api.py tests/test_api_contracts.py tests/test_production_checks.py -q
uv run pytest tests/test_postgres_integration.py -k "board_index_bias or persist_board_index_daily_bars_live" -q
```

Do not run the full unfiltered PostgreSQL integration suite. If the disposable test database is unavailable, report the skipped integration command and reason explicitly.

- [ ] **Step 5: Inspect the complete diff for scope and safety**

```powershell
git status --short
git diff --check
git diff --stat HEAD~5..HEAD
git diff HEAD~5..HEAD -- . ":(exclude)uv.lock"
```

Verify there are no TODO/TBD placeholders, secrets, direct internal-table access from FastAPI, provider-field leakage, OS scheduling changes, unrelated rewrites, or type/contract mismatches.

- [ ] **Step 6: Commit final synchronization**

```powershell
git add contracts/fastapi-openapi-v1.json contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json docs/FastAPI外部接口.md docs/同花顺动态板块指数采集.md deploy/linux/market-data-center-api.env.example scripts/check_fastapi_release.py tests/test_api_contracts.py
git commit -m "docs: publish board index live fallback contract"
```

- [ ] **Step 7: Stop before production mutation**

Report focused verification results and the commit range. Pushing, applying the production migration, switching the release symlink, and restarting services require the user's explicit deployment instruction and production preflight at that time.
