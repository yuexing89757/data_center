# Eastmoney Trading Billboard Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a traceable Eastmoney-backed `TradingBillboard` domain that collects A-share daily billboard summaries and buy/sell top-five seats, schedules bounded Worker ingestion, supports explicit backfill and exposes bounded reads by date, symbol and seat.

**Architecture:** A dedicated Eastmoney capability returns one aggregate per source event, with all source-specific fields contained in the Provider and a combined versioned Raw JSONL. Domain validation rejects incomplete daily aggregates before one-transaction persistence to internal `billboard` tables. Three `api_v1` JSON RPCs are the only public read boundary and the external FastAPI delegates to them.

**Tech Stack:** Python 3.12, dataclasses, `Decimal`, urllib, SQLAlchemy, PostgreSQL ordered migrations, APScheduler, FastAPI/Pydantic, pytest, Ruff, mypy, uv.

**Spec:** `docs/superpowers/specs/2026-08-24-eastmoney-trading-billboard-design.md`

## Global Constraints

- Governing work item is GitHub Issue #65; ADR-0046 is Accepted and `docs/领域详设-TradingBillboard-2026-08-24.md` is effective.
- First release accepts only `SSE|SZSE|BSE:NNNNNN` stocks; convertible-bond rows remain Raw-only.
- One successful ingestion has exactly one provider, `eastmoney`; no fallback, discovery or source merging.
- Prices, ratios and amounts use `Decimal`; amounts are CNY and percentage fields are percentage points; never route values through `float`.
- Domain records do not contain `ingestion_id`; Persistence attaches ingestion lineage at the write boundary.
- Raw objects are immutable, use schema `eastmoney.trading_billboard.v1`, and replay never accesses Eastmoney.
- Every scheduled run is registered in the Worker APScheduler code catalog; never create cron or Windows Task Scheduler instructions.
- Internal `billboard` tables are never public. FastAPI reads only bounded `api_v1` RPCs with a five-second statement timeout.
- Date ranges are at most 366 natural days, `limit` is 1–500, and `offset` is 0–10,000.
- Scheduler enablement defaults to false until the project owner records completion of Eastmoney source-rights review.
- Production migrations or live collection are not part of plan execution; integration tests use only disposable `TEST_DATABASE_URL`.

## File Structure

**Create:**

- `src/market_data_center/domain/trading_billboard.py` — immutable records, hashes, natural keys and pure validation.
- `src/market_data_center/providers/eastmoney_trading_billboard.py` — three-report bounded HTTP adapter and v1 Raw normalizer.
- `src/market_data_center/persistence/trading_billboard_postgres.py` — validation context and atomic ingestion/fact persistence.
- `src/market_data_center/trading_billboard_service.py` — exact-day collection and bounded range orchestration.
- `supabase/migrations/20260824000200_create_trading_billboard.sql` — internal schema, constraints, grants, Operations codes and three `api_v1` RPCs.
- `tests/test_trading_billboard.py` — domain behavior.
- `tests/test_eastmoney_trading_billboard_provider.py` — mocked source and Raw replay behavior.
- `tests/test_trading_billboard_service.py` — orchestration and failure semantics.

**Modify:**

- `src/market_data_center/domain/__init__.py` — public domain exports.
- `src/market_data_center/domain/ingestion.py` — `TRADING_BILLBOARD` dataset code.
- `src/market_data_center/domain/operations.py` — `TRADING_BILLBOARD_DAILY` workflow code.
- `src/market_data_center/providers/contracts.py` — dedicated capability and ProviderRecord union.
- `src/market_data_center/providers/__init__.py` — Provider exports only; do not add to ordinary provider registry.
- `src/market_data_center/reliability.py` — dispatch v1 Raw replay to the dedicated normalizer.
- `src/market_data_center/settings.py` — opt-in scheduler setting.
- `src/market_data_center/scheduling_catalog.py` — workflow/job definitions at 20:30 Asia/Shanghai.
- `src/market_data_center/scheduler.py` — job runner and function registration.
- `src/market_data_center/cli.py` — exact-date and bounded-range manual command.
- `src/market_data_center/public_api/models.py` — summary, seat and page response models.
- `src/market_data_center/public_api/queries.py` — three database query methods.
- `src/market_data_center/public_api/app.py` — three authenticated read endpoints.
- `src/market_data_center/public_api/openapi_zh.py` — owned endpoint descriptions.
- `tests/test_ingestion_models.py`, `tests/test_operations.py` — new enum/workflow behavior.
- `tests/test_cli.py`, `tests/test_scheduler.py`, `tests/test_settings.py`, `tests/test_worker_admin.py` — command and Worker catalog behavior.
- `tests/test_postgres_integration.py`, `tests/test_production_checks.py` — migration, persistence, constraints, grants and RPCs.
- `tests/test_public_api.py`, `tests/test_api_contracts.py` — FastAPI and checked-in contracts.
- `contracts/postgrest-openapi-v1.json`, `contracts/agent-tools-v1.json`, `contracts/fastapi-openapi-v1.json` — stable contracts.
- `.env.example`, `deploy/linux/market-data-center.env.example`, `README.md`, `docs/Worker日常采集与调度.md`, `docs/FastAPI外部接口.md`, `docs/数据库导航.md` — opt-in operation and published behavior.

---

### Task 1: Domain Records, Natural Keys and Pure Validation

**Files:**

- Create: `src/market_data_center/domain/trading_billboard.py`
- Modify: `src/market_data_center/domain/__init__.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/providers/contracts.py`
- Test: `tests/test_trading_billboard.py`
- Test: `tests/test_ingestion_models.py`
- Test: `tests/test_operations.py`

**Interfaces:**

- Produces: `TradingBillboardSide`, `TradingBillboardSeatRecord`, `TradingBillboardRecord`, `TradingBillboardFinding`, `TradingBillboardValidationResult`, `trading_billboard_natural_key()`, `trading_billboard_content_hash()`, `validate_trading_billboards()`.
- Produces: `DatasetCode.TRADING_BILLBOARD`, `WorkflowCode.TRADING_BILLBOARD_DAILY` and `TradingBillboardProvider.fetch_trading_billboard(trade_date)`.

- [ ] **Step 1: Write failing record and validation tests**

Add tests constructing one valid aggregate with two immutable seat tuples and assert:

```python
assert trading_billboard_natural_key(record) == ("eastmoney", "100396303")
assert record.buy_seats[0].symbol == "SZSE:000711"
assert record.buy_seats[0].trade_date == date(2026, 8, 17)
assert trading_billboard_content_hash(record) == trading_billboard_content_hash(record)
```

Add parameterized failures for an unsupported symbol, side/rank mismatch, duplicate ranks, non-contiguous ranks,
seat parent mismatch, negative non-net amount, `deal_amount != buy_amount + sell_amount`,
`net_amount != buy_amount - sell_amount`, duplicate source key and conflicting semantic key. Assert repeated
`seat_code=None` is accepted.

- [ ] **Step 2: Run the new domain tests and verify RED**

Run: `uv run pytest tests/test_trading_billboard.py tests/test_ingestion_models.py tests/test_operations.py -q`

Expected: collection fails because the trading billboard module and enum members do not exist.

- [ ] **Step 3: Implement immutable records and deterministic hash**

Define these exact public shapes:

```python
class TradingBillboardSide(StrEnum):
    BUY = "buy"
    SELL = "sell"

@dataclass(frozen=True, slots=True)
class TradingBillboardSeatRecord:
    source_event_id: str
    symbol: str
    trade_date: date
    side: TradingBillboardSide
    rank: int
    seat_code: str | None
    seat_name: str
    buy_amount: Decimal | None
    sell_amount: Decimal | None
    net_amount: Decimal | None
    buy_to_market_pct: Decimal | None
    sell_to_market_pct: Decimal | None
    source_code: str = "eastmoney"

@dataclass(frozen=True, slots=True)
class TradingBillboardRecord:
    symbol: str
    trade_date: date
    source_event_id: str
    reason_code: str
    reason_text: str
    close_price: Decimal | None
    change_rate_pct: Decimal | None
    turnover_rate_pct: Decimal | None
    market_amount: Decimal | None
    buy_amount: Decimal
    sell_amount: Decimal
    net_amount: Decimal
    deal_amount: Decimal
    deal_to_market_pct: Decimal | None
    net_to_market_pct: Decimal | None
    free_float_market_value: Decimal | None
    buy_seats: tuple[TradingBillboardSeatRecord, ...]
    sell_seats: tuple[TradingBillboardSeatRecord, ...]
    source_code: str = "eastmoney"
```

Hash a canonical JSON representation with sorted keys, decimal values serialized as strings, seat order preserved,
and SHA-256 lowercase hex output. Keep `__post_init__` checks local and use
`validate_trading_billboards(records, known_symbols, known_trading_dates)` for cross-record/context rules.

- [ ] **Step 4: Add dataset, workflow and dedicated Provider capability**

Add the enum values and extend `ProviderRecord` with `TradingBillboardRecord`. Define:

```python
class TradingBillboardProvider(Protocol):
    source_code: str
    def fetch_trading_billboard(
        self, trade_date: date
    ) -> "ProviderBatch[TradingBillboardRecord]": ...
```

Do not add Eastmoney trading billboard to `_PROVIDER_FACTORIES`; this is a dedicated capability.

- [ ] **Step 5: Run focused tests and type checks GREEN**

Run: `uv run pytest tests/test_trading_billboard.py tests/test_ingestion_models.py tests/test_operations.py -q`

Run: `uv run mypy src/market_data_center/domain/trading_billboard.py src/market_data_center/providers/contracts.py`

Expected: all pass.

- [ ] **Step 6: Commit the domain boundary**

```powershell
git add src/market_data_center/domain src/market_data_center/providers/contracts.py tests/test_trading_billboard.py tests/test_ingestion_models.py tests/test_operations.py
git commit -m "feat: define trading billboard domain"
```

---

### Task 2: Bounded Eastmoney Adapter and Raw v1 Normalizer

**Files:**

- Create: `src/market_data_center/providers/eastmoney_trading_billboard.py`
- Modify: `src/market_data_center/providers/__init__.py`
- Test: `tests/test_eastmoney_trading_billboard_provider.py`

**Interfaces:**

- Consumes: Task 1 domain records and `ProviderBatch`.
- Produces: `EastmoneyTradingBillboardProvider.fetch_trading_billboard()` and
  `normalize_eastmoney_trading_billboard_raw(rows, schema_version)`.

- [ ] **Step 1: Write mocked three-report tests**

Use a fake request callable that parses `reportName` and `pageNumber` from the URL and returns fixed pages for:

```python
SUMMARY_REPORT = "RPT_DAILYBILLBOARD_DETAILS"
BUY_REPORT = "RPT_BILLBOARD_DAILYDETAILSBUY"
SELL_REPORT = "RPT_BILLBOARD_DAILYDETAILSSELL"
```

Assert the adapter emits one stock aggregate, filters a convertible-bond event from standard records, retains all
source rows in Raw, maps `.SH/.SZ/.BJ`, normalizes seat code `0` to `None`, and creates independent ranks 1–5.

- [ ] **Step 2: Add failure and replay tests**

Cover non-success response, absent `result`, missing required fields, malformed decimal, response date mismatch,
duplicate page, changing `count`, more than `MAX_PAGES`, a missing buy/sell event, conflicting summary keys, and
network failure after exactly two attempts. Assert `normalize_eastmoney_trading_billboard_raw(batch.raw_rows,
batch.schema_version) == batch.records`.

- [ ] **Step 3: Run Provider tests RED**

Run: `uv run pytest tests/test_eastmoney_trading_billboard_provider.py -q`

Expected: import fails because the dedicated adapter does not exist.

- [ ] **Step 4: Implement bounded HTTP and pagination**

Use these code-owned bounds:

```python
ENDPOINT = "https://datacenter-web.eastmoney.com/api/data/v1/get"
SCHEMA_VERSION = "eastmoney.trading_billboard.v1"
PAGE_SIZE = 500
MAX_PAGES = 20
MAX_RESPONSE_BYTES = 2_000_000
DEFAULT_TIMEOUT_SECONDS = 8.0
MAX_ATTEMPTS = 2
```

The default client uses `urllib.request`, a MarketDataCenter User-Agent and Eastmoney data-page Referer. It requests
one exact `TRADE_DATE` at a time. Validate `success`, `result.count`, `result.pages`, row sequences and exact date on
every page; never infer an empty response from an error.

- [ ] **Step 5: Preserve source JSON inside string-only Raw rows**

Represent each source object as:

```python
{
    "record_kind": "summary",  # or buy_seat / sell_seat
    "source_page": "1",
    "source_index": "0",
    "payload_json": dumps(source_row, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
}
```

This preserves null, numeric and string distinctions without widening the repository-wide `RawRow` type. The v1
normalizer parses only `payload_json` and rejects an unknown `record_kind` or schema version.

- [ ] **Step 6: Implement aggregate joining and deterministic ranks**

Accept summary rows only when their source security type is stock and the suffix maps to SSE/SZSE/BSE. Retain all
detail rows in Raw, but normalize only details whose `TRADE_ID` belongs to an accepted summary. Require one to five
rows per side. Sort each side by its corresponding amount descending, then normalized seat code, name and canonical
row hash. Do not assert that top-five sums equal summary totals.

- [ ] **Step 7: Run Provider tests and lint GREEN**

Run: `uv run pytest tests/test_eastmoney_trading_billboard_provider.py -q`

Run: `uv run ruff check src/market_data_center/providers/eastmoney_trading_billboard.py tests/test_eastmoney_trading_billboard_provider.py`

Expected: all pass without network access.

- [ ] **Step 8: Commit the source adapter**

```powershell
git add src/market_data_center/providers/eastmoney_trading_billboard.py src/market_data_center/providers/__init__.py tests/test_eastmoney_trading_billboard_provider.py
git commit -m "feat: adapt Eastmoney trading billboard data"
```

---

### Task 3: Ordered PostgreSQL Schema, Constraints and Read RPCs

**Files:**

- Create: `supabase/migrations/20260824000200_create_trading_billboard.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Produces: `billboard.entry`, `billboard.seat`, dataset/workflow constraints and the three `api_v1` functions.
- Produces RPC payloads consumed by Task 7.

- [ ] **Step 1: Write failing migration integration tests**

Add tests that run all migrations in a disposable database, insert one Security, trading date, ingestion, entry and
ten seats, then assert primary/unique/composite foreign keys reject mismatched symbol/date/event and duplicate rank.
Assert `market_data_api` cannot select internal tables and can execute all three RPCs.

- [ ] **Step 2: Write failing RPC behavior tests**

Assert:

```text
query_trading_billboard_by_date(date,100,0)
query_trading_billboard_by_symbol('SSE:600000',start,end,100,0)
query_trading_billboard_by_seat(code,null,start,end,null,100,0)
query_trading_billboard_by_seat(null,'机构专用',start,end,'buy',100,0)
```

return stable JSON envelopes with `items`, `returned_count`, `total_count`, `has_more`, `limit`, and `offset`.
Assert exact-date no-fallback, exact seat-name matching, mutual exclusion, blank rejection, 366-day/limit/offset bounds,
stable ordering and SQLSTATE `22023` for invalid input.

- [ ] **Step 3: Run integration tests RED**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k trading_billboard -q`

Expected: failure because the schema and functions do not exist.

- [ ] **Step 4: Implement schema and ingestion/operations constraints**

The migration must:

```sql
create schema if not exists billboard;
alter table ingestion.ingestion_run ... check (...,'trading_billboard') not valid;
alter table audit.quality_result ... check (...,'trading_billboard') not valid;
alter table operations.workflow_run ... check (...,'trading_billboard_daily') not valid;
```

Rebuild each existing check with every previously accepted value plus the new value, then validate it. Create
`billboard.entry` and `billboard.seat` with `numeric`, check constraints, the exact unique/composite keys from
ADR-0046, `content_hash ~ '^[0-9a-f]{64}$'`, and the four specified read indexes. Enable RLS and grant only the
minimum select/insert/update/delete required by `market_data_worker`; delete is restricted to child-seat replacement.

- [ ] **Step 5: Implement the three JSON RPCs**

Each function is `stable`, `security definer`, fixes `search_path`, sets `statement_timeout='5s'`, validates all
inputs before selecting, and returns Decimal/numeric values as JSON strings. Summary queries aggregate `buy_seats`
and `sell_seats` ordered by rank. Seat query returns flat seat items joined to parent reason and totals. Revoke all
from `public`, `anon`, and `authenticated`; grant execute only when `market_data_api` exists.

- [ ] **Step 6: Run migration/integration tests GREEN**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k trading_billboard -q`

Run: `uv run pytest tests/test_production_checks.py -q`

Expected: all pass against the disposable database.

- [ ] **Step 7: Commit the database contract**

```powershell
git add supabase/migrations/20260824000200_create_trading_billboard.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: persist and query trading billboard facts"
```

---

### Task 4: Atomic Persistence and Collection Service

**Files:**

- Create: `src/market_data_center/persistence/trading_billboard_postgres.py`
- Create: `src/market_data_center/trading_billboard_service.py`
- Modify: `src/market_data_center/reliability.py`
- Test: `tests/test_trading_billboard_service.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_raw_store.py`

**Interfaces:**

- Consumes: Task 1 records, Task 2 Provider/normalizer and Task 3 tables.
- Produces: `TradingBillboardCollectionSummary`, `TradingBillboardService.collect(trade_date)` and
  `TradingBillboardService.backfill(start_date, end_date)`.

- [ ] **Step 1: Write service tests with fake ports**

Define fakes for Provider, Raw store and Persistence. Assert `collect()` checks the trading calendar, allocates one
ingestion ID, writes Raw before forcing lazy normalization, validates known stocks, and commits exactly once only on
success. Assert Provider error, Raw error and hard validation failure produce a failed run without fact rows.

- [ ] **Step 2: Write idempotency and revision integration tests**

Assert first write inserts one entry and ten seats; identical content keeps the entry ID and fact timestamps/content;
changed content keeps the entry ID, updates ingestion/hash, and atomically replaces seats. Inject a seat insert error
and assert the entry revision also rolls back.

- [ ] **Step 3: Run focused tests RED**

Run: `uv run pytest tests/test_trading_billboard_service.py tests/test_raw_store.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k trading_billboard_persistence -q`

Expected: failures because service and persistence do not exist.

- [ ] **Step 4: Implement the Persistence port**

Provide exact methods:

```python
class PostgreSQLTradingBillboardPersistence:
    def is_trading_day(self, trade_date: date) -> bool: ...
    def known_stock_symbols(self, trade_date: date) -> frozenset[str]: ...
    def commit_success(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
        records: Sequence[TradingBillboardRecord],
    ) -> TradingBillboardCollectionSummary: ...
    def commit_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality: Sequence[QualityResult],
    ) -> None: ...
```

Use one `engine.begin()` transaction. Match existing entries by natural key, compare content hash, and replace child
seats only when content changes. Never delete an entry to apply a revision.

- [ ] **Step 5: Implement exact-day and bounded-range service methods**

`collect()` writes Raw to the approved partition path, builds IngestionRun/RawManifest/QualityResult, and rejects the
whole date on any hard finding. `backfill()` validates `start_date <= end_date` and range `<= 366` days, iterates
calendar dates, skips known non-trading dates, commits each trading date independently and stops at the first failure
while returning completed dates and the failed date.

- [ ] **Step 6: Add v1 Raw replay dispatch**

Extend recovery only for `DatasetCode.TRADING_BILLBOARD` and schema
`eastmoney.trading_billboard.v1`. Replay uses the stored request date and dedicated normalizer, creates a new
IngestionRun referencing `replayed_from_raw_id`, and never constructs an HTTP Provider.

- [ ] **Step 7: Run service, replay and integration tests GREEN**

Run: `uv run pytest tests/test_trading_billboard_service.py tests/test_raw_store.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k trading_billboard_persistence -q`

Expected: all pass.

- [ ] **Step 8: Commit collection and persistence**

```powershell
git add src/market_data_center/persistence/trading_billboard_postgres.py src/market_data_center/trading_billboard_service.py src/market_data_center/reliability.py tests/test_trading_billboard_service.py tests/test_raw_store.py tests/test_postgres_integration.py
git commit -m "feat: collect trading billboard aggregates"
```

---

### Task 5: Explicit CLI and Worker APScheduler Catalog

**Files:**

- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `.env.example`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_cli.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_worker_admin.py`

**Interfaces:**

- Consumes: Task 4 service.
- Produces: `trading-billboard-collect` CLI and `trading-billboard-daily` Worker job.

- [ ] **Step 1: Write parser and command tests RED**

Specify one command with mutually exclusive modes:

```text
market-data-center trading-billboard-collect --trade-date YYYY-MM-DD --confirm-eastmoney-source-terms-reviewed
market-data-center trading-billboard-collect --start-date YYYY-MM-DD --end-date YYYY-MM-DD --confirm-eastmoney-source-terms-reviewed
```

Assert exact date cannot combine with a range, both range endpoints are required, a range over 366 days fails before
network/database mutation, and the confirmation flag is mandatory.

- [ ] **Step 2: Write scheduler/catalog tests RED**

Assert workflow code `trading_billboard_daily`, step `collect_trading_billboard`, job ID
`trading-billboard-daily`, Monday-Friday 20:30 Asia/Shanghai, default disabled, one Worker registration and local
admin-page visibility. Assert disabling the setting omits the APScheduler job without removing the catalog entry.

- [ ] **Step 3: Run CLI/scheduler tests RED**

Run: `uv run pytest tests/test_settings.py tests/test_cli.py tests/test_scheduler.py tests/test_worker_admin.py -q`

Expected: failures because command, setting and job do not exist.

- [ ] **Step 4: Implement opt-in setting and catalog entries**

Add:

```python
trading_billboard_enabled: bool = False
```

Add `TRADING_BILLBOARD_JOB_ID`, WorkflowDefinition and JobDefinition with code-owned 20:30 time. Do not add hour or
minute settings.

- [ ] **Step 5: Implement manual and scheduled execution**

Both paths create a `WorkflowExecutionService` run using `WorkflowCode.TRADING_BILLBOARD_DAILY`, invoke step
`collect_trading_billboard`, mark failures before re-raising, and dispose the engine. The scheduled path derives the
intended Shanghai fire date and calls exact-day collection. Manual range mode calls `backfill()` once and prints a
JSON summary without provider exception text.

- [ ] **Step 6: Run CLI/scheduler tests GREEN**

Run: `uv run pytest tests/test_settings.py tests/test_cli.py tests/test_scheduler.py tests/test_worker_admin.py -q`

Expected: all pass; no OS scheduling artifact is created.

- [ ] **Step 7: Commit Worker integration**

```powershell
git add src/market_data_center/settings.py src/market_data_center/cli.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py .env.example tests/test_settings.py tests/test_cli.py tests/test_scheduler.py tests/test_worker_admin.py
git commit -m "feat: schedule trading billboard collection"
```

---

### Task 6: FastAPI Models, Query Service and Routes

**Files:**

- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/public_api/openapi_zh.py`
- Modify: `tests/test_public_api.py`

**Interfaces:**

- Consumes: Task 3 RPC payloads.
- Produces: `TradingBillboardPageResponse`, `TradingBillboardSeatPageResponse` and three authenticated HTTP routes.

- [ ] **Step 1: Write response-model and route tests RED**

Add FakeQueryService methods and test:

```text
GET /api/v1/trading-billboard/by-date?trade_date=2026-08-17&limit=100&offset=0
GET /api/v1/trading-billboard/by-symbol/600000?start_date=2026-01-01&end_date=2026-08-17
GET /api/v1/trading-billboard/seats?seat_name=机构专用&start_date=2026-01-01&end_date=2026-08-17&side=buy
```

Assert API-key protection, Decimal strings, nested seats on summary pages, flat parent-enriched seat occurrences,
empty items, mutual-exclusion validation, exact six-digit stock code, range/limit/offset bounds and stable 422/503
safe errors.

- [ ] **Step 2: Run public API tests RED**

Run: `uv run pytest tests/test_public_api.py -k trading_billboard -q`

Expected: routes and Pydantic types are absent.

- [ ] **Step 3: Implement exact Pydantic response shapes**

Define seat and entry item models with the field names from the domain design, using Decimal fields so JSON emits the
project's existing Decimal-string representation. Page models include:

```python
returned_count: int
total_count: int
has_more: bool
limit: int
offset: int
items: tuple[TradingBillboardEntryItem, ...]  # or seat occurrences
```

- [ ] **Step 4: Implement PublicQueryService methods**

Add three SQLAlchemy statements selecting one JSON `payload` from the corresponding RPC, protocol signatures and
PostgreSQL implementations. Pass a transaction-local 5,000 ms statement timeout and validate the payload through the
new Pydantic models. For the six-digit HTTP stock code, first call the existing bounded `api_v1.query_securities`
contract with the exact code, require exactly one stock result, and pass its standard symbol to the billboard RPC;
return not-found or ambiguity without reading `core` directly. Never query `billboard` directly.

- [ ] **Step 5: Implement routes and Chinese OpenAPI annotations**

Add the three route paths tested in Step 1. For seat query, accept optional `seat_code`, `seat_name` and `side`, reject
both/neither before database access, strip whitespace and preserve exact nonblank value. Convert the six-digit path
code through the bounded Security RPC resolution described in Step 4.

- [ ] **Step 6: Run public API tests GREEN**

Run: `uv run pytest tests/test_public_api.py -k trading_billboard -q`

Run: `uv run mypy src/market_data_center/public_api`

Expected: all pass.

- [ ] **Step 7: Commit the external read API**

```powershell
git add src/market_data_center/public_api tests/test_public_api.py
git commit -m "feat(api): query trading billboard facts"
```

---

### Task 7: Checked-In Contracts and Operational Documentation

**Files:**

- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `tests/test_api_contracts.py`
- Modify: `scripts/check_fastapi_release.py`
- Modify: `deploy/linux/market-data-center.env.example`
- Modify: `README.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/数据库导航.md`

**Interfaces:**

- Consumes: Tasks 3, 5 and 6 stable database/HTTP behavior.
- Produces: synchronized consumer and operator documentation.

- [ ] **Step 1: Extend contract tests RED**

Add the three RPC names to the exact expected PostgREST/Agent set and assert FastAPI contains the three route paths,
query bounds, seat side enum, mutual-exclusion description and response schemas. Assert no internal schema, Raw path,
secret or source exception field appears.

- [ ] **Step 2: Run contract tests RED**

Run: `uv run pytest tests/test_api_contracts.py -q`

Expected: checked-in contracts are missing the new functions/routes.

- [ ] **Step 3: Update PostgREST and Agent contracts**

Add exact request schemas and response-envelope schemas for all three RPCs. Keep endpoint sets identical and expose
provider-neutral names only. Do not expose internal `entry_id` as a caller input.

- [ ] **Step 4: Regenerate FastAPI OpenAPI**

Run: `uv run python scripts/export_fastapi_openapi.py`

Review the diff to ensure only intended routes/schemas and deterministic ordering changed.

- [ ] **Step 5: Update operator and consumer docs**

Document the opt-in `TRADING_BILLBOARD_ENABLED=false` default, 20:30 Worker schedule, explicit rights-confirmed CLI,
exact query paths, 366/500/10,000 bounds, exact seat-name semantics, Raw path/schema, no date fallback and no live
API fallback. State that production must remain disabled until rights review is recorded; do not document it as live.

- [ ] **Step 6: Run contracts and release checks GREEN**

Run: `uv run pytest tests/test_api_contracts.py tests/test_production_checks.py -q`

Run: `uv run python scripts/check_fastapi_release.py`

Expected: all pass.

- [ ] **Step 7: Commit contracts and docs**

```powershell
git add contracts tests/test_api_contracts.py scripts/check_fastapi_release.py deploy/linux/market-data-center.env.example README.md docs/Worker日常采集与调度.md docs/FastAPI外部接口.md docs/数据库导航.md
git commit -m "docs: publish trading billboard contracts"
```

---

### Task 8: Complete Verification and Handoff

**Files:**

- Verify all files changed by Tasks 1–7.
- Do not edit unrelated dirty files.

**Interfaces:**

- Consumes: complete Issue #65 implementation.
- Produces: evidence-backed handoff; it does not enable production collection.

- [ ] **Step 1: Run focused feature tests**

```powershell
uv run pytest tests/test_trading_billboard.py tests/test_eastmoney_trading_billboard_provider.py tests/test_trading_billboard_service.py tests/test_cli.py tests/test_scheduler.py tests/test_public_api.py tests/test_api_contracts.py -q
```

Expected: all pass.

- [ ] **Step 2: Run the complete local gate**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all commands exit zero.

- [ ] **Step 3: Run isolated PostgreSQL integration tests**

Verify `TEST_DATABASE_URL` names an isolated disposable database and is not the production URL, then run:

```powershell
uv run pytest -m integration
```

Expected: all integration tests pass.

- [ ] **Step 4: Inspect final scope and safety**

Run:

```powershell
git diff --check origin/master...HEAD
git status --short
rg -n "cron|Task Scheduler" docs README.md deploy src tests
```

Confirm there is no Raw market data, credential, database URL, OS scheduler instruction, live Eastmoney call in tests,
direct FastAPI access to `billboard`, or default-enabled production task. Preserve the unrelated shareholder-count
design modification if it is still present.

- [ ] **Step 5: Commit verification-only fixes if needed**

If verification required scoped fixes, stage only Issue #65 files and commit:

```powershell
git commit -m "fix: complete trading billboard verification"
```

If no fix was required, do not create an empty commit.

- [ ] **Step 6: Prepare the handoff**

Report focused and full command results, migration filenames, public paths, scheduler default-disabled status, and the
remaining production blocker: explicit source-rights review plus protected migration/deployment approval. Do not run
production migrations, enable the schedule, collect live data, push or create a PR unless separately authorized.
