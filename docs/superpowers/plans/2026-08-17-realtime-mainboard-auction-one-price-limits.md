# Realtime Mainboard Auction One-Price Limits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `GET /api/v1/call-auction-one-price-limits` calculate SSE/SZSE mainboard 09:26 one-price limit-up/down lists at read time without requiring the nightly `derived.daily_price_limit` batch.

**Architecture:** Replace the bounded `api_v1.query_auction_one_price_limits(date)` body through one ordered migration. The stable security-definer RPC selects one exact 09:26 ingestion, proves mainboard/listing eligibility from stored facts, calculates numeric 10% limits, and returns the existing envelope plus explicit realtime rule lineage; FastAPI remains a read-only proxy.

**Tech Stack:** Python 3.12, FastAPI, Pydantic v2, PostgreSQL 15+ PL/pgSQL/JSONB, SQLAlchemy 2, pytest, uv.

**Spec:** `docs/superpowers/specs/2026-08-17-realtime-mainboard-auction-one-price-limits-design.md`

## Global Constraints

- Scope is SSE/SZSE mainboard stock only; ChiNext, STAR, BSE, funds and bonds remain excluded.
- Ordinary and ST mainboard stocks both use ratio `0.10`, tick `0.01`, rule `CN_MAINBOARD_2026_07_06`, algorithm `1.0.0`.
- Use PostgreSQL `numeric`/Python `Decimal`; never route a price through `float`.
- Read only the selected 09:26 snapshot and facts available by that time; never use later same-day bars or network fallback.
- The endpoint performs no writes, creates no CalculationRun, triggers no ingestion and adds no scheduler job.
- Exact explicit dates never fall back; latest-date selection applies only when `trade_date` is omitted.
- RPC remains executable only by `market_data_api`, with stable security-definer search path and 5-second timeout.
- Production schema changes only through `supabase/migrations/20260817000100_realtime_auction_one_price_limits.sql`.
- Preserve unrelated user changes in the worktree and do not deploy or apply production migration without a later explicit instruction.

---

### Task 1: Accept the realtime read decision

**Files:**
- Create: `docs/adr/ADR-0039-09点26沪深主板一字涨跌停实时计算.md`
- Modify: `docs/adr/README.md`
- Create: `docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md`
- Test: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: approved spec and GitHub Issue #52.
- Produces: accepted decision and domain rules that Tasks 2–4 implement verbatim.

- [ ] **Step 1: Add a failing governance assertion**

Add a test that reads the new ADR/domain guide and asserts all controlled terms:

```python
def test_realtime_auction_one_price_limit_decision_is_documented() -> None:
    adr = (PROJECT_ROOT / "docs/adr/ADR-0039-09点26沪深主板一字涨跌停实时计算.md").read_text(
        encoding="utf-8"
    )
    detail = (
        PROJECT_ROOT / "docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md"
    ).read_text(encoding="utf-8")
    for term in (
        "CN_MAINBOARD_2026_07_06",
        "realtime_read",
        "price_limit_calculation_id",
        "market_data_api",
        "09:26:00",
    ):
        assert term in adr + detail
```

- [ ] **Step 2: Run the governance test and observe the missing documents**

Run: `uv run pytest tests/test_production_checks.py::test_realtime_auction_one_price_limit_decision_is_documented -q`

Expected: FAIL because ADR-0039 and the domain guide do not exist.

- [ ] **Step 3: Write ADR-0039 and the domain guide**

ADR-0039 must be `Accepted`, track Issue #52, supersede only ADR-0032's persisted CalculationRun dependency, and retain its exact snapshot selection and strict equality rules. Document this response lineage exactly:

```text
ingestion_id=<selected 09:26 ingestion>
price_limit_calculation_id=null
price_limit_rule_version=CN_MAINBOARD_2026_07_06
price_limit_algorithm_version=1.0.0
calculation_mode=realtime_read
```

The domain guide must define mainboard code ranges, listing-day proof, prior-five-bar proof, numeric rounding, candidate/omission counts, succeeded-before-partial selection, P0002 behavior and the no-write/no-provider boundary. Add ADR-0039 to `docs/adr/README.md`.

- [ ] **Step 4: Run the focused governance test**

Run: `uv run pytest tests/test_production_checks.py::test_realtime_auction_one_price_limit_decision_is_documented -q`

Expected: PASS.

- [ ] **Step 5: Commit the accepted decision**

```powershell
git add docs/adr docs/领域详设-09点26沪深主板一字涨跌停实时计算-2026-08-17.md tests/test_production_checks.py
git commit -m "docs: accept realtime auction limit calculation"
```

### Task 2: Replace the database RPC with bounded realtime calculation

**Files:**
- Create: `supabase/migrations/20260817000100_realtime_auction_one_price_limits.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`
- Test: `tests/test_postgres_integration.py`

**Interfaces:**
- Consumes: `api_v1.query_auction_one_price_limits(p_trade_date date default null)` and the selected 09:26 ingestion contract.
- Produces: the same RPC signature returning JSONB with nullable `price_limit_calculation_id`, rule/algorithm versions and `calculation_mode='realtime_read'`.

- [ ] **Step 1: Write the primary failing PostgreSQL integration test**

Create exact-date fixtures with one succeeded `call_auction_market_snapshot` ingestion, six trading-calendar dates, five prior `core.daily_bar` facts, and these snapshot rows:

```python
cases = (
    ("SSE:600000", "600000", Decimal("10.00"), Decimal("11.00"), "up"),
    ("SZSE:000001", "000001", Decimal("10.00"), Decimal("9.00"), "down"),
    ("SSE:600001", "600001", Decimal("10.00"), Decimal("10.50"), None),
    ("SSE:688001", "688001", Decimal("10.00"), Decimal("11.00"), "excluded_board"),
)
```

Call `api_v1.query_auction_one_price_limits(:trade_date)` and assert:

```python
assert payload["price_limit_calculation_id"] is None
assert payload["price_limit_rule_version"] == "CN_MAINBOARD_2026_07_06"
assert payload["price_limit_algorithm_version"] == "1.0.0"
assert payload["calculation_mode"] == "realtime_read"
assert payload["candidate_count"] == 3
assert payload["omitted_incomplete_count"] == 0
assert [item["symbol"] for item in payload["up"]] == ["SSE:600000"]
assert [item["symbol"] for item in payload["down"]] == ["SZSE:000001"]
```

- [ ] **Step 2: Add edge-case integration assertions**

In the same integration area, test independently that:

- a partial ingestion is selected only when no succeeded ingestion exists;
- a newer partial ingestion never overrides a succeeded ingestion;
- IPO trading-day number `<= 5`, missing IPO date and fewer than five prior trading bars each increase `omitted_incomplete_count`;
- `603999`, `605000`, `004999` are included while `604000`, `001001`, `300001`, `688001` are excluded;
- `previous_close=10.05` uses PostgreSQL numeric half-up results `11.06` and `9.05`;
- complete non-limit prices do not count as omissions;
- a snapshot with no strict matches returns HTTP/RPC data with empty arrays, not P0002;
- an exact date without a 09:26 ingestion raises SQLSTATE `P0002`;
- `market_data_api` has EXECUTE but no direct SELECT on `realtime`, `core` or `derived` tables.

- [ ] **Step 3: Run the focused integration tests and verify the old dependency fails**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "auction_one_price_limits" -q`

Expected: FAIL because the current RPC requires `derived.daily_price_limit` and omits realtime lineage fields. If `TEST_DATABASE_URL` is absent, report the exact skip and continue only with unit/static checks; do not point the test at production.

- [ ] **Step 4: Add the ordered migration**

Use `create or replace function` with this CTE shape:

```sql
with prior_five_dates as materialized (
  select trade_date
  from core.trading_calendar
  where market='CN_A_SHARE' and is_trading_day and trade_date < selected_date
  order by trade_date desc limit 5
), mainboard_facts as materialized (
  select s.*, sec.code, sec.exchange, sec.ipo_date, nh.name,
    case when sec.ipo_date is null then null else (
      select count(*) from core.trading_calendar c
      where c.market='CN_A_SHARE' and c.is_trading_day
        and c.trade_date between sec.ipo_date and selected_date
    ) end as listing_trading_day_number,
    (select count(*) from core.daily_bar b
      where b.symbol=s.symbol
        and b.trade_date in (select trade_date from prior_five_dates)
        and b.trade_status in ('trading','unknown')) as prior_five_bar_count
  from realtime.call_auction_market_snapshot s
  join core.security sec on sec.symbol=s.symbol
  left join lateral (
    select name from core.security_name_history nh
    where nh.symbol=sec.symbol and nh.effective_from<=selected_date
      and (nh.effective_to is null or nh.effective_to>=selected_date)
    order by nh.effective_from desc limit 1
  ) nh on true
  where s.ingestion_id=selected_ingestion and s.trade_date=selected_date
    and sec.security_type='stock' and sec.status='listed'
    and ((sec.exchange='SSE' and
          (sec.code between '600000' and '603999' or sec.code between '605000' and '605999'))
      or (sec.exchange='SZSE' and sec.code between '000001' and '004999'
          and sec.code not between '001001' and '001199'))
), calculated as (
  select *, round(previous_close * 1.10::numeric, 2) upper_limit,
            round(previous_close * 0.90::numeric, 2) lower_limit,
    (name is not null and ipo_date is not null and listing_trading_day_number > 5
      and prior_five_bar_count=5 and previous_close>0 and last_price>0
      and high_price>0 and low_price>0) as evidence_complete
  from mainboard_facts
)
```

Build `direction` only from strict four-price equality. Set `candidate_count=count(*)` over `mainboard_facts`; set omissions only where `evidence_complete=false`. Return JSON `null` for calculation ID and literal version/mode fields. Preserve succeeded-before-partial ordering, `[09:26,09:27)` filters, `stable security definer`, controlled search path, 5-second timeout, revokes and the sole `market_data_api` grant.

- [ ] **Step 5: Register the expected migration in release checks**

Update the migration/static inventory in `tests/test_production_checks.py`. Assert the new migration contains no INSERT/UPDATE/DELETE, provider access or `derived.daily_price_limit` reference in the replaced RPC body. `scripts/apply_migrations.py` already discovers ordered SQL files and needs no change because this migration adds no table.

- [ ] **Step 6: Run the focused database and static tests**

Run:

```powershell
uv run pytest -m integration tests/test_postgres_integration.py -k "auction_one_price_limits" -q
uv run pytest tests/test_production_checks.py -q
```

Expected: all configured tests PASS; isolated PostgreSQL tests may only skip for missing `TEST_DATABASE_URL`.

- [ ] **Step 7: Commit the RPC change**

```powershell
git add supabase/migrations/20260817000100_realtime_auction_one_price_limits.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: calculate auction limits at read time"
```

### Task 3: Update FastAPI response lineage and checked-in OpenAPI

**Files:**
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `tests/test_public_api.py`
- Modify: `contracts/fastapi-openapi-v1.json`
- Test: `tests/test_public_api.py`
- Test: `tests/test_api_contracts.py`

**Interfaces:**
- Consumes: Task 2 JSON keys and unchanged RPC signature.
- Produces: `AuctionOnePriceLimitResponse` with accurate realtime lineage and an updated FastAPI OpenAPI contract.

- [ ] **Step 1: Make the public API test expect realtime lineage**

Change the fake response and endpoint assertion to:

```python
expected_lineage = {
    "price_limit_calculation_id": None,
    "price_limit_rule_version": "CN_MAINBOARD_2026_07_06",
    "price_limit_algorithm_version": "1.0.0",
    "calculation_mode": "realtime_read",
}
```

Assert the serialized JSON contains `null` and all three literal fields. Add an OpenAPI assertion that `price_limit_calculation_id` accepts null and the mode enum contains only `realtime_read`.

- [ ] **Step 2: Run the focused API tests and observe validation failure**

Run: `uv run pytest tests/test_public_api.py -k "auction_one_price_limits" -q`

Expected: FAIL because the model requires a UUID and does not define realtime lineage.

- [ ] **Step 3: Implement the response model change**

Update the model exactly:

```python
class AuctionOnePriceLimitResponse(ApiModel):
    trade_date: date
    ingestion_id: UUID
    ingestion_status: Literal["succeeded", "partial"]
    price_limit_calculation_id: UUID | None
    price_limit_rule_version: Literal["CN_MAINBOARD_2026_07_06"]
    price_limit_algorithm_version: Literal["1.0.0"]
    calculation_mode: Literal["realtime_read"]
    snapshot_window: Literal["09:26:00-09:26:59 Asia/Shanghai"]
    candidate_count: int = Field(ge=0)
    omitted_incomplete_count: int = Field(ge=0)
    up_count: int = Field(ge=0)
    down_count: int = Field(ge=0)
    up: list[AuctionOnePriceLimitItem]
    down: list[AuctionOnePriceLimitItem]
```

Update the route summary/description to say query-time stored-fact calculation and no nightly dependency.

- [ ] **Step 4: Regenerate and verify FastAPI OpenAPI**

Run:

```powershell
uv run python scripts/export_fastapi_openapi.py
uv run pytest tests/test_public_api.py -k "auction_one_price_limits" -q
uv run pytest tests/test_api_contracts.py -q
```

Expected: PASS and `contracts/fastapi-openapi-v1.json` changes only for the route description and response schema. Confirm `contracts/postgrest-openapi-v1.json` and `contracts/agent-tools-v1.json` are unchanged.

- [ ] **Step 5: Commit the FastAPI contract**

```powershell
git add src/market_data_center/public_api/models.py src/market_data_center/public_api/app.py tests/test_public_api.py tests/test_api_contracts.py contracts/fastapi-openapi-v1.json
git commit -m "feat: expose realtime auction limit lineage"
```

### Task 4: Release gate and operator documentation

**Files:**
- Modify: `scripts/check_fastapi_release.py`
- Modify: `tests/test_production_checks.py`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/数据库导航.md`
- Test: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: Task 2 RPC and Task 3 response contract.
- Produces: release preflight proof that the API role can execute the RPC without internal writes, plus accurate operator documentation.

- [ ] **Step 1: Add failing release/documentation assertions**

Assert `PUBLISHED_FUNCTIONS` contains:

```python
"api_v1.query_auction_one_price_limits(date)"
```

Assert active docs include `realtime_read`, both version literals, `price_limit_calculation_id=null`, mainboard-only scope, 10% for ordinary/ST, and the statement that a ready 09:26 snapshot is the only data dependency.

- [ ] **Step 2: Run the focused release tests and observe missing coverage**

Run: `uv run pytest tests/test_production_checks.py -q`

Expected: FAIL because the RPC is absent from `PUBLISHED_FUNCTIONS` and current docs describe a persisted calculation dependency.

- [ ] **Step 3: Update preflight and docs**

Add the exact RPC signature to `PUBLISHED_FUNCTIONS`. Update the FastAPI guide and database navigation with request/response examples, valid empty-list behavior, P0002/404 rules, nullable calculation ID and the no-write/no-provider boundary. Do not document the function as anon/authenticated PostgREST or Agent-accessible.

- [ ] **Step 4: Run focused verification**

Run:

```powershell
uv run pytest tests/test_production_checks.py tests/test_api_contracts.py tests/test_public_api.py -q
git diff --check
```

Expected: PASS with no whitespace errors.

- [ ] **Step 5: Commit release/docs changes**

```powershell
git add scripts/check_fastapi_release.py tests/test_production_checks.py docs/FastAPI外部接口.md docs/数据库导航.md
git commit -m "docs: publish realtime auction limit contract"
```

### Task 5: Complete local gate and prepare deployment handoff

**Files:**
- Modify only files already listed if verification reveals a defect.

**Interfaces:**
- Consumes: all previous tasks.
- Produces: a clean, fully verified local `master` ready for an explicitly authorized push/migration/deployment.

- [ ] **Step 1: Run the complete local gate**

Run:

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
git diff --check
```

Expected: all checks PASS. PostgreSQL integration tests may skip only when `TEST_DATABASE_URL` is absent; record the exact skipped count and reason.

- [ ] **Step 2: Verify contract and migration boundaries**

Run:

```powershell
git diff 62870c1..HEAD -- contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json
rg -n "derived\.daily_price_limit|cn_a_mainboard_price_limit_pools" supabase/migrations/20260817000100_realtime_auction_one_price_limits.sql
git status --short
```

Expected: no PostgREST/Agent contract diff, no old persisted dependency in the new function, and a clean worktree.

- [ ] **Step 3: Report the verified commit range**

Report focused/full test results, integration-test availability, migration version, response compatibility change and commits. Stop before push, production migration, release switch or service restart unless the user explicitly authorizes those operations.
