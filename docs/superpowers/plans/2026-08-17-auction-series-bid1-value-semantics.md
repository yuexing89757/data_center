# Auction Series Bid1 Value Semantics Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make every new full-market auction-series snapshot scheduled before 09:25 use bid-1 price, bid-1 share volume, and their Decimal product in the existing value fields, while explicitly distinguishing auction-indicative, opening-trade, and legacy rows.

**Architecture:** Keep the pytdx provider and immutable Raw envelope unchanged. Select task-specific values once in `call_auction_market_series_service.py`, enforce their semantics in the domain record, persist an explicit enum value, and replace the bounded RPC through one ordered migration so FastAPI consumers receive the lineage field.

**Tech Stack:** Python 3.12, dataclasses, Decimal, SQLAlchemy 2, PostgreSQL 15+, ordered SQL migrations, PL/pgSQL, FastAPI/Pydantic v2, pytest, Ruff, mypy, uv.

**Spec:** `docs/superpowers/specs/2026-08-17-auction-series-bid1-value-semantics-design.md`

## Global Constraints

- Governing work item is GitHub Issue #53; ADR-0034 must be clarified through a new Accepted ADR before behavior code lands.
- The boundary is the Round's `scheduled_at` in `Asia/Shanghai`: strictly before `09:25:00` is `auction_indicative`; `09:25:00` and later is `opening_trade`.
- Before 09:25, `last_price=bid1.price`, `cumulative_volume=bid1.volume` in shares, and `cumulative_amount=bid1.price*bid1.volume` in CNY.
- A missing bid-1 price or volume makes all three target values `None`; do not use ask-1, previous close, zero, or another level as fallback.
- Keep `high_price` and `low_price` on their existing provider mapping; do not fabricate auction highs/lows.
- Do not change the Raw schema, cadence, slots, batch size, endpoint behavior, frozen universe, scheduling, `.env`, or 09:26 task.
- Existing rows receive only `legacy_source_quote`; do not backfill their value columns from Raw.
- All price and amount arithmetic uses `Decimal`; volume remains integer shares.
- Production changes use only `supabase/migrations/20260817000200_add_auction_series_value_semantics.sql`; no ad-hoc DDL.
- Do not push, run production migrations, deploy, restart services, trigger ingestion, or edit production facts without a later explicit authorization.

---

### Task 1: Accept the governed semantic clarification

**Files:**
- Create: `docs/adr/ADR-0040-竞价序列买一价量额语义.md`
- Modify: `docs/adr/README.md`
- Create: `docs/领域详设-沪深全市场竞价序列买一价量额-2026-08-17.md`
- Test: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: Issue #53 and the approved design spec.
- Produces: accepted names `auction_indicative`, `opening_trade`, and `legacy_source_quote`, plus the exact 09:25 boundary used by all later tasks.

- [ ] **Step 1: Write the failing governance test**

Add to `tests/test_production_checks.py`:

```python
def test_auction_series_bid1_semantics_are_governed() -> None:
    adr = (PROJECT_ROOT / "docs/adr/ADR-0040-竞价序列买一价量额语义.md").read_text(encoding="utf-8")
    detail = (PROJECT_ROOT / "docs/领域详设-沪深全市场竞价序列买一价量额-2026-08-17.md").read_text(
        encoding="utf-8"
    )
    for token in (
        "scheduled_at < 09:25:00",
        "auction_indicative",
        "opening_trade",
        "legacy_source_quote",
        "bid1.price * bid1.volume",
    ):
        assert token in adr + detail
```

- [ ] **Step 2: Run the test and verify RED**

Run:

```powershell
uv run pytest tests/test_production_checks.py::test_auction_series_bid1_semantics_are_governed -q
```

Expected: FAIL because ADR-0040 and the domain detail do not exist.

- [ ] **Step 3: Write the accepted documents**

ADR-0040 must state the owner-approved reuse of `last_price/cumulative_volume/cumulative_amount`, the semantic discriminator, exact boundary, null rule, Decimal/share units, unchanged Raw, and no historical value backfill. The domain detail must assign provider adaptation to `providers/`, selection to `_series_values`, validation to `MarketSeriesSnapshotRecord`, and writing to Persistence. Add ADR-0040 to `docs/adr/README.md`.

- [ ] **Step 4: Run the focused governance test**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit the governance slice**

```powershell
git add -- docs/adr/ADR-0040-竞价序列买一价量额语义.md docs/adr/README.md docs/领域详设-沪深全市场竞价序列买一价量额-2026-08-17.md tests/test_production_checks.py
git commit -m "docs: accept auction series bid1 semantics"
```

---

### Task 2: Encode and calculate the two new-row semantics

**Files:**
- Modify: `src/market_data_center/domain/call_auction_market_series.py`
- Modify: `src/market_data_center/call_auction_market_series_service.py`
- Modify: `tests/test_call_auction_market_series.py`
- Modify: `tests/test_call_auction_market_series_service.py`

**Interfaces:**
- Consumes: `FiveLevelQuoteSnapshotRecord.bid_levels[0]`, UTC `scheduled_at`, and the three governed enum strings.
- Produces: `MarketSeriesValueSemantics(StrEnum)` and `_series_values(quote, scheduled_at) -> tuple[Decimal | None, int | None, Decimal | None, MarketSeriesValueSemantics]`.

- [ ] **Step 1: Write failing domain tests**

Extend `_snapshot` with `value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE` and add:

```python
def test_auction_indicative_snapshot_requires_consistent_bid1_values() -> None:
    snapshot = _snapshot(
        last_price=Decimal("9.99"),
        high_price=None,
        low_price=None,
        cumulative_volume=1200,
        cumulative_amount=Decimal("11988.00"),
        value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    )
    assert snapshot.cumulative_amount == Decimal("11988.00")
    with pytest.raises(ValueError, match="price multiplied by volume"):
        _snapshot(
            last_price=Decimal("9.99"),
            high_price=None,
            low_price=None,
            cumulative_volume=1200,
            cumulative_amount=Decimal("1"),
            value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
        )


def test_snapshot_semantics_follow_the_scheduled_0925_boundary() -> None:
    with pytest.raises(ValueError, match="before 09:25"):
        _snapshot(
            sample_seq=30,
            scheduled_at=SLOTS[30],
            observed_at=SLOTS[30] + timedelta(seconds=2),
            value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
        )
```

- [ ] **Step 2: Write failing service selection tests**

Import `_series_values`, build a quote whose bid-1 is `Decimal("9.99")` and 1200 shares, then assert:

```python
def test_series_values_use_bid1_before_0925_and_source_trade_at_0925() -> None:
    quote = _quote("SSE:600000", SLOTS[29])
    assert _series_values(quote, SLOTS[29]) == (
        Decimal("9.99"),
        100,
        Decimal("999.00"),
        MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    )
    assert _series_values(quote, SLOTS[30]) == (
        Decimal("10.00"),
        100,
        Decimal("1000.00"),
        MarketSeriesValueSemantics.OPENING_TRADE,
    )


def test_series_values_keep_all_bid1_values_missing_together() -> None:
    quote = replace(
        _quote("SSE:600000", SLOTS[0]),
        bid_levels=(OrderBookLevel(1, None, None),) + _quote("SSE:600000", SLOTS[0]).bid_levels[1:],
    )
    assert _series_values(quote, SLOTS[0]) == (
        None,
        None,
        None,
        MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    )
```

- [ ] **Step 3: Run the four tests and verify RED**

```powershell
uv run pytest tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py -q -k "semantics or series_values"
```

Expected: collection/import failure for the missing enum/helper or assertion failures showing old source values before 09:25.

- [ ] **Step 4: Implement the enum and domain invariants**

In `domain/call_auction_market_series.py`:

```python
class MarketSeriesValueSemantics(StrEnum):
    AUCTION_INDICATIVE = "auction_indicative"
    OPENING_TRADE = "opening_trade"
    LEGACY_SOURCE_QUOTE = "legacy_source_quote"
```

Add `value_semantics: MarketSeriesValueSemantics` to `MarketSeriesSnapshotRecord`. For new-row semantics, compare `scheduled_at.astimezone(ZoneInfo("Asia/Shanghai")).time()` with `time(9, 25)`. For `AUCTION_INDICATIVE`, require the target triple to be all `None` or all present, and if present require exact `cumulative_amount == last_price * cumulative_volume`. Permit `LEGACY_SOURCE_QUOTE` at any slot because it describes pre-migration rows only.

- [ ] **Step 5: Implement the minimal service selector**

In `call_auction_market_series_service.py`:

```python
def _series_values(
    quote: FiveLevelQuoteSnapshotRecord,
    scheduled_at: datetime,
) -> tuple[Decimal | None, int | None, Decimal | None, MarketSeriesValueSemantics]:
    shanghai_time = scheduled_at.astimezone(ZoneInfo("Asia/Shanghai")).time()
    if shanghai_time < time(9, 25):
        bid1 = quote.bid_levels[0]
        if bid1.price is None or bid1.volume is None:
            return None, None, None, MarketSeriesValueSemantics.AUCTION_INDICATIVE
        return (
            bid1.price,
            bid1.volume,
            bid1.price * bid1.volume,
            MarketSeriesValueSemantics.AUCTION_INDICATIVE,
        )
    return (
        quote.last_price,
        quote.cumulative_volume,
        quote.cumulative_amount,
        MarketSeriesValueSemantics.OPENING_TRADE,
    )
```

Call it once in `_to_snapshot`; pass its four outputs to the domain record. Do not modify `_raw_envelopes` or `PytdxHqProvider`.

- [ ] **Step 6: Run the focused domain/service tests**

Run the command from Step 3, then:

```powershell
uv run pytest tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py -q
```

Expected: both files PASS and the existing 32-round/Raw lineage assertions remain green.

- [ ] **Step 7: Commit the semantic calculation slice**

```powershell
git add -- src/market_data_center/domain/call_auction_market_series.py src/market_data_center/call_auction_market_series_service.py tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py
git commit -m "feat: map preopen series values from bid1"
```

---

### Task 3: Persist explicit semantics through one ordered migration

**Files:**
- Create: `supabase/migrations/20260817000200_add_auction_series_value_semantics.sql`
- Modify: `src/market_data_center/persistence/call_auction_market_series_postgres.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: `MarketSeriesSnapshotRecord.value_semantics.value`.
- Produces: non-null PostgreSQL `value_semantics`, historical `legacy_source_quote`, and RPC item `value_semantics`.

- [ ] **Step 1: Write failing persistence and migration inventory tests**

Update the existing persistence integration record to carry `AUCTION_INDICATIVE`, select
`last_price,cumulative_volume,cumulative_amount,value_semantics`, and assert the four stored values.
Add a static test:

```python
def test_auction_series_value_semantics_migration_is_bounded() -> None:
    sql = (
        (MIGRATION_DIR / "20260817000200_add_auction_series_value_semantics.sql")
        .read_text(encoding="utf-8")
        .lower()
    )
    assert "legacy_source_quote" in sql
    assert "auction_indicative" in sql
    assert "opening_trade" in sql
    assert "create or replace function api_v1.query_call_auction_market_series_snapshots" in sql
    assert "drop table" not in sql
```

- [ ] **Step 2: Run focused tests and verify RED**

```powershell
uv run pytest tests/test_production_checks.py -q -k auction_series_value_semantics
if (-not $env:TEST_DATABASE_URL) { throw "TEST_DATABASE_URL must target a disposable database" }
uv run pytest tests/test_postgres_integration.py -q -k market_series_persistence
```

Expected: missing migration and missing database column/value failures. Never use production as `TEST_DATABASE_URL`.

- [ ] **Step 3: Create the ordered migration**

The migration must use this shape:

```sql
alter table realtime.call_auction_market_series_snapshot
    add column value_semantics text;

update realtime.call_auction_market_series_snapshot
set value_semantics = 'legacy_source_quote'
where value_semantics is null;

alter table realtime.call_auction_market_series_snapshot
    alter column value_semantics set not null;

alter table realtime.call_auction_market_series_snapshot
    add constraint call_auction_market_series_snapshot_value_semantics_check
    check (value_semantics in (
        'auction_indicative','opening_trade','legacy_source_quote'
    ));
```

Then `create or replace` the existing RPC body from migration `20260814000800`, adding
`snapshot.value_semantics` to `matched` and the JSON item. Preserve security definer, controlled search path, 5-second timeout, 1–500 code bound, exact-date behavior, session precedence, and the existing execute grant.

- [ ] **Step 4: Write semantics in Persistence**

Add `value_semantics` to the INSERT column and value lists and to `_snapshot_parameters`:

```python
"value_semantics": value.value_semantics.value,
```

Persistence must not calculate price, volume, or amount.

- [ ] **Step 5: Add the historical-label migration test**

Using `empty_database_url`, apply migrations only through `20260817000100`, insert a valid session,
round, ingestion and snapshot with the old column list, then apply only
`20260817000200_add_auction_series_value_semantics.sql`. Assert the existing row is
`legacy_source_quote`, the column is non-null, and inserting without `value_semantics` now fails.

- [ ] **Step 6: Run focused PostgreSQL and static tests**

```powershell
uv run pytest tests/test_production_checks.py -q -k auction_series
if (-not $env:TEST_DATABASE_URL) { throw "TEST_DATABASE_URL must target a disposable database" }
uv run pytest tests/test_postgres_integration.py -q -k "market_series or auction_series_value_semantics"
```

Expected: PASS; temporary test databases are removed by fixtures.

- [ ] **Step 7: Commit the persistence slice**

```powershell
git add -- supabase/migrations/20260817000200_add_auction_series_value_semantics.sql src/market_data_center/persistence/call_auction_market_series_postgres.py tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: persist auction series value semantics"
```

---

### Task 4: Publish the semantics in the bounded FastAPI contract

**Files:**
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_api_contracts.py`
- Modify: `contracts/fastapi-openapi-v1.json`

**Interfaces:**
- Consumes: RPC item string `value_semantics`.
- Produces: required FastAPI item field `Literal["auction_indicative", "opening_trade", "legacy_source_quote"]`.

- [ ] **Step 1: Write the failing HTTP response assertion**

Add `"value_semantics": "auction_indicative"` to the fake query-service item fixture, but do not yet add it to the Pydantic model. Extend the expected JSON item in
`test_call_auction_market_series_snapshots_return_rounds_in_one_session` with the same field. Add an OpenAPI assertion that the item schema requires `value_semantics` and exposes exactly the three enum strings.

- [ ] **Step 2: Run focused API tests and verify RED**

```powershell
uv run pytest tests/test_public_api.py -q -k call_auction_market_series
```

Expected: response validation drops/rejects the unmodeled required contract or OpenAPI lacks the field.

- [ ] **Step 3: Add the Pydantic field**

Change the series item from an empty subclass to:

```python
class CallAuctionMarketSeriesSnapshotItem(CallAuctionMarketSnapshotItem):
    value_semantics: Literal[
        "auction_indicative",
        "opening_trade",
        "legacy_source_quote",
    ]
```

Do not add this field to the separate 09:26 `CallAuctionMarketSnapshotItem`.

- [ ] **Step 4: Regenerate and verify OpenAPI**

```powershell
uv run python scripts/export_fastapi_openapi.py
uv run pytest tests/test_public_api.py tests/test_api_contracts.py -q -k "call_auction_market_series or fastapi"
```

Expected: PASS and only `contracts/fastapi-openapi-v1.json` changes among checked-in contracts.

- [ ] **Step 5: Commit the API contract slice**

```powershell
git add -- src/market_data_center/public_api/models.py tests/test_public_api.py tests/test_api_contracts.py contracts/fastapi-openapi-v1.json
git commit -m "feat: expose auction series value semantics"
```

---

### Task 5: Synchronize active operational documentation and release checks

**Files:**
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/数据库导航.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `scripts/check_fastapi_release.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: migration `20260817000200` and the three response semantics.
- Produces: release preflight proof that the deployed RPC includes `value_semantics`, plus an operator live-gate query.

- [ ] **Step 1: Write failing release/documentation assertions**

Extend production checks so the release script contains
`api_v1.query_call_auction_market_series_snapshots(date,text[])` and active docs contain all three semantics plus the strict 09:25 boundary. Assert active `.env` templates still contain no schedule time or new semantics configuration.

- [ ] **Step 2: Run the focused checks and verify RED**

```powershell
uv run pytest tests/test_production_checks.py -q -k "auction_series or fastapi_release"
```

Expected: documentation/preflight assertions fail before updates.

- [ ] **Step 3: Update preflight and docs**

The FastAPI guide and database navigator must state that the reused fields are interpreted through
`value_semantics`. The production runbook live gate must query rounds 29 and 30 and verify:

```text
sample_seq=29 -> auction_indicative, last_price=bid1, amount=price*volume
sample_seq=30 -> opening_trade, source price/volume/amount
```

The preflight must verify execute permission on the existing RPC; it must not read internal tables with the API role or invoke collection.

- [ ] **Step 4: Run focused checks**

Run the command from Step 2. Expected: PASS.

- [ ] **Step 5: Commit the release/documentation slice**

```powershell
git add -- docs/FastAPI外部接口.md docs/数据库导航.md docs/最小生产发布运行手册.md scripts/check_fastapi_release.py tests/test_production_checks.py
git commit -m "docs: publish auction series value boundary"
```

---

### Task 6: Complete review and verification handoff

**Files:**
- Verify only: all files changed in Tasks 1–5

**Interfaces:**
- Consumes: committed governance, service/domain, migration/persistence, API, and release slices.
- Produces: clean local `master` ready for separately authorized push, production migration, deployment, and next-trading-day live validation.

- [ ] **Step 1: Run focused behavior tests**

```powershell
uv run pytest tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py tests/test_public_api.py tests/test_production_checks.py -q
```

Expected: PASS.

- [ ] **Step 2: Run isolated PostgreSQL coverage**

```powershell
if (-not $env:TEST_DATABASE_URL) { throw "TEST_DATABASE_URL must target a disposable database" }
uv run pytest tests/test_postgres_integration.py -q -k "market_series or auction_series_value_semantics"
Remove-Item Env:TEST_DATABASE_URL
```

Expected: PASS. If the disposable database is unavailable, report the exact command and reason; never substitute production.

- [ ] **Step 3: Run the complete local gate**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
git diff --check origin/master..HEAD
git status --short
```

Expected: all checks pass and the worktree is clean. The default full pytest may skip PostgreSQL tests after the focused isolated run.

- [ ] **Step 4: Request code review and resolve findings**

Review specifically for scheduled-time boundary correctness, Decimal multiplication, unit conversion, historical labeling, partition behavior, RPC permissions, and accidental Raw/provider changes. Any production-code correction must first receive a regression test that fails for the identified defect.

- [ ] **Step 5: Record the deployment handoff**

Report commit range, migration version `20260817000200`, focused/full test results, API compatibility, and the required next-trading-day live gate. Stop before push, production migration, deployment, restart, or ingestion until explicitly authorized.
