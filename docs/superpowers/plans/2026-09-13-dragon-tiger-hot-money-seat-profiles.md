# DragonTiger Historical Repair and Hot-Money Seat Profiles Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Repair completely missing DragonTiger dates for the most recent two years with Tushare, maintain reviewed hot-money seat mappings, materialize time-safe seat profiles, and expose exact-date objective capital-quality components.

**Architecture:** Keep EastMoney as the daily source and run Tushare only through an explicit missing-date repair command; every ingestion remains single-provider with immutable Raw lineage. Add reviewed effective-dated mappings and versioned daily profiles inside the existing `billboard` boundary, then expose one bounded `api_v1` RPC through FastAPI. No component computes a subjective score.

**Tech Stack:** Python 3.12, `uv`, dataclasses, Decimal, SQLAlchemy 2, PostgreSQL ordered migrations, FastAPI/Pydantic, APScheduler, pytest, Ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-09-13-dragon-tiger-hot-money-seat-profiles-design.md`

## Global Constraints

- Governing Issue is #79 and governing ADR is ADR-0055.
- EastMoney remains the daily source; Tushare repairs only trading dates with no successful standardized DragonTiger facts.
- Never merge providers in one successful ingestion, fill a partial date from another provider, or overwrite an EastMoney fact.
- Preserve Tushare immutable Raw, manifest and ingestion lineage.
- Never resolve a stable seat or hot-money identity from a name alone; only reviewed catalog mappings participate.
- Profile samples include only `buy_amount > 0` with `sell_amount is null`, `sell_amount = 0`, or `buy_amount > sell_amount`.
- T+1/T+3/T+5 use unadjusted close-to-close return; `return_value > 0` is a win.
- Profile queries may use only a profile whose `as_of_date < event.trade_date`.
- The public query requires an exact trade date and six-digit stock code and never falls back.
- Do not add a subjective capital-quality score, hot-money rank, strategy, backtest or trading advice.
- `TUSHARE_TOKEN` is runtime-only and must not be written to Git, `.env`, Raw, PostgreSQL, contracts or logs.
- Production schema changes use only `supabase/migrations/*.sql`; FastAPI reads only bounded `api_v1` RPCs.

## File Structure

- `src/market_data_center/domain/hot_money.py`: reviewed actor/mapping value objects and effective-date validation.
- `src/market_data_center/dragon_tiger_analytics.py`: effective-buy eligibility and existing profile calculator behavior.
- `src/market_data_center/dragon_tiger_history_repair.py`: missing-date selection and resumable Tushare repair orchestration.
- `src/market_data_center/hot_money_catalog_service.py`: complete-catalog parsing, validation and atomic publication.
- `src/market_data_center/dragon_tiger_profile_service.py`: outcome preparation and one-day profile materialization.
- `src/market_data_center/persistence/hot_money_postgres.py`: catalog reads and atomic replacement.
- `src/market_data_center/persistence/dragon_tiger_profile_postgres.py`: repair targets, profile inputs and profile writes.
- `catalogs/hot_money_roster.v1.json`: reviewed catalog envelope; starts empty until reviewed actors are supplied.
- `supabase/migrations/20260913000300_add_dragon_tiger_hot_money_profiles.sql`: internal tables, RLS, workflow constraint and bounded RPC.
- Existing CLI, Worker scheduling, public API, contracts and runbooks are modified only at their current extension points.

---

### Task 1: Domain rules and effective-buy profile eligibility

**Files:**
- Create: `src/market_data_center/domain/hot_money.py`
- Modify: `src/market_data_center/domain/__init__.py`
- Modify: `src/market_data_center/dragon_tiger_analytics.py`
- Create: `tests/test_hot_money.py`
- Modify: `tests/test_dragon_tiger_analytics.py`

**Interfaces:**
- Produces: `HotMoneyActor`, `HotMoneySeatMapping`, `HotMoneyReviewStatus`, `is_effective_buy(participation: SeatParticipation) -> bool`.
- Consumes: existing `SeatParticipation` and the keyword-only `build_trading_seat_profile` API.

- [ ] **Step 1: Add failing domain tests**

```python
def test_mapping_requires_review_metadata_and_valid_range() -> None:
    with pytest.raises(ValueError, match="valid range"):
        HotMoneySeatMapping(
            actor_code="FO_SHAN",
            seat_id=SEAT_ID,
            source_alias_name="某营业部",
            valid_from=date(2026, 1, 2),
            valid_to=date(2026, 1, 1),
            evidence_note="reviewed disclosure",
            review_status=HotMoneyReviewStatus.APPROVED,
            catalog_version="v1",
        )


@pytest.mark.parametrize(
    ("buy", "sell", "expected"),
    [
        ("100", None, True),
        ("100", "0", True),
        ("100", "99", True),
        ("100", "100", False),
        ("100", "101", False),
        (None, "100", False),
    ],
)
def test_effective_buy_requires_positive_net_buy(buy, sell, expected) -> None:
    assert is_effective_buy(_participation(buy, sell)) is expected
```

- [ ] **Step 2: Run the focused tests and confirm the missing imports fail**

Run: `uv run pytest tests/test_hot_money.py tests/test_dragon_tiger_analytics.py -q`

Expected: collection fails because `domain.hot_money` and `is_effective_buy` do not exist.

- [ ] **Step 3: Implement immutable reviewed catalog records and filter profile inputs**

```python
class HotMoneyReviewStatus(StrEnum):
    APPROVED = "APPROVED"
    REJECTED = "REJECTED"
    PENDING = "PENDING"


@dataclass(frozen=True, slots=True)
class HotMoneyActor:
    actor_code: str
    canonical_name: str
    aliases: tuple[str, ...]
    is_active: bool = True


@dataclass(frozen=True, slots=True)
class HotMoneySeatMapping:
    actor_code: str
    seat_id: UUID
    source_alias_name: str
    valid_from: date | None
    valid_to: date | None
    evidence_note: str
    review_status: HotMoneyReviewStatus
    catalog_version: str


def is_effective_buy(participation: SeatParticipation) -> bool:
    return (
        participation.buy_amount is not None
        and participation.buy_amount > 0
        and (
            participation.sell_amount is None
            or participation.sell_amount == 0
            or participation.buy_amount > participation.sell_amount
        )
    )
```

Filter `participations` through `is_effective_buy` before totals, consecutive participation and outcome-event eligibility are calculated. Preserve `None` when a horizon has zero samples.

- [ ] **Step 4: Run focused tests**

Run: `uv run pytest tests/test_hot_money.py tests/test_dragon_tiger_analytics.py tests/test_dragon_tiger_features.py -q`

Expected: all selected tests pass.

- [ ] **Step 5: Commit the domain slice**

```bash
git add src/market_data_center/domain/hot_money.py src/market_data_center/domain/__init__.py src/market_data_center/dragon_tiger_analytics.py tests/test_hot_money.py tests/test_dragon_tiger_analytics.py
git commit -m "feat: define reviewed hot money mappings"
```

### Task 2: Ordered schema and PostgreSQL persistence

**Files:**
- Create: `supabase/migrations/20260913000300_add_dragon_tiger_hot_money_profiles.sql`
- Create: `src/market_data_center/persistence/hot_money_postgres.py`
- Create: `src/market_data_center/persistence/dragon_tiger_profile_postgres.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_api_contracts.py`

**Interfaces:**
- Produces: `PostgreSQLHotMoneyPersistence.replace_catalog(actors, mappings)`, `PostgreSQLDragonTigerProfilePersistence.missing_repair_dates(start_date, end_date)`, `load_profile_inputs(as_of_date)`, and `replace_profiles(profiles, calculation_id, input_watermark_date)`.
- Consumes: Task 1 actor, mapping and profile value objects.

- [ ] **Step 1: Add failing migration and persistence contract tests**

```python
def test_hot_money_schema_is_worker_only_and_effective_dated() -> None:
    sql = MIGRATION.read_text(encoding="utf-8").lower()
    assert "create table billboard.hot_money_actor" in sql
    assert "create table billboard.hot_money_seat_mapping" in sql
    assert "create table billboard.trading_seat_profile_daily" in sql
    assert "exclude using gist" in sql
    assert "enable row level security" in sql
    assert "revoke all on all tables in schema billboard from public" in sql


def test_missing_repair_dates_excludes_any_successful_fact_date(database) -> None:
    assert persistence.missing_repair_dates(START, END) == (MISSING_DATE,)
```

- [ ] **Step 2: Run focused tests and confirm failure**

Run: `uv run pytest tests/test_api_contracts.py -k hot_money -q`

Expected: failure because the ordered migration does not exist.

- [ ] **Step 3: Create the ordered migration**

Create the three tables with these natural keys and checks:

```sql
create table billboard.hot_money_actor (
  actor_id uuid primary key default extensions.gen_random_uuid(),
  actor_code text not null unique,
  canonical_name text not null,
  aliases text[] not null default '{}',
  is_active boolean not null default true,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  check (btrim(actor_code) <> '' and btrim(canonical_name) <> '')
);

create table billboard.hot_money_seat_mapping (
  mapping_id uuid primary key default extensions.gen_random_uuid(),
  actor_id uuid not null references billboard.hot_money_actor(actor_id),
  seat_id uuid not null references billboard.trading_seat(seat_id),
  valid_from date,
  valid_to date,
  source_alias_name text not null,
  evidence_note text not null,
  review_status text not null check (review_status in ('APPROVED','REJECTED','PENDING')),
  reviewed_at timestamptz,
  catalog_version text not null,
  check (valid_to is null or valid_from is null or valid_from <= valid_to),
  check (
    (review_status = 'PENDING' and reviewed_at is null)
    or (review_status in ('APPROVED','REJECTED') and reviewed_at is not null)
  )
);

create table billboard.trading_seat_profile_daily (
  seat_id uuid not null references billboard.trading_seat(seat_id),
  as_of_date date not null,
  algorithm_version text not null,
  metric_definition text not null,
  return_definition text not null,
  participation_definition text not null,
  total_lhb_count integer not null check (total_lhb_count >= 0),
  total_buy_amount numeric,
  total_sell_amount numeric,
  t1_sample_count integer not null, t1_win_rate numeric, t1_avg_return numeric,
  t3_sample_count integer not null, t3_win_rate numeric, t3_avg_return numeric,
  t5_sample_count integer not null, t5_win_rate numeric, t5_avg_return numeric,
  consecutive_participation_sample_count integer not null,
  consecutive_participation_rate numeric,
  input_watermark_date date not null,
  calculation_id uuid not null references derived.calculation_run(calculation_id),
  created_at timestamptz not null default now(),
  primary key (seat_id, as_of_date, algorithm_version)
);
```

Add a partial GiST exclusion constraint preventing overlapping `APPROVED` mappings for the same stable seat, indexes on event date/symbol and profile date, Worker-only RLS policies, grants, and the new workflow code constraint. Do not grant internal table access to the API role.

- [ ] **Step 4: Implement focused persistence adapters**

Use these exact public method signatures: `replace_catalog(actors: Sequence[HotMoneyActor], mappings: Sequence[HotMoneySeatMapping]) -> CatalogSyncSummary`, `missing_repair_dates(start_date: date, end_date: date) -> tuple[date, ...]`, `load_profile_inputs(as_of_date: date) -> ProfileInputs`, and `replace_profiles(profiles: Sequence[TradingSeatProfile], calculation_id: UUID, input_watermark_date: date) -> int`.

`missing_repair_dates` reads open `CN_A_SHARE` sessions and excludes every date having a succeeded DragonTiger ingestion with at least one standard event. `replace_catalog` validates all referenced stable seats before one transaction replaces the same catalog version. `replace_profiles` is idempotent on the profile primary key and rejects content conflicts.

- [ ] **Step 5: Run isolated PostgreSQL tests**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k 'hot_money or seat_profile or missing_repair_dates' -q`

Expected: pass only with `TEST_DATABASE_URL` pointing to an isolated disposable database; otherwise report the skip without substituting production.

- [ ] **Step 6: Commit the persistence slice**

```bash
git add supabase/migrations/20260913000300_add_dragon_tiger_hot_money_profiles.sql src/market_data_center/persistence/hot_money_postgres.py src/market_data_center/persistence/dragon_tiger_profile_postgres.py tests/test_postgres_integration.py tests/test_api_contracts.py
git commit -m "feat: persist hot money seat profiles"
```

### Task 3: Versioned reviewed catalog sync

**Files:**
- Create: `catalogs/hot_money_roster.v1.json`
- Create: `src/market_data_center/hot_money_catalog_service.py`
- Modify: `src/market_data_center/cli.py`
- Create: `tests/test_hot_money_catalog_service.py`
- Modify: `tests/test_cli.py`

**Interfaces:**
- Produces: `load_hot_money_catalog(path: Path) -> HotMoneyCatalog`, `HotMoneyCatalogService.sync(catalog) -> CatalogSyncSummary`, CLI `hot-money-catalog-sync --catalog PATH --dry-run|--execute --confirm`.
- Consumes: Task 1 domain objects and Task 2 persistence.

- [ ] **Step 1: Add failing catalog validation tests**

```python
def test_catalog_rejects_generic_and_unreviewed_mapping() -> None:
    candidate = _catalog(alias="机构专用", review_status="APPROVED")
    with pytest.raises(ValueError, match="generic seat alias"):
        validate_hot_money_catalog(candidate)


def test_catalog_dry_run_does_not_write() -> None:
    summary = service.sync(_valid_catalog(), dry_run=True)
    assert summary.validated_mapping_count == 1
    assert persistence.calls == []
```

- [ ] **Step 2: Run tests and confirm failure**

Run: `uv run pytest tests/test_hot_money_catalog_service.py tests/test_cli.py -k hot_money -q`

Expected: failure because the service and command do not exist.

- [ ] **Step 3: Implement exact catalog envelope and full-candidate validation**

```json
{"catalog_version":"v1","reviewed_at":null,"actors":[],"mappings":[]}
```

Reject duplicate actor codes, unknown actor/seat references, blank evidence, invalid dates, overlapping approved ranges, generic aliases (`机构专用`, `沪股通专用`, `深股通专用`, `北向资金专用`) and an approved mapping without `reviewed_at`. Publish only after the entire candidate validates.

- [ ] **Step 4: Add the explicit CLI command**

The command must require exactly one of `--dry-run` or `--execute`; execution additionally requires `--confirm`. Output only counts, catalog version and stable validation codes—never evidence text or database details.

- [ ] **Step 5: Run focused tests**

Run: `uv run pytest tests/test_hot_money.py tests/test_hot_money_catalog_service.py tests/test_cli.py -k 'hot_money or catalog' -q`

Expected: all selected tests pass.

- [ ] **Step 6: Commit the catalog slice**

```bash
git add catalogs/hot_money_roster.v1.json src/market_data_center/hot_money_catalog_service.py src/market_data_center/cli.py tests/test_hot_money_catalog_service.py tests/test_cli.py
git commit -m "feat: sync reviewed hot money catalog"
```

### Task 4: Tushare permission probe and missing-date history repair

**Files:**
- Modify: `src/market_data_center/providers/tushare_dragon_tiger.py`
- Create: `src/market_data_center/dragon_tiger_history_repair.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `tests/test_tushare_dragon_tiger_provider.py`
- Create: `tests/test_dragon_tiger_history_repair.py`
- Modify: `tests/test_cli.py`

**Interfaces:**
- Produces: `TushareDragonTigerAdapter.probe(trade_date) -> DragonTigerProviderProbe`, `DragonTigerHistoryRepairService.repair(start_date, end_date, dry_run=False) -> DragonTigerHistoryRepairSummary`, CLI `dragon-tiger-history-repair`.
- Consumes: existing `TushareHttpClient`, `DragonTigerService.collect`, and Task 2 `missing_repair_dates`.

- [ ] **Step 1: Add failing provider and orchestration tests**

```python
def test_repair_only_collects_dates_returned_by_missing_selector() -> None:
    result = service.repair(START, END)
    assert collector.dates == [MISSING_DATE]
    assert result.candidate_dates == 1
    assert result.succeeded_dates == 1


def test_repair_continues_after_one_date_failure() -> None:
    result = service.repair(START, END)
    assert result.failed_dates == ((FIRST_DATE, "DT_PROVIDER_FETCH_FAILED"),)
    assert collector.dates[-1] == SECOND_DATE
```

Add provider fixtures that verify all amount fields remain Decimal and normalize to CNY according to the live permission/unit probe; encode the confirmed scaling explicitly in the adapter tests before changing production normalization.

- [ ] **Step 2: Run focused tests and confirm failure**

Run: `uv run pytest tests/test_tushare_dragon_tiger_provider.py tests/test_dragon_tiger_history_repair.py tests/test_cli.py -k 'tushare or history_repair' -q`

Expected: new orchestration and CLI tests fail.

- [ ] **Step 3: Implement a secret-safe permission probe**

Call `top_list` and `top_inst` for one caller-supplied closed trading date. Return only API availability, row counts, response field presence and confirmed unit rule. Catch provider messages through the existing token-redacting client. Do not print rows or request payloads.

- [ ] **Step 4: Implement resumable missing-date repair**

```python
@dataclass(frozen=True, slots=True)
class DragonTigerHistoryRepairSummary:
    candidate_dates: int
    succeeded_dates: int
    failed_dates: tuple[tuple[date, str], ...]
    event_count: int
    seat_trade_count: int


class DragonTigerHistoryRepairService:
    def repair(
        self, start_date: date, end_date: date, *, dry_run: bool = False
    ) -> DragonTigerHistoryRepairSummary:
        candidates = self._persistence.missing_repair_dates(start_date, end_date)
        if dry_run:
            return DragonTigerHistoryRepairSummary(len(candidates), 0, (), 0, 0)
        return self._collect_candidates(candidates)
```

Reject ranges over 730 calendar days. In dry-run, return candidate dates without provider calls or writes. In execute mode, collect each candidate independently, retain stable error codes, and continue. Never select a date with any successful standard facts.

- [ ] **Step 5: Add the explicit CLI command**

Require `--start-date`, `--end-date`, exactly one of `--dry-run|--execute`, `--confirm-tushare-source-terms-reviewed`, and `--confirm` for execution. Construct `TushareHttpClient(TushareSettings().tushare_token.get_secret_value())` only inside the command and never serialize settings.

- [ ] **Step 6: Run focused tests**

Run: `uv run pytest tests/test_tushare_dragon_tiger_provider.py tests/test_dragon_tiger_history_repair.py tests/test_dragon_tiger_service.py tests/test_cli.py -q`

Expected: all selected tests pass.

- [ ] **Step 7: Commit the history-repair slice**

```bash
git add src/market_data_center/providers/tushare_dragon_tiger.py src/market_data_center/dragon_tiger_history_repair.py src/market_data_center/cli.py tests/test_tushare_dragon_tiger_provider.py tests/test_dragon_tiger_history_repair.py tests/test_cli.py
git commit -m "feat: repair missing dragon tiger dates with tushare"
```

### Task 5: Daily time-safe seat profile materialization

**Files:**
- Create: `src/market_data_center/dragon_tiger_profile_service.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Create: `tests/test_dragon_tiger_profile_service.py`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_settings.py`

**Interfaces:**
- Produces: `DragonTigerProfileService.materialize(as_of_date) -> DragonTigerProfileSummary`, workflow `dragon_tiger_seat_profile`, job `dragon-tiger-seat-profile-daily`.
- Consumes: existing pure `build_trading_seat_profile` and Task 2 profile persistence.

- [ ] **Step 1: Add failing time-safety and scheduler tests**

```python
def test_materialize_excludes_outcomes_available_after_as_of_date() -> None:
    service.materialize(date(2026, 9, 11))
    assert persistence.written[0].t5_sample_count == 0


def test_profile_job_runs_after_daily_bar_and_dragon_tiger_jobs() -> None:
    job = job_definition("dragon-tiger-seat-profile-daily", settings)
    assert (job.hour, job.minute) == (21, 0)
```

- [ ] **Step 2: Run tests and confirm failure**

Run: `uv run pytest tests/test_dragon_tiger_profile_service.py tests/test_scheduler.py tests/test_settings.py -q`

Expected: failure because the service, workflow and job are absent.

- [ ] **Step 3: Implement profile orchestration**

```python
PROFILE_ALGORITHM_VERSION = "trading-seat-profile-v1"


@dataclass(frozen=True, slots=True)
class DragonTigerProfileSummary:
    as_of_date: date
    eligible_seat_count: int
    profile_count: int
    skipped_outcome_count: int


class DragonTigerProfileService:
    def materialize(self, as_of_date: date) -> DragonTigerProfileSummary:
        inputs = self._persistence.load_profile_inputs(as_of_date)
        profiles = self._build_profiles(inputs, as_of_date)
        self._persistence.replace_profiles(
            profiles, inputs.calculation_id, inputs.input_watermark_date
        )
        return DragonTigerProfileSummary(
            as_of_date, len(inputs.seat_ids), len(profiles), inputs.skipped_outcome_count
        )
```

Load only stable seats with effective-buy observations, prepare outcomes from exact T+1/T+3/T+5 market sessions and unadjusted closes, discard unavailable horizons, call the pure profile calculator, then atomically replace the same date/version profiles under one calculation ID.

- [ ] **Step 4: Register the code-owned Worker job**

Add `dragon_tiger_seat_profile_enabled: bool = False`. Schedule weekdays at 21:00, require the same date's `daily_market` and terminal `dragon_tiger_daily` workflows, and skip verified non-trading dates. Record only counts and stable failure summaries.

- [ ] **Step 5: Run focused tests**

Run: `uv run pytest tests/test_dragon_tiger_profile_service.py tests/test_dragon_tiger_analytics.py tests/test_scheduler.py tests/test_settings.py tests/test_worker_admin.py -q`

Expected: all selected tests pass and the job appears once in the controlled catalog.

- [ ] **Step 6: Commit the profile slice**

```bash
git add src/market_data_center/dragon_tiger_profile_service.py src/market_data_center/domain/operations.py src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py tests/test_dragon_tiger_profile_service.py tests/test_scheduler.py tests/test_settings.py
git commit -m "feat: materialize dragon tiger seat profiles"
```

### Task 6: Exact-date objective capital components API

**Files:**
- Modify: `supabase/migrations/20260913000300_add_dragon_tiger_hot_money_profiles.sql`
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/public_api/openapi_zh.py`
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_api_contracts.py`

**Interfaces:**
- Produces: `api_v1.query_dragon_tiger_capital_components(date,text) -> jsonb`, `GET /api/v1/dragon-tiger/stocks/{code}/capital-components?trade_date=YYYY-MM-DD`, `DragonTigerCapitalComponentsResponse`.
- Consumes: existing event metrics plus approved effective mappings and profiles strictly earlier than the event date.

- [ ] **Step 1: Add failing API and contract tests**

```python
def test_capital_components_requires_exact_date_and_six_digit_code(client) -> None:
    assert client.get("/api/v1/dragon-tiger/stocks/600000/capital-components").status_code == 422
    assert (
        client.get(
            "/api/v1/dragon-tiger/stocks/60000/capital-components",
            params={"trade_date": "2026-09-11"},
        ).status_code
        == 422
    )


def test_capital_components_does_not_fallback(fake_service, client) -> None:
    response = client.get(
        "/api/v1/dragon-tiger/stocks/600000/capital-components", params={"trade_date": "2026-09-11"}
    )
    assert response.status_code == 404
```

- [ ] **Step 2: Run focused tests and confirm failure**

Run: `uv run pytest tests/test_public_api.py tests/test_api_contracts.py -k capital_components -q`

Expected: 404 route-not-found or missing contract assertions.

- [ ] **Step 3: Add the bounded SQL RPC**

The RPC resolves one stock symbol by exact six-digit code, selects only exact-date events, computes existing objective event metrics, joins only `APPROVED` mappings effective on the event date, and joins each seat's latest profile satisfying `profile.as_of_date < event.trade_date`. Set `statement_timeout='5s'`, return at most the event's bounded ten seat rows, revoke public/internal access and grant execute only to `market_data_api`.

- [ ] **Step 4: Add typed FastAPI models and query service method**

```python
class DragonTigerSeatProfileItem(ApiModel):
    seat_id: UUID
    as_of_date: date
    algorithm_version: str
    t1_sample_count: int
    t1_win_rate: Decimal | None
    t1_avg_return: Decimal | None
    t3_sample_count: int
    t3_win_rate: Decimal | None
    t3_avg_return: Decimal | None
    t5_sample_count: int
    t5_win_rate: Decimal | None
    t5_avg_return: Decimal | None


class DragonTigerCapitalComponentsResponse(ApiModel):
    trade_date: date
    symbol: str
    code: str
    events: list[DragonTigerCapitalComponentEvent]
```

Add `PublicQueryService.dragon_tiger_capital_components(code: str, trade_date: date)` and execute only the RPC with a 5-second timeout. A null payload raises `PublicQueryNotFound`.

- [ ] **Step 5: Add the Chinese-documented route and synchronize contracts**

Use the existing API-key dependency, `STOCK_CODE_PATTERN`, error handlers and OpenAPI export script. Update all three checked-in contracts in the same commit and assert that no internal schema or secret field appears.

- [ ] **Step 6: Run focused API and integration tests**

Run: `uv run pytest tests/test_public_api.py tests/test_api_contracts.py -k 'dragon_tiger and (capital or contract)' -q`

Run with isolated PostgreSQL: `uv run pytest -m integration tests/test_postgres_integration.py -k dragon_tiger_capital_components -q`

Expected: exact-date results validate; missing dates return not found; future profiles are absent.

- [ ] **Step 7: Commit the public contract slice**

```bash
git add supabase/migrations/20260913000300_add_dragon_tiger_hot_money_profiles.sql src/market_data_center/public_api/models.py src/market_data_center/public_api/queries.py src/market_data_center/public_api/app.py src/market_data_center/public_api/openapi_zh.py contracts/postgrest-openapi-v1.json contracts/fastapi-openapi-v1.json contracts/agent-tools-v1.json tests/test_postgres_integration.py tests/test_public_api.py tests/test_api_contracts.py
git commit -m "feat: expose dragon tiger capital components"
```

### Task 7: Runbooks, complete verification and production execution gate

**Files:**
- Modify: `README.md`
- Modify: `.env.example`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/数据库导航.md`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Produces: documented secret-safe dry-run, catalog sync, migration, deploy, one-date probe, two-year repair, profile enablement and coverage verification sequence.
- Consumes: all earlier tasks.

- [ ] **Step 1: Add failing production-documentation checks**

```python
def test_runbook_orders_dragon_tiger_profile_release_safely() -> None:
    text = RUNBOOK.read_text(encoding="utf-8")
    assert text.index("只读预检") < text.index("单日权限探测")
    assert text.index("单日权限探测") < text.index("两年缺失日期回填")
    assert "TUSHARE_TOKEN=" not in text
```

- [ ] **Step 2: Update operational documentation**

Document the new opt-in profile switch without a token value. Show commands using an already-injected environment variable, require dry-run before execute, and state that the supplied temporary token must be rotated after the one-off repair because it was shared in conversation.

- [ ] **Step 3: Run focused documentation and production checks**

Run: `uv run pytest tests/test_production_checks.py tests/test_api_contracts.py tests/test_scheduler.py -q`

Expected: all selected checks pass and no credential literal is present.

- [ ] **Step 4: Run the complete local gate**

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: every command exits zero. Run `uv run pytest -m integration` only with an isolated disposable `TEST_DATABASE_URL`; never substitute production.

- [ ] **Step 5: Verify secret absence and review the final diff**

Run: `git grep -n -E 'b1a6e909|TUSHARE_TOKEN=.+|postgresql[^ ]*://' -- ':!uv.lock'`

Expected: no supplied token prefix, populated token, or database URL in tracked files. Review `git diff --check` and the complete branch diff.

- [ ] **Step 6: Commit documentation**

```bash
git add README.md .env.example docs/Worker日常采集与调度.md docs/Worker调度系统.md docs/数据库导航.md docs/FastAPI外部接口.md docs/最小生产发布运行手册.md tests/test_production_checks.py
git commit -m "docs: operate dragon tiger seat profiles"
```

- [ ] **Step 7: Stop at the production mutation gate**

Before migration, deployment or backfill, report the exact commit, local gate results, pending migration name, selected date range and dry-run candidate count. Production execution requires the user's explicit deployment confirmation in that turn. Use the protected migration workflow, run one closed-date permission probe and one-date smoke repair, then perform the resumable two-year missing-date repair and report coverage without exposing credentials.
