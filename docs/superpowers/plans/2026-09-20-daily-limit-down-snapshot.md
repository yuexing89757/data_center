# 每日跌停不可变快照实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 建设真实、可追溯的每日跌停不可变快照及 `/api/v1/daily-limit-down-list`。

**Architecture:** 独立 `today_limit_down` 领域复用现有规范日 K、跌停池、指标及收盘五档；Worker 负责采集、Raw、版本和质量，FastAPI 只调用受限 `api_v1` RPC。21:10 收盘五档任务改为涨停/跌停池去重并集，22:10 独立封存跌停快照。

**Tech Stack:** Python 3.12、uv、AKShare、SQLAlchemy、PostgreSQL、FastAPI、APScheduler、pytest。

**Spec:** `docs/superpowers/specs/2026-09-20-daily-limit-down-snapshot-design.md`

## Global Constraints

- 治理：GitHub Issue #81；实施前增加 Accepted ADR 和领域详设。
- 价格、比例和金额使用 `Decimal`/PostgreSQL `numeric`；缺失保持 `None`/`null`。
- 成员仅由精确同日未复权收盘跌停事实与 ready 跌停池决定；来源榜单只富化。
- 首次跌停封板时间缺失时为 `null`，不得推算；卖一封单额只由卖一价 × 卖一量计算。
- 公开查询仅允许有界、只读 `api_v1` RPC；不得授予 API 角色内部表权限。
- 所有生产 DDL 只通过有序 `supabase/migrations/*.sql`；本计划不授权生产迁移、任务启用、部署或历史回填。
- 保留工作区既有未提交的 `AGENTS.md`、OpenAPI、接口文档、模型及测试的时间示例修正；不要把它们混进本功能提交。

## File Map

- 治理：`docs/adr/ADR-0056-每日跌停不可变快照与只读契约.md`、`docs/领域详设-同日跌停快照-2026-09-20.md`、`docs/adr/README.md`。
- 来源与纯领域：`src/market_data_center/providers/akshare_limit_down.py`、`src/market_data_center/domain/today_limit_down.py`、`tests/test_today_limit_down.py`。
- 收盘五档：`src/market_data_center/snapshot_collector.py`、`tests/test_snapshot_collector.py`。
- 存储与迁移：`src/market_data_center/persistence/today_limit_down_postgres.py`、`supabase/migrations/20260920000100_create_today_limit_down_snapshot.sql`、`tests/test_postgres_integration.py`。
- 执行：`src/market_data_center/today_limit_down_service.py`、`src/market_data_center/scheduling_catalog.py`、`src/market_data_center/scheduler.py`、`src/market_data_center/settings.py`、`src/market_data_center/domain/ingestion.py`、`src/market_data_center/domain/operations.py`、`src/market_data_center/operations_service.py`、`src/market_data_center/cli.py`、`tests/test_today_limit_down.py`。
- 读契约：`src/market_data_center/public_api/models.py`、`src/market_data_center/public_api/queries.py`、`src/market_data_center/public_api/app.py`、`supabase/migrations/20260920000200_add_daily_limit_down_list_api.sql`、三个 `contracts/*openapi*.json`/`contracts/agent-tools-v1.json`、`docs/FastAPI外部接口.md`、`tests/test_public_api.py`、`tests/test_production_checks.py`。

---

### Task 1: 固化 Accepted ADR 与领域设计

**Files:** Create `docs/adr/ADR-0056-每日跌停不可变快照与只读契约.md`, `docs/领域详设-同日跌停快照-2026-09-20.md`; Modify `docs/adr/README.md`.

**Interfaces:** Consumes approved spec and Issue #81; produces the governing rule set for the subsequent tasks.

- [ ] **Step 1: Write the ADR and domain design.** ADR status is `Accepted`, tracks Issue #81, and records the separate domain, deterministic membership, source limitations, EOD union, statuses, exact-date RPC, 22:10 scheduling, and no production operations. Domain design defines natural keys, price/amount units, hashes, nullable enrichment, quality rules, and API bounds. Example core rule:

```text
member(symbol, trade_date) ⇔ ready down-pool member on basis_trade_date
    AND unadjusted daily_bar.close = daily_price_limit.lower_limit
closing_ask1_sealing_amount_cny = ask1_price * ask1_volume
    only when both exist and ask1_price = lower_limit
```

- [ ] **Step 2: Verify docs and commit only governance files.** Run `git diff --check` and inspect `git diff -- docs/adr docs/领域详设-同日跌停快照-2026-09-20.md`; commit with `docs: accept daily limit-down snapshot architecture`.

### Task 2: 扩大收盘五档采集范围，不改变涨停计算

**Files:** Modify `src/market_data_center/snapshot_collector.py`, `src/market_data_center/scheduling_catalog.py`, `tests/test_snapshot_collector.py`.

**Interfaces:** Produces the existing `realtime.eod_quote_snapshot` rows for the deduplicated union of `CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP` and `CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN`, selected by exact `basis_trade_date`.

- [ ] **Step 1: Add failing tests.** Insert two ready pools with an overlapping symbol; assert one fetch request containing each unique symbol exactly once, and assert the missing exact-date down pool fails rather than using an older pool. Assert existing up `seal_amount` still uses upper limit and no down `seal_amount` is fabricated.

```python
assert symbols == sorted({"SSE:600000", "SZSE:000001", "SSE:600001"})
assert up_record.seal_amount == up_record.bid1_price * up_record.bid1_volume
assert down_record.seal_amount is None
```

- [ ] **Step 2: Run** `uv run pytest tests/test_snapshot_collector.py -q`; expect the new union tests to fail.
- [ ] **Step 3: Replace the single-pool selector with a two-pool exact-date selector and keep one provider batch.** Query both pool codes with `basis_trade_date = :d`, select highest ready version per pool, reject if either missing, then sort the set union. Pass only the up-pool symbols to `_upper_limits` so the existing `seal_amount` stays strictly up-side; the down snapshot later computes ask-side amount from stored fields.

```python
POOL_CODES = (
    "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP",
    "CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_DOWN",
)
symbols = sorted({symbol for pool in selected_pools for symbol in pool.members})
```

- [ ] **Step 4: Run** `uv run pytest tests/test_snapshot_collector.py tests/test_scheduler.py -q`; expect pass. Commit collector, catalog description and tests only.

### Task 3: 跌停来源适配器与纯领域模型

**Files:** Create `src/market_data_center/providers/akshare_limit_down.py`, `src/market_data_center/domain/today_limit_down.py`, `tests/test_today_limit_down.py`; inspect `src/market_data_center/providers/akshare_limit_up.py` and `src/market_data_center/domain/today_limit_up.py`.

**Interfaces:** Produces `LimitDownSourceRecord`, `TodayLimitDownMember`, `TodayLimitDownDependencies`, `TodayLimitDownSnapshotStatus`, and `AkshareCurrentDayLimitDownProvider.fetch_limit_down_pool(trade_date) -> ProviderBatch[LimitDownSourceRecord]`.

- [ ] **Step 1: Add failing provider/domain tests.** Mock `stock_zt_pool_dtgc_em` rows containing `代码`, `名称`, `最后封板时间`, `封单资金`, `连续跌停`, `开板次数`; assert `first_limit_down_at is None`, date-local last time, Decimal funds, retained Raw rows and normalization from Raw replay, invalid/negative values rejected, and `close != limit_price` rejected.

```python
record = batch.records[0]
assert record.symbol == "SZSE:000001"
assert record.first_limit_down_at is None
assert record.source_reported_sealed_funds_cny == Decimal("1200000")
```

- [ ] **Step 2: Run** `uv run pytest tests/test_today_limit_down.py -q`; expect import/test failures.
- [ ] **Step 3: Implement source boundary.** Use a bounded, locked AKShare request for `stock_zt_pool_dtgc_em(date=YYYYMMDD)`, with configured timeout/attempts and `ProviderBatch` deferred normalization. Map only source facts; parse HHMMSS as Asia/Shanghai, reject unknown symbols and malformed values, keep first time `None`. Define domain validation using `Decimal` and exact `close == limit_price`; ask-1 amount requires ask-1 price at the lower limit.

```python
if ask1_price == limit_price and ask1_volume is not None:
    closing_ask1_sealing_amount_cny = ask1_price * ask1_volume
else:
    closing_ask1_sealing_amount_cny = None
```

- [ ] **Step 4: Run** `uv run pytest tests/test_today_limit_down.py -q`; expect pass. Commit only provider/domain/tests.

### Task 4: 有序迁移与不可变存储

**Files:** Create `supabase/migrations/20260920000100_create_today_limit_down_snapshot.sql`, `src/market_data_center/persistence/today_limit_down_postgres.py`; Modify `tests/test_postgres_integration.py`, `tests/test_production_checks.py`.

**Interfaces:** Produces Worker-only tables `today_limit_down.source_observation`, `.snapshot`, `.member`, `.calculation_quality`; `PostgreSQLTodayLimitDownPersistence` methods `dependencies`, `create_ingestion_run`, `fail_ingestion_run`, `commit_deferred`, `commit_failed`, `commit_snapshot` and immutable `TodayLimitDownFillSummary`.

- [ ] **Step 1: Add failing integration tests.** Apply migrations to an isolated `TEST_DATABASE_URL`; assert schema/RLS/Worker insert and API no direct select, `ready`/`partial`/`deferred`/`failed` constraints, source missing keeps canonical member with null enrichment, exact-date joins, hash idempotency, new version after changed input, ask1 computed amount and lineage.

```python
assert result.status == "partial"
assert result.member_count == 1
assert member.source_observation_ingestion_id is None
assert member.closing_ask1_sealing_amount_cny == Decimal("750000")
```

- [ ] **Step 2: Run** `uv run pytest -m integration tests/test_postgres_integration.py -q`; expect failures in an isolated test DB. If `TEST_DATABASE_URL` is unavailable, record the skipped gate and still write the tests; never use production.
- [ ] **Step 3: Add ordered DDL and persistence.** Build four tables with the exact columns from the approved spec: `source_observation` has `(ingestion_id, symbol)` PK, Raw ID, source code, trade date, source name, nullable first/last times, open count, consecutive count and reported funds; `snapshot` has UUID IDs, `(trade_date, version)` and `(trade_date, input_hash)` unique keys, status/counts/hashes/versions/generated time; `member` has `(snapshot_id, symbol)` PK, canonical prices/name/float shares, five ask price/volume pairs, calculated ask1 funds and all fact/source/quote lineage; `calculation_quality` has `(snapshot_id, rule_code, symbol)` PK. Add nonnegative, pairwise, exact-close, local-date and hash constraints, indexes on date/version and member symbol, RLS, Worker `select, insert`, and no API internal-table grants. Extend existing `ingestion`, `audit`, `operations` check constraints with the new `today_limit_down_source` and `today_limit_down_snapshot` values while preserving every currently accepted value.

```sql
create schema today_limit_down;
create table today_limit_down.snapshot (
  snapshot_id uuid primary key,
  calculation_id uuid references derived.calculation_run(calculation_id),
  trade_date date not null,
  version integer not null check (version > 0),
  status text not null check (status in ('ready','partial','deferred','failed')),
  member_count integer not null check (member_count >= 0),
  candidate_count integer not null check (candidate_count >= 0),
  rejected_count integer not null check (rejected_count >= 0),
  content_hash text not null check (content_hash ~ '^[0-9a-f]{64}$'),
  input_hash text not null check (input_hash ~ '^[0-9a-f]{64}$'),
  rule_version text not null,
  algorithm_version text not null,
  source_ingestion_id uuid references ingestion.ingestion_run(ingestion_id),
  generated_at timestamptz not null,
  unique (trade_date, version),
  unique (trade_date, input_hash),
  check (member_count + rejected_count <= candidate_count),
  check ((status = 'deferred' and calculation_id is null and member_count = 0)
      or (status <> 'deferred' and calculation_id is not null))
);
```

Implement transactional joins from ready down pool, daily bar, historical name, indicator, and same-date EOD quote; calculate hashes from stable sorted canonical content. `commit_failed` writes a zero-member `failed` snapshot and quality reason, not a false-ready version.
- [ ] **Step 4: Run** focused integration tests when available and `uv run pytest tests/test_production_checks.py -q`; expect pass. Commit migration, persistence and focused tests.

### Task 5: Worker 填充服务、受控目录和显式命令

**Files:** Create `src/market_data_center/today_limit_down_service.py`; Modify `src/market_data_center/domain/ingestion.py`, `src/market_data_center/domain/operations.py`, `src/market_data_center/operations_service.py`, `src/market_data_center/settings.py`, `src/market_data_center/scheduling_catalog.py`, `src/market_data_center/scheduler.py`, `src/market_data_center/cli.py`, `tests/test_today_limit_down.py`, `tests/test_scheduler.py`.

**Interfaces:** Produces `fill_today_limit_down_snapshot(engine, raw_store, trade_date) -> TodayLimitDownFillSummary`, Worker job `today-limit-down-snapshot-daily`, and explicit CLI `market-data-center today-limit-down-snapshot --trade-date YYYY-MM-DD`.

- [ ] **Step 1: Add failing tests.** Assert catalog cron Monday–Friday 22:10 Asia/Shanghai, default disabled, exact trade-date CLI, non-trading-day skip/deferred, missing upstream prevents provider call, duplicate source symbols become partial, provider-wide error creates failed snapshot and recording.

```python
job = job_definition(TODAY_LIMIT_DOWN_SNAPSHOT_JOB_ID, SchedulerSettings(_env_file=None))
assert (job.hour, job.minute, job.enabled) == (22, 10, False)
assert decision.may_collect_source is False
```

- [ ] **Step 2: Run** `uv run pytest tests/test_today_limit_down.py tests/test_scheduler.py -q`; expect failures.
- [ ] **Step 3: Implement orchestration.** Add enum codes and one `TODAY_LIMIT_DOWN_SNAPSHOT_ENABLED` switch, fixed catalog time, scheduler callback and operations summary. Compose provider, Raw store and persistence; no provider fetch before dependencies pass, no network call from FastAPI. Run source attempts as separate request retries, not merged batches. Failed source records ingestion and immutable failed snapshot.

```python
if not decision.may_collect_source:
    return persistence.commit_deferred(trade_date, decision.reasons)
with provider_factory() as provider:
    batch = provider.fetch_limit_down_pool(trade_date)
stored = raw_store.write_jsonl(
    provider="akshare",
    dataset="today_limit_down_source",
    partition_date=trade_date,
    ingestion_id=run.ingestion_id,
    rows=batch.raw_rows,
    schema_version=batch.schema_version,
)
return persistence.commit_snapshot(
    trade_date=trade_date,
    requested_status=decision.status,
    run=completed_run,
    manifest=manifest,
    source_records=tuple(batch.records),
    ingestion_quality=quality,
)
```

- [ ] **Step 4: Run** focused tests; expect pass. Commit Worker/service/config/test files only.

### Task 6: 受限 RPC、FastAPI 模型与契约

**Files:** Create `supabase/migrations/20260920000200_add_daily_limit_down_list_api.sql`; Modify `src/market_data_center/public_api/models.py`, `src/market_data_center/public_api/queries.py`, `src/market_data_center/public_api/app.py`, `contracts/fastapi-openapi-v1.json`, `contracts/postgrest-openapi-v1.json`, `contracts/agent-tools-v1.json`, `docs/FastAPI外部接口.md`, `tests/test_public_api.py`, `tests/test_postgres_integration.py`, `tests/test_production_checks.py`.

**Interfaces:** Produces `api_v1.query_daily_limit_down_list(date, integer, integer, integer) -> jsonb`, `DailyLimitDownListResponse`, and `GET /api/v1/daily-limit-down-list`.

- [ ] **Step 1: Add failing API/SQL contract tests.** Assert required `trade_date`, version > 0, offset 0..50000, limit 1..500, no date fallback, `not_found`, sorted page, decimal strings, Shanghai timestamp formatting, nullable first time and duration, response quality and lineage, RPC execute only API role and no internal SELECT.

```python
response = client.get(
    "/api/v1/daily-limit-down-list",
    params={"trade_date": "2026-09-18", "offset": 0, "limit": 200},
    headers={"X-API-Key": "test-key"},
)
assert response.status_code == 200
assert response.json()["items"][0]["first_limit_down_at"] is None
```

- [ ] **Step 2: Run** `uv run pytest tests/test_public_api.py tests/test_production_checks.py -q`; expect new tests to fail.
- [ ] **Step 3: Implement one bounded read.** SQL picks `trade_date` exact latest/specified version, aggregates bounded quality, pages by symbol, returns members and metadata. Set `statement_timeout='5s'`, revoke `public`, grant execute only `market_data_api`. FastAPI query service calls only that RPC. Follow existing API timestamp serializer and explicit valid OpenAPI examples; do not overwrite the pre-existing local timestamp-example edits.

```sql
select s.snapshot_id from today_limit_down.snapshot s
where s.trade_date = p_trade_date
  and (p_version is null or s.version = p_version)
order by s.version desc limit 1;
```

- [ ] **Step 4: Regenerate/synchronize checked-in contracts and docs; run** `uv run pytest tests/test_public_api.py tests/test_production_checks.py -q` plus isolated RPC integration tests. Expect pass. Commit only the new feature's changes; if pre-existing edits overlap files, stage individual hunks and disclose any unavoidable overlap rather than silently absorbing them.

### Task 7: Final verification and handoff

**Files:** Modify only focused docs/tests needed to close a verified defect; do not alter production environment.

**Interfaces:** Delivers a local, reviewable feature and a list of production prerequisites.

- [ ] **Step 1: Run full local gate.** Use `uv run ruff format --check .`, `uv run ruff check .`, `uv run mypy src`, `uv run pytest`; when isolated `TEST_DATABASE_URL` exists, run `uv run pytest -m integration`.
- [ ] **Step 2: Inspect generated contract diff, migration order and repository status.** Verify no secrets or Raw data are staged, no existing API changed unintentionally, and EOD up-path regression remains passing.
- [ ] **Step 3: Report exact test counts and skips, commit IDs, remaining dirty files, and the separate production sequence:** apply ordered migration through protected workflow, verify least-privilege grants, deploy Worker/API, enable the new job explicitly, then read-only smoke test. Do not perform those steps without a new user instruction.
