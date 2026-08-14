# 沪深全市场开盘竞价序列快照 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在每个沪深交易日 09:15:00–09:25:20 每 20 秒保存一次沪深上市股票全集的开盘竞价来源事实，并与现有 09:26 快照完全隔离。

**Architecture:** Worker 在 09:15 启动一个长会话，创建 32 个确定性 Round；每个 Round 使用冻结全集、单 PYTDX endpoint、最多 80 只一批，并在下个采样点前完成或显式 partial/failed。新 session/round/snapshot 表保存会话、规范 attempt 和月度分区事实；现有涨停池五档任务与新任务共用仅含两个线程的 `morning_auction` executor，其他任务继续单线程。

**Tech Stack:** Python 3.12、uv、SQLAlchemy 2、psycopg 3、PostgreSQL ordered migrations、APScheduler 3、pytdx 1.72、pytest、Ruff、mypy。

**Spec:** `docs/superpowers/specs/2026-08-14-call-auction-market-series-design.md`

## Global Constraints

- 采集窗口固定为 `09:15:00` 至 `09:25:20`（Asia/Shanghai），cadence 固定 20 秒，共 32 轮；最后一轮 deadline 为 `09:25:40`。
- 只采 `SSE/SZSE + security_type=stock + status=listed`；BSE、ETF、可转债和指数不进入全集。
- 每个 Session 冻结一次排序去重后的完整 universe；重启后的新 Workflow attempt 从当日最近 Session 复制已持久化 universe，不重新查询 Core。
- 每批最多 80 只；每个 IngestionRun 固定一个 endpoint；一轮最多两个 endpoint attempt，第二次必须从 universe 第一只全量重试，不得拼接 partial attempt。
- `.env` 只新增默认启用的 `CALL_AUCTION_MARKET_SERIES_ENABLED`，不得加入开始时间、结束时间、cadence、轮数或批量配置。
- 新数据只进入内部 `realtime` 表；不新增或修改 FastAPI、PostgREST、Agent、OpenAPI 公共契约。
- Snapshot append-only；Worker 不能 update/delete Snapshot，也不能创建或删除分区。
- PostgreSQL schema 只能通过 `supabase/migrations/*.sql`；integration tests 只能使用 disposable `TEST_DATABASE_URL`。
- Raw JSONL、Manifest、IngestionRun 和 QualityResult 长期保留；首版 Raw replay fail closed。
- 不手工补采或伪造错过的时槽；下一交易日 live gate 才能证明生产采集成功。
- 实施期间不得运行生产 migration、生产采集、部署或重启；这些外部变更需要用户另行明确授权。

---

### Task 1: 定义序列会话、轮次和来源事实领域模型

**Files:**
- Create: `src/market_data_center/domain/call_auction_market_series.py`
- Create: `tests/test_call_auction_market_series.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/domain/operations.py`

**Interfaces:**
- Produces: `MarketSeriesStatus`, `MarketSeriesSession`, `MarketSeriesRound`, `MarketSeriesSnapshotRecord`
- Produces: `series_slots(trade_date: date) -> tuple[datetime, ...]`
- Produces: `universe_hash(symbols: Sequence[str]) -> str`
- Produces: `DatasetCode.CALL_AUCTION_MARKET_SERIES`
- Produces: `WorkflowCode.CALL_AUCTION_MARKET_SERIES`

- [ ] **Step 1: 写时槽、全集和领域不变量失败测试**

创建 `tests/test_call_auction_market_series.py`，固定 UTC/上海时间并覆盖 32 轮、哈希、SSE/SZSE、计数和价格范围：

```python
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
    series_slots,
    universe_hash,
)


def test_series_slots_are_exactly_thirty_two_twenty_second_points() -> None:
    slots = series_slots(date(2026, 8, 17))
    assert len(slots) == 32
    assert slots[0] == datetime(2026, 8, 17, 1, 15, tzinfo=UTC)
    assert slots[-1] == datetime(2026, 8, 17, 1, 25, 20, tzinfo=UTC)
    assert all(right - left == timedelta(seconds=20) for left, right in zip(slots, slots[1:]))


def test_universe_hash_uses_ordered_unique_standard_symbols() -> None:
    symbols = ("SSE:600000", "SZSE:000001")
    assert universe_hash(symbols) == universe_hash(symbols)
    with pytest.raises(ValueError, match="sorted and unique"):
        universe_hash(tuple(reversed(symbols)))
    with pytest.raises(ValueError, match="SSE/SZSE"):
        universe_hash(("BSE:920000",))


def test_snapshot_rejects_wrong_slot_and_invalid_prices() -> None:
    scheduled_at = series_slots(date(2026, 8, 17))[0]
    with pytest.raises(ValueError, match="observed_at"):
        MarketSeriesSnapshotRecord(
            symbol="SSE:600000",
            trade_date=date(2026, 8, 17),
            sample_seq=0,
            scheduled_at=scheduled_at,
            observed_at=scheduled_at + timedelta(seconds=20),
            source_code="pytdx_hq",
            last_price=Decimal("10.20"),
            previous_close=Decimal("10.00"),
            high_price=Decimal("10.10"),
            low_price=Decimal("10.00"),
            cumulative_volume=100,
            cumulative_amount=Decimal("1010.00"),
        )
```

同文件再构造有效 `MarketSeriesSession` 和 `MarketSeriesRound`，断言 running Session/Round 没有 `finished_at/collected_at`，终态必须有对应结束时间，`sample_seq` 只允许 0–31，汇总计数不能超过 `expected_rounds` 或 `universe_count * expected_rounds`。

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest tests/test_call_auction_market_series.py -q`

Expected: FAIL，模块和类型尚不存在。

- [ ] **Step 3: 写最小领域实现**

在新模块固定常量并实现纯函数：

```python
SHANGHAI_ZONE = ZoneInfo("Asia/Shanghai")
SERIES_START = time(9, 15)
SERIES_CADENCE_SECONDS = 20
SERIES_ROUND_COUNT = 32
FINAL_ROUND_DEADLINE_SECONDS = 20


def series_slots(trade_date: date) -> tuple[datetime, ...]:
    start = datetime.combine(trade_date, SERIES_START, SHANGHAI_ZONE).astimezone(UTC)
    return tuple(start + timedelta(seconds=SERIES_CADENCE_SECONDS * seq) for seq in range(32))


def universe_hash(symbols: Sequence[str]) -> str:
    frozen = tuple(symbols)
    if not frozen or frozen != tuple(sorted(set(frozen))):
        raise ValueError("market series universe must be sorted and unique")
    if any(fullmatch(r"(?:SSE|SZSE):[0-9]{6}", symbol) is None for symbol in frozen):
        raise ValueError("market series universe supports SSE/SZSE stocks only")
    canonical = dumps(frozen, ensure_ascii=False, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()
```

使用 frozen/slots dataclass 定义：

```python
class MarketSeriesStatus(StrEnum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"


@dataclass(frozen=True, slots=True)
class MarketSeriesSession:
    session_id: UUID
    workflow_run_id: UUID
    trade_date: date
    window_start: datetime
    window_end: datetime
    cadence_seconds: int
    expected_rounds: int
    universe_symbols: tuple[str, ...]
    universe_count: int
    universe_hash: str
    status: MarketSeriesStatus
    started_at: datetime
    finished_at: datetime | None = None
    successful_rounds: int = 0
    partial_rounds: int = 0
    failed_rounds: int = 0
    successful_quotes: int = 0
    failed_quotes: int = 0
    error_summary: str | None = None
```

`MarketSeriesRound` 包含 `session_id/sample_seq/scheduled_at/collected_at/status/attempt_count/expected_quotes/successful_quotes/failed_quotes/selected_ingestion_id/error_summary`；Round 允许在 Provider 请求前以 running 状态落库，只有 running 可没有 `collected_at`。`MarketSeriesSnapshotRecord` 包含设计中的 13 个来源字段。所有 datetime 必须 aware 并转成 UTC 比较；`observed_at` 必须满足 `scheduled_at <= observed_at < scheduled_at + 20s`；价格和金额只接受 `Decimal | None`，volume 只接受非 bool 的 `int | None`。

枚举增加：

```python
CALL_AUCTION_MARKET_SERIES = "call_auction_market_series"
```

- [ ] **Step 4: 运行 GREEN 与领域回归**

Run: `uv run pytest tests/test_call_auction_market_series.py tests/test_realtime_quote.py -q`

Expected: PASS。Workflow 枚举与 catalog 的集合一致性在 Task 5 加入 job/workflow 后一起验证。

- [ ] **Step 5: 提交领域边界**

```bash
git add tests/test_call_auction_market_series.py src/market_data_center/domain/call_auction_market_series.py src/market_data_center/domain/ingestion.py src/market_data_center/domain/operations.py
git commit -m "feat: define call auction market series domain"
```

---

### Task 2: 新增月度分区 Schema、权限和受控代码

**Files:**
- Create: `supabase/migrations/20260814000700_create_call_auction_market_series.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `scripts/apply_migrations.py`
- Modify: `src/market_data_center/recovery.py`
- Modify: `tests/test_production_checks.py`
- Modify: `tests/test_recovery.py`

**Interfaces:**
- Produces tables: `realtime.call_auction_market_series_session`, `realtime.call_auction_market_series_round`, `realtime.call_auction_market_series_snapshot`
- Produces partitions: monthly `[2026-08-01, 2027-10-01)`
- Extends database checks for dataset/workflow code `call_auction_market_series`

- [ ] **Step 1: 写 migration 结构与权限失败测试**

在 `tests/test_postgres_integration.py` 增加 `test_call_auction_market_series_schema_is_partitioned_and_internal`，通过 `to_regclass`、`pg_partitioned_table`、`pg_inherits`、`pg_constraint`、`pg_policies` 和 `information_schema.role_table_grants` 断言：

```python
assert (
    connection.scalar(
        text(
            "select partstrat from pg_partitioned_table where partrelid="
            "'realtime.call_auction_market_series_snapshot'::regclass"
        )
    )
    == "r"
)
assert (
    connection.scalar(
        text(
            "select count(*) from pg_inherits where inhparent="
            "'realtime.call_auction_market_series_snapshot'::regclass"
        )
    )
    == 14
)
```

测试还要证明：三张父表启用 RLS；匿名、authenticated、`market_data_api` 无权限；Worker 对 Snapshot 只有 SELECT/INSERT，对 Session/Round 只有 SELECT/INSERT/UPDATE；Snapshot PK 为 `(trade_date, ingestion_id, symbol)`；Session 的 `workflow_run_id` unique；Round PK 为 `(session_id, sample_seq)`；不存在 `api_v1.call_auction_market_series*` view/function。

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market_series_schema" -q`

Expected: FAIL，三张表尚不存在。若没有 `TEST_DATABASE_URL`，记录 skip 原因并继续编写 migration，禁止改用生产库。

- [ ] **Step 3: 编写 ordered migration**

创建 Session 和 Round 表，关键列如下：

```sql
create table realtime.call_auction_market_series_session (
    session_id uuid primary key,
    workflow_run_id uuid not null unique references operations.workflow_run(workflow_run_id),
    trade_date date not null,
    window_start timestamptz not null,
    window_end timestamptz not null,
    cadence_seconds integer not null check (cadence_seconds = 20),
    expected_rounds integer not null check (expected_rounds = 32),
    universe_symbols text[] not null,
    universe_count integer not null check (universe_count > 0),
    universe_hash text not null check (universe_hash ~ '^[0-9a-f]{64}$'),
    status text not null check (status in ('running','succeeded','partial','failed')),
    started_at timestamptz not null,
    finished_at timestamptz,
    successful_rounds integer not null default 0 check (successful_rounds >= 0),
    partial_rounds integer not null default 0 check (partial_rounds >= 0),
    failed_rounds integer not null default 0 check (failed_rounds >= 0),
    successful_quotes bigint not null default 0 check (successful_quotes >= 0),
    failed_quotes bigint not null default 0 check (failed_quotes >= 0),
    error_summary varchar(500),
    check (cardinality(universe_symbols) = universe_count),
    check ((status = 'running' and finished_at is null) or
           (status <> 'running' and finished_at is not null))
);

create table realtime.call_auction_market_series_round (
    session_id uuid not null references realtime.call_auction_market_series_session(session_id),
    sample_seq integer not null check (sample_seq between 0 and 31),
    scheduled_at timestamptz not null,
    collected_at timestamptz,
    status text not null check (status in ('running','succeeded','partial','failed')),
    attempt_count integer not null check (attempt_count between 0 and 2),
    expected_quotes integer not null check (expected_quotes > 0),
    successful_quotes integer not null check (successful_quotes >= 0),
    failed_quotes integer not null check (failed_quotes >= 0),
    selected_ingestion_id uuid references ingestion.ingestion_run(ingestion_id),
    error_summary varchar(500),
    primary key (session_id, sample_seq),
    check ((status = 'running' and collected_at is null) or
           (status <> 'running' and collected_at is not null)),
    check ((status = 'running' and successful_quotes = 0 and failed_quotes = 0) or
           (status <> 'running' and successful_quotes + failed_quotes = expected_quotes))
);
```

创建分区父表和 14 个显式分区（2026-08 到 2027-09）：

```sql
create table realtime.call_auction_market_series_snapshot (
    trade_date date not null,
    ingestion_id uuid not null references ingestion.ingestion_run(ingestion_id),
    session_id uuid not null,
    sample_seq integer not null,
    scheduled_at timestamptz not null,
    symbol text not null references core.security(symbol),
    observed_at timestamptz not null,
    last_price numeric(18,4), previous_close numeric(18,4),
    high_price numeric(18,4), low_price numeric(18,4),
    cumulative_volume bigint, cumulative_amount numeric(30,4),
    source_code text not null check (source_code='pytdx_hq'),
    created_at timestamptz not null default now(),
    primary key (trade_date, ingestion_id, symbol),
    foreign key (session_id, sample_seq)
      references realtime.call_auction_market_series_round(session_id, sample_seq),
    check ((observed_at at time zone 'Asia/Shanghai')::date = trade_date),
    check (observed_at >= scheduled_at and observed_at < scheduled_at + interval '20 seconds'),
    check ((high_price is null or high_price >= 0) and
           (low_price is null or low_price >= 0) and
           (high_price is null or low_price is null or high_price >= low_price) and
           (last_price is null or low_price is null or last_price >= low_price) and
           (last_price is null or high_price is null or last_price <= high_price)),
    check ((last_price is null or last_price >= 0) and
           (previous_close is null or previous_close >= 0) and
           (cumulative_volume is null or cumulative_volume >= 0) and
           (cumulative_amount is null or cumulative_amount >= 0))
) partition by range (trade_date);
```

为父表创建 `(trade_date, sample_seq, symbol)`、`(ingestion_id, symbol)` 索引。建立 14 个分区：`call_auction_market_series_snapshot_202608`、`202609`、`202610`、`202611`、`202612`、`202701`、`202702`、`202703`、`202704`、`202705`、`202706`、`202707`、`202708`、`202709`（每项均使用完整 `call_auction_market_series_snapshot_` 前缀）；每张表的下界为名称对应月份 1 日，上界为下一月 1 日。对三张父表和所有分区启用 RLS；父表创建 Worker policy/grants。不得创建 default partition。

用当前 migration 中完整允许值重建并 validate：`ingestion.ingestion_run_dataset_check`、`audit.quality_result_dataset_check`、`operations.workflow_run_workflow_code_check`，只新增 `call_auction_market_series`，不得丢失已有 `call_auction_indicative_detail`、`today_limit_up_source` 等代码。

- [ ] **Step 4: 更新生产清单和恢复盘点**

在 `scripts/apply_migrations.py::EXPECTED_TABLES` 加入三张父表和 14 张实际分区表；在 `recovery.COUNT_QUERIES` 加入 session/round/snapshot；orphan ingestion UNION 加入 series snapshot。测试必须断言这些表出现在只读盘点中，但不新增任何公共 view/function。

- [ ] **Step 5: 运行 GREEN**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market_series_schema" -q`

Run: `uv run pytest tests/test_production_checks.py tests/test_recovery.py -q`

Expected: 两条命令均 PASS；无 disposable PostgreSQL 时第一条只能显示明确 skip。

- [ ] **Step 6: 提交 Schema**

```bash
git add supabase/migrations/20260814000700_create_call_auction_market_series.sql tests/test_postgres_integration.py scripts/apply_migrations.py src/market_data_center/recovery.py tests/test_production_checks.py tests/test_recovery.py
git commit -m "feat: add partitioned call auction series storage"
```

---

### Task 3: 实现序列专用 PostgreSQL Persistence

**Files:**
- Create: `src/market_data_center/persistence/call_auction_market_series_postgres.py`
- Modify: `tests/test_postgres_integration.py`

**Interfaces:**
- Produces: `PostgreSQLCallAuctionMarketSeriesPersistence`
- Produces: `load_recovery_universe(trade_date: date) -> tuple[str, ...] | None`
- Produces: `create_session(session: MarketSeriesSession) -> None`
- Produces: `create_ingestion_run(run: IngestionRun) -> None`
- Produces: `commit_attempt(run, records, manifest, quality_results) -> None`
- Produces: `start_round(round_state: MarketSeriesRound) -> None`
- Produces: `finish_round(round_summary: MarketSeriesRound) -> None`
- Produces: `finish_session(session_id: UUID, finished_at: datetime) -> MarketSeriesSession`
- Produces: `recover_expired_sessions(now: datetime) -> int`

- [ ] **Step 1: 写 universe 恢复、append-only 和事务失败测试**

在 PostgreSQL integration 文件中插入当日两个 workflow/session，第二个 session 的 universe 必须通过 `load_recovery_universe` 原样返回。再创建 running IngestionRun、Manifest、QualityResult、Round 和两条 Snapshot，调用：

```python
persistence.start_round(running_round)
persistence.commit_attempt(completed_run, records, manifest, quality_results)
persistence.finish_round(round_summary)
restored = persistence.finish_session(session.session_id, finished_at)
assert restored.status is MarketSeriesStatus.SUCCEEDED
assert restored.successful_rounds == 32
```

成功聚合场景必须按相同模式插入并完成 32 个不同 `sample_seq` 的 Round；不能用一轮伪造 32 轮计数。

覆盖以下独立失败：Manifest row_count 与 fetched_rows 不同；record 数与 accepted_rows 不同；snapshot 的 session/seq 与 run `request_params` 不同；duplicate `(trade_date, ingestion_id, symbol)` 导致 Manifest、QualityResult、Snapshot 和 IngestionRun 更新全部回滚；Round selected ingestion 不是本 session/seq 的终态 attempt；Snapshot UPDATE/DELETE 以 Worker 角色被拒绝。

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "market_series_persistence" -q`

Expected: FAIL，专用 Persistence 尚不存在。

- [ ] **Step 3: 实现读取和会话写入**

`load_recovery_universe` 只选精确 `trade_date` 的最近 session，并把 PostgreSQL array 转回 tuple：

```sql
select universe_symbols
from realtime.call_auction_market_series_session
where trade_date=:trade_date
order by started_at desc, session_id desc
limit 1
```

`create_session` 用 `engine.begin()` 插入完整领域字段；同一 `workflow_run_id` 冲突必须传播 IntegrityError，不能覆盖。`listed_sse_szse_stock_symbols` 使用：

```sql
select symbol from core.security
where exchange in ('SSE','SZSE') and security_type='stock' and status='listed'
order by symbol
```

- [ ] **Step 4: 实现 attempt 和 round 原子提交**

`start_round` 在请求前插入 running Round，使 Snapshot 的复合外键始终指向已存在的 Round。`commit_attempt` 先验证 run dataset、manifest/quality ingestion ID、row counts 和每条 record 的 session/seq，再在一个事务中插 Manifest、QualityResult、Snapshot 并终结 IngestionRun。入口校验固定为：

```python
def commit_attempt(
    self,
    run: IngestionRun,
    records: Sequence[MarketSeriesSnapshotRecord],
    manifest: RawManifest,
    quality_results: Sequence[QualityResult],
) -> None:
    if run.dataset_code is not DatasetCode.CALL_AUCTION_MARKET_SERIES:
        raise ValueError("unexpected market-series dataset")
    if manifest.ingestion_id != run.ingestion_id or manifest.row_count != run.fetched_rows:
        raise ValueError("market-series manifest does not match ingestion run")
    if len(records) != run.accepted_rows:
        raise ValueError("market-series facts do not match accepted rows")
```

Snapshot SQL 只用 INSERT，不使用 `on conflict`。

`finish_round` 先锁 running Round 和 Session，再验证 `scheduled_at == window_start + sample_seq * 20s`；若有 selected ingestion，SQL 校验其 dataset、session ID、sample seq 和终态；只允许一次 running→终态更新：

```sql
update realtime.call_auction_market_series_round
set collected_at=:collected_at, status=:status, attempt_count=:attempt_count,
    successful_quotes=:successful_quotes, failed_quotes=:failed_quotes,
    selected_ingestion_id=:selected_ingestion_id, error_summary=:error_summary
where session_id=:session_id and sample_seq=:sample_seq and status='running'
```

随后用终态 Round 聚合 SQL 更新 Session 计数。错过时槽通过 `start_round` 后直接 `finish_round`，使用 `attempt_count=0`、`selected_ingestion_id=null`、status failed。

- [ ] **Step 5: 实现 Session 最终状态聚合**

`finish_session` 使用 `select * from realtime.call_auction_market_series_session where session_id=:session_id for update`；没有落库的轮次计为 failed。状态规则：32 轮全部 succeeded 才 succeeded；有任何成功/partial 报价则 partial；否则 failed。更新 `finished_at/error_summary` 后重读并转换成领域对象。

`recover_expired_sessions` 只处理 `window_end < now` 的 running Session：先把其 running Round 完成为 failed，再把缺少的时槽计入 Session failed 汇总，最后按同一状态规则终结 Session，`error_summary='worker_interrupted'`。该方法不得创建 IngestionRun、调用 Provider 或补写 Snapshot。

- [ ] **Step 6: 运行 GREEN**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "market_series_persistence" -q`

Expected: PASS 或因缺少 `TEST_DATABASE_URL` 明确 skip。

- [ ] **Step 7: 提交 Persistence**

```bash
git add src/market_data_center/persistence/call_auction_market_series_postgres.py tests/test_postgres_integration.py
git commit -m "feat: persist call auction market series"
```

---

### Task 4: 用 TDD 实现 32 轮长会话采集服务

**Files:**
- Create: `src/market_data_center/call_auction_market_series_service.py`
- Create: `tests/test_call_auction_market_series_service.py`
- Modify: `tests/test_pytdx_hq_provider.py`
- Modify: `src/market_data_center/reliability.py`
- Modify: `tests/test_reliability.py`

**Interfaces:**
- Produces: `CallAuctionMarketSeriesService.collect(trade_date, workflow_run_id) -> CallAuctionMarketSeriesSummary`
- Produces: `CallAuctionMarketSeriesSummary(status, expected_rows, accepted_rows, rejected_rows, session_id)`
- Produces private helpers: `_running_round(session, sample_seq, scheduled_at)`, `_missed_round(round_state, collected_at)`, `CallAuctionMarketSeriesService._collect_round(session, round_state, deadline)`
- Consumes: Task 1 domain and Task 3 Persistence
- Consumes provider: `fetch_five_level_quotes(symbols, deadline=deadline) -> RealtimeQuoteFetch`

- [ ] **Step 1: 写完整 32 轮成功测试**

创建内存 Persistence、Raw store、脚本化 provider factory、可推进 clock/sleeper。每轮 provider 返回同一冻结全集的有效事实；断言：

```python
summary = service.collect(date(2026, 8, 17), workflow_run_id)
assert summary.status == "succeeded"
assert [item.sample_seq for item in persistence.rounds] == list(range(32))
assert all(item.attempt_count == 1 for item in persistence.rounds)
assert provider_factory.requested_symbols == [UNIVERSE] * 32
assert provider_factory.deadlines[-1] == datetime(2026, 8, 17, 1, 25, 40, tzinfo=UTC)
assert summary.accepted_rows == len(UNIVERSE) * 32
```

Raw envelope 断言包含 `session_id/sample_seq/scheduled_at/worker_observed_at/provider_raw_json`，schema version 固定为 `market_data_center.call_auction_market_series.raw.v1`。

- [ ] **Step 2: 写 endpoint、deadline、missed slot 和恢复失败测试**

增加以下场景：第一 endpoint partial、剩余预算允许时第二 endpoint 收到完整 universe；两个 attempt 的记录分别 append，Round 只选择第二 ingestion；预算不足时不创建第二 attempt；上一轮超过 deadline 后所有已过去 slot 写 failed 且不调用 provider；重启的新 Workflow attempt 从 Persistence 返回的旧 universe 创建新 Session，即使 Core universe fake 已变化也不重查。

关键断言：

```python
assert provider_factory.calls == [
    (("first.quote", 7709), UNIVERSE),
    (("second.quote", 7709), UNIVERSE),
]
assert persistence.rounds[0].selected_ingestion_id == persistence.completed_runs[1].ingestion_id
assert persistence.rounds[1].attempt_count == 0
assert persistence.rounds[1].status is MarketSeriesStatus.FAILED
```

再覆盖 duplicate/unknown/BSE/missing/normalization error、Raw 与 record 数不等、观察时点越界、无 quote endpoint、非交易日和非法 universe。任何一种都不能令 Round succeeded。

- [ ] **Step 3: 运行 RED**

Run: `uv run pytest tests/test_call_auction_market_series_service.py -q`

Expected: FAIL，服务模块尚不存在。

- [ ] **Step 4: 实现 Session 建立与确定性循环**

构造函数必须保存可注入的 `persistence/raw_store/quote_endpoints/provider_factory/retry_budget_seconds/clock/sleeper`。确定性循环骨架为：

```python
slots = series_slots(trade_date)
for sample_seq, scheduled_at in enumerate(slots):
    deadline = scheduled_at + timedelta(seconds=SERIES_CADENCE_SECONDS)
    running_round = _running_round(session, sample_seq, scheduled_at)
    self._persistence.start_round(running_round)
    now = _utc_clock_sample(self._clock)
    if now >= deadline:
        self._persistence.finish_round(_missed_round(running_round, now))
        continue
    if now < scheduled_at:
        self._sleeper((scheduled_at - now).total_seconds())
    self._collect_round(session, running_round, deadline)
```

先校验交易日和 endpoint；优先 `load_recovery_universe(trade_date)`，没有历史 session 才读取 Core；验证并持久化新 Session。遍历 `series_slots`：每轮先持久化 running Round；当前时间已达到 deadline 则立即完成为 missed failed；尚未到 scheduled_at 则 `sleeper((scheduled_at-now).total_seconds())`；不得在 scheduled_at 前请求。

- [ ] **Step 5: 实现单轮最多两个完整 attempt**

每个 attempt 创建独立 IngestionRun，`request_params` 必须包含 `trade_date/session_id/sample_seq/scheduled_at/endpoint/expected_rows`。Provider 调用始终传完整 universe 和当前 Round deadline。成功条件逐项检查：requested symbols 等于冻结全集；响应无 missing/duplicate/unknown/BSE；Raw 行数等于 normalized record 数；无 provider/normalization error；每条 observed_at 在当前 Round 内；Decimal、非负和 OHLC 领域校验通过。QualityResult rule code 前缀固定为 `call_auction_market_series.*`。

只有 raw/record cardinality 一致、无 provider/normalization error、精确覆盖全集且领域验证全通过才 succeeded。第一 attempt partial 后，只有 `clock() + timedelta(seconds=retry_budget_seconds) < deadline` 才尝试第二 endpoint；runner 传入 `PytdxHqSettings.pytdx_hq_timeout_seconds`，该值只代表既有 Provider 请求预算，不是任务时间配置。

- [ ] **Step 6: 固定 80 只批次并验证 5,208 规模**

Scheduler 构造 Provider 时会显式使用 `PytdxHqSettings(pytdx_hq_batch_size=80)`。在 provider 测试中用 5,208 个标准 symbol 调用现有 provider，并断言批次大小为 65 个 80 和最后 8：

```python
assert len(batches) == 66
assert [len(batch) for batch in batches[:-1]] == [80] * 65
assert len(batches[-1]) == 8
```

再用可推进 clock 证明 deadline 到达后不启动后续 batch，失败 symbols 明确保留。

- [ ] **Step 7: 明确禁用 Raw replay**

在 `reliability.py` 对 `DatasetCode.CALL_AUCTION_MARKET_SERIES` 抛出专用 ProviderError：

```python
CALL_AUCTION_MARKET_SERIES_REPLAY_DISABLED = (
    "Raw replay is disabled for call_auction_market_series until exact "
    "session, round, attempt, and frozen-universe lineage is implemented"
)
```

测试证明 replay 在读取/写入任何事实前 fail closed。

- [ ] **Step 8: 运行 GREEN**

Run: `uv run pytest tests/test_call_auction_market_series_service.py tests/test_pytdx_hq_provider.py tests/test_reliability.py -q`

Expected: PASS。

- [ ] **Step 9: 提交采集服务**

```bash
git add src/market_data_center/call_auction_market_series_service.py tests/test_call_auction_market_series_service.py tests/test_pytdx_hq_provider.py src/market_data_center/reliability.py tests/test_reliability.py
git commit -m "feat: collect full-market auction series"
```

---

### Task 5: 注册独立开关、Workflow 和两线程早盘 Executor

**Files:**
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `tests/test_settings.py`
- Modify: `tests/test_operations.py`
- Modify: `tests/test_scheduler.py`
- Modify: `.env.example`

**Interfaces:**
- Produces switch: `SchedulerSettings.call_auction_market_series_enabled: bool = True`
- Produces job: `CALL_AUCTION_MARKET_SERIES_JOB_ID = "call-auction-market-series"`
- Produces workflow/step: `call_auction_market_series` / `collect_call_auction_market_series`
- Produces executor: `morning_auction`, `max_workers=2`
- Produces runner: `run_call_auction_market_series_job() -> None`

- [ ] **Step 1: 写配置与 catalog 失败测试**

在 settings 测试中证明默认启用且环境变量只能启停：

```python
assert SchedulerSettings(_env_file=None).call_auction_market_series_enabled is True
monkeypatch.setenv("CALL_AUCTION_MARKET_SERIES_ENABLED", "false")
assert SchedulerSettings(_env_file=None).call_auction_market_series_enabled is False
```

在 operations 测试中断言新 job 为周一至周五 09:15、代码固定、workflow step 唯一；设置伪造的 `CALL_AUCTION_MARKET_SERIES_HOUR/MINUTE/CADENCE_SECONDS` 后 trigger 仍为 09:15，服务领域 cadence 仍为 20。

- [ ] **Step 2: 写 executor 与 runner 失败测试**

Scheduler 测试断言：

```python
series = scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID)
limit_up = scheduler.get_job(AUCTION_COLLECTION_JOB_ID)
snapshot_0926 = scheduler.get_job(CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID)
assert series.executor == "morning_auction"
assert limit_up.executor == "morning_auction"
assert snapshot_0926.executor == "default"
assert series.max_instances == limit_up.max_instances == 1
```

检查 scheduler executor 实例：`default._pool._max_workers == 1`、`morning_auction._pool._max_workers == 2`。禁用新开关只删除 series job，不影响涨停池五档和 09:26 job。

Runner 依赖替换测试断言：加载 quote-capable 节点池；用 batch size 80 创建单 endpoint Provider；创建独立 Engine/RawStore；使用 WorkflowCode 和真实 `workflow_run_id` 调用 service；异常时 workflow fail，最终始终 dispose Engine。

扩展 stale recovery runner 测试，断言现有 `recover_ingestion_runs`、`recover_workflow_runs`、`recover_auction_sessions` 之后调用 `recover_call_auction_market_series_sessions`，且该步骤只调用 Task 3 的 `recover_expired_sessions(now)`。

- [ ] **Step 3: 运行 RED**

Run: `uv run pytest tests/test_settings.py tests/test_operations.py tests/test_scheduler.py -q`

Expected: FAIL，新 switch/job/executor/runner 尚不存在。

- [ ] **Step 4: 实现受控目录和设置**

`SchedulerSettings` 只增加：

```python
call_auction_market_series_enabled: bool = True
```

catalog 固定定义：

```python
CALL_AUCTION_MARKET_SERIES_JOB_ID = "call-auction-market-series"

JobDefinition(
    CALL_AUCTION_MARKET_SERIES_JOB_ID,
    "沪深全市场开盘竞价序列快照",
    "09:15-09:25:20 每20秒保存一次沪深上市股票全集来源事实。",
    "call_auction_market_series",
    "cron",
    "周一至周五 09:15",
    SCHEDULER_TIMEZONE,
    settings.call_auction_market_series_enabled,
    JOB_TIMEOUT_SECONDS,
    "错过轮次显式失败，不补采。",
    day_of_week="mon-fri",
    hour=9,
    minute=15,
)
```

Workflow catalog 增加新 workflow 的唯一 step `collect_call_auction_market_series`，并在既有 `stale_run_recovery` workflow 末尾增加 `recover_call_auction_market_series_sessions`；确保 `WorkflowCode` 集合与 catalog 集合再次完全一致。

- [ ] **Step 5: 实现 executor 分配和 runner**

构造 Scheduler：

```python
executors = {
    "default": ThreadPoolExecutor(max_workers=1),
    "morning_auction": ThreadPoolExecutor(max_workers=2),
}
```

注册 job 时只对两个 ID传入 executor：

```python
executor = (
    "morning_auction"
    if definition.code in {AUCTION_COLLECTION_JOB_ID, CALL_AUCTION_MARKET_SERIES_JOB_ID}
    else "default"
)
scheduler.add_job(
    functions[definition.code],
    _trigger(definition),
    id=definition.code,
    replace_existing=True,
    coalesce=True,
    max_instances=1,
    misfire_grace_time=definition.timeout_seconds,
    executor=executor,
)
```

`run_call_auction_market_series_job` 按现有 09:26 runner 的 Operations 模式建立 Workflow；把现有公开属性 `execution.run.workflow_run_id` 传给 Service；新任务不复用现有任务的 Engine、Provider 或 RawStore。

在 `run_stale_recovery_job` 增加一个受控 step `recover_call_auction_market_series_sessions`，实例化专用 Persistence 并终结窗口已过的 running Session；原有 stale recovery 步骤顺序和行为保持不变。

- [ ] **Step 6: 更新 `.env.example` 并运行 GREEN**

只增加：

```dotenv
CALL_AUCTION_MARKET_SERIES_ENABLED=true
```

Run: `uv run pytest tests/test_settings.py tests/test_operations.py tests/test_scheduler.py -q`

Expected: PASS，且不存在任何 series hour/minute/cadence/batch 环境项。

- [ ] **Step 7: 提交 Worker 调度**

```bash
git add src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py tests/test_settings.py tests/test_operations.py tests/test_scheduler.py .env.example
git commit -m "feat: schedule call auction market series"
```

---

### Task 6: 同步运维文档、保留策略和生产检查

**Files:**
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `tests/test_production_checks.py`
- Verify unchanged: `contracts/postgrest-openapi-v1.json`
- Verify unchanged: `contracts/agent-tools-v1.json`
- Verify unchanged: `contracts/fastapi-openapi-v1.json`

**Interfaces:**
- Documents: 32 轮 live gate、两线程边界、12 个月分区发布、Raw 长期保留、09:26 隔离
- Preserves: 所有公共 API contracts 原样不变

- [ ] **Step 1: 更新当前运维事实**

在 Worker 调度文档加入新 job、固定时间、开关和 `morning_auction` executor；明确现有 `opening-auction-limit-up-quotes` 仍每 30 秒逐只采集，两个早盘任务可以并行，其他任务仍串行。日常采集文档写出 endpoint partial、missed round、节点池为空和分区缺失的诊断 SQL/日志字段。

最小发布手册增加：先在隔离库应用 migration；生产受保护 migration 后确认未来分区存在；打包部署并重启 Worker；检查 jobstore 中 09:15/09:26 两项；不得手工补造当日历史轮次。

- [ ] **Step 2: 写 12 个月保留操作边界**

文档明确每次常规发布通过新 ordered migration 创建后续月份、验证 Raw/Manifest/备份后 drop 超过最近 12 个完整月份的分区。Worker、APScheduler job 和 `.env` 均不执行分区 DDL；初始 migration 的分区覆盖到 2027-09-30，必须在 2027-09 前发布后续分区 migration。

- [ ] **Step 3: 扩展生产检查测试**

`tests/test_production_checks.py` 断言 `.env.example` 有新开关且没有时间项，EXPECTED_TABLES 含父表和分区，Worker service 不含 cron/systemd timer。不要对人类文档整段文字做脆弱字符串断言。

Run: `uv run pytest tests/test_production_checks.py -q`

Expected: PASS。

- [ ] **Step 4: 验证公共契约无差异**

Run: `git diff 2cb6b91 -- contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json`

Expected: 无输出。

- [ ] **Step 5: 提交运维文档**

```bash
git add docs/Worker调度系统.md docs/Worker日常采集与调度.md docs/最小生产发布运行手册.md tests/test_production_checks.py
git commit -m "docs: operate call auction market series"
```

---

### Task 7: 完整验证和交付准备

**Files:**
- Verify: Tasks 1–6 的所有文件
- Update only if a verification failure proves a defect: 该失败对应的聚焦 test/source 文件

**Interfaces:**
- Produces: 可审查、未部署的 Issue #48 实现分支

- [ ] **Step 1: 运行格式和静态检查**

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

Expected: 全部 exit 0。

- [ ] **Step 2: 运行完整本地测试**

Run: `uv run pytest`

Expected: exit 0、零失败；记录 skip 数量和具体原因，不能把 integration skip 表述为通过。

- [ ] **Step 3: 运行隔离 PostgreSQL integration gate**

Run: `uv run pytest -m integration`

Expected: 使用 disposable `TEST_DATABASE_URL` 时 exit 0。未配置时报告精确阻塞，不得连接生产 URL 或用户之前提供的生产端口。

- [ ] **Step 4: 核对 migration、契约、秘密和工作树**

```bash
git diff --check
git status --short
git log --oneline 2cb6b91..HEAD
git diff 2cb6b91 -- contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json
```

确认没有 `.env`、数据库 URL/密码、Raw、节点池、备份或本地 TDX 路径进入 Git。只有明确授权的隔离目标才可运行 `uv run python scripts/apply_migrations.py check --postgres-only`。

- [ ] **Step 5: 修复验证暴露的问题并重新运行对应 gate**

任何失败先在该失败所属 Task 已列出的聚焦测试文件中增加最小重现，再修改同一 Task 已列出的直接相关源码。通过后逐个写出真实文件名执行 `git add`，提交信息固定为 `fix: satisfy call auction series delivery gate`。不得使用通配暂存，不得创建空提交，不得改写无关用户变更。

- [ ] **Step 6: 交付实现报告并请求外部变更授权**

报告列出提交、测试计数、integration 状态、migration/分区范围、公共契约无差异、BSE 暂缓和下一交易日 live gate。用户明确要求后才允许 push、生产 migration、本地打包、服务器备份/切换 release、Worker restart；上线后只观察未来时槽，不回填已经错过的竞价序列。
