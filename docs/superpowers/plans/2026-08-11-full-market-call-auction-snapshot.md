# 沪深全市场开盘竞价快照 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在工作日 09:26 采集完整、单 endpoint 的沪深 listed stock 开盘竞价来源快照，并在 21:30 只用该快照与当日 ready 涨停池生成最终“今日竞价量”。

**Architecture:** 新建 append-only `call_auction_market_snapshot` 来源事实和晨间采集服务；每个 IngestionRun 固定一个 PYTDX endpoint，失败后第二 IngestionRun 才能换 endpoint 全量重试。21:30 使用一个 PostgreSQL 事务选择精确日期最新 succeeded ingestion、校验涨停池完整覆盖并全量替换现有最终快照，不再访问行情网络。

**Tech Stack:** Python 3.12、uv、pytdx 1.72、SQLAlchemy 2、psycopg 3、PostgreSQL/Supabase migrations、APScheduler 3、pytest、Ruff、mypy。

## Global Constraints

- 只采 `SSE/SZSE + security_type=stock + status=listed`；BSE 明确暂缓。
- 09:26 和 21:30 固定在 `scheduling_catalog.py`；`.env` 只使用现有 `CALL_AUCTION_SNAPSHOT_ENABLED` 启停开关。
- 一个成功 IngestionRun 只使用一个 endpoint；每批最多 80 只；最多两个全量 endpoint attempt。
- 观察窗口为上海时间 `[09:25:00,09:30:00)`；09:29:30 后不得发起新批次。
- 来源事实 append-only，并保存 Raw JSONL、RawManifest、QualityResult 和 IngestionRun lineage。
- `high_price`、`low_price` 是截至观察时点的当日最高/最低价；允许缺失，非空时必须满足价格区间约束。
- 21:30 不调用 Provider，不回退旧日期、partial ingestion 或收盘累计量。
- 全市场来源表不进入 `api_v1`、FastAPI 或 Agent 公共契约。
- 生产 schema 只通过有序 migration 和受保护 workflow 修改；integration tests 只能连接 disposable `TEST_DATABASE_URL`。
- 生产迁移、Worker 重启、推送和部署不由本计划自动授权，实施完成后另行请求确认。

---

### Task 1: 定义晨间竞价来源记录和受控代码

**Files:**
- Modify: `tests/test_realtime_quote.py`
- Modify: `src/market_data_center/domain/realtime_quote.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/domain/__init__.py`

**Interfaces:**
- Produces: `CallAuctionMarketSnapshotRecord(symbol, trade_date, observed_at, source_code, last_price, previous_close, high_price, low_price, cumulative_volume, cumulative_amount)`
- Produces: `DatasetCode.CALL_AUCTION_MARKET_SNAPSHOT`
- Produces: `WorkflowCode.CALL_AUCTION_MARKET_SNAPSHOT`
- Preserves: `CallAuctionSnapshotRecord`，但增加可空 `observed_at`

- [ ] **Step 1: 写领域记录失败测试**

把时间导入改为 `from datetime import UTC, date, datetime, timedelta`，在 domain 导入列表加入
`CallAuctionMarketSnapshotRecord`，并添加：

```python
def _market_auction(**changes: object) -> CallAuctionMarketSnapshotRecord:
    values: dict[str, object] = {
        "symbol": "SSE:600000",
        "trade_date": date(2026, 8, 12),
        "observed_at": datetime(2026, 8, 12, 1, 26, tzinfo=UTC),
        "source_code": "pytdx_hq",
        "last_price": Decimal("10.10"),
        "previous_close": Decimal("10.00"),
        "high_price": Decimal("10.10"),
        "low_price": Decimal("10.10"),
        "cumulative_volume": 123_400,
        "cumulative_amount": Decimal("1246340.00"),
    }
    values.update(changes)
    return CallAuctionMarketSnapshotRecord(**values)  # type: ignore[arg-type]


def test_market_auction_record_preserves_high_low_and_morning_window() -> None:
    record = _market_auction()
    assert record.high_price == Decimal("10.10")
    assert record.low_price == Decimal("10.10")
    with pytest.raises(ValueError, match="before 09:30"):
        _market_auction(observed_at=datetime(2026, 8, 12, 1, 30, tzinfo=UTC))


def test_market_auction_record_rejects_bse_and_invalid_price_range() -> None:
    with pytest.raises(ValueError, match="SSE/SZSE"):
        _market_auction(symbol="BSE:920000")
    with pytest.raises(ValueError, match="high_price"):
        _market_auction(high_price=Decimal("9.90"), low_price=Decimal("10.00"))
    with pytest.raises(ValueError, match="within"):
        _market_auction(
            last_price=Decimal("10.20"),
            high_price=Decimal("10.10"),
            low_price=Decimal("10.00"),
        )
```

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest tests/test_realtime_quote.py -q`

Expected: FAIL，提示 `CallAuctionMarketSnapshotRecord` 尚未定义或导出。

- [ ] **Step 3: 最小实现领域记录和枚举**

在 `domain/realtime_quote.py` 增加冻结 dataclass；使用 `ZoneInfo("Asia/Shanghai")` 校验本地日期和 `[09:25,09:30)`，复用 Decimal/非负/高低区间规则。为 `CallAuctionSnapshotRecord` 增加：

```python
observed_at: datetime | None = None
```

非空时必须为 aware datetime。随后在 `domain/__init__.py` 导出新记录，并在两个枚举中加入：

```python
CALL_AUCTION_MARKET_SNAPSHOT = "call_auction_market_snapshot"
```

- [ ] **Step 4: 运行 GREEN 与相关领域回归**

Run: `uv run pytest tests/test_realtime_quote.py tests/test_snapshot_collector.py -q`

Expected: PASS。

- [ ] **Step 5: 提交领域边界**

```bash
git add tests/test_realtime_quote.py src/market_data_center/domain
git commit -m "feat: define full-market call auction facts"
```

---

### Task 2: 让 PYTDX Provider 支持显式单 endpoint 和请求截止时间

**Files:**
- Modify: `tests/test_pytdx_hq_provider.py`
- Modify: `src/market_data_center/providers/contracts.py`
- Modify: `src/market_data_center/providers/pytdx_hq.py`
- Modify: `src/market_data_center/cli.py`

**Interfaces:**
- Produces: `PytdxHqProvider(..., endpoints: Sequence[tuple[str, int]] | None = None)`
- Changes: `RealtimeQuoteProvider.fetch_five_level_quotes(symbols, *, deadline: datetime | None = None)`
- Guarantee: 显式 `endpoints=(endpoint,)` 时建立连接和全部批次都只使用该 endpoint

- [ ] **Step 1: 写显式 endpoint 与截止时间失败测试**

在 `tests/test_pytdx_hq_provider.py` 增加以下明确的测试辅助函数；`BatchRecordedClient.fetch`
先记录完整请求，再为请求中的每个 `(market, code)` 生成与现有 `RecordedClient` 相同字段的行情行：

```python
def _symbols(count: int) -> tuple[str, ...]:
    return tuple(f"SSE:{600000 + index:06d}" for index in range(count))


class BatchRecordedClient(RecordedClient):
    def __init__(self, batches: list[tuple[tuple[int, str], ...]]) -> None:
        self.batches = batches
        self.hosts: tuple[tuple[str, int], ...] = ()

    def record_hosts(self, hosts: Sequence[tuple[str, int]]) -> "BatchRecordedClient":
        self.hosts = tuple(hosts)
        return self

    def fetch(self, requests: Sequence[tuple[int, str]]) -> Sequence[Mapping[str, object]]:
        self.batches.append(tuple(requests))
        return [self._row(market, code) for market, code in requests]
```

把现有 `RecordedClient.fetch` 的单行构造提取为 `_row(market, code)`，使原测试行为不变。然后添加：

```python
def test_market_provider_uses_one_explicit_endpoint_and_batches_by_eighty(tmp_path: Path) -> None:
    batches: list[tuple[tuple[int, str], ...]] = []
    client = BatchRecordedClient(batches)
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        endpoints=(("second.quote", 7709),),
        client_factory=lambda hosts, _timeout: client.record_hosts(hosts),
    )
    symbols = _symbols(161)
    with provider:
        result = provider.fetch_five_level_quotes(symbols)
    assert client.hosts == (("second.quote", 7709),)
    assert [len(batch) for batch in batches] == [80, 80, 1]
    assert result.requested_symbols == symbols


def test_provider_stops_starting_batches_at_deadline() -> None:
    batches: list[tuple[tuple[int, str], ...]] = []
    client = BatchRecordedClient(batches)
    clock = iter((
        datetime(2026, 8, 12, 1, 29, 29, tzinfo=UTC),
        datetime(2026, 8, 12, 1, 29, 29, 500000, tzinfo=UTC),
        datetime(2026, 8, 12, 1, 29, 31, tzinfo=UTC),
    )).__next__
    provider = PytdxHqProvider(
        PytdxHqSettings(_env_file=None),
        endpoints=(("only.quote", 7709),),
        client_factory=lambda hosts, _timeout: client.record_hosts(hosts),
        clock=clock,
    )
    with provider:
        result = provider.fetch_five_level_quotes(
            _symbols(161),
            deadline=datetime(2026, 8, 12, 1, 29, 30, tzinfo=UTC),
        )
    assert len(result.records) == 80
    assert result.failed_symbols == _symbols(161)[80:]
```

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest tests/test_pytdx_hq_provider.py -q`

Expected: FAIL，构造器不接受 `endpoints`，fetch 不接受 `deadline`。

- [ ] **Step 3: 实现最小 Provider 扩展**

- `endpoints is None` 时保持现有节点池加载和连接阶段 failover；显式 endpoints 时不加载池；
- 显式 endpoints 必须非空、唯一且端口合法；晨间调用只传一个；
- 每批开始前比较 aware UTC deadline，过期后把剩余 symbol 全部加入 `failed_symbols`；
- 不改变 Decimal 解码、手转股、Raw schema 或 BSE 拒绝规则；
- 更新 protocol 的 kw-only deadline，现有调用依靠默认 `None` 保持兼容。
- 同步 CLI 内 `_NoNetworkProvider` 的兼容签名为
  `fetch_five_level_quotes(self, symbols, *, deadline=None)`，仍然立即抛错且不访问网络。

- [ ] **Step 4: 运行 GREEN 与 Provider 回归**

Run: `uv run pytest tests/test_pytdx_hq_provider.py tests/test_auction_service.py -q`

Expected: PASS。

- [ ] **Step 5: 提交 Provider 能力**

```bash
git add tests/test_pytdx_hq_provider.py src/market_data_center/providers src/market_data_center/cli.py
git commit -m "feat: bound full-market quote endpoint requests"
```

---

### Task 3: 增加有序 migration 和生产 schema 清单

**Files:**
- Create: `supabase/migrations/20260811000400_create_call_auction_market_snapshot.sql`
- Modify: `tests/test_production_checks.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `scripts/apply_migrations.py`
- Modify: `src/market_data_center/recovery.py`

**Interfaces:**
- Produces table: `realtime.call_auction_market_snapshot`
- Alters table: `realtime.call_auction_snapshot add column observed_at timestamptz`
- Extends constraints: ingestion dataset、audit dataset、operations workflow code
- Grants: market_data_worker 对新表仅 `select,insert`；对最终表增加受控 `delete`

- [ ] **Step 1: 写 migration 行为失败测试**

在 `tests/test_postgres_integration.py` 增加 integration 测试，通过现有 disposable
`database_engine`（它会按顺序执行全部 migration）查询 PostgreSQL catalog，断言：

```python
assert connection.scalar(text(
    "select to_regclass('realtime.call_auction_market_snapshot')"
)) == "realtime.call_auction_market_snapshot"

columns = connection.execute(text("""
    select column_name, data_type, numeric_precision, numeric_scale, is_nullable
    from information_schema.columns
    where table_schema='realtime' and table_name='call_auction_market_snapshot'
    order by ordinal_position
""")).mappings().all()
assert {row["column_name"] for row in columns} >= {
    "ingestion_id", "symbol", "trade_date", "observed_at", "last_price",
    "previous_close", "high_price", "low_price", "cumulative_volume",
    "cumulative_amount", "source_code", "created_at",
}
```

同一测试继续查询 `pg_constraint`、`pg_policies` 和 `information_schema.role_table_grants`，验证
主键 `(ingestion_id,symbol)`、价格/非负/观察窗口约束、RLS 已启用、Worker 对来源表只有
`SELECT/INSERT`，并验证 `realtime.call_auction_snapshot.observed_at` 存在。另用枚举值插入最小
IngestionRun/WorkflowRun，证明数据库 check constraint 接受新 dataset/workflow code；查询
`information_schema.tables` 证明 `api_v1` 没有同名表或视图。

`tests/test_production_checks.py` 只扩展 `scripts/apply_migrations.py::EXPECTED_TABLES` 的可执行清单
行为断言，不读取 migration 文本或锁定 SQL 措辞。

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market_schema" -q`

Expected: FAIL，migration 尚未创建新表/约束/权限。必须使用 disposable `TEST_DATABASE_URL`；不得连接生产库。

- [ ] **Step 3: 编写 migration**

SQL 必须包含：

```sql
create table realtime.call_auction_market_snapshot (
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    observed_at timestamptz not null,
    last_price numeric(18, 4),
    previous_close numeric(18, 4),
    high_price numeric(18, 4),
    low_price numeric(18, 4),
    cumulative_volume bigint,
    cumulative_amount numeric(30, 4),
    source_code text not null check (source_code = 'pytdx_hq'),
    created_at timestamptz not null default now(),
    primary key (ingestion_id, symbol),
    constraint call_auction_market_price_range check (
        (high_price is null or high_price >= 0)
        and (low_price is null or low_price >= 0)
        and (high_price is null or low_price is null or high_price >= low_price)
        and (last_price is null or low_price is null or last_price >= low_price)
        and (last_price is null or high_price is null or last_price <= high_price)
    ),
    constraint call_auction_market_nonnegative check (
        (last_price is null or last_price >= 0)
        and (previous_close is null or previous_close >= 0)
        and (cumulative_volume is null or cumulative_volume >= 0)
        and (cumulative_amount is null or cumulative_amount >= 0)
    ),
    constraint call_auction_market_observation_window check (
        (observed_at at time zone 'Asia/Shanghai')::date = trade_date
        and (observed_at at time zone 'Asia/Shanghai')::time >= time '09:25:00'
        and (observed_at at time zone 'Asia/Shanghai')::time < time '09:30:00'
    )
);
```

同时添加 `(trade_date,ingestion_id,symbol)` 索引、RLS/policy/grants；用 drop/add/validate 模式向三个受控 check constraint 加入新 code；给最终表新增 nullable `observed_at` 和 `delete` 权限。

- [ ] **Step 4: 更新 schema/recovery 清单并运行 GREEN**

- `scripts/apply_migrations.py::EXPECTED_TABLES` 加入新表；
- `recovery.COUNT_QUERIES` 加入 `call_auction_market_snapshot`；
- orphan-fact UNION 加入新表 ingestion_id；
- 不新增 `EXPECTED_VIEWS` 或公共 API。

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market_schema" -q`

Run: `uv run pytest tests/test_production_checks.py tests/test_recovery.py -q`

Expected: 两条命令均 PASS。

- [ ] **Step 5: 提交 migration**

```bash
git add supabase/migrations/20260811000400_create_call_auction_market_snapshot.sql tests/test_production_checks.py tests/test_postgres_integration.py scripts/apply_migrations.py src/market_data_center/recovery.py
git commit -m "feat: add full-market call auction storage"
```

---

### Task 4: 实现 append-only Persistence 与事务最终化

**Files:**
- Modify: `tests/test_postgres_integration.py`
- Modify: `src/market_data_center/persistence/postgres.py`

**Interfaces:**
- Produces: `PostgreSQLPersistence.is_trading_day(trade_date) -> bool`
- Produces: `PostgreSQLPersistence.listed_sse_szse_stock_symbols() -> list[str]`
- Produces: `commit_call_auction_market_attempt(run, records, manifest, quality_results) -> None`
- Produces: `finalize_call_auction_snapshot(trade_date) -> int`

- [ ] **Step 1: 写 PostgreSQL integration 失败测试**

新增标记为 integration 的测试，使用现有 `database_engine` fixture 建立 SSE、SZSE、BSE、ETF、退市
样本，并断言：

```python
assert persistence.listed_sse_szse_stock_symbols() == ["SSE:600000", "SZSE:000001"]
```

再创建两个晨间 ingestion：partial run 只提交 `SSE:600000` 一行，较晚的 succeeded run 提交
`SSE:600000`、`SZSE:000001` 两行；所有行都显式填充 high/low。建立精确日期 ready 涨停池并调用：
建立精确日期 ready 涨停池并调用：

```python
written = persistence.finalize_call_auction_snapshot(date(2026, 8, 12))
assert written == 2
rows = connection.execute(text("""
    select symbol, observed_at, high_price, low_price, ingestion_id
    from realtime.call_auction_market_snapshot
    order by ingestion_id, symbol
""")).all()
assert len(rows) == 3
final = connection.execute(text("""
    select symbol, cumulative_volume, cumulative_amount, auction_premium_pct,
           observed_at, ingestion_id
    from realtime.call_auction_snapshot order by symbol
""")).all()
assert {row.ingestion_id for row in final} == {succeeded.ingestion_id}
```

增加三项独立测试：只有 partial 时抛出 `LookupError`；ready 池成员缺失时事务回滚；ready 空池时删除
当日旧最终行并返回 0。

- [ ] **Step 2: 运行 RED**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market or finalize_call_auction" -q`

Expected: FAIL，新方法和表行为尚不存在。若未配置 `TEST_DATABASE_URL`，只记录精确阻塞原因，不得连接生产库。

- [ ] **Step 3: 实现 append-only commit**

新增参数化 INSERT（无 `on conflict do update`）：

```python
INSERT_CALL_AUCTION_MARKET = text("""
insert into realtime.call_auction_market_snapshot (
 ingestion_id,symbol,trade_date,observed_at,last_price,previous_close,
 high_price,low_price,cumulative_volume,cumulative_amount,source_code
) values (
 :ingestion_id,:symbol,:trade_date,:observed_at,:last_price,:previous_close,
 :high_price,:low_price,:cumulative_volume,:cumulative_amount,:source_code
)
""")
```

`commit_call_auction_market_attempt` 在一个事务中插入 Manifest、QualityResult、成功标准事实并更新
IngestionRun；任何约束失败回滚全部数据库写入。

- [ ] **Step 4: 实现精确日期事务最终化**

`finalize_call_auction_snapshot` 在一个 `engine.begin()` 中：

1. 选择 dataset=`call_auction_market_snapshot`、status=`succeeded`、精确 `trade_date` 的最新
   `finished_at,ingestion_id`；
2. 选择精确日期最新 ready `CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP` snapshot；
3. 比较 pool member count 与 morning source join count，不相等则抛错；
4. 删除 `realtime.call_auction_snapshot where trade_date=:trade_date`；
5. `insert ... select` 写 pool 交集，复制来源 `observed_at/ingestion_id`，用 numeric 表达式计算溢价率；
6. 返回写入行数。

SQL 不读取 ingestion `request_params` JSON，不允许旧日期 fallback。

- [ ] **Step 5: 运行 GREEN**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market or finalize_call_auction" -q`

Expected: PASS。

- [ ] **Step 6: 提交 Persistence**

```bash
git add tests/test_postgres_integration.py src/market_data_center/persistence/postgres.py
git commit -m "feat: persist and finalize call auction snapshots"
```

---

### Task 5: 用 TDD 实现 09:26 全市场采集服务

**Files:**
- Create: `tests/test_call_auction_market_service.py`
- Create: `src/market_data_center/call_auction_market_service.py`

**Interfaces:**
- Produces: `CallAuctionMarketSnapshotService`
- Produces: `CallAuctionMarketCollectionSummary(status, attempts, expected_rows, accepted_rows, rejected_rows, ingestion_id)`
- Consumes: Task 1 record、Task 2 endpoint/deadline Provider、Task 4 Persistence

- [ ] **Step 1: 写单 endpoint 完整成功失败测试**

使用固定接口的 fakes：persistence 记录 `requested_universe/committed_runs/committed_records`，实现 Task 4
的四个方法；Raw store 返回含 `row_count` 的 manifest；provider factory 记录每次单 endpoint 和请求全集，
并按脚本返回 `RealtimeQuoteBatch`；clock 返回一个固定 aware UTC 时间。核心断言：

```python
summary = service.collect(date(2026, 8, 12))
assert summary.status == "succeeded"
assert summary.attempts == 1
assert persistence.requested_universe == ("SSE:600000", "SZSE:000001")
assert persistence.committed_runs[0].status is IngestionStatus.SUCCEEDED
assert persistence.committed_records[0].high_price == Decimal("10.10")
assert persistence.committed_records[0].low_price == Decimal("10.10")
assert provider_factory.endpoints == [("first.quote", 7709)]
```

- [ ] **Step 2: 写 partial 后全量换节点失败测试**

第一个 fake provider 返回一个 missing symbol，第二个返回全集：

```python
summary = service.collect(date(2026, 8, 12))
assert summary.attempts == 2
assert provider_factory.requested_symbols == [EXPECTED_UNIVERSE, EXPECTED_UNIVERSE]
assert [run.status for run in persistence.committed_runs] == [
    IngestionStatus.PARTIAL,
    IngestionStatus.SUCCEEDED,
]
assert provider_factory.endpoints == [
    ("first.quote", 7709),
    ("second.quote", 7709),
]
```

增加：两个 endpoint 均失败；BSE 不进入 universe（由 persistence fake 返回时服务防御性拒绝）；
09:25 前/09:30 后拒绝；09:29:30 cutoff 传给 Provider；明确空行情响应可成功；重复/未知/越界观察时点
产生 QualityResult 且不能 succeeded；RawManifest 行数等于 raw rows。

- [ ] **Step 3: 运行 RED**

Run: `uv run pytest tests/test_call_auction_market_service.py -q`

Expected: FAIL，服务模块不存在。

- [ ] **Step 4: 实现最小服务**

固定常量：

```python
SHANGHAI_ZONE = ZoneInfo("Asia/Shanghai")
WINDOW_START = time(9, 25)
REQUEST_CUTOFF = time(9, 29, 30)
WINDOW_END = time(9, 30)
MAX_ENDPOINT_ATTEMPTS = 2
```

服务流程必须：校验交易日与窗口；冻结 universe；取前两个稳定 endpoint；每 attempt 新建带 endpoint
metadata 和 expected count 的 IngestionRun；调用 Provider 全集一次并传 UTC cutoff；复用
`validate_realtime_quotes` 后映射 `high -> high_price`、`low -> low_price`；写 Raw；对 missing/validation
findings 生成 dataset 正确的 QualityResult；完整才 succeeded；首个 partial 后只在 cutoff 前创建第二 attempt。

- [ ] **Step 5: 运行 GREEN 和 Raw 回归**

Run: `uv run pytest tests/test_call_auction_market_service.py tests/test_raw_store.py tests/test_realtime_quote.py -q`

Expected: PASS。

- [ ] **Step 6: 提交晨间服务**

```bash
git add tests/test_call_auction_market_service.py src/market_data_center/call_auction_market_service.py
git commit -m "feat: collect full-market morning auction facts"
```

---

### Task 6: 将 21:30 改为数据库最终化并注册两个 Worker Job

**Files:**
- Modify: `tests/test_snapshot_collector.py`
- Modify: `tests/test_operations.py`
- Modify: `tests/test_scheduler.py`
- Modify: `src/market_data_center/snapshot_collector.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`

**Interfaces:**
- Produces job: `CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID = "call-auction-market-snapshot-daily"`
- Produces workflow step: `collect_call_auction_market_snapshot`
- Changes step: `finalize_call_auction_snapshot`
- Preserves switch: `SchedulerSettings.call_auction_snapshot_enabled`

- [ ] **Step 1: 写 21:30 无网络失败测试**

把旧“只从涨停池取 symbol 后请求 Provider”的测试替换为：

```python
def test_call_auction_finalization_delegates_to_database_only() -> None:
    persistence = FinalizationPersistence(result=2)
    written = finalize_call_auction(
        cast(Engine, object()),
        date(2026, 8, 12),
        persistence_factory=lambda _engine: persistence,
    )
    assert written == 2
    assert persistence.trade_dates == [date(2026, 8, 12)]
```

测试模块不得 monkeypatch/构造 `PytdxHqProvider`；运行后可用源码断言或依赖注入证明最终化路径无网络。

- [ ] **Step 2: 写目录和 scheduler 失败测试**

在 operations/scheduler 测试中断言：

```python
assert (jobs["call-auction-market-snapshot-daily"].hour,
        jobs["call-auction-market-snapshot-daily"].minute) == (9, 26)
assert (jobs["call-auction-snapshot-daily"].hour,
        jobs["call-auction-snapshot-daily"].minute) == (21, 30)
assert jobs["call-auction-market-snapshot-daily"].enabled is True
assert jobs["call-auction-snapshot-daily"].enabled is True
```

禁用 switch 时两个 job 都不存在；启用时两个 job 都 `max_instances=1`、`coalesce=True`。workflow 枚举与目录
集合必须完全相等。

- [ ] **Step 3: 运行 RED**

Run: `uv run pytest tests/test_snapshot_collector.py tests/test_operations.py tests/test_scheduler.py -q`

Expected: FAIL，新 job/workflow 不存在且旧最终化仍访问 Provider。

- [ ] **Step 4: 实现目录和执行函数**

- 新 workflow `call_auction_market_snapshot`，step `collect_call_auction_market_snapshot`；
- 现 workflow `call_auction_snapshot` 的描述改为盘后最终化，step 改为 `finalize_call_auction_snapshot`；
- job catalog 插入 09:26 job，两 job 读取同一 switch；
- `run_call_auction_market_snapshot_job` 加载 quote endpoints，构建 Task 5 service，并完整记录 Operations；
- `run_call_auction_snapshot_job` 使用 `WorkflowExecutionService` 调用纯数据库最终化；
- `build_scheduler` functions 映射加入新 job；
- 删除 `collect_call_auction` 中 21:30 Provider/Raw/IngestionRun 路径，保留或重命名为
  `finalize_call_auction` 的薄封装。

- [ ] **Step 5: 运行 GREEN**

Run: `uv run pytest tests/test_snapshot_collector.py tests/test_operations.py tests/test_scheduler.py -q`

Expected: PASS。

- [ ] **Step 6: 提交 Worker 工作流**

```bash
git add tests/test_snapshot_collector.py tests/test_operations.py tests/test_scheduler.py src/market_data_center/snapshot_collector.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py
git commit -m "feat: schedule morning auction capture and finalization"
```

---

### Task 7: 同步运维文档、备份门禁和公共契约兼容证据

**Files:**
- Modify: `docs/Worker调度系统.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `docs/最小生产发布运行手册.md`
- Verify: `.env.example`
- Verify: `tests/test_settings.py`
- Verify: `tests/test_operations.py`
- Verify: `tests/test_scheduler.py`
- Verify unchanged: `contracts/postgrest-openapi-v1.json`
- Verify unchanged: `contracts/agent-tools-v1.json`
- Verify unchanged: `contracts/fastapi-openapi-v1.json`

**Interfaces:**
- Documents: 09:26 来源采集、21:30 最终化、同一开关、BSE 暂缓、live gate
- Preserves: `.env` 无任务时间；公共 API 字段不变

- [ ] **Step 1: 更新当前事实文档（经项目所有者批准的文档测试例外）**

- Worker 表增加 09:26 job；
- 21:30 描述改成只读晨间 succeeded ingestion 与 ready 涨停池；
- 故障说明区分 partial morning attempt、晨间无成功输入和 ready 空池；
- 运行手册明确 09:29:30 cutoff、BSE 暂缓、单 endpoint 完整性与下一交易日 live gate；
- 最小发布验收检查两个 job 均启用且没有 OS 调度。

不改历史 plan/spec，不把尚未部署写成生产已运行事实。

- [ ] **Step 2: 验证真实配置和调度行为**

Run: `uv run pytest tests/test_settings.py tests/test_operations.py tests/test_scheduler.py -q`

Expected: PASS；现有开关行为测试和 Task 6 调度测试共同证明同一启停开关控制两个代码内固定时间的 job。

人工核对 `.env.example` 只有 `CALL_AUCTION_SNAPSHOT_ENABLED=true`，没有 hour/minute 字段；不为人类文档措辞新增源码文本断言。

- [ ] **Step 3: 验证契约兼容**

Run: `git diff --exit-code c29d893 -- contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json`

Expected: contracts 无差异。

- [ ] **Step 4: 提交文档**

```bash
git add docs/Worker调度系统.md docs/Worker日常采集与调度.md docs/最小生产发布运行手册.md
git commit -m "docs: operate full-market call auction workflow"
```

---

### Task 8: 完整验证和交付准备

**Files:**
- Verify: all modified files
- Update only if required by failures: focused source/test files from Tasks 1–7

**Interfaces:**
- Produces: clean branch with Issue #41 implementation and no production mutation

- [ ] **Step 1: 运行格式与静态检查**

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

Expected: all exit 0，无 warnings/errors。

- [ ] **Step 2: 运行完整单元测试**

Run: `uv run pytest`

Expected: exit 0，零失败。记录 skip 数和原因，不把 skip 描述为通过的 integration 证据。

- [ ] **Step 3: 运行隔离 PostgreSQL integration**

Run: `uv run pytest -m integration`

Expected: 在 disposable `TEST_DATABASE_URL` 上 exit 0。若未配置，报告精确阻塞，不得使用生产 URL。

- [ ] **Step 4: 核对 migration、契约和工作树**

```bash
uv run python scripts/apply_migrations.py check --postgres-only
git diff --check
git status --short
git log --oneline c29d893..HEAD
```

`apply_migrations.py check` 只能对显式提供的隔离/授权目标运行；没有 `MIGRATION_DATABASE_URL` 时记录
该门禁未运行。确认没有 `.env`、Raw、节点池、备份或数据库 URL 进入 Git。

- [ ] **Step 5: 提交任何纯验证修正**

只有验证暴露真实问题时，先在对应 Task 的聚焦测试中补失败用例，再修改最小源码；用
`git status --short` 核对后，只逐个 `git add` 该失败用例及其直接修复文件，并提交
`fix: satisfy call auction delivery gate`。禁止通配暂存、禁止把无关用户改动带入，也禁止创建空提交。

- [ ] **Step 6: 请求推送和生产部署授权**

交付报告必须列出：提交、测试计数、integration 状态、migration 文件、公共契约无差异、BSE 暂缓和
下一交易日 live gate。只有用户明确要求后才推送、运行生产 migration、打包、备份、切换 release 或
重启 Worker；上线验收不得手工补造历史竞价数据。
