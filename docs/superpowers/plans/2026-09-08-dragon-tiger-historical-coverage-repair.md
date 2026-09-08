# DragonTiger Historical Coverage Repair Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 修复 2025-01-01 至今的 DragonTiger 历史采集失败，使多种触发窗口、席位改名、重复/占位/单侧披露、BSE 主数据和 Raw 血缘均可正确、可审计地处理，并替换线上读取契约。

**Architecture:** 以通用 TriggerWindow 和独立 AmountPeriod 替换二值周期枚举；Provider 先冻结完整 Raw，再返回带 SourceFinding 的标准化结果。Ingestion 使用持久化分阶段状态，席位来源代码拥有稳定身份，BSE 证券通过独立 Tushare Worker 任务同步；ordered migration 同批替换数据库模型和四个有界读取契约。

**Tech Stack:** Python 3.12、uv、dataclasses、Decimal、PostgreSQL、psycopg/SQLAlchemy、Supabase/PostgREST、FastAPI/Pydantic、APScheduler、pytest、ruff、mypy。

**Spec:** `docs/superpowers/specs/2026-09-08-dragon-tiger-historical-coverage-repair-design.md`

## Global Constraints

- 治理依据依次为项目宪法、ADR-0049、ADR-0053、DragonTiger v2 领域详设和 Issue #70。
- 生产 schema 只能由 `supabase/migrations/*.sql` 前向修改；不得改历史 migration 或运行 ad-hoc DDL。
- 成功 ingestion 只有一个实际 Provider；不得混合或回退拼接来源事实。
- Raw 对象不可变，现有 v1/v2 Raw 必须保持可重放；不得提交 Raw 市场数据。
- `SeatTrade` 金额使用 `Decimal`，缺失保持 `None`，买卖金额不得同时为空或同时为零。
- Worker 内 APScheduler 是唯一调度入口；不得增加 cron、systemd timer 或 Windows Task Scheduler 触发器。
- FastAPI 只能调用有界 `api_v1` RPC，不得直接读取内部 schema。
- 日志、质量结果和命令输出不得包含 token、DSN、完整来源负载或任意底层异常文本。
- PostgreSQL 集成测试只允许使用隔离可丢弃的 `TEST_DATABASE_URL`。
- 现有用户改动必须保留；每个任务先运行聚焦测试，再提交单一职责 commit。

## File Map

- `src/market_data_center/domain/dragon_tiger.py`：窗口、金额周期、来源发现、Event/SeatTrade 不变量和内容哈希。
- `src/market_data_center/providers/contracts.py`：DragonTiger 专用延迟标准化批次契约。
- `src/market_data_center/providers/eastmoney_dragon_tiger.py`：东财有界传输、Raw 冻结和多页逐 Event 读取。
- `src/market_data_center/providers/eastmoney_dragon_tiger_normalizer.py`：东财 v1/v2/v3 Raw 的显式原因映射、去重、占位过滤和单侧合并。
- `src/market_data_center/providers/tushare_dragon_tiger.py`：Tushare 新窗口模型映射和标准化结果。
- `src/market_data_center/dragon_tiger_service.py`：分阶段采集、窗口解析、质量转换和连续回补。
- `src/market_data_center/persistence/dragon_tiger_postgres.py`：分阶段 run/manifest/fact 事务和来源席位身份持久化。
- `supabase/migrations/20260908000200_repair_dragon_tiger_historical_coverage.sql`：事实 schema、席位身份、数据迁移和四个替换 RPC。
- `src/market_data_center/providers/tushare.py`：仅发布 BSE 的 Tushare Security Provider 包装。
- `src/market_data_center/scheduling_catalog.py`、`scheduler.py`、`settings.py`、`domain/operations.py`：BSE 任务目录、工作流和 20:15 调度。
- `src/market_data_center/reliability.py`：旧 Raw 的 v1/v2/v3 DragonTiger 重放及 BSE scoped Security 重放。
- `src/market_data_center/dragon_tiger_recovery.py`：限定目录的孤儿 Raw 扫描、校验和补登记。
- `src/market_data_center/cli.py`：BSE 同步、孤儿恢复和最多 730 天的连续回补入口。
- `src/market_data_center/public_api/models.py`、`queries.py`、`app.py`、`openapi_zh.py`：替换读取模型和路由参数。
- `contracts/postgrest-openapi-v1.json`、`fastapi-openapi-v1.json`、`agent-tools-v1.json`：三份同步契约。
- `tests/test_*.py`、`tests/test_postgres_integration.py`：每个边界的单元、调度、重放、契约和数据库集成测试。

---

### Task 1: 通用窗口、金额周期与标准化结果

**Files:**
- Modify: `src/market_data_center/domain/dragon_tiger.py`
- Modify: `src/market_data_center/domain/__init__.py`
- Modify: `src/market_data_center/providers/contracts.py`
- Test: `tests/test_dragon_tiger.py`
- Create: `tests/test_provider_contracts.py`

**Interfaces:**
- Produces: `DragonTigerWindowBasis`, `DragonTigerAmountPeriodBasis`, `DragonTigerTriggerWindow`, `DragonTigerAmountPeriod`, `DragonTigerSourceFinding`, `DragonTigerNormalizationResult`, `DragonTigerProviderBatch`。
- Produces: `DragonTigerEventDraft.resolve_windows(trigger_start_date, amount_start_date)`。
- Consumes: 现有 `DragonTigerReasonType`、`SeatTradeRecord` 和通用 ingestion 领域类型。

- [ ] **Step 1: 写窗口和缺失周期的失败测试**

```python
def test_event_keeps_trigger_window_separate_from_unspecified_amount_period() -> None:
    draft = _draft(
        trigger=DragonTigerTriggerWindow(
            basis=DragonTigerWindowBasis.MARKET_SESSIONS,
            session_count=10,
            occurrence_count=4,
            start_date=None,
            end_date=TRADE_DATE,
        ),
        amount_period=DragonTigerAmountPeriod(
            basis=DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED,
            session_count=None,
            start_date=None,
            end_date=None,
        ),
    )

    record = draft.resolve_windows(date(2026, 8, 7), None)

    assert record.trigger_window.start_date == date(2026, 8, 7)
    assert record.amount_period.basis is DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED
    assert record.amount_period.start_date is None
```

同时添加以下参数化断言：市场/证券有成交两种 trigger basis 合法；session count 必须大于 0；
`SOURCE_UNSPECIFIED` 必须同时令 amount session/start/end 为 `None`；已验证 amount period 必须有完整
session/start/end；Event 至少一侧披露为真；Reason 不再包含 `period_type`。

- [ ] **Step 2: 运行测试并确认失败**

Run: `uv run pytest tests/test_dragon_tiger.py tests/test_provider_contracts.py -q`

Expected: collection errors for missing window classes and the obsolete `DragonTigerPeriodType` fields.

- [ ] **Step 3: 实现最小领域类型和专用批次**

```python
class DragonTigerWindowBasis(StrEnum):
    MARKET_SESSIONS = "MARKET_SESSIONS"
    SECURITY_TRADED_SESSIONS = "SECURITY_TRADED_SESSIONS"


class DragonTigerAmountPeriodBasis(StrEnum):
    MARKET_SESSIONS = "MARKET_SESSIONS"
    SECURITY_TRADED_SESSIONS = "SECURITY_TRADED_SESSIONS"
    SOURCE_UNSPECIFIED = "SOURCE_UNSPECIFIED"


@dataclass(frozen=True, slots=True)
class DragonTigerSourceFinding:
    rule_code: str
    severity: QualitySeverity
    source_event_id: str
    report_kind: str
    occurrence_count: int
    filtered_count: int


@dataclass(frozen=True, slots=True)
class DragonTigerNormalizationResult:
    events: tuple[DragonTigerEventDraft, ...]
    findings: tuple[DragonTigerSourceFinding, ...] = ()
```

`DragonTigerProviderBatch` 保存 `raw_rows`、`request_params`、`schema_version` 和一个只执行一次的
`normalization_factory`；`.normalization` 将非 `ProviderError` 包装为通用 Provider normalization
failure。通用 `ProviderBatch` 不修改，避免影响其他领域。

- [ ] **Step 4: 更新 Event 校验、自然键和 content hash**

从 Reason、Draft、Record 和 payload/hash 中删除 `period_type`；加入全部 trigger/amount/disclosure
字段。语义键使用：

```python
semantic_key = (
    record.symbol,
    record.trade_date,
    record.reason.reason_code,
    record.reason.source_code,
)
```

`buy_disclosure_present` 或 `sell_disclosure_present` 至少一个为真；SeatTrade 原有金额和排名硬约束不变。

- [ ] **Step 5: 运行聚焦测试**

Run: `uv run pytest tests/test_dragon_tiger.py tests/test_provider_contracts.py -q`

Expected: PASS.

- [ ] **Step 6: 提交领域模型**

```text
git add src/market_data_center/domain/dragon_tiger.py src/market_data_center/domain/__init__.py src/market_data_center/providers/contracts.py tests/test_dragon_tiger.py tests/test_provider_contracts.py
git commit -m "refactor: model dragon tiger disclosure windows"
```

### Task 2: 东财/Tushare Raw-first 标准化和稳定原因映射

**Files:**
- Create: `src/market_data_center/providers/eastmoney_dragon_tiger_normalizer.py`
- Modify: `src/market_data_center/providers/eastmoney_dragon_tiger.py`
- Modify: `src/market_data_center/providers/tushare_dragon_tiger.py`
- Modify: `src/market_data_center/providers/__init__.py`
- Test: `tests/test_eastmoney_dragon_tiger_provider.py`
- Test: `tests/test_tushare_dragon_tiger_provider.py`

**Interfaces:**
- Consumes: Task 1 的 `DragonTigerProviderBatch` 和 `DragonTigerNormalizationResult`。
- Produces: `normalize_eastmoney_dragon_tiger_raw(rows: Sequence[RawRow], schema_version: str) -> DragonTigerNormalizationResult`。
- Produces: `map_eastmoney_trigger_window(reason: str, trade_date: date) -> DragonTigerTriggerWindow`。

- [ ] **Step 1: 写真实失败族的参数化测试**

```python
@pytest.mark.parametrize(
    ("reason", "basis", "sessions", "occurrences"),
    [
        ("连续三个交易日内涨幅偏离值累计达到20%", "MARKET_SESSIONS", 3, None),
        ("连续3个交易日内涨幅偏离值累计达到20%", "MARKET_SESSIONS", 3, None),
        ("有价格涨跌幅限制的连续10个交易日内收盘价格涨幅偏离值累计达到100%的证券", "MARKET_SESSIONS", 10, None),
        ("连续10个交易日内4次出现同正向异常波动的证券", "MARKET_SESSIONS", 10, 4),
        ("北交所股票最近3个有成交的交易日以内收盘价涨跌幅偏离值累计达到+40%(-40%)", "SECURITY_TRADED_SESSIONS", 3, None),
    ],
)
def test_maps_verified_trigger_windows(reason, basis, sessions, occurrences) -> None:
    result = map_eastmoney_trigger_window(reason, TRADE_DATE)
    assert result.basis.value == basis
    assert result.session_count == sessions
    assert result.occurrence_count == occurrences
```

再添加：未知五日文本返回 `DT_PERIOD_MAPPING_UNSUPPORTED`；10/30/BSE 的 amount period 是
`SOURCE_UNSPECIFIED`；单日和三日 amount period 分别为 market/1 和 market/3。

- [ ] **Step 2: 写重复、占位、单侧与多页传输失败测试**

测试必须断言：

```python
assert len(batch.raw_rows) == source_row_count
assert result.findings[0].rule_code == "DT_SOURCE_DUPLICATE_FILTERED"
assert all(not (trade.buy_amount == 0 and trade.sell_amount == 0) for trade in event.seat_trades)
assert event.buy_disclosure_present is True
assert event.sell_disclosure_present is False
```

多页测试让全局 detail 返回 `pages=2`，并验证 Adapter 随后发出包含单个 `TRADE_ID` 的 filter，且不再
请求 detail page 2。逐 Event 响应包含非重复完整行；来源 count 不一致必须抛
`DT_SOURCE_COUNT_MISMATCH`。

- [ ] **Step 3: 运行 Provider 测试并确认失败**

Run: `uv run pytest tests/test_eastmoney_dragon_tiger_provider.py tests/test_tushare_dragon_tiger_provider.py -q`

Expected: FAIL because duplicate rows are rejected before Raw and only two period values exist.

- [ ] **Step 4: 拆分传输与标准化并升级 Raw schema**

`eastmoney_dragon_tiger.py` 只负责有界请求和 `_raw_row`；设置：

```python
SCHEMA_VERSION = "eastmoney.dragon_tiger.v3"
SUPPORTED_REPLAY_SCHEMAS = frozenset(
    {
        "eastmoney.trading_billboard.v1",
        "eastmoney.dragon_tiger.v2",
        SCHEMA_VERSION,
    }
)
```

完全重复检查从 `_fetch_report` 移到 normalizer。多页 detail 根据冻结 summary IDs 调用
`_fetch_event_details(report, trade_date, event_id)`；每个 Event 每侧最多一页、五条 accepted rows。

- [ ] **Step 5: 实现显式原因映射、精确去重和单侧合并**

Normalizer 以 canonical JSON SHA-256 判断完全重复；Raw 顺序不变，标准化时只接受第一次出现。
零活动占位要求 BUY/SELL/NET 都是明确 Decimal zero 才过滤。单侧为空只写 warning finding，不能
创建伪零金额。可靠代码跨侧合并时只按 code；不同名称均作为同一来源身份的观察，不再降级为匿名行。

- [ ] **Step 6: 更新 Tushare 窗口映射**

Tushare 返回相同的 `DragonTigerNormalizationResult`，Reason 删除 `period_type`；未验证多日原因使用
同一稳定失败码。两个接口仍必须同批成功且不得与东财拼接。

- [ ] **Step 7: 运行聚焦测试并提交**

Run: `uv run pytest tests/test_eastmoney_dragon_tiger_provider.py tests/test_tushare_dragon_tiger_provider.py -q`

Expected: PASS.

```text
git add src/market_data_center/providers tests/test_eastmoney_dragon_tiger_provider.py tests/test_tushare_dragon_tiger_provider.py
git commit -m "fix: preserve and normalize dragon tiger source rows"
```

### Task 3: Ordered migration 与来源席位身份模型

**Files:**
- Create: `supabase/migrations/20260908000200_repair_dragon_tiger_historical_coverage.sql`
- Modify: `src/market_data_center/persistence/dragon_tiger_postgres.py`
- Test: `tests/test_postgres_integration.py`

**Interfaces:**
- Consumes: Task 1 的 Event 字段和 Task 2 的可靠 `seat_source_key`。
- Produces: `billboard.trading_seat_source_identity`、重构后的 `trading_seat_alias` 和 v2 Event schema。
- Produces: `_resolve_seat(connection, trade) -> UUID | None`，可靠代码只按来源 identity 解析。

- [ ] **Step 1: 写 migration/席位改名集成测试**

```python
def test_dragon_tiger_reliable_seat_key_accepts_temporal_renames(database_engine) -> None:
    persistence = PostgreSQLDragonTigerPersistence(database_engine)
    persistence.publish_success(_prepared_event("10488161", "旧北京分公司", date(2025, 1, 2)))
    persistence.publish_success(_prepared_event("10488161", "北京第二分公司", date(2026, 8, 20)))

    identities = _fetch_all(database_engine, "select seat_id from billboard.trading_seat_source_identity where source_seat_key='10488161'")
    aliases = _fetch_all(database_engine, "select alias_name from billboard.trading_seat_alias order by alias_name")
    assert len(identities) == 1
    assert {row[0] for row in aliases} == {"旧北京分公司", "北京第二分公司"}
```

再测试同名不同 key 产生两个 identity；无 key 泛化行不产生 identity；旧 DAY/THREE_DAY 数据迁移为
verified market/1 和 market/3；全部内部表 RLS 与 worker grant 保持。

- [ ] **Step 2: 运行集成测试并确认失败**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "dragon_tiger" -q`

Expected: FAIL because the identity table and new Event columns do not exist. If `TEST_DATABASE_URL` is absent,
record the exact skip and run SQL static assertions in `tests/test_api_contracts.py` until an isolated database is available.

- [ ] **Step 3: 编写 ordered migration 的表和数据迁移部分**

核心约束使用：

```sql
create table billboard.trading_seat_source_identity (
    identity_id uuid primary key default gen_random_uuid(),
    seat_id uuid not null references billboard.trading_seat(seat_id),
    source_code text not null,
    source_seat_key text not null,
    first_seen_date date not null,
    last_seen_date date not null,
    check (btrim(source_code) <> '' and btrim(source_seat_key) <> ''),
    check (first_seen_date <= last_seen_date),
    unique (source_code, source_seat_key)
);
```

Event 新列先 nullable、按旧 `period_type` 回填、验证后设约束；最后删除旧 period unique/index/columns。
Amount `SOURCE_UNSPECIFIED` 必须令 sessions/start/end 全部为 null。Migration 在 drop 前用 `DO` block
拒绝任何无法确定迁移的旧值。

- [ ] **Step 4: 重写可靠席位解析**

在同一 advisory key 下：按 `(source_code, source_seat_key)` upsert identity；已有 identity 永远复用
其 seat；按 `(identity_id, alias_name)` upsert首次/末次日期。删除“名称已绑定其他 key”和“key 名称冲突”
两条 RuntimeError 分支。数据库返回 UUID 类型不符仍以 `DT_SEAT_IDENTITY_CONFLICT` 失败。

- [ ] **Step 5: 运行集成测试和静态 migration 检查**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "dragon_tiger" -q`

Run: `uv run pytest tests/test_api_contracts.py -q`

Expected: PASS.

- [ ] **Step 6: 提交 schema 和身份持久化**

```text
git add supabase/migrations/20260908000200_repair_dragon_tiger_historical_coverage.sql src/market_data_center/persistence/dragon_tiger_postgres.py tests/test_postgres_integration.py tests/test_api_contracts.py
git commit -m "fix: version dragon tiger windows and seat identities"
```

### Task 4: 分阶段 ingestion、质量码和连续回补

**Files:**
- Modify: `src/market_data_center/dragon_tiger_service.py`
- Modify: `src/market_data_center/persistence/dragon_tiger_postgres.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Test: `tests/test_dragon_tiger_service.py`
- Test: `tests/test_postgres_integration.py`

**Interfaces:**
- Produces: `begin_ingestion(run)`, `attach_raw_manifest(run, manifest)`, `publish_success(run, quality, records)`, `complete_failure(run, quality)`。
- Produces: `DragonTigerBackfillFailure(trade_date, error_code)` 和 `DragonTigerBackfillSummary.failed_dates`。
- Consumes: Task 2 的 `batch.normalization.events/findings` 和 Task 3 的 schema。

- [ ] **Step 1: 写严格调用顺序和失败血缘测试**

```python
def test_collect_durably_registers_run_and_manifest_before_normalization() -> None:
    service, persistence, events = _service()
    service.collect(TRADE_DATE)
    assert events == ["begin", "fetch", "raw", "manifest", "normalize", "publish"]


def test_publish_failure_keeps_registered_manifest_and_marks_failed() -> None:
    service, persistence, events = _service(publish_error=RuntimeError("database detail"))
    with pytest.raises(DragonTigerCollectionError) as caught:
        service.collect(TRADE_DATE)
    assert caught.value.code == "DT_FACT_PUBLISH_FAILED"
    assert events == ["begin", "fetch", "raw", "manifest", "normalize", "publish", "failure"]
    assert persistence.manifest is not None
    assert "database detail" not in persistence.failed_run.error_summary
```

补充 provider、Raw write、manifest attach、normalization、validation 和 publish 六个 phase 的稳定码测试。

- [ ] **Step 2: 写回补继续和 730 天边界测试**

三天样本令中间一天失败，断言第三天仍执行，`failed_dates` 只包含中间日；731 天范围抛边界错误。
已存在同 schema 成功 ingestion 的日期由 persistence 返回并跳过，使重跑可从数据库状态恢复。

- [ ] **Step 3: 运行 service 测试并确认失败**

Run: `uv run pytest tests/test_dragon_tiger_service.py -q`

Expected: FAIL because current service begins no durable run, commits manifest atomically with facts and stops at first date failure.

- [ ] **Step 4: 实现状态机和质量转换**

`collect()` 严格执行 begin → fetch → Raw → attach → normalize → validate → publish。Source warning findings
转换为 `QualitySeverity.WARNING/QualityStatus.FAILED`，但 `blocks_core_write` 为 false；领域 ERROR findings
阻止事实发布。失败摘要只使用：

```python
error_summary = f"{error.code}:{error.phase}"
```

`fetched_rows` 是 Raw 行数；`rejected_rows` 是确定性过滤行数；`accepted_rows` 是 Raw 行数减过滤行数，
不得把 Event 数与来源行数混用。

- [ ] **Step 5: 更新 PostgreSQL 状态转换并测试幂等**

每个方法使用独立 transaction；`attach_raw_manifest` 只接受 RUNNING 且无 manifest；`publish_success`
要求 manifest 已存在；`complete_failure` 可从 RUNNING 转 FAILED。相同 terminal 内容重试返回现状，冲突
terminal 内容抛受控错误。

- [ ] **Step 6: 运行 service 与 integration 测试并提交**

Run: `uv run pytest tests/test_dragon_tiger_service.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "dragon_tiger" -q`

Expected: PASS.

```text
git add src/market_data_center/dragon_tiger_service.py src/market_data_center/persistence/dragon_tiger_postgres.py src/market_data_center/domain/ingestion.py tests/test_dragon_tiger_service.py tests/test_postgres_integration.py
git commit -m "fix: persist dragon tiger ingestion stages"
```

### Task 5: BSE 全状态证券 Worker 任务

**Files:**
- Modify: `src/market_data_center/providers/tushare.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `src/market_data_center/reliability.py`
- Test: `tests/test_tushare_provider.py`
- Test: `tests/test_settings.py`
- Test: `tests/test_operations.py`
- Test: `tests/test_scheduler.py`
- Test: `tests/test_cli.py`
- Test: `tests/test_reliability.py`

**Interfaces:**
- Produces: `TushareBseSecurityProvider.fetch_securities() -> ProviderBatch[SecurityRecord]`。
- Produces: `WorkflowCode.SECURITY_BSE_DAILY`、`SECURITY_BSE_JOB_ID = "security-bse-daily"`。
- Produces: CLI `market-data-center --provider tushare security-bse`。

- [ ] **Step 1: 写 BSE scoped Provider 和 Raw replay 测试**

```python
def test_tushare_bse_security_keeps_full_raw_and_publishes_only_bse() -> None:
    batch = TushareBseSecurityProvider(FakeClient()).fetch_securities()
    assert len(batch.raw_rows) == 3
    assert [record.symbol for record in batch.records] == ["BSE:920000"]
    assert batch.request_params["exchange_scope"] == "BSE"
    assert batch.request_params["list_statuses"] == ["L", "D", "P"]
```

Replay 测试使用同一 `tushare.security.v1` schema 加 `exchange_scope=BSE`，断言只重放 BSE 标准记录。

- [ ] **Step 2: 写 20:15 调度和 20:30 依赖测试**

测试 `security_bse_enabled=False` 时不注册；启用后 cron 为工作日 20:15。DragonTiger job 在前置工作流
失败或缺失时记录 dependency failure 且不调用东财；前置成功时 20:30 正常采集。

- [ ] **Step 3: 运行聚焦测试并确认失败**

Run: `uv run pytest tests/test_tushare_provider.py tests/test_settings.py tests/test_operations.py tests/test_scheduler.py tests/test_cli.py tests/test_reliability.py -q`

Expected: FAIL because scoped provider, workflow code, setting and job do not exist.

- [ ] **Step 4: 实现 scoped Provider、CLI 和 Worker catalog**

`TushareBseSecurityProvider` 复用 `TushareProvider.fetch_securities()` 的 Raw，lazy record factory 仅接受
`record.exchange is Exchange.BSE`。设置增加 `security_bse_enabled: bool = False`。Workflow step 固定为
`sync_bse_security`，schedule 固定工作日 20:15，不新增 OS 任务。

- [ ] **Step 5: 实现 DragonTiger 前置检查**

Operations persistence 以同一上海日期查询 `security_bse_daily` 是否 succeeded。缺失/失败使用
`DT_BSE_SECURITY_PREREQUISITE_FAILED`，不把昨日成功当作当日成功，也不启动东财请求。

- [ ] **Step 6: 运行测试并提交**

Run: `uv run pytest tests/test_tushare_provider.py tests/test_settings.py tests/test_operations.py tests/test_scheduler.py tests/test_cli.py tests/test_reliability.py -q`

Expected: PASS.

```text
git add src/market_data_center/providers/tushare.py src/market_data_center/settings.py src/market_data_center/domain/operations.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py src/market_data_center/cli.py src/market_data_center/reliability.py tests/test_tushare_provider.py tests/test_settings.py tests/test_operations.py tests/test_scheduler.py tests/test_cli.py tests/test_reliability.py
git commit -m "feat: sync bse security before dragon tiger"
```

### Task 6: 旧 Raw 重放与孤儿 Raw 恢复

**Files:**
- Create: `src/market_data_center/dragon_tiger_recovery.py`
- Modify: `src/market_data_center/reliability.py`
- Modify: `src/market_data_center/persistence/dragon_tiger_postgres.py`
- Modify: `src/market_data_center/cli.py`
- Test: `tests/test_dragon_tiger_recovery.py`
- Test: `tests/test_reliability.py`
- Test: `tests/test_cli.py`
- Test: `tests/test_postgres_integration.py`

**Interfaces:**
- Produces: `DragonTigerOrphanRecovery.scan() -> DragonTigerOrphanScan`。
- Produces: `DragonTigerOrphanRecovery.register(scan, *, dry_run: bool) -> DragonTigerOrphanRecoverySummary`。
- Produces: CLI `dragon-tiger-raw-recovery --dry-run` 和 `--execute --confirm` 两种互斥模式。

- [ ] **Step 1: 写路径约束、完整性和幂等失败测试**

临时目录创建合法路径：

```text
eastmoney/dragon_tiger/year=2025/month=01/day=03/00000000-0000-0000-0000-000000000201.jsonl
```

断言 dry-run 不调用 persistence；执行模式计算 SHA-256/bytes/rows 并登记一个 FAILED run/manifest；
第二次执行报告 already_registered。另测 `..`、错误 dataset、文件名非 UUID、多交易日、未知 schema、
损坏 JSONL 和已有冲突 manifest 全部拒绝。

- [ ] **Step 2: 写 v1/v2/v3 replay 结果和 findings 测试**

每个 schema fixture 都必须返回 Task 1 的 normalization result；历史中文/阿拉伯三日、零占位和改名
样本重放成功。Replay 新 run 使用新的分阶段 persistence，并把 source findings 写入 audit quality。

- [ ] **Step 3: 运行测试并确认失败**

Run: `uv run pytest tests/test_dragon_tiger_recovery.py tests/test_reliability.py tests/test_cli.py -q`

Expected: FAIL because recovery service and new normalization result handling do not exist.

- [ ] **Step 4: 实现只读扫描和受控补登记**

扫描根目录固定为 `LocalRawStore` 配置 root 下 `eastmoney/dragon_tiger`；使用 `Path.resolve()` 和
`is_relative_to()` 二次验证。只读取有界 JSONL，逐行必须是 string mapping；从 `payload_json` 验证
唯一交易日。执行只插入 FAILED ingestion/manifest 和 `DT_RECOVERED_ORPHAN_RAW` quality，不修改文件。

- [ ] **Step 5: 更新 RawReplayService**

DragonTiger normalizer 返回 `events/findings`；重放使用通用窗口解析，证券有成交起点缺失仅产生 warning。
删除旧 `DragonTigerPeriodType` 分支。BSE scoped Security replay检查 `request_params.exchange_scope`。

- [ ] **Step 6: 运行单元和集成测试并提交**

Run: `uv run pytest tests/test_dragon_tiger_recovery.py tests/test_reliability.py tests/test_cli.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "orphan or dragon_tiger" -q`

Expected: PASS.

```text
git add src/market_data_center/dragon_tiger_recovery.py src/market_data_center/reliability.py src/market_data_center/persistence/dragon_tiger_postgres.py src/market_data_center/cli.py tests/test_dragon_tiger_recovery.py tests/test_reliability.py tests/test_cli.py tests/test_postgres_integration.py
git commit -m "feat: recover and replay dragon tiger raw"
```

### Task 7: 替换 PostgREST/FastAPI/Agent 读取契约

**Files:**
- Modify: `supabase/migrations/20260908000200_repair_dragon_tiger_historical_coverage.sql`
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/public_api/openapi_zh.py`
- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Test: `tests/test_public_api.py`
- Test: `tests/test_api_contracts.py`
- Test: `tests/test_postgres_integration.py`

**Interfaces:**
- Replaces: date/symbol RPC `p_period_type` with `p_trigger_window_basis` and `p_trigger_window_sessions`。
- Produces: Event response trigger/amount/disclosure/quality fields and metrics coverage metadata。

- [ ] **Step 1: 写 API 模型和路由失败测试**

```python
def test_dragon_tiger_event_exposes_window_and_quality_metadata(client) -> None:
    response = client.get(
        "/api/v1/dragon-tiger/events/by-date",
        params={
            "trade_date": "2026-08-20",
            "trigger_window_basis": "MARKET_SESSIONS",
            "trigger_window_sessions": 10,
        },
        headers=API_HEADERS,
    )
    assert response.status_code == 200
    item = response.json()["items"][0]
    assert "period_type" not in item
    assert item["trigger_window_sessions"] == 10
    assert item["amount_period_basis"] == "SOURCE_UNSPECIFIED"
    assert item["buy_disclosure_present"] is True
    assert item["data_quality_codes"] == ["DT_DISCLOSURE_SIDE_MISSING"]
```

另测非法 basis、session count 非正、366 天边界、limit/offset 边界和 API key 保护。

- [ ] **Step 2: 写 RPC/RLS/grant 集成测试**

按新签名调用 date/symbol RPC；断言旧签名不存在。插入单侧 Event 和 audit quality 后，断言 Event 与
metrics 返回安全质量码、coverage 和正确 nullability。API 角色仍不能直接 select `billboard`/`audit`。

- [ ] **Step 3: 运行测试并确认失败**

Run: `uv run pytest tests/test_public_api.py tests/test_api_contracts.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "dragon_tiger" -q`

Expected: FAIL because current contract still requires/returns `period_type`.

- [ ] **Step 4: 完成 migration 中四个替换 RPC**

先精确 drop 旧 signatures，再创建相同业务名称的新 signatures。内部 helper 聚合
`audit.quality_result` 时只返回 `DT_` 白名单 rule codes，并以 source event natural key 关联；不得向
API 角色授予 audit 表权限。所有函数保持 `security definer`、固定 `search_path` 和 5 秒 timeout。

- [ ] **Step 5: 更新 FastAPI 模型、查询和中文 OpenAPI**

模型字段使用 Literal enums 和 `Decimal | None`。Date/symbol routes 将两个新 filter 原样传给 RPC；
seat history 和 metrics models 同步 window/coverage metadata。删除所有 DragonTiger `period_type` 引用。

- [ ] **Step 6: 重新生成并校验三份契约**

使用仓库已有 contract 生成命令更新三个 JSON；运行测试确认 operation IDs、参数、required fields、
enum 和边界一致，且带 `p_period_type` 参数的旧 DragonTiger SQL signature 不存在。

- [ ] **Step 7: 运行 API 测试并提交**

Run: `uv run pytest tests/test_public_api.py tests/test_api_contracts.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "dragon_tiger" -q`

Expected: PASS.

```text
git add supabase/migrations/20260908000200_repair_dragon_tiger_historical_coverage.sql src/market_data_center/public_api contracts tests/test_public_api.py tests/test_api_contracts.py tests/test_postgres_integration.py
git commit -m "feat: replace dragon tiger window read contracts"
```

### Task 8: 可恢复历史回补命令与运行报告

**Files:**
- Modify: `src/market_data_center/dragon_tiger_service.py`
- Modify: `src/market_data_center/persistence/dragon_tiger_postgres.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `src/market_data_center/operations_service.py`
- Test: `tests/test_dragon_tiger_service.py`
- Test: `tests/test_cli.py`
- Test: `tests/test_operations.py`
- Test: `tests/test_postgres_integration.py`

**Interfaces:**
- Produces: 730 天上限、逐日继续、数据库成功日期跳过和稳定失败列表。
- Produces: JSON stdout summary with `completed_dates`, `skipped_non_trading_dates`, `skipped_succeeded_dates`, `failed_dates`, `quality_code_counts`。

- [ ] **Step 1: 写 2025-01-01 至 2026-09-08 范围解析测试**

断言 616 日范围通过；超过 730 日拒绝。中间日期 ProviderError 后继续；最终 stdout 不含异常 message，
stderr/exit code 为 1 且 JSON 只包含 date、phase、stable error code。

- [ ] **Step 2: 写数据库恢复点测试**

Persistence 返回已由 v3 live 或 repaired replay 成功的日期集合；backfill 跳过它们。旧失败、无 run 和
只有 v1/v2 失败 Raw 的日期仍执行。不得仅因旧 run 为 succeeded 就跳过缺少 Event 的日期。

- [ ] **Step 3: 运行测试并确认失败**

Run: `uv run pytest tests/test_dragon_tiger_service.py tests/test_cli.py tests/test_operations.py -q`

Expected: FAIL because range is capped at 366 days and first error stops the loop.

- [ ] **Step 4: 实现连续回补与确定性 JSON 报告**

按 `core.trading_calendar` 顺序生成日期，不按自然日猜交易日。每个交易日独立调用 `collect()`，捕获
`DragonTigerCollectionError` 后追加 `DragonTigerBackfillFailure` 并继续。最终只要存在失败日期，CLI
以 1 退出；无失败以 0 退出。

- [ ] **Step 5: 运行单元与集成测试并提交**

Run: `uv run pytest tests/test_dragon_tiger_service.py tests/test_cli.py tests/test_operations.py -q`

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "dragon_tiger and backfill" -q`

Expected: PASS.

```text
git add src/market_data_center/dragon_tiger_service.py src/market_data_center/persistence/dragon_tiger_postgres.py src/market_data_center/cli.py src/market_data_center/operations_service.py tests/test_dragon_tiger_service.py tests/test_cli.py tests/test_operations.py tests/test_postgres_integration.py
git commit -m "feat: make dragon tiger backfill resumable"
```

### Task 9: 文档、全量质量门与生产发布

**Files:**
- Modify: `README.md`
- Modify: `docs/最小生产发布运行手册.md`
- Modify: `docs/领域详设-DragonTiger-2026-09-02.md`
- Modify: `docs/superpowers/specs/2026-09-08-dragon-tiger-historical-coverage-repair-design.md`
- Modify: `deploy/linux/market-data-center.env.example`
- Test: full repository gate

**Interfaces:**
- Consumes: Tasks 1～8 的最终 CLI、migration、Worker jobs 和 contracts。
- Produces: 可复制但不含秘密的生产命令序列和最终覆盖报告格式。

- [ ] **Step 1: 更新当前事实文档和环境示例**

文档只描述已经实现的命令和字段；加入 `SECURITY_BSE_ENABLED=false` 示例、20:15/20:30 任务顺序、
孤儿 recovery dry-run/execute、BSE sync、Raw replay、730 天回补和四个 smoke queries。明确本次 owner
放弃数据库备份，但迁移预检不可跳过。

- [ ] **Step 2: 运行格式、静态检查和全量单测**

Run:

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all commands exit 0.

- [ ] **Step 3: 运行隔离 PostgreSQL 集成测试**

Run: `uv run pytest -m integration`

Expected: PASS against `TEST_DATABASE_URL`. Never substitute production `DATABASE_URL`.

- [ ] **Step 4: 提交最终文档**

```text
git add README.md docs/最小生产发布运行手册.md docs/领域详设-DragonTiger-2026-09-02.md docs/superpowers/specs/2026-09-08-dragon-tiger-historical-coverage-repair-design.md deploy/linux/market-data-center.env.example
git commit -m "docs: publish dragon tiger repair runbook"
```

- [ ] **Step 5: 推送并执行只读生产预检**

推送当前 `master` 后，在生产主机验证目标 commit、当前 migration watermark、Worker/API service 状态、
磁盘 Raw 根目录和 Tushare token 非空；所有输出对 token/DSN 打码。运行 migration dry/preflight，确认
只应用 `20260908000200_repair_dragon_tiger_historical_coverage.sql`。

- [ ] **Step 6: 执行生产 migration 和同批服务部署**

按 `docs/最小生产发布运行手册.md` 的现有 release 目录与原子切换流程部署，不创建数据库备份。应用
migration 后重启 Worker/FastAPI，执行 health、readiness、Worker catalog 和四个 DragonTiger bounded
smoke checks；任何检查失败停止后续回补并恢复上一版应用代码，数据库只使用 migration 中定义的
前向兼容处理，不执行 destructive reset。

- [ ] **Step 7: 执行 BSE、孤儿恢复、Raw replay 和历史回补**

严格按以下顺序运行已实现 CLI：BSE 全状态同步；孤儿 Raw dry-run；孤儿 Raw execute；枚举并重放失败
DragonTiger manifests；最后从 `2025-01-01` 到最近已收盘交易日执行连续 backfill。每一步保存只含
安全计数、日期和错误码的运行结果。

- [ ] **Step 8: 验证生产覆盖并关闭 Issue 验收项**

只读查询核对：应覆盖交易日数、实际成功交易日数、Event 数、SeatTrade 数、BSE Event 数、各 trigger
window 分布、`SOURCE_UNSPECIFIED` 数、三类 warning 质量码、失败日期及错误码。随机抽取 DAY、三日、
10 日、30 日、BSE 最近三成交日和单侧披露 Event 调用四个 API。只有失败日期为零或每个残余缺口均有
明确外部原因和稳定错误码时，才报告回补完成。

## Self-Review

- Spec coverage: Tasks 1～9 分别覆盖领域窗口、来源标准化、席位身份/migration、分阶段血缘、BSE、
  replay/orphan recovery、公共契约、连续回补和生产发布；没有未分配的 spec requirement。
- Placeholder scan: 计划不包含未定义实现占位；每个实现任务都列明文件、接口、失败测试、通过命令和提交。
- Type consistency: Provider 统一返回 `DragonTigerProviderBatch`；normalizer 统一返回
  `DragonTigerNormalizationResult`；Service/Persistence 的四阶段方法和 Event window 字段在后续任务中
  使用相同名称。
