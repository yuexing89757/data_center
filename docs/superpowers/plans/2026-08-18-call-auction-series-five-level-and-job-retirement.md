# 沪深竞价序列五档扩展与涨停池采集任务退役 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在沪深全市场开盘竞价序列快照中保存确定批次编码和完整买卖五档，同时退役集合竞价涨停池五档采集任务及其 pysnowball 运行时代码。

**Architecture:** 保留现有 PYTDX `get_security_quotes`、32 轮调度和分区事实表，在 Provider 边界保留零价正数量，并把固定五档从领域记录展开到现有序列快照父表。使用一个有序迁移扩展父表和 RPC；FastAPI 只扩展既有响应项。涨停池任务从受控 job catalog 和 scheduler 中移除，但历史 workflow、表、迁移、Raw、ProviderCode 和恢复能力保留。

**Tech Stack:** Python 3.12、uv、Decimal、SQLAlchemy、PostgreSQL 分区表、PL/pgSQL、FastAPI/Pydantic、pytest、ruff、mypy。

**Spec:** `docs/superpowers/specs/2026-08-18-call-auction-series-five-level-and-job-retirement-design.md`

## Global Constraints

- GitHub Issue 是任务规划的唯一事实源；实施前必须创建或关联本变更的 Issue。
- 所有生产 Schema 变化只允许进入新的 `supabase/migrations/*.sql` 有序迁移。
- 不改变 09:15:00–09:25:20、20 秒、32 轮、每批 80 只和沪深全市场范围。
- 零价正数量保存为 `price=None, volume=实际股数`；零价零数量保存为整档缺失。
- `batch_code` 必须由 `scheduled_at` 的上海时间生成，格式固定为六位 `HHMMSS`。
- 历史只回填 `batch_code`；不得从 Raw 重放或推算历史五档。
- 删除运行时任务不得删除历史 workflow code、表、迁移、Raw、记录或查询能力。
- 不把 `.env`、Cookie、数据库 URL、Raw 或生产数据提交到 Git。
- PostgreSQL 集成测试只使用显式隔离的 `TEST_DATABASE_URL`，不得指向生产。
- 本计划不授权生产迁移、部署、服务重启或生产 `.env` 修改。

---

## File Map

- `src/market_data_center/domain/realtime_quote.py`: 通用五档值对象，允许缺价正数量。
- `src/market_data_center/providers/pytdx_hq.py`: PYTDX 价量规范化及手转股。
- `src/market_data_center/domain/call_auction_market_series.py`: 批次编码、序列五档领域字段和校验。
- `src/market_data_center/call_auction_market_series_service.py`: 从 PYTDX 标准快照复制五档并生成批次。
- `src/market_data_center/persistence/call_auction_market_series_postgres.py`: 扁平五档写入参数和 SQL。
- `supabase/migrations/20260818000100_enrich_call_auction_market_series.sql`: 列、回填、约束和 RPC 替换。
- `src/market_data_center/public_api/models.py`: FastAPI 新响应字段。
- `contracts/fastapi-openapi-v1.json`: 运行时导出的 FastAPI 契约。
- `contracts/agent-tools-v1.json`, `contracts/postgrest-openapi-v1.json`: 同步公开工具说明与 RPC 契约说明。
- `src/market_data_center/scheduling_catalog.py`, `src/market_data_center/scheduler.py`: 移除涨停池 job 和 runner。
- `src/market_data_center/settings.py`, `.env.example`: 移除 pysnowball 与任务启用配置。
- `src/market_data_center/providers/pysnowball_quote.py`: 删除。
- `tests/test_pysnowball_quote_provider.py`: 删除。
- `docs/adr/ADR-0012-股票实时五档行情.md`: 澄清缺价正数量语义及 pysnowball 退役。
- `docs/领域详设-RealtimeQuote-2026-08-02.md`: 更新 `OrderBookLevel` 不变量。
- `docs/领域详设-CallAuctionMarketSeries-2026-08-14.md`: 记录批次和五档字段。
- `README.md`: 更新任务清单和序列 API 说明。

### Task 1: 建立治理记录并澄清领域决策

**Files:**
- Modify: `docs/adr/ADR-0012-股票实时五档行情.md`
- Modify: `docs/领域详设-RealtimeQuote-2026-08-02.md`
- Modify: `docs/领域详设-CallAuctionMarketSeries-2026-08-14.md`

**Interfaces:**
- Consumes: 已批准 Spec 中的批次、五档和退役边界。
- Produces: 后续代码与迁移必须遵守的 `price=None, volume>0` 和 `batch_code=HHMMSS(scheduled_at)` 规则。

- [ ] **Step 1: 关联 GitHub Issue**

运行：

```bash
gh issue list --state all --search "竞价序列 五档 涨停池任务退役" --limit 20
```

如果没有对应 Issue，创建：

```bash
gh issue create \
  --title "扩展开盘竞价序列五档并退役涨停池采集任务" \
  --body "实现已批准设计 docs/superpowers/specs/2026-08-18-call-auction-series-five-level-and-job-retirement-design.md。范围：序列快照 batch_code 与完整五档、零价正数量保留、查询契约同步、退役 opening-auction-limit-up-quotes 和 pysnowball 运行时；保留历史事实。"
```

Expected: 得到一个可引用的 Issue 编号；不得创建 Linear 任务。

- [ ] **Step 2: 更新 ADR 的价量语义与 Provider 状态**

在 ADR-0012 的决策中明确：

```text
档位价格存在时数量必须存在；集合竞价来源允许价格缺失而数量存在。
来源零价格、正数量规范化为 NULL 价格和实际股数；零价格、零数量为空档。
PYTDX 是当前运行时五档 Provider；pysnowball 历史身份保留但不再注册运行时 Adapter。
```

不得把零价格称为有效盘口价格，不得改变历史 `provider_code='pysnowball'` 的合法性。

- [ ] **Step 3: 更新两个领域详设**

在 RealtimeQuote 详设中把 `OrderBookLevel` 状态写成三种合法组合；在 CallAuctionMarketSeries 详设中列出 `batch_code` 和 20 个扁平数据库/API 字段，明确历史五档为空。

- [ ] **Step 4: 检查文档无占位和冲突**

运行：

```bash
rg -n "TBD|TODO|待定|价格和数量同时为空|pysnowball.*自动" docs/adr/ADR-0012-股票实时五档行情.md docs/领域详设-RealtimeQuote-2026-08-02.md docs/领域详设-CallAuctionMarketSeries-2026-08-14.md
git diff --check
```

Expected: 无 TBD/TODO；旧的“价格和数量必须同时为空”及 pysnowball 自动路由表述均被纠正；`git diff --check` 成功。

- [ ] **Step 5: Commit**

```bash
git add docs/adr/ADR-0012-股票实时五档行情.md docs/领域详设-RealtimeQuote-2026-08-02.md docs/领域详设-CallAuctionMarketSeries-2026-08-14.md
git commit -m "docs: clarify auction volume-only quote levels"
```

### Task 2: 用 TDD 保留 PYTDX 零价正数量

**Files:**
- Modify: `tests/test_realtime_quote.py`
- Modify: `tests/test_pytdx_hq_provider.py`
- Modify: `src/market_data_center/domain/realtime_quote.py`
- Modify: `src/market_data_center/providers/pytdx_hq.py`

**Interfaces:**
- Consumes: `OrderBookLevel(level: int, price: Decimal | None, volume: int | None)`。
- Produces: `OrderBookLevel` 允许 `(None, positive_volume)`；`PytdxHqProvider.fetch_five_level_quotes()` 保留零价正数量并完成手转股。

- [ ] **Step 1: 写领域失败测试**

在 `tests/test_realtime_quote.py` 增加：

```python
def test_order_book_level_preserves_source_volume_without_a_valid_price() -> None:
    level = OrderBookLevel(2, None, 10_743_200)

    assert level.price is None
    assert level.volume == 10_743_200


def test_order_book_level_rejects_price_without_volume() -> None:
    with pytest.raises(ValueError, match="price requires volume"):
        OrderBookLevel(2, Decimal("7.10"), None)
```

生产变异检查：恢复旧的“价量必须同时出现”应使第一个测试失败；允许正价无量应使第二个测试失败。

- [ ] **Step 2: 运行领域测试确认 RED**

Run:

```bash
uv run pytest tests/test_realtime_quote.py::test_order_book_level_preserves_source_volume_without_a_valid_price tests/test_realtime_quote.py::test_order_book_level_rejects_price_without_volume -q
```

Expected: 第一个测试因现有 pair invariant 抛 `ValueError`；不是 import、fixture 或拼写错误。

- [ ] **Step 3: 最小修改 `OrderBookLevel`**

将成对校验改为单向约束：

```python
if self.price is not None and self.volume is None:
    raise ValueError("order-book price requires volume")
```

保留 Decimal、正价格、非负数量校验。`_validate_levels()` 继续只对存在的正价格做连续性和排序校验。

- [ ] **Step 4: 运行领域测试确认 GREEN**

Run: `uv run pytest tests/test_realtime_quote.py -q`

Expected: PASS。

- [ ] **Step 5: 写 PYTDX 失败测试**

在 `tests/test_pytdx_hq_provider.py` 增加一个 `RecordedClient` 变体，返回：

```python
row["bid2"] = Decimal("0")
row["bid_vol2"] = 107_432
row["ask2"] = Decimal("0")
row["ask_vol2"] = 133
row["bid3"] = Decimal("0")
row["bid_vol3"] = 0
```

断言真实 Provider 输出：

```python
assert quote.bid_levels[1] == OrderBookLevel(2, None, 10_743_200)
assert quote.ask_levels[1] == OrderBookLevel(2, None, 13_300)
assert quote.bid_levels[2] == OrderBookLevel(3, None, None)
```

生产变异检查：恢复 `if price is not None else None` 应使买二和卖二断言失败；忘记乘 100 应使股数断言失败。

- [ ] **Step 6: 运行 Provider 测试确认 RED**

Run: `uv run pytest tests/test_pytdx_hq_provider.py -q`

Expected: 新测试显示买二/卖二 volume 为 `None`。

- [ ] **Step 7: 最小修改 PYTDX `_level()`**

实现精确分支：

```python
price = _optional_price(row[f"{side}{level}"])
volume = _lots_to_shares(row[f"{side}_vol{level}"])
if price is None and volume == 0:
    return OrderBookLevel(level, None, None)
return OrderBookLevel(level, price, volume)
```

不得修改网络请求批量大小、协议命令或其他 Provider。

- [ ] **Step 8: 运行聚焦测试确认 GREEN**

Run:

```bash
uv run pytest tests/test_realtime_quote.py tests/test_pytdx_hq_provider.py -q
```

Expected: PASS。

- [ ] **Step 9: Commit**

```bash
git add src/market_data_center/domain/realtime_quote.py src/market_data_center/providers/pytdx_hq.py tests/test_realtime_quote.py tests/test_pytdx_hq_provider.py
git commit -m "fix: preserve auction volume-only quote levels"
```

### Task 3: 用 TDD 扩展序列领域记录和采集服务

**Files:**
- Modify: `tests/test_call_auction_market_series.py`
- Modify: `tests/test_call_auction_market_series_service.py`
- Modify: `src/market_data_center/domain/call_auction_market_series.py`
- Modify: `src/market_data_center/call_auction_market_series_service.py`

**Interfaces:**
- Consumes: Task 2 的 `OrderBookLevel` volume-only 语义。
- Produces: `series_batch_code(scheduled_at: datetime) -> str`；`MarketSeriesSnapshotRecord.batch_code: str`、`bid_levels`、`ask_levels`；Service 将 Provider 五档原样复制。

- [ ] **Step 1: 写批次编码失败测试**

在 `tests/test_call_auction_market_series.py` 使用手工字面量断言：

```python
@pytest.mark.parametrize(
    ("sample_seq", "expected"),
    [(0, "091500"), (1, "091520"), (2, "091540"), (31, "092520")],
)
def test_series_batch_code_uses_scheduled_shanghai_slot(sample_seq: int, expected: str) -> None:
    assert series_batch_code(series_slots(date(2026, 8, 18))[sample_seq]) == expected
```

再构造 `MarketSeriesSnapshotRecord(batch_code="091501", ...)`，断言因不匹配 `scheduled_at` 被拒绝。

- [ ] **Step 2: 运行确认 RED**

Run: `uv run pytest tests/test_call_auction_market_series.py -q`

Expected: import `series_batch_code` 失败或新字段不存在。

- [ ] **Step 3: 实现批次函数和领域字段**

新增：

```python
def series_batch_code(scheduled_at: datetime) -> str:
    _require_utc(scheduled_at, "scheduled_at")
    return scheduled_at.astimezone(SHANGHAI_ZONE).strftime("%H%M%S")
```

`MarketSeriesSnapshotRecord` 增加必填 `batch_code`、固定五档 `bid_levels`、`ask_levels`，校验：

- batch 等于 `series_batch_code(scheduled_at)`；
- 两组都恰好包含 level 1..5；
- `OrderBookLevel` 自身负责价量类型和非负性。

- [ ] **Step 4: 更新既有领域测试 fixture 并确认 GREEN**

所有构造 `MarketSeriesSnapshotRecord` 的测试使用真实五档 tuple 和字面量批次值。Run: `uv run pytest tests/test_call_auction_market_series.py -q`。Expected: PASS。

- [ ] **Step 5: 写 Service 失败测试**

在现有 fake Provider 返回中设置买二 `OrderBookLevel(2, None, 10_743_200)` 和卖二 `OrderBookLevel(2, None, 13_300)`。从 fake persistence 的 `commit_attempt()` 捕获 records，断言首轮记录：

```python
assert record.batch_code == "091500"
assert record.bid_levels[1].volume == 10_743_200
assert record.bid_levels[1].price is None
assert record.ask_levels[1].volume == 13_300
```

生产变异检查：不复制五档或使用 observed_at 生成批次都会使断言失败。

- [ ] **Step 6: 运行 Service 测试确认 RED**

Run: `uv run pytest tests/test_call_auction_market_series_service.py -q`

Expected: record 缺少 `batch_code`/五档或值未复制。

- [ ] **Step 7: 最小修改 `_series_record()`**

传入：

```python
batch_code=series_batch_code(round_state.scheduled_at),
bid_levels=quote.bid_levels,
ask_levels=quote.ask_levels,
```

不改变 `_series_values()` 的 09:25 前竞价指示值语义。

- [ ] **Step 8: 运行序列单元测试确认 GREEN**

Run:

```bash
uv run pytest tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py -q
```

Expected: PASS。

- [ ] **Step 9: Commit**

```bash
git add src/market_data_center/domain/call_auction_market_series.py src/market_data_center/call_auction_market_series_service.py tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py
git commit -m "feat: carry five-level auction series facts"
```

### Task 4: 用迁移和集成测试持久化批次与五档

**Files:**
- Create: `supabase/migrations/20260818000100_enrich_call_auction_market_series.sql`
- Modify: `src/market_data_center/persistence/call_auction_market_series_postgres.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**
- Consumes: Task 3 的 `MarketSeriesSnapshotRecord`。
- Produces: 分区父表列、约束、历史 batch 回填、五档写入和扩展后的同签名 RPC。

- [ ] **Step 1: 写迁移/持久化失败集成测试**

扩展现有 series persistence fixture，提交一条：

```text
batch_code=091500
bid1=(10.00, 100)
bid2=(NULL, 10_743_200)
ask1=(10.00, 100)
ask2=(NULL, 13_300)
```

随后直接查询父表，断言 batch、bid2/ask2 volume 和 NULL price。再查询 `information_schema.columns`，断言父表及 `202608` 分区都有 20 个五档列和 `batch_code`。

增加约束测试，分别尝试 `batch_code='091501'`、负 volume、正 price 配 NULL volume，断言 PostgreSQL 拒绝。

- [ ] **Step 2: 运行集成测试确认 RED**

Run:

```bash
uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market_series" -q
```

Expected: 缺少 `batch_code`/五档列或 persistence 参数，测试失败。若 `TEST_DATABASE_URL` 未设置，记录为阻塞，不得改用生产 URL。

- [ ] **Step 3: 编写有序迁移**

迁移必须：

```sql
alter table realtime.call_auction_market_series_snapshot
  add column batch_code char(6),
  add column bid1_price numeric(18,4),
  add column bid1_volume bigint,
  -- 明确列出 bid2..bid5、ask1..ask5
  add column ask5_volume bigint;

update realtime.call_auction_market_series_snapshot
set batch_code = to_char(scheduled_at at time zone 'Asia/Shanghai', 'HH24MISS')
where batch_code is null;

alter table realtime.call_auction_market_series_snapshot
  alter column batch_code set not null;
```

添加并 validate：六位格式、batch 与 scheduled_at 相等、每个 price 为 NULL 或大于 0、每个 volume 为 NULL 或非负、每个 price 非 NULL 时对应 volume 非 NULL。不要更新任何历史五档列。

同一迁移用 `create or replace function api_v1.query_call_auction_market_series_snapshots(date,text[])` 扩展 item JSON；参数、权限、search_path 和 5 秒 timeout 不变。

- [ ] **Step 4: 扩展 persistence 参数与 INSERT**

`_snapshot_parameters()` 从 `bid_levels[index]`/`ask_levels[index]` 明确展开 20 个参数，并在 INSERT 列和值中保持同序。不得用动态 SQL。

- [ ] **Step 5: 更新静态生产检查**

在 `tests/test_production_checks.py` 断言新迁移：

- 不创建 OS 定时任务；
- 只更新 `batch_code`，没有历史五档 UPDATE；
- RPC 仍有固定 boundary、security definer、search_path、statement timeout 和仅 API role execute。

- [ ] **Step 6: 运行集成与生产检查确认 GREEN**

Run:

```bash
uv run pytest tests/test_production_checks.py -q
uv run pytest -m integration tests/test_postgres_integration.py -k "call_auction_market_series" -q
```

Expected: PASS；未配置隔离数据库时第二条明确报告 skipped/blocked，不以生产验证替代。

- [ ] **Step 7: Commit**

```bash
git add supabase/migrations/20260818000100_enrich_call_auction_market_series.sql src/market_data_center/persistence/call_auction_market_series_postgres.py tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: persist five-level auction series snapshots"
```

### Task 5: 扩展 FastAPI 与检入契约

**Files:**
- Modify: `src/market_data_center/public_api/models.py`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_api_contracts.py`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/postgrest-openapi-v1.json`

**Interfaces:**
- Consumes: Task 4 RPC item JSON 中的 `batch_code` 和 20 个字段。
- Produces: `CallAuctionMarketSeriesSnapshotItem` 的稳定响应模型和同步契约。

- [ ] **Step 1: 写 API 失败测试**

扩展 `tests/test_public_api.py::test_call_auction_market_series_snapshots_return_rounds_in_one_session` 的 fake RPC payload，加入：

```python
"batch_code": "091500",
"bid1_price": "10.0000",
"bid1_volume": 100,
"bid2_price": None,
"bid2_volume": 10_743_200,
"ask1_price": "10.0000",
"ask1_volume": 100,
"ask2_price": None,
"ask2_volume": 13_300,
# 其余档位完整出现并为 None 或字面量
```

断言 HTTP JSON 精确保留 volume-only 组合且 rounds 时间正序。

- [ ] **Step 2: 运行确认 RED**

Run: `uv run pytest tests/test_public_api.py -k "call_auction_market_series" -q`

Expected: Pydantic 响应模型丢弃新字段或断言缺字段。

- [ ] **Step 3: 扩展 Pydantic 模型**

新增 `BatchCode = Annotated[str, Field(pattern=r"^[0-9]{6}$")]`，在
`CallAuctionMarketSeriesSnapshotItem` 添加 batch 和 20 个可空字段。所有 volume 使用
`Field(default=None, ge=0)`；价格使用 `Decimal | None`。

- [ ] **Step 4: 运行 API 测试确认 GREEN**

Run: `uv run pytest tests/test_public_api.py -k "call_auction_market_series" -q`

Expected: PASS。

- [ ] **Step 5: 写契约失败测试并导出 FastAPI OpenAPI**

在 `tests/test_api_contracts.py` 断言 series item Schema 包含 `batch_code`、`bid2_volume`、`ask5_price`，且 batch pattern 为六位、volume minimum 为 0。

先运行确认 RED：

```bash
uv run pytest tests/test_api_contracts.py -q
```

然后运行：

```bash
uv run python scripts/export_fastapi_openapi.py
```

手工同步 Agent/PostgREST 对该 RPC 的字段说明，不改变参数和权限描述。

- [ ] **Step 6: 运行契约测试确认 GREEN**

Run:

```bash
uv run pytest tests/test_api_contracts.py tests/test_public_api.py -q
```

Expected: PASS；检入 FastAPI contract 与运行时完全一致。

- [ ] **Step 7: Commit**

```bash
git add src/market_data_center/public_api/models.py tests/test_public_api.py tests/test_api_contracts.py contracts/fastapi-openapi-v1.json contracts/agent-tools-v1.json contracts/postgrest-openapi-v1.json
git commit -m "feat: expose auction series five-level fields"
```

### Task 6: 退役涨停池采集任务并完成发布门禁

**Files:**
- Delete: `src/market_data_center/providers/pysnowball_quote.py`
- Delete: `tests/test_pysnowball_quote_provider.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `.env.example`
- Modify: `tests/test_scheduler.py`
- Modify: `tests/test_production_checks.py`
- Modify: `README.md`

**Interfaces:**
- Consumes: 历史 `ProviderCode.PYSNOWBALL`、`WorkflowCode.AUCTION_COLLECTION` 和数据库表必须保留。
- Produces: Worker 不再注册 `opening-auction-limit-up-quotes`，运行时代码不再需要雪球 Cookie。

- [ ] **Step 1: 写任务目录失败测试**

修改 scheduler/catalog 测试，通过真实 `job_definitions(SchedulerSettings(...))` 和构建后的 APScheduler 断言：

```python
assert "opening-auction-limit-up-quotes" not in {job.code for job in definitions}
assert scheduler.get_job("opening-auction-limit-up-quotes") is None
assert scheduler.get_job(CALL_AUCTION_MARKET_SERIES_JOB_ID) is not None
```

同时断言历史 `WorkflowCode.AUCTION_COLLECTION.value == "auction_collection"` 仍可解析；不要用 grep 代替行为测试。

- [ ] **Step 2: 运行确认 RED**

Run: `uv run pytest tests/test_scheduler.py -q`

Expected: catalog 仍包含该 job，测试失败。

- [ ] **Step 3: 最小移除 Worker job**

删除：

- `AUCTION_COLLECTION_JOB_ID` 常量和对应 `JobDefinition`；
- `run_auction_collection_job()`；
- scheduler 中 Pysnowball imports、handler map 和 morning executor 特例中的该 job；
- `SchedulerSettings.auction_collection_enabled`；
- `.env.example` 中 `AUCTION_COLLECTION_ENABLED` 和 `PYSNOWBALL_TOKEN`。

保留 workflow definition、WorkflowCode、ProviderCode、数据库迁移和 stale session recovery。

- [ ] **Step 4: 删除 pysnowball Adapter 和专用测试**

删除两个明确文件，不删除 `auction_service.py`、`persistence/auction_postgres.py` 或历史查询 SQL。检查所有 import：

```bash
rg -n "Pysnowball|PYSNOWBALL_TOKEN|auction_collection_enabled|opening-auction-limit-up-quotes" src tests .env.example README.md
```

Expected: 只允许历史文档/迁移/ProviderCode 中必要的身份记录；运行时代码无匹配。

- [ ] **Step 5: 更新 README 和生产检查**

README 任务清单删除涨停池采集任务，序列接口说明增加 batch 和五档。生产检查继续断言调度只能来自 Worker，且发布包不依赖 pysnowball secret。

- [ ] **Step 6: 运行退役相关测试确认 GREEN**

Run:

```bash
uv run pytest tests/test_scheduler.py tests/test_production_checks.py tests/test_settings.py -q
```

若不存在 `tests/test_settings.py`，使用 `rg --files tests | rg "settings"` 找到实际 settings 测试文件并运行；不得创建无行为价值的 change-detector 测试。

Expected: PASS；序列 job 仍启用，涨停池 job 不存在。

- [ ] **Step 7: 执行完整本地门禁**

Run:

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: 四条命令全部 exit 0；pytest 报告 0 failed。集成测试未配置时必须明确列出 skipped 数量和原因。

- [ ] **Step 8: 检查工作树和秘密**

Run:

```bash
git diff --check
git status --short
git diff -- .env
rg -n "xq_a_token=|xq_r_token=|PYSNOWBALL_TOKEN=.*;" --glob "!.env" .
```

Expected: `.env` 未进入 diff；仓库无 Cookie；只有本计划涉及的文件变化。

- [ ] **Step 9: Commit**

```bash
git add -A src/market_data_center/providers/pysnowball_quote.py tests/test_pysnowball_quote_provider.py src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py .env.example tests/test_scheduler.py tests/test_production_checks.py README.md
git commit -m "refactor: retire limit-up auction quote job"
```

- [ ] **Step 10: 最终提交审计**

Run:

```bash
git status --short
git log --oneline --decorate -8
git diff HEAD~6..HEAD --stat
```

Expected: 工作树干净；提交顺序与六个任务一致。不要 push、迁移生产或部署，除非用户随后明确授权。
