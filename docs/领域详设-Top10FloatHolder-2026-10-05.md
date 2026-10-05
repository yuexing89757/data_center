# 领域详设：Top10FloatHolder

- 状态：已接受，已实现
- 日期：2026-10-05
- ADR：ADR-0064（前置 ADR-0047）

## 1. 边界与依赖

`Top10FloatHolder` 表达某标准证券在某公告披露的前十大流通股东名单：报告截止日、公告日、
名次、持有人名称与持股事实。它依赖 Security 和 Ingestion，不依赖 Trading Calendar、
Capital 或 StockDailyIndicator。

```text
Ingestion ───────────────► Top10FloatHolder
Security ────────────────► Top10FloatHolder
```

首版不包含十大股东（限售）、股东增减持、户均持股、筹码集中度或主观标签，也不引入第二
来源。

## 2. 领域记录

```python
@dataclass(frozen=True, slots=True)
class Top10FloatHolderRecord:
    symbol: str
    end_date: date
    announcement_date: date
    holder_rank: int
    holder_name: str
    hold_amount: int
    hold_ratio: Decimal | None
    hold_float_ratio: Decimal | None
    hold_change: int | None
    holder_type: str | None
    source_code: str
```

标准记录不包含 `ingestion_id`。Pipeline 通过 `IngestionEnvelope` 附加采集血缘，
Persistence 首次插入时生成 `first_observed_at`。

不变量：

- symbol 必须存在于 `core.security`；
- `1 <= holder_rank <= 10`；
- `hold_amount > 0`（单位：股）；
- `end_date <= announcement_date`；
- `holder_name` 非空白；
- `hold_ratio`、`hold_float_ratio` 为空或 >= 0（来源百分比数值，Decimal）；
- `hold_change` 为空或整数股数；
- 首期 `source_code = 'tushare'`；
- 批内 `(symbol, end_date, announcement_date, holder_rank)` 唯一；
- 批内 `(symbol, end_date, announcement_date, holder_name)` 唯一。

名次或名称冲突时冲突对整体隔离进入质量拒绝（`top10_float_holder.invalid_record`，
ERROR/FAILED），不静默择一；因为存储层名称唯一键没有 ON CONFLICT 目标，分区必须把
矛盾行全部拦在 Core 之外。

## 3. Provider 与 Raw

Tushare `top10_floatholders`（经 `TUSHARE_ENDPOINT` 配置的兼容端点）映射如下：

| 来源字段 | 标准字段 |
| --- | --- |
| `ts_code` | `symbol` |
| `ann_date` | `announcement_date` |
| `end_date` | `end_date` |
| `holder_name` | `holder_name` |
| `hold_amount` | `hold_amount`（派生名次输入） |
| `hold_ratio` | `hold_ratio` |
| `hold_float_ratio` | `hold_float_ratio` |
| `hold_change` | `hold_change` |
| `holder_type` | `holder_type` |

来源无名次字段。adapter 在 `(ts_code, end_date, ann_date)` 组内按 `hold_amount` 降序、
`holder_name` 升序确定性派生 `holder_rank` 1..10；组内超过 10 行抛 ProviderError 硬失败。
该推导只依赖 Raw 行，重放结果一致。

`hold_amount` 是整股数，来源以 JSON float 表示（`945568990.0`）：解析接受整数值的 float
序列化，拒绝真正的小数部分（如 `.45`）。`hold_amount` 为 NULL/空白时该行完整保留在
Raw，不生成 Core 记录，不使批次失败。`hold_ratio`/`hold_float_ratio`/`hold_change`/
`holder_type` 允许缺失，标准化为 `None`。Raw schema 固定为
`tushare.top10_float_holder.v1`；每个实际请求各保存一份 JSONL Raw、SHA-256、请求参数
和 manifest；Token 不属于请求参数。

单次响应上限 6000 行。状态机为：

```text
少于 6000 行                       -> 接受
全市场单日等于 6000 行              -> 该请求作废（INFO response_split），按公告日之前
                                      最近 8 个季度末 period 逐期拆分
单个 period 请求等于 6000 行         -> 硬失败（remains truncated）
```

`period` 参数必须是 8 位日期字符串，否则请求前拒绝。合法空响应成功并产生零记录。权限、
限流、字段缺失和无法证明完整性都抛出可诊断但不含秘密的 ProviderError。请求节奏复用
股东接口限速器（`TUSHARE_SHAREHOLDER_COUNT_MAX_CALLS_PER_MINUTE`，默认 180，共享计数）。

## 4. 采集应用服务

每周同步以执行日及此前 8 个自然日（共 9 日）为公告日期窗口：逐日一个全市场请求，覆盖
上一整周并留余量。手动补采 `market-data-center top10-float-holders --start-date
--end-date` 使用同一 `sync_range`，默认窗口为当日回溯 9 日；`--end-date` 不得早于
`--start-date`。

每个真实请求分别创建 IngestionRun 和 Raw manifest；全部请求准备完成后在一个数据库事务
中发布 manifests、质量结果、全部事实并终结运行。任一请求失败（含拆分后仍截断）时，
已准备批次统一置 `failed`、登记 Raw 与 `top10_float_holder.batch_aborted`（ERROR/
FAILED）但不写 Core。

满额作废的全日请求保留 Raw 与 INFO `top10_float_holder.response_split`，`accepted_rows`
置 0；其 `fetched_rows`/`rejected_rows` 照实进入汇总。汇总（`Top10FloatHolderSyncSummary`）
校验计数非负、`accepted + rejected <= fetched`、`superseded <= requests`。

## 5. 调度与开关

每周六 08:00 Asia/Shanghai 运行 Job `top10-float-holders-weekly`
（`day_of_week='sat', hour=8, minute=0`），Workflow `top10_float_holders_weekly`，
step `collect_top10_float_holders`。开关 `TOP10_FLOAT_HOLDERS_WEEKLY_ENABLED` 默认
false，首次生产窗口探测通过后显式启用。所有调度只在 Worker 进程内，不注册任何操作
系统级触发器。

## 6. 存储

`core.top10_float_holder` 字段：

```text
symbol text
end_date date
announcement_date date
holder_rank smallint
holder_name text
hold_amount bigint
hold_ratio numeric(10, 4)
hold_float_ratio numeric(10, 4)
hold_change bigint
holder_type text
source_code text
ingestion_id uuid
first_observed_at timestamptz
```

主键为 `(symbol, end_date, announcement_date, holder_rank)`；唯一键
`(symbol, end_date, announcement_date, holder_name)`。CHECK 约束镜像领域不变量
（名次 1..10、持股为正、`end_date <= announcement_date`、比例非负、名称非空白、
`source_code = 'tushare'`）。表启用 RLS；Worker 只有 SELECT/INSERT；持久化对主键冲突
`ON CONFLICT DO NOTHING`，追加而不覆盖，保留既有行的 `ingestion_id` 与
`first_observed_at`。索引为：

```text
(announcement_date)
(symbol, end_date desc, announcement_date desc)
(ingestion_id)
```

## 7. 公共查询

首期不新增 `api_v1` 视图或 RPC。公开只读契约（严格 as-of / current-known、双时间门禁、
有界参数）待消费方分析需求明确后按宪法流程另行决策；在此之前 `core.top10_float_holder`
不是稳定公共契约。

## 8. 质量与测试

测试覆盖领域不变量与冲突对隔离、名次派生与 Raw 重放一致、float 股数解析（`.0` 接受、
真小数拒绝）、来源空值 Raw 保留、period 校验、6000 行拆分与拆分仍满额失败、多请求单事务
发布、批内自然键/manifest/信封预检、abort 不发布事实、每周窗口与手动 CLI、Scheduler/
Operations、数据库约束/RLS/最小授权清单与追加幂等（重复批次不重复不覆盖）。

实施不执行生产 migration、生产采集或凭据变更。PostgreSQL integration 只使用隔离的
`TEST_DATABASE_URL`。
