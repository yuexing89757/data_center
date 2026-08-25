# 领域详设：TradingBillboard 股票龙虎榜 v0

> 状态：有效，待实施
> 日期：2026-08-24
> 关联 Issue：#65
> 上级决策：`adr/ADR-0046-东方财富股票龙虎榜采集与只读契约.md`（Accepted）

## 1. 领域职责

`TradingBillboard` 保存东方财富发布的 A 股每日龙虎榜上榜汇总和买入/卖出前五席位，提供
来源可追溯、单位统一、可重放和有界读取的客观事实。

本领域不负责：

- 可转债龙虎榜；
- 游资身份、别名、席位对应关系或置信度；
- 营业部排行、活跃度、成功率和上榜后收益；
- 东方财富文本解读、情绪标签、交易建议或策略判断；
- 分钟、逐笔、Level-2 或请求时实时回源；
- 跨来源事件合并和事实仲裁。

## 2. 聚合与依赖

```text
TradingBillboardRecord
├── summary scalar facts
├── buy_seats: tuple[TradingBillboardSeatRecord, ...]
└── sell_seats: tuple[TradingBillboardSeatRecord, ...]

TradingBillboardSeatRecord
├── parent source_event_id
├── symbol + trade_date
├── side + rank
└── disclosed seat amounts and identity text
```

一个 `TradingBillboardRecord` 是事务和校验聚合根。Provider 不能分别发布孤立汇总或席位；
Persistence 必须在同一事务内写入聚合。

依赖方向：

```text
Ingestion ───────────────┐
Security ────────────────┼──> TradingBillboard
TradingCalendar ─────────┘

TradingBillboard ──> api_v1 bounded reads
```

本领域不依赖 DailyBar、StockDailyIndicator、Classification、Derived 或消费者标签。

## 3. `TradingBillboardRecord`

| 字段 | 类型 | 语义与约束 |
| --- | --- | --- |
| `symbol` | str | `SSE|SZSE|BSE:NNNNNN` 股票 |
| `trade_date` | date | `CN_A_SHARE` 交易所本地交易日 |
| `source_event_id` | str | 来源事件标识，非空 |
| `reason_code` | str | 来源上榜原因代码，非空 |
| `reason_text` | str | 来源上榜原因原文，非空 |
| `close_price` | Decimal \| None | CNY/股，非负 |
| `change_rate_pct` | Decimal \| None | 涨跌幅，百分点，可正可负 |
| `turnover_rate_pct` | Decimal \| None | 换手率，百分点，非负 |
| `market_amount` | Decimal \| None | 当日市场成交额，CNY，非负 |
| `buy_amount` | Decimal | 龙虎榜买入合计，CNY，非负 |
| `sell_amount` | Decimal | 龙虎榜卖出合计，CNY，非负 |
| `net_amount` | Decimal | 龙虎榜净额，CNY，可负 |
| `deal_amount` | Decimal | 龙虎榜成交合计，CNY，非负 |
| `deal_to_market_pct` | Decimal \| None | 成交额占市场成交比例，百分点，非负 |
| `net_to_market_pct` | Decimal \| None | 净额占市场成交比例，百分点，可负 |
| `free_float_market_value` | Decimal \| None | 来源发布的流通市值，CNY，非负 |
| `buy_seats` | tuple | 一至五条买入侧席位 |
| `sell_seats` | tuple | 一至五条卖出侧席位 |
| `source_code` | str | 固定 `eastmoney` |

Domain Record 不包含 `ingestion_id`。Pipeline 在校验后以
`IngestionEnvelope[TradingBillboardRecord]` 附加采集批次。

自然键为 `(source_code, source_event_id)`。同一批次内
`(symbol, trade_date, reason_code)` 也必须唯一，用于发现来源事件身份冲突；它不是跨来源统一键。

## 4. `TradingBillboardSeatRecord`

| 字段 | 类型 | 语义与约束 |
| --- | --- | --- |
| `source_event_id` | str | 所属汇总事件标识 |
| `symbol` | str | 必须与父汇总一致 |
| `trade_date` | date | 必须与父汇总一致 |
| `side` | enum | `buy` 或 `sell` |
| `rank` | int | 本侧 1～5，连续且不重复 |
| `seat_code` | str \| None | 可靠来源营业部代码；来源 `0` 标准化为空 |
| `seat_name` | str | 披露时的营业部或席位名称，非空 |
| `buy_amount` | Decimal \| None | 该席位买入额，CNY，非负 |
| `sell_amount` | Decimal \| None | 该席位卖出额，CNY，非负 |
| `net_amount` | Decimal \| None | 该席位净额，CNY，可负 |
| `buy_to_market_pct` | Decimal \| None | 买入额占市场成交比例，百分点，非负 |
| `sell_to_market_pct` | Decimal \| None | 卖出额占市场成交比例，百分点，非负 |
| `source_code` | str | 固定 `eastmoney` |

自然键为 `(source_code, source_event_id, side, rank)`。营业部代码和名称不唯一：同一营业部可
同时进入买入榜和卖出榜，多条“机构专用”也可以同侧出现。

东方财富没有独立名次字段。Normalizer 按事件和 side 分组，以对应侧金额降序排列；同额时依次
使用规范化 `seat_code`、`seat_name` 和完整来源行内容哈希。该排序只表达来源前五列表位置，
不解释交易主体身份。

## 5. 东方财富 Provider 映射

首个 Adapter 为 `EastmoneyTradingBillboardProvider`，通过固定东方财富数据中心端点分页读取：

- 每日上榜证券汇总；
- 买入席位明细；
- 卖出席位明细。

主要映射：

| 东方财富字段 | 标准字段 |
| --- | --- |
| `TRADE_ID` | `source_event_id` |
| `SECURITY_CODE` / `SECUCODE` | `symbol` |
| `TRADE_DATE` | `trade_date` |
| `CHANGE_TYPE` | `reason_code` |
| `EXPLANATION` | `reason_text` |
| `CLOSE_PRICE` | `close_price` |
| `CHANGE_RATE` | `change_rate_pct` |
| `TURNOVERRATE` | `turnover_rate_pct` |
| `ACCUM_AMOUNT` | `market_amount` |
| `BILLBOARD_BUY_AMT` | `buy_amount` |
| `BILLBOARD_SELL_AMT` | `sell_amount` |
| `BILLBOARD_NET_AMT` | `net_amount` |
| `BILLBOARD_DEAL_AMT` | `deal_amount` |
| `DEAL_AMOUNT_RATIO` | `deal_to_market_pct` |
| `DEAL_NET_RATIO` | `net_to_market_pct` |
| `FREE_MARKET_CAP` | `free_float_market_value` |
| `OPERATEDEPT_CODE` | `seat_code` |
| `OPERATEDEPT_NAME` | `seat_name` |
| `BUY` / `SELL` / `NET` | 席位买入额/卖出额/净额 |
| `TOTAL_BUYRIO` / `TOTAL_SELLRIO` | 席位买入/卖出占比 |

不映射东财 `EXPLAIN`、未来 1/2/5/10/20/30 日收益、席位成功率等解释或衍生字段。证券名称由
Security 名称历史提供，不把来源当前简称复制为标准事实。

金额直接按 JSON 的 CNY 语义保存，不根据网页“万元”展示二次换算。所有数值从 JSON 数字文本
直接构造 `Decimal`；禁止经过二进制浮点。

## 6. Raw、采集与重放

每个交易日创建一个 IngestionRun，dataset 固定为 `trading_billboard`。三个来源数据集全部分页
完成后，合并写入一个不可变 JSONL：

```text
eastmoney/trading_billboard/year=YYYY/month=MM/day=DD/<ingestion-id>.jsonl
```

Raw schema 为 `eastmoney.trading_billboard.v1`。每行保留完整来源字段，并由 Pipeline 增加
`record_kind=summary|buy_seat|sell_seat`。Manifest 记录 SHA-256、字节数、来源总行数、请求日期、
三个数据集各自的行数和分页数。

重放必须验证路径、格式、字节数、SHA-256、行数和 schema version，创建新 IngestionRun 并引用
原 RawManifest。重放不访问东方财富、不复制 Raw，并经过相同 Normalizer、Validator、Envelope
和 Persistence。

## 7. 完整性与失败语义

整个交易日必须满足：

1. 三个响应成功，分页结果覆盖各自声明总数，所有来源日期等于请求日期。
2. 汇总 `symbol` 是已知股票，trade_date 是交易日且位于 Security 生命周期内。
3. 可转债和非股票行只保留 Raw，不产生标准记录。
4. 每个接受席位匹配一个接受汇总，source_event_id、symbol 和 trade_date 完全一致。
5. 每个汇总每侧一至五条席位，rank 从 1 连续递增且不重复。
6. 汇总在 CNY 分精度满足 `deal_amount = buy_amount + sell_amount`、
   `net_amount = buy_amount - sell_amount`。
7. 席位 buy/sell 都存在时满足 `net_amount = buy_amount - sell_amount`；任一输入缺失时不推导。
8. 非净额金额和非净额百分比非负，自然键不存在冲突内容。

前五席位对应侧金额之和不要求等于汇总金额。真实来源响应不保证该等式，不能把它降格为告警或
用补值强行满足。

任一硬规则失败时，记录 QualityResult 并使整个日期失败。不得写半套汇总、孤立席位或空成功，
不得切换 Provider。

## 8. 持久化模型

内部 schema 为 `billboard`，消费者无直接权限。

### 8.1 `billboard.entry`

- `entry_id uuid` 主键；
- 聚合根全部标量字段；
- `ingestion_id`、`content_hash`、`created_at`、`updated_at`；
- unique `(source_code, source_event_id)`；
- unique `(symbol, trade_date, reason_code)`；
- Security 与统一交易日外键。

索引：

- `(trade_date, symbol, entry_id)`；
- `(symbol, trade_date desc, entry_id)`。

### 8.2 `billboard.seat`

- `entry_id`、`source_code`、`source_event_id`、`symbol`、`trade_date`；
- 席位其余字段和 `ingestion_id`；
- primary key `(entry_id, side, rank)`；
- 组合外键保证来源事件、股票和交易日与父汇总一致。

索引：

- `(entry_id, side, rank)`；
- `(symbol, trade_date desc)`；
- `(seat_code, trade_date desc, entry_id, side) where seat_code is not null`；
- `(seat_name, trade_date desc, entry_id, side)`。

相同内容重跑幂等跳过。来源内容修订时，在一个事务内更新 entry、替换其 seats 并将最新事实指向
新 ingestion；旧响应只通过 immutable Raw 与 ingestion lineage 保留，不复制为第二套当前事实。

## 9. Service、回补与 Worker

专用 `TradingBillboardProvider` capability 输出 `ProviderBatch[TradingBillboardRecord]`。
Service 提供：

- 单个明确交易日采集；
- 明确起止日期、逐日执行的有界回补。

一个日期一个事务。范围回补中某日失败不回滚此前成功日期，并返回首个失败日期和已完成日期，供
相同命令显式恢复。CLI 不提供无界全历史模式。

Workflow code 为 `trading_billboard_daily`，Job ID 为 `trading-billboard-daily`。Worker 在周一至
周五 20:30 Asia/Shanghai 触发，执行前检查 `CN_A_SHARE` 交易日历；非交易日正常跳过。连接、读取、
每页行数、最大页数和重试次数均为代码有界常量。

Operations 记录汇总数、买席位数、卖席位数、过滤非股票数、三个来源分页/行数、接受/拒绝数和
质量规则计数。任务只注册 APScheduler 代码目录，不创建 Windows Task Scheduler 或 cron。

## 10. `api_v1` 读取契约

三个 RPC 都是锁定 `search_path` 的只读 `SECURITY DEFINER`，使用 5 秒 statement timeout，只向
API 角色授予函数执行权，不授予内部表读取权；RPC 只读取数据库且不触发采集。

### 10.1 按准确交易日

```text
query_trading_billboard_by_date(p_trade_date, p_limit=100, p_offset=0)
```

不回退日期。每条结果返回汇总和按 rank 排序的 `buy_seats`、`sell_seats` 数组。

### 10.2 按股票和日期范围

```text
query_trading_billboard_by_symbol(
  p_symbol, p_start_date, p_end_date, p_limit=100, p_offset=0
)
```

要求标准股票 symbol，闭区间最多 366 个自然日。返回格式与按日期查询一致。

### 10.3 按席位和日期范围

```text
query_trading_billboard_by_seat(
  p_seat_code, p_seat_name, p_start_date, p_end_date,
  p_side=null, p_limit=100, p_offset=0
)
```

`p_seat_code` 和 `p_seat_name` 必须且只能提供一个非空值，均为精确匹配；不提供模糊搜索。
`p_side` 接受 `buy`、`sell` 或 null。无可靠代码的“机构专用”通过精确名称查询。

席位查询返回扁平席位记录，并组合所属汇总的 symbol、trade_date、reason、汇总买卖金额和
source_event_id。排序为 `(trade_date desc, symbol, entry_id, side, rank)`。

三个 RPC 的 limit 为 1～500，offset 为 0～10,000。无事实返回空集合。

## 11. FastAPI 与契约同步

FastAPI 角色只能调用上述 `api_v1` RPC，不访问 `billboard` 或其他内部 schema。提供：

- 按准确交易日查询；
- 按六位股票代码和日期范围查询；
- `GET /api/v1/trading-billboard/seats`，按席位与日期范围查询。

六位代码通过既有安全解析转换为标准 symbol。数据库错误映射为稳定外部错误，不暴露 SQL、内部
schema、Raw 路径或来源异常文本。

实现必须同步更新：

- `contracts/postgrest-openapi-v1.json`；
- `contracts/agent-tools-v1.json`；
- `contracts/fastapi-openapi-v1.json`。

## 12. 测试矩阵

- 同一股票同日多个原因、自然键冲突和来源事件冲突；
- 席位 symbol/trade_date/source_event 与父记录不一致；
- 同侧多条代码 `0` 的“机构专用”；
- 每侧 rank、同额稳定排序和 Raw 重放确定性；
- Decimal、CNY、百分点、None、零和负净额；
- 混入可转债、未知股票、生命周期和非交易日；
- 三个数据集分页、重复页、总数漂移、字段缺失、部分成功和超时；
- 幂等、修订替换、组合外键和事务回滚；
- 三个 RPC 的参数边界、范围、分页、排序、空集合、权限与超时；
- 席位代码/名称互斥、精确名称、“机构专用”和可选 side；
- FastAPI/PostgREST/Agent 契约同步；
- Worker 20:30 时区、休市跳过、Operations 记录和本地只读任务页。

PostgreSQL 测试只使用隔离 disposable `TEST_DATABASE_URL`，不得指向生产。

## 13. 数据授权与实施门禁

东方财富网页公开可访问不代表自动采集、长期保存和再分发已获授权。生产启用前，项目所有者必须
完成来源条款、相关交易信息权利和公开 API 暴露范围审阅。未完成时只允许 mocked 测试和受控
只读验证，不注册启用生产任务。

Issue #65、Accepted ADR-0046 和本有效详设共同约束实现。只有完成实施计划、ordered migration、
测试和来源授权审阅后，才能进入生产发布流程。
