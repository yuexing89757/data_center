# 沪深全市场竞价序列 09:25 前买一价量额语义设计

- 日期：2026-08-17
- 状态：Draft for owner review
- 跟踪：GitHub Issue #53
- 影响：ADR-0034、全市场竞价序列领域模型、PostgreSQL、FastAPI 读取契约

## 问题与目标

2026-08-17 生产 live gate 证明 `call-auction-market-series` 的 32 轮、冻结全集和
单节点完整性均正常，但 pytdx 在 09:15:00–09:24:59 的普通行情字段中返回
`price/open/high/low=0`。同一 Raw 行的 `bid1`、`ask1` 和买一量承载竞价阶段可用值，
导致当前标准表在竞价结束前只有昨收和来源的零成交量/填充值金额，没有用户需要的价格、量、额。

本变更只修正该既有任务的标准化语义，不改变任务时间、32 个采样点、20 秒 cadence、
每批 80 只、冻结全集、endpoint 选择、Raw、重试或 09:26 快照任务。

## 已确认规则

以持久化 Round 的 `scheduled_at` 转为 `Asia/Shanghai` 后判断边界，不使用 Worker 收齐时间或
不可靠的来源时钟：

- `scheduled_at < 09:25:00`：
  - `last_price = bid1.price`；
  - `cumulative_volume = bid1.volume`，Provider 已把手乘 100 转为股；
  - `cumulative_amount = bid1.price * bid1.volume`，单位为 CNY；
  - `high_price`、`low_price` 继续保持普通行情来源的规范化结果，当前来源为零时即 `NULL`；
  - 买一价或买一量任一缺失时，`last_price`、`cumulative_volume`、
    `cumulative_amount` 三者全部为 `NULL`，不补零、不用卖一或昨收回退。
- `scheduled_at >= 09:25:00`：继续原样使用 pytdx 的 `price`、累计成交量和累计成交额。

不要求 `bid1=ask1` 才使用买一价，因为项目所有者明确指定“按买一价、买一量”；卖一数据仍只存在于
不可变 Raw 和上游五档 DTO，不参与这三个标准字段的计算。

## 显式语义标识

复用既有字段是项目所有者确认的兼容性选择，因此每条新事实必须增加非空
`value_semantics`：

- `auction_indicative`：09:25 前按买一价、买一量形成的值；
- `opening_trade`：09:25 起的来源开盘成交字段；
- `legacy_source_quote`：迁移前已经存在的历史行。

ordered migration 为已有行设置 `legacy_source_quote`，但不修改其价格、量或金额，也不从 Raw
回填历史。迁移完成后移除列默认值，后续写入必须显式提供语义，防止调用方忘记分类。

## 数据流与职责

`pytdx_hq` Provider 继续只负责来源字段规范化、Decimal 和手到股的单位转换；其普通五档 DTO
已经携带买一价和买一量，无需修改 Raw schema。竞价序列服务的 `_to_snapshot` 是选择任务专用语义的
边界：它根据 Round 计划时间，从 DTO 选择买一值或来源成交值，并构造带
`value_semantics` 的 `MarketSeriesSnapshotRecord`。

领域记录验证以下不变量：语义值只能来自受控枚举；`auction_indicative` 的价、量、额必须三者同空或
三者齐全，齐全时金额精确等于价格乘数量；`opening_trade` 保持现有非负和 OHLC 范围规则。
Persistence 只负责显式写入，不重新计算。

## PostgreSQL 与公共读取契约

新增 ordered migration：

1. 为父分区表增加 `value_semantics text`，给历史行标记 `legacy_source_quote`，设置非空和枚举约束，
   再移除默认值；各分区继承父表列和约束。
2. 替换 `api_v1.query_call_auction_market_series_snapshots(date,text[])`，在每个 item 中返回
   `value_semantics`；现有参数边界、session 选择、5 秒 timeout 和权限不变。

FastAPI item 模型新增同名 Literal 字段并同步 `contracts/fastapi-openapi-v1.json`。这是向响应 item
增加必填字段的兼容扩展；旧历史查询返回 `legacy_source_quote`，新数据明确区分竞价参考和开盘成交。
PostgREST 与 Agent 契约没有暴露此 RPC，不新增相关表面。

## 质量、错误与可追溯性

- Raw JSONL、Manifest 和 IngestionRun 完全不变，仍可核对原始 `price/bid1/bid_vol1`；
- 缺买一价或买一量不是 Provider 响应缺行，记录仍可被 accepted，但三个目标字段为 `NULL`；
- 负价、负量、Decimal/整数类型错误继续阻断记录；
- 金额只使用 Decimal 乘法，不经过 float；
- 不把 09:25 前来源 `amount` 的协议填充值写入标准金额；
- 本变更不触发 replay，不修改 2026-08-17 或更早历史快照。

## 测试与发布边界

TDD 覆盖：

1. 09:24:40 使用买一价、买一股数和乘积，并标记 `auction_indicative`；
2. 09:25:00 使用来源成交字段，并标记 `opening_trade`；
3. 买一价或买一量缺失时三字段同为 `NULL`；
4. 领域金额一致性、非负、枚举和边界验证；
5. PostgreSQL migration、历史标签、显式写入、RLS/grant 和 RPC item；
6. FastAPI 模型与 OpenAPI 合同；
7. Raw envelope 在变更前后保持相同来源字段。

实现完成后运行聚焦测试、隔离 PostgreSQL 集成测试以及 Ruff、mypy、全量 pytest。推送、生产 migration、
服务器发布和服务重启仍需项目所有者另行明确授权。
