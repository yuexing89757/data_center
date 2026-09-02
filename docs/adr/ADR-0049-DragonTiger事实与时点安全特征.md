# ADR-0049：DragonTiger 事实与时点安全特征

- 状态：Accepted
- 日期：2026-09-02
- 关联 Issue：#70
- 决策者：项目所有者
- Supersedes：ADR-0046
- 影响：替换 `TradingBillboard` v1 领域、内部表和三个公共读取契约

## 背景

ADR-0046 建立了可追溯的东方财富龙虎榜采集闭环，但其事实模型把同一席位按买榜和卖榜拆成
两行，未表达 `DAY`/`THREE_DAY` 统计周期，也没有稳定席位、来源别名和原因字典。真实来源验证
进一步确认：三日榜与普通榜共用来源数据集；可靠席位代码可跨买卖榜合并；多条“机构专用”既
没有可靠代码也不代表同一主体；部分明细只披露一侧金额，缺失对侧不能解释为零。

项目所有者确认旧三个 FastAPI/PostgREST 契约直接删除，由新契约替代，不提供兼容层。历史
migration 仍保持不可变，所有数据库变化必须通过新的 ordered migration 前向完成。

## 决策

1. 领域名称改为 `DragonTiger`。一个 `DragonTigerEvent` 表示一只股票在一个披露统计周期内因
   一个原因上榜的事实；普通榜与三日榜只通过 `period_type` 区分，不建两套模型。
2. `THREE_DAY` 必须同时保存 `period_start_date` 和 `period_end_date`。结束日等于披露交易日，
   起始日由 `CN_A_SHARE` 交易日历向前取三个交易日计算，Adapter 不按自然日猜测。
3. 建立 `DragonTigerReason` 及来源映射。项目原因代码、原因类型与周期是标准语义；来源原因代码
   和原文只存在于通用来源映射/来源事实字段，不把东方财富或 Tushare 字段名带入领域层。
4. 建立 `TradingSeat` 与 `TradingSeatAlias`。可靠来源席位身份映射到稳定 `seat_id`；来源代码和
   原始名称保留。无法唯一识别主体的“机构专用”等占位记录不创建虚假稳定席位，`seat_id` 为
   `NULL`，但来源行和 `seat_name_raw` 必须保留。
5. 一个 `SeatTrade` 表示一个 Event 内一个可识别席位的完整披露行为。可靠身份在买卖榜同时出现
   时合并为一行并分别保存 `buy_rank`/`sell_rank`。不可靠身份按来源行保存，禁止仅按名称合并。
6. `buy_amount`、`sell_amount` 都可为空，但不能同时为空或同时为零；缺失保持 `NULL`。`net_amount`、纯买、
   纯卖和买卖重叠是计算结果，不进入事实表；只有两侧金额都已披露时才能计算净额。
7. Event 只持久化来源直接披露的价格、比例和金额基础事实。净额、龙虎榜成交合计、占比、集中度
   及各种计数由纯 Calculator 计算，不重复存储。
8. 复用 ADR-0046 的东方财富有界 HTTP、三报表原子读取、Decimal 解析、Raw、ingestion、quality
   和 Worker APScheduler 基础设施；替换无法复用的 v1 Record、Service、Persistence 和契约。
9. 新增独立 `TushareDragonTigerAdapter`。它与东方财富同属 `DragonTigerProvider` 能力，但不做
   自动切换、不跨来源拼接、不仲裁冲突；一次成功 ingestion 只有一个实际 Provider。
10. Raw schema 升为来源各自的 v2。v1 Raw 永久保留，并由显式版本化 replay 适配器重放到新事实；
    不在 migration 中伪造或改写 Raw。
11. 新 ordered migration 创建 `billboard.dragon_tiger_reason`、`reason_source_alias`、
    `trading_seat`、`trading_seat_alias`、`dragon_tiger_event` 和 `seat_trade`，删除 v1 RPC、
    `billboard.seat` 与 `billboard.entry`。本仓库不执行生产 migration 或生产 replay。
12. 旧的三个 RPC 和 FastAPI 路由直接删除。新契约按日期、股票、稳定席位和事件客观指标读取；
    全部具有日期、limit、offset、statement timeout 硬上限，并只访问 `api_v1`。
13. `TradingSeatProfile`、`DragonTigerCapitalMetrics` 和 `DragonTigerFeature` 只包含确定性客观统计，
    使用版本化算法和显式 `as_of_date`。训练 Label 独立建模，不能出现在预测 Feature 中。
14. 项目宪法排除主观解释和应用判断，因此不实现 `CapitalQuality` 五项主观评分、
    `overall_score`、游资身份、席位等级或交易建议。数据中心提供这些评分可能消费的客观组成指标。

## 公共契约

- `query_dragon_tiger_events_by_date`：准确交易日，可选 `period_type`，分页返回事件及合并席位。
- `query_dragon_tiger_events_by_symbol`：六位股票代码和不超过 366 天的日期范围。
- `query_dragon_tiger_trades_by_seat`：稳定 `seat_id` 和不超过 366 天的日期范围。
- `query_dragon_tiger_event_metrics`：按 `event_id` 返回确定性资金结构指标。
- FastAPI 使用新的 `/api/v1/dragon-tiger/...` 路由调用上述 RPC；不保留
  `/api/v1/trading-billboard/...`。

三份 checked-in contract 必须同步，内部 `billboard` schema 不向消费者授予直接表读取权。

## 后果

- 破坏性升级要求 Worker、数据库契约和 FastAPI 同批发布；旧消费者必须迁移。
- 旧标准事实表删除后，历史数据通过不可变 v1 Raw 受控重放到新模型；生产执行前必须备份并验证
  replay 覆盖率。
- 无可靠席位身份的数据仍可用于事件级资金结构分析，但不能污染跨期席位画像。
- 主观评分与策略继续由消费者项目负责，Market Data Center 保持事实和客观派生边界。

## 实施条件

- Issue #70、领域详设和实现保持一致；
- migration、Provider、Domain、Service、Persistence、Raw replay、API、scheduler 和契约测试齐全；
- PostgreSQL 集成测试只能使用 `TEST_DATABASE_URL` 指向隔离可丢弃数据库；
- 东方财富/Tushare 生产采集和再分发仍受各自来源权利审阅门禁约束。
