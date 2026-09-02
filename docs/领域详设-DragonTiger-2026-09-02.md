# 领域详设：DragonTiger A股龙虎榜资金事实与客观特征 v1

> 状态：已实现，待生产迁移与历史 Raw 回放
> 日期：2026-09-02
> 关联 Issue：#70
> 上级决策：`adr/ADR-0049-DragonTiger事实与时点安全特征.md`（Accepted）

## 1. 边界与依赖

```text
EastMoney / Tushare
        ↓
DragonTigerProvider → immutable Raw
        ↓
period/reason/seat normalization + validation
        ↓
DragonTigerEvent ──< SeatTrade >── TradingSeat ──< TradingSeatAlias
        ↓
pure objective analytics → as-of Feature
        └────────────────→ separately available Label
```

复用 `core.security`、`core.trading_calendar`、`core.daily_bar`、每日指标、涨停池和版本化
`derived.calculation_run`。本领域不复制证券、日线、市值、涨停或市场环境事实。

## 2. 事实对象

### 2.1 DragonTigerReason

- `reason_id: UUID`
- `reason_code: str`：项目稳定代码
- `reason_name: str`
- `reason_type: PRICE_DEVIATION | TURNOVER | AMPLITUDE | CONTINUOUS_LIMIT | ST | OTHER`
- `period_type: DAY | THREE_DAY`
- `description: str | None`
- `is_active: bool`

`ReasonSourceAlias` 以 `(source_code, source_reason_code, source_reason_name, period_type)` 保存真实来源
映射。没有核实的原因只能进入 `OTHER`，不得根据中文片段虚构更细分类。

### 2.2 TradingSeat / TradingSeatAlias

`TradingSeat` 保存 `seat_id`、canonical/broker/branch 名称、`BROKER|INSTITUTION|NORTHBOUND|OTHER|UNKNOWN`
类型、可空省市、首次/末次出现日期及 active 状态。

`TradingSeatAlias` 保存 `seat_id`、`source_code`、可空可靠来源身份 `source_seat_key`、`alias_name`。
唯一键为 `(source_code, alias_name)`，可靠来源身份另有条件唯一键。泛化占位名不建立 Alias。

### 2.3 DragonTigerEvent

- `event_id: UUID`
- `symbol: str`
- `trade_date: date`
- `period_type: DAY | THREE_DAY`
- `period_start_date / period_end_date: date`
- `reason_id: UUID`
- `reason_name_raw: str`
- `close_price / change_pct / turnover_amount / turnover_rate / amplitude: Decimal | None`
- `lhb_buy_amount / lhb_sell_amount: Decimal | None`
- `source_code / source_record_id / ingestion_id / content_hash`

`DAY` 的起止日期都等于 `trade_date`；`THREE_DAY` 的结束日等于 `trade_date`，起始日必须是统一
交易日历中包含结束日在内的第三个交易日。来源自然键为 `(source_code, source_record_id)`；同来源
语义键 `(symbol, trade_date, period_type, reason_id)` 冲突时整批失败。

### 2.4 SeatTrade

- `seat_trade_id / event_id`
- `seat_id: UUID | None`
- `seat_name_raw: str`
- `buy_amount / sell_amount: Decimal | None`
- `buy_rank / sell_rank: int | None`
- `is_institution / is_northbound: bool`
- `source_code / source_record_id / ingestion_id / content_hash`

至少一侧金额存在且不允许负数，至少一个名次存在且名次为 1～5。可靠来源身份相同才允许合并。
`net_amount` 仅在两侧都披露时返回 `buy_amount - sell_amount`；纯买/纯卖要求另一侧明确为零，
`NULL` 不等于零。

## 3. Provider 语义

### 3.1 EastMoney

保留现有固定端点、三报表原子读取、有界分页/字节数/重试、Decimal JSON 和 Raw 包装。v2 Adapter：

- 从 `EXPLANATION` 判定已验证的 DAY/THREE_DAY；未知周期硬失败；
- 输出通用 `source_reason_code/name`，不把来源字段名带出 Adapter；
- 可靠 `OPERATEDEPT_CODE` 跨买卖榜合并；代码 `0` 和泛化机构名不合并；
- BUY/SELL 任一缺失保持 `None`，忽略来源派生 NET；
- 北交所与其他 `core.security` 中有效 A 股允许进入标准验证，不再由领域硬编码过滤。

### 3.2 Tushare

`top_list` 提供事件汇总，`top_inst` 提供席位明细。Adapter 以日期、股票、周期和原因构造确定性
来源事件标识；席位无可靠代码，按来源披露行保留并仅通过显式 Alias 映射建立稳定 seat_id。
两个接口必须同批成功；不与 EastMoney 结果拼接。

## 4. 应用与持久化

Application 在写 Raw 后，使用持久化端提供的统一交易日序列补齐三日榜起点，解析/创建原因映射
和可靠席位 Alias，再执行领域校验。成功交易日原子 upsert Event/SeatTrade；来源修订替换该 Event
的 SeatTrade，旧来源仍由 Raw/ingestion 保存。回补逐交易日提交，首个失败即停止。

v1 Raw replay 通过专用 adapter 转成 v2 DTO；迁移本身不访问 Worker 文件系统。

## 5. 客观 Analytics 与 Feature/Label

`DragonTigerCapitalMetrics` 纯函数计算：可计算净额、龙虎榜金额/全天成交额、买卖席位数、明确
纯买/纯卖数、重叠数、机构/北向金额，以及 top1/top3/top5 买卖集中度。分母缺失或为零时结果
为 `None`。

`TradingSeatProfile` 以 `seat_id + as_of_date + algorithm_version` 生成总参与次数、累计买卖额、
连续交易日参与率及已有到期 Label 的 T+1/T+3/T+5 样本数、胜率和平均收益。连续参与率明确为
“参与日的下一交易日再次参与 / 存在下一交易日的参与日样本数”，交易日序列由统一日历提供并
记录 `participation_definition`。Participation 与 Outcome 以稳定来源事件标识关联，孤立或重复
Outcome 硬失败；只有 `label_available_date <= as_of_date` 的结果可进入画像。
“一日游”“锁仓”“连续买入”等其他行为必须先定义独立、可复现的 `metric_definition`，本版不
猜测业务口径，也不硬编码主观标签。

`DragonTigerFeature` 只组合截至事件日收盘可知的 Event、Metrics、历史 Profile 和外部客观环境
输入，历史 Profile 必须严格早于事件日。`build_dragon_tiger_labels` 按明确有序的后续交易日收盘
价生成已到期的 T+1/T+3/T+5 未复权收盘到收盘收益；日期必须与统一交易日日序精确对齐，缺失
K 线保持空洞且不移位。`DragonTigerLabel` 独立保存事件日、可用日和收益定义，Feature 类型和
契约不得包含任何 label 字段。不生成主观分数或 `overall_score`。

## 6. 公共查询与边界

- 日期查询和股票历史查询返回 Event、Reason 与嵌套 SeatTrade；
- 席位历史只接受稳定 UUID `seat_id`，不以模糊名称查询；
- Event Metrics 按单一 `event_id` 返回；
- 日期范围最多 366 天，limit 1～500，offset 0～10,000，数据库超时 5 秒；
- FastAPI 仅调用 `api_v1` RPC，内部表启用 RLS 且不向 API 角色授权直接读取。

## 7. 删除范围

删除旧 `TradingBillboardRecord`/`TradingBillboardSeatRecord`、v1 service/persistence、旧模型与路由、
三个 `query_trading_billboard_*` RPC 及相应契约。历史 migration `20260824000200` 不修改；新 migration
负责 drop。可复用的采集传输逻辑提取到新的 EastMoney Adapter，其余旧实现删除。

## 8. 验收

覆盖普通/三日 Event、周期日历、跨侧合并、未知对侧、纯买/纯卖、机构占位、Alias、幂等/修订、
Raw replay、双 Provider、集中度、as-of 画像、Feature/Label 隔离、破坏性契约替换和 Worker 调度。
