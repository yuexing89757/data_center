# StockDailyIndicator 领域详设

- 状态：有效
- 日期：2026-08-02
- 依据：ADR-0014、Issue #29

## 聚合与自然键

聚合根为 `StockDailyIndicatorSnapshotRecord`，表示一个标准股票在一个交易日由数据源
发布的每日指标快照。自然键为 `(symbol, trade_date)`，来源不参与自然键。

## 字段

| 字段 | 类型/单位 | Tushare 来源 |
| --- | --- | --- |
| `symbol` | 标准证券代码 | `ts_code` |
| `trade_date` | 交易日 | `trade_date` |
| `close` | Decimal，元/股 | `close` |
| `turnover_rate_pct` | Decimal，百分点 | `turnover_rate` |
| `free_float_turnover_rate_pct` | Decimal，百分点 | `turnover_rate_f` |
| `volume_ratio` | Decimal，无量纲 | `volume_ratio` |
| `pe`, `pe_ttm`, `pb`, `ps`, `ps_ttm` | Decimal，倍 | 同名字段 |
| `dividend_yield_pct`, `dividend_yield_ttm_pct` | Decimal，百分点 | `dv_ratio`, `dv_ttm` |
| `total_shares` | int，股 | `total_share * 10000` |
| `circulating_shares` | int，股 | `float_share * 10000` |
| `free_float_shares` | int，股 | `free_share * 10000` |
| `total_market_value` | Decimal，元 | `total_mv * 10000` |
| `circulating_market_value` | Decimal，元 | `circ_mv * 10000` |
| `price_limit_status` | 枚举 | `limit_status` |

## 不变量与质量规则

- 标准证券代码、交易日和来源不能为空；
- close、百分比、股本和市值非负，股本换算后必须为整数；
- 自由流通股本不大于流通股本，流通股本不大于总股本；
- 流通市值不大于总市值；
- 同批次同自然键内容冲突、未知证券和非交易日阻断 Core 写入；
- 相同内容重复记录去重；缺少可选指标不阻断写入。

## 存储与访问

- 表：`core.stock_daily_indicator`；
- Raw：`tushare.stock_daily_indicator.v1`；
- API：`api_v1.stock_daily_indicators`，不暴露采集技术字段；
- 默认查询必须限定 symbol 和日期区间，消费者自行处理缺失交易日。

## 调度与保留

- Windows Worker 在工作日收盘后触发，先同步并核验真实交易日；
- 交易日使用 Tushare `trade_date` 参数一次获取完整市场快照；
- 成功或部分成功后，Core 仅保留当前交易日向前一个自然月的数据；
- 清理条件为 `trade_date < cutoff_date`，截止日当天保留；
- Raw、Manifest、IngestionRun 和 QualityResult 不参与该清理，继续支持重放与审计；
- 规则依据 ADR-0015 和 Issue #30。

## 明确不做

- 不基于这些字段输出买卖建议、趋势或主观评分；
- 不将来源 PE/PB 变成项目自主计算指标；
- 不用每日快照替代公司行为驱动的 Capital 事实；
- 不引入复权价格事实。
