# StockPool 领域详设

## 模型

- `PriceLimitRule`：交易所、板块、ST/普通比例、价格档位、上市初期豁免天数、生效区间和稳定版本。
- `DailyPriceLimit`：证券、交易日、前收盘价、上下限、比例、档位、ST 观测值、规则/算法版本。
- `PriceLimitEvent`：证券、交易日、方向、收盘价和对应限价；只表示精确收盘涨/跌停。
- `StockPoolDefinition`：代码控制的稳定目录，不建立无必要的动态配置表。
- `StockPoolSnapshot`：按 basis/effective 日期和 version 冻结的不可变快照。
- `StockPoolMember`：只保存 snapshot、symbol 和 direction；每日指标在查询边界组合。
- `PriceLimitSealSummary`：未来由带 lineage 的盘口快照计算，当前没有记录即保持不存在。

## 持久化

- `derived.daily_price_limit`
- `derived.price_limit_event`
- `stock_pool.snapshot`
- `stock_pool.member`
- `stock_pool.calculation_quality`
- `derived.calculation_run` 继续承载 calculation lineage。

Worker 角色只有 `select/insert`，没有 snapshot/member update/delete 权限。修订必须创建新版本。

## 构建门禁

1. basis 必须是已知交易日，effective 必须等于日历下一交易日。
2. basis 当日 `daily_market` 与 `stock_daily_indicator` Operations 必须成功。
3. 仅处理当前已上市沪深股票；板块代码、IPO 日期和连续上市交易阶段必须可证明。
4. 精确 basis 日 K、前收盘价、每日指标及双方 ingestion lineage 必须存在。
5. 缺失、非交易、上市前五日、前五日不连续、OHLC 越界均产生逐证券 error finding 并排除。
6. 相同 input hash 返回 `unchanged`；修订生成新 calculation 与两份新 snapshot version。

## 读取契约

`api_v1.query_stock_pool_snapshot(pool_code, effective_trade_date, version, limit)` 返回一个 JSON
快照 envelope。空成员快照仍返回 metadata 与空数组；精确 ready 快照不存在则 `P0002`，不会选择
更早日期。成员详情组合名称、交易所、涨跌停价、前收盘价、自由流通换手率/股本/市值。

CLI：

```bash
market-data-center stock-pools-build --basis-trade-date 2026-07-31
market-data-center stock-pool-check \
  --pool-code CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP \
  --effective-trade-date 2026-08-03
```

## FastAPI 涨停池读契约

`GET /api/v1/limit-up-pool` 接受必填 `trade_date`（事件发生的上海交易日）、可选正整数
`version` 和 `1..5000` 的 `limit`。它只读取
`CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP` 的精确 basis 日期 ready 快照。

每个成员返回标准 `symbol`、证券 `code`、该交易日有效的历史 `name`，以及
`free_float_market_cap_cny = 当日未复权 close（元/股） × 当日 free_float_shares（股）`。
乘法在 PostgreSQL numeric 中完成，JSON/OpenAPI 使用 Decimal 字符串表达元值。该字段不是
`circulating_market_value` 的别名，也不从其他来源补值。输入不完整的成员单独省略；其他有效
成员仍返回。响应包含 `total_candidate_count`、`valid_count`、`returned_count`、唯一
`omitted_count`、`has_more` 和按缺失名称/收盘价/自由流通股本分组的 `omission_reasons`。
一名成员可同时计入多个原因。空的 ready 快照合法返回空 items 和全零计数。

RPC 先对整个不可变快照分类质量，再按 symbol 升序对有效成员应用 `limit`。因此省略计数不受
limit 影响，`returned_count = min(valid_count, limit)`，`has_more` 明确表示有效结果被截断。
首版不提供 offset/cursor；完整读取必须请求上限 5000。

自然键与修订仍由不可变快照的 `(pool_code, effective_trade_date, version)`、成员
`(snapshot_id, symbol)` 和 calculation lineage 承载。响应显式返回 basis/effective 日期、版本、
规则/算法版本、input hash 和 calculation ID，消费者可固定 version 重放历史结果。
