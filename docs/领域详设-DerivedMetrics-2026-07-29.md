# 领域详设：Derived 与 Metrics v1

> 状态：有效
> 日期：2026-07-29
> 依据：`adr/ADR-0009-版本化复权行情与客观Metrics.md`

## 1. 边界与依赖

Derived 保存可由 Daily Bar 和 Capital 重算的证券级客观结果；Metrics 保存基于 Classification 快照聚合的横截面结果。依赖方向固定为：

```text
Market + Capital ──► Derived
Derived + Market + Classification ──► Metrics
```

Calculator 是纯函数。数据库读取、输入快照、计算批次、锁和原子写入不进入 Calculator。

## 2. CalculationRun

`derived.calculation_run` 是所有结果的版本和血缘根：

| 字段 | 语义 |
| --- | --- |
| `calculation_code` | 稳定计算族，当前为 `cn_a_share_daily_derived` |
| `algorithm_version` | 不可变算法契约版本 |
| `mode` | `full` / `incremental` 运维意图 |
| `start_date` / `end_date` | 输出区间和前复权末端锚点 |
| `input_watermark` | 各输入表本次快照内最大的 `updated_at` |
| `input_hash` | 排序、规范化后的业务输入 SHA-256 |
| `calculated_at` | 成功计算时间 |

成功签名 `(calculation_code, algorithm_version, start_date, end_date, input_hash)` 唯一。水位线用于定位可能修订，哈希用于判断业务内容是否真的改变；来源代码和 ingestion 元数据不进入业务哈希。

## 3. 复权行情

`derived.adjusted_daily_bar` 每个交易日分别保存 `forward` 和 `backward`。因子与价格使用 Decimal，禁止经过 float。

- 前复权：`end_date` 的因子为 1，历史价格乘以后续事件因子；
- 后复权：历史起点因子为 1，除权日起乘以事件因子的倒数；
- 除权日的 adjusted previous close 使用上一交易日因子，保证事件边界可计算总收益；
- `1.0.0` 要求事件日必须有行情；当前默认 `1.1.0` 在事件日无行情时对齐到第一条后续可用 Daily Bar，并把无前值的首条历史记录作为事件已生效后的因子锚点；
- OHLC 和 previous close 调整；volume、amount 继续从 Core 读取，不复制到派生事实。

## 4. 日指标

`derived.daily_metric` 当前发布：

- `total_return_1d`：前复权 close / 前复权 previous close - 1；
- `moving_average_5`、`moving_average_10`、`moving_average_20`：前复权 close 的简单移动平均。

窗口必须完整且无 NULL。Persistence 加载请求区间、每只证券开始日前最近 20 条记录，以及用于后复权累计的历史公司行为事件日记录；Calculator 预热窗口后只输出请求区间。pytdx v1 遗留记录的 `previous_close` 为空时，读取边界使用同证券严格上一条 close 确定性补足，不改写 Core。

## 5. 市值

`derived.market_capitalization` 使用未复权 close：

```text
total_market_cap = raw_close × total_shares
circulating_market_cap = raw_close × coalesce(listed_a_shares, circulating_shares)
```

股本按 `effective_date <= trade_date` 选择最新事实。缺少 close 或有效股本时不输出，绝不使用当前股本倒填早期历史。

## 6. 分类横截面 Metrics

`metrics.classification_daily_metric` 对每个 `(namespace, type, code, trade_date)` 选择 `snapshot_date <= trade_date` 的最新完整成员快照，保存：

- `member_count` 与 `priced_member_count`；
- 上涨、下跌、平盘成员数；
- 成交量、成交额；
- 可用成员的等权总收益率；
- 可用成员总市值及其覆盖数；
- 实际使用的 `membership_snapshot_date`。

空成分快照产生 member_count=0 的客观统计；没有任何不晚于交易日的快照则不输出。

## 7. CLI 与重算

```bash
market-data-center derived-recompute \
  --start-date 2026-01-01 \
  --end-date 2026-07-29 \
  --mode incremental \
  --algorithm-version 1.1.0
```

`incremental` 对相同签名返回 `unchanged`；输入修订后保守重算整个请求区间并创建新版本。`full` 用于表达人工全量校验意图，但同样不会重复创建完全相同的成功签名。

## 8. PostgREST

只读 View：

- `api_v1.calculation_runs`
- `api_v1.adjusted_daily_bars`
- `api_v1.daily_metrics`
- `api_v1.market_capitalizations`
- `api_v1.classification_daily_metrics`

版本化 View 都包含计算批次、算法版本、计算区间、输入哈希和计算时间。消费者需要固定这些字段；`api_v1.daily_bars` 始终是原始未复权事实。

## 9. 失败与验收

- 计算前获取日期区间级 advisory lock；
- 输入在 PostgreSQL Repeatable Read 快照中加载；
- 所有输出和成功状态在一个事务中提交；
- 计算失败只更新 failed run，不留下部分输出；
- Golden Dataset 覆盖复权、收益率、市值和分类统计；
- 参数化测试覆盖多种送股比例的理论连续性；
- 集成测试覆盖 migration、幂等签名、Core 修订和多版本 API。
