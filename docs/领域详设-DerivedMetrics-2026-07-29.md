# 领域详设：Derived 与 Metrics v1

> 状态：有效
> 日期：2026-07-29
> 依据：`adr/ADR-0009-版本化复权行情与客观Metrics.md`
> 数量完整性修订：2026-10-23，ADR-0068 / GitHub Issue #92；本地实现不代表已部署或完成历史重算。

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

当前新计算仅接受 `1.2.0`，默认使用该版本；拒绝以 `1.0.0` / `1.1.0` 创建新结果，避免旧版本静默获得新的缺失值语义。已有旧批次和事实保留供审计，不原地更新。

## 3. 复权行情

`derived.adjusted_daily_bar` 每个交易日分别保存 `forward` 和 `backward`。因子与价格使用 Decimal，禁止经过 float。

- 前复权：`end_date` 的因子为 1，历史价格乘以后续事件因子；
- 后复权：历史起点因子为 1，除权日起乘以事件因子的倒数；
- 除权日的 adjusted previous close 使用上一交易日因子，保证事件边界可计算总收益；
- `1.0.0` 要求事件日必须有行情；`1.1.0` 在事件日无行情时对齐到第一条后续可用 Daily Bar，并把无前值的首条历史记录作为事件已生效后的因子锚点；当前默认 `1.2.0` 沿用 `1.1.0` 的价格与事件语义，仅增加分类数量完整性契约；
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
- `total_volume`：完整成员当日成交股数之和，单位股，可空；
- `total_amount`：完整成员当日成交额之和，单位人民币元，可空；
- `volume_member_count` / `amount_member_count`：各自具有非 NULL 标准量/额的成员数；
- 可用成员的等权总收益率；
- 可用成员总市值及其覆盖数；
- 实际使用的 `membership_snapshot_date`。

空成分快照产生 member_count=0 的客观统计；没有任何不晚于交易日的快照则不输出。

`1.2.0` 的应参与集合严格沿用当前有效快照的 `active.members`。任一成员整条日 K 缺失，量额覆盖数均不增加、两项合计均为 NULL；只有 `volume` 为 NULL 时仅成交股数合计为 NULL，只有 `amount` 为 NULL 时仅成交额合计为 NULL。覆盖数各自满足 `0 <= count <= member_count`。显式来源零是可用事实：0 股或 0 元均增加相应覆盖数并按零求和；空集合的两项覆盖和合计均为零。停牌、上市/退市状态不自动剔除成员或补零。

例如两个成员分别为 `(100股,1000元)` 与 `(NULL,25元)`，结果为 `total_volume=NULL`、`volume_member_count=1`、`total_amount=1025`、`amount_member_count=2`。100 股就是 100 股，不是 100 手；Calculator 不进行来源单位换算。量与额的准入由输入边界认证，不完整输入不得冒充已认证值。

上涨/下跌/平盘、`priced_member_count`、等权收益率及其分母、市值和成员选择规则保持原有语义，不因缺量或缺额改变。新覆盖数由 Persistence 写入；旧事实覆盖数未知保持 NULL，不从旧合计或价格覆盖数推算。

## 7. CLI 与重算

```bash
market-data-center derived-recompute \
  --start-date 2026-01-01 \
  --end-date 2026-07-29 \
  --mode incremental \
  --algorithm-version 1.2.0
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

`classification_daily_metrics` 保持已有列顺序，末尾追加 `volume_member_count`、`amount_member_count` 与 `quantity_quality`。后者含 `unit_contract_version=stock-quantity.v1`、`status`、`reason` 和 `field_groups`：新 `1.2.0` 量额及市值覆盖均完整时为 `certified`，任一不完整为 `incomplete`；旧算法结果公开量额及市值均为 NULL、质量为 `unverified`。字段组独立报告 `classification_volume`、`classification_amount` 与 `classification_market_cap`；市值缺失不遮蔽已经完整的量额。已有受限内部事实仍保留原值，公共 View 不把旧版部分合计误作完整事实发布。

上述数据库改动由 `20261023000300_classification_quantity_completeness.sql` 声明；是否已应用、历史认证及重算是否完成应由部署和修复证据另行证明。

## 9. 失败与验收

- 计算前获取日期区间级 advisory lock；
- 输入在 PostgreSQL Repeatable Read 快照中加载；
- 日 K 输入按证券 keyset 每页最多 100 个加载，所有页和 watermark 共用该快照；
  标识分页只读取 Core 证券键，目标、20 日预热及事件行情的数量仍通过单位认证视图准入。
  分页不改变输入顺序、去重、哈希、计算公式或算法版本。
- 所有输出和成功状态在一个事务中提交；
- 计算失败只更新 failed run，不留下部分输出；
- Golden Dataset 覆盖复权、收益率、市值和分类统计；
- 参数化测试覆盖多种送股比例的理论连续性；
- 集成测试覆盖 migration、幂等签名、Core 修订和多版本 API。
