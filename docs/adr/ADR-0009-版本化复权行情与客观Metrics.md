# ADR-0009：版本化复权行情与客观 Metrics

- 状态：Accepted
- 日期：2026-07-29
- 实现澄清：2026-07-29 增加算法 `1.1.0`，处理无交易记录的公司行为日
- 关联 Issue：#12

## 背景

Daily Bar 是未复权客观事实，Capital 提供股本、分红送转和配股输入，Classification 提供版本化成分快照。复权价格、收益率、市值和板块横截面统计都能由这些事实重算，不应覆盖 Core，也不能失去算法版本和输入修订血缘。

## 决策

1. 新增 `derived` 和 `metrics` Schema。Core、Capital、Classification 保持不可被 Calculator 改写；`api_v1.daily_bars` 继续只代表未复权事实。
2. 纯 Calculator 只接收领域 DTO，不访问数据库、不创建批次、不写表。I/O、快照读取、锁、版本判断和事务提交由 DerivationService/Persistence 负责。
3. 首个不可变算法版本为 `1.0.0`，计算代码为 `cn_a_share_daily_derived`。改变公式、事件纳入规则、窗口规则、精度或锚点必须使用新版本，不得静默改写旧版本语义。
4. 每个计算批次记录 `calculation_id`、算法版本、请求区间、full/incremental 运维意图、输入水位线、业务输入 SHA-256、计算时间、状态和输出行数。派生 Record 不携带 `calculation_id`；Persistence 在写入边界附加。
5. 同一计算代码、算法版本、日期区间和业务输入哈希只有一个成功批次。full 和 incremental 均读取完整依赖闭包；incremental 在签名未改变时直接返回 `unchanged`，full 也遵守相同幂等约束，避免制造逻辑重复版本。
6. 当前增量失效策略是保守的区间级重算：Daily Bar、股本、公司行为或 Classification 的业务内容发生修订，输入哈希改变，整个请求区间生成一个新版本。旧结果保留。后续只有在有性能证据时才引入更细粒度依赖图。
7. 除权日理论价公式为：

   ```text
   theoretical_ex_price
     = (previous_close - cash_dividend_per_share
        + rights_price × rights_ratio)
       / (1 + bonus_ratio + transfer_ratio + rights_ratio)

   event_factor = theoretical_ex_price / previous_close
   ```

   同日事件先合并。分红送转只纳入 `implemented` 且具有 `ex_date` 的事实；配股只纳入具有 `ex_date` 的事实。缺少对应 Daily Bar、正数 `previous_close` 或理论价不为正时阻断整个计算批次，不猜测。
8. 前复权因子是交易日之后、截至本次 `end_date` 的事件因子连乘；区间末端锚定为 1。后复权因子从该证券已加载历史起点为 1，并在除权日起累乘事件因子的倒数。OHLC 与 previous close 调整，成交量和成交额不调整。
9. 一日总收益率使用前复权 close/previous_close；MA5/10/20 使用前复权收盘价，窗口不足或窗口内存在空值则为 NULL。计算区间开始日前的历史数据必须用于窗口预热，但只输出请求区间。
10. 总市值使用当日未复权收盘价 × 当日有效总股本；流通市值优先使用 `listed_a_shares`，缺失时使用 `circulating_shares`。没有有效股本或收盘价时不生成该日市值记录。
11. Classification 横截面按交易日选择不晚于该日的最新完整成分快照，发布成员数、有价格成员数、上涨/下跌/平盘数、成交量、成交额、等权总收益率和可计算总市值；不得产生情绪、周期或交易建议。
12. `api_v1` 通过独立版本化 View 暴露计算批次、复权行情、日指标、市值和分类统计。每行带 `calculation_id`、算法版本、计算区间、输入哈希和计算时间，消费者必须明确选择版本；原始未复权 View 不变。
13. 算法 `1.0.0` 保持第 7 条的严格事件日匹配语义。算法 `1.1.0` 是当前默认版本：公司行为 `ex_date` 没有 Daily Bar 时，将事件对齐到该证券第一条严格晚于 `ex_date` 的可用 Daily Bar；若截至区间末仍无后续记录，批次继续失败。若事件落在已加载价格历史的第一条记录且不存在前收盘，则把该记录视为事件已生效后的历史锚点，因子从 1 开始；其余事件仍必须具有正数前收盘。该规则覆盖停牌、有限历史窗口和用户明确不补历史日 K 的场景，不新增、不猜测 Core 行情事实。
14. 对 pytdx v1 遗留事实缺少 `previous_close` 的情况，Persistence 使用同一证券严格上一条 Core Daily Bar 的 close 作为确定性回退。日常计算只加载请求区间、每只证券最近 20 条预热记录以及公司行为事件日记录，不传输与输出无关的完整价格历史。

## 结果

- 相同输入与算法版本产生相同业务结果，并通过输入哈希幂等复用。
- Core 修订会形成新输入哈希和新计算版本，旧结果仍可审计。
- 原始行情、前复权、后复权以及不同算法版本在 API 中不会混淆。
- 当前策略优先保证正确和可解释，代价是任一依赖变化会重算整个请求区间。
