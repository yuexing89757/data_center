# ADR-0007：Capital 与公司行为基础事实

- 状态：Accepted
- 日期：2026-07-29
- 关联 Issue：#10

## 背景

复权、市值和市场统计需要可追溯的股本与公司行为输入。第一阶段只保存 Security、Trading Calendar 和不复权 Daily Bar，不能用当前股本快照推断历史，也不能把来源字段直接暴露为公共契约。

## 决策

1. 新增 Capital 领域，首批事实为 `ShareCapitalRecord`、`DistributionRecord` 和 `RightsIssueRecord`。
2. 自然键分别为 `(symbol, effective_date)`、`(symbol, report_period)`、`(symbol, record_date)`。`source_code` 和 `ingestion_id` 不参与去重。
3. 股数统一为“股”；现金统一为“人民币元/股”；送股、转增和配股比例统一为“新增股/原股”。Provider 的“每 10 股”值必须在边界除以 10。
4. 股本快照的 `effective_date` 表示该结构开始生效；分配方案保留报告期、公告日、登记日和除权日；配股保留登记日、缴款期和上市日。缺失日期保持 `NULL`，不得猜测。
5. 同一自然键再次出现视为来源修订，以最新成功批次 UPSERT 覆盖当前标准事实，同时通过新的 `ingestion_id` 和对应 RawManifest 追溯修订来源。不同 Provider 的冲突也使用同一规则，来源不是事实身份。
6. 首个可重复 Adapter 使用 AKShare：东方财富股本结构、东方财富分红配送详情和新浪配股历史。BaoStock 与本地 pytdx 明确不提供该能力；自动路由当前仅选择 AKShare。
7. AKShare 三个接口均返回完整历史，因此 `backfill` 和 `incremental` 都执行全历史对账。两种模式只表达运维意图，不改变事实语义；重复执行必须幂等。
8. 全流程继续执行 Raw → 标准化 → 校验 → `IngestionEnvelope` → Persistence。Raw 重放必须支持 Capital。
9. 本 ADR 只发布复权所需输入事实，不计算或发布复权价格、复权因子、收益率、估值和交易信号。

## 结果

- 可基于有效期查询任意历史时点的股本输入。
- 公司行为方案的来源修订可通过 IngestionRun 和不可变 Raw 追溯。
- 将来新增来源时必须输出相同 DTO 和单位，不能扩散来源专用字段。

## 参考

- AKShare `stock_zh_a_gbjg_em`：完整股本结构历史。
- AKShare `stock_fhps_detail_em`：按报告期的分红送转方案。
- AKShare `stock_history_dividend_detail(indicator="配股")`：配股历史。
