# ADR-0030：Daily Limit-Up List 切换 TodayLimitUp 契约

- 状态：Accepted
- 日期：2026-08-12
- 跟踪：GitHub Issue #43

## 背景

`/api/v1/daily-limit-up-list` 原先直接组合通用股票池、可变收盘/竞价投影和
`core.security.current_name`，没有同日涨停快照的 version、status、Raw/ingestion 与质量语义。
用户明确要求该稳定路径改为读取 ADR-0029 定义的 `today_limit_up` 领域。

## 决策

1. 保留 HTTP 路径，但有意替换其响应契约。这是用户批准的 v1 兼容性变更；
   旧字段 `volume`、`free_float_turnover_rate_pct`、`seal_amount`、
   `seal_volume_ratio`、`consecutive_limit_up_days` 和 auction 字段不再返回。
2. RPC 仅读取 `today_limit_up.snapshot/member/calculation_quality`。必须精确指定
   `trade_date`，可选指定正 version；未指定时取该日最新版本，不回退旧日期。
3. 响应提供 snapshot/calculation/source lineage、status、计数、版本/哈希/算法、
   按 rule 分组的有界质量摘要和领域成员。
4. 成员按 `symbol` 升序、先 offset 后 limit；`limit` 范围 1..500，`offset` 范围
   0..50000。`has_more` 基于 snapshot 实际成员数。
5. 价格、比率和金额仍用 Decimal 序列化为 JSON string。NULL 保持缺失，不用
   其他日期、其他市值或来源字段替代。来源报告封板资金与 bid-1 计算封单金额分列。
6. FastAPI 仍要求 API Key；RPC 仅 grant `market_data_api`，不给 `public`/`anon`/
   `authenticated`，API 角色不获得底表 SELECT。
7. `/api/v1/limit-up-pool` 与其契约保持不变。

## 字段语义

- `free_float_market_cap_cny = close * free_float_shares`。
- `closing_bid1_sealing_amount_cny = closing_bid1_price * closing_bid1_volume_shares`，
  不是五档合计。
- `source_reported_sealed_funds_cny` 是来源报告值，不与上述计算值混同。
- v1 `limit_up_duration_seconds` 通常为 NULL，并由 `duration_semantics` 解释；
  不从首次/末次封板时间伪造时长。
