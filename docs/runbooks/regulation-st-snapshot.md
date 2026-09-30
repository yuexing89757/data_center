# 监管异动 ST 日快照

ADR-0061。每日 22:30 的 Worker 监管工作流先采当日 Tushare `stock_st` 完整名单，
再计算监管状态与异动监控。非交易日跳过；上游失败、空名单、重复或达到 1000 行上限时，
不发布该日完整快照，也不继续当次监控计算。

手工补一个已结束的交易日：

```bash
uv run market-data-center regulation-st-snapshot --trade-date 2026-07-06
```

命令只处理指定日期。历史补采按交易日历逐日执行，核对每个成功批次的
`ingestion.ingestion_run`、`ingestion.raw_manifest` 与 `regulation.st_day_snapshot`
的日期、行数和 `ingestion_id`；不要把命令返回零误认为全区间已完整。
已成功的同日重采保留旧 Raw 与运行记录；名单变化才替换现行快照血缘。
名单更正会立即使使用旧 ST 血缘的监控批次及后继链失效，须从更正日按交易日顺序重算。

监管计算算法升为 `regulation-calculator.v3`，监控算法为 `regulation-monitor.v2`；
公开监控响应结构版本仍为 `regulation-monitor.v1`。旧批次和旧 Raw 保持不变。
历史名称只用于展示；缺当日 ST 快照时，适用性为 `INSUFFICIENT_DATA`。
次日 ST 事实尚未知时，次日参考价仍是条件性数值；API 在股票的
`missing_reasons` 标明 `next_day_st_unverified`，不声明为确定安全。
`core.daily_bar.is_st` 不由本流程改写。

历史监控补算前先用只读预检核查日 K、基准指数、ST 快照与连续检查点；
仅按交易日顺序发布，缺失换手率和实际日 K 不得填零或推断为完整。
