# ADR-0028：暂停全市场竞价 Raw 重放与移除自动最终化

- 状态：Accepted
- 日期：2026-08-11
- 关联 Issue：#41
- 决策者：项目所有者
- 替代：ADR-0027 第 7–9 项、Raw replay 质量门禁及相关验收陈述

## 背景

`call_auction_market_snapshot` 的首版 Raw 与 IngestionRun 只保存预期行数，没有持久化当次采集前冻结的
SSE/SZSE listed-stock 精确身份。仅校验行数、一般已知证券、重复和交易日，不能排除“缺失一只预期股票，
同时混入一只其他已知证券”的等量替换。因此该数据集当前不能从 Raw 安全证明与原冻结全集完全一致。

项目所有者同时决定撤销 21:30 “今日竞价量”自动最终化，不为它提供替代计划。

## 决策

1. 晨间 `call-auction-market-snapshot-daily` 继续在工作日 09:26 采集，沿用
   `CALL_AUCTION_SNAPSHOT_ENABLED` 布尔开关。来源事实、逐行原始 Raw、RawManifest、质量结果和 ingestion
   lineage 继续不可变保留，供审计及未来安全重放实现使用。
2. `RawReplayService.replay` 对 `call_auction_market_snapshot` fail closed：在读取 Raw、创建 replay
   IngestionRun 或任何提交前返回稳定错误。通用 normalizer registry 不注册 `pytdx_hq` 的该数据集路径，
   也不存在可产生 succeeded/partial replay 的私有提交分支。
3. 只有后续接受的决策持久化并验证“原始冻结全集”的确定性身份（例如规范编码后的加密摘要）后，才可重新
   启用本数据集的 Raw replay。当前不得用今天的证券全集、一般 known-symbol 集合或单纯行数替代原始身份。
4. 从 Worker job catalog、scheduler function map 和本地任务页移除
   `call-auction-snapshot-daily`；构建 Scheduler 时按该精确 ID 清理旧 SQLite JobStore 残留，防止旧部署继续
   自动触发。`run_call_auction_snapshot_job` 不再存在。
5. 不新增替代时间、`.env` hour/minute、cron、systemd timer、Windows Task Scheduler 或其他自动执行。
   `CALL_AUCTION_SNAPSHOT_ENABLED` 只控制 09:26 晨间来源采集。
6. 历史 `call_auction_snapshot` workflow/database code 和数据库最终化实现保留，以兼容既有 operations 事实
   和内部显式维护能力；不新增调度入口或公共 API。
7. 本决策不改变数据库 schema、migration、公共 PostgREST/FastAPI/Agent 契约或生产数据。

## 后果

- Raw 与来源事实仍完整保留，但运维和文档不得宣称该数据集当前可重放；
- Worker 和本地任务页只显示 09:26 全市场来源采集，不显示 21:30 自动最终化；
- 最终化代码继续可被内部验证和维护，但没有自动执行承诺；
- 若未来恢复 replay 或自动最终化，必须通过新的已接受决策和相应测试、文档及运维门禁。

## 验收

- replay 在 Raw 读取和持久化写入前以稳定错误拒绝该数据集，Raw 文件保持不变；
- catalog/function map 不含 21:30 job，旧 JobStore 中该精确 job ID 会被移除；
- 09:26 job 仍受现有布尔开关控制，`.env` 无时间字段；
- 历史 workflow/database code、内部最终化实现和公共契约保持兼容。
