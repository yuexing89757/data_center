# 竞价序列快照历史归档设计

- 日期：2026-09-13
- 状态：Approved for planning
- GitHub Issue：#78
- 关联决策：ADR-0017、ADR-0034、ADR-0050、ADR-0051、ADR-0054

## 目标与范围

在不降低在线表早盘写入性能的前提下，把仍存在的竞价序列标准事实归档到独立历史表，滚动保留六个
自然月，供未来有界回测读取。03:00删除在线旧数据前，必须证明每条待删事实已经归档。

本阶段不实现回测算法、公共历史查询API、Raw自动重放或运行时分区管理。

## 方案选择

采用“02:30独立归档，03:00清理带保护门禁”。独立任务符合三点前完成的要求；两项任务均幂等，
02:30临时失败时03:00不会丢数据，次日可以自动追赶。

不采用03:00单任务串行方案，因为归档不能在三点前完成；不采用只保留Raw的方案，因为当前该数据集
Raw replay仍fail closed，无法提供低成本回测输入。

## 数据模型

新增月度Range Partition父表：

```text
realtime.call_auction_market_series_snapshot_history
```

字段与发布时在线Snapshot一比一，包括：

- `trade_date`、`ingestion_id`、`session_id`、`sample_seq`；
- `scheduled_at`、`batch_code`、`observed_at`；
- `symbol`、`source_code`、`value_semantics`；
- `last_price`、`previous_close`、`high_price`、`low_price`；
- `cumulative_volume`、`cumulative_amount`；
- 买卖一至五档价格和股数；
- 原事实`created_at`。

历史行不增加`archived_at`；归档时间属于Operations事实，不改变来源事实。主键沿用
`(trade_date, ingestion_id, symbol)`；Session/Round、IngestionRun和Security外键继续约束lineage，
数值、时段和值语义硬约束与在线表一致。

唯一辅助索引为`(trade_date, symbol, sample_seq)`，不复制在线表的ingestion-symbol索引。migration
创建前六个月、当前月及未来十二个月分区；后续月份仍由ordered migration延伸。

## 02:30归档任务

Worker任务目录新增：

```text
job_id: call-auction-market-series-archive-daily
display_name: 沪深全市场开盘竞价序列历史归档
workflow: call_auction_market_series_archive
schedule: 每天 02:30 Asia/Shanghai
```

任务从调度fire time解析上海本地`reference_date`，选择在线表中全部
`trade_date < reference_date`行，不按Session状态过滤，并执行
`INSERT ... SELECT ... ON CONFLICT DO NOTHING`。返回扫描、新增和已存在行数，满足
`scanned_rows = inserted_rows + existing_rows`。

周末、节假日和重复触发最多扫描三个在线交易日，已有行不重复、不更新。任务中断时事务回滚；下次运行
重试同一集合。任务默认启用，唯一开关为`CALL_AUCTION_MARKET_SERIES_ARCHIVE_ENABLED`；时间、保留期、
表名和字段映射不进入环境配置。

## 03:00清理保护与历史保留

现有`data-cleanup-daily`继续用统一交易日历确定最近三个已完成交易日。删除
`trade_date < online_cutoff_date`前，对待删在线自然键执行历史表反连接；存在任一缺失行时抛出稳定
错误，在线DELETE不执行。只比较总行数不构成完整性证明。

验证成功后执行既有在线DELETE。然后以纯函数计算六个自然月历史截止日，删除
`trade_date < history_cutoff_date`。月末采用目标月份最后一天，例如8月31日减六个月在非闰年得到
2月28日。截止日当天保留。

历史DELETE使用独立事务。它失败时workflow显式失败，但已安全完成的在线清理不回滚，历史数据只会
多保留并于次日重试。过去已从在线表删除的日期不在首次归档范围，不从Raw伪造恢复。

## 权限与事务

ordered migration创建父子表、约束、索引、RLS和Operations workflow code。`market_data_worker`仅获
SELECT、INSERT、DELETE，不获UPDATE；public、anon、authenticated和`market_data_api`没有历史表权限。
migration本身不复制或删除生产数据。

归档INSERT在单事务中完成。03:00的逐键验证与在线DELETE共享一个事务，防止检查和删除之间出现边界
漂移；历史保留DELETE使用后续独立事务。Worker不执行DDL、分区detach/drop或VACUUM。

## 应用边界与可观测性

纯函数只计算六个月截止日；Persistence拥有固定SQL和事务；Service编排归档、验证和清理；Scheduler
只创建受控WorkflowRun并调用Service。任何层都不接受动态表名、执行时间或保留月份。

Operations分别记录归档扫描、新增、已存在、完整性验证、在线删除和历史删除行数。归档或验证失败时
不得伪报成功，也不得降级为警告。

## API与回测边界

现有竞价序列RPC和FastAPI仍只读取在线表，checked-in contracts不变。历史表是内部事实层，消费者
不能直连`realtime`。未来历史API必须先明确日期跨度、股票数量、轮次筛选和返回上限，再据真实访问
模式设计`api_v1` RPC和索引。本Issue不实现策略、撮合或收益计算。

## 容量与生命周期

生产基线为每交易日约166,900行、平均行宽约223字节。六个月约两千万行；只追加历史表加主键和一个
辅助索引预计6～10 GB。上线后记录`pg_total_relation_size`、归档耗时和autovacuum结果。

在线表保留最近三个已完成交易日；历史Snapshot保留六个自然月；Raw、Manifest、IngestionRun、
QualityResult、Session、Round和Operations长期保留。DML删除不承诺立即归还磁盘空间，完全过期分区
的物理回收只能在受保护migration或运维窗口执行。

## 失败语义

- 02:30归档或分区缺失：事务回滚，workflow失败，03:00门禁阻止相关在线删除；
- partial会话：合法已落库事实照常归档，缺失保持可见；
- 重复任务：冲突行不更新，摘要记录existing；
- 03:00验证缺失：在线和历史DELETE均不执行；
- 在线DELETE失败：事务回滚，不继续历史保留；
- 历史DELETE失败：历史多保留，次日重试；
- Operations失败：任务失败，不伪报成功。

## 测试与发布

单元测试覆盖六个月截止的普通日期、月末和闰年，归档摘要不变量、partial、幂等、缺失阻断删除、
02:30固定任务、默认开关、周末节假日及Operations步骤顺序。

隔离PostgreSQL测试覆盖最新表结构全字段复制、单Session 32轮、partial、重复归档不更新、逐键门禁、
六个月截止、分区、外键、RLS、Worker最小权限、API无权、UPDATE被拒绝及事务边界。

发布前执行Ruff、mypy、全量pytest、隔离PostgreSQL integration、migration check和只读容量基线。
生产migration、服务部署和首次归档必须另行明确授权；首次归档不恢复已经清理的历史日期。
