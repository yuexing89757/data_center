# ADR-0015：股票每日指标调度与 Core 保留策略

- 状态：Accepted
- 日期：2026-08-02
- 关联 Issue：#30
- 决策者：项目所有者
- 影响：StockDailyIndicator 生产调度、Core 热数据保留、Worker 权限

> 部署入口已由 ADR-0017 调整为统一的 `market-data-center worker`；本 ADR 的任务时间、
> 交易日判断和保留策略继续有效。

## 背景

Tushare `daily_basic` 已支持按交易日一次获取完整市场快照。生产环境需要在每个交易日
收盘后自动采集，同时限制 `core.stock_daily_indicator` 的热数据规模。Raw、Manifest、
采集批次和质量结果仍承担审计与重放职责，不能随 Core 热数据一起删除。

## 决策

1. 使用独立 APScheduler 3.11.3 守护进程作为跨平台生产调度器；周一至周五 20:30
   触发 Worker。Worker 先同步当日交易日历，只有 `CN_A_SHARE` 当日开市时才请求
   全市场每日指标，法定休市日自动跳过。
2. 每次调用 Tushare `daily_basic` 的 `trade_date` 参数，一次获取完整市场快照；继续使用
   `tushare.stock_daily_indicator.v1` Raw schema、领域校验和自然键 UPSERT。
3. 当前交易日快照完成且状态为 `succeeded` 或 `partial` 后，执行显式 Core 清理；采集失败
   或休市跳过时不得清理。
4. 保留窗口为一个自然月。截止日由当前交易日减一个自然月计算，删除
   `trade_date < cutoff_date` 的 `core.stock_daily_indicator` 行，截止日当天保留。
5. 清理仅作用于 Core 每日指标。Raw 对象、`ingestion.raw_manifest`、
   `ingestion.ingestion_run` 和 `audit.quality_result` 长期保留，以支持追溯与重放。
6. 清理通过受测 Persistence 方法和显式 Worker 工作流执行，不使用触发器、TTL 扩展或
   无界后台删除。`market_data_worker` 仅增加该表的 `DELETE` 权限。
7. APScheduler 使用独立 SQLite JobStore `data/scheduler/jobs.sqlite` 保存调度状态，不在
   生产 PostgreSQL 中自动建表。任务固定 ID、`replace_existing`、`coalesce`、单实例和
   六小时 misfire grace；应用层 advisory lock 继续阻止重复采集。
8. Linux 使用 systemd、容器使用 restart policy 监管 Scheduler 进程；进程监管器不定义
   业务时间表。Windows Task Scheduler 不再承载该每日指标任务。

## 后果

- Core/API 只暴露最近一个自然月的每日指标事实，磁盘 Raw 和数据库审计仍会持续增长；
- 历史 Core 如需恢复，可从保留的 Raw 按 ingestion 批次重放；
- APScheduler 的工作日触发只是节流，真实交易日判断以数据库中刚同步的交易日历为准；
- JobStore 文件需要持久化挂载；单实例部署不需要额外事件总线。

## 验收

- 单测覆盖交易日执行、休市跳过、自然月截止日和“采集后清理”顺序；
- PostgreSQL 集成测试覆盖显式截止日删除和截止日保留；
- 提供跨平台 Scheduler 入口、Linux systemd 单元和运行手册；
- Ruff、mypy、pytest 与 migration check 通过。
