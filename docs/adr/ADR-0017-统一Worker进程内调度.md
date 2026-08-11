# ADR-0017：统一 Worker 进程内调度

- 状态：Accepted
- 日期：2026-08-02
- 关联 Issue：#30、#40
- 决策者：项目所有者
- 取代：ADR-0015 第 1 条、ADR-0016 第 1 条与第 6 条中的独立 Scheduler 产品入口
- 影响：Worker 命令行入口、systemd/容器部署、健康检查

## 背景

ADR-0015 和 ADR-0016 将 APScheduler 暴露为独立的
`market-data-center-scheduler` 程序。虽然任务已经在同一个进程内执行，但独立名称容易让
部署者误以为还需要部署一套 Scheduler 服务和另一套采集 Worker，增加理解和运维成本。
项目当前没有可以嵌入定时器的常驻 API 服务，临时 CLI 采集命令也无法自行触发未来任务。

## 决策

1. 只保留 `market-data-center` 一个产品命令入口。`market-data-center worker` 启动常驻
   采集 Worker，APScheduler 是 Worker 的内部调度组件，不再提供
   `market-data-center-scheduler` console script。
2. `market-data-center worker --check` 提供只读健康检查，并保持原有 JSON 结果和退出码。
3. systemd 或容器只部署一个 Worker 服务/容器。所有定时任务直接在 Worker 进程的受限
   单线程执行器中运行，不启动独立任务执行进程。
4. SQLite JobStore、PostgreSQL 单实例锁、SIGTERM 优雅退出、陈旧运行恢复、任务时间与
   数据安全门禁保持 ADR-0016 的既有决定。
5. 手工一次性采集继续使用同一个 `market-data-center` 命令的其他子命令；它们不是额外
   常驻服务。
6. Worker 的受控任务目录是任务时区、cron/interval、采样节奏、misfire 与 timeout 的唯一
   事实来源。运行环境只允许通过三个既有布尔开关启停可选任务，不允许覆盖任务执行时间。

## 后果

- 部署只有一个应用入口和一个常驻 Worker，不再区分 Scheduler 与采集 Worker；
- APScheduler 仍要求一个被 systemd 或容器监管的常驻进程，这是进程内定时触发的必要条件；
- 若未来引入常驻应用服务，是否进一步嵌入该服务必须通过新的 ADR 评估故障隔离与资源竞争。

## 验收

- 包只生成 `market-data-center` console script；
- `market-data-center worker` 注册并运行全部固定任务；
- `market-data-center worker --check` 保持只读且健康时退出码为零；
- systemd、README、运行手册和 AGENTS.md 只描述统一 Worker 部署。
