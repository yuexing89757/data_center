# ADR-0019：Operations 运行可观测模型

- 状态：Accepted
- 日期：2026-08-02
- 关联 Issue：#32
- 决策者：项目所有者
- 影响：统一 Worker 工作流、运行恢复、本地只读管理页面

## 决策

1. 新建 `operations` bounded context。`WorkflowRun` 记录一次业务工作流尝试，
   `JobExecution` 记录其有序步骤；两者与 APScheduler 内部表和 IngestionRun 分离。
2. 运行事实写入 PostgreSQL `operations.workflow_run` 和 `operations.job_execution`。
   任务与工作流定义保留为代码中的受控目录，不建立动态配置表。
3. `WorkflowDefinition` 提供稳定 code、名称、说明和步骤顺序；`JobDefinition` 提供所属
   workflow、触发计划/时区、启用状态、超时与恢复策略。APScheduler 注册、执行记录和
   HTML 展示共同引用该目录。
4. 每个同 workflow code、scheduled time 的再次触发递增 attempt，不覆盖旧记录。全局
   Scheduler 锁和 Pipeline 锁阻止正常重复；attempt 使人工重试和异常重复保持可见。
5. 状态只能从 `running` 进入 `succeeded`、`partial` 或 `failed`。进程崩溃后，启动及
   每小时恢复把超过 60 分钟的 running 工作流和步骤标记 failed。
6. 错误摘要只保存稳定错误类别或受控代码，不保存异常消息、请求参数、路径、URL、Token
   或源数据。行数统计来自对应 IngestionRun；无法可靠聚合时保持零而不猜测。
7. 本地 HTML 页面有限展示最近 WorkflowRun；不复制或反序列化 APScheduler `job_state`，
   也不将 operations 表加入公共 PostgREST API。

## 后果与限制

- 可看到工作流完成到哪个步骤，以及中断恢复后的终态；
- IngestionRun 仍是数据批次事实，Operations 只描述编排执行；
- 当前不记录日志正文、堆栈和 APScheduler 事件历史，详细诊断仍依赖受控日志。
