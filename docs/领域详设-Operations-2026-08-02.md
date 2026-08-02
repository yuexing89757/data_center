# 领域详设：Operations

Operations bounded context 只记录 Worker 编排事实，不承载市场数据，也不复制 APScheduler
内部状态。

## 模型

- `WorkflowDefinition`：代码内受控定义，包含稳定 code、名称、说明和步骤顺序。
- `JobDefinition`：代码内受控定义，包含所属 workflow、触发类型、计划/时区、启用状态、
  超时和恢复策略。它是 APScheduler 注册、执行记录和管理页面的共同目录。
- `WorkflowRun`：一次 workflow 尝试，唯一 ID；同 code 和 scheduled time 的重复尝试递增
  attempt，保留触发来源、时间、终态、汇总行数和脱敏错误类别。
- `JobExecution`：WorkflowRun 下的有序步骤尝试，记录 job code、sequence、attempt、时间、
  状态和行数。

合法状态从 `running` 单向进入 `succeeded`、`partial` 或 `failed`，终态不可再次转换。
进程崩溃留下的 running 记录由启动及每小时恢复任务在 60 分钟后统一标记 failed，错误码
固定为 `worker_interrupted_or_timed_out`。

## 边界

- APScheduler JobStore 只负责计划持久化，不作为执行历史；`job_state` 不进入领域。
- IngestionRun 继续记录每个 Provider 数据批次；Operations 只记录工作流和步骤编排。
- 错误摘要只允许异常类名或受控错误码，不保存异常正文、路径、参数、URL 和凭据。
- Definition 不动态入库。修改计划和步骤顺序必须通过代码、ADR 与测试审查。
- Operations 不进入 `api_v1`，仅由 Worker 和 loopback 本地管理页读取。
