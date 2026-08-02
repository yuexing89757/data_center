# Worker 本地只读管理页面

统一 Worker 启动后，在本机打开：

```text
http://127.0.0.1:8765/admin/scheduled-tasks
```

端口可通过 `WORKER_ADMIN_PORT` 修改。监听地址固定为 `127.0.0.1`，页面没有应用层认证，
不得改为公网监听或通过未认证反向代理暴露。远程查看应使用经过认证的 SSH 隧道：

```bash
ssh -L 8765:127.0.0.1:8765 <worker-host>
```

页面显示受控任务/Workflow 定义、步骤顺序、超时恢复策略、JobStore 持久化状态、
Asia/Shanghai 下次运行时间、当前 Worker 存活状态、健康摘要和 PostgreSQL 中最近十次
WorkflowRun。页面只读取 SQLite 的 `id` 与 `next_run_time`，不读取或反序列化 `job_state`，
也不提供任何任务操作。

“已持久化”不表示 Worker 正在运行；本页面由统一 Worker 自身提供，因此页面能够访问时
才表示该 Worker 进程正在运行。APScheduler JobStore 不保存项目所需的可靠执行历史，页面
不会猜测最近一次成功状态；请结合 IngestionRun、systemd 状态和日志诊断。
