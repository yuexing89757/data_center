# ADR-0018：Worker 本地只读管理页面

- 状态：Accepted
- 日期：2026-08-02
- 关联 Issue：#31
- 决策者：项目所有者
- 影响：统一 Worker、本地运维可见性、SQLite JobStore 读取

## 背景

统一 Worker 已将 APScheduler 作为内部组件，但 CLI 健康检查只返回任务 ID，不能直观看到
计划描述、下次运行时间，以及“任务已持久化”和“Worker 正在运行”的区别。项目没有
FastAPI，第一阶段也禁止为本地运维页面引入新的应用 API 框架。

## 决策

1. 统一 Worker 使用 Python 标准库 HTTP server 提供只读 HTML 页面，不引入 FastAPI、
   JavaScript 前端、第二个服务或公共 API 契约。
2. 服务地址固定绑定 IPv4 loopback `127.0.0.1`，端口由 `WORKER_ADMIN_PORT` 配置，默认
   `8765`。不得配置公网监听，也不得通过未认证反向代理暴露。
3. 浏览器不访问 SQLite。后端使用 SQLite URI `mode=ro`，只查询
   `apscheduler_jobs.id` 和 `next_run_time`；禁止读取、反序列化或展示 `job_state`。
4. 任务名称、触发类型和计划描述来自与任务注册同步维护的受控代码元数据。JobStore 不含
   可靠历史时，不推断最近执行状态。
5. 页面由已获得 PostgreSQL Scheduler 单实例锁的 Worker 同进程提供，因此页面可访问时
   “Worker 正在运行”为可靠事实；任务是否持久化单独按 JobStore 查询展示。
6. 页面不提供写操作，不包含启停、删除、修改、立即执行或凭据/路径输出。POST 返回
   `405`，响应启用 no-store、CSP、nosniff、DENY frame 和 no-referrer 安全头。
7. 远程运维如有需要，使用经认证的 SSH loopback 端口转发；本功能自身不承担认证。

## 后果

- 本地运维可在一个页面核对受控计划和实际持久化状态；
- 页面不是生产监控系统，不保存执行历史，也不替代 systemd 存活探针和日志；
- SQLite 不可读或 PostgreSQL 健康查询失败时只能展示有限诊断，不能执行修复。

## 验收

- 自动化测试覆盖只读 SQLite 字段、页面转义、loopback 绑定、安全头和 POST 拒绝；
- 页面不包含 JobStore 路径、`job_state`、密钥或写操作；
- Worker 优雅退出时关闭本地 HTTP server；
- Ruff、mypy 和 pytest 通过。
