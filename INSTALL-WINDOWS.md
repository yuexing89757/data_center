# Market Data Center Windows 安装

## 运行前准备

1. 将压缩包解压到不会移动的固定目录，例如 `D:\market-data-center`。
2. 确认机器可以访问公共 TDX 行情节点；Worker 启动时会自动探测并建立能力节点池。
3. 安装 `uv`：

```powershell
winget install --id astral-sh.uv -e
```

安装完成后重新打开 PowerShell 或 CMD。

## 首次部署

在解压目录执行：

```powershell
.\deploy.cmd
```

第一次执行会从 `.env.example` 创建 `.env` 并停止。编辑 `.env`，至少填写数据库和 Raw 目录：

```dotenv
DATABASE_URL=postgresql+psycopg://<用户>:<密码>@<主机>:<端口>/<数据库>
FASTAPI_DATABASE_URL=
FASTAPI_API_KEY=<可保持占位符，部署脚本会自动生成>
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
RAW_DATA_ROOT=data/raw
PYTDX_POOL_PATH=data/pytdx_pool.json
PYTDX_VIPDOC_PATH=D:\new_tdx64\vipdoc
EOD_QUOTE_SNAPSHOT_ENABLED=true
CALL_AUCTION_MARKET_SERIES_ENABLED=true
```

不要把 `.env` 发给其他人，也不要提交到 Git。填写完成后再次执行；部署脚本会在 `.env` 中自动生成随机 `FASTAPI_API_KEY`：

```powershell
.\deploy.cmd
```

脚本会自动完成：

- 按 `uv.lock` 安装生产依赖；
- 创建 `.venv` 和 `market-data-center.exe`；
- 创建 `market-data-api.exe` 并检查两个命令行程序是否可运行；
- Worker 首次启动时探测 PYTDX 节点，并每 1 小时刷新一次能力节点池。

脚本**不再注册 Windows 计划任务**。所有定时采集（日 K、每日指标、涨跌停股票池、扣非净利润等）都由 worker 进程内置的 APScheduler 调度。执行时间固定在受控代码目录中；`.env` 只允许启用或停用三个可选行情任务。

## 验证

启动 API 与常驻 worker：

```powershell
.\serve.cmd
```

该命令会分别启动 FastAPI 只读 API（`http://127.0.0.1:8000`）和 `market-data-center worker` 两个常驻进程。浏览器打开 `http://127.0.0.1:8000/docs` 查看接口文档，业务请求必须携带 `.env` 中配置的 `X-API-Key`。worker 窗口会输出 APScheduler 的调度日志；到点会自动触发各定时任务。停止服务时在各自窗口按 Ctrl+C。

只读健康检查（不启动常驻进程）：

```powershell
.\deploy.cmd -Check
```

需要立即手动跑一次采集（不经过 worker 调度）时：

```powershell
.\.venv\Scripts\market-data-center.exe daily-run --as-of-date 2026-08-05
```

## 更新版本

保留原有 `.env` 和 `data/raw`，用新版本文件覆盖程序目录，然后重新执行：

```powershell
.\deploy.cmd
```

## 当前运行范围

- 股票日 K 与五档行情共用 Worker 自动维护的 PYTDX 能力节点池；
- 每日任务只处理最近交易日，不回补历史日 K；
- 每日任务不计算衍生指标；
- 数据库迁移由 GitHub Actions 或管理员单独执行，本安装包不会修改数据库结构。

## 常见问题

`uv is required`：安装 `uv` 后重新打开终端。

TDX 连接失败：检查 `PYTDX_POOL_PATH` 所在目录可写、出站端口、防火墙和公共节点可用性。刷新失败时 Worker 会使用最后一个有效节点池；首次启动没有有效池时会拒绝启动。

数据库连接失败：检查 `DATABASE_URL`、PostgreSQL 端口和网络连通性，不要把连接串粘贴到 Issue 或日志中。
