# Market Data Center Windows 安装

## 运行前准备

1. 将压缩包解压到不会移动的固定目录，例如 `D:\market-data-center`。
2. 确认本机通达信已经下载日线数据，并能找到 `vipdoc` 目录。
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

第一次执行会从 `.env.example` 创建 `.env` 并停止。编辑 `.env`，至少填写数据库和本地通达信目录：

```dotenv
DATABASE_URL=postgresql+psycopg://<用户>:<密码>@<主机>:<端口>/<数据库>
FASTAPI_DATABASE_URL=
FASTAPI_API_KEY=<可保持占位符，部署脚本会自动生成>
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
RAW_DATA_ROOT=data/raw
PYTDX_VIPDOC_PATH=D:\new_tdx64\vipdoc
```

不要把 `.env` 发给其他人，也不要提交到 Git。填写完成后再次执行；部署脚本会在 `.env` 中自动生成随机 `FASTAPI_API_KEY`：

```powershell
.\deploy.cmd
```

脚本会自动完成：

- 按 `uv.lock` 安装生产依赖；
- 创建 `.venv` 和 `market-data-center.exe`；
- 创建 `market-data-api.exe` 并检查两个命令行程序是否可运行；
- 创建或更新每天 18:30 运行的 `MarketDataCenter-Daily` 计划任务。

## 验证

启动只读 API：

```powershell
.\serve-api.cmd
```

浏览器打开 `http://127.0.0.1:8000/docs` 查看接口文档。业务请求必须携带 `.env` 中配置的 `X-API-Key`。

查看任务状态：

```powershell
Get-ScheduledTask -TaskName MarketDataCenter-Daily
Get-ScheduledTaskInfo -TaskName MarketDataCenter-Daily
```

需要立即采集一次时执行：

```powershell
.\deploy.cmd -RunNow
```

只安装程序、不创建计划任务：

```powershell
.\deploy.cmd -SkipTask
```

## 更新版本

保留原有 `.env` 和 `data/raw`，用新版本文件覆盖程序目录，然后重新执行：

```powershell
.\deploy.cmd
```

## 当前运行范围

- 股票日 K 只读取 `PYTDX_VIPDOC_PATH` 下的本地通达信文件；
- 每日任务只处理最近交易日，不回补历史日 K；
- 每日任务不计算衍生指标；
- 数据库迁移由 GitHub Actions 或管理员单独执行，本安装包不会修改数据库结构。

## 常见问题

`uv is required`：安装 `uv` 后重新打开终端。

`PYTDX_VIPDOC_PATH does not exist`：检查 `.env` 中的通达信目录，配置值应指向 `vipdoc` 文件夹。

数据库连接失败：检查 `DATABASE_URL`、Supabase PostgreSQL 端口和网络连通性，不要把连接串粘贴到 Issue 或日志中。
