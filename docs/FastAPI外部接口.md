# FastAPI 外部只读接口

## 定位

FastAPI 是现有 Supabase `api_v1` 查询契约的外部 HTTP 层。它只读取数据，不触发采集、不写数据库，也不计算 Derived/Metrics。

## 配置

`.env` 至少增加：

```dotenv
FASTAPI_API_KEY=<至少16位随机字符串；deploy.cmd 可自动生成>
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
```

API 默认复用 `DATABASE_URL`。生产环境建议配置独立只读连接：

```dotenv
FASTAPI_DATABASE_URL=postgresql+psycopg://<只读用户>:<密码>@<主机>:<端口>/<数据库>
```

## 启动

前台启动：

```powershell
.\serve-api.cmd
```

临时覆盖监听地址和端口：

```powershell
.\serve-api.cmd -HostAddress 0.0.0.0 -Port 8000
```

健康检查和交互文档：

- `GET /healthz`
- `GET /readyz`
- `GET /docs`
- `GET /openapi.json`

## 认证

业务接口必须携带：

```http
X-API-Key: <FASTAPI_API_KEY>
```

示例：

```powershell
$headers = @{ "X-API-Key" = "<你的API Key>" }
Invoke-RestMethod `
  -Uri "http://127.0.0.1:8000/api/v1/securities?query=600000&limit=20" `
  -Headers $headers
```

## 接口

### 证券搜索

```http
GET /api/v1/securities?query=浦发&limit=20
```

证券最多返回 100 条。

### 未复权日 K

```http
GET /api/v1/daily-bars/SSE:600000?start_date=2026-07-01&end_date=2026-07-31&limit=1000
```

日期范围最多 3661 天，最多返回 5000 条。价格和金额使用 JSON 十进制字符串，避免浮点精度损失。

### 分类成员

```http
GET /api/v1/classifications/tdx/industry/T1001/members?as_of_date=2026-07-29&limit=5000
```

`classification_type` 允许 `industry`、`concept` 或 `index`。接口返回不晚于指定日期的最近完整快照。

## 对外部署

应用默认只监听 `127.0.0.1`。正式对外提供时，建议由 Nginx、Caddy 或现有网关负责 HTTPS、来源 IP 限制和限流，再反向代理到 FastAPI。不要把 PostgreSQL 端口直接暴露给 API 使用者。

当前应用不包含 Windows 服务管理器；`serve-api.cmd` 是前台进程。需要无人值守运行时，应由 Windows 服务管理器或受控进程管理器托管该命令。
