# PostgREST `api_v1` 权限验证

## 目标

第一阶段只通过 PostgREST 暴露 `api_v1`。`core`、`ingestion` 和 `audit`
是内部 Schema，不进入 PostgREST 暴露列表。客户端角色 `anon` 和
`authenticated` 只能读取三个版本化 View；采集 Worker 使用
`market_data_worker` 的最小表权限。

## 自动验证

GitHub CI 的 `migrations` Job 在隔离 PostgreSQL 15 中执行：

```bash
uv run pytest -m integration
```

验证范围：

- `api_v1.securities`、`api_v1.trading_calendar` 和
  `api_v1.daily_bars` 的字段顺序；
- `symbol` 加 `trade_date` 闭区间查询；
- `anon`、`authenticated` 可读取 `api_v1`，但不能读取内部 Schema
  或写入 API View；
- `market_data_worker` 只有采集所需的 `SELECT`、`INSERT`、`UPDATE`
  权限，没有 `DELETE`，也不依赖公开 API；
- 所有内部事实和审计表都启用 RLS，策略只授予
  `market_data_worker`；
- `supabase/config.toml` 仅暴露 `api_v1`。

## PostgREST 查询契约

日 K 闭区间查询：

```text
GET /rest/v1/daily_bars
  ?select=symbol,trade_date,open,high,low,close,previous_close,volume,amount,trade_status,is_st
  &symbol=eq.SSE:600000
  &trade_date=gte.2026-07-01
  &trade_date=lte.2026-07-28
  &order=trade_date.asc
```

请求使用 publishable/anon Key。生产 Secret Key、JWT Secret 和数据库
连接串不得写入命令、日志、Issue、PR 或本文档。

## 部署检查

部署时确认 PostgREST/Supabase API 的公开 Schema 配置等价于：

```toml
[api]
schemas = ["api_v1"]
extra_search_path = ["extensions"]
```

若配置包含 `core`、`ingestion` 或 `audit`，必须停止部署并修正。
