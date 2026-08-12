# PostgREST `api_v1` 权限验证

## 目标

系统只通过 PostgREST 暴露 `api_v1`。`core`、`capital`、`classification`、
`derived`、`metrics`、`ingestion` 和 `audit` 是内部 Schema，不进入
PostgREST 暴露列表。客户端角色 `anon` 和 `authenticated` 只能读取
已发布 View、执行 ADR-0010 接受的只读 RPC；Worker 使用最小内部表权限。

## 自动验证

GitHub CI 的 `migrations` Job 在隔离 PostgreSQL 15 中执行：

```bash
uv run pytest -m integration
```

验证范围：

- `api_v1` 当前所有稳定 View 和 RPC 是否存在；
- `symbol` 加 `trade_date` 闭区间查询；
- `anon`、`authenticated` 可读取/执行 `api_v1` 查询契约，但不能读取
  内部 Schema 或写入 API View；
- `market_data_worker` 只有采集所需的表级权限；`DELETE` 仅授予完整快照替换所需的
  Classification/BoardIndex 成员表，也不依赖公开 API；
- 所有内部事实和审计表都启用 RLS，策略只授予
  `market_data_worker`；
- `supabase/config.toml` 仅暴露 `api_v1`。

## PostgREST 查询契约

稳定 RPC、参数边界、错误和版本规则见
`服务接口与Agent评估-2026-07-29.md`。原始日 K 闭区间查询仍可直接使用 View：

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

部署时确认 PostgREST 的公开 Schema 配置等价于：

```toml
[api]
schemas = ["api_v1"]
extra_search_path = ["extensions"]
```

若配置包含任一内部 Schema，必须停止部署并修正。

自托管环境还通过 migration 为 `authenticator` 角色设置等价的
`pgrst.db_schemas=api_v1` 与 `pgrst.db_extra_search_path=extensions`，并发送
PostgREST 配置/Schema 缓存重载通知。生产 smoke check 必须同时确认公开
`api_v1` 返回 200、请求内部 `core` 返回 406。
