# ADR-0045：腾讯实时五档 API 请求时直连且不落库

- 状态：Accepted
- 日期：2026-08-23
- 关联 Issue：#63
- 决策者：项目所有者
- 替代：ADR-0044 第 8～10 条及其 FastAPI 数据库读取结论
- 例外：ADR-0011“FastAPI 只调用 api_v1 RPC”的边界，仅限本接口

## 背景

ADR-0044 首版要求先显式采集并持久化，再由 FastAPI 查询新鲜快照。生产验证显示，在没有
持续 Worker 任务时该接口必然返回 `missing_codes`；而项目所有者明确要求本接口在请求时
实时获取腾讯数据且不保存数据库。

## 决策

1. `POST /api/v1/realtime-quotes/latest/query` 每次请求直接调用固定腾讯批量行情端点，不读取
   `api_v1.query_latest_stock_quotes`、`realtime.stock_quote_snapshot` 或 Security 表。
2. 请求路径不创建 IngestionRun、不保存 Raw、不写 PostgreSQL、不触发 Worker，也不使用
   数据库快照兜底。该返回值是临时外部响应，不是数据中心可追溯的持久事实，不得作为派生
   计算或历史重放输入。
3. 继续要求 `X-API-Key`，保留 1～500 个去重六位代码、每腾讯请求最多 50 只、GBK 严格
   解码、2 MB 响应上限、Decimal 和手转股语义。FastAPI 总截止时间为 1～15 秒，默认 8 秒。
4. 股票代码以确定性规则路由：`6` 开头为 SSE，`0`/`3` 开头为 SZSE；其他代码不访问腾讯并
   进入 `missing_codes`。不查询数据库消歧，也不支持 BSE。
5. `max_age_seconds` 为兼容既有客户端暂时保留并原样回显，但实时请求不使用它过滤本次结果。
   腾讯 `source_timestamp` 仍原样返回，闭市时可能是最近交易时点。
6. 腾讯整体不可用或全部支持代码均请求失败时返回 502 `upstream_error`；部分批次成功时返回
   已成功项目，其他代码进入 `missing_codes`。不泄露上游异常文本。
7. ordered migration 删除已发布的 `api_v1.query_latest_stock_quotes(text[],integer)`，并同步移除
   PostgREST/Agent 契约，防止消费者继续依赖持久化读取。既有空表与显式 Worker 采集能力暂时
   保留，后续删除需独立迁移，不在本次做破坏性数据清理。

## 后果

- 接口延迟和可用性直接受腾讯影响，返回结果不具备 Raw/ingestion lineage。
- FastAPI 仍不获得数据库写权限；此接口的 Provider 只存在于 API 进程内。
- 数据中心其他公共接口继续遵守 ADR-0011 的有界 `api_v1` RPC 读取边界。
