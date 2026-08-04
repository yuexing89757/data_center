# ADR-0011：FastAPI 外部只读接口

- 状态：Accepted
- 日期：2026-08-01
- 关联 Issue：#26
- 替代范围：替代 ADR-0010 中“不引入 FastAPI”的服务层决策；ADR-0010 的 `api_v1` RPC、查询边界、版本和错误语义继续有效

## 背景

PostgREST 已经为内部消费者提供稳定查询契约，但外部调用方需要独立于 Supabase 的服务地址、应用级 API Key、健康检查、统一错误响应和面向调用方的 OpenAPI 文档。该需求已经超出直接暴露 PostgREST 的部署边界，满足 ADR-0010 所要求的“由真实外部服务用例驱动”的条件。

## 决策

1. 新增 FastAPI 作为外部只读协议层。FastAPI 不参与 Provider 采集、Validator 校验、Persistence 写入、Raw 重放或 Derived/Metrics 计算。
2. FastAPI 只调用既有 `api_v1` 有界查询函数，不直接查询或暴露 `core`、`capital`、`classification`、`derived`、`metrics`、`ingestion` 和 `audit` Schema。
3. 第一版只发布当前仍持续维护的事实查询：
   - `GET /healthz`；
   - `GET /readyz`；
   - `GET /api/v1/securities`；
   - `GET /api/v1/daily-bars/{symbol}`；
   - `GET /api/v1/classifications/{namespace}/{classification_type}/{classification_code}/members`。
4. 不发布复权行情和市场指标接口。已有 Derived/Metrics 数据及 RPC 保留，但当前跳过计算，避免外部调用方把旧计算批次误认为持续更新的数据。
5. 除 `/healthz` 和 `/readyz` 外，所有接口必须使用 `X-API-Key`。API Key 只从 `FASTAPI_API_KEY` 环境变量读取，使用常量时间比较，不写入日志、OpenAPI 示例或仓库。
6. 生产环境优先使用独立的只读 `FASTAPI_DATABASE_URL`；未配置时允许复用 `DATABASE_URL` 以便本地试运行。服务代码即使使用高权限连接，也只能执行固定的 `api_v1` RPC。
7. 保留 ADR-0010 的数据库硬边界：证券最多 100 行；日 K 和分类成员最多 5000 行；日 K 日期范围最多 3661 天；数据库语句超时继续为 5 秒。
8. 价格、金额继续使用 `Decimal`，JSON 输出为十进制字符串，不经过二进制浮点数。
9. 数据库参数错误映射为稳定的客户端错误；无历史快照映射为 404；查询超时映射为 504；其他数据库错误映射为 503。响应不得包含 SQL、内部 Schema、数据库地址或凭据。
10. FastAPI 自动生成 OpenAPI。`/docs` 和 `/openapi.json` 可公开描述契约，但真实数据接口仍需 API Key。
11. TLS、IP 限流、访问日志保留周期和密钥轮换由反向代理/网关及安全治理 Issue #15 负责；应用内不实现不可共享的进程内限流器。
12. FastAPI 与采集 Worker 分进程运行。Windows 的既有 18:30 采集任务不启动 API 服务；API 使用独立启动脚本。

## 结果

- 外部消费者获得稳定、可认证的 HTTP/OpenAPI 边界，同时数据库查询语义仍只有一份。
- 采集故障与 API 进程相互隔离，外部请求不能触发采集或写入。
- 当前跳过衍生指标的决定不会因新增 API 被绕过。
- 后续新增写接口、导出任务、WebSocket、MCP 或新的数据集接口，必须另建 Issue 并重新评估 ADR。
