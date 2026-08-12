# ADR-0010：PostgREST 稳定查询与 Agent 工具契约

- 状态：Superseded（服务层决策由 ADR-0011 替代；RPC 契约继续沿用）
- 日期：2026-07-29
- 关联 Issue：#13

## 背景

研究脚本、看板、回测和 AI Agent 需要稳定查询接口。当前所有已接受事实和派生结果均位于 PostgreSQL，PostgREST 已提供 OpenAPI 和 View/RPC 暴露能力。新增 FastAPI 或 MCP 会增加第二套认证、权限、部署、限流和版本边界，必须由 PostgREST 无法满足的真实用例驱动。

## 决策

1. 当前继续使用 PostgREST，不引入 FastAPI 或 MCP。所有客户端只依赖 `api_v1` View/RPC，不查询 `core`、`capital`、`classification`、`derived`、`metrics`、`ingestion` 或 `audit`。
2. 发布五个稳定、只读且有界的 RPC：
   - `query_securities`
   - `query_daily_bars`
   - `query_adjusted_daily_bars`
   - `query_market_snapshot`
   - `query_classification_members_as_of`
3. 复权行情和市场快照必须解析为一个成功的 `calculation_id`，不得从多个计算批次拼接。调用方可固定计算 ID；省略时选择覆盖请求日期且算法版本匹配的最新成功批次。
4. 分类成员查询选择 `snapshot_date <= as_of_date` 的最新完整快照，并返回快照日期、完整成员数、实际返回数和成员数组。空快照返回空数组，不与“无历史快照”混淆。
5. 所有 RPC 使用 `SECURITY INVOKER`，只从 `api_v1` 查询，撤销 public 执行权限，仅授予现有 `anon`、`authenticated` 角色（在纯 PostgreSQL 部署中为可选，缺失时这些授予为 no-op）。生产数据库凭据不进入客户端契约。
6. 数据库硬限制：证券搜索最多 100 行；其他 RPC 最多 5000 行；单证券日期范围最多 3661 天；每个 RPC 的 PostgreSQL statement timeout 为 5 秒。更大的回测读取必须分页；出现稳定的跨 PostgreSQL/Raw 或大文件导出需求时再评估服务层。
7. 错误语义固定为：`22023` 表示参数或边界无效，`P0002` 表示没有兼容计算版本或历史快照，`42501` 表示权限不足，`57014` 表示超时。错误不得包含 SQL、内部 Schema、凭据或原始数据。
8. `api_v1` 兼容策略是只做向后兼容的新增字段、View 或带默认值参数。删除/重命名字段、改变单位/含义或收紧已有成功输入时，创建新 RPC 名称或 `api_v2`，并保留明确弃用窗口。
9. 仓库发布 OpenAPI 3.1 描述和 Agent 工具 JSON Schema。Agent 工具只是 PostgREST RPC 的只读调用契约，不是 MCP Server；它不包含 Base URL、Key、Token 或数据库位置。
10. 可观测性继续使用 PostgREST 请求日志、数据库慢查询/统计和 RPC SQLSTATE。数据库行数/日期上限已经强制；网关级每身份/IP 速率配额属于部署配置与 Issue #15，不在数据库中维护可绕过或竞争的计数器。
11. 只有出现以下已量化缺口时才创建新 ADR 引入 FastAPI/MCP：跨 PostgreSQL 与 Raw/Parquet 编排、持续超过 PostgREST 能力的缓存/限流、长任务/流式导出、协议转换，或 MCP 客户端确实无法使用现有工具调用边界。

## 结果

- 研究脚本、看板、回测和 Agent 共用一套查询语义和权限边界。
- 版本化派生结果不会在一次响应中混用计算批次。
- 请求成本由数据库硬边界控制，契约可自动校验且不包含 Secret。
- FastAPI/MCP 保持延后，避免无真实需求的空壳服务。
