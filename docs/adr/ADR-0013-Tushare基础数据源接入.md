# ADR-0013：Tushare 基础数据源接入

- 状态：Accepted
- 日期：2026-08-02
- 关联 Issue：#28
- 决策者：项目所有者
- 影响：扩展 ADR-0002 的显式可选 Provider；不改变 ADR-0004/ADR-0005 的默认路由

## 背景

项目需要一个带鉴权、字段契约相对稳定的数据源，用于证券目录、交易日历和未复权
日 K 的显式采集与多源质量核对。Tushare Pro 提供这些能力，但其证券代码、日期格式、
上市状态及成交量/成交额单位均与领域模型不同，且访问依赖个人 Token 和接口积分权限。

用户提供的远程 MCP 适合 Agent 交互查询，但数据中心生产采集必须继续通过 Provider
协议进入 Raw、校验、Core 和审计闭环，不能让 MCP 响应绕开 Pipeline 或成为领域模型。

## 决策

1. 新增普通 Provider `tushare`，首期实现 `Security`、`TradingCalendar` 和未复权股票
   `DailyBar`；Capital、Classification 与实时行情明确返回 `ProviderRequestUnavailable`。
2. Adapter 使用 Tushare Pro 官方 JSON API，避免引入 SDK 的 Pandas/隐式浮点转换。Token 只从 `TUSHARE_TOKEN` 环境变量读取，
   不写入代码、请求参数、Raw、日志、异常或数据库。
3. `stock_basic` 分别请求 `L`、`D`、`P` 状态并合并；`ts_code` 在边界内映射为
   `SSE|SZSE|BSE:NNNNNN`，上市/退市日期映射为领域日期。
4. `trade_cal` 请求完整自然日区间。响应必须覆盖请求中的每个自然日，否则当前请求
   失败；不得把响应缺口静默解释为休市。
5. `daily` 必须请求未复权日线并升序输出。来源 `vol` 的单位为“手”，转换为“股”时
   乘以 100；来源 `amount` 的单位为“千元”，转换为“元”时乘以 1000。价格和金额
   必须从字符串构造 `Decimal`，不得经过 `float`。
6. Raw schema 分别固定为 `tushare.security.v1`、`tushare.trading_calendar.v1` 和
   `tushare.daily_bar.v1`，并提供版本化 normalizer 以支持重放。
7. `tushare` 加入显式 Provider 注册表和 CLI 选项，但不加入 ADR-0005 的自动路由。
   股票 Daily Bar 的自动来源仍仅为本地 `pytdx`。
8. 数据库 migration 仅扩展现有 Provider/source check constraint，不改变自然键、
   PostgREST 契约或 Core 表结构。
9. Tushare 的接口积分、频率限制和历史覆盖属于来源能力；权限不足、限流、空响应或
   字段缺失必须作为可诊断的 `ProviderError`，不得自动切换来源或伪造空事实。

## 后果

- Tushare 可用于显式补充目录、日历和多源核对，同时保持默认生产路由不变。
- 使用者必须自行提供有相应积分权限的 Token，并遵守 Tushare 的使用与再分发条款。
- MCP 与 Provider 各自独立：MCP 配置不会自动成为 Worker 凭据，Worker 仍需
  `TUSHARE_TOKEN`。

## 验收

- Adapter 测试覆盖证券状态、代码映射、日历完整性、日 K 排序、Decimal 和单位转换；
- 缺失 Token、来源异常、字段缺失、不支持数据集和 Raw 重放均有测试；
- migration 允许 `provider_code/source_code=tushare`；
- Ruff、mypy、pytest 和 migration 检查通过。
