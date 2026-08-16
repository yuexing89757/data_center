# 沪深 120 交易日收盘新高每日物化设计

- 状态：已确认，待实施
- 日期：2026-08-16
- Issue：#51
- ADR：ADR-0038

## 目标与非目标

目标是把沪深普通股票“当日收盘严格突破此前 119 个交易日最高收盘”的确定性结果每日物化，
让现有无参数 FastAPI 接口只读取最近 ready 快照。实现必须保留 Decimal、完整 120 日门禁、
版本与 lineage，并在成功零结果时仍能表达该交易日已完成。

不采集新来源，不补造缺失日 K，不纳入北交所，不提供策略标签、分页、调用方日期或请求时计算，
也不增加操作系统级定时任务。

## 架构与数据流

```text
core.trading_calendar + core.security + core.daily_bar + security_name_history
                              |
                              v
Persistence 有界聚合输入（日期范围索引，最多 10,000 证券 × 120 日）
                              |
                              v
纯 Calculator（资格、Decimal 比率、排序、输入/内容哈希）
                              |
                              v
Derivation Service 单事务发布 calculation + snapshot + members
                              |
                              v
api_v1 RPC -> FastAPI（最近 ready 快照）
```

Worker 的代码目录在 21:30 触发 Derivation Service。Service 先按 Asia/Shanghai 当前日期确认交易日，
再检查同日 `daily_market` 已进入 `succeeded` 或保留显式缺口的 `partial` 终态。APScheduler 使用现有
默认单线程 executor；若早先任务仍运行，本任务按既有 misfire grace 排队，不与日 K 写入并发发布。

## 数据模型

### `derived.close_price_new_high_120d_snapshot`

自然查询键是 `(trade_date, version)`，另以 `(trade_date, input_hash)` 保证同输入幂等。字段包括：

- `snapshot_id`、`calculation_id`；
- `trade_date`、`version`、`status='ready'`；
- `candidate_count`、`eligible_history_count`、`omitted_count`、`member_count`；
- `incomplete_history_count`、`non_trading_bar_count`、`nonpositive_price_count`、
  `missing_name_count`；分组可重叠，不要求总和等于遗漏数；
- `input_hash`、`content_hash`、`algorithm_version='1.0.0'`、`generated_at`。

约束保证版本和计数非负、member 不超过 eligible/candidate、哈希为 64 位小写十六进制。
索引 `(trade_date desc, version desc) where status='ready'` 支持 API 常量级定位。

### `derived.close_price_new_high_120d_member`

主键 `(snapshot_id, symbol)`；字段为 `display_name`、`close`、`previous_119d_high`、
`breakout_pct`。价格和比例使用 numeric/Decimal，约束 `close > previous_119d_high > 0`。
保存计算时有效名称，使历史版本不因名称事实后续修订而静默改变。

## 计算与幂等

最新目标日从 `core.daily_bar` 的 `(market, trade_date)` 索引倒序选择，并与交易日历、沪深普通股票
门禁联结；不得使用对日历全集执行 `max(...) + correlated exists` 的旧计划。120 日窗口先取得
首末日期，再以 `b.trade_date between first_day and last_day` 命中现有日期索引，只返回每证券聚合输入。

Calculator 接收按 symbol 排序的候选输入。有效记录数必须正好 120，当前状态和所有参与状态只允许
`trading/unknown`，价格必须为正且有当日历史名称。突破率公式为
`(close / previous_119d_high - 1) * 100`，按突破率降序、symbol 升序稳定排序。

输入哈希覆盖目标交易日、窗口边界及排序后的候选聚合输入；内容哈希覆盖排序后的成员输出。同一输入
直接返回既有 ready 快照；不同输入在 advisory transaction lock 下分配下一 version，单事务写入
calculation、snapshot 和 members。异常回滚整个事务，不产生半快照。

## 调度与运维

- Workflow code：`close_price_new_highs_120d`；step：`build_close_price_new_highs_120d_snapshot`。
- Job ID：`close-price-new-highs-120d-daily`。
- 固定计划：周一至周五 21:30，Asia/Shanghai，默认启用。
- 唯一配置：`CLOSE_PRICE_NEW_HIGHS_120D_ENABLED`；不提供 hour/minute 配置。
- 非交易日正常跳过；缺依赖、窗口不足、候选超过 10,000 或硬数据不变量失败时，Operations 记录失败，
  不发布 ready，不回退旧输入计算。
- CLI 提供受控单次构建并要求显式 `--trade-date`；部署时用它生成首个快照。

## API 契约

路径和响应模型保持兼容。`api_v1.query_close_price_new_highs_120d()` 只选择最近 ready 快照及其成员，
仍设置 10 秒超时并仅授权 `market_data_api`。返回 trade date、120/119 会话数、候选/有效/遗漏/返回
计数、遗漏分组和全部成员。FastAPI 仍执行事务局部 10 秒 statement timeout，但数据库查询不再读取
日 K。没有 ready 快照时返回既有 not-found 映射，不实时回退计算。

## 测试与验收

- Calculator 单元测试：严格突破、相等排除、Decimal、状态、缺失/非正、排序和哈希确定性。
- Service/Persistence 测试：成功零成员、同输入幂等、修订递增版本、事务回滚和权限边界。
- Scheduler/Operations 测试：21:30 固定目录、默认启用、非交易日跳过、依赖失败、函数映射和重启注册。
- PostgreSQL 集成测试：迁移空库/幂等、RLS、最近 ready 选择、API 不读取旧版本及成员完整性。
- 契约测试：FastAPI/PostgREST/Agent schema 保持同步。
- 生产验收：应用唯一迁移，部署 Worker/API，手工生成最新快照；接口 HTTP 200、耗时小于 10 秒，
  API 与 Worker 均 active，任务页显示下一次 21:30 运行。

## 上线与回退

先应用 migration，再部署兼容新旧 RPC 的代码，手工构建首个快照，最后在线验证。旧发布目录保留用于
代码回退；新表和历史快照不删除。若新任务失败，停用代码目录开关并回退 API/Worker，保留 Operations
失败事实和已发布不可变快照供诊断。
