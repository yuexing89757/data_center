# 龙虎榜领域详设（2026-08-18）

## 边界与自然键

| 事实 | 逻辑自然键 | 修订方式 |
|---|---|---|
| 来源快照 | `(trade_date, version)`；同日 `input_hash` 唯一 | 内容变化新增 version |
| 来源观察 | `(snapshot_id, source_event_key)`，同快照 symbol 唯一 | 跟随来源快照新增 |
| 龙虎榜事件 | `(symbol, trade_date, revision)`；同快照 symbol 唯一 | revision 等于来源快照版本 |
| 原因 | `(event_id, reason_code)`，display_order 唯一 | 新事件修订重新冻结 |
| 席位身份 | `(identity_key, valid_from)` | 名称/归一化变化新增有效期 |
| 席位活动 | `(event_id, seat_id)`，source_order 唯一 | 新事件修订重新冻结 |
| 客观汇总 | `(event_id, calculation_version)` | 算法变化新增 calculation_version |

`source_event_key` 和来源席位名称只作为边界证据；跨领域连接使用标准 `symbol` 和稳定
`identity_key`。`event` 的 `historical_name` 必须来自该交易日有效的名称事实，不能使用当前名
回填历史。`unadjusted_close` 必须与同日 `core.daily_bar.close` 一致；本基础层尚未实现来源采集，
未来服务必须在提交前完成这些交叉验证。

## 数值和客观汇总

- 价格：`numeric(18,4)` CNY/股。
- 金额：`numeric(30,4)` CNY。
- 涨跌幅、换手率：`numeric(24,10)` percentage points。
- `net_amount_cny = buy_amount_cny - sell_amount_cny`，由模型和数据库双重约束。
- 机构总额只包含 `seat_type=institution` 的活动。
- `top5_buy_concentration_ratio`：当前事件所有已存活动按 `buy_amount_cny` 降序前五项之和，
  除以这些已存活动的买入总额；卖出同理。总额为 0 时比例必须为 NULL。
- 来源仅给出部分席位时，快照必须 partial；比例仍可重算，但只能解释为“已存活动范围”。

## 缺失、质量与 Raw

完整快照不得携带 partial 原因；partial 快照必须至少一个非空原因。事件、原因和活动均指向
同一来源 observation、ingestion 与 Raw manifest。硬错误包括未知证券、非交易日、重复自然键、
孤立原因/活动、未知席位引用和不可重算汇总；它们阻止规范事实提交。来源确实不提供原因或席位
时不造值，应把快照标为 partial 并写质量原因。

Raw 文件保持不可变。未来 provider adapter 只能把来源字段转换为本文件定义的标准单位/类型；
来源字段名、方向代码和页面结构不得泄漏到规范表。一个 ingestion 只对应一个实际 provider。

## 持久化与权限

`PostgreSQLDragonTigerPersistence.commit_snapshot` 对已验证的 terminal run 执行：同日 advisory
lock → 幂等 hash 检查 → Ingestion/Manifest/Quality → source snapshot/observation → seat identity →
event/reason/activity/summary。所有新事实处于一个事务；任何失败整体回滚。席位既有有效期记录只有
完全一致时复用，冲突要求调用方创建新有效期修订。

所有 `dragon_tiger` 表启用 RLS。Worker 仅 SELECT/INSERT；无更新、删除、公开 schema grant、
FastAPI RPC 或 PostgREST 暴露。

## 后续门禁

实际采集前必须另行完成 provider ADR/字段样本验证、许可与限频、Raw schema、空结果与反爬错误
区分、历史覆盖范围和运行调度决策。对外 API、席位标签和任何策略评分分别立项。
