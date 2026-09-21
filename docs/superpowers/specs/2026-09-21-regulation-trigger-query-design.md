# 监管异动触发与下一交易日条件查询设计

**日期：** 2026-09-21

**状态：** 已确认设计，尚未实施

**领域：** Regulation

**关联 Issue：** [#83](https://github.com/yuexing89757/data_center/issues/83)

**治理前置：** 以 clarification 形式更新已接受的
`ADR-0048-沪深主板与创业板监管异动规则测算`。

## 1. 目标

为外部消费者提供两个有界、只读、精确日期的 FastAPI 接口：

1. 查询某一交易日收盘后，本系统按有效交易所规则计算达到普通异常或严重异常条件的股票；
2. 查询截至该交易日最近 30 个交易日内，被交易所正式公告认定发生过异动的股票，以及这些股票
   在下一交易日、三个基准指数情景下再次达到普通异常或严重异常条件所需的收盘价和涨跌幅。

首个结果目标在交易日 22:00 工作流完成后发布。晚到或新发布的官方事件由次日 08:30 对账任务
处理；输入发生变化时生成新的计算版本，旧版本继续保留。

本能力只给出可追溯的规则条件测算，不预测股价，不预测交易所认定，也不推断停牌、重点监控或
其他监管措施。

## 2. 已确认口径

- “当日触发”是本系统根据当日收盘事实计算达到规则条件，使用 `calculated_state` 表达。
- “近 30 日有异动”只接受交易所正式公告认定，不能使用行情推断、龙虎榜原因、媒体或第三方标签。
- “近 30 日”是包含查询日在内、截至查询日的最近 30 个交易日，不是 30 个自然日。
- 普通异常 `ABNORMAL` 与严重异常 `SERIOUS_ABNORMAL` 分别计算和返回。
- T+1 条件同时返回基准指数下跌 2%、持平、上涨 2% 三种情景。
- 查询日必须精确匹配已发布批次，不回退到更早日期。
- API 只读取数据库，不触发采集、重算或外部网络访问。
- 第一阶段继续遵循 ADR-0048 的适用范围：沪市主板、深市主板和创业板普通股票；科创板、北交所、
  ST/*ST、退市整理期及无涨跌幅限制交易日不适用。

## 3. 当前实现与缺口

现有代码已经具备：

- `regulation.rule`、`event`、`calculation_run`、`status`、`rule_result`、`warning` 六张内部表；
- 2026-07-06 起生效的 26 条版本化规则；
- 累计价格偏离、换手率复合条件、正式事件次数和 T+1 情景的纯计算器；
- 基准指数日线采集、版本化计算服务、持久化和 22:30 的可选 Worker 任务；
- 手工 `regulation-calculate` 命令与核心单元测试。

当前缺口是：

- SSE/SZSE 官方异动事件 Provider 尚未实现；
- Worker 当前只采集基准指数并执行计算，没有采集官方事件或执行依赖校验；
- 08:30 官方事件对账任务尚未实现；
- ADR-0048 中规划的公开 Regulation RPC 和 FastAPI 查询接口尚未实现；
- `REGULATION_DAILY_ENABLED` 默认关闭；
- 当前触发规则若已在查询日达到条件，只保存 `CURRENT` 提示，不应伪造“下一次”触发价。

本设计补齐这些缺口，但不引入新分析领域、消息队列、动态规则编辑器或在线计算接口。

## 4. 总体架构

```text
20:00 daily_market 完成
          ↓
22:00 regulation_daily_calculation
          ├─ collect_sse_regulation_events
          ├─ collect_szse_regulation_events
          ├─ collect_regulation_benchmarks
          ├─ validate_regulation_dependencies
          ├─ calculate_regulation_status
          └─ publish_regulation_results
                          ↓
                calculation_id 原子发布
                          ↓
      api_v1 RPC → FastAPI 只读接口 → 消费者

次日 08:30 regulation_event_reconciliation
          ↓
官方事件水位变化 → 对受影响日期重新计算 → 新 calculation_id
```

所有调度继续运行在 Worker 的 APScheduler 中。不得增加 cron、Windows Task Scheduler 或其他
操作系统级采集任务。

## 5. 官方事件采集

### 5.1 Provider

实现两个独立 Provider：

- `SSEOfficialRegulationEventProvider`，`source_code=sse_official`；
- `SZSEOfficialRegulationEventProvider`，`source_code=szse_official`。

每个 Provider 只访问对应交易所固定、允许列举的官方主机，不参与自动路由或跨来源回退。SSE 与
SZSE 分开创建 IngestionRun、Raw 和失败结果；不得把两个 Provider 的部分记录合并成一次成功采集。

### 5.2 接受证据

只有以下内容可以标准化为 `RegulationEventRecord`：

1. 交易所公开交易信息中明确披露的异常或严重异常结论；
2. 交易所官网托管且正文明确记录交易所计算结论的公告。

标题命中但正文没有明确结论的内容只进入 Raw，不进入 `regulation.event`。方向只能来自官方正文
明确的有符号偏离描述，不能根据日线涨跌推断。

### 5.3 Raw、幂等与更正

- 原始响应和公告正文以既有 Raw/manifest/ingestion 机制不可变保存；
- 相同 `source_code + source_event_id + source_content_hash` 重复抓取时幂等跳过；
- 相同 `source_event_id` 但正文哈希变化时保存新 Raw、登记硬质量冲突、阻断标准事件发布，不覆盖
  已有事件；
- 交易所以新的官方事件编号发布更正时追加新事件，并由对账任务对受影响日期生成新计算版本；
- 旧 Raw、旧事件和旧 CalculationRun 永久保留以供审计。

## 6. 计算与版本发布

### 6.1 输入快照

`PostgreSQLRegulationPersistence` 在一个 `REPEATABLE READ` 输入事务内固定：

- 查询日和下一交易日；
- 生效规则及规则集哈希；
- 股票、名称历史和板块；
- 最多 30 个交易日的未复权日线；
- 对应基准指数日线；
- 每日换手率指标；
- 公司行动和下一交易日参考价；
- 截至事件水位已正式发布的交易所事件。

离开输入事务后调用无 I/O 的 `calculate_regulation()`，再在一个写事务中原子发布
`calculation_run`、`status`、`rule_result` 和 `warning`。

### 6.2 版本选择

- 输入哈希相同则复用已有 CalculationRun；
- 官方事件、行情、Capital、规则集或计算算法变化时生成新的 CalculationRun；
- API 对精确 `trade_date` 选择 `SUCCEEDED` 或 `PARTIAL` 中最新完成的版本；
- 排序键为 `completed_at DESC, calculation_id DESC`，保证确定性；
- `FAILED` 和 `RUNNING` 批次不得公开；
- API 返回 `calculation_id`、输入水位、算法版本、规则集版本和覆盖数，使结果可追溯。

### 6.3 T+1 语义

价格偏离规则继续为三个指数情景求解：

- `INDEX_DOWN_2`：基准指数 -2%；
- `INDEX_FLAT`：基准指数 0%；
- `INDEX_UP_2`：基准指数 +2%。

上涨触发价按 0.01 元向上取整，下跌触发价按 0.01 元向下取整；取整后重新验证规则条件，并与
下一交易日涨跌停价比较。

结果状态包括：

- `REACHABLE_NEXT_SESSION`；
- `NOT_REACHABLE_NEXT_SESSION`；
- `NOT_PRICE_CALCULABLE`，例如换手率条件不能仅由收盘价求解；
- `CURRENTLY_TRIGGERED`，表示查询日已经达到该规则，不伪造下一次触发价。

`CURRENTLY_TRIGGERED` 是公开响应对现有 `scenario_code=CURRENT` 的稳定映射，不修改内部
`regulation.warning.reachability` 枚举。

次数型严重异常的 T+1 路径必须返回
`requires_official_event_confirmation=true`，因为是否计入次数仍以交易所正式认定为准。

## 7. API 一：当日收盘触发列表

### 7.1 请求

```http
GET /api/v1/regulation/triggers?trade_date=2026-09-18&limit=200&cursor=...
X-API-Key: <secret>
```

参数：

- `trade_date`：必填，ISO 日期，且不得早于规则集生效日；
- `limit`：默认 100，范围 1–500；
- `cursor`：可选、不透明、与本接口及查询参数绑定的稳定游标。

### 7.2 股票聚合语义

一只股票只返回一个 item。所有 `triggered=true` 且数据完整的逐规则结果聚合到
`triggered_rules`。若至少一条严重异常规则触发，股票状态为 `SERIOUS_TRIGGERED`；否则为
`ABNORMAL_TRIGGERED`。

`announced_state` 只反映该 CalculationRun 的事件水位以内，交易所是否已经正式公告认定，不把
本系统计算结果转换成正式事件。

### 7.3 响应字段

顶层字段：

- `trade_date`
- `calculation_id`
- `calculation_status`
- `completed_at`
- `event_watermark`
- `algorithm_version`
- `rule_set_version`
- `coverage.expected_count`
- `coverage.complete_count`
- `coverage.incomplete_count`
- `coverage.not_applicable_count`
- `returned_count`
- `next_cursor`
- `items`

每个 item：

- `code`、`symbol`、`name`、`exchange`、`segment`
- `calculated_state`、`announced_state`
- `triggered_rules[]`

每条 `triggered_rules`：

- `rule_code`、`level`、`direction`、`kind`
- `window_start_date`、`window_end_date`、`observed_window_days`
- `current_value`、`threshold`
- `secondary_current_value`、`secondary_threshold`
- `event_count`、`required_count`
- `selected_reset_date`

结果按严重异常优先、普通异常其次、`symbol` 升序排列。游标编码状态等级和 symbol，客户端不得
解析游标内部结构。

## 8. API 二：近 30 个交易日正式异动及下一次触发条件

### 8.1 请求

```http
GET /api/v1/regulation/recent-events/next-triggers?trade_date=2026-09-18&limit=200&cursor=...
X-API-Key: <secret>
```

回看窗口固定为 30 个交易日，不开放任意窗口参数。窗口包含查询日；
`lookback_start_date` 是这 30 个交易日中最早的一日。

### 8.2 事件与计算快照一致性

只选择同时满足以下条件的正式事件：

- `period_end_date` 位于 30 个交易日集合中；
- `observed_at <= calculation_run.event_watermark`；
- 事件来源和证据满足官方事件约束；
- 事件自然键在该水位下唯一。

因此接口不会把 CalculationRun 发布后才观察到的事件拼入旧计算版本。晚到事件必须先触发新版本
计算，才能出现在接口响应中。

### 8.3 响应字段

顶层除版本和覆盖字段外，还返回：

- `trade_date`
- `next_trade_date`
- `lookback_trading_days=30`
- `lookback_start_date`
- `returned_count`
- `next_cursor`
- `items`

每个股票 item：

- `code`、`symbol`、`name`、`exchange`、`segment`
- `official_event_count_30d`
- `latest_source_event_id`
- `latest_event_date`
- `latest_event_published_at`
- `latest_event_level`
- `latest_event_direction`
- `latest_event_source_title`
- `latest_event_source_url`
- `next_triggers[]`

每条 `next_triggers`：

- `level`、`direction`、`rule_code`
- `benchmark_symbol`
- `scenario_code`、`scenario_index_pct`
- `next_day_reference_price`
- `raw_trigger_price`
- `trigger_price`
- `trigger_change_pct`
- `lower_limit_price`、`upper_limit_price`
- `reachability`
- `window_start_date`、`window_end_date`
- `requires_official_event_confirmation`

结果按 `latest_event_date DESC, symbol ASC` 排列。游标编码最近事件日期和 symbol。

## 9. 数据库与公开边界

不新增重复的结果表，继续使用现有 Regulation 六表。通过新的 ordered migration 完成：

1. 为跨股票的 30 日事件查询增加 `(period_end_date DESC, symbol)` 索引；
2. 增加 `api_v1.query_regulation_triggers` 有界只读 RPC；
3. 增加 `api_v1.query_regulation_recent_event_next_triggers` 有界只读 RPC；
4. 为公开 API 数据库角色只授予两个 RPC 的执行权限；
5. 保持 `regulation` schema 和内部表不可被公开角色直接读取；
6. 同步 `contracts/postgrest-openapi-v1.json`、`contracts/agent-tools-v1.json` 和
   `contracts/fastapi-openapi-v1.json`。

FastAPI 只能调用上述 `api_v1` RPC，不得直连 `regulation`、`core`、`capital`、`metrics` 或其他
内部 schema。

## 10. 调度与依赖

### 10.1 收盘任务

`regulation-daily-calculation` 从工作日 22:30 调整为工作日 22:00，时区固定为
`Asia/Shanghai`。只有当查询日是交易日时执行实际采集和计算。

工作流步骤按以下顺序执行并记录 Operations 事实：

1. `collect_sse_regulation_events`
2. `collect_szse_regulation_events`
3. `collect_regulation_benchmarks`
4. `validate_regulation_dependencies`
5. `calculate_regulation_status`
6. `publish_regulation_results`

同日 `daily_market` 必须已经处于 `SUCCEEDED` 或允许的 `PARTIAL` 终态。依赖尚未终态时本次任务
失败并保留可诊断错误，不读取旧日期行情代替。

### 10.2 次日对账

新增 `regulation-event-reconciliation`，工作日 08:30 执行并默认关闭，直到来源权利和生产启用
审查完成。

- 无新增事件时幂等成功，不重算；
- 新官方事件或使用新事件编号发布的更正影响历史状态时，对受影响日期至当前日期内最多 30 个
  交易日逐日创建新计算版本；
- 相同事件编号正文哈希冲突时保存 Raw、任务失败并要求人工审查，不自动覆盖事实；
- 不重新采集普通股票日线，不修改旧 CalculationRun。

## 11. 错误与数据质量语义

- `422 Unprocessable Entity`：日期格式、limit 或游标非法；
- `404 Not Found`：指定日期不存在可发布的 `SUCCEEDED` 或 `PARTIAL` 批次；
- `503 Service Unavailable`：数据库或 RPC 不可用；
- `200 OK + calculation_status=PARTIAL`：批次已发布但存在不完整证券。

缺失价格、比例、日期或事件字段保持 `null`，不得替换为零。数据不完整的股票不进入数值触发列表，
但顶层 coverage 必须明确暴露不完整数量。公开错误不得包含数据库连接信息、内部 SQL、凭据或Raw
路径。

所有 FastAPI datetime 字段使用共享 `ApiTimestamp`，按 `Asia/Shanghai` 输出
`YYYY-MM-DD HH:mm:ss`；日期字段保持 `YYYY-MM-DD`。

## 12. 验收测试

### 12.1 Provider

- SSE/SZSE 官方页面和正文的 mocked 正常样本；
- 普通异常、严重异常、多条明确原因和方向不明确事件；
- 标题命中但正文无明确结论时不发布标准事件；
- 重复页面、重复事件、相同 ID 相同哈希幂等；
- 相同 ID 不同哈希保存 Raw 并阻断标准发布；
- 分页上限、响应大小、主机白名单、超时和格式错误；
- Raw replay 不访问网络且经过同一标准化与校验路径。

### 12.2 Calculator

- 沪深主板 3 日 ±20% 和创业板 3 日 ±30% 的精确边界；
- 10 日 +100%/-50%、30 日 +200%/-70%；
- 正式事件次数、方向去重、普通/严重重置边界；
- 复合收益差，禁止简单相加每日偏离；
- 三个指数情景、0.01 元方向取整和涨跌停可达性；
- 换手率规则返回 `NOT_PRICE_CALCULABLE`；
- 次数路径设置 `requires_official_event_confirmation=true`；
- 当日已触发返回 `CURRENTLY_TRIGGERED`，不生成伪造价格；
- ST、上市初期、科创板、北交所和输入缺失语义。

### 12.3 PostgreSQL 与 API

- migration、索引、RLS、grant 和两个 RPC；
- 精确日期、不回退、最新已发布 calculation 选择；
- 最近 30 个交易日而非自然日；
- event watermark 防止跨版本拼接；
- 股票聚合、排序、稳定游标和 1–500 上限；
- 404、422、503、PARTIAL 和 `null` 保留；
- API key；
- datetime 中文文档与格式；
- 三份契约同步。

PostgreSQL集成测试只能使用由 `TEST_DATABASE_URL` 指向的隔离可丢弃数据库，禁止连接生产数据库。

### 12.4 Worker

- 工作日 22:00 注册且默认关闭；
- 非交易日幂等跳过；
- 步骤顺序、daily_market 前置依赖和 Operations 终态；
- SSE 与 SZSE 独立失败；
- 08:30 无变化不重算，有新事件生成新版本；
- APScheduler 为唯一调度层。

## 13. 文档与治理

实施必须同步更新：

- ADR-0048 clarification；
- `docs/领域详设-Regulation-2026-09-02.md`；
- README 和 Worker 运行手册；
- 三份公开契约；
- 对应 GitHub Issue 的验收标准。

正式启用官方网页采集前，项目所有者必须确认交易所网站条款、访问频率、长期保存和使用边界。
生产迁移、真实来源采集、调度启用和历史重算分别需要明确授权；设计或代码合并本身不构成这些
生产操作的授权。

## 14. 非目标

- 不预测股票次日涨跌或异动概率；
- 不给出买卖建议或主观风险评分；
- 不把龙虎榜上榜原因当作正式监管事件；
- 不提供公开写接口或在线规则编辑；
- 不支持2026-07-06以前旧规则历史回算；
- 不在本阶段扩展科创板、北交所和ST特殊规则；
- 不增加缓存、消息队列、分布式计算或操作系统级定时任务。
