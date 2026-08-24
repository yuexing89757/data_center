# 09:26 沪深主板一字涨跌停实时计算设计

- 日期：2026-08-17
- 状态：待评审
- 跟踪：[GitHub Issue #52](https://github.com/yuexing89757/data_center/issues/52)
- 影响决策：ADR-0032

## 背景

`GET /api/v1/call-auction-one-price-limits` 当前先选择指定交易日 09:26 的
`realtime.call_auction_market_snapshot`，再要求同日存在成功且单一的
`cn_a_mainboard_price_limit_pools` CalculationRun 与 `derived.daily_price_limit`。生产环境的
09:26 快照在早盘已经 ready，但主板涨跌停派生批次在晚间运行，因此接口在早盘返回 404。

本变更把涨跌停价改为只读请求中的确定性实时计算。这里的“实时”是指查询时基于已经持久化的
09:26 来源事实计算，不访问行情 Provider、不触发采集、不写数据库，也不使用 09:26 之后的日线。

## 目标与非目标

### 目标

1. 保持现有 GET 路径、API Key、可选 `trade_date` 和精确日期不回退语义。
2. 09:26 succeeded 快照 ready 后立即可查；无 succeeded 时仍允许最新 partial。
3. 只分类沪深主板股票，请求时从快照 `previous_close` 计算上下限。
4. 保持当前受控规则：普通股和 ST 股均为 10%，价格档位为 0.01 元，四舍五入。
5. 保持严格一字板证据与 ingestion lineage，显式报告遗漏。
6. 查询只读、有界，FastAPI 仍只执行 `api_v1` RPC。

### 非目标

- 不覆盖创业板、科创板、北交所或其他证券类型。
- 不修订普通/ST 比例，不引入 5%、20% 或 30% 新规则。
- 不把实时结果写入 `derived`，不创建 CalculationRun 或新定时任务。
- 不回补历史事实，不访问网络，不使用盘中或收盘日线。
- 不改变 09:26 全市场快照采集任务。

## 方案选择

### 采用：在 `api_v1` RPC 内确定性计算

用 ordered migration 替换 `api_v1.query_auction_one_price_limits(date)` 的函数体。RPC 在一个稳定、
只读、5 秒 statement timeout 的数据库快照中完成批次选择、主板资格校验、价格计算和分类。
这保留最小权限与单次往返，也避免 FastAPI 直接读取内部 schema。

### 未采用：FastAPI/Python 计算

该方案需要新增一个输入 RPC，把大量内部事实传到 FastAPI 后再计算，扩大跨层契约并增加一次对象
转换。它没有提高可追溯性或正确性。

### 未采用：盘前或早盘物化

新增 Worker 任务会引入额外调度、失败恢复和持久化版本，且不符合“请求时实时计算”的要求。

## 数据选择与主板资格

批次选择保持 ADR-0032：

1. 只接受 `observed_at` 上海时间位于 `[09:26:00, 09:27:00)` 的快照。
2. 显式日期只查该日；省略日期选择最新可用交易日。
3. 同日优先选择最新 succeeded ingestion；没有 succeeded 时选择最新 partial。
4. 指定范围没有快照时以 SQLSTATE `P0002` 返回 not found。

选中批次后，只把满足以下全部条件的记录作为主板候选：

- `core.security` 为 SSE/SZSE、`stock`、`listed`；
- SSE 代码为 `600000..603999` 或 `605000..605999`；
- SZSE 代码为 `000001..004999`，排除 `001001..001199`；
- IPO 日期可证明，目标日在上市交易日序号上大于 5；
- 目标日前五个交易日均存在 `trading` 或 `unknown` 的日 K，用于证明已过无涨跌幅限制阶段。

非主板证券不进入 `candidate_count`，也不计入遗漏。主板资格或报价证据不完整的候选计入
`omitted_incomplete_count`，不得猜测或补值。

## 实时计算与分类

对证据完整的主板候选使用 PostgreSQL `numeric` 计算，禁止 `float`：

```text
upper_limit = round(previous_close * 1.10, 2)
lower_limit = round(previous_close * 0.90, 2)
```

所有价格必须为正数。普通股和 ST 股均使用 10%，与用户确认的现有规则一致。规则版本沿用
`CN_MAINBOARD_2026_07_06`，算法版本沿用 `1.0.0`；本次只改变计算时机和 lineage 表达，不静默
修改比例或舍入口径。

仅以下完整等式成立时分类：

- 涨停：`last_price = high_price = low_price = upper_limit`；
- 跌停：`last_price = high_price = low_price = lower_limit`。

未命中上下限但证据完整的主板股票不是遗漏，只是不进入 `up`/`down`。缺名称、昨收、现价、
最高价、最低价或上市阶段证据的候选才计入遗漏。

## 公共响应与兼容性

路径、方法和主要列表字段不变。响应继续返回：

- `trade_date`、`ingestion_id`、`ingestion_status`、`snapshot_window`；
- `candidate_count`、`omitted_incomplete_count`、`up_count`、`down_count`；
- `up`、`down` 及每项的 symbol/code/name、方向、观察时间、指示价、限价、昨收、累计量额。

`price_limit_calculation_id` 改为可空且实时结果固定为 `null`。不得伪造 CalculationRun UUID 或把
ingestion ID 冒充 calculation lineage。新增：

- `price_limit_rule_version = "CN_MAINBOARD_2026_07_06"`；
- `price_limit_algorithm_version = "1.0.0"`；
- `calculation_mode = "realtime_read"`。

FastAPI OpenAPI 契约同步更新，并把该 RPC 加入独立 API release preflight。RPC 继续只授予
`market_data_api`；不得加入 anon/authenticated PostgREST 或 Agent 工具契约。消费者可根据非空
ingestion lineage、规则版本、算法版本和计算模式重放同一结果。

## 错误与性能

- 404：指定日期没有符合窗口的 succeeded/partial 09:26 快照。
- 401：API Key 无效。
- 503：数据库或公共 RPC 不可用。
- 空列表：快照存在但没有严格命中的一字涨停/跌停，这是合法 200，不是 404。

RPC 保持 `stable security definer`、受控 `search_path`、5 秒 statement timeout，只授予
`market_data_api` EXECUTE。查询以选中 ingestion 和日期为边界，使用现有快照索引；上市阶段的
交易日与前五日证据采用有界 lateral/CTE，不扫描无关历史区间。

## 迁移、文档与测试

实施需要：

1. 新 accepted ADR 取代 ADR-0032 中“必须读取同日 CalculationRun”的条款，明确实时只读计算。
2. ordered SQL migration 替换 RPC，不做 ad-hoc DDL。
3. FastAPI response model/OpenAPI、API release preflight 和接口/数据库导航文档同步更新；
   PostgREST/Agent 契约保持不暴露该 RPC。
4. PostgreSQL integration tests 覆盖 succeeded/partial 选择、精确日期、主板代码边界、上市前五日、
   10% numeric 舍入、严格等式、遗漏计数、空列表、无快照 P0002、权限和5秒上限。
5. FastAPI 单元/契约测试覆盖 nullable calculation ID 和新增版本字段。

生产发布只应用该单一预期 migration、切换 API release 并做只读在线验证；不重启或触发 Worker
采集，不创建操作系统级任务。
