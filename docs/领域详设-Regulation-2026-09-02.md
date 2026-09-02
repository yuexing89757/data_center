# 领域详设：Regulation 监管异动规则测算 v0

> 状态：有效；核心规则计算、T+1测算、结果落库和每日Worker调度已实现，正式事件自动采集与公开API待后续实施
> 日期：2026-09-02
> 关联 Issue：#69
> 上级决策：`adr/ADR-0048-沪深主板与创业板监管异动规则测算.md`（Accepted）

## 1. 领域职责

`Regulation` 保存沪深交易所现行有效的股票异常波动规则、交易所正式公布的监管事件，以及由
可追溯行情和事件输入确定性计算的每日状态、逐规则结果和 T+1 条件测算。

本领域负责：

- 官方规则的版本、有效期、参数和来源；
- 交易所正式异常/严重异常事件事实；
- 沪市主板、深市主板、创业板普通股票的每日规则状态；
- 固定指数情景下可达到规则条件的 T+1 收盘价；
- 计算版本、输入哈希、数据完整性和稳定只读契约。

本领域不负责：

- 预测价格、停牌概率、资金行为或监管措施；
- HIGH/MEDIUM/LOW 等主观风险等级；
- 科创板、北交所、ST/*ST、退市整理期和无涨跌幅限制交易日；
- 龙虎榜、游资、题材、策略或回测；
- 2026年7月6日前规则历史；
- 监管规则在线编辑器、通用规则 DSL 或事件溯源平台。

## 2. 边界与依赖

```text
Ingestion ───────────────────────────────┐
Security ────────┐                       │
Trading ─────────┼──> Regulation <───────┤ official exchange events
Market ──────────┤                       │
Capital ─────────┤                       │
StockDailyIndicator ─────────────────────┘

Regulation ──> api_v1.query_regulation_warnings ──> FastAPI
```

- Provider 只采集和标准化来源事实。
- Validator 校验来源、身份、时间、单位和完整性。
- Calculator 是纯函数，不访问数据库、Raw、网络或系统时间。
- Service 装载一个一致输入快照、调用 Calculator 并组织事务。
- Persistence 负责规则、事件、计算版本和结果的 SQL 写入。
- FastAPI 只调用 `api_v1` RPC，不查询内部 schema。

## 3. 标识、枚举与时间

### 3.1 标识

- 股票统一使用 `SSE:600000`、`SZSE:000001` 等标准 `symbol`，不保存 `ts_code`。
- 基准指数分别为 `SSE:000002`、`SZSE:399107`、`SZSE:399102`。
- 交易日使用 `Asia/Shanghai` 的本地 `date`。
- 来源发布时间、首次观察时间和审计时间使用 `timestamptz`。

### 3.2 板块

```text
SSE_MAIN
SZSE_MAIN
GEM
```

板块由 Security 身份、交易所、证券类型和代码范围确定。无法唯一判定时不应用规则。

### 3.3 核心枚举

```text
RuleLevel       = ABNORMAL | SERIOUS_ABNORMAL
RuleKind        = CUMULATIVE_DEVIATION | TURNOVER_COMPOSITE | EVENT_COUNT
Direction       = UP | DOWN | NONE
ResetLevel      = ABNORMAL | SERIOUS_ABNORMAL

CalculatedState = NORMAL | ABNORMAL_TRIGGERED | SERIOUS_TRIGGERED
AnnouncedState  = NONE | ABNORMAL | SERIOUS_ABNORMAL

EvaluationState = NOT_TRIGGERED | TRIGGERED_CALCULATED | ANNOUNCED_BY_EXCHANGE
Reachability    = CURRENT | REACHABLE_NEXT_SESSION |
                  NOT_REACHABLE_NEXT_SESSION | NOT_PRICE_CALCULABLE

ScenarioCode    = INDEX_DOWN_2 | INDEX_FLAT | INDEX_UP_2 | CURRENT | NONE
Applicability   = APPLICABLE | NOT_APPLICABLE | INSUFFICIENT_DATA
RunStatus       = RUNNING | SUCCEEDED | PARTIAL | FAILED
```

`NOT_APPLICABLE` 表示规则明确排除；`INSUFFICIENT_DATA` 表示理论适用但输入不能证明。二者不得合并。

## 4. 官方规则集

### 4.1 版本与来源

规则集版本固定为：

```text
cn-a-share-regulation-2026-07-06.v1
```

有效期从2026年7月6日开始。上交所依据《上海证券交易所交易规则（2026年修订）》第5.4.1至
5.4.6条；深交所依据《深圳证券交易所交易规则（2026年修订）》第5.4.2至5.4.6条。

### 4.2 26条规则字典

表中 `threshold` 单位为百分点；`turnover ratio=30` 表示30倍，不是30%。

| rule_code | segment | level | kind | direction | parameters | reset | clause |
| --- | --- | --- | --- | --- | --- | --- | --- |
| SSE_MAIN_ABNORMAL_3D_DEV_UP | SSE_MAIN | ABNORMAL | CUMULATIVE_DEVIATION | UP | window=3, threshold=20 | ABNORMAL | SSE 5.4.2(1) |
| SSE_MAIN_ABNORMAL_3D_DEV_DOWN | SSE_MAIN | ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=3, threshold=-20 | ABNORMAL | SSE 5.4.2(1) |
| SSE_MAIN_ABNORMAL_TURNOVER | SSE_MAIN | ABNORMAL | TURNOVER_COMPOSITE | NONE | latest=3, prior=5, ratio=30, cumulative=20 | ABNORMAL | SSE 5.4.2(2) |
| SSE_MAIN_SERIOUS_10D_COUNT_UP | SSE_MAIN | SERIOUS_ABNORMAL | EVENT_COUNT | UP | count_window=10, required=4 | SERIOUS_ABNORMAL | SSE 5.4.3(1) |
| SSE_MAIN_SERIOUS_10D_COUNT_DOWN | SSE_MAIN | SERIOUS_ABNORMAL | EVENT_COUNT | DOWN | count_window=10, required=4 | SERIOUS_ABNORMAL | SSE 5.4.3(1) |
| SSE_MAIN_SERIOUS_10D_DEV_UP | SSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | UP | window=10, threshold=100 | SERIOUS_ABNORMAL | SSE 5.4.3(2) |
| SSE_MAIN_SERIOUS_10D_DEV_DOWN | SSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=10, threshold=-50 | SERIOUS_ABNORMAL | SSE 5.4.3(2) |
| SSE_MAIN_SERIOUS_30D_DEV_UP | SSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | UP | window=30, threshold=200 | SERIOUS_ABNORMAL | SSE 5.4.3(3) |
| SSE_MAIN_SERIOUS_30D_DEV_DOWN | SSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=30, threshold=-70 | SERIOUS_ABNORMAL | SSE 5.4.3(3) |
| SZSE_MAIN_ABNORMAL_3D_DEV_UP | SZSE_MAIN | ABNORMAL | CUMULATIVE_DEVIATION | UP | window=3, threshold=20 | ABNORMAL | SZSE 5.4.3(1) |
| SZSE_MAIN_ABNORMAL_3D_DEV_DOWN | SZSE_MAIN | ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=3, threshold=-20 | ABNORMAL | SZSE 5.4.3(1) |
| SZSE_MAIN_ABNORMAL_TURNOVER | SZSE_MAIN | ABNORMAL | TURNOVER_COMPOSITE | NONE | latest=3, prior=5, ratio=30, cumulative=20 | ABNORMAL | SZSE 5.4.3(2) |
| SZSE_MAIN_SERIOUS_10D_COUNT_UP | SZSE_MAIN | SERIOUS_ABNORMAL | EVENT_COUNT | UP | count_window=10, required=4 | SERIOUS_ABNORMAL | SZSE 5.4.4(1) |
| SZSE_MAIN_SERIOUS_10D_COUNT_DOWN | SZSE_MAIN | SERIOUS_ABNORMAL | EVENT_COUNT | DOWN | count_window=10, required=4 | SERIOUS_ABNORMAL | SZSE 5.4.4(1) |
| SZSE_MAIN_SERIOUS_10D_DEV_UP | SZSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | UP | window=10, threshold=100 | SERIOUS_ABNORMAL | SZSE 5.4.4(2) |
| SZSE_MAIN_SERIOUS_10D_DEV_DOWN | SZSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=10, threshold=-50 | SERIOUS_ABNORMAL | SZSE 5.4.4(2) |
| SZSE_MAIN_SERIOUS_30D_DEV_UP | SZSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | UP | window=30, threshold=200 | SERIOUS_ABNORMAL | SZSE 5.4.4(3) |
| SZSE_MAIN_SERIOUS_30D_DEV_DOWN | SZSE_MAIN | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=30, threshold=-70 | SERIOUS_ABNORMAL | SZSE 5.4.4(3) |
| GEM_ABNORMAL_3D_DEV_UP | GEM | ABNORMAL | CUMULATIVE_DEVIATION | UP | window=3, threshold=30 | ABNORMAL | SZSE 5.4.3(1) |
| GEM_ABNORMAL_3D_DEV_DOWN | GEM | ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=3, threshold=-30 | ABNORMAL | SZSE 5.4.3(1) |
| GEM_SERIOUS_10D_COUNT_UP | GEM | SERIOUS_ABNORMAL | EVENT_COUNT | UP | count_window=10, required=3 | SERIOUS_ABNORMAL | SZSE 5.4.4(1) |
| GEM_SERIOUS_10D_COUNT_DOWN | GEM | SERIOUS_ABNORMAL | EVENT_COUNT | DOWN | count_window=10, required=3 | SERIOUS_ABNORMAL | SZSE 5.4.4(1) |
| GEM_SERIOUS_10D_DEV_UP | GEM | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | UP | window=10, threshold=100 | SERIOUS_ABNORMAL | SZSE 5.4.4(2) |
| GEM_SERIOUS_10D_DEV_DOWN | GEM | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=10, threshold=-50 | SERIOUS_ABNORMAL | SZSE 5.4.4(2) |
| GEM_SERIOUS_30D_DEV_UP | GEM | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | UP | window=30, threshold=200 | SERIOUS_ABNORMAL | SZSE 5.4.4(3) |
| GEM_SERIOUS_30D_DEV_DOWN | GEM | SERIOUS_ABNORMAL | CUMULATIVE_DEVIATION | DOWN | window=30, threshold=-70 | SERIOUS_ABNORMAL | SZSE 5.4.4(3) |

### 4.3 基准指数

| segment | benchmark_symbol | benchmark_name |
| --- | --- | --- |
| SSE_MAIN | `SSE:000002` | 上证A股指数 |
| SZSE_MAIN | `SZSE:399107` | 深证A股指数 |
| GEM | `SZSE:399102` | 创业板综合指数 |

规则迁移必须检查：

- `rule_code` 全局唯一；
- 相同 segment/level/kind/direction/规则窗口/比较窗口 的有效期不重叠；窗口属于规则
  维度，因此10日与30日规则可同时有效，同一窗口的语义版本不得重叠；
- kind 所需参数完整，其他 kind 专属参数为空；
- UP 阈值为正、DOWN 阈值为负；
- `effective_date >= 2026-07-06`；
- source document、clause、URL 和规则集版本非空；
- GEM 不存在首版换手率复合规则。

## 5. 领域对象

### 5.1 `RegulationRule`

| 字段 | 类型 | 语义 |
| --- | --- | --- |
| `rule_code` | str | 全局稳定代码 |
| `exchange` | enum | SSE 或 SZSE |
| `segment` | enum | SSE_MAIN、SZSE_MAIN、GEM |
| `rule_name` | str | 中性官方规则名称 |
| `level` | enum | ABNORMAL 或 SERIOUS_ABNORMAL |
| `kind` | enum | 三个类型化公式族之一 |
| `direction` | enum | UP、DOWN 或 NONE |
| `window_days` | int \| None | 累计偏离或换手率最新窗口 |
| `threshold_pct` | Decimal \| None | 偏离阈值，百分点 |
| `comparison_window_days` | int \| None | 换手率前置比较窗口，首版5 |
| `ratio_threshold` | Decimal \| None | 日均换手率倍数，首版30 |
| `secondary_threshold_pct` | Decimal \| None | 最新窗口累计换手率，首版20 |
| `count_window_days` | int \| None | 事件次数窗口，首版10 |
| `required_count` | int \| None | 主板4、创业板3 |
| `counted_event_kind` | str \| None | 固定价格偏离型异常事件 |
| `reset_level` | enum | 对应官方重置边界 |
| `benchmark_symbol` | str \| None | 偏离规则必填 |
| `rule_set_version` | str | 规则集稳定版本 |
| `effective_date` | date | 含首日 |
| `expire_date` | date \| None | 含末日 |
| `source_document` | str | 官方文件名和文号 |
| `source_clause` | str | 官方条款 |
| `source_url` | str | 官方直链 |
| `enabled` | bool | 仅用于受控迁移切换 |

Rule Domain Record 不包含数据库 ID、创建时间或任意可执行表达式。

### 5.2 `RegulationEventRecord`

| 字段 | 类型 | 语义 |
| --- | --- | --- |
| `symbol` | str | 标准股票 symbol |
| `exchange` | enum | SSE 或 SZSE |
| `segment` | enum | 事件发生时板块 |
| `event_type` | enum | 首版两个事件类型 |
| `event_level` | enum | ABNORMAL 或 SERIOUS_ABNORMAL |
| `direction` | enum \| None | UP、DOWN；不能确定时为空 |
| `period_start_date` | date | 官方披露的计算起点 |
| `period_end_date` | date | 官方披露的计算终点 |
| `published_at` | datetime | 交易所来源发布时间 |
| `effective_reset_date` | date \| None | 次一交易日或复牌日；需日历解析 |
| `source_event_id` | str | 交易所来源稳定标识 |
| `source_title` | str | 公告/公开信息原题 |
| `source_url` | str | 交易所官方链接 |
| `source_content_hash` | str | 规范内容 SHA-256 |
| `source_code` | str | `sse_official` 或 `szse_official` |
| `explicit_rule_codes` | tuple[str, ...] | 来源能够明确映射时保存 |
| `observed_at` | datetime | 数据中心首次观察时间 |

正式事件不能从 `RuleResult` 构造。若来源只写“股价异动”但没有明确交易所结论、期间和证券，
Raw 可以保存，标准事件必须拒绝。

### 5.3 Calculator输入与输出

Calculator 输入是完整不可变对象：

```text
RegulationCalculationInput
├── trade_date + next_trade_date
├── algorithm_version + scenario_config_version
├── active_rules
├── stock candidates and segment/applicability facts
├── exact trading calendar window
├── stock daily bars and official reference previous closes
├── benchmark daily bars
├── turnover-rate facts
├── official events and reset boundaries
└── next-day PriceLimitRule inputs
```

输出：

```text
RegulationCalculationOutput
├── statuses
├── rule_results
├── warnings
├── coverage summary
└── quality findings
```

Domain 对象不包含 `ingestion_id` 或 `calculation_id`；服务和持久化边界附加血缘。

## 6. 持久化模型

内部 schema 为 `regulation`，启用 RLS，消费者无直接权限。

### 6.1 `regulation.rule`

- `rule_id uuid` 主键；
- 5.1全部字段；
- `created_at`、`updated_at`；
- unique `rule_code`；
- PostgreSQL exclusion constraint 阻止同一规则维度的有效日期重叠；
- kind-specific check constraints 保证参数组合合法。

规则修改只通过 ordered migration：旧记录设置 `expire_date`，新记录使用新 `rule_code` 或新规则集
版本和后续 `effective_date`。不得原地改写已被计算批次引用的规则语义。

### 6.2 `regulation.event`

- `event_id uuid` 主键；
- 5.2全部事实字段；
- `ingestion_id` 外键、`created_at`；
- unique `(source_code, source_event_id)`；
- unique `(source_code, symbol, period_start_date, period_end_date, event_level, direction)` 用于发现
  来源身份漂移；
- 相同内容重放幂等；相同来源身份出现不同哈希时保留新Raw并使该次ingestion硬失败，既有事件不变。
  交易所正式更正必须以新的官方来源事件身份追加，随后触发新计算。

### 6.3 `regulation.calculation_run`

| 字段 | 语义 |
| --- | --- |
| `calculation_id` | UUID主键 |
| `trade_date`、`next_trade_date` | T日和准确T+1交易日 |
| `status` | RUNNING/SUCCEEDED/PARTIAL/FAILED |
| `algorithm_version` | 公式语义版本 |
| `rule_set_version`、`rule_set_hash` | 规则版本与内容哈希 |
| `scenario_config_version` | -2/0/+2及价格取整配置版本 |
| `input_hash` | 全部标准输入的确定性哈希 |
| `market_watermark` | 股票/指数行情输入水位 |
| `capital_watermark` | 公司行动输入水位 |
| `event_watermark` | 官方事件最大观察时间/来源水位 |
| `expected_count`、`complete_count` | 覆盖统计 |
| `incomplete_count`、`not_applicable_count` | 缺口与排除统计 |
| `started_at`、`completed_at` | 审计时间 |

同一 `(trade_date, input_hash)` 幂等。消费者只能读取 SUCCEEDED 或 PARTIAL 且已完成发布的单一批次。

### 6.4 `regulation.status`

自然键为 `(calculation_id, symbol)`，保存：

- trade_date、symbol、exchange、segment；
- applicability、applicability_reason、data_completeness；
- calculated_state、announced_state；
- close、stock_daily_return_pct；
- benchmark_symbol、benchmark_close、benchmark_daily_return_pct、daily_deviation_pct；
- abnormal_count_10d、abnormal_count_10d_up、abnormal_count_10d_down；
- abnormal_reset_date、serious_reset_date；
- created_at。

不保存单一 `next_trigger_rule` 或单一 `deviation_10d` 作为权威值，因为不同方向和不同合法窗口可能
选择不同起点。完整窗口值在 `rule_result` 中保存。

### 6.5 `regulation.rule_result`

自然键为 `(calculation_id, symbol, rule_id)`，保存：

- evaluation_state、triggered；
- window_start_date、window_end_date、observed_window_days；
- current_value、threshold、distance；
- secondary_current_value、secondary_threshold；
- event_count、required_count；
- selected_reset_date；
- data_completeness、incomplete_reason；
- created_at。

`distance` 在未触发时为非负 Decimal；已触发时为0。原始 current/threshold 保留符号。

### 6.6 `regulation.warning`

自然键为 `(calculation_id, symbol, rule_id, scenario_code)`，保存：

- trade_date、next_trade_date、symbol；
- warning_type、rule level、direction；
- current_value、threshold、distance；
- scenario_code、scenario_index_pct；
- next_day_reference_price、raw_trigger_price；
- next_day_trigger_price、next_day_trigger_pct；
- price_limit_ratio、lower_limit_price、upper_limit_price；
- reachability；
- selected window start/end；
- requires_official_event_confirmation；
- deterministic message template code和message；
- created_at。

`warning` 不保存主观等级。`rule_result` 保存全量规则，`warning` 对每个
`symbol + direction + level + scenario` 选择所需价格变动最小的规则；相同要求时按 `rule_code`
排序打破平局。当前已触发规则全部保留，避免选择过程隐藏事实。

## 7. 计算语义

### 7.1 适用性

证券必须同时满足：

- `security_type=stock`，交易所为SSE或SZSE；
- 能确定属于首版三个板块之一；
- 目标日在证券上市生命周期内；
- 有价格涨跌幅限制；
- 能依据目标日 SecurityNameHistory 和每日指标证明不是 ST/*ST；
- 所需股票日K、基准指数日K、换手率及公司行动输入完整。

无价格涨跌幅限制、非首版板块和已确认ST属于 `NOT_APPLICABLE`。名称历史、上市阶段或公司行动
输入不足属于 `INSUFFICIENT_DATA`。不得把行情缺口、停牌或缺值压缩成连续窗口。

### 7.2 官方参考前收盘价

每个交易日股票收益为：

```text
r_stock[d] = close[d] / official_reference_previous_close[d] - 1
```

参考价选择顺序：

1. 来源提供且经校验的正数 `previous_close`；
2. 无公司行动时，严格前一可用交易日未复权收盘价；
3. 有公司行动时，复用 ADR-0009 接受的除权理论价和事件对齐语义计算参考价。

若需要的分配、配股或前收盘输入缺失，阻断该证券。Calculator 不读取 Capital，自身只接收已经
解析好的参考价序列。

### 7.3 最大窗口累计偏离

对以 T 日结束、长度1至 N、未跨越对应官方重置边界的每个合法窗口 `[a,T]`：

```text
stock_factor(a,T) = product(1 + r_stock[d]), d=a..T
index_factor(a,T) = product(1 + r_index[d]), d=a..T
deviation_pct(a,T) = (stock_factor(a,T) - index_factor(a,T)) * 100
```

- UP规则选择最大 `deviation_pct`；
- DOWN规则选择最小 `deviation_pct`；
- `达到`包含阈值等号；
- 保存真正被选中的 `[a,T]`，不能只保存固定N日值。

### 7.4 换手率复合条件

仅沪深主板：

```text
latest_avg = sum(turnover_pct[T-2..T]) / 3
prior_avg  = sum(turnover_pct[T-7..T-3]) / 5
ratio      = latest_avg / prior_avg
latest_sum = sum(turnover_pct[T-2..T])

triggered = ratio >= 30 and latest_sum >= 20
```

必须恰好具备连续8个目标证券交易日的有效换手率。`prior_avg=0` 不解释为无穷大，结果为
`INSUFFICIENT_DATA`；缺值不补零。

### 7.5 事件次数

- 只读取 `regulation.event` 中正式价格偏离型异常事件；
- 按事件 `period_end_date` 落入最近10个交易日；
- 方向必须与规则一致；
- 不早于最近严重异常重置日；
- 同一官方事件只计一次；
- 换手率事件和方向为空事件不计入。

主板达到4次、创业板达到3次时触发。系统计算出的异常但尚无官方事件只能用于条件性 T+1 提示，
不能提前增加计数。

### 7.6 状态汇总

```text
if any serious rule triggered:
    calculated_state = SERIOUS_TRIGGERED
elif any abnormal rule triggered:
    calculated_state = ABNORMAL_TRIGGERED
else:
    calculated_state = NORMAL
```

`announced_state` 独立从截至事件水位的正式事件计算。二者可以出现任意合法组合，不做隐式状态迁移。

## 8. T+1 条件反解

### 8.1 情景配置

```text
regulation-scenarios.v1
INDEX_DOWN_2 = -0.02
INDEX_FLAT   =  0
INDEX_UP_2   =  0.02
price_tick   =  0.01 CNY
```

情景配置和算法版本进入 `input_hash`，但不是交易所官方规则。

### 8.2 反解公式

对每个明日仍合法的窗口起点 `a` 重新求解。设：

```text
A = product(1 + r_stock[d]), d=a..T；若 a=T+1，则 A=1
B = product(1 + r_index[d]), d=a..T，再乘 (1 + scenario)；若 a=T+1，则 B=1+scenario
tau = threshold_pct / 100
x = T+1股票相对官方参考价的收益率
```

阈值等式：

```text
A * (1 + x) - B = tau
x = (tau + B) / A - 1
```

原始目标价：

```text
raw_trigger_price = next_day_reference_price * (1 + x)
```

UP向上取整到0.01元，DOWN向下取整到0.01元。取整后重新计算完整偏离值并断言达到阈值。

对UP选择所需涨幅最小的合法起点；对DOWN选择所需跌幅绝对值最小的合法起点。T日已触发的规则
使用 `scenario=CURRENT`，不伪造明日目标价。

### 8.3 涨跌幅限制

T+1上下限复用 ADR-0021 的版本化 `PriceLimitRule` 和纯价格取整实现。ADR-0048实施时在同一
代码目录新增创业板普通股票20%、0.01元档位和上市初期5个交易日无涨跌幅限制的规则版本；
Regulation Calculator 只消费已解析的 `DailyPriceLimit` 输入，不复制10%/20%常量。

- UP目标价不高于上限时可达；
- DOWN目标价不低于下限时可达；
- 超出范围为 `NOT_REACHABLE_NEXT_SESSION`；
- 无限制日属于首版不适用，不输出数值预警。

### 8.4 次数和换手率规则

当前事件次数只差一次，且T+1价格路径可满足对应方向异常价格规则时，严重次数规则可标记
`REACHABLE_NEXT_SESSION`，但必须设置 `requires_official_event_confirmation=true`。

换手率规则依赖明日成交量和自由流通股数，不能由收盘价格反解；对应情景为 `NONE`，可达性为
`NOT_PRICE_CALCULABLE`。提示不得把它转换成价格预测。

## 9. 官方事件 Provider 与 Raw

### 9.1 能力边界

新增专用 capability：

```text
RegulationEventProvider.fetch_events(observed_from, observed_to)
    -> ProviderBatch[RegulationEventRecord]
```

实现：

- `SSEOfficialRegulationEventProvider`
- `SZSEOfficialRegulationEventProvider`

每个 Provider 只访问对应交易所固定官方来源，不参与自动路由或跨来源回退。一次成功 IngestionRun
只有一个实际 Provider。SSE 与 SZSE 分开运行、分开 Raw、分开失败。

### 9.2 接受来源

来源优先级：

1. 交易所公开交易信息中的明确披露原因；
2. 交易所官网托管且正文明确记载交易所计算结论的公告。

媒体、行情软件、搜索摘要、市场俗称和本系统计算结果均不是事件来源。仅标题包含“异动”而正文
缺少明确结论的文档不进入标准事件。

### 9.3 Raw与重放

Raw schema：

```text
sse.regulation_event.v1
szse.regulation_event.v1
```

路径：

```text
sse/regulation_event/year=YYYY/month=MM/day=DD/<ingestion-id>.jsonl
szse/regulation_event/year=YYYY/month=MM/day=DD/<ingestion-id>.jsonl
```

Raw 保留来源响应字段、文档 URL、来源发布时间、抓取页游标和内容哈希，不保存凭据。重放验证路径、
字节数、SHA-256、行数和 schema version，创建新的 IngestionRun，引用原 manifest，不访问网络。

相同事件重复抓取幂等。相同 `source_event_id` 内容改变时保留新Raw、登记质量冲突并阻断标准事件
发布；交易所使用新 `source_event_id` 发布正式更正时，追加新事件并重算受影响事件日至当前日期中
最多30个交易日。旧Raw、旧事件和旧CalculationRun保留。

## 10. 基准指数日线采集

新增 allowlist-only 服务，使用 BaoStock 的 Daily Bar 能力采集三个指数。每个交易日单独创建
指数 ingestion，与普通股票 pytdx ingestion 分离。

完整性要求：

- 恰好请求三个允许的标准指数 symbol；
- `security_type=index`；
- trade_date 等于请求日；
- OHLC和previous_close为正，volume/amount保持来源单位转换；
- 单个指数缺失不得用其他指数替代；
- Raw和manifest遵循现有 BaoStock Daily Bar 契约。

若某指数缺失，仅其对应 segment 标记不完整，其他 segment 继续计算。

## 11. Service 与 Worker

### 11.1 收盘计算

Workflow code：`regulation_daily_calculation`
Job ID：`regulation-daily-calculation`
触发：周一至周五22:30，`Asia/Shanghai`，默认关闭。

步骤：

```text
collect_regulation_benchmarks
collect_sse_regulation_events
collect_szse_regulation_events
validate_regulation_dependencies
calculate_regulation_status
publish_regulation_results
```

每个来源采集步骤创建自己的 IngestionRun。计算服务在一个可重复读输入快照中确定规则、行情、
Capital、每日指标和事件水位，离开事务后调用纯 Calculator，再在单个写事务中发布一个
CalculationRun 的 status/rule_result/warning。不得把两个 Provider 的部分记录合并成一个成功来源批次。

### 11.2 盘前对账

Workflow code：`regulation_event_reconciliation`
Job ID：`regulation-event-reconciliation`
触发：周一至周五08:30，`Asia/Shanghai`，默认关闭。

该任务仅增量检查官方事件。没有变化时幂等成功；存在新增或更正时，根据事件重置影响范围为精确
交易日创建新计算版本。它不采集普通股票行情，不操作旧结果。

### 11.3 Operations

WorkflowRun/JobExecution 记录：

- 每个来源请求页数、Raw行数、接受/拒绝/歧义事件数；
- 三个指数接受数和缺失symbol；
- 规则集版本/哈希；
- expected/complete/incomplete/not-applicable证券数；
- 各 segment 完整性；
- 生成的 status/rule_result/warning 数；
- SUCCEEDED/PARTIAL/FAILED终态。

APScheduler JobStore 只保存调度状态，不复制到 Regulation 或 Operations 事实。

## 12. 失败与质量语义

### 12.1 整批失败

- 没有覆盖目标日的唯一有效规则集；
- 规则参数非法或有效期重叠；
- 交易日历不可用；
- calculation input hash无法确定；
- 持久化事务失败；
- 同一来源事件身份出现无法解释的语义冲突。

失败批次不发布可读 warning。

### 12.2 板块不完整

- 对应基准指数缺失或无效；
- 板块识别规则整体不可用。

该 segment 的证券保存不完整状态；其他 segment 可发布，CalculationRun 为 PARTIAL。

### 12.3 单股不完整

- 行情缺口或不连续；
- ST状态、上市阶段或涨跌幅限制不能证明；
- 公司行动参考价缺失；
- 换手率窗口缺值；
- 正式事件方向不明确导致次数不可计算。

单股不完整不阻断其他股票，但不生成该股票的数值 warning。

## 13. 公开读取契约

RPC：

```text
api_v1.query_regulation_warnings(
    p_trade_date date,
    p_exchange text default null,
    p_segment text default null,
    p_symbol text default null,
    p_rule_code text default null,
    p_calculated_state text default null,
    p_announced_state text default null,
    p_reachability text default null,
    p_cursor text default null,
    p_limit integer default 100
) -> jsonb
```

约束：

- `p_trade_date` 必填且不得早于2026-07-06；
- 不回退到其他交易日；
- `p_limit` 为1至500；
- cursor编码上一页 `(symbol, rule_code, scenario_code)`，与过滤参数绑定；
- 选择该日最新已完成的 SUCCEEDED/PARTIAL CalculationRun，一次响应只使用一个 calculation_id；
- 无兼容计算版本使用 SQLSTATE `P0002`；非法参数使用 `22023`；
- 函数锁定 `search_path`、5秒 statement timeout、撤销 public执行权，只授予API角色；
- 返回 `items`、`returned_count`、`has_more`、`next_cursor` 和计算批次/覆盖元数据。

每个 item 返回：

```text
trade_date, next_trade_date, symbol, exchange, segment,
rule_code, rule_level, direction,
calculated_state, announced_state,
current_value, threshold, distance,
scenario_code, scenario_index_pct,
next_day_reference_price, next_day_trigger_price, next_day_trigger_pct,
price_limit_ratio, lower_limit_price, upper_limit_price,
reachability, requires_official_event_confirmation,
window_start_date, window_end_date, message,
data_completeness,
calculation_id, algorithm_version, rule_set_version,
scenario_config_version, event_watermark
```

FastAPI：

```text
GET /api/v1/regulation/warnings
```

FastAPI复用现有API Key，只调用RPC。同步：

- `contracts/postgrest-openapi-v1.json`
- `contracts/agent-tools-v1.json`
- `contracts/fastapi-openapi-v1.json`

## 14. 提示语

消息由稳定模板生成，不接受自由文本模型生成。价格型示例：

```text
在基准指数明日涨跌幅为 {scenario_index_pct}% 的情景下，若股票收盘价达到
{trigger_price} 元（相对官方参考价 {trigger_pct}%），按规则测算可能达到
{rule_name} 条件。该结果不是价格预测，实际认定及监管措施以交易所公开信息为准。
```

次数型追加：

```text
是否形成并计入新的同向异常事件，仍需交易所正式确认。
```

禁止出现“明天一定停牌”“必然重点监控”“监管概率”或主观风险等级。

## 15. 测试矩阵

### 15.1 Rule/Domain/Calculator Golden Tests

1. migration正好包含26条有效规则，板块分布9/9/8；
2. 两个交易日达到“3日内”异常条件；
3. 六个交易日达到“10日内”严重异常条件；
4. 复合收益与逐日差值求和不同；
5. 阈值等号触发；
6. UP取最大窗口、DOWN取最小窗口；
7. T+1加入后旧日移出，最优窗口改变；
8. 三个指数情景反解；
9. UP向上取整、DOWN向下取整且回代通过；
10. 主板10%与创业板20%可达性不同；
11. 除权参考价正确，缺失时阻断；
12. 异常和严重异常重置互不混淆；
13. 只有正式价格事件计数，换手率和未知方向不计数；
14. 事件次数只差一次时带确认依赖；
15. 换手率 `prior_avg=0`、缺值和边界等号；
16. ST、无涨跌幅限制及非首版板块不适用。

### 15.2 Provider/Raw/Service

- SSE/SZSE来源字段映射、symbol、期间、发布时间和方向；
- 标题相似但正文无正式结论时拒绝；
- 页数/总数漂移、超时、空响应、重复记录和结构变化；
- Decimal不经过float；
- Raw先于标准化、manifest校验和离线重放；
- 相同事件幂等、内容修订触发新计算；
- BaoStock三个指数白名单及普通股票路由不变；
- segment级和symbol级partial语义；
- 相同input_hash幂等，不混合calculation_id。

### 15.3 PostgreSQL/API/Worker

- 表约束、有效期排斥、RLS、最小授权和事务回滚；
- RPC精确日期、过滤、cursor、limit、排序、P0002/22023和5秒超时；
- API角色不能select内部表，FastAPI不直接查询内部schema；
- 三份契约字段、枚举和边界一致；
- 22:30/08:30 Asia/Shanghai注册、默认关闭、休市跳过；
- Operations步骤、partial/failed和崩溃恢复；
- 仓库不存在新增cron或Windows Task Scheduler指令。

## 16. 实施拆分

该规格跨越三个可独立评审的子系统，ADR接受后分别生成实施计划：

1. **规则与计算核心**：Domain、26条规则migration、六表schema、纯Calculator和Golden Tests；
2. **来源与Worker**：三个基准指数、SSE/SZSE正式事件、Raw/replay、Service、调度和Operations；
3. **公开读取**：RPC、FastAPI、权限、三份契约和运行文档。

每个子计划都必须先写失败测试、实现最小通过代码、运行聚焦验证并独立提交。第三个计划依赖前两个
稳定接口；生产迁移、真实来源访问和调度启用不属于代码完成即自动获得的权限。

## 17. 官方链接

- 上交所2026交易规则通知及现行文本：
  https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml
- 上交所股票交易规则正文入口：
  https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml
- 深交所2026交易规则通知：
  https://www.szse.cn/lawrules/rule/trade/current/t20260424_620190.html
- 深交所2026交易规则PDF：
  https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf
- 上证A股指数编制方案：
  https://www.sse.com.cn/market/sseindex/indexlist/indexdetails/indexmethods/c1/000002_000002hbook_CN.pdf
- 深证A股指数官方资料：
  https://www.szse.cn/marketServices/message/index/project/P020250711381739218475.pdf
- 创业板综合指数官方说明：
  https://www.szse.cn/aboutus/trends/news/t20250711_614840.html
