# 沪深主板与创业板监管异动规则测算设计

> 日期：2026-09-02
> 状态：书面规格已确认
> 关联 Issue：#69
> 提议 ADR：`docs/adr/ADR-0048-沪深主板与创业板监管异动规则测算.md`
> 领域详设：`docs/领域详设-Regulation-2026-09-02.md`

## 1. 目标

每日收盘后，对沪市主板、深市主板和创业板普通股票执行交易所官方异常波动规则计算，输出：

- 当前计算是否达到异常或严重异常条件；
- 交易所是否已经正式公布对应事件；
- 在基准指数明日-2%、0%、+2%情景下，股票T+1达到规则条件所需的收盘价格和涨跌幅；
- 该价格是否受适用涨跌幅限制而不可达；
- 规则版本、算法版本、计算批次、官方事件水位和数据完整性。

本模块做规则触发条件测算，不预测股价，不预测监管措施，不把严重异常等同于停牌。

## 2. 范围

### 包含

- 2026年7月6日起的SSE_MAIN、SZSE_MAIN、GEM；
- 3日内价格偏离异常；
- 沪深主板3日/前5日换手率复合异常；
- 10日内同向价格异常次数；
- 10日、30日累计价格偏离严重异常；
- SSE/SZSE正式异常与严重异常事件；
- 三个官方板块基准指数；
- Worker收盘计算与盘前事件对账；
- 精确日期、有界、只读的PostgREST/FastAPI契约。

### 不包含

- 科创板、北交所、ST/*ST及无涨跌幅限制期；
- 2023规则历史回算；
- 主观风险等级、价格/停牌概率、资金行为、龙虎榜和游资；
- regulation_snapshot、复杂事件溯源、规则DSL、MCP或公开写入API。

## 3. 方案选择

### 采用：类型化公式族 + 数据库参数

`regulation.rule` 保存适用范围、窗口、阈值、次数、有效期、基准指数和官方来源；Python只实现三个
稳定公式族：累计偏离、换手率复合条件和事件次数。

该方案使交易所仅调整数值或有效期时可以通过migration增加规则版本；同时避免执行数据库任意
表达式，便于mypy、单元测试和安全审计。

### 未采用：JSON规则DSL

DSL可以表达更多未来规则，但第一版只有三个明确公式族。引入解析器、类型系统、迁移兼容和运行时
错误会扩大故障面，不符合YAGNI。

### 未采用：SQL存储过程直接计算全部规则

SQL适合约束和有界读取，不适合作为复杂滚动窗口、公司行动调整和T+1反解的唯一实现。纯Python
Calculator更容易做Decimal Golden Tests和算法版本化。

## 4. 架构

新增独立Regulation边界，依赖现有客观事实，不修改基础事实：

```text
Security + Trading + Market + Capital + StockDailyIndicator
                         │
official SSE/SZSE events ┤
                         ▼
                Regulation Service
                         │
                  pure Calculator
                         │
 rule + event + calculation_run + status + rule_result + warning
                         │
            api_v1 bounded RPC → FastAPI
```

六类持久化对象各自承担单一职责：

- `rule`：官方规则配置；
- `event`：交易所正式发生事实；
- `calculation_run`：版本、输入哈希和覆盖率；
- `status`：证券在T日的汇总状态；
- `rule_result`：每条适用规则的可审计计算结果；
- `warning`：面向消费者的当前触发或T+1最接近条件。

## 5. 规则口径

首个规则集为 `cn-a-share-regulation-2026-07-06.v1`，共26条：

| segment | abnormal deviation | turnover | 10d count | 10d serious | 30d serious | count |
| --- | --- | --- | --- | --- | --- | --- |
| SSE_MAIN | ±20%, 3日内 | 3日均/前5日均≥30倍且3日累计≥20% | 同向4次 | +100%/-50% | +200%/-70% | 9 |
| SZSE_MAIN | ±20%, 3日内 | 3日均/前5日均≥30倍且3日累计≥20% | 同向4次 | +100%/-50% | +200%/-70% | 9 |
| GEM | ±30%, 3日内 | 无 | 同向3次 | +100%/-50% | +200%/-70% | 8 |

详细26行、条款号和字段约束以领域详设第4节为准。所有阈值采用“达到”包含本数。

基准指数：

- SSE_MAIN：`SSE:000002`；
- SZSE_MAIN：`SZSE:399107`；
- GEM：`SZSE:399102`。

## 6. 计算

### 最大窗口

“连续N个交易日内”按最多N日处理。对以T日结束、长度1至N且不跨越官方重置边界的全部连续窗口
求值。UP取最大偏离，DOWN取最小偏离，保存实际窗口起止日期。行情缺口不能被跳过后压缩窗口。

### 累计偏离

股票和指数分别复合，再相减：

```text
stock_factor = product(close[d] / official_reference_previous_close[d])
index_factor = product(index_close[d] / index_previous_close[d])
deviation_pct = (stock_factor - index_factor) * 100
```

股票官方参考前收盘价优先使用已校验来源值；无公司行动时可由前一日收盘得到；公司行动日复用
ADR-0009接受的除权理论价。输入缺失时标记不完整，不近似。

### 换手率

主板规则要求连续8个交易日的有效换手率。最近3日日均除以前5日日均达到30倍，并且最近3日累计
达到20%时触发。前5日日均为0或窗口缺值时不计算。

### 事件次数与重置

- 异常和严重异常分别维护重置日；
- 自交易所公布的次一交易日或复牌日起重新计算；
- 次数只统计正式、同向、价格偏离型异常事件；
- 本系统计算触发和换手率事件不自动增加次数。

`calculated_state` 与 `announced_state` 独立，允许计算已触发但尚无正式公告，也允许后来事件修订
导致公告状态变化。

## 7. T+1反解

不能用“阈值减当前值”，因为明日加入后窗口起点会滚动。每个指数情景和每个明日合法窗口分别
反解：

```text
A = T日前窗口股票复合因子
B = 包含明日指数情景的指数复合因子
tau = 规则阈值小数
x = 股票明日相对官方参考价的收益率

A * (1 + x) - B = tau
x = (tau + B) / A - 1
```

目标价按T+1官方参考价换算。UP向上取整至0.01元，DOWN向下取整至0.01元，取整后重新回代。
对UP选择所需涨幅最小的窗口，对DOWN选择所需跌幅绝对值最小的窗口。

涨跌停可达性复用版本化PriceLimitRule。现有主板规则为10%；实施时扩展创业板普通股票20%规则，
Regulation不复制具体比例。无涨跌幅限制期在V1明确不适用。

次数规则若只差一次，可以输出价格路径上的条件性可达，但必须标记仍依赖交易所正式确认。
换手率依赖明日成交量，返回 `NOT_PRICE_CALCULABLE`，不伪造触发价格。

## 8. 事件和指数采集

SSE与SZSE各有独立官方事件Provider、IngestionRun和Raw schema。只接受交易所公开交易信息或
交易所托管且正文明确记录官方结论的公告；标题、媒体、行情软件或计算结果不能生成事件。

相同事件幂等；来源ID相同但内容哈希变化时保留Raw修订并触发新计算。方向无法确定的正式事件可
保存，但不计入同向次数。

BaoStock白名单采集三个基准指数到现有 `core.daily_bar`。指数批次与普通股票pytdx批次分离；
不改变普通Daily Bar路由，不用替代指数补缺口。

## 9. 调度和一致性

`regulation_daily_calculation` 于工作日22:30运行：指数采集、SSE事件、SZSE事件、依赖校验、
纯计算和单批次发布。`regulation_event_reconciliation` 于工作日08:30检查晚到/更正事件并创建新
计算版本。两个任务默认关闭，只能进入Worker APScheduler代码目录。

输入哈希覆盖规则集、算法、情景配置、股票/指数行情、Capital、ST/板块适用性和事件水位。同一
输入幂等；任一事实变化生成新的CalculationRun，旧结果不可变。

规则集无效或交易日历不可用使整批失败；指数缺失只阻断对应segment；单股输入缺失只阻断该证券。
PARTIAL批次公开覆盖元数据，不能伪装为完整成功。

## 10. 公开契约

新增 `api_v1.query_regulation_warnings`：精确交易日必填，可按交易所、板块、symbol、规则、状态和
可达性过滤，limit 1至500，稳定键集分页，不回退日期，一次响应只包含一个已发布calculation_id。

FastAPI提供 `GET /api/v1/regulation/warnings`，只调用RPC。响应携带规则、当前/公告状态、情景、
参考价、触发价、触发涨跌幅、涨跌幅限制、可达性、窗口、确认依赖、版本、水位和完整性。

消息使用确定性模板，并固定声明：这是规则触发条件测算，不构成预测；实际认定和监管措施以交易所
公开信息为准。

## 11. 测试与验收

Golden Tests至少覆盖：两日达到3日内条件、六日达到10日内条件、复合而非求和、阈值等号、滚动
窗口改变、三种指数情景、价格方向取整、涨跌停不可达、创业板/主板差异、除权、两级重置、事件
次数来源、换手率边界和缺失数据。

Provider测试覆盖官方字段、空/超时/分页漂移、歧义、幂等、内容修订、Raw replay和Decimal。
数据库/API/Worker测试覆盖约束、RLS、最小权限、版本一致性、精确日期、分页、契约同步、时区、
默认关闭和Operations终态。

完整本地门禁：

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration   # 仅隔离 TEST_DATABASE_URL
```

## 12. 实施边界与拆分

该设计包含三个可独立评审的子系统，书面规格确认且ADR变为Accepted后分别编写实施计划：

1. 规则配置、六表schema、Domain、Calculator和Golden Tests；
2. 官方事件/指数采集、Raw replay、Service、Worker和Operations；
3. RPC、FastAPI、权限、三份契约和运行文档。

代码完成不授权生产迁移、真实交易所抓取或调度启用。三者仍需单独的受保护操作批准。

## 13. 官方依据

- 上交所《交易规则（2026年修订）》及上证发〔2026〕41号：
  https://www.sse.com.cn/lawandrules/sselawsrules2025/trade/universal/c/c_20260424_10816492.shtml
- 深交所《交易规则（2026年修订）》及深证上〔2026〕551号：
  https://www.szse.cn/lawrules/rule/trade/current/t20260424_620190.html
- 深交所规则PDF：
  https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf

两份规则均自2026年7月6日起施行，2023年版本同时废止。V1规则日期边界据此确定。
