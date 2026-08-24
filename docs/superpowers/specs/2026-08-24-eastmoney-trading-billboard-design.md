# 东方财富股票龙虎榜采集领域设计

- 状态：设计已确认，待项目治理审批
- 日期：2026-08-24
- 数据来源：东方财富
- 治理状态：尚未创建 GitHub Issue 或 Accepted ADR，不得据此直接实施

## 目标与非目标

新增独立的 `TradingBillboard` 领域，采集并保存 A 股每日龙虎榜上榜证券汇总，以及每条
上榜记录对应的买入前五和卖出前五席位明细。事实必须可追溯到东方财富响应、采集批次和
不可变 Raw 对象，并支持按准确交易日、按股票与日期范围进行有界读取。

首期只接受 `SSE`、`SZSE`、`BSE` 股票，不采集可转债。首期不保存或计算游资身份映射、
营业部排行、席位成功率、上榜后收益、东财文本“解读”、交易建议或其他主观/衍生标签。
不引入第二数据源，不做来源自动切换，不提供请求时实时回源，不增加操作系统级计划任务。

## 领域边界与数据流

`TradingBillboard` 是独立领域，不并入以 `(symbol, trade_date)` 为自然键的普通日行情。
一只股票同日可能因多个原因形成多条龙虎榜记录，因此汇总记录必须拥有独立事件身份。

```text
Worker 受控任务目录
        |
        v
EastmoneyTradingBillboardProvider
        |
        v
不可变 Raw JSONL -> 版本化 Normalizer -> Validator
                                      |
                                      v
                    TradingBillboardRecord 聚合
                     /                    \
                    v                      v
          billboard.entry          billboard.seat
                    \                      /
                     v                    v
                       api_v1 有界 RPC
                              |
                              v
                       外部只读 FastAPI
```

Provider 负责来源请求、字段映射、代码转换、单位转换、空值转换、席位关联和名次生成。
Domain Record 不包含 `ingestion_id`；Pipeline 在校验后通过 `IngestionEnvelope` 附加采集
血缘，Persistence 在同一事务内写入汇总及席位。

## 来源契约与已验证特征

首期使用东方财富数据中心龙虎榜页面背后的普通 JSON 数据接口，分别读取：

- 每日上榜证券汇总；
- 买入席位明细；
- 卖出席位明细。

2026-08-24 对 2026-08-17 响应的只读验证确认：

- `TRADE_ID` 在当日股票汇总中唯一；
- 同一股票同日可以存在多个不同 `CHANGE_TYPE`；
- 买入和卖出明细分别最多提供五条席位行；
- 多条“机构专用”可以重复使用营业部代码 `0`，营业部代码不能作为席位自然键；
- 席位响应会混入可转债，而股票汇总与席位必须通过事件身份关联后再接受；
- 前五席位对应侧金额之和经常不等于汇总买入额或卖出额，不能建立错误的跨表金额等式；
- 网页展示使用“万元”，JSON 金额字段实际按 CNY 进入标准模型，不执行万元到元的二次换算。

东方财富接口没有项目可依赖的公开版本和 SLA。Provider 必须固定请求地址、参数、分页上限、
连接超时和读取超时；字段缺失、类型变化、总数变化或分页不一致必须失败，不能静默忽略。

## 领域模型

### `TradingBillboardRecord`

聚合根表示一条上榜汇总，并包含对应买入、卖出席位：

| 字段 | 类型与语义 |
| --- | --- |
| `symbol` | 标准股票代码 `SSE|SZSE|BSE:NNNNNN` |
| `trade_date` | 交易所本地交易日 |
| `source_event_id` | 来源事件标识；首个 Adapter 映射东财 `TRADE_ID` |
| `reason_code` | 来源发布的上榜原因代码 |
| `reason_text` | 来源发布的上榜原因原文 |
| `close_price` | `Decimal`，CNY/股，可缺失但不得为负 |
| `change_rate_pct` | `Decimal`，百分点，可正可负 |
| `turnover_rate_pct` | `Decimal`，百分点，不得为负 |
| `market_amount` | `Decimal`，当日市场成交额，CNY |
| `buy_amount` | `Decimal`，龙虎榜买入合计，CNY |
| `sell_amount` | `Decimal`，龙虎榜卖出合计，CNY |
| `net_amount` | `Decimal`，龙虎榜净额，允许负数 |
| `deal_amount` | `Decimal`，龙虎榜成交合计，CNY |
| `deal_to_market_pct` | `Decimal`，成交额占市场成交比例，百分点 |
| `net_to_market_pct` | `Decimal`，净额占市场成交比例，允许负数 |
| `free_float_market_value` | `Decimal`，来源发布的流通市值，CNY |
| `buy_seats` | `tuple[TradingBillboardSeatRecord, ...]` |
| `sell_seats` | `tuple[TradingBillboardSeatRecord, ...]` |
| `source_code` | 固定为 `eastmoney` |

汇总自然键为 `(source_code, source_event_id)`。同一批次另以
`(symbol, trade_date, reason_code)` 检测语义冲突；未来接入第二来源前必须以新 ADR 明确
原因代码命名空间和跨来源事实对齐规则。

### `TradingBillboardSeatRecord`

席位 Record 显式携带股票和交易日，使 Provider、Validator 和 Persistence 可以独立校验
父子归属：

| 字段 | 类型与语义 |
| --- | --- |
| `source_event_id` | 所属汇总事件标识 |
| `symbol` | 必须与所属汇总一致 |
| `trade_date` | 必须与所属汇总一致 |
| `side` | `buy` 或 `sell` |
| `rank` | 本侧名次，1 至 5 |
| `seat_code` | 来源营业部代码；`0` 或无可靠标识时规范化为 `None` |
| `seat_name` | 披露时的营业部、机构专用或其他席位名称 |
| `buy_amount` | `Decimal | None`，该席位买入额，CNY |
| `sell_amount` | `Decimal | None`，该席位卖出额，CNY |
| `net_amount` | `Decimal | None`，该席位净额，允许负数 |
| `buy_to_market_pct` | `Decimal | None`，买入额占市场成交比例，百分点 |
| `sell_to_market_pct` | `Decimal | None`，卖出额占市场成交比例，百分点 |
| `source_code` | 固定为 `eastmoney` |

席位自然键为 `(source_code, source_event_id, side, rank)`。同一营业部可以同时进入买入榜和
卖出榜；同一侧也允许多条 `seat_code=None` 的“机构专用”，因此不对代码或名称建立唯一约束。

东财席位响应不提供独立名次字段。Normalizer 在每个事件、每个方向内按对应侧金额降序生成
`rank`；金额相同时依次使用规范化席位代码、席位名称和完整行内容哈希稳定排序。相同 Raw 必须
得到完全相同的名次和内容哈希。

## 标准化与质量规则

所有价格、金额和比例直接从 JSON 数字文本构造 `Decimal`，不得经过 `float`。`None` 保持缺失，
不得自动转成零。非净额类金额必须非负；`net_amount` 和净额比例允许为负。

一个交易日的成功批次必须满足：

1. 汇总响应和两个席位响应的分页均完整，响应日期与请求日期一致。
2. 每条接受记录引用已知股票，且交易日处于证券生命周期内并属于 `CN_A_SHARE` 交易日。
3. 可转债和其他非股票行保留在 Raw，但不产生标准 Record。
4. 每条接受席位匹配一个接受汇总，且 `symbol`、`trade_date`、`source_event_id` 一致。
5. 每个汇总的每侧包含一至五条席位，名次连续且不重复。
6. 汇总满足 `deal_amount = buy_amount + sell_amount` 和
   `net_amount = buy_amount - sell_amount`，比较精度为 CNY 分。
7. 席位的买卖额都存在时，`net_amount = buy_amount - sell_amount`；任一输入缺失时不推导净额。
8. 批次内自然键不能出现冲突记录。

前五席位金额之和不与汇总金额建立相等约束。硬规则失败时记录 `QualityResult`，整个交易日不写
标准事实；不得降级为空成功、保留半套汇总或写入孤立席位。

## Raw、采集与重放

每个交易日创建独立 `IngestionRun`。三个分页数据集全部取得后合并为一个 Raw JSONL 对象：

```text
eastmoney/trading_billboard/year=YYYY/month=MM/day=DD/<ingestion-id>.jsonl
```

Raw schema 固定为 `eastmoney.trading_billboard.v1`。每行增加 Pipeline 自有的
`record_kind=summary|buy_seat|sell_seat`，用于版本化 Normalizer 路由；来源字段和值保持原样。
Manifest 记录 SHA-256、字节数、来源行数、请求交易日和三个来源分页计数。

重放创建新的 `IngestionRun`，引用既有 RawManifest，并完整经过 Normalizer、Validator、
`IngestionEnvelope` 和 Persistence。重放不得访问东方财富或复制 Raw 对象。

## 持久化

新增内部 schema `billboard`。

### `billboard.entry`

- `entry_id uuid` 内部主键；
- `TradingBillboardRecord` 的标量业务字段；
- `ingestion_id`、`content_hash`、`created_at`、`updated_at`；
- 唯一约束 `(source_code, source_event_id)`；
- 唯一约束 `(symbol, trade_date, reason_code)`；
- 外键关联 Security 和统一 A 股交易日。

### `billboard.seat`

- `entry_id`；
- `source_code`、`source_event_id`、`symbol`、`trade_date`；
- `TradingBillboardSeatRecord` 的其余字段；
- `ingestion_id`；
- 主键 `(entry_id, side, rank)`；
- 组合外键保证来源事件、股票和日期与父汇总完全一致。

索引为：

- `entry(trade_date, symbol, entry_id)`；
- `entry(symbol, trade_date desc, entry_id)`；
- `seat(entry_id, side, rank)`；
- `seat(symbol, trade_date desc)`。

同一日期回补创建新的采集批次和 Raw。内容哈希未变化时返回幂等结果；内容修订时在一个事务内
更新汇总、替换该汇总的席位并把最新事实指向新 `ingestion_id`。旧来源版本由不可变 Raw 和采集
记录保留。一个日期一个事务，某日失败不得回滚已完成的其他回补日期。

生产结构只能由一个新的有序 SQL migration 建立。内部表不得直接授权消费者；Worker 只获得
完成幂等写入所需的最小权限。

## Provider、Service 与命令边界

新增专用 `TradingBillboardProvider` capability，不扩大不断膨胀的普通
`MarketDataProvider`。首个实现为 `EastmoneyTradingBillboardProvider`。

建议模块：

- `domain/trading_billboard.py`；
- `providers/eastmoney_trading_billboard.py`；
- `persistence/trading_billboard_postgres.py`；
- `trading_billboard_service.py`。

Service 提供单交易日采集和有界日期范围回补。日期范围回补按日顺序执行，每日独立创建
WorkflowRun/JobExecution 与 IngestionRun，支持从首个失败日期继续恢复。CLI 必须要求显式日期或
起止日期，不提供无界全历史命令。

一个成功采集批次只有 `eastmoney` 一个实际 Provider。网络错误、限流、响应结构漂移和校验失败
均抛出可诊断 `ProviderError` 或领域质量错误，不自动切换来源。

## Worker 调度与运行记录

Workflow code 建议为 `trading_billboard_daily`，Job ID 建议为
`trading-billboard-daily`。任务在周一至周五 `20:30 Asia/Shanghai` 由 Worker 进程内
APScheduler 触发，执行前检查统一交易日历；非交易日正常跳过。

网络连接、读取、单页行数、最大页数和重试次数全部使用代码控制的有界常量。运行指标记录：

- 汇总记录数；
- 买入席位数和卖出席位数；
- 过滤的可转债/非股票数；
- 三个数据集的分页数和来源行数；
- 接受、拒绝和质量规则计数。

新增任务必须进入受控任务目录，并同步 Operations workflow/job code 数据库约束和本地只读任务页。
不得生成 Windows Task Scheduler、cron 或其他操作系统定时触发说明。

## 公开读取契约

新增两个只读、`SECURITY INVOKER`、5 秒 statement timeout 的 `api_v1` RPC：

```text
api_v1.query_trading_billboard_by_date(
    p_trade_date,
    p_limit default 100,
    p_offset default 0
)

api_v1.query_trading_billboard_by_symbol(
    p_symbol,
    p_start_date,
    p_end_date,
    p_limit default 100,
    p_offset default 0
)
```

日期查询只读取准确日期，不回退到更早日期。股票查询要求标准股票代码，起止日期闭区间且跨度
不超过 366 个自然日。`limit` 必须为 1 至 500，offset 必须非负并受实现规定的最大值限制。
结果按 `trade_date desc, symbol, entry_id` 稳定排序。

每条结果包含汇总字段，以及按 `rank` 排序的 `buy_seats`、`sell_seats` JSON 数组。席位数组中的
每项显式返回 `symbol` 和 `trade_date`。无事实时返回空集合，不触发实时东方财富请求。

外部 FastAPI 只调用上述 `api_v1` RPC，不访问内部 schema，并提供：

- 按准确交易日查询；
- 按六位股票代码和日期范围查询。

迁移和代码必须同步更新 `contracts/postgrest-openapi-v1.json`、
`contracts/agent-tools-v1.json` 和 `contracts/fastapi-openapi-v1.json`。

## 测试与验收

### 单元与 Provider 测试

- 同一股票同日多个上榜原因；
- 来源事件键、语义键和父子关系冲突；
- 席位 Record 的 `symbol`、`trade_date` 与父记录不一致；
- 代码 `0` 的多条“机构专用”；
- 买卖两侧独立排名及金额相同时的确定性排序；
- `None`、零、负净额、CNY、百分点和 `Decimal` 转换；
- 东财响应混入可转债；
- 分页、空页、重复页、总数变化、字段缺失和来源错误；
- 三个数据集部分成功时不产生标准写入；
- Raw v1 重放得到相同自然键、名次和内容哈希。

### Service、Persistence 与集成测试

- 单日成功、日期范围逐日提交和失败后恢复；
- 相同内容幂等、来源修订、组合外键和事务回滚；
- 未知证券、非交易日、证券生命周期和金额恒等式；
- 空库迁移、RLS/GRANT、Worker 最小权限和消费者不可见内部表；
- 两个 RPC 的日期范围、分页、稳定排序、空集合、权限和超时；
- FastAPI、PostgREST 和 Agent 三份契约同步。

### Worker 与生产检查

- 20:30 任务目录注册、时区、非交易日跳过和有限重试；
- Operations workflow/job code 约束和本地只读任务页展示；
- 不存在操作系统级计划任务说明；
- 测试环境只使用隔离的 `TEST_DATABASE_URL`。

实现完成后的本地门禁为：

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration  # 仅隔离的 disposable PostgreSQL
```

## 数据授权与运行风险

东方财富龙虎榜页面声明数据来源为东方财富 Choice。公开页面可访问不代表自动化采集、长期保存和
再分发已获授权。生产启用前，项目所有者必须完成东方财富条款、相关交易信息权利和消费者暴露范围
审阅；未完成时只允许 mocked 测试和受控只读验证，不启动生产调度。

接口没有公开稳定契约，因此 Provider 的字段白名单和 schema version 是运行门禁。出现未知必需
字段变化时失败并保留诊断，不自动猜测新语义。

## 治理与实施门禁

本文件记录已确认的设计，不是 Accepted ADR。根据项目宪法，实施前必须依次完成：

1. 创建 GitHub Issue，记录范围、来源授权前提、契约和验收标准；
2. 创建并由项目所有者接受新的 TradingBillboard ADR；
3. 将本设计整理为当前领域详设并与 Accepted ADR 对齐；
4. 编写有序 SQL migration 和测试；
5. 按测试驱动方式实施 Provider、Domain、Service、Persistence、Worker 和公开契约；
6. 通过完整本地门禁和隔离 PostgreSQL 集成测试后再申请生产启用。

在 GitHub Issue、Accepted ADR 和数据授权审阅完成前，不得实现或启用生产采集。
