# 领域详设：DragonTiger A股龙虎榜资金事实与客观特征 v2

> 状态：质量修复设计已批准，待 migration、实现与生产回补
> 日期：2026-09-08
> 关联 Issue：#70
> 上级决策：`adr/ADR-0049-DragonTiger事实与时点安全特征.md`、
> `adr/ADR-0053-DragonTiger历史覆盖与质量修复.md`（Accepted）

## 1. 边界与依赖

```text
EastMoney / Tushare
        ↓
DragonTigerProvider → immutable Raw + durable Manifest
        ↓
window/reason/seat normalization + source findings + validation
        ↓
DragonTigerEvent ──< SeatTrade >── TradingSeat
        │                                  ↑
        │                    SourceIdentity ──< AliasObservation
        ↓
pure objective analytics → as-of Feature
        └────────────────→ separately available Label
```

复用 `core.security`、`core.trading_calendar`、`core.daily_bar`、每日指标、涨停池和版本化
`derived.calculation_run`。本领域不复制证券、日线、市值、涨停或市场环境事实。

Fact 层只保存来源披露事实及可审计的来源质量元数据。主观 CapitalQuality 分数、游资身份、策略、
回测和交易建议仍属于消费者项目。

## 2. 事实对象

### 2.1 DragonTigerReason

- `reason_id: UUID`
- `reason_code: str`：项目稳定代码
- `reason_name: str`
- `reason_type: PRICE_DEVIATION | TURNOVER | AMPLITUDE | CONTINUOUS_LIMIT | ST | OTHER`
- `description: str | None`
- `is_active: bool`

`ReasonSourceAlias` 以来源代码、来源原因代码和来源原文保存真实映射，并关联经过验证的触发窗口规则。
没有核实的原因不得根据中文片段虚构语义，必须以 `DT_PERIOD_MAPPING_UNSUPPORTED` 失败关闭。

### 2.2 TriggerWindow / AmountPeriod

`TriggerWindow` 表达“为什么在披露日触发上榜”：

- `basis: MARKET_SESSIONS | SECURITY_TRADED_SESSIONS`
- `session_count: int > 0`
- `occurrence_count: int | None`
- `start_date: date | None`
- `end_date: date == trade_date`

市场交易日窗口使用 `CN_A_SHARE` 日历解析起点。证券有成交交易日窗口只使用该证券已确认的未复权
`core.daily_bar` 日期；输入不足时起点保持 `NULL` 并产生质量码，不得替换为市场日历。

`AmountPeriod` 表达 Event 和 SeatTrade 金额所属周期：

- `basis: MARKET_SESSIONS | SECURITY_TRADED_SESSIONS | SOURCE_UNSPECIFIED`
- `session_count: int | None`
- `start_date / end_date: date | None`

只有来源字段或已验证来源展示语义能够证明金额周期时才填写具体周期。单日榜和已验证三日榜分别为
市场 1/3 个交易日；10/30 日及北交所触发规则首版保持 `SOURCE_UNSPECIFIED`，禁止从触发原因反推。

### 2.3 TradingSeat / SourceIdentity / Alias

`TradingSeat` 保存 `seat_id`、canonical/broker/branch 名称、`BROKER|INSTITUTION|NORTHBOUND|OTHER|UNKNOWN`
类型、可空省市、首次/末次出现日期及 active 状态。

`TradingSeatSourceIdentity` 保存：

- `identity_id / seat_id`
- `source_code / source_seat_key`
- `first_seen_date / last_seen_date`
- 唯一键 `(source_code, source_seat_key)`

`TradingSeatAlias` 是来源身份下的时点名称观察，保存 `identity_id`、`alias_name`、首次/末次观察日期，
唯一键为 `(identity_id, alias_name)`。可靠来源代码只按代码解析身份；历史改名不创建新 Seat，同名可
属于不同来源身份。泛化占位名或无可靠代码的来源行不创建 SourceIdentity/Alias，`seat_id` 为 `NULL`。

### 2.4 DragonTigerEvent

- `event_id: UUID`
- `symbol: str`
- `trade_date: date`
- TriggerWindow 全部字段
- AmountPeriod 全部字段
- `reason_id: UUID`
- `reason_name_raw: str`
- `close_price / change_pct / turnover_amount / turnover_rate / amplitude: Decimal | None`
- `lhb_buy_amount / lhb_sell_amount: Decimal | None`
- `buy_disclosure_present / sell_disclosure_present: bool`
- `source_code / source_record_id / ingestion_id / content_hash`

至少一侧披露存在。来源自然键为 `(source_code, source_record_id)`；同来源语义冲突整批失败。
原 `period_type` 不再参与事实身份，也不在 v2 公共契约中返回。

### 2.5 SeatTrade

- `seat_trade_id / event_id`
- `seat_id: UUID | None`
- `seat_name_raw: str`
- `buy_amount / sell_amount: Decimal | None`
- `buy_rank / sell_rank: int | None`
- `is_institution / is_northbound: bool`
- `source_code / source_record_id / ingestion_id / content_hash`

至少一侧金额存在且不允许负数；两侧不能同时为零；至少一个名次存在且名次为 1～5。可靠来源身份
相同才允许跨买卖榜合并。不可靠身份按来源行保存，禁止仅按名称合并。`net_amount` 仅在两侧金额都
披露时计算；`NULL` 不等于零。

### 2.6 SourceFinding

Normalizer 输出可空的来源质量发现，字段包括稳定 rule code、severity、source event ID、report kind、
出现数和过滤数。不得保存完整来源行或任意异常文本。首版规则包括重复过滤、零活动占位过滤、单侧
披露和无法解析证券有成交窗口起点。

## 3. Provider 语义

### 3.1 EastMoney

保留固定端点、有界 HTTP、Decimal JSON 和 Raw 包装。采集顺序为汇总、买入明细、卖出明细：

- 单页明细按原报告冻结；多页明细改为按冻结汇总中的 `TRADE_ID` 有界逐事件读取；
- Raw 保存所有返回行，包括完全重复行和零金额占位；
- Normalizer 使用显式原因映射表生成 TriggerWindow，不使用单一模糊 substring；
- 完全相同来源行确定性去重，非完全重复自然键冲突失败；
- 明确零买零卖的来源占位不创建 SeatTrade；
- 只有一侧榜单时仍构造 Event，缺失侧保持 `NULL`；
- 可靠 `OPERATEDEPT_CODE` 跨买卖榜合并；代码 `0` 和泛化机构名不合并；
- BUY/SELL 任一缺失保持 `None`，忽略来源派生 NET；
- 北交所与其他 `core.security` 中有效 A 股进入同一标准验证。

新采集 Raw schema 升版。现有 `eastmoney.trading_billboard.v1` 和 `eastmoney.dragon_tiger.v2` Raw 永久
保持不变，并由各自版本化 normalizer 重放。

### 3.2 Tushare

`top_list` 提供事件汇总，`top_inst` 提供席位明细。Adapter 以日期、股票、触发窗口和原因构造确定性
来源事件标识；席位无可靠代码时按来源披露行保留。两个接口必须同批成功，不与 EastMoney 拼接。
`top_list` 混入的可转债行保留在 Raw，并以 `DT_NON_STOCK_SECURITY_FILTERED` 记录过滤事实，
不得伪装成沪深股票事件。历史重复汇总只在核心市场事实一致时合并；来源修订值无法判定时保持空值。
Tushare DragonTiger 仍不自动切换或仲裁来源冲突。

### 3.3 BSE Security

独立 `security_bse_daily` ingestion 显式使用 Tushare `stock_basic` 的 `L/D/P` 三种状态，Raw 保存
完整来源结果，标准事实只发布 BSE 股票。该批次不与 BaoStock 或 AKShare 合并；Provider 权利和 token
预检继续作为生产启用门禁。

## 4. 应用与持久化

DragonTiger 采集生命周期：

1. `begin_ingestion` 持久化 `RUNNING`；
2. Provider 返回冻结 Raw；
3. RawStore 写不可变对象；
4. `attach_raw_manifest` 独立持久化 manifest；
5. Normalizer 生成 Event drafts 与 SourceFindings；
6. Persistence 解析交易日窗口、原因和可靠席位身份；
7. Domain 校验自然键、证券、窗口、金额、排名和披露状态；
8. `publish_success` 原子写入质量、Event/SeatTrade 并结束成功；
9. 任意异常由 `complete_failure` 独立事务结束，并保留已登记 manifest。

每个状态转换检查 ingestion ID 和期望前态。终态幂等重试只有在内容一致时成功，冲突重试失败。
operations 仅保存稳定错误码和受控阶段名。僵尸 `RUNNING` 继续按 ADR-0006 恢复。

孤儿 Raw 恢复命令只接受配置的 DragonTiger Raw 根目录，先 dry-run，再校验绝对路径范围、UUID、
schema、单一交易日、JSONL、SHA-256、字节数和行数。通过后补登记失败 run/manifest 并写
`DT_RECOVERED_ORPHAN_RAW`；永不修改、移动或删除 Raw。

## 5. 客观 Analytics 与 Feature/Label

`DragonTigerCapitalMetrics` 纯函数计算可计算净额、龙虎榜金额/全天成交额、买卖席位数、明确纯买/
纯卖数、重叠数、机构/北向金额和 top1/top3/top5 集中度。

依赖缺失披露侧的指标返回 `None`；仅依赖已披露侧的指标仍可计算。每个指标响应同时返回披露覆盖、
金额周期是否已验证及 Event 质量码，消费者不得把 `SOURCE_UNSPECIFIED` 金额周期静默当作单日。

`TradingSeatProfile` 以 `seat_id + as_of_date + algorithm_version` 生成客观历史统计。只有
`label_available_date <= as_of_date` 的 T+1/T+3/T+5 Outcome 可进入画像。Feature 必须使用严格早于
事件日的 Profile；Label 独立保存，不能进入预测 Feature。本次修复不新增主观分数。

## 6. 公共查询与边界

- 日期查询和股票历史查询返回 Event、Reason、窗口语义、金额周期、披露状态、质量码和嵌套 SeatTrade；
- 两者删除 `period_type`，增加可选 `trigger_window_basis` 与 `trigger_window_sessions` 过滤；
- 席位历史只接受稳定 UUID，返回事件时点 `seat_name_raw` 和窗口语义；
- Event Metrics 返回客观指标、披露覆盖、金额周期验证状态和质量码；
- 日期范围最多 366 天，limit 1～500，offset 0～10,000，数据库超时 5 秒；
- FastAPI 只调用 `api_v1` RPC，内部 schema 启用 RLS，不向 API 角色授权直接读取。

旧 SQL 函数签名由 ordered migration 显式删除，避免 PostgREST 同名重载。PostgREST、FastAPI 和
Agent Tools 三份 checked-in contract 必须与实现同批更新。

## 7. Worker 与回补

- `security_bse_daily`：工作日 20:15，Tushare BSE 全状态证券同步；
- `dragon_tiger_daily`：工作日 20:30；当日 BSE 前置任务失败时显式失败，不发布不完整批次；
- 所有调度继续由 Worker 内 APScheduler 注册，不增加操作系统级计划任务。

生产回补先同步 BSE 主数据，再恢复孤儿 Raw，重放已登记失败 Raw，最后重新抓取无可用 Raw 或仍失败
的日期。范围从 2025-01-01 到运行时最近一个已收盘交易日。每个日期独立提交、失败继续、状态可恢复；
不得跨 Provider 拼接、使用旧日期代替缺口或伪造空成功。

## 8. 验收

- 普通、阿拉伯/中文三日、10/30 日、多次同向异常和北交所有成交窗口映射测试；
- 触发窗口与金额周期严格分离，未知金额周期显式可见；
- 席位改名、同名不同代码、无代码泛化席位和跨侧合并测试；
- 完全重复、跨页逐事件读取、零占位、单侧披露和冲突失败测试；
- 分阶段 ingestion、manifest 保留、终态幂等、僵尸恢复和孤儿 Raw 恢复测试；
- BSE `L/D/P` 同步、调度顺序和依赖失败测试；
- migration、RLS、权限、有界 RPC、三份契约和 FastAPI 测试；
- v1/v2 Raw replay 与新 schema 实时采集测试；
- 完整本地质量门及隔离 PostgreSQL 集成测试通过。
