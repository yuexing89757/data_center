# 领域详设：股票五日均量（包含当日）

> 状态：Accepted（项目所有者于 2026-11-04 确认，授权本地实现；部署状态另行核验）。
> 日期：2026-11-04。
> 依据：ADR-0076，承接 ADR-0009、0017、0019、0068。
> 关联 Issue：[#103](https://github.com/yuexing89757/data_center/issues/103)；实施进度只在 GitHub Issue 维护。

## 需求与计算口径

新增常驻 Worker 的每日计算任务，维护每只股票的每日五日均量。
用户明确包含当日，并已确认下述细化语义。

对 CN_A_SHARE 目标交易日 T，日历确定严格递增的五个交易日 D1…D5，D5=T。

```text
volume_average_5d = Decimal(V(D1) + V(D2) + V(D3) + V(D4) + V(T)) / Decimal(5)
```

V 为标准整数股。均量允许小数股，五个整数的算术平均最多一位小数，不经 float。
例如五日为 100、200、300、400、501 股，均量为 300.2 股。
周末和节假日不占窗口，不得用 `ROWS 4 PRECEDING` 在稀疏行情上跨缺口取数。
从窗口开始日至 T 的每个自然日都必须存在日历记录；中间缺日历行时失败，
不能因仍能查够五个 is_trading_day=true 的日期而跳过未知日期。

候选集合为目标日有效的 SSE/SZSE/BSE `security_type=stock` 证券；
ipo_date 不晚于 T，delisting_date 为空或不早于 T；已知在目标日以后上市的证券剔除。
目标日生命周期无法确认时不猜测其有效性，整批输入依赖检查失败并报告原因。
不包括 THS 动态板块、指数、可转债或 ETF。

每只候选股票对应五个固定日期输入，只有五个日期均有已认证的非负整数成交股数、
且不存在停牌记录时才计算。真实成交零保留并计入覆盖数；缺失、未认证或停牌不补零。
新股在窗口开始之后上市时保留结果行、均量 null，不用较短窗口除以实际天数。
数量准入只检查 `daily_bar.volume` 字段组，不以同时聚合量额的顶层质量状态判断。
金额未认证不得阻断已认证成交量；整条日 K 缺失、量未认证和已认证原量缺失分别记录。

## 存储与版本

新增 `derived.stock_volume_average_5d`，一行是一只股票在一个计算批次内的目标日结果。

| 字段 | 约束与语义 |
| --- | --- |
| calculation_id | 写入边界附加，外键关联 derived.calculation_run |
| symbol | 标准证券代码，外键关联 core.security |
| trade_date | 精确目标交易日 |
| window_start_date | 五日窗口第一日 |
| volume_average_5d | numeric(28,1)，完整时非负，否则 null，单位股 |
| covered_days | 0…5，已准入且非停牌的有效成交量天数 |
| quality_reasons | text[]，完整时为空；否则含确定性排序的原因码 |
| input_evidence | 仅供 Worker/审计读取的 JSONB，五个日期的输入状态和血缘证据 |

主键 `(calculation_id, symbol, trade_date)`；日期/symbol 索引、窗口日期与空值/覆盖数
一致性 CHECK、RLS 及 Worker 最小读写权限由 migration 定义。不得向 anon、
authenticated 或 market_data_api 授予底表读取权。

另设轻量不可变发布表 `derived.stock_volume_average_5d_snapshot`，主键
`(trade_date, version)`，含 calculation_id、published_at、candidate_count、
complete_count，约束 `0 <= complete_count <= candidate_count`。
发布表同时保存当次完整市场 input_evidence、input_watermark 和 evidence_hash；
业务数值哈希继续不包含血缘ID，发布证据哈希则覆盖实际来源和数量认证引用。
单日当前发布由最大 version 唯一确定，结果只从其 calculation_id 读取。
该表解决共享 CalculationRun 成功签名唯一且输入可能 A→B→A 的发布顺序问题，
不是另一套计算引擎。首发计算成功、全部结果及发布记录原子提交。

复用 CalculationRun：calculation_code=`cn_a_stock_volume_average_5d`，
algorithm_version=`1.0.0`，start_date=end_date=T。
输入在 Repeatable Read 中读取候选证券、真实日历和五日单位安全日 K，
最多 10,000 个证券，每证券最多五个输入，不读取全历史。
证券键在同一 Repeatable Read 连接内按每批最多100个查询，全部页共享日历、候选集合和水位。
该分页只约束查询计划及单次读取，不改变数量准入、输入顺序、哈希、窗口或计算结果。
input_watermark 记录依赖工作流、输入水位及单位契约版本；input_hash 按稳定顺序
覆盖日期窗口、证券生命周期、成交量、停牌和单位准入状态；业务未变时血缘 ID 变化
不强制创建新的数值版本。只有当前最新发布的数值输入与证据均相同时返回 unchanged。
数值相同但认证/来源改变时复用原计算、追加发布并保存新证据，不覆盖旧计算及输入证据。

input_evidence 在持久化边界保存每个日期的已准入整数股数或 null、是否缺整条日 K、
trade_status、数量准入原因、可用 ingestion_id、认证契约及认证记录引用。
不保存未认证的错误量值、凭据、原始响应或 Raw 路径。保留输入证据用于审计当时准入；
Raw 超期清除后的来源回放限制继续服从宪法及 ADR-0073。
DTO 不携带 ingestion_id/calculation_id，血缘与证据由 Persistence 单独管理。

同日构建获取日期级 advisory lock，批次及全部候选结果在一个事务内成功提交。
完整发布是指所有候选都具有数值或明确空值原因，不代表每只股票都有均量。
同日重算输入改变时保留旧版本并新建成功计算批次；禁止逐股票从不同批次拼接。
输入 A→B→A 时，在日期锁内复用历史 A 的计算和结果，追加发布 version，
报告 published/reused，不能返回 unchanged；保留 A 原 calculated_at，
以新 published_at 记录本次发布。不得修改共享 CalculationRun 的成功签名唯一约束。

## 缺失与失败

quality_reasons 可包含：`insufficient_listing_history`、`missing_daily_bar`、
`missing_volume`、`quantity_units_unverified`、`suspended`；多原因稳定排序并去重。
NULL 与零严格区分；不足五天时不保存貌似可用的部分均值。
负值、非整数、非有限数、冲突重复自然键、窗口日期错误、证券/日历未知属于硬失败，
整批不发布。没有五个已知市场交易日或没有候选证券时同样失败。
个别证券正常缺失可发布 null，并使 Operations 显示 partial 和完整/不完整股票数；
逐股票原因在受限结果表记录，不扩展通用 Operations 模型；
所有证券均不完整时仍可发布一致的空值快照，但必须显示 partial，不能冒称数据齐全。

## Worker 与边界

任务 `stock-volume-average-5d-daily`，工作流 `stock_volume_average_5d`，
单一步骤 `build_stock_volume_average_5d`，在代码受控目录共享注册、执行记录和本地任务页。
上海时间工作日 21:45 默认启用；沿用 Worker 的单实例、misfire 与运行超时约束。
日期由计划触发的上海日期确定，不以重启时的最新行情日期替代。

先检查目标日日历，休市成功跳过；未知日历失败。同日 daily_market 须为 succeeded/partial
终态，后者只允许实际准入数据参与，不放宽完整五日条件。
日 K 未完成时失败并保留缺口，不等待无限重试，不串入每日指标或技术因子采集流程。
提供显式日期 CLI 重算入口，复用同一 Service、校验、锁和血缘，不默认执行回填。
不访问网络 Provider，不创建 OS 定时任务，不运行生产迁移/重算/部署。

首期不新增公共 API，不修改三份现有契约。将来若需要消费新表，必须通过
单独批准的有界 api_v1 查询，明确目标日、选定一个计算批次和数量质量信息。
本地只读任务页只展示任务与运行摘要，不暴露输入证据或内部数据库路径。

## 实现范围与验收依据

实现应沿用 `close_price_new_highs_service.py` 的日期锁、批次和原子发布分层，
以及 `persistence/derived_postgres.py` 的单位安全读取，不扩展价格均线现有算法版本。
新增独立领域 DTO、纯 Calculator、Service、PostgreSQL Persistence，接入 CLI、
Worker Settings、scheduler、controlled catalog、Operations 白名单；更新 README 与运行手册。
Schema 只能通过有序 migration 改变；历史清理白名单首期不包含新表。

必须覆盖以下验收，实施计划及完成情况记录在关联 GitHub Issue：

- Calculator：包含当日、跨周末/节假日、整数输入的小数均量、真实零、缺日/null、
  停牌、新股不足五日、超大数量精度、非法负值/非整数/重复键、同输入确定性。
- Service/Worker：休市、日历未知、依赖未终态、部分输入、日期锁、幂等、
  日历中间缺行但仍可查够五日、修订新批次、不混批次、旧哈希回归场景、
  同值来源认证替换与证据幂等、失败不改变旧成功结果、时区与调度绑定。
- 隔离 PostgreSQL：迁移和约束、RLS/最小权限、stock-quantity.v1 认证门禁、
  量认证与额认证独立、原子提交/回滚、同日发布版本和同事务一致快照读取。
- 完整本地门禁：`uv run ruff format --check .`、`uv run ruff check .`、
  `uv run mypy src`、`uv run pytest`；集成验证仅使用隔离 TEST_DATABASE_URL。

本地实现使用迁移 `20261104000100_stock_volume_average_5d.sql`；迁移存在不代表生产已应用。
实际验证结果和部署边界见运行手册及关联 Issue，不能据本地实现宣布线上任务已启用。
