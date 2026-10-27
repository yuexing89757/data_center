# StockTechnicalFactor 领域详设

- 状态：已上线；2026-10-24来源尺度修正与补数验收见 docs/runbooks/stock-technical-factor.md
- 验证：[实施记录](superpowers/plans/2026-10-10-stock-technical-factor.md)
- 日期：2026-10-10
- 依据：项目宪法、ADR-0013、ADR-0017、ADR-0066
- 关联 Issue：[#88](https://github.com/yuexing89757/data_center/issues/88)
- 用户要求：原有指标保持不变，独立新增 Tushare `stk_factor_pro` 专业版技术因子，每日定时更新。

## 边界与选择

采用独立的来源技术因子快照领域。现有 `daily_basic`、StockDailyIndicator、普通日 K、
Core 保留策略及公开接口继续原样运行。来源计算结果属于来源事实，不是本项目自主计算
的 Derived 结果；不生成买卖建议、评分或解释标签。

相比扩展现有每日指标宽表，独立快照避免改变旧契约、混淆来源及复权口径。相比每个因子
一行的存储方式，按股票和交易日保存一次完整快照避免将全市场每日采集放大为百万级行。

## 模型与完整字段覆盖

数据集：`stock_technical_factor`。聚合根：`StockTechnicalFactorSnapshotRecord`。
自然键：`(symbol, trade_date)`。证券仅限 Security 已登记的 A 股；日期为 `CN_A_SHARE`
真实交易日。领域记录包含标准 symbol、trade_date、market、source_code、
`factor_catalog_version`，以及 `unadjusted`、`forward_adjusted`、`backward_adjusted`、
`common` 四个指标映射。值为 `Decimal | None`，计数项为 `int | None`。
记录不得携带 ingestion_id。目录版本初始为 `stock-technical-factor.v1`，固定源字段、
标准名和默认计算参数；参数差异必须逐口径保存，不推断各口径参数必然相同。

依据官方文档输出表，v1 覆盖 74 个指标在三种口径下的 222 个值以及 4 个共同计数项。
实施时逐项核对并用目录覆盖测试锁定；不动态接受未知字段，不只选择 MACD/KDJ 等少量指标。

| 指标组 | 标准指标名 |
| --- | --- |
| ASI、ATR、BBI | asi, asit, atr, bbi |
| BIAS、BOLL、BRAR | bias1, bias2, bias3, boll_lower, boll_mid, boll_upper, brar_ar, brar_br |
| CCI、CR、DFMA、DMI | cci, cr, dfma_dif, dfma_difma, dmi_adx, dmi_adxr, dmi_mdi, dmi_pdi |
| DPO、EMV、EXPMA | dpo, madpo, emv, maemv, expma_12, expma_50 |
| EMA | ema_5, ema_10, ema_20, ema_30, ema_60, ema_90, ema_250 |
| KDJ、KTN | kdj_j, kdj_d, kdj_k, ktn_down, ktn_mid, ktn_upper |
| MA | ma_5, ma_10, ma_20, ma_30, ma_60, ma_90, ma_250 |
| MACD、MASS | macd, macd_dea, macd_dif, mass, ma_mass |
| MFI、MTM、OBV、PSY、ROC | mfi, mtm, mtmma, obv, psy, psyma, roc, maroc |
| RSI | rsi_6, rsi_12, rsi_24 |
| TAQ、TRIX、VR、WR、XSII | taq_down, taq_mid, taq_up, trix, trma, vr, wr, wr1, xsii_td1, xsii_td2, xsii_td3, xsii_td4 |
| 共同计数项（无复权口径） | down_days, up_days, low_range_days, high_range_days |

Provider 映射规则：`*_bfq` → unadjusted，`*_qfq` → forward_adjusted，
`*_hfq` → backward_adjusted。`ema_qfq_10` 等插入式口径字段转为 `ema_10`，
`rsi_qfq_6` 转为 `rsi_6`。来源 `kdj_qfq` 转为 `kdj_j`，禁止把它误作 K 值。
`downdays/updays/lowdays/topdays` 分别转为上表共同计数项。

接口中的 OHLC、复权因子、成交量额、估值、股本和市值随完整响应保存在 Raw；
本领域标准事实仅保存技术因子，不复制这些已有事实，也不建立新的复权价格事实。
公开字段不泄漏 bfq/qfq/hfq 等来源命名。

## 采集、Raw 与质量

- 明确只调用 `stk_factor_pro(trade_date=YYYYMMDD)`，请求完整受控输出字段集合，
  使用现有 Tushare HTTP 客户端及配置；JSON 数字直接解析为 Decimal。
- Raw schema 为 `tushare.stock_technical_factor.v1`；完整来源响应不可变保存，
  request_params 记录接口及精确交易日，重放必须复用同一目录映射与校验。
- 单次上限 10,000 行；空响应或达到上限视为不可确认完整，整批失败，不截断写入。
  无权限、限流、超时保留明确失败，不回退其他接口或旧交易日。
- 请求节流默认不超过 30 次/分钟，有限次瞬时错误重试，不重试权限错误。
- 校验请求日期匹配、证券身份与生命周期、日历、自然键唯一、有限数值与完整目录。
  可选值缺失为 None，零保持零；负值是 MACD/CCI 等合法结果，不能统一禁止负数。
  计数必须为非负整数，NaN/Infinity/非法文本或冲突重复键阻断整批事实写入。
- 同内容重复键去重；对可选值为空的单元保留空值，不补值。
  响应结构缺少已请求字段须报错，不能把结构错误掩盖成可选值为空。
- 使用已有事务、任务锁、IngestionEnvelope、RawManifest、IngestionRun、QualityResult；
  一次成功仅对应 Tushare，一个完整快照必须来自同一采集批次。
- 检查前一个成功快照行数作为覆盖基线，对显著下降阻断发布并报告缺口；
  首次运行以数据库当日活跃股票数作为基线，阈值沿用每日指标的受测规则。
  证券停牌或上市初期可能缺指标，不造行补齐，公开缺失代码。

## 存储与查询

新增 `core.stock_technical_factor_snapshot`，自然键主键及 Security 外键；
四组映射用 JSONB 保存标准指标名与精确 JSON 数字/空值，公共契约统一序列化为
十进制字符串/空值，避免客户端浮点精度损失。source_code、目录版本及 ingestion lineage
在表内保存；每次自然键 UPSERT 整体替换四组值，禁止合并不同批次的因子。
同日完整快照重采在同一事务删除当日新批次未返回证券的旧记录，再整批 UPSERT，
确保当日只有一个成功批次的事实并显式暴露缺口；失败回滚到此前快照。
正式采集和 Raw 重放使用同一个来源/数据集/日期任务锁，防止并发修订交错。
SQL migration 扩展数据集约束、operations 工作流约束、RLS、Worker 权限，
只允许受控 api_v1 RPC 读取，不直接给消费者内部表权限。
既有备份角色保持对应用数据的只读范围，新表显式授予 SELECT；不增加任何写权限。

新增有界 RPC `api_v1.query_stock_technical_factors(p_symbols text[], p_trade_date date)`：
1～100 个标准股票 symbol、一个必填精确交易日，最多 100 条快照。
返回四组指标、目录版本及来源信息；不暴露 ingestion_id，不回退历史日期。
同步 PostgREST 与 agent-tools 契约；FastAPI 增加独立只读查询入口，六位股票代码
由 Security 解析，返回 items 和 missing_codes，仅调用 api_v1 的有界 RPC。
日期字段使用 YYYY-MM-DD，若包含时间字段须使用 ApiTimestamp。

首期保留每日历史快照，不新增自动清理，也不将旧每日指标一个月策略套用到新领域。
不启动全历史回补。支持显式日期的单日采集命令和既有 Raw 重放入口。

## 每日更新

2026-10-16运维修正见ADR-0066：既有21:00入口增加22:00、23:00机会，
次日补采原为08:30，2026-10-27按Issue #100调整为04:00，仍显式补采上海昨日。两类入口继续使用同一工作流与单日完整批次，
不会将前一日数据当作当天发布；实际生产启用以runbook部署记录为准。

Worker 目录任务 `stock-technical-factor-daily` 默认周一至周五 21:00、22:00、23:00
Asia/Shanghai 执行；`stock-technical-factor-catchup` 每日 04:00 补采上海前一日。
两者由 APScheduler 在 Worker 进程内执行；上述时间不是来源承诺的数据就绪时间。
先同步并确认目标日真实交易日；休市跳过；快照始终归属精确目标日。

复用单实例、coalesce、misfire 配置、数据库锁与 operations 执行记录；
Worker 本地只读任务页可见任务和执行状态。超过当日的延迟运行需按当前上海日期重新
核验，不因旧调度时间写入错误日期。失败须可诊断、可用显式单日命令重试，
不得把失败记录为成功。OS 只监管 Worker 常驻，不注册 OS 采集计划任务。

## 实施与验收范围

1. 编写 migration、领域/目录、Mock Provider 与 Raw replay 回归测试。
2. 实现领域、Provider、Pipeline/Persistence 的独立快照路径与原子发布。
3. 新增 CLI、scheduling_catalog、scheduler、operations 与任务页接线。
4. 新增有界 RPC、FastAPI 模型/查询、三份契约及独立数据库集成测试。
5. 更新 README 和独立运行手册，保留工作区已有修改。

验收覆盖 226 个因子、三种复权口径、特殊命名、Decimal/空值/零值/负值、
无权限/限流/超时/空响应/上限、错日期/未知证券/生命周期/重复键、Raw 重放、
事务回滚/幂等修订、RPC 边界/RLS/契约、交易日/休市/失败/任务目录注册。
回归现有每日指标和行情行为。运行完整 ruff format/check、mypy、pytest；
integration 仅使用隔离 TEST_DATABASE_URL。实现不包括生产迁移、采集或部署。

## 官方依据及外部限制

[Tushare 股票技术面因子专业版](https://tushare.pro/document/2?doc_id=328)。
文档当前列出单次最多 10,000 条、5,000 积分 30 次/分钟、8,000 积分以上
500 次/分钟。账户权限和真实数据覆盖须在获准生产操作后另行核验。


## 2026-10-24 技术因子来源尺度修正（Accepted）

项目所有者最终明确同意：OBV、EMV、MAEMV 按 Tushare 原生指标值完整保存和返回，
分别保持不复权、前复权、后复权口径，不做手/股换算。本决定替代此前该领域的新批次
整批阻断及叶子null隔离方案。三个指标采用独立的来源指标契约，不声明为股或无量纲，
不将来源原生尺度误写为已认证的股票数量单位。其他股票数量、金额门禁不变。

所有因子继续执行日期、证券、完整目录、有限数值、重复键及市场覆盖校验；
合法零和负数保留，来源缺失为null，Raw不可变。公开目录升级stock-technical-factor.v3，
来源目录保持v1，quantity_quality.status=source_native、reason=provider_native_factor_scale。
v2曾返回null的9个叶子现在返回来源原值，调用方不得额外乘除100；适用于所有查询日期。
不重新解释或改写历史源JSON。通过20261024000100迁移更新既有只读投影视图，
不更改表结构、RPC边界或权限；FastAPI/PostgREST/agent契约同步。
标准请求日期统一ISO；历史YYYYMMDD在重放的新批次中规范化，原审计不改写。
继续使用既有Worker晚间三次及次晨补偿，无新增OS任务。
