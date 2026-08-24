# Architecture Decision Records

ADR 记录已经接受的架构决策。其优先级低于项目宪法，高于一般领域说明和历史文档。

## 状态

- `Proposed`：讨论中，不得作为编码依据；
- `Accepted`：已经定稿，代码和 migration 必须遵守；
- `Superseded`：已被后续 ADR 替代；
- `Deprecated`：不再适用，但没有直接替代方案。

## 规则

1. 已接受 ADR 不直接改写核心结论；变化通过新 ADR 替代旧 ADR。
2. PR 必须链接相关 ADR。
3. 实现与 ADR 冲突时，先修改决策，不允许在代码中静默偏离。
4. Wiki 只同步 `Accepted` 决策和已经合并的实现事实。

## 当前 ADR

- `ADR-0047-股东人数点时事实与Tushare采集.md`：新增 ShareholderCount 追加式点时事实，以 Tushare `stk_holdernumber` 受控回填并由 Worker 每日增量采集，区分严格 as-of 与 current-known 查询。
- `ADR-0046-东方财富股票龙虎榜采集与只读契约.md`：新增独立 TradingBillboard 领域，以东方财富采集 A 股每日上榜汇总和买卖前五席位，提供按日期、股票和席位的有界只读契约。
- `ADR-0045-腾讯实时五档API请求时直连且不落库.md`：实时五档 FastAPI 在请求时有界直连腾讯且不落库；仅对该接口替代 ADR-0044 的持久化读取边界。
- `ADR-0044-腾讯批量实时五档Provider与只读契约.md`：其 Provider 字段、单位、Raw replay 与显式采集决定继续有效；持久化 FastAPI 读取边界已由 ADR-0045 替代。
- `ADR-0043-六位股票代码集合最新每日指标只读契约.md`：按 1–500 个六位股票代码解析标准 symbol，逐股票返回 Core 热数据中最新每日指标的有界 FastAPI 只读契约。
- `ADR-0042-集合竞价一字形态只读契约.md`：以竞价序列 sample_seq 1–29 的完整成功事实，实时筛选29轮同价且相对昨收涨跌幅位于 [-4%, 4%] 的沪深上市股票，并提供有界 FastAPI 只读接口。
- `ADR-0041-20260818竞价快照五档一次性受控修复.md`：仅允许对固定的 2026-08-18 ingestion 在完整 Manifest、Raw 与数据库 symbol 一致性门禁下原地补齐五档及封单额；不开放通用 Raw replay。
- `ADR-0038-沪深120交易日收盘新高每日物化快照.md`：以 21:30 Worker 任务物化版本化快照，API 只读最近 ready 批次；替代 ADR-0037 的请求时实时聚合。
- `ADR-0037-沪深股票收盘价近120交易日新高只读契约.md`：沪深范围、连续 120 日、严格突破与排序口径继续有效；请求时实时聚合已由 ADR-0038 替代。
- `ADR-0036-883423乖离率数据库优先实时兜底.md`：接口优先读取有界数据库 RPC；无、少于 34 条或陈旧时读取固定同花顺年度行情，Raw 后立即返回并异步幂等登记 BoardIndex 日 K。
- `ADR-0035-883423板块MA5乖离率只读契约.md`：其数据库唯一读取决定已由 ADR-0036 替代；MA5、BIAS5、前日方向及 30 日极值公式继续有效。
- `ADR-0028-暂停全市场竞价Raw重放与移除自动最终化.md`：保留 09:25:30 五档来源采集和 Raw，暂停缺少原冻结全集身份的 replay，并移除 21:30 自动最终化及旧 JobStore 残留；不增加替代计划。
- `ADR-0027-沪深全市场开盘竞价快照与涨停池最终化.md`：接受单 endpoint 沪深 listed-stock 来源快照；其 Raw replay、调度时点与 21:30 自动最终化部分已由 ADR-0028 及其澄清替代。
- `ADR-0026-统一PYTDX能力节点池.md`：Daily Bar 与五档行情共用带能力标记的本地节点池，由 Worker 启动时及每 12 小时有界刷新，失败时保留 last-good。
- `ADR-0023-可转债领域.md`：新增 `convertible_bond` 独立 schema（基础条款/每日行情/转股价调整/赎回事件），复用 core.security 但不复用 core.daily_bar；Tushare cb_* 主源。
- `ADR-0022-集合竞价涨停池五档快照采集.md`：冻结当日涨停池，由单个 Worker 会话在 09:15–09:25 有界采样，并以 live validation 门禁防止误解竞价档位。
- `ADR-0021-沪深主板涨跌停事实与通用股票池.md`：内部未复权日 K 与每日指标确定性计算对称涨跌停事实和不可变股票池，不依赖第三方榜单。
- `ADR-0020-扣非净利润点时事实与增量同步.md`：以 disclosure_date 发现增量、fina_indicator 拉取金额，保留修订点时历史并提供有界 as-of 查询。
- `ADR-0019-Operations运行可观测模型.md`：以代码任务目录和 PostgreSQL WorkflowRun/JobExecution 事实记录统一调度定义、步骤进度、attempt 与崩溃恢复。
- `ADR-0018-Worker本地只读管理页面.md`：统一 Worker 通过 loopback-only 标准库 HTTP 页面只读展示受控任务定义、JobStore 状态和 Worker 存活信息。
- `ADR-0017-统一Worker进程内调度.md`：只保留 `market-data-center worker` 产品入口，将 APScheduler 作为采集 Worker 的内部实现，统一 systemd/容器部署。
- `ADR-0016-跨平台调度与运行可靠性.md`：统一普通日采集、每日指标和陈旧运行恢复的跨平台调度，定义完整性门禁、单实例、健康检查与隔离测试环境。

- `ADR-0014-股票每日指标快照.md`：定义每日估值、换手率、股本、市值和涨跌停状态快照，以及 Tushare 单位映射、Raw 重放和存储边界。
- `ADR-0015-股票每日指标调度与Core保留策略.md`：定义每交易日全市场采集、休市跳过和一个自然月 Core 热数据保留边界。
- `ADR-0013-Tushare基础数据源接入.md`：允许显式选择 Tushare 获取证券目录、交易日历和未复权股票日 K，固定凭据、单位、Raw 重放与默认路由边界。
- `ADR-0012-股票实时五档行情.md`：定义可追溯的股票买卖五档采样快照、Provider
  边界、单位、校验、存储、单次采集与 authenticated-only 查询。
- `ADR-0011-FastAPI外部只读接口.md`：在既有 `api_v1` 有界 RPC 之上增加外部只读 FastAPI、API Key、健康检查和独立部署；不触发采集或衍生指标计算。
- `ADR-0010-PostgREST稳定查询与Agent工具契约.md`：其“不引入 FastAPI”决定已被 ADR-0011 替代；有界 RPC、版本一致性、权限、错误和兼容契约继续沿用。
- `ADR-0009-版本化复权行情与客观Metrics.md`：定义复权公式、算法版本、输入水位线/哈希、重算与客观 Metrics 语义。
- `ADR-0008-Classification分类与成分历史.md`：定义分类命名空间、完整目录/成分快照、历史重建、有效区间和 AKShare 行业/概念接入。
- `ADR-0007-Capital与公司行为基础事实.md`：定义股本、分红送转和配股输入事实的自然键、单位、修订语义与 AKShare 接入。

- `ADR-0001-第一阶段架构基线.md`：第一阶段开工所需的领域和工程基线。
- `ADR-0002-AKShare第二数据源接入.md`：允许显式选择 AKShare 第二数据源，不引入自动路由或回退。
- `ADR-0003-同花顺动态板块指数.md`：以独立 BoardIndex 领域接入同花顺动态板块指数及每日成分股快照。
- `ADR-0004-pytdx日K补数源.md`：历史决定；本地 `.day` Daily Bar Reader 已被 ADR-0024 取代。
- `ADR-0024-远程TDX日K数据源.md`：远程未复权 Daily Bar 语义继续有效；显式 endpoint 配置和禁止运行时发现的决定已由 ADR-0026 取代。
- `ADR-0025-Standalone-PostgreSQL-FastAPI.md`：独立 FastAPI 直接读取 PostgreSQL；首版复用现有数据库，不引入额外平台服务依赖，并保留未来 standalone 切换门禁。
- `ADR-0029-同日涨停不可变快照与22点填充任务.md`：定义同日涨停来源观察、版本化快照、封板口径和 22:00 依赖门禁。
- `ADR-0030-DailyLimitUpList切换TodayLimitUp契约.md`：将既有 daily-limit-up-list 明确切换为版本化同日涨停领域读契约。
- `ADR-0005-Provider自动路由与故障切换.md`：按数据集能力自动选择 Provider，仅对来源错误执行确定性回退和进程内熔断。
- `ADR-0006-Raw重放与运行恢复.md`：重放创建新 IngestionRun 并引用原 RawManifest，同时定义僵尸运行恢复与只读多源差异报告。
- `ADR-0031-近20交易日涨幅Top10只读契约.md`：定义未复权收盘价 20 交易日（19 区间）确定性排名与遗漏语义。
- `ADR-0032-0926集合竞价一字涨跌停只读契约.md`：定义只使用 09:26 快照和版本化涨跌停价的一字板证据。
- `ADR-0033-当日集合竞价虚拟撮合明细.md`：定义当日单证券虚拟参考/匹配明细、Raw/版本/许可门禁和只读 API；明确它不是逐笔成交或逐笔委托。
- `ADR-0033-live-fetch-clarification.md`: accepts bounded on-request fetch with narrow append-only persistence before response.
- `ADR-0034-沪深全市场开盘竞价序列快照.md`：接受工作日 09:15–09:25:20 每 20 秒采集沪深上市股票全集，使用独立月度分区事实表和两线程早盘执行器，不改变 09:26 契约。
- `ADR-0039-09点26沪深主板一字涨跌停实时计算.md`：取代 ADR-0032 的晚间 CalculationRun 依赖，基于已存 09:25:30 快照只读实时计算沪深主板一字涨跌停。
- `ADR-0040-竞价序列买一价量额语义.md`：明确 09:25 前全市场竞价序列按买一价、买一股数及两者乘积复用价量额字段，并用显式语义标识区分开盘成交和历史记录。
