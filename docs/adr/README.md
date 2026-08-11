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

- `ADR-0027-沪深全市场开盘竞价快照与涨停池最终化.md`：09:26 采集单 endpoint、完整且可追溯的沪深 listed stock 开盘竞价来源快照，21:30 仅以当日成功快照和 ready 涨停池完成最终化；BSE 暂缓。
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
- `ADR-0025-Standalone-PostgreSQL-FastAPI.md`：独立 FastAPI 直接读取 PostgreSQL；首版复用现有数据库但不依赖 Supabase 服务，并保留未来 standalone 切换门禁。
- `ADR-0005-Provider自动路由与故障切换.md`：按数据集能力自动选择 Provider，仅对来源错误执行确定性回退和进程内熔断。
- `ADR-0006-Raw重放与运行恢复.md`：重放创建新 IngestionRun 并引用原 RawManifest，同时定义僵尸运行恢复与只读多源差异报告。
