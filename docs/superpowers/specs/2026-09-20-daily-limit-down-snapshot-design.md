# 每日跌停不可变快照与只读列表设计

- 状态：项目负责人已确认设计口径；实施前仍须完成本文件复核及 Accepted ADR
- 跟踪：[GitHub Issue #81](https://github.com/yuexing89757/data_center/issues/81)
- 依据：ADR-0021、ADR-0029、ADR-0030 及同日涨停领域详设

## 目标与边界

新增 `GET /api/v1/daily-limit-down-list`，提供与现有每日涨停列表基本对称的同日跌停不可变快照。首版仅覆盖 ADR-0021 已治理的沪深主板规则，不修改既有涨停快照、通用股票池或其公开接口。新领域保存可追溯的来源观察、规范成员、质量结果与计算版本；不提供交易策略或主观判断。

## 方案选择

采用独立的 `today_limit_down` 领域。直接拼接现有 `stock_pool` 无法冻结来源封板信息及完整血缘；将 `today_limit_up` 改成通用涨跌停领域会扩大既有任务和接口的回归范围。实现可以复用无方向性的校验、序列化辅助函数，但不改变既有涨停行为。

## 成员与来源语义

1. 快照自然键为 `(trade_date, version)`，成员自然键为 `(snapshot_id, symbol)`。成员资格仅来自未复权当日日 K 的 `close` 精确等于版本化跌停价，以及精确 `basis_trade_date` 的 ready 跌停池；来源榜单不能决定或改写成员资格。
2. 历史名称、前收、收盘、跌停价、自由流通股数及各自 lineage 来自现有规范事实。`change_percent = (close / previous_close - 1) * 100`；`free_float_market_cap_cny = close * free_float_shares`。价格、比例和金额使用 `Decimal`/PostgreSQL `numeric`。
3. AKShare/EastMoney 当日跌停池作为独立来源观察，保存 Raw、ingestion 与来源字段。可映射 `last_limit_down_at`、`open_count`、`consecutive_limit_down_days`、`source_reported_sealed_funds_cny`。其当前接口没有首次封板时间；保留 `first_limit_down_at` 可空字段，缺失时为 `null`，绝不从末次时间或其他事实推算。`limit_down_duration_seconds` 也为 `null`，`duration_semantics='unavailable_without_event_stream'`，不能由末次封板时间伪造时长。来源提供的价格、涨跌幅、市值或名称不覆盖规范事实。
4. 现有 21:10 收盘五档任务须从只采集涨停池改为一次采集同日 ready 涨停池与跌停池的去重并集；两池仍按同一个 `basis_trade_date` 精确选取，不回退旧池。已有涨停证券的采集结果和计算口径不变。跌停快照冻结同日收盘五档的卖一至卖五价量及盘口 ingestion。仅在卖一价量完整且卖一价等于跌停价时，计算 `closing_ask1_sealing_amount_cny = closing_ask1_price * closing_ask1_volume_shares`。来源报告封单资金与计算值分列，不合并、不补零。缺失盘口保留成员并记录质量原因。
5. 任何来源或上游修订生成新版本；同一输入哈希重复运行幂等返回既有版本。旧快照及来源 Raw 不更新。

## Worker 数据流与失败状态

Worker 内新增独立任务，交易日 22:10（Asia/Shanghai）触发；时间写在受控任务目录，开关只控制启停，不开放运行时间配置。先验证当日日 K、每日指标和精确日期 ready 跌停池，再请求来源、保存 Raw、规范化并封存快照。来源调用有超时和有限重试，重复来源证券作为质量错误处理，不合并不同尝试的部分响应。

缺必需上游时封存 `deferred`；上游部分成功、成员级必需事实缺失或规范成员缺来源观察时封存 `partial` 并逐项留痕。缺单个来源观察的规范成员仍保留，来源字段为 `null`。来源请求整体失败时封存零成员 `failed` 快照，并记录失败 ingestion/任务与质量原因；不能发布伪装为 `ready` 的快照。非交易日不抓取。任务默认停用，须在迁移、Worker 权限与来源访问验证后显式启用。首次上线不自动回填历史；当前来源仅支持近期日期。

## 公开读契约

`GET /api/v1/daily-limit-down-list` 要求 `trade_date`；`version` 可选，缺省选择指定日期最高版本；`offset` 为 0..50000，`limit` 为 1..500（默认 200），按 `symbol` 升序分页。不回退其他交易日，找不到精确快照返回现有 `not_found` 语义。

响应与涨停接口对齐：快照/计算 ID、日期、版本、状态、规则/算法版本、输入哈希、来源 ingestion、生成时间、候选/成员/拒绝计数、分页信息、分组质量摘要以及成员事实和各上游 lineage。方向字段采用 `limit_down`、`closing_ask1..5` 命名；包含 `first_limit_down_at`、`last_limit_down_at`、`open_count`、`consecutive_limit_down_days`、`limit_down_duration_seconds`、`duration_semantics`、来源报告封单资金及收盘卖一封单额。可空来源值保持 `null`，价格/比例/金额为十进制定点字符串，时间遵循 API 的上海时区 `YYYY-MM-DD HH:mm:ss` 格式。

FastAPI 仅调用有界、只读的 `api_v1` RPC，不访问内部表。RPC 保持 5 秒语句超时和最小权限，只授权 API 专用角色；内部表采用 Worker 权限和 RLS。同步 OpenAPI、PostgREST 与 Agent Tools 契约以及接口说明。

## 迁移、验证和发布边界

实施前为 Issue #81 增加 Accepted ADR 与领域详设；通过有序 `supabase/migrations/*.sql` 建表、约束、索引、权限及 RPC，不使用临时生产 DDL。单元测试覆盖来源字段、空值、重复证券、成员价格规则、卖一封单额、哈希/版本和状态；隔离 PostgreSQL 集成测试覆盖迁移、RLS、只读 RPC、精确日期和分页；契约测试覆盖 FastAPI 响应。完成相关检查后再报告结果。

本任务的本地实现不等于生产发布。生产迁移、启用任务、部署和历史回填均须用户另行明确授权。现有未提交的时间示例修正不纳入本设计提交。
