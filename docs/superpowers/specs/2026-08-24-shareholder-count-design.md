# 股东人数点时事实与 Tushare 采集设计

- 状态：设计已确认，待建立 GitHub Issue 与 Accepted ADR
- 日期：2026-08-24
- 计划 ADR：ADR-0047
- 来源接口：[Tushare `stk_holdernumber`](https://tushare.pro/document/2?doc_id=166)

## 目标与非目标

新增最小 `ShareholderCount` 领域，保存上市公司在特定统计截止日披露的股东户数，并提供严格
防未来数据穿越的时点查询和当前已知历史查询。首版使用 Tushare `stk_holdernumber` 作为唯一
Provider，支持受控全历史回填和 Worker 每日增量采集。查询契约在选定可见修订后确定性计算
较上一统计期的增减户数和增减比例。

本领域不扩展为宽泛的股权或股东领域，不保存十大股东、股东增减持、户均持股、筹码集中度
标签或其他主观判断。股东户数不是股本变更或公司行为，不进入 Capital；它也不是逐交易日
估值快照，不进入 StockDailyIndicator。首版不增加 FastAPI 路由、MCP、第二 Provider 或操作
系统级计划任务。

按照项目宪法，实施前必须先建立 GitHub Issue、接受 ADR-0047，并新增领域详设、SQL migration
和测试。本文不授权生产回填、迁移或其他外部写操作。

## 领域边界与依赖

```text
Ingestion ───────────────► ShareholderCount
Security ────────────────► ShareholderCount
ShareholderCount + as_of ─► 查询时客观变化指标
```

`ShareholderCount` 通过标准 `symbol` 依赖 Security，通过 Ingestion 获得 Raw 和采集血缘。它不
依赖 Trading Calendar：统计截止日和公告日都可能不是交易日，统计截止日也不限定为季度末。
Provider 专用字段、代码格式和请求限制停在 Tushare adapter 边界。

领域记录定义为：

```python
ShareholderCountRecord(
    symbol: str,
    statistics_date: date,
    announcement_date: date,
    shareholder_count: int,
    revision_key: str,
    source_code: str,
)
```

领域记录不包含 `ingestion_id`。Pipeline 用 `IngestionEnvelope` 附加采集血缘；Persistence 在首次
插入时生成 `first_observed_at`。

## 不变量与修订语义

- `symbol` 必须是 `core.security` 中已知的沪、深或北 A 股标准代码。
- `statistics_date <= announcement_date`。
- `shareholder_count` 标准记录必须为正整数。Tushare `holder_num` 字段存在但值为空时保留 Raw、
  登记质量拒绝且不生成标准记录；字段缺失以及非空的零、负数和非整数字符串仍为硬错误。
- 首期 `source_code` 只能为 `tushare`。
- 同一标准事实的确定性修订键为
  `SHA-256(symbol, statistics_date, announcement_date, shareholder_count)`；字段使用无歧义的
  长度前缀或固定分隔编码，不使用 Python 对象字符串表示。
- 修订键必须能由记录字段重新计算并完全匹配。
- 同一批内 `(symbol, statistics_date, revision_key)` 必须唯一。
- 相同 Raw 重放幂等；同一统计截止日的公告日期或股东户数变化形成新修订，不覆盖旧事实。

Core 只保存来源事实。`previous_shareholder_count`、`change_count` 和 `change_ratio` 都在稳定查询
选择唯一可见修订后计算，不进入基础表。

## 双时间语义

`announcement_date` 表示来源声称事实向市场披露的日期；`first_observed_at` 表示数据中心第一次
实际观察到该修订的时间。严格时点查询的可见条件为：

```text
announcement_date <= p_as_of_date
and first_observed_at < (p_as_of_date + 1 day) 00:00 Asia/Shanghai
```

对同一 `(symbol, statistics_date)` 的多个可见修订，按 `announcement_date desc`、
`first_observed_at desc`、`revision_key desc` 选择唯一版本。随后按 `statistics_date` 排序，再用
相邻统计期计算变化；跨证券的最新时点查询最后为每只证券选择最大的可见 `statistics_date`。

历史回填只能证明数据中心在回填时观察到旧公告，不能证明 Tushare 当前返回的版本就是公告日
当天的原始版本。因此不得把回填记录的 `first_observed_at` 回写为公告日。严格时点查询不会把
本次回填伪装成过去已知事实；当前已知历史查询可以展示完整回填，但其名称和文档必须明确说明
它不能作为严格无未来数据回测输入。系统持续运行后形成的观察历史才同时具备两个可靠时间轴。

变化字段按以下规则计算：

```text
change_count = shareholder_count - previous_shareholder_count
change_ratio = change_count / previous_shareholder_count
```

人数下降返回负值。首条可见记录的所有 previous/change 字段为 `NULL`。`change_ratio` 使用
`numeric`/`Decimal`，以小数比率表达，例如 `-0.125` 表示减少 12.5%，不经过 `float`。

## Tushare Provider 与 Raw

Tushare `stk_holdernumber` 的来源字段为 `ts_code`、`ann_date`、`end_date` 和 `holder_num`。Adapter
分别映射为 `symbol`、`announcement_date`、`statistics_date` 和 `shareholder_count`，并固定 Raw
schema 为：

```text
tushare.shareholder_count.v1
```

当 `holder_num` 为 NULL、空字符串或纯空白时，Raw 保留该行及空值证据，Adapter 省略对应标准记录；
Pipeline 用 `shareholder_count.missing_source_value` 记录聚合拒绝数。合法事实和质量结果在同一
事务发布，Operations 将有接受也有拒绝的执行标记为 `partial`。

每个来源请求保存不可变 Raw 对象、请求参数、SHA-256 和 manifest。Raw 保留来源字段名；从 Raw
重放时由版本化 normalizer 生成相同标准记录。`TUSHARE_TOKEN` 只从环境变量读取，不进入请求
参数 manifest、Raw、日志、异常或数据库。

权限不足、限流、来源错误、字段缺失、日期或整数非法以及无法证明完整性都转换为可诊断的
`ProviderError`。来源成功但数据为空是合法响应，因为该接口不定期更新。空响应与来源返回的错误
码必须严格区分。首期只有一个实际 Provider，不自动回退或合并其他来源。

接口当前单次最多返回 3000 行。任何恰好返回 3000 行的响应都按“可能截断”处理：

```text
返回行数 < 3000  -> 接受
返回行数 = 3000  -> 递归缩小公告日期区间
单日仍为 3000    -> 对该日改为逐证券查询
逐证券单日仍 3000 -> 硬失败并记录质量错误
```

不得把可能截断的响应当作成功，也不得只保存前 3000 行标准事实。接口限速使用独立配置
`TUSHARE_SHAREHOLDER_COUNT_MAX_CALLS_PER_MINUTE`，允许范围为 1 至 200，默认 180；该默认值低于
官方当前基础上限 200 次。实施前必须再次核对账户权限中心和官方接口页，权限或频率变化通过配置
收紧，不静默放宽。

## 受控全历史回填

历史回填仅由显式 CLI 启动，不注册 APScheduler Job，不随 Worker 启动。CLI 在执行外部请求前
输出证券数量、公告日期范围和预计请求数，并要求显式确认；非交互自动确认必须使用明确参数。
生产回填仍属于单独的受控外部操作，设计或部署本身不自动执行。

回填从 `core.security` 按标准 symbol 升序枚举沪、深、北全部 A 股，包括已退市证券。命令要求
显式 `--cutoff-date` 且不得晚于当前上海自然日；每只证券的完整历史区间从其标准 `ipo_date` 开始，
`ipo_date` 缺失时保守使用 `1990-12-19`，到该 cutoff date 结束。请求以“证券 + 公告日期区间”切片，
使用截断防护递归缩小区间。每只证券形成
独立 IngestionRun；一个受控
`shareholder_count_backfill` Workflow 汇总全局状态。单证券只有在其全部请求、Raw 保存、标准化、
校验和 Core 事务写入成功后才标记成功。

单证券失败会使 Workflow 失败，但此前已成功证券的不可变事实保留。重新执行依赖自然键幂等，
不删除或覆盖旧事实。CLI 支持明确的 symbol 子集和 `--resume-after-symbol`；后者按标准 symbol
字典序排除该 symbol 及此前证券。不增加可变的数据库断点表。恢复参数只决定从哪些证券重新发起
请求，不改变事实语义。

## 每日增量采集

Worker 任务目录新增：

```text
Workflow code: shareholder_count_daily
Step code:     ingest_shareholder_count_updates
Job ID:        shareholder-count-daily
Schedule:      每天 21:00 Asia/Shanghai
```

任务每天运行，包括周末，并重扫执行日及此前 29 个自然日的公告日期，以吸收延迟提供的数据。
正常路径按公告日期区间获取全市场记录；截断时缩小日期区间，必要时切换逐证券查询。该 Workflow
的一次执行只使用 Tushare。

一次每日执行是一个原子采集单元。每个实际来源请求各有一个 IngestionRun 和 Raw manifest，以保持
单 Raw 重放边界；全部请求完成后再做整批自然键校验，并在单一数据库事务中插入全部 manifest、
发布全部 Core 事实和终结这些 IngestionRun。任一切片失败时，已准备的请求运行统一失败且不发布
任何标准事实；已保存 Raw 仍登记 manifest 供诊断和重放。原子性只覆盖当次增量执行，不跨越历史
回填的不同证券。

滚动 30 日窗口不能证明发现了更早公告日期的后来修订，因此另提供显式“全历史重新核验”CLI。
它复用受控回填流程，不自动定时执行，也不因每日任务成功而宣称全历史无遗漏。

新增唯一调度开关 `SHAREHOLDER_COUNT_DAILY_ENABLED`，代码默认值为 `false`。受保护部署完成
migration、Tushare 权限探测和一次增量试运行后，由运维显式设为 `true`。运行时间固定在代码目录，
不提供 hour/minute 环境配置。所有触发都在 `market-data-center worker` 的 APScheduler 内，禁止
Windows Task Scheduler、cron 或其他系统级采集触发器。

Operations 受控目录同时登记 `shareholder_count_daily` 和无定时 Job 的
`shareholder_count_backfill` Workflow。WorkflowRun、JobExecution 和 IngestionRun 记录执行、
失败及血缘；本地只读任务页只展示既有只读状态，不增加任务修改能力或秘密信息。

## 存储模型与权限

新增 `core.shareholder_count`：

```sql
create table core.shareholder_count (
    symbol text not null references core.security (symbol),
    statistics_date date not null,
    announcement_date date not null,
    shareholder_count bigint not null,
    revision_key text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    first_observed_at timestamptz not null default now(),
    primary key (symbol, statistics_date, revision_key)
);
```

Migration 增加以下数据库约束：

- `statistics_date <= announcement_date`；
- `shareholder_count > 0`；
- `revision_key` 是 64 位小写十六进制 SHA-256；
- `source_code = 'tushare'`；
- ingestion/audit/operations 受控 code constraint 包含新 dataset、workflow、step 和 job。

索引为：

```text
(symbol, statistics_date, announcement_date desc, first_observed_at desc)
(announcement_date)
(ingestion_id)
```

表启用 RLS。`market_data_worker` 只有 `SELECT` 和 `INSERT`，没有 `UPDATE`、`DELETE` 或 TRUNCATE；
持久化使用 `ON CONFLICT DO NOTHING`。PostgREST、anon、authenticated、FastAPI reader 和其他消费
角色都不能直接访问 Core。

## `api_v1` 查询契约

为避免可空参数混淆严格时点和当前已知语义，新增三个独立、有界的 PostgREST RPC。

### `query_shareholder_counts_as_of`

输入 `p_as_of_date date`、可选 `p_symbols text[]` 和 `p_limit integer default 500`。应用完整双时间
可见条件，为每只证券返回该时点可见的最新统计记录和上一统计期变化。`p_symbols` 去重后最多
500 个；超过 500 个时拒绝请求。空数组返回空结果，`NULL` 表示全市场。`p_limit` 被收敛到
1 至 2000，最终结果按 `symbol asc` 确定性排序后应用 limit。

### `query_shareholder_count_history_as_of`

输入单个 `p_symbol text`、`p_as_of_date date`、`p_start_statistics_date date`、
`p_end_statistics_date date` 和 `p_limit integer default 500`。应用完整双时间可见条件，返回指定
统计日期范围内当时可见的序列。起始日期不得晚于结束日期；`p_limit` 被收敛到 1 至 2000，结果
按 `statistics_date asc, announcement_date asc` 确定性排序后应用 limit。

### `query_shareholder_count_history_latest`

输入单个 `p_symbol text`、`p_start_statistics_date date`、`p_end_statistics_date date` 和
`p_limit integer default 500`。对每个统计截止日选择数据中心当前已知的最新修订，包含历史回填
结果。函数名、注释和契约描述必须明确：该结果不能替代严格无未来数据的 as-of 查询。日期校验、
limit 收敛和输出排序与 `query_shareholder_count_history_as_of` 相同。

三个 RPC 共同返回：

```text
symbol
statistics_date
announcement_date
shareholder_count
previous_statistics_date
previous_shareholder_count
change_count
change_ratio
```

上一期是同一证券在同一查询知识边界内，按 `statistics_date` 排序后的前一条可见记录；不是简单
季度偏移，也不跨越到查询范围外取隐藏前值。换言之，`p_start_statistics_date` 之前的记录不参与
history RPC 的 `previous_*` 和 change 计算，使响应只依赖显式请求范围。调用方若需要范围首条记录
的前值，必须扩大起始日期。

RPC 使用 `language plpgsql stable security definer`，只以 `RAISE EXCEPTION` 实现证券数量和日期范围
参数门禁，返回数据仍由一条只读 `RETURN QUERY` SQL 产生。函数固定 `search_path` 和 5 秒 statement
timeout，撤销 public 默认执行权，只授予现有 PostgREST 只读角色。公开响应不包含 `ingestion_id`、
`source_code`、`revision_key` 或 `first_observed_at`。

Migration 同步 `contracts/postgrest-openapi-v1.json` 和 `contracts/agent-tools-v1.json`。首版不增加
FastAPI 路由，因此不修改 `contracts/fastapi-openapi-v1.json`；消费者仍只能通过 `api_v1`，不能
直接查询 Core。

## 错误处理与质量结果

- Provider 请求失败、权限不足或限流：IngestionRun/WorkflowRun 失败，保留诊断信息但不包含 Token。
- Raw 成功保存、标准化失败：保留 Raw 和失败质量结果，不写 Core。
- 未知证券、日期倒置、非正人数、非空非整数、字段缺失、修订键不匹配或批内重复：硬质量失败。
- 字段存在但 `holder_num` 为空：Raw 保留、质量拒绝、Core 跳过，其余合法行继续。
- 恰好 3000 行且无法继续证明完整：硬质量失败，不降级为警告。
- 每日窗口合法空响应：成功并记录零事实，不伪造股东人数记录。
- Core 写入异常：回滚当次数据库事务，IngestionRun 标记失败；Raw 保持不可变。
- 同一事实重放：冲突忽略且原 `first_observed_at` 不变。

查询不回退到较旧 ready 快照、其他 Provider 或猜测日期。某证券没有符合知识边界的事实时不返回
该证券；缺失与零严格区分。

## 测试与验收

领域单元测试覆盖标准记录、不规则统计日期、修订键确定性、正整数、日期顺序、未知证券、批内
重复以及零/缺失/负数拒绝。

Tushare adapter 使用 mocked client 覆盖：

- SH/SZ/BJ 与标准 symbol 的双向映射，包括已退市证券；
- 正常、合法空响应、来源错误、权限不足和限流；
- 缺失字段、非法日期、非整数人数和重复来源行；
- 小于/等于 3000 行、日期递归切分、单日逐证券降级和最终硬失败；
- 请求参数不含 Token，来源字段不泄漏到标准记录；
- `tushare.shareholder_count.v1` Raw 重放得到相同修订键和记录。

Pipeline/Persistence 测试覆盖每日整批事务原子性、历史单证券事务、同一 Raw 幂等、同一统计日多修订
留存、失败 Raw 保留和恢复参数不改变结果。Scheduler/Operations 测试覆盖每天 21:00 注册、周末不
排除、默认关闭开关、函数映射、陈旧运行恢复、历史回填无 Scheduler Job，以及目录 code 与数据库
constraint 同步。

PostgreSQL 集成测试覆盖 migration、外键和 check constraint、RLS 与 grants、不可更新、首次观察时间
冲突不变、严格 as-of 排除后来观察修订、current-known history 包含回填、同一统计日版本选择、相邻
期变化符号和 Decimal、范围首条无前值、空数组语义、日期/证券/行数边界、5 秒超时和 Core 不可公开
访问。契约测试保证 PostgREST 和 Agent schema 与 RPC 完全同步。

实施完成后运行：

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
uv run pytest -m integration  # 仅隔离 TEST_DATABASE_URL
```

任何无法执行的检查都必须报告准确命令和原因，不能从部分检查推断整体通过。

## 文档、上线与回退

设计批准后的实施文档包括 Accepted `ADR-0047-股东人数点时事实与Tushare采集.md`、
`领域详设-ShareholderCount-2026-08-24.md`，并更新领域模型总纲、数据库导航、Worker 日常采集与
调度说明、Tushare 权限快照及 ADR 索引。不得把未部署能力写成当前已实现事实。

上线顺序为：通过保护流程应用 migration；部署默认关闭的新代码；用本地账户做有界权限探测；
显式执行一次增量试运行并核对 Raw、Core、Operations 和三个 RPC；按单独授权执行历史回填；最后
设置 `SHAREHOLDER_COUNT_DAILY_ENABLED=true` 并重启/重载 Worker 目录。历史回填和生产 migration
都必须由用户另行明确授权。

回退只关闭调度开关并回退兼容代码，不删除新表、Raw、Operations 或已写事实。公共 RPC 若已经有
消费者，按版本化契约和弃用规则处理，不能通过破坏性 migration 静默删除。
