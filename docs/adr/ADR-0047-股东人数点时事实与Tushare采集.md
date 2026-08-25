# ADR-0047：股东人数点时事实与 Tushare 采集

- 状态：Accepted
- 日期：2026-08-24
- 关联 Issue：#66
- 决策者：项目所有者
- 影响：ShareholderCount、Tushare Provider、Raw 重放、Worker 调度、`api_v1`

## 2026-08-26 澄清

Tushare `stk_holdernumber` 存在公告日比统计截止日早 1 至 2 个自然日的来源事实。该短差异不定义为
数据异常，标准事实保持来源日期原值；只有公告日比统计截止日早 3 个自然日及以上时才硬失败。
领域校验与 `core.shareholder_count` 数据库约束使用相同边界。

## 背景

股东人数是上市公司在特定统计截止日披露的客观来源事实，可用于观察持有人结构变化。
它既不是股本变更或公司行为，也不是逐交易日估值快照。Tushare `stk_holdernumber`
提供标准股票代码、公告日期、统计截止日期和股东户数，但数据不定期发布、单次最多
返回 3000 行，且当前响应不能证明历史修订在原公告日即可获得。

## 决策

1. 新增 provider-neutral `ShareholderCount` 领域，保存 `symbol`、`statistics_date`、
   `announcement_date`、`shareholder_count`、确定性 `revision_key` 和 `source_code`。
   首期来源仅为 `tushare`，Raw schema 固定为 `tushare.shareholder_count.v1`。
2. Core 表为 `core.shareholder_count`。自然键是
   `(symbol, statistics_date, revision_key)`；修订键由标准 symbol、统计截止日、公告日和
   股东户数确定性计算。相同 Raw 重放幂等，修订追加而不覆盖。
3. `announcement_date` 表示来源披露时间，`first_observed_at` 表示数据中心首次观察时间。
   严格 as-of 查询同时限制两者，防止后来采集的历史修订穿越。受控历史回填不篡改
   `first_observed_at`，完整回填结果只进入明确命名的 current-known 历史查询。
4. 标准股东户数必须为正整数。Tushare 响应中字段存在但 `holder_num` 为空的行只表示来源
   没有可发布事实：完整保留在 Raw，登记 `shareholder_count.missing_source_value` 质量拒绝，
   不生成 Core 记录，并继续处理同一请求中的合法行。字段缺失、零、负数、非整数字符串、
   公告日比统计截止日早 3 个自然日及以上、未知证券、批内重复和修订键不匹配仍是硬错误；
   早 1 至 2 个自然日的来源差异允许入库。统计截止日不要求是季度末或
   交易日。
5. 每个实际 Tushare 请求产生一个 IngestionRun、一个不可变 Raw 对象和一个 manifest。
   同一次每日增量的所有请求完成后，在一个数据库事务中登记 manifests、发布全部事实并
   终结运行；任一切片失败时不发布部分事实。
6. 恰好 3000 行的响应视为可能截断。多日区间递归拆分；单日全市场仍满额时逐证券查询；
   单证券单日仍满额时硬失败。合法空响应是成功，不伪造事实。
7. 历史回填是显式 CLI Workflow，不注册 Scheduler Job。范围包含沪、深、北全部已知股票，
   包括已退市证券；从 `ipo_date` 开始，缺失时使用 `1990-12-19`。生产回填需另行授权。
8. 每日增量在 Worker 内每天 21:00 Asia/Shanghai 运行，重扫当前日及此前 29 个自然日。
   `SHAREHOLDER_COUNT_DAILY_ENABLED` 默认关闭，部署探测通过后显式启用。
9. Tushare 股东人数接口独立限速为每分钟 1–200 次，默认 180 次。Token 仅从
   `TUSHARE_TOKEN` 读取，不进入 Raw、请求参数 manifest、日志、异常或数据库。
10. `api_v1` 提供三个有界 RPC：
    `query_shareholder_counts_as_of`、`query_shareholder_count_history_as_of` 和
    `query_shareholder_count_history_latest`。前两个执行严格双时间门禁；最后一个明确是
    current-known 历史。查询时确定性计算上一期人数、增减户数和增减比例。
11. RPC 最多接收 500 个证券、最多返回 2000 行，使用固定 5 秒 statement timeout，
    不暴露 ingestion、Provider、修订键或首次观察字段。同步 PostgREST 与 Agent 契约；
    首版不增加 FastAPI 或 MCP。
12. 所有定时触发只进入 `market-data-center worker` 的 APScheduler 任务目录。不得增加
    Windows Task Scheduler、cron 或其他操作系统级采集触发器。

## 后果

- 数据中心可以分别支持严格历史时点复现和当前已知历史趋势，不会把后来回填伪装成过去已知。
- 每请求一份 Raw 保持现有单 Raw 重放血缘；每日多请求通过一次事务发布保证消费者不见半批事实。
- 来源空值不会被伪造成零或导致合法事实全部丢失；对应 IngestionRun 和 Workflow 以拒绝计数
  明确呈现 `partial`，全为空时该请求为 `failed`，但受控回填仍可继续下一只证券。
- 30 日重扫不能证明发现更早公告日期的后来修订，因此全历史重新核验仍是显式受控操作。
- 首期不抽象十大股东、股东增减持、户均持股或筹码标签，也不引入第二来源。

## 验收

- Domain、Tushare adapter、Raw replay、Pipeline、批量持久化、CLI、Scheduler 和 Operations
  均有聚焦测试。
- PostgreSQL 集成测试覆盖约束、RLS、最小授权、双时间防穿越、current-known 修订选择、
  相邻期变化、参数边界和 Core 不可公开访问。
- `contracts/postgrest-openapi-v1.json` 与 `contracts/agent-tools-v1.json` 同步，
  `contracts/fastapi-openapi-v1.json` 保持不变。
- Ruff format/check、mypy、pytest 和隔离 PostgreSQL integration gate 通过；无法运行的检查
  必须报告准确命令和原因。
