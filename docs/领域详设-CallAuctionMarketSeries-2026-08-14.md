# 领域详设：CallAuctionMarketSeries

> 状态：有效，已实现
> 日期：2026-08-14
> GitHub Issue：#48、#54、#72
> 上级决策：`adr/ADR-0034-沪深全市场开盘竞价序列快照.md`、`adr/ADR-0051-竞价序列采集与单Writer解耦.md`（Accepted）

## 1. 领域职责

CallAuctionMarketSeries 保存 Worker 在开盘集合竞价窗口内，从 PYTDX 观察到的沪深上市股票全集序列来源事实。它负责冻结输入全集、生成确定性时槽、验证单轮完整性、保存 Raw 和 ingestion lineage、聚合会话质量状态。

它不负责解释撮合机制、构造逐笔/Level-2 数据、生成策略标签、回测、盘后补采或修改现有09:26快照语义。

## 2. 术语与身份

- Session：一个交易日的一次09:15长会话尝试。
- Frozen Universe：Session开始时冻结的SSE/SZSE listed-stock标准symbol有序集合。
- Round：一个确定性计划采样点，`sample_seq=0..31`。
- Attempt：Round对一个PYTDX endpoint的完整全集请求，对应一个IngestionRun。
- Selected Ingestion：Round规范读取所选择的成功attempt；没有成功时可以指向最终partial attempt。
- Snapshot：一个Attempt中某个symbol的标准来源事实。

Session ID、Round自然键和Ingestion ID互不替代。Domain Record不携带 ingestion ID；Persistence在写入边界附加lineage。

## 3. 时间模型

统一交易日市场为 `CN_A_SHARE`，全部领域时间为 aware UTC；展示和调度使用 `Asia/Shanghai`。

```text
window_start = 09:15:00 Asia/Shanghai
cadence = 20 seconds
sample_seq = 0..31
scheduled_at(seq) = window_start + seq * cadence
last scheduled_at = 09:25:20
round deadline(seq<31) = scheduled_at(seq+1)
round deadline(seq=31) = 09:25:40
```

`scheduled_at` 表示计划点；每条 `observed_at` 表示Worker收到并标准化该证券响应的时间；Round `collected_at` 表示该轮来源采集完成或截止的时间，与异步数据库事务的提交时间无关。三者不得互相伪造。

每轮另保存由 `scheduled_at` 上海时间格式化得到的六位 `batch_code=HHMMSS`，例如
09:15:00 为 `091500`、09:15:20 为 `091520`。它是展示和检索字段，不替代 Round 自然键。

## 4. 领域对象

### MarketSeriesSession

包含session ID、交易日、窗口、cadence、冻结全集、全集哈希、期望轮数、状态、时间和汇总计数。构造时验证：

- trade date是统一日历交易日；
- cadence固定20、轮数固定32；
- symbols有序、唯一、非空且只含SSE/SZSE股票；
- count与symbols长度相等，hash与规范编码相等；
- finished状态和计数一致。

### MarketSeriesRound

包含session ID、sample sequence、scheduled/collected time、状态、attempt count、报价计数、selected ingestion和错误摘要。构造时验证slot公式、deadline、计数非负及终态时间。

### MarketSeriesSnapshotRecord

包含symbol、trade date、batch code、scheduled/observed time、价格、累计量额、买卖各五档和
source code。价格/金额使用Decimal，数量为股，missing保持None，zero不等同missing。OHLC和
非负约束与现有09:26来源事实一致。09:25前的 `auction_indicative` 语义以买一价作为
`last_price`、买一量作为累计量，并以二者乘积作为累计额；provider 的 `high_price`、
`low_price` 仍按来源事实保存，但它们是已成交区间，不约束尚未成交的竞价指示价。
`opening_trade` 和 legacy 语义仍要求 `last_price` 位于来源最高价、最低价区间内。

档位价格为零且数量为正时保存为 `price=NULL`、`volume=实际股数`；价格和数量均为零时
两者均为 `NULL`。

## 5. 服务流程

1. 校验交易日、窗口和quote节点池，并在进入采样循环前同步持久化Session、冻结全集及哈希。
2. 根据当前时间确定首个未错过seq；过去seq形成内存中的failed Round，不调用Provider。
3. Producer严格等待当前slot，不提前请求；按80只批次和deadline读取完整全集。
4. Provider响应返回后立即保存不可变Raw v2，再复制五档事实并执行全集、时间、symbol、数值、
   档位和基数校验；单条事实构造失败转为该symbol质量拒绝，不中断其余证券处理。
5. Producer把该轮全部endpoint attempts和Round终态组成不可变 `CapturedRound`，按FIFO放入容量32
   的进程内队列；数据库写入耗时不计入attempt重试预算，也不阻塞下一个20秒计划点。
6. 单个非daemon Writer按sample sequence消费队列，每个CapturedRound使用一个数据库事务持久化
   Round、全部IngestionRun、Manifest、质量结果和Snapshot事实。
7. 成功则选择该ingestion；partial且剩余时间至少覆盖配置的最小重试预算与上一完整attempt的
   Provider加Raw实际耗时二者较大值时，才从第二endpoint全量重试。
8. 32个计划点生产完成后，Producer等待Writer排空，再根据已经持久化的Round聚合Session终态。

进程恢复只读取持久化Session冻结全集并继续未来slot。已错过slot不调用Provider。

## 6. Persistence

所有生产schema变更来自 `supabase/migrations/*.sql`。Persistence负责事务、锁和lineage，不允许 `create_all()`、Alembic或运行时DDL。

Snapshot父表按trade date月度Range Partition；初始migration创建当前及未来分区。分区主键包含partition key。父表只授予Worker select/insert；Session/Round允许Worker完成受控状态转换所需的select/insert/update。RLS策略只允许Worker角色。

Snapshot 保存 `batch_code` 及 `bid/ask_price_1..5`、`bid/ask_volume_1..5`。迁移仅从历史
`scheduled_at` 回填 `batch_code`；历史五档字段保持 `NULL`，不得从其他表或后续行情反推。

同一CapturedRound（含该轮全部attempt）写入必须使用一个事务：任何Snapshot、Manifest、质量结果、
IngestionRun或Round写入失败时，整轮回滚，不能留下部分lineage或伪成功事实。Round selected ingestion
只能引用同一事务内已终态的同dataset attempt。Writer严格单线程、FIFO写入，不允许多个数据库写线程
并发争用；Session最终化必须等待队列排空。

## 7. Raw与重放

每个Attempt写一个独立JSONL对象和Manifest。Raw在领域事实构造、校验和数据库排队前落盘，因此
数据库阻塞不影响下一个计划点，也不会令来源响应丢失。Raw v2 envelope记录标准symbol身份、provider
原始字符串字段、worker observed time、ingestion ID、交易日、session ID、sample sequence、scheduled
time、endpoint和attempt number。Raw对象不可覆盖。

首版replay fail closed。未来若启用，必须从持久化Frozen Universe证明预期集合，并确保一个Raw对象只重建原Attempt，不能跨endpoint或跨Round拼接。

## 8. 失败语义

- 非交易日：Session跳过，不创建来源事实。
- 全集为空/非法：Session failed，不请求Provider。
- 节点池为空：Session failed。
- endpoint连接/读取失败：Attempt partial，质量结果记录provider error。
- response缺失/重复/未知/越界：Attempt partial，保留合法事实。
- first attempt partial：仅在deadline预算允许时执行第二全量attempt。
- 两次均partial：Round partial，selected ingestion指向最后attempt。
- slot错过：Round failed，无IngestionRun。
- Raw写入失败：该Round failed且不入队attempt；后续计划点继续采集，不在内存中伪造Raw lineage。
- Writer持久化某轮失败：该轮事务回滚并记录失败sample sequence与异常类型；Writer继续消费后续轮次，
  Producer仍按20秒节奏采集。Session最终化把数据库中缺失的Round计为failed并保存摘要。
- 任一Round非succeeded：Session不得succeeded。

当前实现不自动重放Writer失败轮次；Raw replay仍按第7节fail closed。进程在队列排空前不正常退出，
但进程崩溃时尚未提交的内存队列不具备跨进程耐久性。

## 9. 调度边界

Worker注册一个代码目录固定的工作日09:15 job。`CALL_AUCTION_MARKET_SERIES_ENABLED`仅控制启停。
专用`morning_auction` executor承载该早盘长会话；default executor保持1线程。已退役的涨停池
五档任务不得占用该 executor 或重新注册。

JobStore仅保存调度定义，WorkflowRun/JobExecution、Session/Round和IngestionRun分别保存操作、会话和来源事实，不复制或反序列化APScheduler job state。

## 10. 容量与保留

以5,208只估算：每轮5,208行、每天166,656行、每年约4,166万行。PostgreSQL只保留最近12个完整月份；Raw、Manifest、IngestionRun和质量结果长期保留。

分区创建和删除是受保护migration操作，不属于Worker任务。删除旧分区前核对Raw/Manifest和备份；不得为释放空间直接删除未验证来源事实。

## 11. 读取边界

消费者不得直接依赖realtime内部表。只读
`api_v1.query_call_auction_market_series_snapshots(p_trade_date,p_codes)` 接受精确交易日和
1～500个六位代码，优先选择最新succeeded Session；没有成功Session时选择最新partial
Session，不拼Session、不回退日期。响应按sample sequence正序返回每轮，并逐轮公开 batch code、
五档事实、缺失代码、规范事实和provider-neutral selected ingestion ID。历史行五档字段为 NULL。

FastAPI通过`POST /api/v1/call-auction-market-series-snapshots/query`代理该RPC，只使用
`market_data_api`最小执行权限，不直接读取realtime表。PostgREST、Agent和FastAPI contracts
必须同步签入；Raw、source code、节点及内部创建时间不进入公共契约。

## 12. 测试与上线

单元测试覆盖领域不变量、时槽、Provider批次、deadline、重试、恢复和Session汇总。PostgreSQL integration tests使用隔离`TEST_DATABASE_URL`覆盖schema、分区、RLS、权限、事务和lineage。

生产只通过打包发布和受保护migration上线，不手工触发采集。下一交易日live gate验证全市场
序列任务的32轮实际时间、batch code、五档保存、每轮完整率、Raw/Manifest、数据库行数、存储
增长和09:26任务成功。

## 13. 指定批次读取

`api_v1.query_call_auction_market_series_snapshots(p_trade_date,p_codes,p_batch_code)` 的
`p_batch_code` 为可选六位 `HHMMSS`。RPC 仍先按既有规则选择指定交易日的单一 Session；传入
批次时，仅在该 Session 内按 Round 的上海计划时间精确筛选，不跨 Session、不回退日期。合法但
不存在的批次返回 `returned_rounds=0` 和空 `rounds`；不传批次时保持返回全部轮次的既有行为。
FastAPI 请求字段名为 `batch_code`，PostgREST 参数名为 `p_batch_code`。
