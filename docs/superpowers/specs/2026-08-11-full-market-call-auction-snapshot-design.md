# 沪深全市场开盘竞价快照设计

日期：2026-08-11
状态：设计已获项目所有者批准
关联 Issue：#41
治理决策：ADR-0027、ADR-0028（Accepted）

## 1. 目标

Worker 在 09:26 采集沪深全部 listed stock 的完整开盘竞价来源快照，避免再把盘后累计成交量/额
解释为开盘竞价事实。项目所有者已移除 21:30 自动最终化；来源事实、Raw 和内部数据库最终化能力
保留，但没有替代自动计划。

## 2. 非目标

- 不支持 BSE、ETF、可转债、指数或板块；
- 不采分钟、tick、逐笔、Level-2 或盘后历史成交；
- 不公开全市场晨间来源表；
- 不增加 OS 级计划任务、并行行情洪泛或 `.env` 时间配置；
- 不自动执行“今日竞价量”最终化；
- 不回填本功能上线前的历史日期。

## 3. 架构

### 3.1 09:26 来源采集

Worker 从 `core.security` 冻结 SSE/SZSE、stock、listed 的有序唯一全集。一个 attempt 选择节点池
中一个 quote endpoint，以每批最多 80 只顺序调用 `pytdx_hq`。所有标准记录、Raw JSONL、
RawManifest、QualityResult 和 IngestionRun 使用 dataset `call_auction_market_snapshot`。

成功的必要条件是预期证券都有明确响应、无重复/未知证券、所有观察时点均在上海时间
`[09:25,09:30)`。09:29:30 后不再发起新请求。停牌或来源明确返回但价格/量为空的证券保留为
缺失事实，不算漏采。

任一批失败时保存当前 attempt 为 partial。若仍在窗口内，第二 attempt 使用下一个 endpoint，
从完整全集起点重新采集；不得合并两个 endpoint 的成功子集。最多两个 attempt。

### 3.2 非调度的内部最终化能力

内部数据库最终化不创建网络 Provider。它选择精确交易日、dataset 正确、状态 succeeded、finished_at 最新的
晨间 ingestion，并读取该 ingestion 的完整事实；然后选择精确交易日最新 ready 涨停池。集合交集
必须覆盖全部池成员，否则事务失败。最终记录复用晨间 `ingestion_id` 和 `observed_at`，计算
`(last_price-previous_close)/previous_close*100`，原子写入现有最终表。

没有晨间成功批次、没有 ready 池、成员缺失或日期不一致时不回退。ready 空池是合法零行成功。
该能力不在 Worker job catalog 或 scheduler function map 中，不自动运行，也没有替代时间。

## 4. 数据模型

新表 `realtime.call_auction_market_snapshot`：

| 字段 | 类型 | 约束 |
|---|---|---|
| `ingestion_id` | uuid | IngestionRun 外键；主键组成 |
| `symbol` | text | Security 外键；主键组成 |
| `trade_date` | date | 非空；统一交易日 |
| `observed_at` | timestamptz | 非空；上海时间 `[09:25,09:30)` |
| `last_price` | numeric(18,4) | 可空、非负 |
| `previous_close` | numeric(18,4) | 可空、非负 |
| `high_price` | numeric(18,4) | 可空、非负；不低于非空最低价 |
| `low_price` | numeric(18,4) | 可空、非负；不高于非空最高价 |
| `cumulative_volume` | bigint | 可空、非负，股 |
| `cumulative_amount` | numeric(30,4) | 可空、非负，元 |
| `source_code` | text | 固定 `pytdx_hq` |
| `created_at` | timestamptz | 默认 now() |

主键 `(ingestion_id,symbol)`；索引 `(trade_date,ingestion_id,symbol)`。表启用 RLS，只向
`market_data_worker` 授予必要的 select/insert，不授予 update/delete 或 API 角色。

`high_price`、`low_price` 是来源在 09:26 观察时点给出的截至当日最高/最低价，不是盘口档位价。
二者允许缺失；均非空时必须 `high_price >= low_price`，最新价与二者均非空时必须位于该区间。

现有 `realtime.call_auction_snapshot` 增加可空 `observed_at` 以兼容旧行；新代码始终写非空值。
现有 `ingestion_id` 改为引用晨间来源 ingestion，不创建伪造的盘后 ingestion。

迁移同步扩展 ingestion dataset check、audit quality dataset check、operations workflow code check。
新 workflow 为 `call_auction_market_snapshot`；最终化继续使用 `call_auction_snapshot`。

## 5. 代码边界

- `providers/pytdx_hq.py`：显式选择一个 endpoint，保持单 attempt 单节点；批量协议和 Decimal
  解码不变。
- 新的晨间采集服务：冻结 universe、执行最多两个全量 attempt、校验时间/完整性、写 Raw 和事实。
- `snapshot_collector.py`：保留纯数据库内部最终化，不实例化 Provider。
- Persistence：append-only 写晨间事实；精确选择成功 ingestion；事务写最终池结果。
- `scheduling_catalog.py`：只注册 09:26 job；保留历史 workflow code 以兼容既有 operations 事实。
- `scheduler.py`：注册晨间采集，并从持久 JobStore 清理已退役的 21:30 job ID。
- `reliability.py`：在 Raw 读取或 persistence 写入前 fail closed 拒绝本数据集 replay。

## 6. 错误和恢复

- 非交易日：任务不创建市场事实；
- 09:26 job 在窗口外触发：显式失败，不补采；
- endpoint 连接或批次失败：当前 attempt partial；窗口允许时换下一 endpoint 全量重试一次；
- 两次不完整：当日无 succeeded 晨间快照；
- Worker 崩溃：stale recovery 标记运行失败，重启后只有仍在窗口内的正常调度/人工明确操作可新建
  attempt，不能补过去时点；
- Raw replay：来源 Raw 继续保留，但缺少原冻结全集确定性身份时稳定拒绝且不创建 replay ingestion；
- 重复执行：晨间产生新 append-only ingestion；内部最终化若被显式调用，对相同最终自然键保持幂等。

## 7. 配置与运维

现有 `CALL_AUCTION_SNAPSHOT_ENABLED` 只控制 09:26 job，默认 true。环境模板不新增 hour/minute、
批量大小、重试节点数或窗口变量；这些属于受控代码和 Provider 固定边界。管理页只显示：

- `call-auction-market-snapshot-daily`：工作日 09:26；

`call-auction-snapshot-daily` 已退役；没有 cron、timer、Windows Task Scheduler 或其他自动替代。

生产部署需要 migration、备份、受保护 schema workflow、单 Worker 重启和只读 smoke。不得为验收手工
补造历史竞价快照；等待下一交易日检查实际 attempt 数、批次数、耗时、缺失数、Raw 和来源事实行数。

## 8. 测试策略

测试按 TDD 实施。单元测试使用 fake clock、fake endpoint client、临时 Raw 目录和 fake persistence，
不访问公共节点。覆盖：

- universe 只含 SSE/SZSE listed stock，排除 BSE/非 stock/退市；
- 5,200 只按 80 分批且稳定排序；
- 09:25、09:29:30、09:30 边界；
- 单 endpoint 完整成功；首 endpoint partial 后第二 endpoint 全量成功；两次失败；
- 明确空行情与缺失响应的区别；重复、未知、负数和跨日期拒绝；
- 最高价/最低价缺失、非负、倒置和最新价越界校验；
- Raw、Manifest、QualityResult、attempt ingestion 状态和单 endpoint lineage；
- 本数据集 Raw replay 在读取 Raw/写数据库前稳定拒绝，且 Raw 保持不变；
- 内部最终化只选精确日期最新 succeeded ingestion，拒绝 partial/旧日期；
- ready 空池零行成功、成员缺失整次失败、Provider 不被构造；
- 只有 09:26 job 受开关控制，21:30 job 不在目录、function map、JobStore 或本地管理页；
- migration 从空库执行，RLS/grants/check constraints 正确；
- public PostgREST/FastAPI/Agent contracts 保持兼容。

完整门禁为 Ruff format、Ruff lint、mypy、全部 pytest，以及只对 disposable
`TEST_DATABASE_URL` 运行的 PostgreSQL integration tests。

## 9. 成功标准

- 09:26 成功批次包含当天全部沪深 listed stock，单 ingestion 单 endpoint；
- partial 或 09:30 后事实不能成为成功晨间快照；
- 来源 Raw 保留，但 operational replay 在原冻结全集身份可证明前保持禁用；
- Worker 不自动执行最终化，也不提供替代调度；
- Worker 管理页、Operations、Ingestion、Raw 和数据库 lineage 可共同解释一次运行；
- BSE 保持显式暂缓，`.env` 仍只控制启用/停用。
