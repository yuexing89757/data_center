# 竞价序列快照三交易日清理任务设计

- 日期：2026-09-03
- 状态：Approved for planning
- GitHub Issue：#71
- 关联决策：ADR-0015、ADR-0017、ADR-0034、ADR-0040、ADR-0050

## 目标

新增默认启用的 Worker 内部“数据清理任务”，每天上海时间 03:00 删除沪深全市场竞价序列标准快照中
早于最近三个已完成交易日的数据。任务只缩短 PostgreSQL 标准快照的在线保留期，不删除采集身份、
状态、Raw 或审计血缘，也不影响独立的 09:25:30 单点竞价快照。

## 已评估方案

### 方案一：Worker 执行受限 DML 删除（采用）

通过交易日历计算明确截止日，在一个事务中删除父分区表的 `trade_date < cutoff_date` 行。该方案沿用
现有 Worker、Persistence、Operations 和 migration 边界，能精确保留三个交易日，并让 Raw 与审计
事实继续长期存在。月度分区文件不保证立即缩小，但过期行会被逻辑清除并等待 PostgreSQL vacuum。

### 方案二：Worker 自动删除月度分区（拒绝）

删除整月分区速度快，但月度粒度不能表达三个交易日窗口，而且让 Worker 在运行时执行 DDL，违反
Schema 即代码和 ADR-0034 的分区管理边界。

### 方案三：连同 Raw 和血缘一起删除（拒绝）

该方案释放空间最多，但会破坏来源追溯、失败诊断和未来 Raw replay，违反项目宪法。质量结果当前虽占用
更大空间，也必须通过独立 Issue 和保留决策评估，不能搭载到本任务。

## 数据范围

唯一删除目标：

```text
realtime.call_auction_market_series_snapshot
```

明确保留：

- `realtime.call_auction_market_series_session`；
- `realtime.call_auction_market_series_round`；
- `realtime.call_auction_market_snapshot`；
- Raw JSONL 文件；
- `ingestion.raw_manifest`；
- `ingestion.ingestion_run`；
- `audit.quality_result`；
- `operations.workflow_run` 和 `operations.job_execution`。

本任务不接收数据集或表名参数，避免权限扩散和误删。

## 截止日语义

任务获取当前上海本地日期 `reference_date`，从统一交易日历查找：

```sql
market = 'CN_A_SHARE'
is_open = true
trade_date < reference_date
order by trade_date desc
limit 3
```

查询结果恰好有三日时，最早日期为 `cutoff_date`，删除：

```sql
delete from realtime.call_auction_market_series_snapshot
where trade_date < :cutoff_date
```

例如任务在 2026-09-04 03:00 执行时，如果 9 月 1、2、3 日均开市，则截止日是 9 月 1 日；保留
9 月 1、2、3 日，删除更早数据。周六、周日和节假日仍按最近三个开市日计算，不按自然日删除周五数据。

任务在当天早盘采集之前运行，所以“已完成交易日”必须严格早于 `reference_date`。如果日历不足三日，
抛出稳定错误且不打开删除事务。截止日当天始终保留。

## 应用边界

新增纯截止日服务，负责使用 Persistence 读取最近三个已完成交易日并产生明确 cutoff；数据库删除只存在于
PostgreSQL Persistence。Scheduler 只负责创建 Operations workflow、调用服务并记录统计，不拼接 SQL。

清理结果至少包含：

- `cutoff_date`；
- `deleted_rows`；
- 固定保留交易日数 `3`。

Operations 的 fetched/accepted 行数均使用实际删除行数，rejected 为零。删除失败时事务回滚，步骤与
workflow 标记失败；次日调度自然重试。没有过期行时成功并记录零，保证幂等。

## 调度与配置

任务目录新增：

- Job ID：`data-cleanup-daily`；
- 显示名：`数据清理任务`；
- Workflow：`data_cleanup`；
- Step：`cleanup_call_auction_market_series_snapshots`；
- Cron：每天 `03:00`；
- 时区：`Asia/Shanghai`；
- 默认执行器、`max_instances=1`、现有 coalesce/misfire/timeout 规则。

`SchedulerSettings.data_cleanup_enabled` 默认 `True`，映射环境变量 `DATA_CLEANUP_ENABLED`。不增加执行时间、
保留天数、市场或目标表环境变量。本地只读任务页从统一任务目录自动显示任务和下次执行时间。

## 数据库与权限

ordered migration：

1. 扩展 `operations.workflow_run_workflow_code_check`，允许 `data_cleanup`；
2. 授予 `market_data_worker` 对竞价序列 snapshot 父表的 `DELETE`；
3. 创建仅对 `market_data_worker` 生效的 DELETE RLS policy；
4. 不向 API、匿名或 authenticated 角色授权 DELETE；
5. 不修改 Session、Round 或其他事实表权限；
6. 不执行历史数据删除，实际删除由部署后的 03:00 Worker 任务完成。

Persistence 使用显式 cutoff 参数和单事务 DELETE，不运行 DDL、VACUUM、TRUNCATE 或动态表名 SQL。

## API 和历史读取

公共 RPC、FastAPI 路由、请求响应模型和 checked-in OpenAPI 不变。API 对保留窗口内数据继续按现有规则
查询；对已清理日期仍选择保留的历史 Session 和 Round，但每轮 `items=[]`、`returned_count=0`，请求代码
进入 `missing_codes`。Session 和 Round 继续证明当日任务及各轮状态，不把已清理数据伪装为从未采集。

## 失败与安全

- 交易日历不足三日：删除前失败；
- 数据库 DELETE 失败：事务回滚，Operations 记录失败；
- Worker 重复触发：任务单实例，DELETE 本身幂等；
- Worker 中途退出：数据库事务回滚，下一次运行重试；
- 月度分区缺失或权限错误：显式失败，不绕过 RLS 或改用管理员连接；
- Raw 和审计存储不受影响；
- 不提供手工传入任意 cutoff 的公共 API。

## 验证

单元测试：

- 普通连续交易日选择第三个已完成交易日；
- 周末、节假日仍保留三个交易日；
- 当天是交易日时不把未完成当天计入窗口；
- 日历不足三日 fail closed；
- 无过期行时返回零；
- 任务目录固定为每天 03:00，默认启用，开关只影响启停；
- Operations 精确记录删除行数与失败。

隔离 PostgreSQL 集成测试：

- cutoff 前行删除、cutoff 当天及之后行保留；
- 重复删除幂等；
- 删除异常事务回滚；
- Worker 角色可删除目标行但不能删除其他竞价表；
- API 角色没有 DELETE 权限；
- migration、RLS 和受控 workflow code 正确。

发布前执行 Ruff、mypy、全量 pytest、隔离 integration tests 和 migration check。生产迁移与部署必须由
项目所有者另行明确授权；部署不主动手工触发首次清理。
