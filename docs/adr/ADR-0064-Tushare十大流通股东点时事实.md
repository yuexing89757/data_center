# ADR-0064：Tushare 十大流通股东点时事实与每周采集

- 状态：Accepted
- 日期：2026-10-05
- 决策者：项目所有者
- 影响：Top10FloatHolder、Tushare Provider、Raw 重放、Worker 调度、CLI
- 前置：ADR-0047（股东人数点时事实与 Tushare 采集）、ADR-0061（Tushare 历史 ST 日快照的端点配置）

## 背景

前十大流通股东是上市公司在定期报告或公告中披露的持有人结构事实：报告截止日、公告日、
持有人名称、持股数量、占总股本比例、占流通股本比例、较上期变动与持有人类型。它不是
股本变更，也不是逐交易日快照，而是按公告披露的点时（point-in-time）事实，可用于股东
结构集中度与边际变化分析。

Tushare `top10_floatholders` 接口按公告日全市场返回该事实（经兼容端点实测：9 个字段、
`ann_date` 单日全市场查询、单次上限 6000 行、响应数值以 JSON float 表示）。来源响应不
包含名次字段，同组名次必须由消费方确定；来源对同一报告期可能存在多次披露修订。

## 决策

1. 新增 provider-neutral `Top10FloatHolder` 领域，保存标准 `symbol`、报告截止日
   `end_date`、公告日 `announcement_date`、名次 `holder_rank`、持有人名称
   `holder_name`、持股数量 `hold_amount`（股）、占总股本比例 `hold_ratio`、占流通股本
   比例 `hold_float_ratio`（Decimal 百分比数值）、较上期持股变动 `hold_change`（股，
   可空）、持有人类型 `holder_type`（可空）和 `source_code`。首期来源仅为 `tushare`，
   Raw schema 固定为 `tushare.top10_float_holder.v1`。
2. Core 表为 `core.top10_float_holder`。主键是
   `(symbol, end_date, announcement_date, holder_rank)`，另对
   `(symbol, end_date, announcement_date, holder_name)` 建唯一键防止同名重复行。事实
   追加而不覆盖：重复自然键 `on conflict do nothing`，保留既有行及其首次血缘
   `ingestion_id` 与 `first_observed_at`，不执行更新。
3. 来源无名次字段，`holder_rank` 由 adapter 在
   `(symbol, end_date, announcement_date)` 组内按 `hold_amount` 降序、`holder_name`
   升序确定性派生。来源可能对同一公告组返回超过 10 行（同一实体以全称与简称
   重复列示等冗余）：每行都忠实派生名次，第 11 名及以后由领域校验隔离为质量拒绝
   （ERROR 留痕、计入拒绝行数），不静默丢弃也不使整批失败；该推导在 Raw 重放时
   产生一致结果。
4. `hold_amount` 语义为整股数。来源 JSON float 序列化（如 `945568990.0`）解析为整数：
   接受 `.0` 尾缀，拒绝真正的小数部分。`hold_amount` 为空的行表示来源没有可发布
   事实：完整保留在 Raw，不生成 Core 记录，不使批次失败。零、负数、名次越界、公告日
   早于报告截止日、负比例、空白名称、未知证券、批内名次或名称冲突均为硬错误，冲突对
   整体隔离进入质量拒绝，不静默择一。
5. 每个实际 Tushare 请求产生一个 IngestionRun、一个不可变 Raw 对象和一个 manifest。
   一次同步窗口（每日一请求）内全部请求完成后，在一个数据库事务中登记 manifests、
   质量结果、发布全部事实并终结运行；任一请求失败时中止并标记全部已准备批次
   `failed`，不发布部分事实。
6. 恰好 6000 行的响应视为可能截断：该全日请求作废（保留 Raw 与 INFO 质量
   `top10_float_holder.response_split`，`accepted_rows` 置 0），改按公告日前最近的
   8 个季度末报告期逐期拆分请求；单个拆分请求仍满额时硬失败，显式暴露截断而不是
   静默丢弃。合法空响应是成功，不伪造事实。
7. 每周六 08:00 Asia/Shanghai 在 Worker 内运行滚动 9 日公告窗口（覆盖上一整周并留
   两侧余量）。`TOP10_FLOAT_HOLDERS_WEEKLY_ENABLED` 默认关闭，首次生产窗口探测通过后
   显式启用。手动补采走 `market-data-center top10-float-holders` CLI（Workflow
   `top10_float_holders_weekly`，`TriggerSource.MANUAL`）。
8. 请求节奏复用 ADR-0047 的 Tushare 股东接口限速器（默认每分钟 180 次，令牌间隔共享
   计数）。Token 仅从 `TUSHARE_TOKEN` 读取，端点遵循 ADR-0061 的
   `TUSHARE_ENDPOINT`，均不进入 Raw、请求参数、日志或数据库。
9. 首期不新增 `api_v1` 公开视图或 RPC：本数据集的消费者分析需求尚未定型，公开只读
   契约（有界 as-of / current-known 语义、双时间门禁）待消费方需求明确后按宪法流程
   另行决策。在此之前任何消费者不得依赖 `core.top10_float_holder` 作为稳定契约。
10. 所有定时触发只进入 `market-data-center worker` 的 APScheduler 任务目录。不得增加
    Windows Task Scheduler、cron 或其他操作系统级采集触发器。

## 后果

- 数据中心获得按公告修订粒度追加的前十大流通股东事实，同一报告期的多次公告各自成行，
  不互相覆盖，支持后续按公告时点或最新已知口径分别消费。
- 9 日滚动窗口不能证明发现更早公告日的修订；全历史回填如需进行，须另行显式受控
  Workflow 与授权。
- 6000 行截断拆分最多产生 1 + 8 个请求/日；公告峰值日请求量上升，但避免了单请求静默
  丢行。
- 首期不抽象十大股东（限售）、股东增减持、户均持股或筹码集中度标签，也不引入第二
  来源。

## 验收

- Domain 校验/分区、Tushare adapter（名次派生、float 股数解析、period 参数校验、
  Raw 重放）、Service（窗口、拆分、中止语义）、批量持久化（单事务、自然键预检、
  abort 不发布事实）均有聚焦测试。
- PostgreSQL 集成测试覆盖 migration、约束（名次、正持股、日期序、非空名称）、
  追加幂等（重复批次不重复不覆盖）、Worker 最小授权与 RLS 清单。
- 不改变 `contracts/postgrest-openapi-v1.json`、`contracts/agent-tools-v1.json`、
  `contracts/fastapi-openapi-v1.json`。
- Ruff format/check、mypy、pytest 和隔离 PostgreSQL integration gate 通过；无法运行的
  检查必须报告准确命令和原因。
