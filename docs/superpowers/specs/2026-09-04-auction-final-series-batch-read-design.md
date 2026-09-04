# 09:25:20 竞价序列末批复用设计

## 目标

让现有批量竞价快照接口和主板一字涨跌停接口统一读取全市场竞价序列的 09:25:20 最后一批，
并退役 09:25:30 的独立全市场竞价快照定时任务。接口路径和响应结构保持兼容，历史单次快照
事实不删除。

关联 GitHub Issue：#73。治理决策见 ADR-0052。

## 当前边界

- 序列任务在工作日 09:15:00 至 09:25:20 每 20 秒采集，共 32 轮；会话冻结 SSE/SZSE、
  `security_type=stock`、`status=listed` 全集。
- 每轮 attempt 使用单一 PYTDX endpoint，Snapshot 已保存价格、量额和买卖五档。
- 单 Writer 按 FIFO 原子提交 round、ingestion、Raw manifest、quality 和 Snapshot。
- `call_auction_market_snapshot` 是独立 09:25:30 采集的历史来源表。本设计不修改其事实。

## 方案比较

### 方案 A：RPC 直接读取序列末批（采用）

原位替换两个 RPC 的来源查询，固定选择 `batch_code='092520'` 的 selected ingestion，并在 SQL
中确定性计算封单额。移除 09:25:30 job 的调度注册，保留旧表和非调度实现。

该方案没有重复采集和复制，公共契约改动最小，lineage 直接指向真实的序列 attempt。

### 方案 B：把序列末批复制到旧表

接口无需改来源，但会制造同一行情事实的第二份持久化记录，增加事务、Raw/ingestion 语义和失败
恢复复杂度，也不能消除重复写入，因此不采用。

### 方案 C：继续保留 09:25:30 任务

观察时间更晚十秒，但继续重复读取全市场并与序列 Writer 竞争资源，不符合本次目标，因此不采用。

## 末批选择

### 显式交易日

1. 只检查请求交易日。
2. 只接受 `sample_seq=31` 且 `batch_code='092520'` 的轮次和 Snapshot。
3. 在同日候选会话中，末轮 selected ingestion 为 `succeeded` 的会话优先；没有 succeeded 时，
   允许 selected ingestion 为 `partial`。
4. 同一优先级按会话启动时间、ingestion 完成时间和稳定 UUID 顺序选择最新单批。
5. 不存在可用末批时抛出 `P0002`，FastAPI 返回 404。

不得选择 09:25:00 或更早轮次，不得跨会话补齐缺失证券，不得从旧单次快照表兜底。

### 省略交易日

仅一字涨跌停接口允许省略日期。RPC 选择最近存在可用 09:25:20 末批的交易日，再应用同样的
succeeded 优先、partial 兜底规则。不得选择只有较早轮次的日期。

## 批量竞价快照接口

`POST /api/v1/call-auction-market-snapshots/query` 的请求边界保持不变：必须提供交易日和 1 至
500 个六位代码，重复代码去重。RPC 返回单一 selected ingestion 的匹配证券和明确的
`missing_codes`。

字段映射直接来自序列 Snapshot。`seal_amount` 使用以下表达式产生：

```text
ask1_volume in (NULL, 0)
and ask2_volume in (NULL, 0)
and ask3_volume in (NULL, 0)
and bid1_price is not NULL
and bid1_volume is not NULL
=> bid1_price * bid1_volume
otherwise NULL
```

价格和金额保持 PostgreSQL numeric，不经过 float。响应中的 `ingestion_id` 和
`ingestion_status` 表示所选序列 attempt。

## 一字涨跌停接口

`GET /api/v1/call-auction-one-price-limits` 保留现有主板代码范围、证券生命周期、名称、上市前五
交易日完整性、百分之十价格限制算法、一字形态判定和遗漏计数。

唯一变化是来源 CTE 改为所选 09:25:20 序列末批。封单额使用与批量快照 RPC 完全相同的 SQL
表达式；不新增持久化字段，不访问 Provider，不写入数据库。

响应字段保持不变，但 `snapshot_window` 的固定值改为
`09:25:20-09:25:39 Asia/Shanghai`，与末轮计划时点及其二十秒硬截止一致。

## 调度退役

从 `scheduling_catalog.py` 删除 `call-auction-market-snapshot-daily` 定义，并从 Scheduler job
handler 注册中移除对应条目。任务管理页由同一代码目录生成，因此同步不再展示该任务。

保留以下内容：

- `realtime.call_auction_market_snapshot` 及历史行；
- 已执行 workflow、ingestion、Raw manifest 和 quality lineage；
- ordered migration 历史；
- 非调度 service/persistence 代码，便于审计旧批次和受控恢复。

环境文件不新增或保留执行时间配置。遗留 enable 设置若已无运行消费者，则从 Settings 公共面和
相关测试中移除，避免产生“可以启用已退役任务”的误导。

## 迁移与契约

新增一个 ordered SQL migration，使用 `create or replace function` 修改：

- `api_v1.query_call_auction_market_snapshots(date,text[])`；
- `api_v1.query_auction_one_price_limits(date)`。

函数签名、`security definer`、固定 `search_path`、statement timeout、revoke/grant 保持不变。
FastAPI OpenAPI 路径和模型不变化，但中文描述、PostgREST 合同说明和 Agent tool 描述更新为
09:25:20 序列末批语义。

## 错误与时序

- 09:25:20 轮尚未提交：404；
- 末轮 selected partial：200，并显式返回 partial ingestion 状态和缺失代码；
- 数据库/RPC 错误：503；
- 非法代码集合：现有 4xx 映射保持不变。

接口可用时间由末轮 Provider、Raw 和 Writer 实际完成时间决定，不承诺 09:25:30 前可见。移除
重复任务会消除已知竞争，但不能用更早轮次换取提前响应。

## 测试

1. PostgreSQL fixture 同时放入旧单次快照和多个序列会话，证明两个 RPC 只读取 09:25:20。
2. 覆盖 succeeded 优先、partial 兜底、末轮缺失、较早轮次禁止回退、显式日期禁止回退。
3. 覆盖五档字段、零价有量、封单额有效与无效分支。
4. 复用一字涨跌停既有主板边界和遗漏用例，替换其来源 fixture。
5. Scheduler 测试证明任务目录不再注册 09:25:30 job，序列任务仍默认启用。
6. 同步生产 schema inventory、发布检查、运行手册及三个 JSON API 契约。

## 发布与验证

发布按代码提交、生产 migration、Worker/API 部署和服务重启顺序执行。迁移后只读验证两个 RPC
和 FastAPI 接口；下一交易日在 09:25:20 末批落库后验证 32 轮完整性、末批 lineage、封单额和
一字涨跌停结果，同时确认 09:25:30 job 不再出现。
