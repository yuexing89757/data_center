# ADR-0051：竞价序列采集与单 Writer 解耦

- 状态：Accepted
- 日期：2026-09-03
- 关联 Issue：#72
- 决策者：项目所有者
- 扩展：ADR-0034、ADR-0006、ADR-0017、ADR-0019

## 背景

`call-auction-market-series` 原实现把 PYTDX 读取、Raw 保存、质量校验、约 5,200 行
Snapshot 写入和 Round 状态更新串在同一采集循环中。2026-09-03 生产会话的行情读取平均约
3.71 秒，但读取完成至下一次读取启动的路径最长约 27.42 秒，超过固定 20 秒 cadence；结果
32 轮中 4 轮完全错过、1 轮 partial，缺失 22,535 条来源事实。错过轮次没有 Raw，盘后无法
真实重建当时五档。

生产回滚基准显示，当前文本 executemany 写入 5,215 行约 6.02 秒；直接 COPY 约 3.09 秒，
但 Worker 角色不能绕过目标表 RLS。临时表 COPY 后再 INSERT SELECT 约 3.40 秒，只优化 SQL
仍不足以保证在数据库负载波动时不超过 20 秒。根因是采样时钟与持久化延迟耦合，而不是
PYTDX 请求批量大小。

## 决策

1. 一个 `call-auction-market-series` 会话在同一 Worker 进程中使用单一采集生产者和单一
   PostgreSQL Writer。不得增加第二个 Worker、外部队列服务或 OS 级调度。
2. 采集生产者按 32 个固定时槽运行。时槽内只执行 PYTDX 请求、不可变 Raw 写入以及决定
   endpoint 重试所必需的内存校验；不得等待 PostgreSQL Round、Ingestion、Manifest、质量或
   Snapshot 写入。
3. 每轮形成一个不可变 `CapturedRound` 队列项，其中包含该轮全部 endpoint attempts、Raw
   身份、规范候选、质量结果和最终 Round 选择。一个 attempt 仍只来自一个 endpoint，禁止
   合并不同 endpoint 的 partial 数据。
4. 使用进程内有界 FIFO，容量固定为 32 个 Round。每轮只占一个队列项，因此采集窗口内不会
   因 Writer 落后而阻塞；结束标记允许在最后采样点之后等待空位。不得丢弃、覆盖或重排队列项。
5. Writer 只有一个线程，严格按 sample sequence 顺序消费。每轮数据库事实以一个事务写入：
   Round、全部 IngestionRun、RawManifest、质量结果、Snapshot 和 Round 终态共同成功或回滚。
6. Writer 单轮失败不得停止采集生产者。Writer 保存第一个异常类别并继续消费后续队列项；
   失败事务不伪造 Round 或 attempt 事实，由 Session 聚合把未持久化序号计入 `failed_rounds`，
   数据库整体不可用时则由 Workflow 失败和陈旧运行恢复暴露缺口。采集结束后必须等待队列排空，
   再聚合 Session 和 Workflow 状态。存在未持久化轮次时不得发布 succeeded。
7. Raw schema 升级为 `market_data_center.call_auction_market_series.raw.v2`。每条 envelope 除
   原始响应外还保存 ingestion ID、trade date、endpoint、attempt number、session ID、sample
   sequence、scheduled time 和 worker observed time，使未提交 Raw 具备未来受控恢复所需身份。
8. 本 ADR 不启用历史 Raw replay。ADR-0034 的 replay fail-closed 继续有效；扫描无 Manifest
   Raw、重开终态 Round 或创建恢复 Session 必须由后续 Accepted ADR 单独定义。
9. 首 endpoint partial 后的第二 endpoint 预算只计算采集路径实际耗时：Provider 请求、Raw
   保存和必要内存校验。数据库排队与持久化耗时不再占用当前 Round deadline。
10. 交易日、09:15:00–09:25:20 时槽、20 秒 cadence、最后 09:25:40 deadline、每批 80 只、
    冻结全集、表列、分区、RLS、调度开关和公共 API 契约保持不变。
11. 新 ordered migration 不增加列或改写事实，仅更新 Round `collected_at` 的数据库注释：该值
    表示来源采集完成时间，不表示数据库提交完成时间。

## 失败与关闭顺序

1. 生产者完成最后时槽后发送结束标记。
2. Writer 消费结束标记前的全部 Round，然后退出。
3. 主线程等待 Writer 完成，调用 Session 聚合。
4. 如果数据库短暂失败后恢复，后续 Round 仍可写入；失败 Round 和缺失写入保持可见。
5. 如果 Worker 收到停止信号，现有 Worker 优雅关闭机制等待会话返回；会话先停止新增采样，再
   等待已入队 Round 落盘。不得在后台遗留脱离 Worker 生命周期的 Writer。

## 后果

- 数据库延迟不再改变后续 PYTDX 请求的计划时点；
- PostgreSQL 写入仍为单线程、确定顺序和单 provider 事实；
- 单日内存上限由 32 个 Round 队列项限定，最坏情况下约保存一整个会话的标准化数据；
- Session 可能在 09:25:40 后继续等待 Writer，但不再影响已完成的采样时间；
- Writer 崩溃时 Raw 仍保留，但在后续 replay 决策落地前不会自动写入事实表。

## 验收

- 使用慢 Persistence 的确定性测试证明 32 次 Provider 请求仍按固定时槽发生；
- 测试证明所有 PostgreSQL 方法仅由一个 Writer 线程调用，且 sample sequence 单调；
- Writer 单轮异常不阻止后续轮次获取和 Raw 保存，最终状态不为 succeeded；
- 测试证明队列无丢弃、正常关闭等待排空、异常关闭不泄漏线程；
- Raw v2 身份字段、单 endpoint attempt、deadline 与第二 endpoint 预算均有单元测试；
- 隔离 PostgreSQL 测试证明每轮事务原子性、RLS 与既有只读契约不变；
- Ruff、mypy、全量 pytest 和生产检查通过，下一交易日 live gate 验证 32 轮及实际队列延迟。
