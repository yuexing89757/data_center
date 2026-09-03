# CallAuctionMarketSeries 异步单 Writer 设计

> 状态：已确认，待实施
> 日期：2026-09-03
> GitHub Issue：#72
> 架构决策：`docs/adr/ADR-0051-竞价序列采集与单Writer解耦.md`

## 1. 目标

将早盘竞价序列的来源采集时钟与 PostgreSQL 写入延迟隔离。采集生产者必须尽量贴近固定的
32 个 20 秒时槽执行 PYTDX 请求并立即保存 Raw；数据库写入由同进程、单线程、顺序 Writer
完成。Writer 故障不得提前终止后续来源采集。

成功标准：

- 人为把 Writer 阻塞时间设置为超过 20 秒时，Provider 仍收到全部未错过时槽请求；
- 每个已取得 Provider 响应都有不可变 Raw；
- Writer 只使用一个线程并按 `sample_seq` 单调提交；
- Writer 失败后生产者继续，Session/Workflow 不会被误标为 succeeded；
- 既有表列、调度配置、80 只批次、Provider 路由和公共 API 响应不变。

## 2. 非目标

- 不回填 2026-09-03 已经没有 Raw 的四个历史时槽；
- 不从相邻 Round、当前行情或其他 Provider 合成缺失事实；
- 不在本次实现 Raw replay、自动扫描孤儿 Raw 或重开终态 Round；
- 不引入 Kafka、Redis、SQLite JobStore 扩展、另一个 Worker 或操作系统定时任务；
- 不改变 09:25:30 单次快照任务和 FastAPI/PostgREST 契约；
- 不用直接 COPY 绕过 PostgreSQL RLS。

## 3. 组件边界

### 3.1 采集生产者

`CallAuctionMarketSeriesService.collect()` 继续拥有会话时钟、冻结全集、slot/deadline 和 endpoint
选择。它创建 Writer 后进入采样循环，每轮执行：

1. 根据真实时钟判断当前 slot 是否已错过；
2. 未错过时按既有 80 只批次调用一个 endpoint；
3. Provider 返回后立即写 Raw v2；
4. 完成决定重试所需的内存标准化、全集和时间窗判断；
5. 必要且仍有采集预算时对第二 endpoint 从全集第一只重试；
6. 把该轮所有 attempt 与最终选择封装成一个 `CapturedRound` 并入队。

采样循环不得调用 Persistence。Session 启动前冻结全集和创建 Session 的数据库操作仍属于会话
准备，不在 32 轮采样循环内；Session 聚合只在 Writer 排空后执行。

### 3.2 队列模型

新增内部不可变类型：

```python
@dataclass(frozen=True, slots=True)
class CapturedAttempt:
    run: IngestionRun
    records: tuple[MarketSeriesSnapshotRecord, ...]
    manifest: RawManifest
    quality_results: tuple[QualityResult, ...]
    elapsed: timedelta
    succeeded: bool


@dataclass(frozen=True, slots=True)
class CapturedRound:
    running_round: MarketSeriesRound
    completed_round: MarketSeriesRound
    attempts: tuple[CapturedAttempt, ...]


@dataclass(frozen=True, slots=True)
class WriterOutcome:
    persisted_sequences: tuple[int, ...]
    failed_sequences: tuple[int, ...]
    first_error_type: str | None
```

队列类型为 `Queue[CapturedRound | object]`，`maxsize=SERIES_ROUND_COUNT`。一次 Round 无论有一个
还是两个 attempts 都只占一个槽位。生产者总计最多写入 32 个 Round，因此在 Writer 完全停滞时
第 32 个 Round 仍能立即入队；结束标记在采样窗口结束后写入，允许等待 Writer 消费。

不得使用覆盖式队列、超时丢弃或多个 Writer。

### 3.3 单 Writer

新增聚焦组件 `CallAuctionMarketSeriesWriter`，生命周期完全由 Service 管理。Writer 启动一个
命名线程，循环读取 FIFO：

1. 调用 Persistence 的原子 `persist_captured_round(captured_round)`；
2. 记录成功/失败的 sample sequence 和第一个异常类别；
3. 单轮异常时继续读取后续 Round；
4. 收到结束标记后退出。

`close_and_wait()` 必须先入队结束标记，再 `Queue.join()` 和 `Thread.join()`；如果线程仍存活则
抛出明确错误；正常退出时返回不可变 `WriterOutcome`。Service 使用该结果和数据库 Session 聚合
共同决定最终状态，任何 `failed_sequences` 都禁止 succeeded。线程不得设置为脱离 Worker 生命周期
的 daemon。

Writer 不执行 Provider、Raw 文件或时钟等待操作。

## 4. 原子持久化

Persistence 新增：

```python
def persist_captured_round(self, captured: CapturedRound) -> None: ...
```

单个 `engine.begin()` 事务内顺序执行：

1. 插入 running Round；
2. 对每个 attempt 插入 running IngestionRun；
3. 插入该 attempt 的 RawManifest、质量结果和 Snapshot；
4. 将 IngestionRun 更新为 succeeded/partial；
5. 将 Round 更新为 producer 已决定的终态和 selected ingestion；
6. 刷新 Session 汇总计数。

任一步骤失败则整轮回滚，不留下半个 Round 或 running IngestionRun。两个 endpoint attempts 可以
处于同一 Round 事务；它们仍各有独立 IngestionRun、Manifest、Raw 和来源身份，不能合并记录。

现有小粒度方法可保留给恢复测试和兼容调用，但在线采样只调用新的原子入口。不得在运行时建表
或创建临时 COPY staging 表；本次性能收益主要来自消除采样阻塞和减少事务边界。

## 5. Raw v2

Raw schema 常量升级为：

```text
market_data_center.call_auction_market_series.raw.v2
```

每条 envelope 必须包含：

- `ingestion_id`
- `trade_date`
- `session_id`
- `sample_seq`
- `scheduled_at`
- `endpoint`
- `attempt_number`
- `worker_observed_at`
- `provider_schema_version`
- `provider_raw_json`

空响应仍创建零行 Raw 对象，由 `StoredRawObject` 和内存中的 `CapturedAttempt` 保留 object path、
hash、size 和 row count。Writer 成功时写 Manifest；Writer 失败时文件保持不可变，不自动扫描或
重放。v1 Raw 继续可读且不修改。

## 6. 时间与重试

采集 deadline 仍为当前 slot 后 20 秒。`CapturedAttempt.elapsed` 从 Provider attempt 开始计至
Raw 保存和必要内存校验完成，不包含队列等待或数据库提交。

首 attempt partial 时，第二 endpoint 仅在：

```text
now + max(configured_retry_budget, first_capture_elapsed) < round_deadline
```

时启动。Writer 延迟不得影响该判断。Provider 返回的 `observed_at` 仍必须属于原 Round 窗口；
Writer 再晚也不得改写来源时间。

## 7. 失败语义

- Provider 异常：生成带 provider error 的 CapturedAttempt 和 Raw，按预算决定是否重试。
- Raw 写失败：不得持久化对应标准事实；该 Round 作为采集失败项入队，生产者继续下一 slot。
- 队列 Writer 数据异常：整轮事务回滚，序号加入 `failed_sequences`，继续消费后续 Round；不得
  为失败事务另造无 attempt 的 Round 行。
- PostgreSQL 暂时不可用：受影响 Round 无数据库事实但 Raw 保留；后续 Round 仍尝试写入。
- Writer 退出异常：主线程仍等待线程结束；能连接数据库时聚合 Session 为 partial/failed，否则
  Workflow 失败并由陈旧运行恢复处理。
- 任一 Round 未完整持久化：Session 和 Workflow 不得 succeeded。

异常摘要继续只保存类型或受控枚举，不写 endpoint、路径、SQL 参数或凭据。

## 8. 数据库与 migration

新增 ordered migration `20260903000200_clarify_auction_series_collection_time.sql`，只执行：

```sql
comment on column realtime.call_auction_market_series_round.collected_at is
    'Source collection completion time; independent of asynchronous persistence commit time.';
```

它不新增列、索引、权限、函数或数据更新。Snapshot、Session、Round、Ingestion、Manifest 和质量
表的自然键、RLS 与 grants 保持不变。

## 9. 测试设计

单元测试使用真实线程和可控时钟：

- Writer 被 Event 阻塞时，生产者仍完成所有 Provider/Raw 调用；
- 32 个 Round 入队不阻塞，结束标记只在采样后等待；
- Persistence 记录的线程 ID 唯一且 sample sequence 为 `0..31`；
- 指定 Round 写失败后，后续 Round 仍写入且最终 summary 非 succeeded；
- close 后线程不存在，异常不会遗失；
- Raw v2 每个身份字段存在且值对应 attempt；
- 第二 endpoint 预算不包含 Writer 延迟。

Persistence 测试证明一个 CapturedRound 的多 attempt、Manifest、质量、Snapshot 和 Round 终态
原子提交；注入中途约束错误后整轮为零行。隔离 PostgreSQL 测试继续验证 Worker RLS，现有 API
契约测试必须零差异。

## 10. 运维验收

上线后不手工补造历史轮次。下一交易日检查：

- 32 个 Round 均有明确终态；
- Provider 开始时间相对 scheduled time 的延迟和错过轮数；
- Raw 文件数、Manifest 数、selected ingestion 和 Snapshot 数一致；
- Writer 排空时间、Session 最终时间和错误摘要；
- 09:25:30 单次快照任务不受影响；
- Worker 仍为唯一实例且调度锁健康。
