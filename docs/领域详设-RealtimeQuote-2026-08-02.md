# 领域详设：RealtimeQuote 股票实时五档 v0

> 沪深全市场开盘竞价来源快照：ADR-0027；Raw replay 暂停及自动最终化移除：ADR-0028（Accepted）。

## 全市场开盘竞价来源快照（v2）

### 领域边界

`CallAuctionMarketSnapshotRecord` 是 09:25 最终撮合后、09:30 连续竞价前观察到的沪深 listed
stock 来源事实。它不是逐笔成交、分钟行情或盘后历史重建。BSE 不在 v2 支持范围。

```text
09:25:30 Security 全集
  → 单 endpoint、每批 ≤80 的 pytdx_hq 采集
  → Raw JSONL + RawManifest + IngestionRun
  → realtime.call_auction_market_snapshot（append-only）
```

### 标准记录

```text
CallAuctionMarketSnapshotRecord
├── symbol: str
├── trade_date: date
├── observed_at: aware datetime
├── last_price: Decimal | None
├── previous_close: Decimal | None
├── high_price: Decimal | None
├── low_price: Decimal | None
├── cumulative_volume: int | None
├── cumulative_amount: Decimal | None
├── bid_levels: tuple[OrderBookLevel, ×5]
├── ask_levels: tuple[OrderBookLevel, ×5]
├── seal_amount: Decimal | None
└── source_code: pytdx_hq
```

记录不包含 `ingestion_id`；Persistence 在写边界附加。累计量/额只因观察窗口被限定为
`[09:25,09:30)` 才具有本设计中的开盘竞价语义。09:30 后同名来源字段属于全天连续成交累计，
不得写入该聚合。

### 全集和完整性

- 全集来自 `core.security` 的 SSE/SZSE、`security_type=stock`、`status=listed`；
- BSE、ETF、可转债、指数和退市证券排除；
- 每个 attempt 的预期 symbol 固定、排序且唯一；
- 一个成功 ingestion 只使用一个 endpoint；失败后新 ingestion 才能换 endpoint 全量重试；
- 响应集合与预期集合完全相同才能成功；停牌/空行情的明确响应仍是事实；
- partial/failed ingestion 的行和 Raw 用于审计；本数据集 operational Raw replay 暂停，不能由通用
  replay 路径创建新 ingestion。

### 存储和查询

`realtime.call_auction_market_snapshot` 以 `(ingestion_id,symbol)` 为主键，保存 `trade_date`、
`observed_at`、最新价、昨收、截至观察时点的当日最高价/最低价、标准量额、买卖五档、
`seal_amount` 和来源。档位价格为零且数量为正时保存为 `price=NULL` 并保留数量；卖一至卖三量
分别为空或零且买一价量完整时，`seal_amount=bid1_price*bid1_volume`，否则为 `NULL`。最高价/最低价
允许缺失；非空时必须非负且 `high_price >= low_price`，最新价与两者均非空时必须落在该区间。
索引支持按 `(trade_date,ingestion_id,symbol)` 读取。
成功输入的选择先在 `ingestion.ingestion_run` 限定 dataset/status，再与精确日期事实连接；不得从
`request_params` JSON 猜测交易日。

现有数据库最终化实现仍可写 `realtime.call_auction_snapshot`，保留晨间 ingestion lineage 和
`observed_at`，但它是非调度内部能力。公共 `query_daily_limit_up_list` 继续只连接最终表。

外部只读服务可调用受限的
`api_v1.query_call_auction_market_snapshots(p_trade_date date,p_codes text[])` 查询全市场来源事实：
代码数量为 1～500，格式固定为六位数字；重复代码去重；同一代码若同时属于 SSE/SZSE，返回两条
标准 symbol。精确日期优先选择最新 `succeeded` ingestion；没有成功批次时才选择最新 `partial`
ingestion；不得拼接批次或回退日期。响应显式返回 provider-neutral ingestion ID/status、缺失代码和
最高价/最低价等事实，不公开 `source_code`、Raw、节点或内部创建时间。内部表仍不直接授权 API
角色，RPC 使用五秒 statement timeout。

### 时间和调度

- `call-auction-market-snapshot-daily`：工作日 09:25:30；
- 09:29:30 后不发新请求，`observed_at >=09:30` 硬拒绝；
- `call-auction-snapshot-daily` 已移除，无替代自动调度；
- `CALL_AUCTION_SNAPSHOT_ENABLED` 只控制 09:25:30 任务；时间只在代码目录；
- 非交易日跳过，错过晨间窗口不补采，盘后不调用历史成交或实时行情伪造。

### 保留和容量

来源事实和 Raw 长期保留，首版不增加清理任务；只有原冻结全集的确定性身份被持久化并验证后，
才可重新启用 Raw replay。每日单次快照不构成逐秒持续采集，也不改变 ADR-0012 对分钟、tick、
逐笔和 Level-2 的禁止边界。

> 集合竞价采集落地决策：`adr/ADR-0022-集合竞价涨停池五档快照采集.md`（Accepted）

## 集合竞价涨停池采集边界（已退役）

Issue #54 删除 `opening-auction-limit-up-quotes` Worker 任务和 pysnowball 运行时 Adapter。
`AuctionCollectionSession`、历史来源身份、历史事实表、Raw、查询和陈旧会话恢复仍保留，以保证
既有数据可追溯；不得再注册、启动或补采该会话。全市场竞价序列与 09:25:30 快照继续独立使用
`pytdx_hq`。

> 状态：有效，已实现
> 日期：2026-08-02
> 上级决策：`adr/ADR-0012-股票实时五档行情.md`（Accepted）

## 1. 领域职责

RealtimeQuote 保存 Worker 从行情 Provider 观察到的股票最新行情和买卖五档快照，提供
来源可追溯、单位统一、可重放的短周期客观事实。

它不负责：

- 逐笔委托、逐笔成交、撤单或订单队列；
- 六至十档和 Level-2 权限数据；
- 从盘口生成分钟 K、复权行情、情绪标签或交易信号；
- 向客户端保证交易所级低延迟、无丢包或推送流。

## 2. 聚合：FiveLevelQuoteSnapshot

候选 Python DTO：

```text
FiveLevelQuoteSnapshotRecord
├── symbol: str
├── market: CN_A_SHARE
├── observed_at: aware datetime
├── source_timestamp: aware datetime | None
├── quote_status: trading | suspended | closed | unknown
├── last_price: Decimal | None
├── previous_close: Decimal | None
├── open / high / low: Decimal | None
├── cumulative_volume: int | None
├── cumulative_amount: Decimal | None
├── bid_levels: tuple[OrderBookLevel, ×5]
├── ask_levels: tuple[OrderBookLevel, ×5]
└── source_code: str

OrderBookLevel
├── level: 1..5
├── price: Decimal | None
└── volume: int | None
```

`OrderBookLevel` 是值对象，方向由其所在的 `bid_levels`/`ask_levels` 决定，避免同一层级
同时携带相互冲突的 side。元组固定五个位置。`price` 非空时 `volume` 必须非空；集合竞价
来源允许 `price=NULL` 且 `volume>0`，表示尚未形成价格的真实委托量；两者同时为空表示空档。

DTO 不包含 `ingestion_id`。Pipeline 在校验后使用
`IngestionEnvelope[FiveLevelQuoteSnapshotRecord]` 附加采集批次。

## 3. 字段语义和单位

| 字段 | 语义 | 标准单位/精度 |
| --- | --- | --- |
| `symbol` | 标准证券标识 | `SSE/SZSE/BSE:NNNNNN` |
| `observed_at` | Worker 完整收到响应的时点 | aware datetime；数据库 `timestamptz` |
| `source_timestamp` | 来源明确提供的行情时点 | aware datetime，可空 |
| `last_price` | 来源最新成交价/闭市价 | 人民币元，`numeric(18,4)` |
| `previous_close` | 来源昨收 | 人民币元，`numeric(18,4)` |
| `open/high/low` | 当日截至快照的开高低 | 人民币元，`numeric(18,4)` |
| `cumulative_volume` | 当日累计成交量 | 股，`bigint` |
| `cumulative_amount` | 当日累计成交额 | 人民币元，`numeric(24,2)` |
| `level.price` | 指定方向和档位的委托价格 | 人民币元，`numeric(18,4)` |
| `level.volume` | 指定档位显示数量 | 股，`bigint` |

`observed_at` 不是交易所事件时间。只有来源给出完整 Unix 时间戳或可以无猜测地得到完整
上海时间时，才填写 `source_timestamp`。pytdx 仅给出服务器时分秒时，首版保持
`source_timestamp=NULL`，原始字段只留在 Raw。

## 4. 标识、幂等和修订

- 候选自然键：`(symbol, observed_at)`；
- Provider 对一次完整响应只捕获一个 `observed_at`，同一批所有证券共享该观察时点；
- `observed_at` 必须随标准 Raw 行保存，Raw 重放使用原值，不重新生成当前时间；
- 相同自然键和相同内容重复写入时幂等跳过；相同自然键内容冲突时拒绝整组并产生
  `realtime_quote.conflicting_snapshot`；
- 快照 append-only，不用后到来源覆盖已经观察到的历史；
- `source_code` 不参与自然键。跨来源比较属于 Audit，不拼接成一个快照。

## 5. Provider 映射

### 5.1 pytdx_hq

- 输入标准 symbol，在 Adapter 内转换为通达信市场编号和 6 位代码；
- 只使用网络 `get_security_quotes`，与本地 `.day` Provider `pytdx` 分开注册；
- `bid1..bid5`/`ask1..ask5` 转换为五档价格；
- `bid_vol1..5`/`ask_vol1..5` 和累计量从“手”转换为股；
- 来源档位价格为零且数量为正时保留股数并将价格标准化为 `NULL`；价格和数量均为零时
  标准化为空档；
- 记录节点、连接耗时和协议字段版本到采集参数/Raw，不进入领域 DTO；
- 未验证 BSE 前返回 `ProviderRequestUnavailable`，不得猜测市场编号。

### 5.2 pysnowball

`pysnowball` 仅作为历史来源身份保留。Issue #54 删除其运行时 Adapter、配置和自动路由；历史
快照及 ingestion lineage 不迁移、不改写。

## 6. 校验

### 6.1 严重失败，禁止入库

- symbol 不存在、不是允许的股票类型或交易所未获支持；
- `observed_at` 无时区、晚于 Worker 当前时间的允许偏差，或同批观察时间不一致；
- 任一价格、数量或金额为负；
- 同一档位价格非空但数量为空；
- 买/卖元组不是严格的 level 1～5 且有重复或缺少位置；
- 非空买价不按 level 递减，或非空卖价不按 level 递增；
- OHLC 关系非法，或最新价明显超出当日高低范围；
- 同一自然键出现冲突内容；
- Provider 原始字段、Cookie 或节点地址进入标准 DTO。

### 6.2 质量告警，可按策略入库

- 买一高于卖一（交叉盘口），因为集合竞价、闭市或来源不同步时可能短暂出现；
- 交易状态为 trading 时任一方向第一档为空；
- `source_timestamp` 缺失，或它与 `observed_at` 差值超过配置阈值；
- pytdx 手转股导致数量只有 100 股精度；
- 昨收、累计量、累计额或 quote status 缺失；
- 闭市后重复返回完全相同盘口。

不因空档补零。零数量只有来源明确给出零且语义经过契约验证时才能保留为零。

## 7. 候选存储模型

内部 Schema 为 `realtime`，不把高频事实混入 `core.daily_bar`。

### 7.1 `realtime.five_level_quote_snapshot`

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `snapshot_id` | uuid | 主键；Persistence 生成 |
| `symbol` | text | Security 外键 |
| `market` | text | `CN_A_SHARE` |
| `observed_at` | timestamptz | 非空 |
| `source_timestamp` | timestamptz | 可空 |
| `quote_status` | text | 受控枚举 |
| `last_price/previous_close/open/high/low` | numeric(18,4) | 可空、非负 |
| `cumulative_volume` | bigint | 可空、非负，股 |
| `cumulative_amount` | numeric(24,2) | 可空、非负，元 |
| `source_code` | text | `pytdx_hq`/`pysnowball` |
| `ingestion_id` | uuid | IngestionRun 外键 |
| `created_at` | timestamptz | 数据库写入时间 |

唯一约束：`unique(symbol, observed_at)`；查询索引：
`(symbol, observed_at desc)`。

### 7.2 `realtime.five_level_order_book`

| 字段 | 类型 | 约束 |
| --- | --- | --- |
| `snapshot_id` | uuid | 快照外键，级联删除仅用于受控保留策略 |
| `side` | text | `bid`/`ask` |
| `level` | smallint | 1～5 |
| `price` | numeric(18,4) | 可空、非负 |
| `volume` | bigint | 可空、非负，股 |

主键：`(snapshot_id, side, level)`。数据库约束禁止 price 非空而 volume 为空；允许集合竞价
阶段的 price 为空、volume 为正，并禁止用双零表示空档。

历史清理不能直接进入首个 migration。必须先确定热数据保留期、Raw 保留和恢复目标，并
通过独立受测运维命令按明确截止时间执行；禁止依赖无界触发器静默删除。

## 8. 客观派生指标

以下指标不属于 Core 字段，可由纯函数或有界查询从同一个 snapshot 计算：

```text
spread = ask1.price - bid1.price
mid_price = (ask1.price + bid1.price) / 2
bid_depth_5 = Σ bid1..bid5.volume（仅在五档数量全部非空时）
ask_depth_5 = Σ ask1..ask5.volume（仅在五档数量全部非空时）
imbalance_5 = (bid_depth_5 - ask_depth_5)
              / (bid_depth_5 + ask_depth_5)
```

任一必需输入缺失或分母为零时结果为 `NULL`，不把缺档当零。`imbalance_5` 只描述该
快照的显示深度，不命名为资金流、买卖意愿或交易信号。

## 9. 候选 API

首个稳定接口只查询最新快照：

```text
api_v1.query_latest_five_level_quote(
    p_symbol text,
    p_max_age_seconds integer default 15
)
```

返回一行扁平契约：快照元数据、行情字段、`bid_price_1..5`、`bid_volume_1..5`、
`ask_price_1..5`、`ask_volume_1..5`，以及可空的 `spread`、`mid_price`、
`bid_depth_5`、`ask_depth_5`、`imbalance_5`。

- symbol 必须符合标准格式；
- `p_max_age_seconds` 范围 1～300；
- 没有满足时效的快照返回 SQLSTATE `P0002`；
- statement timeout 为 2 秒；
- `SECURITY INVOKER`，仅授予 `authenticated`，不授予 `anon`；
- 不公开 `source_code`、`ingestion_id`、节点、Cookie 或 Raw 字段；
- 历史查询在出现明确消费者与容量目标后另行设计，禁止先发布无界 View。

PostgREST 是查询已有快照，不在请求线程中调用外部行情源。因此返回“最新已采集快照”，
而不是收到 HTTP 请求后实时抓取上游。若 ADR-0011 的 FastAPI 对外发布该能力，也只能
代理此 RPC 并复用 `X-API-Key`，不能绕过 Persistence 直接请求 Provider。

## 10. 首版采集和容量边界

首版只实现显式单次采集，不进入 `daily-run`。持续采集启用前必须确定：

- 交易时段和闭市行为；
- 采样间隔与允许最大延迟；
- 每批最大证券数和单 Worker 并发；
- 一次 IngestionRun/Raw 对象覆盖的采样窗口；
- PostgreSQL 每日行数预算、索引体积和热数据保留期；
- 网关查询频率和 authenticated 身份预算；
- Worker 重启、半批失败、节点切换和 Raw 重放语义。

没有容量证据时，不默认全市场逐秒采集，也不创建每个快照一个小 Raw 文件的实现。

## 11. 测试矩阵

- 标准 symbol 与 SSE/SZSE 映射；BSE 未验证时明确拒绝；
- pytdx 手到股、价格精度和集合竞价“无价格但有量”语义；
- 正常五档、单侧空档、涨跌停、停牌、闭市；
- 乱序档位、重复 level、负数、price 非空但 volume 为空、交叉盘口；
- observed/source 时间、时区、陈旧和未来时间；
- 同批多证券共享 observed_at、自然键幂等与冲突；
- Provider 超时、节点失败和字段变更；
- Raw 重放产生相同 DTO；
- RLS、authenticated-only、时效上限、P0002 和 statement timeout；
- OpenAPI/Agent 契约不含 Secret 和内部 Schema。

## 12. 实施顺序

```text
GitHub Issue
  → ADR-0012 Accepted
  → 本领域详设转为“有效”
  → Provider/DTO/Validator 单元测试
  → 容量基准与数据授权确认
  → SQL migration + PostgreSQL 集成测试
  → Pipeline/Raw 重放
  → api_v1 RPC + 契约
  → 小范围生产观察
  → 再决定扩大标的和采样频率
```

## 13. 腾讯批量 Provider 与最新批量查询（2026-08-23）

ADR-0044 / Issue #62 新增 `tencent_quote`。该 Adapter 严格解码 GBK，以字段 30 作为
Asia/Shanghai 来源时间，以字段 35 第三段作为 CNY 累计成交额，并把所有“手”转换为“股”。
未验证的下标 29 和尾部字段只保留在 Raw，不进入领域 Record。

显式命令每次接收 1～500 个唯一 SSE/SZSE 标准 symbol，每个腾讯请求最多 50 只，不注册
持续任务。事实追加写入 `realtime.stock_quote_snapshot`；公共读取通过
`api_v1.query_latest_stock_quotes(text[],integer)` 和 FastAPI
`POST /api/v1/realtime-quotes/latest/query`。RPC 同时检查数据中心观察时间与腾讯来源时间，
陈旧行情进入 `missing_codes`，FastAPI 不在请求路径访问 Provider。
