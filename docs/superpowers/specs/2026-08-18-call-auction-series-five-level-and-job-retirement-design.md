# 沪深竞价序列五档扩展与涨停池采集任务退役设计

## 1. 背景

`call_auction_market_series_snapshot` 当前只保存价格、量额等聚合来源事实，没有保存
PYTDX `get_security_quotes` 已返回的完整买卖五档。生产 Raw 证明集合竞价期间可能出现
档位价格为零、档位数量大于零的组合；现有 `OrderBookLevel` 和 PYTDX Adapter 把零价
规范化为缺失后，也把真实来源数量一并丢弃。

与此同时，`opening-auction-limit-up-quotes` 已不再需要。该任务及其专用
`PysnowballQuoteProvider` 应退出运行时代码，但历史数据库事实、Raw、迁移和工作流编码
必须保留，以维持可追溯性和历史查询兼容性。

## 2. 目标

1. 沪深全市场开盘竞价序列快照保存买一至买五、卖一至卖五的价格和数量。
2. 来源给出零价格、正数量时，标准事实保存为 `price = NULL`、`volume = 实际股数`。
3. 每条序列快照保存由计划执行时间确定的六位批次编码，如 `091500`、`091520`。
4. 现有序列查询 API 返回批次编码和完整五档，轮次继续按计划时间正序。
5. 从 Worker 运行时彻底移除集合竞价涨停池五档采集任务和 pysnowball 依赖面。
6. 不重放历史 Raw，不删除或改写历史来源事实。

## 3. 非目标

- 不改变序列任务的 09:15:00–09:25:20 窗口、20 秒节奏、32 个轮次或每批 80 只配置。
- 不改变沪深股票全集冻结、节点选择、失败重试和部分成功语义。
- 不为五档新增子表或 JSONB 载荷。
- 不补采或推算历史五档。
- 不删除历史 `auction_collection` 工作流编码、表、迁移、Raw 或已保存记录。
- 不改变收盘五档快照和 09:26 全市场竞价快照的调度。

## 4. 数据语义

### 4.1 批次编码

新增 `batch_code char(6)`。它由该轮 `scheduled_at` 转换到 `Asia/Shanghai` 后按
`HH24MISS` 生成，而不是使用逐只股票的 `observed_at`。因此同一轮全市场记录共享一个
确定批次：

| sample_seq | scheduled_at（上海） | batch_code |
| ---: | --- | --- |
| 0 | 09:15:00 | `091500` |
| 1 | 09:15:20 | `091520` |
| 2 | 09:15:40 | `091540` |
| 31 | 09:25:20 | `092520` |

领域层提供单一纯函数从 `scheduled_at` 生成批次编码，并由
`MarketSeriesSnapshotRecord` 校验编码、交易日和 `sample_seq` 一致。数据库用正则和
`scheduled_at` 对等约束防止错误编码进入事实表。

### 4.2 五档价量

序列快照增加以下 20 列：

- `bid1_price`/`bid1_volume` 至 `bid5_price`/`bid5_volume`；
- `ask1_price`/`ask1_volume` 至 `ask5_price`/`ask5_volume`。

价格继续使用 `Decimal`/PostgreSQL `numeric(18,4)`；数量在 PYTDX Adapter 边界从手
乘以 100 转为股，并使用 `bigint`。

价量状态只有以下三种合法形式：

| 来源状态 | 标准价格 | 标准数量 |
| --- | --- | --- |
| 正价格、非负数量 | 正价格 | 实际数量 |
| 零价格、正数量 | `NULL` | 实际数量 |
| 零价格、零数量 | `NULL` | `NULL` |

价格存在时数量必须存在；数量可在价格缺失时独立存在，以忠实表达集合竞价来源事实。
不得把零价格伪造为有效价格，也不得复制买一价或其他档位价格。买卖档价格顺序校验只
比较实际存在的正价格，仍拒绝有价格的非连续盘口。

`MarketSeriesSnapshotRecord` 使用固定五个 `OrderBookLevel` 的买卖元组承载领域值，
持久化边界再展开为扁平列。现有竞价指示价格、累计量额和值语义保持不变。

## 5. 数据库迁移

新增一个有序生产迁移，对分区父表
`realtime.call_auction_market_series_snapshot` 执行以下变更，并由 PostgreSQL 传播到
现有分区：

1. 添加可空 `batch_code char(6)` 和 20 个可空五档列。
2. 使用历史 `scheduled_at AT TIME ZONE 'Asia/Shanghai'` 回填 `batch_code`。
3. 将 `batch_code` 改为 `NOT NULL`。
4. 添加批次格式、批次与计划时间一致、价格正数、数量非负以及“价格存在则数量存在”
   的约束，并显式验证约束。

历史五档列保持 `NULL`。迁移不读取 Worker 文件系统 Raw，也不对历史快照作推算。迁移
完成后更新生产表盘点测试；不创建新表、不更改主键和分区范围。

## 6. 采集与持久化流程

`run_call_auction_market_series_job()` 继续从 PYTDX 节点池选择 quote 节点，并使用
`PytdxHqProvider` 的 `get_security_quotes` 协议，每批 80 只。

PYTDX Adapter 调整档位规范化：始终读取并验证来源数量；价格为零且数量为正时生成
`OrderBookLevel(price=None, volume=shares)`，价格和数量均为零时生成空档。正常正价
档位行为不变。

`CallAuctionMarketSeriesService` 将 Provider 的完整买卖五档复制到
`MarketSeriesSnapshotRecord`，并从当前轮 `scheduled_at` 生成 `batch_code`。Raw 写入、
质量检查、选定 ingestion、轮次终态和会话终态流程不变。

`PostgreSQLCallAuctionMarketSeriesPersistence` 在同一个事务中把批次编码和扁平五档列
与现有事实一并写入。任何领域或数据库约束失败都使该 ingestion 显式失败，不降级成
静默丢字段。

## 7. 查询与公开契约

`api_v1.query_call_auction_market_series_snapshots(date, text[])` 保持现有参数、边界、
会话选择规则和 5 秒 statement timeout。每个 `items` 元素新增：

- `batch_code`；
- 买一至买五、卖一至卖五的价格和数量字段。

轮次仍按 `sample_seq` 正序，轮内股票仍按六位代码和标准 symbol 排序。历史记录返回
已回填的 `batch_code` 和值为 `NULL` 的五档字段。

同步更新 FastAPI Pydantic 模型、`contracts/fastapi-openapi-v1.json`、PostgREST/Agent
契约中受影响的说明或 Schema，以及 API 单元和 PostgreSQL 集成测试。六位代码请求、
500 只上限、部分会话读取规则均保持不变。

## 8. 退役集合竞价涨停池五档任务

从运行时代码移除：

- `opening-auction-limit-up-quotes` 的 Worker job definition 和 APScheduler 注册；
- `run_auction_collection_job()` 执行入口及其 scheduler import；
- `PysnowballQuoteProvider` 与网络客户端；
- `PysnowballSettings`、`.env.example` 中的 `PYSNOWBALL_TOKEN`；
- 只验证上述 Provider 或任务注册的测试。

保留：

- `auction_collection` 历史工作流编码；
- `realtime.auction_collection_session`、round、五档快照和派生指标表；
- 所有既有迁移、历史 Raw 和生产数据；
- 历史查询 RPC 及读取权限；
- 陈旧历史会话恢复所需的最小持久化能力。

Worker 目录同步时不再持久化该 job，任务管理页不再显示它。生产部署时从
`/home/project/.env` 安全移除 `PYSNOWBALL_TOKEN`，不得在日志或命令输出中打印值。

## 9. 失败处理与兼容性

- 新代码上线前必须先应用数据库迁移，避免新 Worker 写入不存在的列。
- 迁移回填只更新批次编码，不修改价格、数量、来源或 lineage。
- 旧 API 客户端会忽略新增 JSON 字段；现有字段和嵌套结构不删除。
- 历史五档为空是明确缺口，不从未来或其他来源填充。
- 删除调度任务不删除历史事实；仍处于 running 的历史 session 由现有恢复流程终态化。
- PYTDX 零价正数量是来源事实，不触发整行拒绝；非法负量、非 Decimal 价格或错误标识
  仍按现有质量规则失败。

## 10. 测试与验收

### 单元测试

- `OrderBookLevel` 接受 `price=None, volume>0`，拒绝 `price>0, volume=None` 和负量。
- PYTDX 批量响应的零价正买二/卖二量按手乘 100 后保留。
- 零价零数量仍映射为空档；正价五档行为不变。
- `batch_code` 对 32 个计划轮次生成正确且唯一的六位值。
- 序列 Service 将完整五档和批次编码传到领域记录。
- Worker job catalog、调度注册和管理页不再包含涨停池采集任务。

### PostgreSQL 集成测试

- 迁移对父表和所有现有分区增加列，并回填历史批次编码。
- 五档写入和读取保留 `NULL` 价格与正数量。
- 数据库拒绝错误批次、负数量以及有价格无数量。
- RPC 对新旧记录都返回完整稳定结构，并保持轮次正序。

### API 与契约测试

- FastAPI 返回 `batch_code` 和 20 个五档字段，Decimal 仍按现有 JSON 规则编码。
- 六位代码校验、500 只上限、无成功/部分会话的错误映射保持不变。
- 检入的 OpenAPI 和 Agent 契约与运行时一致。

### 本地发布门禁

实施完成后运行：

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

PostgreSQL 集成测试只允许使用显式提供的隔离 `TEST_DATABASE_URL`；不得指向生产库。

## 11. 生产发布验收

经用户另行明确授权后，发布顺序为：应用迁移、切换 Worker/API 发布目录、重启服务、
运行只读冒烟检查。验收项包括：

1. Worker 和 API 服务持续 active；
2. 任务页不存在 `opening-auction-limit-up-quotes`；
3. 序列任务仍显示 09:15、20 秒、32 轮；
4. 小范围 API 查询返回批次编码和完整五档；
5. 零价正数量以 `price=null, volume>0` 返回；
6. 迁移历史包含本次版本，数据盘点和孤儿事实检查通过；
7. 生产环境不再配置 `PYSNOWBALL_TOKEN`。
