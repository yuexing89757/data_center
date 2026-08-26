# 领域详设：ShareholderCount

- 状态：已接受，待实现
- 日期：2026-08-24
- ADR：ADR-0047
- Issue：#66

## 1. 边界与依赖

`ShareholderCount` 只表达某标准证券在统计截止日披露的股东户数。它依赖 Security 和
Ingestion，不依赖 Trading Calendar、Market、Capital 或 StockDailyIndicator。

标准事实首期只覆盖 `SSE` 和 `SZSE` 股票。来源中的 `BSE` 行保留在 Raw，以
`shareholder_count.unsupported_exchange` 记录过滤数量，不进入 Core。

```text
Ingestion ───────────────► ShareholderCount
Security ────────────────► ShareholderCount
ShareholderCount + as_of ─► 查询时变化指标
```

首版不包含十大股东、股东增减持、户均持股、筹码集中度或主观标签。

## 2. 领域记录

```python
@dataclass(frozen=True, slots=True)
class ShareholderCountRecord:
    symbol: str
    statistics_date: date
    announcement_date: date
    shareholder_count: int
    revision_key: str
    source_code: str
```

标准记录不包含 `ingestion_id`。Pipeline 通过 `IngestionEnvelope` 附加采集血缘，
Persistence 首次插入时生成 `first_observed_at`。

修订键是以下 UTF-8 字符串以 `\x1f` 连接后的 SHA-256：

```text
symbol
statistics_date.isoformat()
announcement_date.isoformat()
str(shareholder_count)
```

不变量：

- symbol 必须存在于 `core.security`；
- `announcement_date >= statistics_date - 2 days`；公告日早 1 至 2 个自然日允许，早 3 个自然日
  及以上硬失败；
- `shareholder_count > 0`；
- 首期 `source_code = 'tushare'`；
- 修订键必须重新计算一致；
- 批内 `(symbol, statistics_date, revision_key)` 唯一。

## 3. Provider 与 Raw

Tushare `stk_holdernumber` 映射如下：

| 来源字段 | 标准字段 |
| --- | --- |
| `ts_code` | `symbol` |
| `ann_date` | `announcement_date` |
| `end_date` | `statistics_date` |
| `holder_num` | `shareholder_count` |

非空 `holder_num` 只能由十进制整数字符串直接构造 Python `int`，不得经过 `float`。字段存在
但值为 NULL、空字符串或纯空白时，Adapter 保留原始行但不生成标准记录；Pipeline 按
`shareholder_count.missing_source_value` 登记聚合质量拒绝和拒绝行数。字段本身缺失以及非空的
零、负数、非整数字符串仍硬失败。Raw schema 固定为 `tushare.shareholder_count.v1`。每个实际
来源请求各保存一份 JSONL Raw、SHA-256、请求参数和 manifest；Token 不属于请求参数。

单次响应上限为 3000 行。状态机为：

```text
少于 3000 行             -> 接受
等于 3000 行且范围多于一天 -> 按公告日期拆成两个闭区间
全市场单日等于 3000 行     -> 按标准 symbol 升序逐证券查询
单证券单日等于 3000 行     -> 硬失败
```

合法空响应成功并产生零记录。权限、限流、字段缺失、非法类型和无法证明完整性都抛出
可诊断但不含秘密的 ProviderError。

## 4. 双时间与变化语义

`announcement_date` 是来源披露日期，`first_observed_at` 是数据中心首次观察时间。严格查询
只允许：

```text
announcement_date <= p_as_of_date
first_observed_at < (p_as_of_date + 1 day) 00:00 Asia/Shanghai
```

同一 `(symbol, statistics_date)` 的可见修订按 `announcement_date desc`、
`first_observed_at desc`、`revision_key desc` 选择一条。历史回填保留真实观察时间，不回写为
公告日。current-known 查询可使用当前最新修订，但不得作为严格无未来数据回测接口。

每个查询先选择修订，再按 `statistics_date` 排序计算：

```text
change_count = shareholder_count - previous_shareholder_count
change_ratio = change_count / previous_shareholder_count
```

首条记录的 previous/change 字段为 NULL。`change_ratio` 是 numeric/Decimal 小数比率。

## 5. 采集应用服务

每日同步以执行日及此前 29 个自然日为公告日期窗口。每个真实请求分别创建 IngestionRun 和
Raw manifest；所有切片准备完成并完成整批自然键校验后，在一个数据库事务中发布。切片失败时，
已经准备的运行统一失败、登记 Raw/质量结果但不写 Core。

来源空值行不属于切片失败：合法记录、Raw manifest、质量拒绝和 IngestionRun 在同一事务提交。
同时存在合法记录和空值时请求状态为 `partial`；全部来源行均为空时请求状态为 `failed`，但历史
回填按证券隔离，继续处理下一目标。汇总和 Operations 同步累计 fetched、accepted、rejected。

历史回填由显式 CLI 启动，按 symbol 升序处理沪深所有 stock 状态。起始日为 `ipo_date`，
缺失时使用 1990-12-19；截止日必须显式指定且不晚于当前上海自然日。每只证券单独原子提交，
支持显式 symbol 子集和 `resume-after-symbol`。回填及全历史重新核验都不注册定时 Job。

接口独立限速配置为 `TUSHARE_SHAREHOLDER_COUNT_MAX_CALLS_PER_MINUTE`，范围 1–200，默认 180。
每日 Job 为 `shareholder-count-daily`，每天 21:00 Asia/Shanghai，开关
`SHAREHOLDER_COUNT_DAILY_ENABLED` 默认 false。所有调度只在 Worker 进程内。

## 6. 存储

`core.shareholder_count` 字段：

```text
symbol text
statistics_date date
announcement_date date
shareholder_count bigint
revision_key text
source_code text
ingestion_id uuid
first_observed_at timestamptz
```

主键为 `(symbol, statistics_date, revision_key)`。表启用 RLS；Worker 只有 SELECT/INSERT，
持久化使用 `ON CONFLICT DO NOTHING`，不提供更新、删除或覆盖路径。索引为：

```text
(symbol, statistics_date, announcement_date desc, first_observed_at desc)
(announcement_date)
(ingestion_id)
```

## 7. 公共查询

- `api_v1.query_shareholder_counts_as_of`：可选最多 500 个 symbols，返回严格知识时点下每个
  symbol 的最新统计记录。
- `api_v1.query_shareholder_count_history_as_of`：返回单证券、统计日期范围内的严格时点序列。
- `api_v1.query_shareholder_count_history_latest`：返回单证券当前已知修订序列。

共同返回：

```text
symbol
statistics_date
announcement_date
shareholder_count
previous_statistics_date
previous_shareholder_count
change_count
change_ratio
```

history RPC 先应用请求统计日期范围再计算 lag，因此范围首条不读取隐藏前值。RPC 使用
`plpgsql stable security definer`，只用 RAISE 执行参数门禁；返回查询只读、固定 search_path、
5 秒超时、最终上限 2000 行。只通过 `api_v1` 授权，不公开 Core 或内部血缘字段。

## 8. 质量与测试

测试覆盖领域不变量、SH/SZ/BJ 映射、严格整数、来源空值 Raw 保留与质量拒绝、空响应、
3000 行拆分与最终失败、Raw 重放、
每日整批原子性、历史单证券恢复、Scheduler/Operations、数据库约束/RLS/授权、双时间查询、
变化计算、契约同步和备份恢复行数/孤儿血缘检查。

实施不执行生产 migration、生产回填、真实采集或凭据变更。PostgreSQL integration 只使用隔离的
`TEST_DATABASE_URL`。
