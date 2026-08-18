# 集合竞价一字形态只读接口设计

## 目标

新增 `GET /api/v1/call-auction-one-price-patterns`，从既有沪深全市场竞价序列来源事实中，
实时返回 `09:15:20–09:24:40` 的29个采样点价格完全相同，且相对昨收涨跌幅处于
`[-4%, 4%]` 的股票列表。

本设计实现 GitHub Issue #56，并遵守 ADR-0042。

## 范围与非目标

范围包括 SSE/SZSE 当日冻结 listed-stock 全集，覆盖主板、创业板和科创板。BSE、ETF、
可转债和指数不进入结果。

本变更不新增采集任务、结果表、缓存、分页、Provider 请求、数据库写接口、PostgREST 公共权限
或 Agent Tool。它不修改竞价序列采集语义和历史事实。

## 会话选择

观察窗口固定对应 `sample_seq between 1 and 29`，共29轮：

- 第一轮：`09:15:20 Asia/Shanghai`；
- 最后一轮：`09:24:40 Asia/Shanghai`；
- `sample_seq` 0、30、31 不参与任何判定。

合格会话必须满足：

- session 的 `trade_date` 符合请求日期；
- session 状态为 `succeeded` 或 `partial`；
- sample_seq 1–29 每个 Round 均存在；
- 29个 Round 均为 `succeeded`；
- 每轮 `successful_quotes = expected_quotes`；
- 每轮 `selected_ingestion_id is not null`。

同日多个合格会话按 `started_at desc, session_id desc` 选一个。不传 `trade_date` 时先按
`trade_date desc` 选择最近合格会话；显式日期无合格会话时 RPC 抛出 not-found，FastAPI 返回404。
窗口外 Round 的状态不得影响会话资格。

## 股票形态计算

只读取选定 session、sample_seq 1–29 的 Snapshot，并按 symbol 聚合。一个合格股票同时满足：

1. `count(*) = count(distinct sample_seq) = 29`；
2. 29条 `value_semantics = 'auction_indicative'`；
3. 29条 `last_price`、`previous_close` 均非空且大于零；
4. `min(last_price) = max(last_price)`；
5. `min(previous_close) = max(previous_close)`；
6. 精确 `change_pct = (one_price / previous_close - 1) * 100` 位于闭区间 `[-4, 4]`。

筛选使用未舍入 numeric 值。响应 `change_pct` 使用 PostgreSQL `round(value, 10)`；价格和涨跌幅
继续由 Pydantic 作为 Decimal 字符串输出，不经过 float。

Snapshot 已由 session 冻结全集产生，因此历史查询不再要求 `core.security.status = 'listed'`。
查询只校验 `security_type = 'stock'` 与 exchange 为 SSE/SZSE，并按 trade_date 关联
`core.security_name_history` 获取当日有效名称。缺少名称不删除客观形态，响应 `name` 允许为 null。

## 数据库设计

ordered migration 新增：

- 分区表父表索引 `(trade_date, session_id, symbol, sample_seq)`，支持按日期和会话聚合；
- `api_v1.query_call_auction_one_price_patterns(p_trade_date date default null) returns jsonb`。

RPC 使用 `language plpgsql stable security definer`、固定 search_path 和 `statement_timeout='10s'`。
它返回以下 JSON：

```json
{
  "trade_date": "2026-08-18",
  "session_id": "00000000-0000-0000-0000-000000000000",
  "session_status": "succeeded",
  "window_start": "2026-08-18T09:15:20+08:00",
  "window_end": "2026-08-18T09:24:40+08:00",
  "round_count": 29,
  "candidate_count": 1,
  "items": [
    {
      "symbol": "SSE:600000",
      "code": "600000",
      "name": "浦发银行",
      "exchange": "SSE",
      "one_price": 10.20,
      "previous_close": 10.00,
      "change_pct": 2.0000000000,
      "sample_count": 29
    }
  ]
}
```

结果顺序固定为 `change_pct desc, code asc, symbol asc`。没有形态成员时仍返回会话元数据和空数组。
函数撤销 public、anon、authenticated 权限，只向 `market_data_api` 授权 execute。

## FastAPI 设计

新增模型：

- `CallAuctionOnePricePatternItem`；
- `CallAuctionOnePricePatternResponse`。

新增 QueryRepository/QueryService 方法
`auction_one_price_patterns(trade_date: date | None) -> CallAuctionOnePricePatternResponse`，调用单一 RPC。
数据库 not-found 映射为现有404错误，其他数据库不可用错误保持503映射。

路由：

```text
GET /api/v1/call-auction-one-price-patterns?trade_date=2026-08-18
X-API-Key: <required>
```

`trade_date` 省略时传 SQL null。`/docs` 使用中文 summary、description 和字段注释，明确29轮、时间窗口、
闭区间、日期回退规则及 Decimal 字符串语义。

## 契约与文档

生成并提交更新后的 `contracts/fastapi-openapi-v1.json`。不修改
`contracts/postgrest-openapi-v1.json` 或 `contracts/agent-tools-v1.json`，因为 RPC 是 FastAPI 私有读取边界，
没有新增直接消费者权限。

README/API 运行文档仅在已有接口目录需要同步时增加一条，不扩展运维配置。

## 错误与边界

- API Key 缺失或错误：401；
- 显式日期无合格会话，或省略日期但不存在任何合格会话：404；
- 数据库不可用或 RPC 超时：503；
- 合格会话存在但无形态成员：200，`candidate_count=0`，`items=[]`；
- 响应最多为 session `universe_count`，不接受 limit/offset，避免不同分页看到不同实时聚合状态。

## 测试

PostgreSQL integration tests 覆盖：

- 29轮同价进入结果，排序和10位涨跌幅正确；
- 精确 -4% 与 4% 均进入，越界值排除；
- 缺轮、重复/缺失采样、NULL/零价、变价、昨收变化和错误 `value_semantics` 排除；
- sample_seq 0、30、31 的失败或不同价格不影响结果；
- SSE/SZSE stock 进入，BSE 和非 stock 排除；
- 最新合格会话选择、显式日期不回退和空成员响应；
- RPC grant、statement timeout 与索引存在。

FastAPI tests 覆盖认证、可选日期传递、响应模型、Decimal 字符串、中文 OpenAPI、404/503 映射和 checked-in
OpenAPI 契约同步。实现后运行聚焦测试、Ruff、Mypy、全量 pytest 以及隔离数据库 integration tests。
