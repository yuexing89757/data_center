# 集合竞价一字形态只读接口 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 新增一个 authenticated FastAPI 只读接口，返回沪深上市股票在 09:15:20–09:24:40 的29轮竞价价格完全相同且相对昨收涨跌幅位于 [-4%, 4%] 的列表。

**Architecture:** ordered PostgreSQL migration 提供一个10秒超时的私有 `api_v1` RPC，直接在选定完整窗口 session 上聚合29轮来源事实。FastAPI 只调用该 RPC，用 Pydantic 保持 Decimal、边界和中文 OpenAPI 契约，不新增表、缓存、Provider 调用或数据写入。

**Tech Stack:** Python 3.12、FastAPI、Pydantic v2、SQLAlchemy、PostgreSQL/PLpgSQL、pytest、uv、Ruff、Mypy。

**Spec:** `docs/superpowers/specs/2026-08-18-call-auction-one-price-patterns-design.md`

## Global Constraints

- 固定窗口为 `sample_seq=1..29`，即 `09:15:20–09:24:40 Asia/Shanghai`；0、30、31不参与任何判定。
- 29个 Round 必须全部 succeeded、`successful_quotes=expected_quotes` 且 `selected_ingestion_id` 非空；session 本身允许 succeeded 或 partial。
- 每只股票必须恰有29个不同采样点，全部 `value_semantics='auction_indicative'`，竞价价和昨收均为正且分别完全一致。
- 使用未舍入 Decimal 计算和筛选 `-4 <= (one_price / previous_close - 1) * 100 <= 4`；响应四舍五入10位且不经过 float。
- 使用 session 冻结的 SSE/SZSE listed-stock 集合；包含主板、创业板、科创板，不含 BSE 和非 stock。
- 显式日期不回退；省略日期选择最近合格 session；无成员返回200空列表，无合格 session 返回404。
- FastAPI 只能调用 bounded `api_v1` RPC；RPC 只授权 `market_data_api`，statement timeout 固定10秒。
- 不新增定时任务、环境配置、物化表、分页、网络请求或写入行为。

---

### Task 1: PostgreSQL RPC、索引与数据库契约

**Files:**
- Create: `supabase/migrations/20260818000500_query_call_auction_one_price_patterns.sql`
- Modify: `tests/test_postgres_integration.py:1537`

**Interfaces:**
- Consumes: `realtime.call_auction_market_series_session`、`round`、`snapshot`、`core.security`、`core.security_name_history`。
- Produces: `api_v1.query_call_auction_one_price_patterns(p_trade_date date default null) returns jsonb`。

- [ ] **Step 1: 写失败的集成测试，证明29轮同价、边界和窗口外轮次语义**

在 `tests/test_postgres_integration.py` 增加 helper，使用现有 `database_engine` fixture 创建两个 SSE stock、一个 BSE stock 和一个 ETF；创建 partial session，并用 `generate_series(0,31)` 生成 Round。让 seq 1–29 succeeded，seq 0、30、31 failed；只为 seq 1–29 插入 Snapshot：

```python
def _seed_one_price_pattern_session(connection, *, day: date, session_id: UUID) -> None:
    connection.execute(text("""
        insert into realtime.call_auction_market_series_round (
            session_id, sample_seq, scheduled_at, collected_at, status,
            attempt_count, expected_quotes, successful_quotes, failed_quotes,
            selected_ingestion_id
        )
        select :session_id, seq,
               (:day + time '09:15:00') at time zone 'Asia/Shanghai'
                   + make_interval(secs => seq * 20),
               (:day + time '09:15:00') at time zone 'Asia/Shanghai'
                   + make_interval(secs => seq * 20 + 2),
               case when seq between 1 and 29 then 'succeeded' else 'failed' end,
               1, 1,
               case when seq between 1 and 29 then 1 else 0 end,
               case when seq between 1 and 29 then 0 else 1 end,
               case when seq between 1 and 29 then :ingestion_id else null end
        from generate_series(0, 31) seq
    """), {"session_id": session_id, "day": day, "ingestion_id": INGESTION_ID})
```

测试调用尚不存在的 RPC，并断言：2% 股票进入；精确 -4% 与 4% 进入；4.0001% 排除；BSE/ETF 排除；`round_count=29`；seq 0、30、31 失败不影响结果；排序为 change_pct desc、code asc。

```python
payload = connection.scalar(
    text("select api_v1.query_call_auction_one_price_patterns(:day)"),
    {"day": day},
)
assert payload["round_count"] == 29
assert [item["code"] for item in payload["items"]] == ["600004", "600002", "600001"]
assert payload["items"][1]["change_pct"] == 2
assert all(item["sample_count"] == 29 for item in payload["items"])
```

- [ ] **Step 2: 运行数据库测试并确认 RED**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k one_price_pattern -v`

Expected: FAIL，PostgreSQL 报 `function api_v1.query_call_auction_one_price_patterns(date) does not exist`。

- [ ] **Step 3: 编写最小 migration**

创建父分区索引：

```sql
create index call_auction_market_series_snapshot_session_symbol_idx
    on realtime.call_auction_market_series_snapshot
       (trade_date, session_id, symbol, sample_seq);
```

创建 `stable security definer` RPC，固定 `search_path` 和超时：

```sql
create function api_v1.query_call_auction_one_price_patterns(
    p_trade_date date default null
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, realtime, core
set statement_timeout = '10s'
as $$
declare
    selected_session realtime.call_auction_market_series_session%rowtype;
    payload jsonb;
begin
    select session.* into selected_session
    from realtime.call_auction_market_series_session session
    where session.status in ('succeeded', 'partial')
      and (p_trade_date is null or session.trade_date = p_trade_date)
      and 29 = (
          select count(*)
          from realtime.call_auction_market_series_round round
          where round.session_id = session.session_id
            and round.sample_seq between 1 and 29
            and round.status = 'succeeded'
            and round.successful_quotes = round.expected_quotes
            and round.selected_ingestion_id is not null
      )
    order by session.trade_date desc, session.started_at desc, session.session_id desc
    limit 1;

    if selected_session.session_id is null then
        raise exception 'call-auction one-price pattern session not found'
            using errcode = 'P0002';
    end if;

    with grouped as materialized (
        select snapshot.symbol,
               min(snapshot.last_price) one_price,
               min(snapshot.previous_close) previous_close,
               count(*)::integer sample_count
        from realtime.call_auction_market_series_snapshot snapshot
        where snapshot.trade_date = selected_session.trade_date
          and snapshot.session_id = selected_session.session_id
          and snapshot.sample_seq between 1 and 29
        group by snapshot.symbol
        having count(*) = 29
           and count(distinct snapshot.sample_seq) = 29
           and bool_and(snapshot.value_semantics = 'auction_indicative')
           and bool_and(
               snapshot.last_price is not null
               and snapshot.previous_close is not null
               and snapshot.last_price > 0
               and snapshot.previous_close > 0
           )
           and min(snapshot.last_price) = max(snapshot.last_price)
           and min(snapshot.previous_close) = max(snapshot.previous_close)
    ), calculated as (
        select grouped.*,
               (one_price / previous_close - 1) * 100 exact_change_pct
        from grouped
    ), matched as (
        select calculated.*, security.code, security.exchange,
               name_history.name
        from calculated
        join core.security security using (symbol)
        left join lateral (
            select history.name
            from core.security_name_history history
            where history.symbol = calculated.symbol
              and history.effective_from <= selected_session.trade_date
              and (history.effective_to is null
                   or history.effective_to >= selected_session.trade_date)
            order by history.effective_from desc
            limit 1
        ) name_history on true
        where security.security_type = 'stock'
          and security.exchange in ('SSE', 'SZSE')
          and exact_change_pct between -4 and 4
    )
    select jsonb_build_object(
        'trade_date', selected_session.trade_date,
        'session_id', selected_session.session_id,
        'session_status', selected_session.status,
        'window_start', (selected_session.trade_date + time '09:15:20')
            at time zone 'Asia/Shanghai',
        'window_end', (selected_session.trade_date + time '09:24:40')
            at time zone 'Asia/Shanghai',
        'round_count', 29,
        'candidate_count', count(*),
        'items', coalesce(jsonb_agg(jsonb_build_object(
            'symbol', symbol, 'code', code, 'name', name,
            'exchange', exchange, 'one_price', one_price,
            'previous_close', previous_close,
            'change_pct', round(exact_change_pct, 10),
            'sample_count', sample_count
        ) order by exact_change_pct desc, code, symbol), '[]'::jsonb)
    ) into payload
    from matched;
    return payload;
end $$;

revoke all on function api_v1.query_call_auction_one_price_patterns(date)
    from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_one_price_patterns(date)
    to market_data_api;
```

- [ ] **Step 4: 增加排除和选择测试**

增加参数化数据，使以下每种股票都被排除：缺少 seq29、某轮 NULL/零价、某轮价格变化、某轮昨收变化、某轮 `opening_trade`。另建更晚但窗口缺轮的 session，断言省略日期跳过它并选择最近合格 session；显式查询无合格 session 的日期断言 SQLSTATE `P0002`；合格 session 无成员断言200语义的空 JSON 数组。

```python
@pytest.mark.parametrize(
    ("mutation_sql", "reason"),
    [
        ("delete from realtime.call_auction_market_series_snapshot where sample_seq=29", "missing"),
        ("update realtime.call_auction_market_series_snapshot set last_price=null where sample_seq=7", "null"),
        ("update realtime.call_auction_market_series_snapshot set last_price=10.01 where sample_seq=7", "changed"),
        ("update realtime.call_auction_market_series_snapshot set previous_close=9.99 where sample_seq=7", "close"),
        ("update realtime.call_auction_market_series_snapshot set value_semantics='opening_trade' where sample_seq=7", "semantics"),
    ],
)
def test_one_price_pattern_rejects_incomplete_symbol(database_engine, mutation_sql, reason):
    day = date(2026, 8, 18)
    session_id = UUID("00000000-0000-0000-0000-000000000056")
    symbol = "SSE:600001"
    with database_engine.begin() as connection:
        _seed_one_price_pattern_session(connection, day=day, session_id=session_id)
        connection.execute(
            text(mutation_sql + " and session_id=:session_id and symbol=:symbol"),
            {"session_id": session_id, "symbol": symbol},
        )
        payload = connection.scalar(
            text("select api_v1.query_call_auction_one_price_patterns(:day)"),
            {"day": day},
        )
    assert payload["candidate_count"] == 0, reason
```

实现测试时将注释中的固定 session/symbol 条件写入每条 SQL，不保留省略号；每个参数断言 `candidate_count == 0`。

- [ ] **Step 5: 运行集成测试并确认 GREEN**

Run: `uv run pytest -m integration tests/test_postgres_integration.py -k "one_price_pattern or call_auction_market_series_schema" -v`

Expected: PASS；同时断言 `pg_proc.proconfig` 含 `statement_timeout=10s`、只有 `market_data_api` 有 execute、索引存在。

- [ ] **Step 6: 提交数据库切片**

```bash
git add supabase/migrations/20260818000500_query_call_auction_one_price_patterns.sql tests/test_postgres_integration.py
git commit -m "feat: add auction one-price pattern RPC"
```

### Task 2: FastAPI 模型与查询服务

**Files:**
- Modify: `src/market_data_center/public_api/models.py:375`
- Modify: `src/market_data_center/public_api/queries.py:98`
- Modify: `tests/test_public_api.py:426`

**Interfaces:**
- Consumes: `api_v1.query_call_auction_one_price_patterns(date)` from Task 1。
- Produces: `CallAuctionOnePricePatternItem`、`CallAuctionOnePricePatternResponse` 和 `auction_one_price_patterns(date | None)` service method。

- [ ] **Step 1: 写失败的 QueryService/API 模型测试**

扩展 `FakeQueryService` 记录日期调用，构造以下响应并断言 Decimal 不转 float：

```python
def auction_one_price_patterns(
    self, trade_date: date | None
) -> CallAuctionOnePricePatternResponse:
    self.auction_one_price_pattern_calls.append(trade_date)
    return CallAuctionOnePricePatternResponse(
        trade_date=date(2026, 8, 18),
        session_id=UUID("00000000-0000-0000-0000-000000000056"),
        session_status="partial",
        window_start=datetime(2026, 8, 18, 9, 15, 20, tzinfo=SHANGHAI),
        window_end=datetime(2026, 8, 18, 9, 24, 40, tzinfo=SHANGHAI),
        round_count=29,
        candidate_count=1,
        items=[CallAuctionOnePricePatternItem(
            symbol="SSE:600000", code="600000", name="浦发银行",
            exchange="SSE", one_price=Decimal("10.20"),
            previous_close=Decimal("10.00"), change_pct=Decimal("2.0000000000"),
            sample_count=29,
        )],
    )
```

Repository 测试使用 fake `_execute`，断言 SQL 常量只调用
`query_call_auction_one_price_patterns(p_trade_date => :trade_date)`，payload 交给 Pydantic 验证。

- [ ] **Step 2: 运行测试并确认 RED**

Run: `uv run pytest tests/test_public_api.py -k one_price_pattern -v`

Expected: FAIL，模型和 service 方法尚不存在。

- [ ] **Step 3: 实现模型与查询服务**

在 `models.py` 增加：

```python
class CallAuctionOnePricePatternItem(ApiModel):
    symbol: str
    code: SixDigitCode
    name: str | None
    exchange: Literal["SSE", "SZSE"]
    one_price: Decimal = Field(gt=0)
    previous_close: Decimal = Field(gt=0)
    change_pct: Decimal = Field(ge=Decimal("-4"), le=Decimal("4"))
    sample_count: Literal[29]


class CallAuctionOnePricePatternResponse(ApiModel):
    trade_date: date
    session_id: UUID
    session_status: Literal["succeeded", "partial"]
    window_start: datetime
    window_end: datetime
    round_count: Literal[29]
    candidate_count: int = Field(ge=0)
    items: list[CallAuctionOnePricePatternItem]
```

在 `queries.py` 增加 SQL 常量、Protocol 方法和 PostgreSQL 实现：

```python
QUERY_AUCTION_ONE_PRICE_PATTERNS = text("""
select api_v1.query_call_auction_one_price_patterns(
    p_trade_date => :trade_date
) as payload
""")

def auction_one_price_patterns(
    self, trade_date: date | None
) -> CallAuctionOnePricePatternResponse:
    rows = self._execute(QUERY_AUCTION_ONE_PRICE_PATTERNS, {"trade_date": trade_date})
    if not rows or rows[0]["payload"] is None:
        raise PublicQueryNotFound("call-auction one-price pattern session was not found")
    return CallAuctionOnePricePatternResponse.model_validate(rows[0]["payload"])
```

- [ ] **Step 4: 运行聚焦测试并确认 GREEN**

Run: `uv run pytest tests/test_public_api.py -k one_price_pattern -v`

Expected: PASS，包括 `trade_date=None` 原样传入、显式日期原样传入和非法 Decimal/轮次数被模型拒绝。

- [ ] **Step 5: 提交查询服务切片**

```bash
git add src/market_data_center/public_api/models.py src/market_data_center/public_api/queries.py tests/test_public_api.py
git commit -m "feat: model auction one-price patterns"
```

### Task 3: FastAPI 路由、中文 OpenAPI 与发布契约

**Files:**
- Modify: `src/market_data_center/public_api/app.py:431`
- Modify: `scripts/check_fastapi_release.py:15`
- Modify: `tests/test_public_api.py:1256`
- Modify: `tests/test_production_checks.py:80`
- Modify: `tests/test_api_contracts.py`
- Modify: `contracts/fastapi-openapi-v1.json`

**Interfaces:**
- Consumes: `PublicQueryService.auction_one_price_patterns(date | None)` from Task 2。
- Produces: authenticated `GET /api/v1/call-auction-one-price-patterns` 和 checked-in OpenAPI v1 contract。

- [ ] **Step 1: 写失败的路由与 OpenAPI 测试**

```python
def test_call_auction_one_price_patterns_uses_optional_date(client, service) -> None:
    response = client.get(
        "/api/v1/call-auction-one-price-patterns",
        params={"trade_date": "2026-08-18"},
        headers={"X-API-Key": "test-key"},
    )
    assert response.status_code == 200
    assert service.auction_one_price_pattern_calls == [date(2026, 8, 18)]
    assert response.json()["items"][0]["one_price"] == "10.20"
    assert response.json()["items"][0]["change_pct"] == "2.0000000000"


def test_call_auction_one_price_patterns_openapi_is_chinese(app) -> None:
    operation = app.openapi()["paths"]["/api/v1/call-auction-one-price-patterns"]["get"]
    assert "集合竞价" in operation["summary"]
    assert "09:15:20" in operation["description"]
    assert "09:24:40" in operation["description"]
    assert "29" in operation["description"]
    assert "不回退" in operation["description"]
```

复用现有认证测试方式，断言缺失/错误 API key 为401；让 fake service 抛
`PublicQueryNotFound`/`PublicQueryUnavailable`，断言404/503。

- [ ] **Step 2: 运行测试并确认 RED**

Run: `uv run pytest tests/test_public_api.py -k one_price_pattern -v`

Expected: FAIL，路由为404且 OpenAPI path 不存在。

- [ ] **Step 3: 实现路由和发布前检查**

在 `app.py` 增加：

```python
@app.get(
    "/api/v1/call-auction-one-price-patterns",
    response_model=CallAuctionOnePricePatternResponse,
    responses={401: {"model": ErrorResponse}, 404: {"model": ErrorResponse},
               503: {"model": ErrorResponse}},
    tags=["市场数据"],
    summary="查询集合竞价29轮同价形态股票",
    description=(
        "读取沪深上市股票在 09:15:20–09:24:40 的29轮集合竞价序列事实。"
        "仅返回29轮价格完全相同、相对昨收精确涨跌幅位于闭区间 [-4%, 4%] 的股票。"
        "显式交易日无完整窗口时返回404且不回退；省略日期时选择最近完整窗口。"
    ),
)
def auction_one_price_patterns(
    _: ApiKeyDependency,
    service: QueryServiceDependency,
    trade_date: Annotated[date | None, Query()] = None,
) -> CallAuctionOnePricePatternResponse:
    return service.auction_one_price_patterns(trade_date)
```

把 `api_v1.query_call_auction_one_price_patterns(date)` 加入
`scripts/check_fastapi_release.py` 的 `PUBLISHED_FUNCTIONS`，并增加 production check 测试。

- [ ] **Step 4: 生成并验证 OpenAPI 契约**

使用项目现有 contract 生成方式从 `create_app()` 的 `app.openapi()` 写出
`contracts/fastapi-openapi-v1.json`；不得手工编辑生成 JSON。运行：

```bash
uv run pytest tests/test_api_contracts.py tests/test_public_api.py -k "contract or one_price_pattern" -v
```

Expected: PASS；`contracts/postgrest-openapi-v1.json` 和 `contracts/agent-tools-v1.json` 无 diff。

- [ ] **Step 5: 提交公共接口切片**

```bash
git add src/market_data_center/public_api/app.py scripts/check_fastapi_release.py \
  tests/test_public_api.py tests/test_production_checks.py tests/test_api_contracts.py \
  contracts/fastapi-openapi-v1.json
git commit -m "feat: expose auction one-price pattern API"
```

### Task 4: 完整验证与交付准备

**Files:**
- Modify only if verification exposes a defect in Task 1–3 files.

**Interfaces:**
- Consumes: 完整 RPC、FastAPI 路由和 checked-in contract。
- Produces: 可合并、可迁移、可部署的验证证据。

- [ ] **Step 1: 运行格式、Lint 和类型门禁**

```bash
uv run ruff format --check .
uv run ruff check .
uv run mypy src
```

Expected: 三条命令 exit 0，无错误。

- [ ] **Step 2: 运行完整单元测试**

Run: `uv run pytest`

Expected: exit 0，无 failed/error。

- [ ] **Step 3: 运行隔离 PostgreSQL integration tests**

先确认 `TEST_DATABASE_URL` 指向可丢弃测试库且不是生产端口25432，再运行：

```bash
uv run pytest -m integration
```

Expected: exit 0；不得以生产数据库替代。

- [ ] **Step 4: 检查 diff、migration 顺序和契约范围**

```bash
git diff --check
git status --short
git diff --name-only 6f4fd95..HEAD
```

Expected: 只出现计划列出的 migration、FastAPI、测试、contract 文件；不出现 `.env`、Raw、凭据、
`contracts/postgrest-openapi-v1.json` 或 `contracts/agent-tools-v1.json`。

- [ ] **Step 5: 处理验证结果**

若任一门禁失败，返回产生该文件的 Task 1、2 或3，补写一个能够复现失败的测试，按该 Task 的 RED/GREEN
步骤修复，并使用该 Task 已列出的精确 `git add` 和提交命令。若没有新改动，不创建空提交。完成后按
`superpowers:finishing-a-development-branch` 选择合并/推送方式；生产 migration 和部署必须等待用户显式授权。
