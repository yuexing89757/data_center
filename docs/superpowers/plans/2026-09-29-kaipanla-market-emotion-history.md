# Kaipanla Market Emotion History Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose four bounded, authenticated, request-time Kaipanla historical stock lists—auction, limit-up, limit-down and broken-limit-up—for an exact past trading date.

**Architecture:** One new provider fixes the verified historical HTTP action and category-to-parameter mapping, validates source arrays and returns category-specific immutable records. Four FastAPI operations share provider injection and pagination while giving each list its own named response model. No database, Raw, Worker, migration or existing auction-pool behavior changes.

**Tech Stack:** Python 3.12, urllib, Decimal, dataclasses, FastAPI/Pydantic, pytest, ruff, mypy.

**Spec:** `docs/superpowers/specs/2026-09-29-kaipanla-market-emotion-history-design.md`

## Global Constraints

- Develop on `master` per AGENTS.md; preserve current unrelated uncommitted Kaipanla-board files and hunks.
- Fixed upstream POST: `https://apphis.kaipanla.com/w1/api/index.php`, `c=HisHomeDingPan`, `a=HisDaBanList`, `PidType/Type/Order` = auction 8/18/1, limit-up 1/6/1, limit-down 3/6/1, broken-limit-up 2/4/1.
- Each operation requires `trade_date` strictly before the current Shanghai date; `offset` 0–10000; `limit` 1–30, default 30; one upstream page, no retry/fallback.
- Fixed `Is_st=1` and other filter/version fields from the spec; no caller-controlled upstream URL, category number or sort expression.
- Reuse `FASTAPI_KAIPANLA_TIMEOUT_SECONDS` 1–10 seconds; 2 MB maximum, no redirects/system proxy, no credentials; API Key required.
- Decimal for all market values; `ApiTimestamp` for every event/observation datetime; nullable fields stay null, numeric zero stays zero.
- Do not save source responses, Raw facts or account data in the repository; contract/docs may contain bounded anonymized examples.

## File Map

- Create `src/market_data_center/domain/kaipanla_market_history.py`: four category-specific immutable item types, a typed page, and a list-kind literal.
- Create `src/market_data_center/providers/kaipanla_market_history.py`: fixed request, response/date validation, category row mapping, typed source errors.
- Create `src/market_data_center/public_api/kaipanla_market_history.py`: four routes and response models, no source parsing.
- Modify `src/market_data_center/public_api/app.py`: inject/register the provider/router and install 422/502 handlers; touch only this feature's hunks.
- Create `tests/test_kaipanla_market_history.py`: mocked transport + FastAPI contract regression.
- Modify `contracts/fastapi-openapi-v1.json`, `docs/FastAPI外部接口.md`, `docs/adr/ADR-0059-开盘啦异动提醒实时只读接口.md`; create `docs/开盘啦市场情绪历史股票列表实时接口.md`.

## Review Focus

1. A current/future or malformed calendar date must return 422 before any upstream request—Task 1 and 2 tests.
2. A source page with `errcode=0` but absent/mismatched `day`, an HTML placeholder or duplicate code must return 502, not an empty success—Task 1 tests.
3. Invalid Unix seconds (including Boolean, zero and a time outside the requested Shanghai date) must return 502; valid seconds render an actual calendar-valid Shanghai timestamp—Task 1 and 2 tests.
4. Decimal input `NaN`, negative sealed amount or a stringified fractional amount must not silently become float or zero—Task 1 tests.
5. A 2 MB overflow, redirect/transport failure, or source returning more than the requested `limit` must return 502 without exposing the source body—Task 1 and 2 tests.

---

### Task 1: Typed historical provider and source validation

**Files:**
- Create: `src/market_data_center/domain/kaipanla_market_history.py`
- Create: `src/market_data_center/providers/kaipanla_market_history.py`
- Test: `tests/test_kaipanla_market_history.py`

**Interfaces:**
- Produces `HistoryListKind = Literal["auction", "limit_up", "limit_down", "broken_limit_up"]`.
- Produces `KaipanlaMarketHistoryProvider.fetch(*, kind: HistoryListKind, trade_date: date, offset: int = 0, limit: int = 30) -> MarketHistoryPage`.
- Produces `KaipanlaMarketHistoryInvalid(ValueError)` and `KaipanlaMarketHistoryUpstream(RuntimeError)`; Task 2 uses these exact names.
- `MarketHistoryPage` has `source_code`, `list_type`, `persisted`, `requested_date`, `trade_date`, `observed_at`, `offset`, `limit`, `returned_count`, `total`, `next_offset`, `items`.

- [ ] **Step 1: Write the failing provider tests.** In `tests/test_kaipanla_market_history.py`, build a fake `request_bytes(Request, float) -> bytes` transport recording requests. Use `NOW=datetime(2026, 9, 28, 17, tzinfo=UTC)` and a 34-column fixture whose category-relevant cells are set:

```python
@pytest.mark.parametrize(
    ("kind", "pid_type", "sort_type"),
    [("auction", "8", "18"), ("limit_up", "1", "6"),
     ("limit_down", "3", "6"), ("broken_limit_up", "2", "4")],
)
def test_fixed_request(kind, pid_type, sort_type):
    transport = FakeTransport({"errcode": "0", "day": "2026-09-28", "list": [row(kind)]})
    result = provider(transport).fetch(kind=kind, trade_date=date(2026, 9, 28))
    request, timeout = transport.calls[0]
    params = parse_qs(request.data.decode("ascii"))
    assert request.method == "POST"
    assert urlsplit(request.full_url).hostname == "apphis.kaipanla.com"
    assert (params["PidType"], params["Type"], params["Order"]) == (
        [pid_type], [sort_type], ["1"]
    )
    assert params["Day"] == ["2026-09-28"]
    assert params["Is_st"] == ["1"]
    assert not {"Token", "UserID", "DeviceID"} & params.keys()
    assert result.list_type == kind and 0 < timeout <= 10
```

Add per-kind assertions: auction row[18]=`"2404248.123456789"`, row[19]=`"10.0567"`; limit-up row[6]=1790578605, row[9]=`"首板"`, row[16]=`"海峡两岸"`; broken row[4]=`"17.06"`, row[6]=1790559894, row[7]=1790559912; limit-down row[6]=1790578293, row[8]=`"2404248"`, row[11]=`"光模块"`. Assert the returned immutable item has only its category's fields and exact Decimal/datetime values.

- [ ] **Step 2: Run the focused tests and verify red.** Run `uv run pytest tests/test_kaipanla_market_history.py -q`; expected import failure for the missing provider/domain, not unrelated environment failure.

- [ ] **Step 3: Implement the minimum domain and provider.** Define four frozen item dataclasses with exact field sets: `AuctionHistoryItem(symbol, code, name, board_name, limit_up_bid_amount_cny, auction_change_pct)`; `LimitUpHistoryItem(symbol, code, name, limit_up_at, status, reason)`; `LimitDownHistoryItem(symbol, code, name, limit_down_at, sealed_amount_cny, board_name)`; `BrokenLimitUpHistoryItem(symbol, code, name, change_pct, limit_up_at, opened_at)`. Use `str | None` for nullable names/text, `Decimal | None` for nullable numeric values, and `datetime | None` for nullable events. Define a frozen `MarketHistoryPage` with `items: tuple[AuctionHistoryItem | LimitUpHistoryItem | LimitDownHistoryItem | BrokenLimitUpHistoryItem, ...]`. In the provider, use a fixed `KIND_PARAMS` mapping and the same bounded `urllib` pattern as `providers/kaipanla_auction.py`:

```python
KIND_PARAMS: dict[HistoryListKind, tuple[str, str]] = {
    "auction": ("8", "18"),
    "limit_up": ("1", "6"),
    "limit_down": ("3", "6"),
    "broken_limit_up": ("2", "4"),
}
# In fetch(): verify trade_date < clock().astimezone(SHANGHAI).date();
# send one POST with Day, Index, st, fixed filters and KIND_PARAMS[kind];
# json.loads(body.decode("utf-8-sig"), parse_float=Decimal,
#            parse_constant=_invalid_constant);
# require errcode == 0, date.fromisoformat(day) == trade_date,
# list is a list with len <= limit and unique six-digit codes.
```

Convert non-null Unix seconds via `datetime.fromtimestamp(seconds, SHANGHAI)`; reject bool/non-integer/zero or a converted date different from `trade_date`. Parse money/percent using `Decimal`, require finite and nonnegative sealed/bid amounts, preserve null placeholders and numeric zero. Map indexes exactly from the spec; do not expose the remaining source array. Derive `SSE:`, `SZSE:`, `BSE:` symbols as `providers/kaipanla_auction.py` does and reject unsupported prefixes. Catch only source/parse/transport errors into `KaipanlaMarketHistoryUpstream`, never leak body or URL. Do not catch `KaipanlaMarketHistoryInvalid` as a source error.

- [ ] **Step 4: Add adversarial tests and make them green.** Parametrize malformed/mismatched `day`, failed errcode, placeholder row, duplicate code, over-limit page, `NaN`, negative amount, invalid Unix seconds, current/future date, paging bounds and a byte body larger than 2 MB. Test all three exchange-prefix mappings and an unsupported prefix. For a valid empty page assert `items == ()`; for 30 rows assert `next_offset == 30`; for 11 rows assert `next_offset is None`. Run `uv run pytest tests/test_kaipanla_market_history.py tests/test_kaipanla_auction.py -q`; expected PASS.

- [ ] **Step 5: Commit only Task 1 paths.** Inspect `git status --short` and `git diff --check`, then stage only the two new source files and new test file; commit `feat: add typed Kaipanla history provider`. Do not stage unrelated board files.

### Task 2: Four authenticated FastAPI routes and checked-in contract

**Files:**
- Create: `src/market_data_center/public_api/kaipanla_market_history.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `tests/test_kaipanla_market_history.py`
- Modify: `contracts/fastapi-openapi-v1.json`

**Interfaces:**
- Consumes `KaipanlaMarketHistoryProvider.fetch` and the two exceptions from Task 1.
- Produces GET `/api/v1/realtime/kaipanla/market-emotion/stocks/{auction|limit-up|limit-down|broken-limit-up}` as four static operations (not an unrestricted dynamic category parameter).

- [ ] **Step 1: Write failing route tests.** Inject the fake provider through a new optional `create_app(..., kaipanla_market_history_provider=provider)` parameter while other services are stubs, as `tests/test_kaipanla_auction.py` does. For each exact path assert 200, the corresponding `list_type`, response fields, `ApiTimestamp` examples (`2026-09-28 14:56:45`, `2026-09-28 09:44:54`) and decimal strings. Assert missing `trade_date`, today/future date, bad paging and invalid API key produce 422/401 with zero provider calls; provider exceptions produce existing `upstream_error` 502 without source details.

```python
ROUTES = {
    "auction": "/api/v1/realtime/kaipanla/market-emotion/stocks/auction",
    "limit_up": "/api/v1/realtime/kaipanla/market-emotion/stocks/limit-up",
    "limit_down": "/api/v1/realtime/kaipanla/market-emotion/stocks/limit-down",
    "broken_limit_up": "/api/v1/realtime/kaipanla/market-emotion/stocks/broken-limit-up",
}
assert client.get(ROUTES["auction"], headers=auth).status_code == 422
assert client.get(ROUTES["auction"], params={"trade_date": "2026-09-28"}).status_code == 401
```

- [ ] **Step 2: Run red tests.** Run `uv run pytest tests/test_kaipanla_market_history.py -q`; expected missing-route 404 or missing `create_app` injection parameter.

- [ ] **Step 3: Implement four explicit response models/routes.** In the new module, set router prefix `/api/v1/realtime/kaipanla/market-emotion` and tag `实时接口`; define common response fields with `ApiTimestamp`; four item models with exactly the corresponding Task 1 item fields; and `KaipanlaAuctionHistoryResponse`, `KaipanlaLimitUpHistoryResponse`, `KaipanlaLimitDownHistoryResponse`, `KaipanlaBrokenLimitUpHistoryResponse` with category-specific `list_type: Literal[...]` and `items` types. Each route requires `trade_date: Annotated[date, Query()]`, bounds `offset: Annotated[int, Query(ge=0, le=10000)] = 0` and `limit: Annotated[int, Query(ge=1, le=30)] = 30`. Resolve the provider from `request.app.state.kaipanla_market_history_provider`; pass a fixed literal kind to `fetch`. Use `ConfigDict(extra="forbid", from_attributes=True)`; do not accept arbitrary source parameters. In `app.py`, inject/configure the provider with existing timeout, register the router under `Depends(_require_api_key)`, and map provider Invalid/Upstream to existing 422/502 error shape.

```python
@router.get("/stocks/limit-up", response_model=KaipanlaLimitUpHistoryResponse,
            summary="查询开盘啦历史涨停股票")
def limit_up_history(provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
                     trade_date: Annotated[date, Query()],
                     offset: Annotated[int, Query(ge=0, le=10000)] = 0,
                     limit: Annotated[int, Query(ge=1, le=30)] = 30
                     ) -> KaipanlaLimitUpHistoryResponse:
    return KaipanlaLimitUpHistoryResponse.model_validate(
        provider.fetch(kind="limit_up", trade_date=trade_date, offset=offset, limit=limit)
    )
```

Register the other three static operations with separate function names and category-specific response models; each calls `provider.fetch` once. Follow `public_api/kaipanla_auction.py` for descriptions/error responses.

```python
@router.get("/stocks/auction", response_model=KaipanlaAuctionHistoryResponse)
def auction_history(provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
                    trade_date: Annotated[date, Query()],
                    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
                    limit: Annotated[int, Query(ge=1, le=30)] = 30
                    ) -> KaipanlaAuctionHistoryResponse:
    return KaipanlaAuctionHistoryResponse.model_validate(
        provider.fetch(kind="auction", trade_date=trade_date, offset=offset, limit=limit)
    )

@router.get("/stocks/limit-down", response_model=KaipanlaLimitDownHistoryResponse)
def limit_down_history(provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
                       trade_date: Annotated[date, Query()],
                       offset: Annotated[int, Query(ge=0, le=10000)] = 0,
                       limit: Annotated[int, Query(ge=1, le=30)] = 30
                       ) -> KaipanlaLimitDownHistoryResponse:
    return KaipanlaLimitDownHistoryResponse.model_validate(
        provider.fetch(kind="limit_down", trade_date=trade_date, offset=offset, limit=limit)
    )

@router.get("/stocks/broken-limit-up", response_model=KaipanlaBrokenLimitUpHistoryResponse)
def broken_limit_up_history(
    provider: Annotated[KaipanlaMarketHistoryProvider, Depends(_provider)],
    trade_date: Annotated[date, Query()],
    offset: Annotated[int, Query(ge=0, le=10000)] = 0,
    limit: Annotated[int, Query(ge=1, le=30)] = 30,
) -> KaipanlaBrokenLimitUpHistoryResponse:
    return KaipanlaBrokenLimitUpHistoryResponse.model_validate(
        provider.fetch(kind="broken_limit_up", trade_date=trade_date, offset=offset, limit=limit)
    )
```

- [ ] **Step 4: Export and verify the contract.** Run `uv run python scripts/export_fastapi_openapi.py`; in the test assert all four saved operations exactly equal `create_app(...).openapi()` entries, each tagged `实时接口`, API-key security and 401/422/502 responses. Run `uv run pytest tests/test_kaipanla_market_history.py tests/test_kaipanla_auction.py -q`; expected PASS. Review the generated contract diff carefully because the current working tree already has unrelated board-route changes; stage only this feature's hunks. If unrelated hunks cannot be isolated safely, leave them unstaged and do not claim a clean commit.

- [ ] **Step 5: Commit only Task 2 changes.** Inspect `git diff` and `git diff --cached`; stage the new route file and this feature's app/test/contract hunks only. Commit `feat(api): expose Kaipanla historical stock lists`. Never clear or silently include existing user changes.

### Task 3: Source documentation, ADR clarification and complete verification

**Files:**
- Create: `docs/开盘啦市场情绪历史股票列表实时接口.md`
- Modify: `docs/FastAPI外部接口.md`
- Modify: `docs/adr/ADR-0059-开盘啦异动提醒实时只读接口.md`
- Test: `tests/test_kaipanla_market_history.py`

**Interfaces:** Documents and verifies the four routes delivered by Task 2; introduces no new runtime interface.

- [ ] **Step 1: Write the usage document.** Include the four paths, required `trade_date`, offset/limit, API-key header, field names/units, 2026-09-28 verified source category mapping and page counts, `total=null` semantics, timestamp/date examples, 401/422/502 examples, no persistence, and explicit distinction from the existing `auction-pool`. Document that current-day Socket 2103 is outside scope and source array tails are intentionally omitted. No raw response dumps or credentials.

- [ ] **Step 2: Clarify the existing navigation and ADR.** Add one link/summary in `docs/FastAPI外部接口.md` and an incremental 2026-09-29 subsection in ADR-0059 describing the fixed history action, four list types and no-migration boundary. Preserve concurrently modified board text. Add a docs assertion to `tests/test_kaipanla_market_history.py` that the paths are mentioned in the usage document and contract.

```python
def test_history_docs_cover_all_routes():
    doc = (Path(__file__).parents[1] / "docs/开盘啦市场情绪历史股票列表实时接口.md").read_text(encoding="utf-8")
    assert all(path in doc for path in ROUTES.values())
```

- [ ] **Step 3: Run focused and full checks.** Run `uv run pytest tests/test_kaipanla_market_history.py tests/test_kaipanla_auction.py -q`, then `uv run ruff format --check .`, `uv run ruff check .`, `uv run mypy src`, `uv run pytest`. Run isolated `uv run pytest -m integration` only when `TEST_DATABASE_URL` points to a disposable database; never production. Report exact skips/failures, not blanket success.

- [ ] **Step 4: Limited read-only source check.** Against one completed trading day, query only first page of each kind; assert `errcode=0`, exact day, valid stock codes and plausible category counts, and do not write response bodies. If the upstream is unavailable, preserve passing mocked tests and report live check as unavailable. No production deployment or ingestion.

- [ ] **Step 5: Review and commit docs.** Inspect all intended diffs, `git diff --check`, secrets/Raw absence and `git status --short`. Commit only this feature's documentation/test hunks as `docs: document Kaipanla historical lists`. Do not push, migrate or deploy without a separate user request. Handoff the route list, commit IDs, full verification evidence and any unrelated dirty changes left untouched.
