# FastAPI external read-only API

The FastAPI process is an independent protocol boundary that connects directly to PostgreSQL. It
does not require PostgREST or the Worker scheduler. Provider access and Raw capture exist only for
the explicitly documented bounded single-symbol live-auction endpoint. The fixed THS:883423 bias
endpoint is database-only. Its SQL is limited to explicitly granted bounded `api_v1` functions.

```dotenv
FASTAPI_DATABASE_URL='postgresql+psycopg://<api-login>:<password>@<host>:5432/<database>'
FASTAPI_API_KEY='<at-least-32-random-characters>'
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
```

`FASTAPI_DATABASE_URL` is mandatory and identifies a dedicated login inheriting only the
`market_data_api` group role. The API never falls back to the Worker's `DATABASE_URL`. Connections
force read-only transactions and a five-second statement timeout.

Stable v1 routes are security search (100 rows), recent unadjusted daily bars (5,000 rows),
classification members (5,000 rows), the exact-date generic limit-up pool (5,000 rows), the
versioned same-day limit-up snapshot (500 rows per page), and exact-date call-auction market
snapshots and market-series sessions (500 requested six-digit codes), plus latest retained stock
daily indicators (500 requested six-digit codes). Business routes require
`X-API-Key`. `/healthz` is
process-local and `/readyz` verifies a bounded database query. Prices and amounts remain decimal
strings. Errors never return SQL, internal schema names, database addresses, or credentials.

DragonTiger 提供四个数据库只读路由：按精确日期查询事件、按六位股票代码查询有界历史、按稳定
席位 UUID 查询行为，以及按事件 UUID 查询即时计算的客观资金指标。路径分别为
`/api/v1/dragon-tiger/events/by-date`、`/api/v1/dragon-tiger/events/by-symbol/{code}`、
`/api/v1/dragon-tiger/seats/{seat_id}/trades` 和
`/api/v1/dragon-tiger/events/{event_id}/metrics`。事件支持 `DAY`/`THREE_DAY` 周期；日期区间最长
366 个自然日，`limit` 为 1..500，`offset` 为 0..10000。所有数值为 Decimal 字符串；接口不回退
日期、不访问数据源、不触发采集或 Raw 重放，也不返回主观评分或策略标签。

Keep the application on `127.0.0.1`. Public authentication, HTTPS/domain, reverse proxy, firewall,
rate limits, request-log retention, and API-key rotation are separate deployment decisions. See
`Standalone-PostgreSQL-FastAPI-Linux.md` for Linux gates.

`GET /api/v1/limit-up-pool?trade_date=YYYY-MM-DD&version=&limit=` returns the exact-date,
versioned mainboard limit-up pool (maximum 5,000 members). `trade_date` is the limit-up event
date. Each item contains the standard symbol, stock code, historical name effective that day,
and `free_float_market_cap_cny`, calculated exactly as that day's unadjusted closing price times
that day's free-float shares. Decimal values are strings. Rows missing a historical name, close,
or free-float shares are omitted individually and reported through total/valid/returned/omitted
counts plus grouped omission reasons; no value or date is substituted. Validation covers the whole
snapshot, then `limit` selects valid rows in ascending symbol order. `has_more` reports truncation;
v1 has no offset/cursor, so request 5,000 for the complete bounded set.

`GET /api/v1/daily-bars/{code}?trade_date=YYYY-MM-DD&limit=20` accepts exactly one six-digit
stock code. `trade_date` is an inclusive cutoff and `limit` is the maximum number of most-recent
stored unadjusted daily bars to return. The database resolves the code to one standard symbol from
Security facts; it does not guess an exchange from the code prefix. Items are newest first and
never later than the cutoff. Missing or suspended sessions are not fabricated, so `count` may be
less than `limit`. Unknown codes return 404 and ambiguous codes are rejected.

`POST /api/v1/stock-daily-indicators/latest/query` accepts `codes` containing 1–500 six-digit
stock codes. Duplicate codes are removed while preserving their first position. Security facts
resolve each code to one standard symbol; the service never guesses an exchange from a prefix, and
an ambiguous cross-exchange code returns 422. Each symbol independently selects the greatest
retained `trade_date` in `stock_daily_indicator`, so items need not share a date. Unknown codes and
known stocks without a retained indicator are returned in `missing_codes`; an all-missing query is
still 200 with an empty `items` list. Items preserve request order and expose all provider-neutral
daily indicator fields. Decimal values are strings, missing values remain `null`, and the route does
not fetch providers, replay Raw data, fill dates, or expose source/ingestion fields.

`GET /api/v1/daily-limit-up-list?trade_date=YYYY-MM-DD&version=&offset=0&limit=200`
returns the immutable `today_limit_up` snapshot for the exact date. When `version` is omitted it
selects only the highest version for that date; it never falls back to an older trading date.
The response includes snapshot status (`ready`, `partial`, `deferred`, or `failed`), immutable
version/rule/hash metadata, candidate/member/rejected counts, grouped quality findings, and the
domain member fields: historical name, objective unadjusted limit-up price facts, same-date
free-float shares and market capitalization, source-reported sealing observations, nullable
closing bid levels and computed bid-1 sealing amount, plus provider-neutral lineage identifiers.
Missing enrichment remains `null`; a non-ready snapshot is never presented as complete. Members
are ordered by `symbol`; `offset` is bounded to 50,000 and `limit` to 500. This route intentionally
replaces its former rich-list response under ADR-0030. `/api/v1/limit-up-pool` is unchanged.

`POST /api/v1/call-auction-market-snapshots/query` accepts one exact `trade_date` and 1–500
six-digit `codes`. Duplicate codes are removed. It returns the latest successful ingestion for that
date; only when no successful ingestion has facts does it select the latest partial ingestion. It
never combines ingestions or falls back to another date. A code shared by SSE and SZSE can return
both standardized symbols. `missing_codes` reports requested codes absent from the selected batch.
The envelope includes the selected provider-neutral ingestion ID and status so consumers can prove
batch coherence. Items expose observation time, latest/previous-close/high/low prices, cumulative
volume and amount, bid/ask levels 1–5, and nullable `seal_amount`. The seal amount is calculated as
bid-1 price times bid-1 shares only when ask-1 volume is missing or zero; source codes, Raw fields
and internal timestamps remain private.

`POST /api/v1/realtime-quotes/latest/query` accepts 1–500 six-digit stock `codes` and
`max_age_seconds` from 1 to 86,400 (default 15). On every request it directly performs bounded
Tencent batch reads and returns that response without querying or writing PostgreSQL, saving Raw,
creating an ingestion run, or triggering the Worker. `max_age_seconds` remains only for client
compatibility and does not filter the request-time result. Codes beginning with `6` route to SSE;
`0` and `3` route to SZSE; other codes are reported in `missing_codes`. Quantities are shares and
cumulative amount is CNY. Total upstream failure returns 502 and never falls back to stored data.

`POST /api/v1/call-auction-market-series-snapshots/query` accepts the same exact `trade_date`,
1–500 six-digit `codes`, and an optional six-digit `batch_code` in `HHMMSS` form. It selects the
latest succeeded session for that date, or the latest
partial session only when no succeeded session exists; sessions and dates are never merged or
substituted. The response contains session status and all persisted rounds ordered by
`sample_seq`. When `batch_code` is provided, only the matching round inside that selected session
is returned; a valid but absent batch returns an empty `rounds` list and does not select another
session or date. Every round reports its scheduled/collected times, status, selected provider-neutral
ingestion ID, returned facts and its own `missing_codes`, so partial coverage remains explicit.
Every item includes `value_semantics`. Before 09:25 it is `auction_indicative`: `last_price` is
bid-1 price, `cumulative_volume` is bid-1 shares and `cumulative_amount` is their exact product; if
either bid-1 input is missing, all three values are `null`. From 09:25 onward it is `opening_trade`
and preserves the provider's actual trade price, cumulative volume and amount. Rows written before
this contract are labeled `legacy_source_quote`; their historical values are not rewritten.

`GET /api/v1/top-gainers-20d?end_date=&limit=10` ranks unadjusted close-to-close returns over
exactly 20 calendar trading sessions (19 intervals), with exact observation dates/prices, end-date
historical names and bounded omission counts. Exact-date positive pytdx bars with the provider-neutral
`unknown` trade status are eligible, while explicitly suspended bars remain excluded. Ties break by
symbol; explicit dates never fall back.

`GET /api/v1/close-price-new-highs-120d` takes no parameters and returns every SSE/SZSE stock
from the latest ready daily snapshot whose positive unadjusted close strictly exceeds the highest close from the previous 119
`CN_A_SHARE` trading sessions. The stock must have valid bars in all 120 exact market sessions;
`pytdx` `unknown` status is accepted, explicit suspension, missing/nonpositive bars, missing names,
equal highs, and BSE securities are excluded. The response reports the selected date, prior high,
breakout percentage and bounded omission counts. It has no pagination; the universe is hard-limited
to 10,000 candidates and this RPC uses a ten-second statement timeout. The Worker materializes the
snapshot Monday-Friday at 21:30 Asia/Shanghai after the exact-date `daily_market` workflow. Before
that run, on weekends, or after an upstream failure, the endpoint continues to return the most recent
ready immutable snapshot and exposes its `trade_date`; it never recalculates from `core.daily_bar` on
the request path. If no ready snapshot has ever been published, the endpoint returns not found.

`GET /api/v1/board-indexes/883423/bias` takes no parameters and only reads stored
`THS:883423` daily bars through a bounded `api_v1` RPC. At least 34 rows are required; otherwise
the endpoint returns 404. When today's bar is not yet stored, the endpoint returns the latest
persisted board date and exposes that actual `trade_date` rather than failing or accessing THS.
Successful responses always use `data_origin=database` and `persistence_status=persisted`.

`moving_average_5` is the simple mean of the current close and four
preceding available positive closes; `bias_5_pct=(close-moving_average_5)/moving_average_5*100`.
The response compares the current value with the previous available board session as
`up`/`down`/`flat`, and reports the highest and lowest valid BIAS5 values across the latest 30
board sessions, uses the latest date for tied extrema, and returns Decimal strings. The endpoint
never accepts a board/date input, fills gaps, changes the formula, accesses a Provider, writes Raw,
or writes PostgreSQL. Worker collection and its retry status are independent of API requests.

`GET /api/v1/call-auction-one-price-limits?trade_date=` selects the exact stored 09:25:30
Asia/Shanghai snapshot and calculates SSE/SZSE mainboard limits at read time. Ordinary and ST
stocks both use the accepted 10% rule, tick 0.01, `CN_MAINBOARD_2026_07_06` and algorithm `1.0.0`.
Only complete `last_price=high_price=low_price=upper_limit/lower_limit` evidence enters the separate
up/down lists. The response identifies `calculation_mode=realtime_read` and
`price_limit_calculation_id=null`; the selected ingestion remains the source lineage. A ready 09:25:30
snapshot is the only market-data dependency, so the endpoint does not wait for the nightly
price-limit batch. It does not fetch providers, write data or use later bars. Partial status and
incomplete mainboard omissions remain visible; a valid empty list is HTTP 200, while no exact
09:25:30 snapshot is HTTP 404. Each item exposes the stored `seal_amount`; under the accepted
snapshot rule it is `bid1_price * bid1_volume` when ask-1 through ask-3 volumes are each missing
or zero, so down-limit items normally return `null`. `observed_at` is rendered in Asia/Shanghai as
`YYYY-MM-DD HH:mm:ss`.

`GET /api/v1/call-auction-indicative-details?code=688796&offset=0&limit=200` requires only one
six-digit SSE/SZSE stock code; the service derives the current Asia/Shanghai date and standardized
symbol. It first reads the latest exact-date succeeded or partial database snapshot. A hit returns
without provider I/O with `data_origin=database` and `persistence_status=persisted`. Only SQLSTATE
`P0002` triggers one bounded Eastmoney fetch for current-day 09:15:00-09:25:59 virtual
indicative/reference and matching-volume observations. Other database failures return the normal
API error and never trigger scraping. A live result is returned after immutable Raw capture with
`data_origin=eastmoney_live` and `persistence_status=queued`, while bounded database registration
runs asynchronously. The single-writer queue has one waiting slot. Full queue or Raw failure rejects
the request rather than returning untracked data. It is not a trade-tick or order-by-order API; the
source display classification is untrusted. Provider failure maps to 502, provider absence/Raw
failure to 503, and the single-process concurrency/rate/queue gate to 429.

The live adapter uses two fixed Eastmoney hosts without endpoint discovery: `push2delay` first and
`push2` second. Each host is attempted at most once, preserving the existing two-attempt bound. A
safe server-side warning records the standardized symbol and deepest provider exception when both
fixed hosts fail; the public response remains the generic 502 contract.

Response items are ordered ascending by `observed_at`, then by `source_sequence` for equal
timestamps. `fetched_at` and `items[].observed_at` are rendered as Asia/Shanghai wall-clock strings
in `YYYY-MM-DD HH:mm:ss` form; the date-only `trade_date` remains `YYYY-MM-DD`.
