# FastAPI external read-only API

The FastAPI process is an independent protocol boundary that connects directly to PostgreSQL. It
does not require PostgREST or the Worker scheduler. Provider access and Raw capture exist only for
the explicitly documented bounded single-symbol live-auction endpoint and the fixed THS:883423
bias fallback. Its SQL is limited to explicitly granted bounded `api_v1` functions.

```dotenv
FASTAPI_DATABASE_URL='postgresql+psycopg://<api-login>:<password>@<host>:5432/<database>'
FASTAPI_API_KEY='<at-least-32-random-characters>'
FASTAPI_HOST=127.0.0.1
FASTAPI_PORT=8000
```

`FASTAPI_DATABASE_URL` is mandatory and identifies a dedicated login inheriting only the
`market_data_api` group role. The API never falls back to the Worker's `DATABASE_URL`. Connections
force read-only transactions and a five-second statement timeout.

Stable v1 routes are security search (100 rows), unadjusted daily bars (5,000 rows and 3,661 days),
classification members (5,000 rows), the exact-date generic limit-up pool (5,000 rows), the
versioned same-day limit-up snapshot (500 rows per page), and exact-date call-auction market
snapshots and market-series sessions (500 requested six-digit codes). Business routes require
`X-API-Key`. `/healthz` is
process-local and `/readyz` verifies a bounded database query. Prices and amounts remain decimal
strings. Errors never return SQL, internal schema names, database addresses, or credentials.

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
volume and amount; source codes, Raw fields and internal timestamps remain private.

`POST /api/v1/call-auction-market-series-snapshots/query` accepts the same exact `trade_date` and
1–500 six-digit `codes`. It selects the latest succeeded session for that date, or the latest
partial session only when no succeeded session exists; sessions and dates are never merged or
substituted. The response contains session status and all persisted rounds ordered by
`sample_seq`. Every round reports its scheduled/collected times, status, selected provider-neutral
ingestion ID, returned facts and its own `missing_codes`, so partial coverage remains explicit.
Snapshot fields use the same provider-neutral price, cumulative volume/amount and observation-time
semantics as the 09:26 batch route.

`GET /api/v1/top-gainers-20d?end_date=&limit=10` ranks unadjusted close-to-close returns over
exactly 20 calendar trading sessions (19 intervals), with exact observation dates/prices, end-date
historical names and bounded omission counts. Ties break by symbol; explicit dates never fall back.

`GET /api/v1/board-indexes/883423/bias` takes no parameters and first reads stored
`THS:883423` daily bars. The database result is ready only with at least 34 rows and a latest bar
matching the most recent expected `CN_A_SHARE` trading date. A ready hit returns
`data_origin=database` and `persistence_status=persisted`. Only SQLSTATE `P0002` for missing,
insufficient, or stale history triggers the fixed THS annual URL; other database failures never
trigger provider access. The live request uses a five-second timeout, reads the current year, and
reads the previous year only when fewer than 34 unique bars were obtained.

`moving_average_5` is the simple mean of the current close and four
preceding available positive closes; `bias_5_pct=(close-moving_average_5)/moving_average_5*100`.
The response compares the current value with the previous available board session as
`up`/`down`/`flat`, and reports the highest and lowest valid BIAS5 values across the latest 30
board sessions, uses the latest date for tied extrema, and returns Decimal strings. Before a live
response is returned, exact source bytes are captured losslessly in immutable JSONL Raw. The
response is marked `data_origin=ths_live`, `persistence_status=queued`, and the standard bars are
registered asynchronously through a narrowly granted idempotent RPC. The queue has one running
and one waiting slot. Provider failure is 502, Raw/persistence failure is 503, and a busy fetch or
full queue is 429. The endpoint never accepts a board/date input, fills gaps, changes the formula,
or adds a Worker/OS schedule.

`GET /api/v1/call-auction-one-price-limits?trade_date=` returns separate up/down lists only when
the stored 09:26 Asia/Shanghai snapshot has complete equal last/high/low evidence at the applicable
versioned price limit. Partial status and incomplete omissions remain visible; later bars are unused.

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
