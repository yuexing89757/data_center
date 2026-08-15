# THS 883423 Bias Database-First Live Fallback Design

## Goal

Keep `GET /api/v1/board-indexes/883423/bias` fast and deterministic when
recent `THS:883423` daily bars are stored, while making an empty, insufficient,
or stale database self-healing through a bounded live read from the fixed THS
annual-line endpoint. A live response is returned immediately after immutable
Raw capture and its source facts are registered asynchronously.

## Read decision

The existing authenticated route keeps accepting no parameters. It first calls
a bounded `api_v1` read RPC. The RPC raises SQLSTATE `P0002` when any of these
conditions is true:

- no `THS:883423` daily bars exist;
- fewer than 34 latest bars exist;
- the latest stored date is older than the latest `CN_A_SHARE` trading-calendar
  date on or before the current `Asia/Shanghai` date.

Only `P0002` permits live fallback. Parameter, timeout, permission, connection,
and other database errors retain the existing 4xx/5xx mappings and never
trigger provider traffic.

## Live source boundary

The provider URL is fixed to
`https://d.10jqka.com.cn/v4/line/bk_883423/01/{year}.js`. Neither URL, code nor
year is client-controlled. The adapter sends the existing tested User-Agent and
Referer headers and uses a five-second request timeout.

The THS JavaScript wrapper contains JSON whose `data` rows are semicolon
separated. The first seven values are date, open, high, low, close, volume and
amount. The existing `akshare_ths` provider parser remains the only boundary
that understands this schema. The API service consumes standard
`BoardIndexDailyBarRecord` values and never handles THS field names.

The service requests the current Shanghai year first. If it yields fewer than
34 records through the current date, it also requests the previous year and
combines the two standard-record sequences by `(board_id, trade_date)`. Empty,
malformed, duplicated, nonpositive-close, or invalid OHLC data fails the live
request; no partial provider result is returned.

## Calculation

One pure calculator receives an ascending sequence of standard board daily
bars. It uses `Decimal` throughout and applies ADR-0035's unchanged semantics:

```text
MA5 = arithmetic mean of the current and preceding four closes
BIAS5 = (close - MA5) / MA5 * 100
```

The response compares the latest BIAS5 with the preceding session and computes
extrema from valid BIAS5 values across the latest 30 sessions. Ties select the
most recent date. Public decimal values are rounded to six fractional places
and serialized as strings.

The response adds `data_origin`, `persistence_status`, and `fetched_at`:

- database hit: `database`, `persisted`, request-time Shanghai timestamp;
- live fallback: `ths_live`, `queued`, time at which the live payload was
  accepted and Raw capture completed.

## Raw capture and asynchronous persistence

A live payload is not returned until its exact response bytes are written to
the API Raw root with a SHA-256 digest and collision-safe object path. The
existing protected API state directory is reused; no Raw market data enters
Git. Raw failure rejects the request.

After Raw capture, a dedicated single-writer queue with one waiting slot calls
one narrowly granted security-definer RPC. A full queue rejects the live
request before claiming that persistence is queued. The HTTP request never
waits for database registration after successful enqueue.

The persistence RPC accepts only the fixed board identity plus bounded Raw
metadata and normalized JSON rows. It validates:

- `THS:883423`, `akshare_ths`, schema version and request years;
- row count, byte size, SHA-256 format, unique dates and maximum row bound;
- known board identity and `CN_A_SHARE` trading-calendar membership;
- positive Decimal OHLC, valid OHLC ranges, nonnegative volume and amount.

In one transaction it creates a succeeded IngestionRun and RawManifest and
idempotently upserts `core.board_index_daily_bar` by `(board_id, trade_date)`.
The API login retains no direct table DML. Async failure is logged with safe
identifiers and leaves the immutable Raw object for recovery; a later request
will fall back again while the read RPC remains insufficient or stale.

## Errors and lifecycle

- THS HTTP/timeout/schema/data failure: 502.
- Raw filesystem or persistence subsystem unavailable: 503.
- queue busy: 429.
- database read failures other than `P0002`: existing 4xx/5xx database error.

The queue is created with the FastAPI application and drained during graceful
shutdown. Worker scheduling is unchanged and no OS or APScheduler task is
added. Existing ingestion CLI behavior remains available but is not invoked by
this endpoint.

## Contracts and verification

ADR-0036 supersedes ADR-0035's database-only read decision while retaining its
formula. The read RPC remains the database-first contract; a second migration
adds the bounded persistence RPC and grants only the API role. FastAPI,
PostgREST and Agent contracts, the API runbook, and the BoardIndex domain guide
are updated together.

Focused tests cover read-hit/no-provider behavior, each `P0002` fallback
condition, no fallback for other database errors, current/previous-year live
reads, Decimal calculations, Raw-before-enqueue ordering, queue bounds,
security-definer validation, idempotent persistence, and safe error mappings.
The intentionally slow full PostgreSQL suite is not run for this delivery;
only focused integration smoke tests are used.

