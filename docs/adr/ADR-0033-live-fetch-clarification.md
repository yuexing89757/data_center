# ADR-0033 clarification: bounded live fetch and append-only registration

- Status: Accepted
- Date: 2026-08-14
- Related issue: #47
- Clarifies: ADR-0033

`GET /api/v1/call-auction-indicative-details` accepts one six-digit SSE/SZSE stock code and derives
the standardized symbol and current Asia/Shanghai date. It first calls the bounded database RPC;
the latest succeeded or partial snapshot is returned without provider I/O. Only SQLSTATE `P0002`
permits the Eastmoney request. Database invalid/timeout/unavailable failures remain API failures and
must not be converted into live fetches. Per API process, provider concurrency is one. The adapter
uses the fixed `push2delay.eastmoney.com` endpoint first and the fixed `push2.eastmoney.com` endpoint
second, with no discovery and at most two total attempts with bounded timeout/backoff. The response
is capped below 5,000 source rows. A
private cache of no more than five seconds and a minimum request interval prevent the endpoint
becoming a general scraping proxy. It never schedules or expands a request to other symbols.

On a database miss, Raw JSONL is written synchronously before response. The prepared registration is then accepted by
a bounded single-writer queue with at most one waiting task, and the market data is returned with
an explicit `persistence_status=queued`. Queue saturation or Raw failure rejects the request, so a
response is never silently described as already persisted. The queued `SECURITY DEFINER` function
validates current date, symbol, size, hashes and each normalized row and atomically appends
ingestion, manifest, quality, snapshot and detail facts. The API login receives no direct table
DML; only this function is executable by `market_data_api`, with public/anon/authenticated revoked.

Matching `(symbol, trade_date, input_hash)` reuses its immutable version and removes the newly
written duplicate Raw object. Changed content creates the next immutable version under an advisory
transaction lock. A later database failure is logged and retains the immutable Raw object for
bounded operational recovery; it cannot retroactively alter an already returned response. Source
HTTP failure is 502, absence/Raw failure is 503, and concurrency/rate/queue rejection is 429.

The response labels the data as live provider-derived virtual indicative/matching detail, not
exchange trades or order-by-order records. This technical boundary does not grant collection or
redistribution rights. Production enablement still requires Eastmoney/exchange terms and retention
approval.

`data_origin=database`/`persistence_status=persisted` identifies a stored response;
`data_origin=eastmoney_live`/`persistence_status=queued` identifies the live fallback.
