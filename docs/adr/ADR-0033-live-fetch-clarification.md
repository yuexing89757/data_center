# ADR-0033 clarification: bounded live fetch and append-only registration

- Status: Accepted
- Date: 2026-08-14
- Related issue: #47
- Clarifies: ADR-0033

`GET /api/v1/call-auction-indicative-details` performs the Eastmoney request directly for one
SSE/SZSE symbol only after 09:26 Asia/Shanghai on the current date. Per API process, provider
concurrency is one, attempts are at most two with bounded timeout/backoff, and the response is
capped below 5,000 source rows. A private cache of no more than five seconds and a minimum request
interval prevent the endpoint becoming a general scraping proxy. It never schedules or expands a
request to other symbols.

Raw JSONL is written synchronously before response. The prepared registration is then accepted by
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
