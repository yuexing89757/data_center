# DragonTiger Historical Coverage and Quality Repair Design

- Status: approved by the project owner on 2026-09-08
- Governing issue: #70
- Governing ADRs: ADR-0049 and ADR-0053
- Domain design: `docs/领域详设-DragonTiger-2026-09-02.md`

## Objective

Repair DragonTiger historical ingestion so that source-valid A-share events from 2025-01-01 onward
are persisted as traceable facts without weakening domain invariants. The repair must also make every
remaining failure diagnosable by a stable safe code, recover existing orphan Raw objects, restore BSE
security coverage, replace the affected read contracts, deploy the result, and complete a resumable
production backfill.

This remains a market-data fact and deterministic-metrics project. Subjective capital-quality scores,
tourist-capital labels, strategies, backtests and trading advice remain outside the boundary.

## Evidence and root causes

The production backfill recorded 375 unique failed dates:

- 244 dates contained period text rejected by the two-value parser;
- 80 dates hit zero-buy/zero-sell source placeholders;
- 31 dates ended as opaque runtime failures, with 32 orphan Raw files;
- 7 dates contained exact duplicated source detail rows or unstable cross-page duplicates;
- 6 dates contained an Event with only one disclosed side;
- 7 failures referenced BSE securities absent from `core.security`.

Offline replay of the runtime-failure Raw normalized successfully. Twenty-seven of those dates reproduce
as temporal alias conflicts: the same reliable EastMoney seat key has changed names, while the current
schema treats both key and name as globally one-to-one identities. Four old failures lack enough stored
diagnostic detail to reconstruct their exact cause; the repaired error taxonomy makes future occurrences
explicit.

Direct source inspection found no separate amount-period field in the EastMoney summary or seat-detail
response. Reason text proves a trigger window but does not, by itself, prove the aggregation period of
the disclosed amounts.

## Domain model

### Trigger window

`DragonTigerEvent` replaces `period_type`, `period_start_date` and `period_end_date` with:

- `trigger_window_basis`: `MARKET_SESSIONS | SECURITY_TRADED_SESSIONS`;
- `trigger_window_sessions`: positive integer;
- `trigger_occurrence_count`: nullable positive integer;
- `trigger_start_date`: nullable date;
- `trigger_end_date`: disclosure trade date.

Market-session starts are resolved from `CN_A_SHARE`. Security-traded-session starts are resolved only
from confirmed unadjusted `core.daily_bar` dates. A missing bar history leaves the start date null and a
quality code; it does not convert the rule to a market-session window.

### Amount period

The Event separately stores:

- `amount_period_basis`: `MARKET_SESSIONS | SECURITY_TRADED_SESSIONS | SOURCE_UNSPECIFIED`;
- `amount_period_sessions`: nullable positive integer;
- `amount_period_start_date`: nullable date;
- `amount_period_end_date`: nullable date.

Verified ordinary daily events use one market session. Previously verified consecutive-three-session
events use three market sessions. Other source reason families remain `SOURCE_UNSPECIFIED` until their
amount aggregation semantics are independently verified. Metrics may still be returned, but consumers
can exclude unverified periods deterministically.

### Reason mapping

Reason mapping is an explicit, tested table rather than one broad substring check. Initial mappings cover:

| Source rule family | Trigger basis | Sessions | Occurrences | Amount period |
| --- | --- | ---: | ---: | --- |
| ordinary single-session reason | market | 1 | null | verified market/1 |
| 连续三个/连续3个交易日 | market | 3 | null | verified market/3 |
| 连续10/30个交易日累计偏离 | market | 10/30 | null | source unspecified |
| 10/30日内 N 次同向异常 | market | 10/30 | N | source unspecified |
| 北交所最近3个有成交交易日 | security traded | 3 | null | source unspecified |

Unknown text produces `DT_PERIOD_MAPPING_UNSUPPORTED`. No fallback mapping is allowed.

### Seat identity

`trading_seat_source_identity` owns the durable source identity:

- `identity_id`, `seat_id`, `source_code`, `source_seat_key`;
- `first_seen_date`, `last_seen_date`;
- unique `(source_code, source_seat_key)`.

`trading_seat_alias` becomes a name observation owned by one source identity:

- `alias_id`, `identity_id`, `alias_name`;
- `first_seen_date`, `last_seen_date`;
- unique `(identity_id, alias_name)`.

Reliable keys resolve by key only. A changed name extends or creates an observation without changing
`seat_id`. The same name may belong to multiple identities. Keyless generic rows retain `seat_id=null`
and are never auto-merged by name. `SeatTrade.seat_name_raw` remains immutable source text.

### Disclosure and quality

Event facts include `buy_disclosure_present` and `sell_disclosure_present`. One may be false, but both
cannot be false for a published Event. Source findings carry a safe rule code, severity, source event ID,
report kind and counts; they never carry the full source row.

Exact duplicate rows and explicit zero-activity placeholders remain in Raw. They are omitted from
standard `SeatTrade` facts and produce warning findings. Non-identical natural-key conflicts, invalid
amounts, invalid ranks and count mismatches remain hard failures.

## Provider and normalization flow

1. Register the ingestion as `RUNNING` before provider I/O.
2. Fetch the summary report with existing byte, page, timeout and retry bounds.
3. Fetch each detail report globally when it is one page. If the report declares multiple pages, fetch
   details with a bounded `TRADE_ID` filter for each frozen summary Event instead of paginating by the
   non-unique global sort key.
4. Freeze every returned source row, including exact duplicates and zero placeholders.
5. Write immutable Raw and durably attach its manifest.
6. Normalize Raw into `DragonTigerNormalizationResult(events, findings)`.
7. Validate symbols, dates, natural keys, amounts, ranks and window invariants.
8. Publish Event, SeatTrade and quality results in a transaction, then mark the run succeeded.
9. On any failure, use a separate transaction to mark the run failed with a safe code while retaining
   any attached manifest.

Per-event detail calls are bounded by the frozen summary count and the existing maximum row/page policy.
Unexpected counts or a response referring to an unfrozen Event fail closed.

## Error taxonomy

At minimum the implementation exposes these stable codes:

- `DT_SOURCE_DUPLICATE_FILTERED`
- `DT_ZERO_ACTIVITY_PLACEHOLDER_FILTERED`
- `DT_DISCLOSURE_SIDE_MISSING`
- `DT_PERIOD_MAPPING_UNSUPPORTED`
- `DT_TRIGGER_START_UNAVAILABLE`
- `DT_UNKNOWN_SECURITY`
- `DT_SEAT_IDENTITY_CONFLICT`
- `DT_SOURCE_COUNT_MISMATCH`
- `DT_RAW_WRITE_FAILED`
- `DT_RAW_MANIFEST_ATTACH_FAILED`
- `DT_FACT_PUBLISH_FAILED`
- `DT_RECOVERED_ORPHAN_RAW`

The persisted error summary may include the stable code and controlled phase name. It must not include
arbitrary exception text, credentials, URLs with secrets, database DSNs or source payloads.

## BSE security coverage

A new `security_bse_daily` workflow/job runs in the Worker catalog at 20:15 Asia/Shanghai on weekdays.
It explicitly uses the existing Tushare security capability, reads `L/D/P`, preserves all returned rows
in Raw, and publishes only standardized BSE securities. This is one provider and one ingestion; it is not
a merge into a BaoStock run.

DragonTiger remains scheduled at 20:30. A failed current BSE prerequisite prevents that day's DragonTiger
workflow from publishing and records a dependency failure. The rollout performs one full BSE sync before
historical DragonTiger replay so delisted symbols can validate.

## Persistence and migration

One ordered migration will:

1. create the window-basis constraints and new Event columns;
2. backfill existing `DAY` and `THREE_DAY` rows deterministically;
3. add disclosure-presence columns;
4. create `trading_seat_source_identity` and reshape Alias observations without changing stable seat IDs;
5. validate migrated rows before removing old period columns and uniqueness constraints;
6. drop the old DragonTiger RPC signatures;
7. create replacement bounded RPCs and grants;
8. preserve RLS, role separation, statement timeouts and indexes.

No historical migration is edited. No migration reads or modifies filesystem Raw objects.

Ingestion persistence is split into `begin_ingestion`, `attach_raw_manifest`, `publish_success` and
`complete_failure` operations. Each operation validates the expected prior state and ingestion ID.
Repeated calls are idempotent only when content and terminal state agree; conflicting repeats fail.

## Public contract

The four DragonTiger business reads remain, but affected SQL signatures and response schemas are replaced:

- by-date and by-symbol reads accept optional `trigger_window_basis` and `trigger_window_sessions` instead
  of `period_type`;
- Event items return trigger-window fields, amount-period fields, disclosure flags and `data_quality_codes`;
- seat-history items retain the historical raw name and return the Event window fields;
- metrics return disclosure coverage, amount-period verification and quality codes with the existing
  objective amounts, counts and concentrations.

A metric dependent on a missing side is null. Metrics supported by the disclosed side remain available.
All current date, range, limit, offset and five-second database timeout bounds remain. PostgREST OpenAPI,
FastAPI OpenAPI and Agent Tools contracts change in the same commit as implementation and migration.

## Raw orphan recovery

The recovery command accepts only the configured DragonTiger Raw root. For each unregistered candidate it:

1. resolves and verifies the absolute path remains under that root;
2. validates the ingestion UUID encoded by the object path;
3. parses bounded JSONL and validates the DragonTiger schema version and one trade date;
4. computes SHA-256, bytes and row count;
5. refuses any existing conflicting ingestion or manifest;
6. inserts a failed recovered run and manifest with `DT_RECOVERED_ORPHAN_RAW`.

The command never edits, moves or deletes Raw. A dry-run lists counts and dates without payload content.

## Backfill and rollout

After local verification and push:

1. run a read-only production migration preflight;
2. apply the ordered migration without a database backup, as explicitly directed by the owner;
3. deploy and restart Worker and FastAPI;
4. verify Tushare configuration without printing the token;
5. run the full BSE security sync;
6. dry-run and then execute DragonTiger orphan registration;
7. replay registered failed Raw using schema-versioned normalizers;
8. refetch dates with no usable Raw or a remaining replay failure;
9. backfill from 2025-01-01 through the latest completed trading session;
10. verify coverage, Event/SeatTrade counts, quality-code totals, residual failures and all four public reads.

Backfill commits one trading date at a time, continues after an isolated failure, uses bounded provider
retries and writes resumable state. It never fills a failed date from a different provider or an older date.

## Verification

Focused tests cover:

- every initial reason-window family and unsupported text;
- verified and unspecified amount periods;
- market and security-traded start resolution;
- temporal seat rename, same-name/different-key and keyless generic seats;
- exact duplicate filtering, cross-page per-Event retrieval and conflicting duplicate rejection;
- zero placeholder filtering and non-zero hard invariants;
- buy-only, sell-only and both-side disclosure metrics;
- staged run/manifest/fact transactions, failure transitions and stale recovery;
- BSE `L/D/P` filtering, Worker ordering and dependency failure;
- orphan dry-run, path confinement, hash verification and idempotency;
- migration, RLS/grants/RPC bounds and all three checked-in contracts;
- Raw v1/v2 replay and new-schema live collection.

The final local gate is:

```text
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

PostgreSQL integration tests run only with an isolated disposable `TEST_DATABASE_URL`.
