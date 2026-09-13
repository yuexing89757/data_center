# DragonTiger Historical Repair and Hot-Money Seat Profiles Design

- Status: approved by the project owner on 2026-09-13
- Governing issue: #79
- Governing ADR: ADR-0055
- Domain design: `docs/领域详设-DragonTiger游资席位画像-2026-09-13.md`

## Objective

Repair completely missing DragonTiger trading dates for the most recent two years with Tushare,
add an audited and effective-dated hot-money-to-seat catalog, materialize point-in-time-safe seat
performance profiles, and expose bounded objective capital-quality components for an exact stock and
trade date.

## Approved decisions

- EastMoney remains the daily source; Tushare is an explicit independent repair source.
- Repair only dates with no successful standardized facts. Never merge providers inside a successful
  ingestion, fill partial dates from another source, or overwrite EastMoney facts.
- Preserve provider-specific immutable Raw and full ingestion lineage.
- Maintain the hot-money roster as a versioned, manually reviewed catalog. Never infer identity from
  a seat name alone.
- Count only effective buy participation in seat win-rate samples. Pure-sell and net-sell observations
  are excluded.
- Define wins as positive unadjusted close-to-close returns at T+1, T+3 and T+5. Preserve sample counts
  and average returns, and do not synthesize missing bars.
- Materialize daily profiles with explicit as-of dates and algorithm versions. Queries for an event may
  use only profiles strictly earlier than the event date.
- Add one bounded query requiring an exact trade date and six-digit code. Never fall back to another
  date.
- Return objective components and quality metadata only. Do not create a subjective capital-quality
  score, rank, strategy or recommendation.
- Keep credentials runtime-only and out of Git, configuration files, Raw, database records and logs.

## Components

1. Extend the explicit Tushare history-repair CLI with a missing-date selector, dry-run, execution
   confirmation, per-date commits and a final coverage report.
2. Add internal hot-money actor, effective mapping and daily seat-profile tables through one ordered
   migration with Worker-only writes and RLS.
3. Add a reviewed catalog file and explicit catalog-sync service. Reject overlapping approved mappings,
   generic institution aliases and mappings without stable seats.
4. Reuse pure DragonTiger outcome/profile calculations, tightening effective-buy eligibility and adding
   persistence plus daily orchestration.
5. Add one bounded `api_v1` RPC and matching FastAPI route for stock capital-quality components; update
   PostgREST, FastAPI and Agent Tools contracts together.
6. Register the profile job in the Worker catalog after its daily data dependencies. Do not create an OS
   scheduler entry.

## Failure and recovery

The history repair is resumable by trading date. Source, normalization or persistence failure records a
stable safe code and proceeds to the next date. Existing successful facts remain immutable. Catalog sync
validates the complete candidate catalog before a single transaction publishes it. Profile calculation
is idempotent per as-of date and algorithm version; failure never rolls back DragonTiger facts.

## Verification

Provider and replay tests cover source semantics and failures. Domain tests cover identity, effective
dates, buy eligibility, missing bars and look-ahead prevention. PostgreSQL integration tests cover
migration constraints, RLS, idempotency and bounded RPC behavior. Contract tests keep all three public
contracts synchronized. Before production repair, run a read-only preflight and one-date smoke test;
afterwards report coverage, anonymous-seat share, reviewed-mapping hit rate and remaining failures.

## Non-goals

- Automatic web collection of hot-money rumors or aliases.
- Name-only seat identity resolution.
- Cross-provider row-level merging or conflict arbitration.
- Subjective capital-quality scoring, backtesting, portfolio logic or trading advice.
