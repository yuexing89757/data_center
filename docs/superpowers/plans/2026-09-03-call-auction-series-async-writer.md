# Call Auction Market Series Async Writer Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep all 32 opening-auction provider/Raw captures on their fixed 20-second slots while one independent in-process Writer serializes PostgreSQL persistence.

**Architecture:** `CallAuctionMarketSeriesService` remains the only sampling producer and prepares immutable round payloads without touching PostgreSQL inside the slot loop. A bounded FIFO with capacity 32 feeds one named Writer thread, and PostgreSQL persists each complete round—including every endpoint attempt—in one transaction before the Service finalizes the Session.

**Tech Stack:** Python 3.12, `queue.Queue`, `threading.Thread`, frozen dataclasses, SQLAlchemy 2, PostgreSQL ordered migrations, pytest, Ruff, mypy, uv.

**Spec:** `docs/superpowers/specs/2026-09-03-call-auction-series-async-writer-design.md`

## Global Constraints

- Preserve the code-fixed 09:15:00–09:25:20 schedule, 20-second cadence, 09:25:40 final deadline, 32 rounds, and PYTDX batches of at most 80 securities.
- The producer performs provider reads, immutable Raw writes, normalization, and retry-required validation; it performs no PostgreSQL operation inside the 32-round loop.
- Use one same-process FIFO with `maxsize=32`; never discard, overwrite, merge, or reorder round items.
- Use one non-daemon Writer thread; later captures continue after a Writer item fails.
- One attempt uses exactly one endpoint; attempts from different endpoints never merge into one successful fact set.
- Raw schema is `market_data_center.call_auction_market_series.raw.v2`; historical Raw replay remains fail-closed.
- Preserve all table columns, natural keys, partitions, RLS, grants, scheduler controls, and public API contracts.
- Do not use direct COPY, runtime DDL, operating-system scheduling, external queues, or production credentials in tests.
- Prices, ratios, and amounts remain `Decimal`; missing values remain `None` and are not converted to zero.

---

### Task 1: Immutable capture payloads and the single Writer

**Files:**
- Create: `src/market_data_center/call_auction_market_series_writer.py`
- Create: `tests/test_call_auction_market_series_writer.py`

**Interfaces:**
- Consumes: `MarketSeriesRound`, `MarketSeriesSnapshotRecord`, `IngestionRun`, `RawManifest`, and `QualityResult`.
- Produces: `CapturedAttempt`, `CapturedRound`, `WriterOutcome`, `CapturedRoundPersistence.persist_captured_round(captured)`, and `CallAuctionMarketSeriesWriter.submit/close_and_wait`.

- [ ] **Step 1: Write failing tests for FIFO order, one thread, failure continuation, and immutable outcome**

Create `tests/test_call_auction_market_series_writer.py`. Build two small terminal `CapturedRound` fixtures with different `sample_seq` values and a recording persistence whose `persist_captured_round` stores `(sample_seq, current_thread().name, current_thread().ident)`. Add these tests:

```python
def test_writer_persists_rounds_in_fifo_on_one_named_thread() -> None:
    persistence = RecordingPersistence()
    writer = CallAuctionMarketSeriesWriter(persistence)

    writer.submit(captured_round(sample_seq=0))
    writer.submit(captured_round(sample_seq=1))
    outcome = writer.close_and_wait()

    assert [item[0] for item in persistence.calls] == [0, 1]
    assert {item[1] for item in persistence.calls} == {"call-auction-series-writer"}
    assert len({item[2] for item in persistence.calls}) == 1
    assert outcome == WriterOutcome((0, 1), (), None)


def test_writer_records_failure_and_continues_with_later_rounds() -> None:
    persistence = RecordingPersistence(fail_sequences={1})
    writer = CallAuctionMarketSeriesWriter(persistence)

    for sample_seq in range(3):
        writer.submit(captured_round(sample_seq=sample_seq))
    outcome = writer.close_and_wait()

    assert [item[0] for item in persistence.calls] == [0, 1, 2]
    assert outcome == WriterOutcome((0, 2), (1,), "RuntimeError")
```

The mutation caught by the first test is parallel or reordered persistence. The mutation caught by the second is terminating the consumer after one failed transaction.

- [ ] **Step 2: Run the new tests and verify RED**

Run:

```powershell
uv run pytest tests/test_call_auction_market_series_writer.py -q
```

Expected: collection fails because `market_data_center.call_auction_market_series_writer` does not exist.

- [ ] **Step 3: Implement the immutable payload and Writer lifecycle**

Create `src/market_data_center/call_auction_market_series_writer.py` with these public shapes:

```python
@dataclass(frozen=True, slots=True)
class CapturedAttempt:
    run: IngestionRun
    records: tuple[MarketSeriesSnapshotRecord, ...]
    manifest: RawManifest
    quality_results: tuple[QualityResult, ...]
    elapsed: timedelta
    succeeded: bool


@dataclass(frozen=True, slots=True)
class CapturedRound:
    running_round: MarketSeriesRound
    completed_round: MarketSeriesRound
    attempts: tuple[CapturedAttempt, ...]


@dataclass(frozen=True, slots=True)
class WriterOutcome:
    persisted_sequences: tuple[int, ...]
    failed_sequences: tuple[int, ...]
    first_error_type: str | None


class CapturedRoundPersistence(Protocol):
    def persist_captured_round(self, captured: CapturedRound) -> None: ...
```

Implement `CallAuctionMarketSeriesWriter` with `Queue[CapturedRound | _StopToken](maxsize=SERIES_ROUND_COUNT)`. Its constructor starts exactly one `Thread(name="call-auction-series-writer", daemon=False)`. `submit()` uses blocking FIFO `put()` and rejects calls after close. `_run()` catches `Exception` around each persistence call, appends the failed sequence and the first `type(error).__name__`, continues consuming, and calls `task_done()` in `finally`. The stop token also calls `task_done()` before exit. `close_and_wait()` enqueues one private stop token, calls `Queue.join()`, joins the thread, rejects a second close, and returns tuples copied from the Writer-owned lists.

Add constructor validation to the frozen payloads:

- the completed and running rounds have the same `(session_id, sample_seq, scheduled_at, expected_quotes)`;
- `running_round.status` is `running`, and `completed_round.status` is terminal;
- attempt ingestion IDs are unique and all request params match the captured session and sample sequence;
- `completed_round.attempt_count == len(attempts)`;
- a selected ingestion ID is either absent or belongs to an attempt;
- `manifest.ingestion_id == run.ingestion_id`, `manifest.row_count == run.fetched_rows`, and `len(records) == run.accepted_rows`;
- `succeeded` is true exactly when `run.status` is `IngestionStatus.SUCCEEDED`.

- [ ] **Step 4: Run Writer tests and the type/lint checks for the new module**

Run:

```powershell
uv run pytest tests/test_call_auction_market_series_writer.py -q
uv run ruff check src/market_data_center/call_auction_market_series_writer.py tests/test_call_auction_market_series_writer.py
uv run mypy src/market_data_center/call_auction_market_series_writer.py
```

Expected: all commands pass and the test process exits without a live Writer thread.

- [ ] **Step 5: Commit the Writer unit**

```powershell
git add src/market_data_center/call_auction_market_series_writer.py tests/test_call_auction_market_series_writer.py
git commit -m "feat: add auction series single writer"
```

---

### Task 2: Make the Service a strict capture/Raw producer

**Files:**
- Modify: `src/market_data_center/call_auction_market_series_service.py:50-405`
- Modify: `tests/test_call_auction_market_series_service.py:1-403`

**Interfaces:**
- Consumes: Task 1 `CapturedAttempt`, `CapturedRound`, `WriterOutcome`, and `CallAuctionMarketSeriesWriter`.
- Produces: `CallAuctionMarketSeriesPersistence.persist_captured_round(captured)` and `finish_session(session_id, finished_at, error_summary=None)` calls; Raw v2 envelopes with complete attempt identity.

- [ ] **Step 1: Rewrite the fake persistence around whole-round commits**

In `tests/test_call_auction_market_series_service.py`, replace `start_round`, `create_ingestion_run`, `commit_attempt`, and `finish_round` on `FakePersistence` with:

```python
def persist_captured_round(self, captured: CapturedRound) -> None:
    self.persistence_thread_ids.append(current_thread().ident)
    if captured.completed_round.sample_seq in self.fail_sequences:
        raise RuntimeError("database unavailable")
    self.captured_rounds.append(captured)


def finish_session(
    self,
    session_id: UUID,
    finished_at: datetime,
    error_summary: str | None = None,
) -> MarketSeriesSession:
    assert self.session is not None and self.session.session_id == session_id
    rounds = [captured.completed_round for captured in self.captured_rounds]
    successful = sum(item.status is MarketSeriesStatus.SUCCEEDED for item in rounds)
    partial = sum(item.status is MarketSeriesStatus.PARTIAL for item in rounds)
    explicit_failed = sum(item.status is MarketSeriesStatus.FAILED for item in rounds)
    missing = self.session.expected_rounds - len(rounds)
    successful_quotes = sum(item.successful_quotes for item in rounds)
    failed_quotes = (
        sum(item.failed_quotes for item in rounds) + missing * self.session.universe_count
    )
    status = (
        MarketSeriesStatus.SUCCEEDED
        if successful == self.session.expected_rounds
        else MarketSeriesStatus.PARTIAL
        if successful_quotes or partial
        else MarketSeriesStatus.FAILED
    )
    self.session = replace(
        self.session,
        status=status,
        finished_at=finished_at,
        successful_rounds=successful,
        partial_rounds=partial,
        failed_rounds=explicit_failed + missing,
        successful_quotes=successful_quotes,
        failed_quotes=failed_quotes,
        error_summary=error_summary or ("missed_sampling_rounds" if missing else None),
    )
    return self.session
```

The fake must preserve real externally observable effects: completed rounds, attempt runs, records, quality results, missing-sequence counts, final status, and `error_summary`. Do not assert that a mock method merely existed.

- [ ] **Step 2: Add failing tests for capture independence, Writer failure, and Raw failure**

Add a blocking fake persistence whose first `persist_captured_round` waits on an `Event`, plus an `all_provider_calls` event set by the provider factory on its 32nd request. Run `service.collect()` on a test thread:

```python
def test_slow_writer_does_not_delay_any_of_the_thirty_two_captures() -> None:
    persistence = BlockingPersistence()
    factory = FakeProviderFactory(MutableClock(SLOTS[0]))
    result: list[CallAuctionMarketSeriesSummary] = []
    collection = Thread(
        target=lambda: result.append(
            _service(persistence, factory.clock, factory, FakeRawStore()).collect(
                TRADE_DATE, uuid4()
            )
        )
    )

    collection.start()
    assert factory.all_provider_calls.wait(timeout=2)
    assert factory.requested == [UNIVERSE] * 32
    assert collection.is_alive()
    persistence.release_writer.set()
    collection.join(timeout=2)

    assert not collection.is_alive()
    assert result[0].status == "succeeded"
```

Add a persistence variant that raises only for `sample_seq=1`; assert all 32 provider calls and Raw writes occurred, sequences 2–31 were still persisted, `summary.status == "partial"`, and the Session error summary is `writer_persistence_error:RuntimeError`.

Add a Raw store variant that raises `OSError` on its first write; assert the first completed round is failed with no attempts, all later provider requests run, and the Session is partial. This catches accidental propagation of a disk error that would stop the remaining slot loop.

- [ ] **Step 3: Update existing tests for retry elapsed time and Raw v2 identity**

Move the 16-second synthetic delay in `test_retry_reserves_the_previous_complete_attempt_duration` from `FakePersistence` to `FakeRawStore`, because database time is no longer part of capture elapsed time. Keep the expectation that a slow first capture cannot start a second endpoint inside the same deadline.

Extend the first Raw assertion in `test_collects_thirty_two_exact_rounds_and_raw_lineage`:

```python
first_attempt = persistence.captured_rounds[0].attempts[0]
assert CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION == (
    "market_data_center.call_auction_market_series.raw.v2"
)
assert first_raw["ingestion_id"] == str(first_attempt.run.ingestion_id)
assert first_raw["trade_date"] == TRADE_DATE.isoformat()
assert first_raw["session_id"] == str(summary.session_id)
assert first_raw["sample_seq"] == "0"
assert first_raw["scheduled_at"] == SLOTS[0].isoformat()
assert first_raw["endpoint"] == "first.quote:7709"
assert first_raw["attempt_number"] == "1"
assert first_raw["worker_observed_at"] == SLOTS[0].isoformat()
assert first_raw["provider_schema_version"] == "pytdx_hq.security_quotes.v1"
assert loads(first_raw["provider_raw_json"]) == {"symbol": "SSE:600000"}
```

- [ ] **Step 4: Run Service tests and verify RED**

Run:

```powershell
uv run pytest tests/test_call_auction_market_series_service.py -q
```

Expected: failures show the Service still calls per-round Persistence synchronously, Raw is still v1, and no whole-round persistence method is used.

- [ ] **Step 5: Refactor the Service into a producer**

Change `CALL_AUCTION_MARKET_SERIES_RAW_SCHEMA_VERSION` to `market_data_center.call_auction_market_series.raw.v2`. Reduce `CallAuctionMarketSeriesPersistence` to preparation/finalization reads and writes plus:

```python
def persist_captured_round(self, captured: CapturedRound) -> None: ...

def finish_session(
    self,
    session_id: UUID,
    finished_at: datetime,
    error_summary: str | None = None,
) -> MarketSeriesSession: ...
```

After `create_session`, instantiate `CallAuctionMarketSeriesWriter(self._persistence)`. In the 32-slot loop, create the running round in memory only. For a missed slot, submit a `CapturedRound` with no attempts and a failed `completed_round`. For an active slot, call `_collect_round()` and submit its returned `CapturedRound`. After the loop, call `writer.close_and_wait()` before `finish_session`.

Use this finalization rule:

```python
writer_error = (
    f"writer_persistence_error:{outcome.first_error_type}"
    if outcome.first_error_type is not None
    else None
)
finished = self._persistence.finish_session(
    session.session_id,
    _utc_clock_sample(self._clock),
    writer_error,
)
```

Make `_collect_round()` accumulate all `CapturedAttempt` values and return one `CapturedRound`. Make `_attempt()` return `CapturedAttempt`; it must create the running `IngestionRun` only in memory, fetch the provider, write Raw, validate/convert, build the terminal run and Manifest, and never call Persistence. Its `elapsed` ends immediately after Raw plus in-memory validation.

Catch only `OSError` from Raw storage at the `_collect_round()` boundary. Convert that round to `MarketSeriesStatus.FAILED`, `attempt_count=0`, full `failed_quotes`, `selected_ingestion_id=None`, and `error_summary="raw_persistence_error"`; then continue the next slot. Do not turn programming, validation, or queue errors into a fake market-data result.

Change `_raw_envelopes` to accept `trade_date`, `ingestion_id`, `endpoint`, and `attempt_number`, and add those four values to every returned provider row envelope. Keep provider payload serialization deterministic and keep empty provider responses as immutable zero-row Raw objects.

Wrap the producer loop so that an unexpected producer exception still closes and drains the Writer before re-raising; do not leave a thread outside the Worker lifecycle.

- [ ] **Step 6: Run Service and Writer tests until GREEN**

Run:

```powershell
uv run pytest tests/test_call_auction_market_series_service.py tests/test_call_auction_market_series_writer.py -q
uv run ruff check src/market_data_center/call_auction_market_series_service.py src/market_data_center/call_auction_market_series_writer.py tests/test_call_auction_market_series_service.py tests/test_call_auction_market_series_writer.py
uv run mypy src/market_data_center/call_auction_market_series_service.py src/market_data_center/call_auction_market_series_writer.py
```

Expected: all tests pass; the slow Writer test proves all provider calls occur before its release event.

- [ ] **Step 7: Commit the producer refactor**

```powershell
git add src/market_data_center/call_auction_market_series_service.py tests/test_call_auction_market_series_service.py
git commit -m "refactor: decouple auction series capture from database writes"
```

---

### Task 3: Persist each captured round in one PostgreSQL transaction

**Files:**
- Modify: `src/market_data_center/persistence/call_auction_market_series_postgres.py:1-406`
- Modify: `tests/test_postgres_integration.py:1218-1513`

**Interfaces:**
- Consumes: Task 1 `CapturedAttempt` and `CapturedRound`.
- Produces: `PostgreSQLCallAuctionMarketSeriesPersistence.persist_captured_round(captured)` and `finish_session(..., error_summary=None)`.

- [ ] **Step 1: Add a failing integration test for one-round atomic persistence**

Add `test_market_series_persistence_commits_captured_round_atomically`. Create a running Session and a `CapturedRound` containing first a partial attempt and then a succeeded attempt. Call `persist_captured_round()` once and assert in one database read:

```python
assert round_row.status == "succeeded"
assert round_row.attempt_count == 2
assert round_row.selected_ingestion_id == succeeded_run.ingestion_id
assert ingestion_counts == (2, 1, 1)  # total, partial, succeeded
assert manifest_count == 2
assert snapshot_counts == (
    (partial_run.ingestion_id, partial_run.accepted_rows),
    (succeeded_run.ingestion_id, succeeded_run.accepted_rows),
)
```

Then create a second captured round whose terminal run claims two accepted rows but whose tuple repeats the same symbol twice. Expect `IntegrityError`, and assert that the second sample sequence has zero Round rows, zero IngestionRun rows, zero Manifests, zero QualityResults, and zero Snapshots. The mutation caught is splitting round persistence across transactions and leaving partial lineage after a late constraint error.

- [ ] **Step 2: Run the integration test and verify RED**

Run only against the isolated disposable database:

```powershell
uv run pytest -m integration tests/test_postgres_integration.py::test_market_series_persistence_commits_captured_round_atomically -q
```

Expected: fail because `persist_captured_round` is missing. If `TEST_DATABASE_URL` is absent, record that exact environment precondition and do not substitute production.

- [ ] **Step 3: Extract connection-scoped SQL operations**

Refactor existing method bodies into these private methods without changing their SQL or validation behavior:

```python
def _insert_round(self, connection: Connection, round_state: MarketSeriesRound) -> None:

def _insert_ingestion_run(self, connection: Connection, run: IngestionRun) -> None:

def _commit_attempt(
    self,
    connection: Connection,
    run: IngestionRun,
    records: Sequence[MarketSeriesSnapshotRecord],
    manifest: RawManifest,
    quality_results: Sequence[QualityResult],
) -> None:

def _finish_round(self, connection: Connection, round_summary: MarketSeriesRound) -> None:
```

Move the exact Session lock/identity checks and Round INSERT from `start_round` into `_insert_round`. Move the exact dataset/status checks and IngestionRun INSERT from `create_ingestion_run` into `_insert_ingestion_run`. Move the Manifest/record/quality lineage checks, three INSERT groups, and terminal IngestionRun UPDATE from `commit_attempt` into `_commit_attempt`. Move the Round identity/selected-ingestion checks, terminal UPDATE, and `_refresh_session_counts` call from `finish_round` into `_finish_round`.

Keep `start_round`, `create_ingestion_run`, `commit_attempt`, and `finish_round` as compatibility wrappers that each open their existing transaction and pass their existing arguments to the extracted helper. This preserves recovery and current integration coverage while the online Service switches to the new entry point.

- [ ] **Step 4: Implement the atomic entry point**

Import `CapturedAttempt` and `CapturedRound` from `call_auction_market_series_writer`. Add:

```python
def persist_captured_round(self, captured: CapturedRound) -> None:
    with self._engine.begin() as connection:
        self._insert_round(connection, captured.running_round)
        for attempt in captured.attempts:
            running = replace(
                attempt.run,
                status=IngestionStatus.RUNNING,
                finished_at=None,
                fetched_rows=0,
                accepted_rows=0,
                rejected_rows=0,
                error_summary=None,
            )
            self._insert_ingestion_run(connection, running)
            self._commit_attempt(
                connection,
                attempt.run,
                attempt.records,
                attempt.manifest,
                attempt.quality_results,
            )
        self._finish_round(connection, captured.completed_round)
```

Before opening the transaction, reject a captured attempt unless its dataset is `CALL_AUCTION_MARKET_SERIES`, its status is succeeded/partial, its Manifest/record/quality lineage matches the run, and all Snapshot `(session_id, sample_seq)` values match the Round. Reuse the existing checks rather than weakening them.

Change the public finalizer to:

```python
def finish_session(
    self,
    session_id: UUID,
    finished_at: datetime,
    error_summary: str | None = None,
) -> MarketSeriesSession:
    return self._finish_session(session_id, finished_at, error_summary)
```

Missing sequence numbers remain absent Round facts. `_finish_session` already adds them to `failed_rounds` and `failed_quotes`; retain that behavior and prefer the provided Writer error summary over `missed_sampling_rounds`.

- [ ] **Step 5: Run focused PostgreSQL and compatibility tests**

Run:

```powershell
uv run pytest -m integration tests/test_postgres_integration.py::test_market_series_persistence_commits_captured_round_atomically tests/test_postgres_integration.py::test_market_series_persistence_commits_attempt_and_finishes_partial_session tests/test_postgres_integration.py::test_market_series_attempt_rolls_back_manifest_quality_and_facts -q
uv run pytest tests/test_call_auction_market_series_service.py tests/test_call_auction_market_series_writer.py -q
uv run ruff check src/market_data_center/persistence/call_auction_market_series_postgres.py tests/test_postgres_integration.py
uv run mypy src/market_data_center/persistence/call_auction_market_series_postgres.py
```

Expected: whole-round atomic test and old compatibility tests all pass.

- [ ] **Step 6: Commit atomic persistence**

```powershell
git add src/market_data_center/persistence/call_auction_market_series_postgres.py tests/test_postgres_integration.py
git commit -m "feat: persist captured auction rounds atomically"
```

---

### Task 4: Ordered migration and operational documentation

**Files:**
- Create: `supabase/migrations/20260903000200_clarify_auction_series_collection_time.sql`
- Modify: `tests/test_postgres_integration.py:919-1010`
- Modify: `tests/test_production_checks.py:928-956`
- Modify: `docs/领域详设-CallAuctionMarketSeries-2026-08-14.md:3-118`
- Modify: `docs/Worker日常采集与调度.md:120-186`
- Modify: `docs/最小生产发布运行手册.md:75-85`

**Interfaces:**
- Consumes: ADR-0051 and Tasks 1–3 behavior.
- Produces: ordered database comment migration and current operational truth; no table, role, or public contract change.

- [ ] **Step 1: Add a failing PostgreSQL assertion for collection-time semantics**

Extend `test_call_auction_market_series_schema_is_partitioned_and_internal` with a catalog query:

```python
assert connection.scalar(
    text("""
        select col_description(
          'realtime.call_auction_market_series_round'::regclass,
          attnum
        )
        from pg_attribute
        where attrelid='realtime.call_auction_market_series_round'::regclass
          and attname='collected_at'
    """)
) == "Source collection completion time; independent of asynchronous persistence commit time."
```

Change the cleanup migration test so it no longer claims `20260903000100` is the newest migration. Do not replace that assertion with a source-text check for the new comment; the integration test exercises the applied schema behavior.

- [ ] **Step 2: Run the schema assertion and verify RED**

Run:

```powershell
uv run pytest -m integration tests/test_postgres_integration.py::test_call_auction_market_series_schema_is_partitioned_and_internal -q
```

Expected: the column comment is absent before the new migration. If the isolated database setting is absent, do not connect to production.

- [ ] **Step 3: Add the ordered migration**

Create `supabase/migrations/20260903000200_clarify_auction_series_collection_time.sql` containing only:

```sql
comment on column realtime.call_auction_market_series_round.collected_at is
    'Source collection completion time; independent of asynchronous persistence commit time.';
```

Do not add DDL objects, grants, data updates, or transaction-control statements.

- [ ] **Step 4: Update domain and runbook truth**

In the CallAuctionMarketSeries domain design:

- add Issue #72 and ADR-0051 to the document header;
- define `collected_at` as source capture completion rather than database commit time;
- replace the synchronous Steps 5–10 with producer → Raw v2 → bounded 32-item FIFO → single Writer → Session aggregation;
- state that retries use provider/Raw/in-memory elapsed time only;
- state that one Writer transaction contains the Round and all attempts;
- document Writer failure continuation, missing Round aggregation, and replay still being disabled.

In `docs/Worker日常采集与调度.md`, document that the 09:15 job uses one capture producer and one non-daemon database Writer; database backlog can extend Session completion past 09:25:40 but cannot delay later provider slots. Extend diagnostics with `started_at`, `collected_at`, Session `finished_at`, and missing sequence checks so operators can distinguish acquisition delay from Writer drain time.

In `docs/最小生产发布运行手册.md`, extend the next-trading-day live gate to verify 32 provider/Raw captures, 32 terminal Round facts, ordered Writer completion, no missing sequence, and no duplicate Worker instance. Explicitly retain the rule that production collection is not manually triggered for smoke testing.

- [ ] **Step 5: Run migration and documentation-adjacent checks**

Run:

```powershell
uv run pytest tests/test_production_checks.py -q
uv run pytest -m integration tests/test_postgres_integration.py::test_call_auction_market_series_schema_is_partitioned_and_internal -q
git diff --check
```

Expected: production checks and the applied-schema comment assertion pass; the migration remains the only schema artifact added.

- [ ] **Step 6: Commit migration and documentation**

```powershell
git add supabase/migrations/20260903000200_clarify_auction_series_collection_time.sql tests/test_postgres_integration.py tests/test_production_checks.py docs/领域详设-CallAuctionMarketSeries-2026-08-14.md docs/Worker日常采集与调度.md docs/最小生产发布运行手册.md
git commit -m "docs: govern asynchronous auction series persistence"
```

---

### Task 5: Complete verification and handoff

**Files:**
- Modify only files required to correct failures introduced by Tasks 1–4; do not reformat unrelated code.

**Interfaces:**
- Consumes: all implementation, migration, and documentation commits.
- Produces: a verified branch ready for review, merge, protected migration, and next-trading-day live gate.

- [ ] **Step 1: Run the focused non-database regression suite**

```powershell
uv run pytest tests/test_call_auction_market_series.py tests/test_call_auction_market_series_service.py tests/test_call_auction_market_series_writer.py tests/test_operations.py tests/test_scheduler.py tests/test_production_checks.py -q
```

Expected: all focused tests pass with no unhandled thread exception or warning.

- [ ] **Step 2: Run the complete local gate**

```powershell
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: all commands exit zero. If any unrelated pre-existing failure appears, record its exact test/command and prove the focused suite remains green; do not hide or rewrite unrelated user changes.

- [ ] **Step 3: Run the isolated PostgreSQL gate**

```powershell
uv run pytest -m integration
```

Expected: all integration tests pass using `TEST_DATABASE_URL` for an isolated disposable database. Never point this command at production.

- [ ] **Step 4: Confirm contract and repository invariants**

Run:

```powershell
git diff 7a50dcf -- contracts
git diff --check 7a50dcf..HEAD
git status --short
```

Expected: contract diff is empty, diff check is clean, and only intentional uncommitted fixes—if any—are listed.

- [ ] **Step 5: Confirm the task commits and clean worktree**

If a verification failure was caused by Tasks 1–4, return to that task's RED/GREEN step, make the smallest correction in that task's listed files, rerun its focused checks, and commit with that task's message. After all checks pass, run:

```powershell
git log --oneline 7a50dcf..HEAD
git status --short
```

Expected: the log contains the design, Writer, producer, atomic persistence, and governance commits; status output is empty.

- [ ] **Step 6: Prepare deployment evidence without mutating production**

Record the branch commit list, migration filename, focused/full/integration results, and the next-trading-day live-gate SQL from the runbooks. Production push, merge, migration, service restart, or collection execution requires a separate explicit user instruction after review.
