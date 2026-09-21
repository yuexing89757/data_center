# Regulation Trigger Query Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Publish exact-date APIs for stocks whose closing data triggered Regulation rules and for officially abnormal stocks in the previous 30 trading sessions with deterministic next-session trigger conditions.

**Architecture:** Complete the existing Regulation bounded context rather than adding a parallel analysis path. Dedicated SSE/SZSE adapters preserve official evidence in Raw, a staged ingestion service publishes immutable official events, the existing pure calculator produces versioned status/rule/warning facts, and two bounded `api_v1` RPCs provide calculation-coherent reads to an API-key-protected FastAPI process.

**Tech Stack:** Python 3.12, urllib/BeautifulSoup/pypdf, SQLAlchemy 2, PostgreSQL PL/pgSQL/JSONB, APScheduler 3, FastAPI, Pydantic v2, pytest, Ruff, mypy, uv.

**Spec:** `docs/superpowers/specs/2026-09-21-regulation-trigger-query-design.md`

## Global Constraints

- Create and link a GitHub Issue before implementation; GitHub Issues are the only task-planning system of record.
- Follow accepted ADR-0048 and record the approved 22:00 schedule and two-query clarification before behavior changes.
- Official events come only from fixed SSE/SZSE official sources; no media, search result, 龙虎榜 reason, or third-party fallback is accepted.
- Keep `calculated_state` and `announced_state` independent; calculated triggers never create official events.
- Use exact `Decimal` values and preserve `None`; never pass prices, amounts, or ratios through `float`.
- Query dates are exact, include no fallback, and must not mix calculation IDs or event watermarks.
- The recent window is exactly the 30 trading sessions ending on the requested trading date.
- Public reads are API-key protected, bounded to 1–500 rows, and may call only `api_v1` RPCs.
- FastAPI datetime fields use `ApiTimestamp` and render Asia/Shanghai `YYYY-MM-DD HH:mm:ss` strings.
- Worker APScheduler is the only scheduling layer; do not add cron or Windows Task Scheduler entries.
- Official source collection, production migration, deployment, task enablement, and historical recomputation require separate explicit production authorization.
- Integration tests require an isolated disposable `TEST_DATABASE_URL`; never point them at production.

---

### Task 1: Governance Issue and Accepted-Design Clarification

**Files:**

- Modify: `docs/adr/ADR-0048-沪深主板与创业板监管异动规则测算.md`
- Modify: `docs/领域详设-Regulation-2026-09-02.md`
- Modify: `docs/superpowers/specs/2026-09-21-regulation-trigger-query-design.md`
- Modify: `README.md`

**Interfaces:**

- Consumes: approved design commit `050d5c5`.
- Produces: one GitHub Issue URL and documentation that makes 22:00, 08:30, the two RPCs, exact-date semantics, and 30-session official-event semantics normative.

- [ ] **Step 1: Create the GitHub Issue with the exact accepted scope**

Run:

```powershell
gh issue create --title "Implement regulation trigger and next-trigger query APIs" --body @'
Implement docs/superpowers/specs/2026-09-21-regulation-trigger-query-design.md.

Acceptance criteria:
- collect traceable SSE/SZSE official abnormal-volatility events with immutable Raw;
- publish Regulation results around 22:00 and reconcile late events at 08:30;
- expose exact-date /api/v1/regulation/triggers;
- expose exact-date /api/v1/regulation/recent-events/next-triggers using 30 trading sessions;
- return ABNORMAL and SERIOUS_ABNORMAL conditions for INDEX_DOWN_2, INDEX_FLAT, INDEX_UP_2;
- keep calculated and announced states independent;
- keep API roles unable to read internal schemas;
- synchronize migrations, tests, contracts, ADR/domain docs, README, and runbook;
- do not perform production migration, enablement, or live collection without separate authorization.
'@
```

Expected: command prints the newly created GitHub Issue URL.

- [ ] **Step 2: Add a failing documentation assertion**

Add to `tests/test_production_checks.py`:

```python
def test_regulation_design_documents_lock_query_and_schedule_semantics() -> None:
    adr = Path("docs/adr/ADR-0048-沪深主板与创业板监管异动规则测算.md").read_text(encoding="utf-8")
    design = Path("docs/领域详设-Regulation-2026-09-02.md").read_text(encoding="utf-8")
    for required in (
        "周一至周五22:00",
        "api_v1.query_regulation_triggers",
        "api_v1.query_regulation_recent_event_next_triggers",
        "最近30个交易日",
        "regulation_event_reconciliation",
    ):
        assert required in adr or required in design
```

- [ ] **Step 3: Run the documentation assertion RED**

Run: `uv run pytest tests/test_production_checks.py::test_regulation_design_documents_lock_query_and_schedule_semantics -q`

Expected: FAIL because the accepted ADR still says 22:30 and does not name both RPCs.

- [ ] **Step 4: Update the accepted documents**

Append an ADR clarification dated `2026-09-21` that states:

```text
- the close workflow starts at 22:00 Asia/Shanghai;
- the 08:30 reconciliation workflow remains separate and version-producing;
- query_regulation_triggers returns calculated closing triggers;
- query_regulation_recent_event_next_triggers selects official events from exactly 30 sessions;
- both RPCs select one latest published calculation ID and never fall back dates;
- CURRENTLY_TRIGGERED is a public mapping of scenario_code=CURRENT, not a new internal reachability value.
```

Update the domain design and README with the same names and semantics. Add the actual Issue URL returned by Step 1 to the ADR, domain design, and approved spec.

- [ ] **Step 5: Run the documentation assertion GREEN**

Run: `uv run pytest tests/test_production_checks.py::test_regulation_design_documents_lock_query_and_schedule_semantics -q`

Expected: PASS.

- [ ] **Step 6: Commit the governance change**

```powershell
git add README.md docs/adr/ADR-0048-沪深主板与创业板监管异动规则测算.md docs/领域详设-Regulation-2026-09-02.md docs/superpowers/specs/2026-09-21-regulation-trigger-query-design.md tests/test_production_checks.py
git commit -m "docs: clarify regulation trigger query contracts"
```

---

### Task 2: Official Event Provider Contract and Bounded Document Parser

**Files:**

- Modify: `pyproject.toml`
- Modify: `uv.lock`
- Modify: `src/market_data_center/providers/contracts.py`
- Modify: `src/market_data_center/providers/__init__.py`
- Create: `src/market_data_center/providers/official_regulation_common.py`
- Create: `tests/test_official_regulation_common.py`

**Interfaces:**

- Consumes: `RegulationEventRecord` from `domain/regulation.py` and generic `ProviderBatch`.
- Produces: `RegulationEventProvider.fetch_events(observed_from, observed_to) -> ProviderBatch[RegulationEventRecord]`, `extract_official_text(content_type, body) -> str`, and `validate_official_url(url, allowed_hosts) -> None`.

- [ ] **Step 1: Write failing contract and parser tests**

Create tests that assert:

```python
def test_html_text_extraction_discards_script_and_normalizes_whitespace() -> None:
    text = extract_official_text(
        "text/html; charset=utf-8",
        b"<html><script>bad()</script><body>属于 股票交易异常波动</body></html>",
    )
    assert text == "属于 股票交易异常波动"


def test_url_validator_rejects_redirect_to_unapproved_host() -> None:
    with pytest.raises(ProviderError, match="official host is not allowed"):
        validate_official_url("https://example.com/a.pdf", frozenset({"www.sse.com.cn"}))


def test_pdf_extraction_rejects_oversized_or_overlong_documents() -> None:
    with pytest.raises(ProviderError, match="document exceeds bounded size"):
        extract_official_text("application/pdf", b"x" * (5 * 1024 * 1024 + 1))
```

Also assert the Protocol signature accepts timezone-aware observation bounds and that `ProviderRecord` includes `RegulationEventRecord`.

- [ ] **Step 2: Run common parser tests RED**

Run: `uv run pytest tests/test_official_regulation_common.py -q`

Expected: FAIL because the common module and Protocol do not exist.

- [ ] **Step 3: Add direct runtime dependencies**

Add these project dependencies and regenerate the lockfile:

```toml
"beautifulsoup4>=4.14,<5",
"pypdf>=6,<7",
```

Run: `uv lock`

Do not rely on transitive packages from AKShare.

- [ ] **Step 4: Implement the provider boundary and parser**

Add to `providers/contracts.py`:

```python
class RegulationEventProvider(Protocol):
    source_code: str

    def fetch_events(
        self, observed_from: datetime, observed_to: datetime
    ) -> "ProviderBatch[RegulationEventRecord]":
        raise NotImplementedError
```

Implement common parsing with these fixed limits:

```python
MAX_DOCUMENT_BYTES = 5 * 1024 * 1024
MAX_PDF_PAGES = 50
MAX_TEXT_CHARS = 500_000
```

Use `BeautifulSoup(body, "html.parser")` for HTML and `PdfReader(BytesIO(body))` for PDF. Reject unknown MIME types, encrypted PDFs, more than 50 pages, blank extracted text, non-HTTPS URLs, non-default ports, credentials in URLs, and hosts outside the passed allowlist. Normalize Unicode width and whitespace without altering signed numbers or dates.

- [ ] **Step 5: Run focused verification GREEN**

Run:

```powershell
uv run pytest tests/test_official_regulation_common.py -q
uv run ruff check src/market_data_center/providers/contracts.py src/market_data_center/providers/official_regulation_common.py tests/test_official_regulation_common.py
uv run mypy src/market_data_center/providers/contracts.py src/market_data_center/providers/official_regulation_common.py
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit the common boundary**

```powershell
git add pyproject.toml uv.lock src/market_data_center/providers/contracts.py src/market_data_center/providers/__init__.py src/market_data_center/providers/official_regulation_common.py tests/test_official_regulation_common.py
git commit -m "feat: define official regulation provider boundary"
```

---

### Task 3: SSE Official Regulation Event Adapter

**Files:**

- Create: `src/market_data_center/providers/sse_regulation.py`
- Modify: `src/market_data_center/providers/__init__.py`
- Create: `tests/test_sse_regulation_provider.py`

**Interfaces:**

- Consumes: `RegulationEventProvider`, `ProviderBatch`, official parser helpers, and `RegulationEventRecord`.
- Produces: `SSEOfficialRegulationEventProvider` with `source_code="sse_official"` and Raw schema `sse.regulation_event.v1`.

- [ ] **Step 1: Write mocked provider tests RED**

Use injected HTTP fetch functions; tests must not call the network. Cover:

```python
def test_sse_provider_accepts_only_explicit_body_conclusion() -> None:
    batch = provider_with_fixture("sse_abnormal_explicit.html").fetch_events(FROM, TO)
    assert batch.schema_version == "sse.regulation_event.v1"
    assert len(batch.raw_rows) == 1
    [event] = batch.records
    assert event.symbol == "SSE:600000"
    assert event.event_level is RegulationRuleLevel.ABNORMAL
    assert event.direction is RegulationDirection.UP
    assert event.explicit_rule_codes == ("SSE_MAIN_ABNORMAL_3D_DEV_UP",)


def test_sse_provider_keeps_title_only_match_in_raw_but_emits_no_event() -> None:
    batch = provider_with_fixture("sse_title_only.html").fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert tuple(batch.records) == ()
```

Also cover serious 10/30-day reasons, multiple reasons in one announcement, unknown direction, unrelated announcements, duplicate pages, truncated pagination, HTTP timeout, redirect host rejection, malformed dates, and a future publication outside the observation bounds.

- [ ] **Step 2: Run SSE tests RED**

Run: `uv run pytest tests/test_sse_regulation_provider.py -q`

Expected: FAIL because the adapter does not exist.

- [ ] **Step 3: Implement bounded SSE retrieval and normalization**

Allow only:

```python
SSE_ALLOWED_HOSTS = frozenset({"www.sse.com.cn"})
SSE_LIST_ROOT = "https://www.sse.com.cn/disclosure/diclosure/public/"
SSE_DOCUMENT_ROOT = "https://www.sse.com.cn/disclosure/listedinfo/announcement/"
MAX_PAGES = 20
MAX_DOCUMENTS = 500
CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 15
```

Fetch list pages in the requested observation interval, store source fields and document bytes/text evidence in Raw-safe string fields, compute lowercase SHA-256 over the normalized official body, and create records only when the body explicitly states abnormal or serious abnormal volatility. Standardize six-digit symbols as `SSE:NNNNNN`; map only SSE mainboard codes supported by the Regulation domain. Never infer direction from market prices.

- [ ] **Step 4: Run SSE verification GREEN**

Run:

```powershell
uv run pytest tests/test_sse_regulation_provider.py tests/test_official_regulation_common.py -q
uv run ruff check src/market_data_center/providers/sse_regulation.py tests/test_sse_regulation_provider.py
uv run mypy src/market_data_center/providers/sse_regulation.py
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit the SSE adapter**

```powershell
git add src/market_data_center/providers/sse_regulation.py src/market_data_center/providers/__init__.py tests/test_sse_regulation_provider.py
git commit -m "feat: adapt SSE regulation events"
```

---

### Task 4: SZSE Official Regulation Event Adapter

**Files:**

- Create: `src/market_data_center/providers/szse_regulation.py`
- Modify: `src/market_data_center/providers/__init__.py`
- Create: `tests/test_szse_regulation_provider.py`

**Interfaces:**

- Consumes: the same common boundary as Task 3.
- Produces: `SZSEOfficialRegulationEventProvider` with `source_code="szse_official"` and Raw schema `szse.regulation_event.v1`.

- [ ] **Step 1: Write mocked SZSE tests RED**

Cover the following concrete assertions:

```python
def test_szse_provider_distinguishes_mainboard_and_gem_rules() -> None:
    events = tuple(provider_with_fixture("szse_main_and_gem.json").fetch_events(FROM, TO).records)
    assert [(item.symbol, item.segment, item.explicit_rule_codes) for item in events] == [
        ("SZSE:000001", RegulationSegment.SZSE_MAIN, ("SZSE_MAIN_ABNORMAL_3D_DEV_UP",)),
        ("SZSE:300001", RegulationSegment.GEM, ("GEM_ABNORMAL_3D_DEV_UP",)),
    ]
```

Also cover mainboard four-count serious rules, GEM three-count serious rules, 10/30-day serious reasons, multiple explicit reasons, unknown direction, duplicate documents, malformed PDF text, pagination cap, timeout, redirect rejection, and title-only false positives.

- [ ] **Step 2: Run SZSE tests RED**

Run: `uv run pytest tests/test_szse_regulation_provider.py -q`

Expected: FAIL because the adapter does not exist.

- [ ] **Step 3: Implement bounded SZSE retrieval and normalization**

Use only these official hosts and bounds:

```python
SZSE_ALLOWED_HOSTS = frozenset({"www.szse.cn", "disc.static.szse.cn"})
SZSE_LIST_ROOTS = (
    "https://www.szse.cn/disclosure/deal/public/",
    "https://www.szse.cn/disclosure/deal/inquiry/",
)
MAX_PAGES = 20
MAX_DOCUMENTS = 500
CONNECT_TIMEOUT_SECONDS = 5
READ_TIMEOUT_SECONDS = 15
```

Resolve board membership from normalized six-digit code rules and require later persistence validation against `core.security`; do not trust prose labels alone. Apply the same evidence, Raw, hash, URL, interval, and no-price-inference rules as the SSE adapter.

- [ ] **Step 4: Run SZSE verification GREEN**

Run:

```powershell
uv run pytest tests/test_szse_regulation_provider.py tests/test_official_regulation_common.py -q
uv run ruff check src/market_data_center/providers/szse_regulation.py tests/test_szse_regulation_provider.py
uv run mypy src/market_data_center/providers/szse_regulation.py
```

Expected: all commands exit 0.

- [ ] **Step 5: Commit the SZSE adapter**

```powershell
git add src/market_data_center/providers/szse_regulation.py src/market_data_center/providers/__init__.py tests/test_szse_regulation_provider.py
git commit -m "feat: adapt SZSE regulation events"
```

---

### Task 5: Staged Official Event Ingestion, Persistence, and Raw Replay

**Files:**

- Create: `src/market_data_center/regulation_event_service.py`
- Create: `src/market_data_center/persistence/regulation_event_postgres.py`
- Modify: `src/market_data_center/persistence/__init__.py`
- Modify: `src/market_data_center/domain/ingestion.py`
- Modify: `src/market_data_center/reliability.py`
- Create: `supabase/migrations/20260921000100_add_regulation_event_ingestion.sql`
- Create: `tests/test_regulation_event_service.py`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_raw_store.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Consumes: `RegulationEventProvider.fetch_events()` from Tasks 2–4 and existing `LocalRawStore`/ingestion models.
- Produces: `RegulationEventCollectionService.collect(observed_from, observed_to) -> RegulationEventCollectionSummary`, `PostgreSQLRegulationEventPersistence`, provider codes `sse_official`/`szse_official`, and dataset code `regulation_event`.

- [ ] **Step 1: Write failing service tests**

Use in-memory fakes to verify the phase order:

```python
def test_event_service_persists_raw_before_normalizing_or_publishing() -> None:
    summary = service.collect(FROM, TO)
    assert calls == ["begin", "fetch", "write_raw", "attach_manifest", "records", "publish"]
    assert summary.accepted_events == 1


def test_changed_source_hash_fails_without_overwriting_existing_event() -> None:
    persistence.existing_hash = "a" * 64
    provider.batch = batch_with_hash("b" * 64)
    with pytest.raises(RegulationEventCollectionError, match="REG_EVENT_CONTENT_CONFLICT"):
        service.collect(FROM, TO)
    assert persistence.updated_events == []
    assert persistence.failed_run is not None
```

Also assert identical content is idempotent, unknown securities are rejected with an ERROR quality finding, source/date/segment mismatches fail, and partial standard facts never publish after a hard error.

- [ ] **Step 2: Write failing migration and integration tests**

Add tests asserting:

- `ProviderCode.SSE_OFFICIAL`, `ProviderCode.SZSE_OFFICIAL`, and `DatasetCode.REGULATION_EVENT` exist;
- the migration preserves every current provider, dataset, and workflow code while adding only the new values;
- `regulation_event_reconciliation` is allowed in `operations.workflow_run`;
- index `regulation_event_period_symbol_idx` is `(period_end_date desc, symbol)`;
- identical events do not duplicate rows;
- same official ID with a different hash raises and leaves the existing row unchanged;
- API roles still cannot select `regulation.event`.

- [ ] **Step 3: Run service/migration tests RED**

Run:

```powershell
uv run pytest tests/test_regulation_event_service.py tests/test_production_checks.py -k "regulation_event" -q
uv run pytest -m integration tests/test_postgres_integration.py -k "regulation_event" -q
```

Expected: unit/production checks fail because the service and migration are absent; integration tests fail against an isolated database for the same reason.

- [ ] **Step 4: Implement the ordered migration and enums**

In the migration:

```sql
create index regulation_event_period_symbol_idx
    on regulation.event (period_end_date desc, symbol);
```

Replace `ingestion_run_provider_check`, `ingestion_run_dataset_check`, and
`workflow_run_workflow_code_check` by copying every value from the newest prior constraints and adding:

```text
sse_official
szse_official
regulation_event
regulation_event_reconciliation
```

Validate every replacement constraint. Do not change public grants on internal Regulation tables.

- [ ] **Step 5: Implement staged collection and atomic persistence**

Define:

```python
@dataclass(frozen=True, slots=True)
class RegulationEventCollectionSummary:
    provider_code: str
    ingestion_id: UUID
    status: IngestionStatus
    observed_from: datetime
    observed_to: datetime
    fetched_rows: int
    accepted_events: int
    unchanged_events: int
```

Implement `RegulationEventCollectionService.collect(observed_from: datetime,
observed_to: datetime) -> RegulationEventCollectionSummary` with the staged sequence below.

Require aware UTC bounds with `observed_from < observed_to`. Begin the ingestion, fetch, write JSONL Raw, attach a SHA-256/size/row-count manifest, force lazy normalization, validate official source identity and known stock facts, then publish events plus terminal ingestion status in one database transaction. Map same-ID/different-hash and semantic identity conflicts to stable quality codes and `FAILED`; never update an existing event row.

- [ ] **Step 6: Add Raw replay support**

Register `sse.regulation_event.v1` and `szse.regulation_event.v1` in `reliability.py`. Replay must verify path, bytes, SHA-256, row count and schema version, create a new IngestionRun referencing the old manifest, and invoke the same normalization/validation/publish path with a no-network provider.

- [ ] **Step 7: Run focused verification GREEN**

Run:

```powershell
uv run pytest tests/test_regulation_event_service.py tests/test_raw_store.py tests/test_production_checks.py -k "regulation_event or raw_replay" -q
uv run pytest -m integration tests/test_postgres_integration.py -k "regulation_event" -q
uv run ruff check src/market_data_center/regulation_event_service.py src/market_data_center/persistence/regulation_event_postgres.py tests/test_regulation_event_service.py
uv run mypy src/market_data_center/regulation_event_service.py src/market_data_center/persistence/regulation_event_postgres.py
```

Expected: all commands exit 0 using the isolated integration database.

- [ ] **Step 8: Commit event ingestion**

```powershell
git add src/market_data_center/regulation_event_service.py src/market_data_center/persistence/regulation_event_postgres.py src/market_data_center/persistence/__init__.py src/market_data_center/domain/ingestion.py src/market_data_center/reliability.py supabase/migrations/20260921000100_add_regulation_event_ingestion.sql tests/test_regulation_event_service.py tests/test_postgres_integration.py tests/test_raw_store.py tests/test_production_checks.py
git commit -m "feat: ingest official regulation events"
```

---

### Task 6: 22:00 Daily Workflow and 08:30 Reconciliation

**Files:**

- Modify: `src/market_data_center/domain/operations.py`
- Modify: `src/market_data_center/settings.py`
- Modify: `src/market_data_center/scheduling_catalog.py`
- Modify: `src/market_data_center/scheduler.py`
- Modify: `src/market_data_center/cli.py`
- Modify: `tests/test_regulation_scheduler.py`
- Modify: `tests/test_regulation_service.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Consumes: both official Provider adapters, `RegulationEventCollectionService`, `RegulationBenchmarkService`, `RegulationService`, and workflow-code support from Task 5.
- Produces: `run_regulation_daily_calculation_job()` at 22:00, `run_regulation_event_reconciliation_job()` at 08:30, and manual exact-date commands for controlled recovery.

- [ ] **Step 1: Write scheduler/catalog tests RED**

Add assertions:

```python
def test_regulation_jobs_are_opt_in_at_2200_and_0830(tmp_path: Path) -> None:
    settings = SchedulerSettings(_env_file=None)
    assert settings.regulation_daily_enabled is False
    assert settings.regulation_event_reconciliation_enabled is False
    jobs = {item.job_id: item for item in job_definitions(settings)}
    assert (
        jobs["regulation-daily-calculation"].hour,
        jobs["regulation-daily-calculation"].minute,
    ) == (22, 0)
    assert (
        jobs["regulation-event-reconciliation"].hour,
        jobs["regulation-event-reconciliation"].minute,
    ) == (8, 30)
```

Assert the daily workflow step order is SSE, SZSE, benchmark, validate, calculate, publish; non-trading dates create successful zero-work steps; reconciliation does not collect ordinary stock bars; missing `daily_market` terminal state fails before publication.

- [ ] **Step 2: Run scheduler tests RED**

Run: `uv run pytest tests/test_regulation_scheduler.py tests/test_regulation_service.py -q`

Expected: FAIL because the current catalog is 22:30 and has no reconciliation job.

- [ ] **Step 3: Implement catalog/settings/operations changes**

Add:

```python
REGULATION_EVENT_RECONCILIATION_JOB_ID = "regulation-event-reconciliation"
```

and `WorkflowCode.REGULATION_EVENT_RECONCILIATION`. Add
`regulation_event_reconciliation_enabled: bool = False`. Change the daily cron to `hour=22,
minute=0`; add the reconciliation cron at `hour=8, minute=30`, Monday–Friday,
`Asia/Shanghai`, also disabled by default.

- [ ] **Step 4: Implement the daily workflow**

Before collection, require same-date `daily_market` to be `SUCCEEDED` or `PARTIAL`. Execute separate SSE and SZSE services so their IngestionRuns and Raw remain independent. Collect all three benchmark indices, validate rules/calendar/required facts, invoke `RegulationService.calculate(trade_date)`, and publish only through its existing transaction boundary. Record stable Operations step names and counts.

- [ ] **Step 5: Implement reconciliation**

The 08:30 job observes from the prior successful official-event watermark through its current fire time. If neither provider publishes a new event, finish without calculation. For newly appended events, derive affected trading dates from each event's `period_end_date` through the latest completed trading date, cap the union to 30 sessions, and call `RegulationService.calculate(date)` in ascending date order. Same-ID/different-hash conflicts fail the job and publish no revised event.

Add manual commands:

```text
market-data-center regulation-events-collect --observed-from <ISO8601> --observed-to <ISO8601>
market-data-center regulation-reconcile --as-of-date YYYY-MM-DD
```

Both commands are opt-in operations and must not silently use the current date when an argument is missing.

- [ ] **Step 6: Run scheduler/service tests GREEN**

Run:

```powershell
uv run pytest tests/test_regulation_scheduler.py tests/test_regulation_service.py tests/test_regulation_benchmark_service.py -q
uv run ruff check src/market_data_center/domain/operations.py src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py src/market_data_center/cli.py
uv run mypy src/market_data_center/domain/operations.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py
```

Expected: all commands exit 0.

- [ ] **Step 7: Commit Worker workflows**

```powershell
git add src/market_data_center/domain/operations.py src/market_data_center/settings.py src/market_data_center/scheduling_catalog.py src/market_data_center/scheduler.py src/market_data_center/cli.py tests/test_regulation_scheduler.py tests/test_regulation_service.py tests/test_production_checks.py
git commit -m "feat: schedule regulation event calculation and reconciliation"
```

---

### Task 7: Bounded Regulation Query RPCs

**Files:**

- Create: `supabase/migrations/20260921000200_add_regulation_trigger_query_apis.sql`
- Modify: `tests/test_postgres_integration.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Consumes: published Regulation calculation/event tables.
- Produces: `api_v1.query_regulation_triggers(date,text,integer) -> jsonb` and `api_v1.query_regulation_recent_event_next_triggers(date,text,integer) -> jsonb`.

- [ ] **Step 1: Write failing exact-version and aggregation tests**

Seed two completed calculations, one running calculation, and one failed calculation for the same date. Assert each RPC selects only the newest `SUCCEEDED`/`PARTIAL` calculation, never combines IDs, and returns `calculation_id`, `completed_at`, `event_watermark`, versions, coverage, `returned_count`, `next_cursor`, and items.

For `query_regulation_triggers`, seed one stock with two triggered rules and assert it returns one stock item with two `triggered_rules`, with serious state sorted before abnormal state.

For the recent-event RPC, seed a 30-session calendar plus events just inside and just outside the window. Assert only the inside event observed at or before the selected calculation watermark is returned.

- [ ] **Step 2: Write failing cursor, permission, and precision tests**

Assert:

```python
assert payload["items"][0]["next_triggers"][0]["trigger_change_pct"] == "6.48000000"
assert second_page_symbols.isdisjoint(first_page_symbols)
assert combined_symbols == expected_order
```

Reject null/pre-effective/non-trading dates, limits outside 1–500, malformed cursor, wrong cursor version, and a cursor reused across RPCs with SQLSTATE `22023`. Return `P0002` when the exact date has no completed calculation. Verify API roles can execute the RPCs but cannot select internal tables.

- [ ] **Step 3: Run RPC tests RED**

Run:

```powershell
uv run pytest tests/test_production_checks.py -k "regulation_trigger_query" -q
uv run pytest -m integration tests/test_postgres_integration.py -k "regulation_trigger_query" -q
```

Expected: FAIL because the migration/functions are absent.

- [ ] **Step 4: Implement `query_regulation_triggers`**

Create a `stable security definer` function with locked `search_path`, `set statement_timeout='5s'`, explicit validation, and one selected CalculationRun CTE. Aggregate triggered `rule_result` rows per stock. Join the security name valid on `p_trade_date`. Cast every numeric/Decimal field to text before building JSON. Encode the keyset cursor as base64url JSON containing:

```json
{"v":1,"endpoint":"triggers","state_rank":2,"symbol":"SZSE:000001"}
```

Order by serious rank descending, abnormal rank descending, then symbol ascending.

- [ ] **Step 5: Implement `query_regulation_recent_event_next_triggers`**

Derive the exact 30-session set from `core.trading_calendar`, including `p_trade_date`. Select official events with `period_end_date` in that set and `observed_at <= event_watermark`. Aggregate unique natural keys per stock, select the latest event deterministically by `period_end_date DESC, published_at DESC, source_code, source_event_id`, and attach warning rows from the same calculation ID.

Map warning scenarios as follows:

```text
CURRENT -> CURRENTLY_TRIGGERED
NONE -> NOT_PRICE_CALCULABLE
INDEX_DOWN_2 / INDEX_FLAT / INDEX_UP_2 -> stored reachability
```

Return both ordinary and serious levels. Encode the cursor as:

```json
{"v":1,"endpoint":"recent-next","latest_event_date":"2026-09-18","symbol":"SZSE:000001"}
```

Order by latest event date descending, then symbol ascending.

- [ ] **Step 6: Lock grants and indexes**

Revoke execute from `public`, `anon`, and `authenticated`. Conditionally grant execute only to `market_data_api`. Do not grant internal table access. Reuse the event index from Task 5 and existing calculation/status/rule/warning indexes; add no speculative indexes unless the isolated integration `EXPLAIN` shows a sequential scan over `regulation.event` or selected calculation rows.

- [ ] **Step 7: Run RPC verification GREEN**

Run:

```powershell
uv run pytest tests/test_production_checks.py -k "regulation_trigger_query" -q
uv run pytest -m integration tests/test_postgres_integration.py -k "regulation_trigger_query" -q
```

Expected: all tests pass; `EXPLAIN` uses bounded indexed access and both functions enforce the 5-second timeout.

- [ ] **Step 8: Commit the public database contract**

```powershell
git add supabase/migrations/20260921000200_add_regulation_trigger_query_apis.sql tests/test_postgres_integration.py tests/test_production_checks.py
git commit -m "feat: expose regulation trigger query RPCs"
```

---

### Task 8: FastAPI Models, Query Service, and Routes

**Files:**

- Modify: `src/market_data_center/public_api/models.py`
- Modify: `src/market_data_center/public_api/queries.py`
- Modify: `src/market_data_center/public_api/app.py`
- Modify: `src/market_data_center/public_api/openapi_zh.py`
- Modify: `tests/test_public_api.py`
- Modify: `tests/test_api_contracts.py`

**Interfaces:**

- Consumes: the two `api_v1` RPCs from Task 7.
- Produces: `RegulationTriggerResponse`, `RegulationRecentNextTriggerResponse`, `PublicQueryService.regulation_triggers()`, `PublicQueryService.regulation_recent_next_triggers()`, and the two approved GET routes.

- [ ] **Step 1: Write failing response-model tests**

Add representative payloads and assert:

```python
assert RegulationTriggerResponse.model_validate(payload).items[0].triggered_rules[
    0
].threshold == Decimal("20")
assert RegulationRecentNextTriggerResponse.model_validate(payload).items[0].next_triggers[
    0
].trigger_change_pct == Decimal("6.48")
```

Assert `completed_at`, `event_watermark`, and `latest_event_published_at` use `ApiTimestamp`; dates use `date`; numeric fields are `Decimal | None`; six-digit codes retain leading zeros; response models reject invalid reachability/scenario combinations.

- [ ] **Step 2: Write failing route/query tests**

Test:

```python
response = client.get(
    "/api/v1/regulation/triggers",
    params={"trade_date": "2026-09-18", "limit": 200},
    headers=API_HEADERS,
)
assert response.status_code == 200
assert response.json()["trade_date"] == "2026-09-18"
```

Repeat for `/api/v1/regulation/recent-events/next-triggers`. Assert missing API key is 401, malformed inputs are 422, `PublicQueryNotFound` is 404, timeout is 503, PARTIAL stays 200 with coverage, and neither route calls a live provider or write service.

- [ ] **Step 3: Run FastAPI tests RED**

Run: `uv run pytest tests/test_public_api.py tests/test_api_contracts.py -k "regulation" -q`

Expected: FAIL because models, service methods, and routes are absent.

- [ ] **Step 4: Implement exact Pydantic models**

Add focused model groups:

```python
class RegulationCoverage(ApiModel):
    expected_count: int = Field(ge=0)
    complete_count: int = Field(ge=0)
    incomplete_count: int = Field(ge=0)
    not_applicable_count: int = Field(ge=0)


class RegulationTriggeredRuleItem(ApiModel):
    rule_code: str
    level: Literal["ABNORMAL", "SERIOUS_ABNORMAL"]
    direction: Literal["UP", "DOWN", "NONE"]
    kind: Literal["CUMULATIVE_DEVIATION", "TURNOVER_COMPOSITE", "EVENT_COUNT"]
    window_start_date: date | None
    window_end_date: date | None
    observed_window_days: int | None
    current_value: Decimal | None
    threshold: Decimal | None
    secondary_current_value: Decimal | None
    secondary_threshold: Decimal | None
    event_count: int | None
    required_count: int | None
    selected_reset_date: date | None


class RegulationTriggerStockItem(ApiModel):
    code: str = Field(pattern=r"^[0-9]{6}$")
    symbol: str = Field(pattern=r"^(SSE|SZSE):[0-9]{6}$")
    name: str
    exchange: Literal["SSE", "SZSE"]
    segment: Literal["SSE_MAIN", "SZSE_MAIN", "GEM"]
    calculated_state: Literal["ABNORMAL_TRIGGERED", "SERIOUS_TRIGGERED"]
    announced_state: Literal["NONE", "ABNORMAL", "SERIOUS_ABNORMAL"]
    triggered_rules: list[RegulationTriggeredRuleItem]


class RegulationTriggerResponse(ApiModel):
    trade_date: date
    calculation_id: UUID
    calculation_status: Literal["SUCCEEDED", "PARTIAL"]
    completed_at: ApiTimestamp
    event_watermark: ApiTimestamp
    algorithm_version: str
    rule_set_version: str
    coverage: RegulationCoverage
    returned_count: int = Field(ge=0)
    next_cursor: str | None
    items: list[RegulationTriggerStockItem]


class RegulationNextTriggerItem(ApiModel):
    level: Literal["ABNORMAL", "SERIOUS_ABNORMAL"]
    direction: Literal["UP", "DOWN", "NONE"]
    rule_code: str
    benchmark_symbol: str | None
    scenario_code: Literal["CURRENT", "NONE", "INDEX_DOWN_2", "INDEX_FLAT", "INDEX_UP_2"]
    scenario_index_pct: Decimal | None
    next_day_reference_price: Decimal | None
    raw_trigger_price: Decimal | None
    trigger_price: Decimal | None
    trigger_change_pct: Decimal | None
    lower_limit_price: Decimal | None
    upper_limit_price: Decimal | None
    reachability: Literal[
        "CURRENTLY_TRIGGERED",
        "NOT_PRICE_CALCULABLE",
        "REACHABLE_NEXT_SESSION",
        "NOT_REACHABLE_NEXT_SESSION",
    ]
    window_start_date: date | None
    window_end_date: date | None
    requires_official_event_confirmation: bool


class RegulationRecentEventStockItem(ApiModel):
    code: str = Field(pattern=r"^[0-9]{6}$")
    symbol: str = Field(pattern=r"^(SSE|SZSE):[0-9]{6}$")
    name: str
    exchange: Literal["SSE", "SZSE"]
    segment: Literal["SSE_MAIN", "SZSE_MAIN", "GEM"]
    official_event_count_30d: int = Field(ge=1)
    latest_source_event_id: str
    latest_event_date: date
    latest_event_published_at: ApiTimestamp
    latest_event_level: Literal["ABNORMAL", "SERIOUS_ABNORMAL"]
    latest_event_direction: Literal["UP", "DOWN"] | None
    latest_event_source_title: str
    latest_event_source_url: str
    next_triggers: list[RegulationNextTriggerItem]


class RegulationRecentNextTriggerResponse(ApiModel):
    trade_date: date
    next_trade_date: date
    lookback_trading_days: Literal[30]
    lookback_start_date: date
    calculation_id: UUID
    calculation_status: Literal["SUCCEEDED", "PARTIAL"]
    completed_at: ApiTimestamp
    event_watermark: ApiTimestamp
    algorithm_version: str
    rule_set_version: str
    coverage: RegulationCoverage
    returned_count: int = Field(ge=0)
    next_cursor: str | None
    items: list[RegulationRecentEventStockItem]
```

Use `Literal` for fixed enums, `Decimal` for all numeric market values, `ApiTimestamp` for datetimes, and calendar-valid OpenAPI examples. Keep missing values `None`.

- [ ] **Step 5: Implement read-only query-service methods**

Add SQLAlchemy text calls:

```python
QUERY_REGULATION_TRIGGERS = text(
    "select api_v1.query_regulation_triggers(:trade_date, :cursor, :limit) as payload"
)
QUERY_REGULATION_RECENT_NEXT = text(
    "select api_v1.query_regulation_recent_event_next_triggers(:trade_date, :cursor, :limit) as payload"
)
```

Validate returned JSON through the response models. Use the existing `_execute` and safe SQLSTATE mapping; do not query internal schemas.

- [ ] **Step 6: Implement the two routes and Chinese OpenAPI text**

Add:

```python
@app.get("/api/v1/regulation/triggers", response_model=RegulationTriggerResponse)
def regulation_triggers(
    _: ApiKeyDependency,
    service: QueryServiceDependency,
    trade_date: Annotated[date, Query(description="需要精确查询的交易日。")],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    cursor: Annotated[str | None, Query(description="上一页返回的不透明游标。")] = None,
) -> RegulationTriggerResponse:
    return service.regulation_triggers(trade_date, cursor, limit)


@app.get(
    "/api/v1/regulation/recent-events/next-triggers",
    response_model=RegulationRecentNextTriggerResponse,
)
def regulation_recent_next_triggers(
    _: ApiKeyDependency,
    service: QueryServiceDependency,
    trade_date: Annotated[date, Query(description="需要精确查询的交易日。")],
    limit: Annotated[int, Query(ge=1, le=500)] = 100,
    cursor: Annotated[str | None, Query(description="上一页返回的不透明游标。")] = None,
) -> RegulationRecentNextTriggerResponse:
    return service.regulation_recent_next_triggers(trade_date, cursor, limit)
```

Require `trade_date`; constrain `limit` to 1–500; allow optional cursor; keep API-key dependency. Chinese descriptions must state exact date, no fallback, official-event 30-session window, three index scenarios, and the non-prediction disclaimer.

- [ ] **Step 7: Run FastAPI verification GREEN**

Run:

```powershell
uv run pytest tests/test_public_api.py tests/test_api_contracts.py -k "regulation" -q
uv run ruff check src/market_data_center/public_api tests/test_public_api.py tests/test_api_contracts.py
uv run mypy src/market_data_center/public_api
```

Expected: all commands exit 0.

- [ ] **Step 8: Commit FastAPI support**

```powershell
git add src/market_data_center/public_api/models.py src/market_data_center/public_api/queries.py src/market_data_center/public_api/app.py src/market_data_center/public_api/openapi_zh.py tests/test_public_api.py tests/test_api_contracts.py
git commit -m "feat(api): add regulation trigger queries"
```

---

### Task 9: Contracts, Runbook, and Complete Verification

**Files:**

- Modify: `contracts/postgrest-openapi-v1.json`
- Modify: `contracts/agent-tools-v1.json`
- Modify: `contracts/fastapi-openapi-v1.json`
- Modify: `README.md`
- Modify: `docs/Worker日常采集与调度.md`
- Modify: `.env.example`
- Modify: `tests/test_api_contracts.py`
- Modify: `tests/test_production_checks.py`

**Interfaces:**

- Consumes: completed provider, ingestion, scheduling, RPC, and FastAPI work.
- Produces: synchronized checked-in contracts and an operator-visible, still-disabled production configuration.

- [ ] **Step 1: Write failing checked-contract assertions**

Assert all three contracts include both query operations, exact date, limit bounds, cursor, response enums, Decimal strings, and timestamp formats. Assert the FastAPI contract contains calendar-valid examples and does not expose internal schemas, Raw paths, ingestion IDs, database URLs, or provider payload fields.

- [ ] **Step 2: Run contract tests RED**

Run: `uv run pytest tests/test_api_contracts.py tests/test_production_checks.py -k "regulation" -q`

Expected: FAIL because checked-in contracts and operations documentation are stale.

- [ ] **Step 3: Regenerate and review contracts**

Use the repository's existing OpenAPI generation path to regenerate `contracts/fastapi-openapi-v1.json`. Update PostgREST and Agent Tools contracts with the exact two RPC signatures and response schemas. Review the diff to ensure only Regulation operations and shared referenced schemas changed.

- [ ] **Step 4: Update operator documentation and safe defaults**

Document:

```text
REGULATION_DAILY_ENABLED=false
REGULATION_EVENT_RECONCILIATION_ENABLED=false
```

Describe 22:00 and 08:30 workflows, prerequisites, exact manual commands, event-content conflict handling, safe retry, PARTIAL interpretation, and the explicit production source-rights/enablement gate. Do not include secrets, database URLs, Raw contents, cron commands, or deployment authorization.

- [ ] **Step 5: Run focused and complete verification**

Run fresh commands:

```powershell
uv sync --all-groups
uv run pytest tests/test_official_regulation_common.py tests/test_sse_regulation_provider.py tests/test_szse_regulation_provider.py tests/test_regulation_event_service.py tests/test_regulation.py tests/test_regulation_calculator.py tests/test_regulation_persistence.py tests/test_regulation_service.py tests/test_regulation_scheduler.py tests/test_public_api.py tests/test_api_contracts.py tests/test_production_checks.py -q
uv run pytest -m integration tests/test_postgres_integration.py -k "regulation" -q
uv run ruff format --check .
uv run ruff check .
uv run mypy src
uv run pytest
```

Expected: every command exits 0. If `TEST_DATABASE_URL` is unavailable, stop and report the exact skipped integration command; do not claim the full gate passed.

- [ ] **Step 6: Verify scope and secret hygiene**

Run:

```powershell
git diff --check
git status --short
git diff --name-only
rg -n "postgresql://|FASTAPI_API_KEY=.+|token=.+|BEGIN (RSA|OPENSSH) PRIVATE KEY" README.md docs src tests contracts .env.example
```

Expected: no diff errors; only planned files changed; secret scan returns no real credentials. Review every match before continuing because examples and variable names may be benign.

- [ ] **Step 7: Commit contracts and operations documentation**

```powershell
git add contracts/postgrest-openapi-v1.json contracts/agent-tools-v1.json contracts/fastapi-openapi-v1.json README.md docs/Worker日常采集与调度.md .env.example tests/test_api_contracts.py tests/test_production_checks.py
git commit -m "docs: publish regulation query contracts"
```

- [ ] **Step 8: Stop before production mutation**

Report the commit list, full verification evidence, isolated integration database used, and remaining production gates. Do not push, run production migrations, enable jobs, perform live official-source collection, or deploy until the user explicitly requests those operations.
