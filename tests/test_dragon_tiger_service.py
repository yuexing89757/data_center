from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerAmountPeriod,
    DragonTigerAmountPeriodBasis,
    DragonTigerEventDraft,
    DragonTigerNormalizationResult,
    DragonTigerReason,
    DragonTigerReasonType,
    DragonTigerSourceFinding,
    DragonTigerTriggerWindow,
    DragonTigerWindowBasis,
    SeatTradeRecord,
)
from market_data_center.domain.ingestion import (
    IngestionRun,
    IngestionStatus,
    QualityResult,
    QualitySeverity,
    RawManifest,
)
from market_data_center.dragon_tiger_service import (
    DragonTigerCollectionError,
    DragonTigerCollectionSummary,
    DragonTigerService,
)
from market_data_center.providers.contracts import DragonTigerProviderBatch, ProviderError
from market_data_center.raw_store import StoredRawObject

TRADE_DATE = date(2026, 8, 20)
INGESTION_ID = UUID("00000000-0000-0000-0000-000000000101")
RAW_ID = UUID("00000000-0000-0000-0000-000000000102")
QUALITY_IDS = tuple(UUID(int=value) for value in range(0x103, 0x120))


def _draft(
    *,
    trade_date: date = TRADE_DATE,
    basis: DragonTigerWindowBasis = DragonTigerWindowBasis.MARKET_SESSIONS,
    sessions: int = 3,
) -> DragonTigerEventDraft:
    reason = DragonTigerReason(
        reason_code=f"PRICE_DEVIATION_{basis.value}_{sessions}",
        reason_name="价格偏离",
        reason_type=DragonTigerReasonType.PRICE_DEVIATION,
        source_code="eastmoney",
        source_reason_code="01",
        source_reason_name="测试原因",
    )
    trade = SeatTradeRecord(
        source_record_id=f"event-{trade_date}:seat-1",
        source_event_id=f"event-{trade_date}",
        symbol="SSE:600000",
        trade_date=trade_date,
        seat_id=None,
        seat_source_key="seat-1",
        seat_name_raw="测试营业部",
        buy_amount=Decimal("100"),
        sell_amount=Decimal("20"),
        buy_rank=1,
        sell_rank=1,
        is_institution=False,
        is_northbound=False,
        source_code="eastmoney",
    )
    return DragonTigerEventDraft(
        source_record_id=f"event-{trade_date}",
        symbol="SSE:600000",
        trade_date=trade_date,
        trigger_window=DragonTigerTriggerWindow(
            basis=basis,
            session_count=sessions,
            occurrence_count=None,
            start_date=None,
            end_date=trade_date,
        ),
        amount_period=DragonTigerAmountPeriod(
            basis=DragonTigerAmountPeriodBasis.SOURCE_UNSPECIFIED,
            session_count=None,
            start_date=None,
            end_date=None,
        ),
        reason=reason,
        reason_name_raw="测试原因",
        close_price=Decimal("10"),
        change_pct=Decimal("7"),
        turnover_amount=Decimal("1000"),
        turnover_rate=Decimal("8"),
        amplitude=None,
        lhb_buy_amount=Decimal("100"),
        lhb_sell_amount=Decimal("20"),
        buy_disclosure_present=True,
        sell_disclosure_present=True,
        seat_trades=(trade,),
        source_code="eastmoney",
    )


class FakeProvider:
    source_code = "eastmoney"

    def __init__(
        self,
        events: list[str],
        *,
        draft: DragonTigerEventDraft | None = None,
        findings: tuple[DragonTigerSourceFinding, ...] = (),
        fail: str | None = None,
    ) -> None:
        self.events = events
        self.draft = draft
        self.findings = findings
        self.fail = fail

    def fetch_dragon_tiger(self, trade_date: date) -> DragonTigerProviderBatch:
        self.events.append("fetch")
        if self.fail == "provider":
            raise ProviderError("secret provider detail")

        def normalize() -> DragonTigerNormalizationResult:
            self.events.append("normalize")
            if self.fail == "normalize":
                raise ProviderError("secret normalization detail")
            return DragonTigerNormalizationResult(
                events=(self.draft or _draft(trade_date=trade_date),),
                findings=self.findings,
            )

        return DragonTigerProviderBatch(
            raw_rows=(
                {"record_kind": "summary", "payload_json": "{}"},
                {"record_kind": "seat", "payload_json": "{}"},
            ),
            request_params={"trade_date": trade_date.isoformat()},
            schema_version="eastmoney.dragon_tiger.v3",
            normalization_factory=normalize,
        )


class FakeRawStore:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def write_jsonl(self, **kwargs: object) -> StoredRawObject:
        self.events.append("raw")
        if self.fail:
            raise OSError("secret filesystem detail")
        return StoredRawObject(
            object_path="eastmoney/dragon_tiger/test.jsonl",
            content_sha256="a" * 64,
            byte_size=4,
            row_count=2,
            file_format="jsonl",
            schema_version=cast(str, kwargs["schema_version"]),
        )


class FakePersistence:
    def __init__(self, events: list[str], *, fail: str | None = None) -> None:
        self.events = events
        self.fail = fail
        self.records: tuple[object, ...] = ()
        self.manifest: RawManifest | None = None
        self.failed_run: IngestionRun | None = None
        self.quality: tuple[QualityResult, ...] = ()

    def is_trading_day(self, trade_date: date) -> bool:
        return trade_date.weekday() < 5

    def period_start_date(self, trade_date: date, session_count: int) -> date:
        self.events.append(f"market-period:{session_count}")
        return trade_date - timedelta(days=session_count - 1)

    def security_traded_period_start_date(
        self, symbol: str, trade_date: date, session_count: int
    ) -> date | None:
        self.events.append(f"security-period:{symbol}:{session_count}")
        if self.fail == "security-period":
            return None
        return trade_date - timedelta(days=session_count - 1)

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]:
        return frozenset({"SSE:600000"})

    def known_trading_dates(self, start_date: date, end_date: date) -> frozenset[date]:
        return frozenset(
            start_date + timedelta(days=i) for i in range((end_date - start_date).days + 1)
        )

    def succeeded_dates(self, start_date: date, end_date: date) -> frozenset[date]:
        return frozenset()

    def begin_ingestion(self, run: IngestionRun) -> None:
        self.events.append("begin")
        assert run.status is IngestionStatus.RUNNING

    def attach_raw_manifest(self, run: IngestionRun, manifest: RawManifest) -> None:
        self.events.append("manifest")
        if self.fail == "manifest":
            raise RuntimeError("secret manifest detail")
        self.manifest = manifest

    def publish_success(
        self,
        run: IngestionRun,
        quality: tuple[QualityResult, ...],
        records: tuple[object, ...],
    ) -> DragonTigerCollectionSummary:
        self.events.append("publish")
        if self.fail == "publish":
            raise RuntimeError("secret database detail")
        self.records = records
        self.quality = quality
        return DragonTigerCollectionSummary(
            status=run.status.value,
            ingestion_id=run.ingestion_id,
            trade_date=cast(object, records[0]).trade_date,
            fetched_rows=run.fetched_rows,
            accepted_events=len(records),
            accepted_seat_trades=1,
            filtered_rows=run.rejected_rows,
        )

    def complete_failure(self, run: IngestionRun, quality: tuple[QualityResult, ...]) -> None:
        self.events.append("failure")
        self.failed_run = run
        self.quality = quality


def _service(
    *,
    draft: DragonTigerEventDraft | None = None,
    findings: tuple[DragonTigerSourceFinding, ...] = (),
    provider_fail: str | None = None,
    raw_fail: bool = False,
    persistence_fail: str | None = None,
) -> tuple[DragonTigerService, FakePersistence, list[str]]:
    events: list[str] = []
    persistence = FakePersistence(events, fail=persistence_fail)
    ids = iter((INGESTION_ID, RAW_ID, *QUALITY_IDS))
    return (
        DragonTigerService(
            persistence=persistence,
            raw_store=FakeRawStore(events, fail=raw_fail),
            provider=FakeProvider(events, draft=draft, findings=findings, fail=provider_fail),
            clock=lambda: datetime(2026, 8, 20, 12, tzinfo=UTC),
            uuid_factory=ids.__next__,
        ),
        persistence,
        events,
    )


def test_collect_durably_registers_run_and_manifest_before_normalization() -> None:
    service, persistence, events = _service()

    summary = service.collect(TRADE_DATE)

    assert events == [
        "begin",
        "fetch",
        "raw",
        "manifest",
        "normalize",
        "market-period:3",
        "publish",
    ]
    assert summary.accepted_events == 1
    assert persistence.manifest is not None


def test_publish_failure_keeps_registered_manifest_and_marks_failed() -> None:
    service, persistence, events = _service(persistence_fail="publish")

    with pytest.raises(DragonTigerCollectionError) as caught:
        service.collect(TRADE_DATE)

    assert caught.value.code == "DT_FACT_PUBLISH_FAILED"
    assert caught.value.phase == "publish"
    assert events[-2:] == ["publish", "failure"]
    assert persistence.manifest is not None
    assert persistence.failed_run is not None
    assert persistence.failed_run.error_summary == "DT_FACT_PUBLISH_FAILED:publish"
    assert "secret" not in persistence.failed_run.error_summary


@pytest.mark.parametrize(
    ("provider_fail", "raw_fail", "persistence_fail", "code", "phase"),
    [
        ("provider", False, None, "DT_PROVIDER_FETCH_FAILED", "fetch"),
        (None, True, None, "DT_RAW_WRITE_FAILED", "raw"),
        (None, False, "manifest", "DT_MANIFEST_ATTACH_FAILED", "manifest"),
        ("normalize", False, None, "DT_NORMALIZATION_FAILED", "normalize"),
    ],
)
def test_collect_uses_stable_phase_error_codes(
    provider_fail: str | None,
    raw_fail: bool,
    persistence_fail: str | None,
    code: str,
    phase: str,
) -> None:
    service, persistence, _ = _service(
        provider_fail=provider_fail,
        raw_fail=raw_fail,
        persistence_fail=persistence_fail,
    )

    with pytest.raises(DragonTigerCollectionError) as caught:
        service.collect(TRADE_DATE)

    assert (caught.value.code, caught.value.phase) == (code, phase)
    assert persistence.failed_run is not None
    assert persistence.failed_run.error_summary == f"{code}:{phase}"


def test_collect_publishes_source_findings_as_nonblocking_quality() -> None:
    finding = DragonTigerSourceFinding(
        rule_code="DT_ZERO_ACTIVITY_PLACEHOLDER_FILTERED",
        severity=QualitySeverity.WARNING,
        source_event_id="event-1",
        report_kind="BUY",
        occurrence_count=1,
        filtered_count=1,
    )
    service, persistence, _ = _service(findings=(finding,))

    summary = service.collect(TRADE_DATE)

    assert summary.filtered_rows == 1
    assert persistence.quality[0].rule_code == finding.rule_code
    assert persistence.quality[0].severity is QualitySeverity.WARNING
    assert persistence.quality[0].blocks_core_write is False


def test_collect_resolves_security_traded_session_window() -> None:
    draft = _draft(basis=DragonTigerWindowBasis.SECURITY_TRADED_SESSIONS)
    service, persistence, events = _service(draft=draft)

    service.collect(TRADE_DATE)

    assert "security-period:SSE:600000:3" in events
    assert cast(object, persistence.records[0]).trigger_window.start_date == date(2026, 8, 18)


def test_collect_publishes_missing_security_traded_start_as_quality() -> None:
    draft = _draft(basis=DragonTigerWindowBasis.SECURITY_TRADED_SESSIONS)
    service, persistence, _ = _service(draft=draft, persistence_fail="security-period")

    summary = service.collect(TRADE_DATE)

    assert summary.status == "succeeded"
    assert cast(object, persistence.records[0]).trigger_window.start_date is None
    finding = next(
        item for item in persistence.quality if item.rule_code == "DT_TRIGGER_START_UNAVAILABLE"
    )
    assert finding.severity is QualitySeverity.WARNING
    assert finding.natural_key == {"source_event_id": f"event-{TRADE_DATE}"}


def test_backfill_continues_after_a_failed_trading_date() -> None:
    service, _, _ = _service()
    attempted: list[date] = []

    def collect(day: date) -> DragonTigerCollectionSummary:
        attempted.append(day)
        if day == date(2026, 8, 19):
            raise DragonTigerCollectionError("DT_PROVIDER_FETCH_FAILED", "fetch")
        return DragonTigerCollectionSummary(
            status="succeeded",
            ingestion_id=INGESTION_ID,
            trade_date=day,
            fetched_rows=2,
            accepted_events=1,
            accepted_seat_trades=1,
            filtered_rows=0,
        )

    service.collect = collect  # type: ignore[method-assign]
    summary = service.backfill(date(2026, 8, 18), date(2026, 8, 20))

    assert attempted == [date(2026, 8, 18), date(2026, 8, 19), date(2026, 8, 20)]
    assert summary.completed_dates == (date(2026, 8, 18), date(2026, 8, 20))
    assert [(failure.trade_date, failure.error_code) for failure in summary.failed_dates] == [
        (date(2026, 8, 19), "DT_PROVIDER_FETCH_FAILED")
    ]


def test_backfill_accepts_730_days_but_rejects_731() -> None:
    service, _, _ = _service()
    service.backfill(TRADE_DATE, TRADE_DATE + timedelta(days=729))
    with pytest.raises(ValueError, match="730"):
        service.backfill(TRADE_DATE, TRADE_DATE + timedelta(days=730))
