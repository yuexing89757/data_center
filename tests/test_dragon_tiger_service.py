from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerEventDraft,
    DragonTigerPeriodType,
    DragonTigerReason,
    DragonTigerReasonType,
    SeatTradeRecord,
)
from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.dragon_tiger_service import (
    DragonTigerCollectionSummary,
    DragonTigerService,
)
from market_data_center.providers.contracts import ProviderBatch
from market_data_center.raw_store import StoredRawObject

TRADE_DATE = date(2026, 8, 20)
INGESTION_ID = UUID("00000000-0000-0000-0000-000000000101")
RAW_ID = UUID("00000000-0000-0000-0000-000000000102")


def _draft(period: DragonTigerPeriodType) -> DragonTigerEventDraft:
    reason = DragonTigerReason(
        reason_code=f"PRICE_DEVIATION_{period.value}",
        reason_name="价格偏离",
        reason_type=DragonTigerReasonType.PRICE_DEVIATION,
        period_type=period,
        source_code="eastmoney",
        source_reason_code="01",
        source_reason_name="测试原因",
    )
    trade = SeatTradeRecord(
        source_record_id="event-1:seat-1",
        source_event_id="event-1",
        symbol="SSE:600000",
        trade_date=TRADE_DATE,
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
        source_record_id="event-1",
        symbol="SSE:600000",
        trade_date=TRADE_DATE,
        period_type=period,
        period_start_date=TRADE_DATE if period is DragonTigerPeriodType.DAY else None,
        period_end_date=TRADE_DATE,
        reason=reason,
        reason_name_raw="测试原因",
        close_price=Decimal("10"),
        change_pct=Decimal("7"),
        turnover_amount=Decimal("1000"),
        turnover_rate=Decimal("8"),
        amplitude=None,
        lhb_buy_amount=Decimal("100"),
        lhb_sell_amount=Decimal("20"),
        seat_trades=(trade,),
        source_code="eastmoney",
    )


def _draft_on_date(trade_date: date) -> DragonTigerEventDraft:
    draft = _draft(DragonTigerPeriodType.DAY)
    return replace(
        draft,
        trade_date=trade_date,
        period_start_date=trade_date,
        period_end_date=trade_date,
        seat_trades=tuple(replace(trade, trade_date=trade_date) for trade in draft.seat_trades),
    )


class FakeProvider:
    source_code = "eastmoney"

    def __init__(self, events: list[str], period: DragonTigerPeriodType) -> None:
        self.events = events
        self.period = period
        self.draft: DragonTigerEventDraft | None = None

    def fetch_dragon_tiger(self, trade_date: date) -> ProviderBatch[DragonTigerEventDraft]:
        self.events.append("fetch")

        def normalize() -> tuple[DragonTigerEventDraft, ...]:
            self.events.append("normalize")
            assert "raw" in self.events
            return (self.draft or _draft(self.period),)

        return ProviderBatch(
            raw_rows=(
                {"record_kind": "summary", "payload_json": "{}"},
                {"record_kind": "seat", "payload_json": "{}"},
            ),
            request_params={"trade_date": trade_date.isoformat()},
            schema_version="eastmoney.dragon_tiger.v2",
            record_factory=normalize,
        )


class FakeRawStore:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def write_jsonl(self, **kwargs: object) -> StoredRawObject:
        self.events.append("raw")
        assert kwargs["provider"] == "eastmoney"
        assert kwargs["dataset"] == "dragon_tiger"
        return StoredRawObject(
            object_path="eastmoney/dragon_tiger/test.jsonl",
            content_sha256="a" * 64,
            byte_size=4,
            row_count=2,
            file_format="jsonl",
            schema_version=cast(str, kwargs["schema_version"]),
        )


class FakePersistence:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.records = ()
        self.failed_run: IngestionRun | None = None

    def is_trading_day(self, trade_date: date) -> bool:
        return trade_date == TRADE_DATE

    def period_start_date(self, trade_date: date, session_count: int) -> date:
        self.events.append(f"period:{session_count}")
        assert trade_date == TRADE_DATE
        return date(2026, 8, 18)

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]:
        return frozenset({"SSE:600000"})

    def known_trading_dates(self, start_date: date, end_date: date) -> frozenset[date]:
        return frozenset({date(2026, 8, 18), date(2026, 8, 19), TRADE_DATE})

    def commit_success(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: tuple[QualityResult, ...],
        records: tuple[object, ...],
    ) -> DragonTigerCollectionSummary:
        self.events.append("commit")
        self.records = records
        return DragonTigerCollectionSummary(
            status=run.status.value,
            ingestion_id=run.ingestion_id,
            trade_date=TRADE_DATE,
            fetched_rows=run.fetched_rows,
            accepted_events=len(records),
            accepted_seat_trades=1,
            filtered_rows=run.rejected_rows,
        )

    def commit_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality: tuple[QualityResult, ...],
    ) -> None:
        self.events.append("failure")
        self.failed_run = run


def _service(
    period: DragonTigerPeriodType,
) -> tuple[DragonTigerService, FakePersistence, list[str]]:
    events: list[str] = []
    persistence = FakePersistence(events)
    service = DragonTigerService(
        persistence=persistence,
        raw_store=FakeRawStore(events),
        provider=FakeProvider(events, period),
        clock=lambda: datetime(2026, 8, 20, 12, tzinfo=UTC),
        uuid_factory=iter((INGESTION_ID, RAW_ID)).__next__,
    )
    return service, persistence, events


def test_collect_writes_raw_before_normalization_and_commits_once() -> None:
    service, persistence, events = _service(DragonTigerPeriodType.DAY)

    summary = service.collect(TRADE_DATE)

    assert events == ["fetch", "raw", "normalize", "commit"]
    assert summary.accepted_events == 1
    assert summary.fetched_rows == 2
    assert summary.filtered_rows == 0
    assert persistence.records[0].period_start_date == TRADE_DATE


def test_collect_resolves_three_day_start_from_the_unified_calendar() -> None:
    service, persistence, events = _service(DragonTigerPeriodType.THREE_DAY)

    service.collect(TRADE_DATE)

    assert "period:3" in events
    assert persistence.records[0].period_start_date == date(2026, 8, 18)


@pytest.mark.parametrize(
    ("draft", "message"),
    [
        (replace(_draft(DragonTigerPeriodType.DAY), source_code="tushare"), "source"),
        (
            _draft_on_date(date(2026, 8, 19)),
            "date",
        ),
    ],
)
def test_collect_rejects_provider_or_date_lineage_mismatch(
    draft: DragonTigerEventDraft, message: str
) -> None:
    events: list[str] = []
    provider = FakeProvider(events, DragonTigerPeriodType.DAY)
    provider.draft = draft
    persistence = FakePersistence(events)
    service = DragonTigerService(
        persistence=persistence,
        raw_store=FakeRawStore(events),
        provider=provider,
        clock=lambda: datetime(2026, 8, 20, 12, tzinfo=UTC),
        uuid_factory=iter(
            (INGESTION_ID, RAW_ID, UUID("00000000-0000-0000-0000-000000000103"))
        ).__next__,
    )

    with pytest.raises(ValueError, match=message):
        service.collect(TRADE_DATE)

    assert events == ["fetch", "raw", "normalize", "failure"]
    assert persistence.failed_run is not None
