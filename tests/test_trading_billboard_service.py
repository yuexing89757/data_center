from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from typing import cast
from uuid import UUID

import pytest

from market_data_center.domain import (
    IngestionRun,
    QualityResult,
    RawManifest,
    TradingBillboardRecord,
    TradingBillboardSeatRecord,
    TradingBillboardSide,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError
from market_data_center.raw_store import StoredRawObject
from market_data_center.trading_billboard_service import (
    TradingBillboardBackfillSummary,
    TradingBillboardCollectionSummary,
    TradingBillboardService,
    TradingBillboardValidationError,
)

TRADE_DATE = date(2026, 8, 17)
NOW = datetime(2026, 8, 17, 12, tzinfo=UTC)
INGESTION_ID = UUID("00000000-0000-0000-0000-000000000101")
RAW_ID = UUID("00000000-0000-0000-0000-000000000102")


def _seat(side: TradingBillboardSide) -> TradingBillboardSeatRecord:
    return TradingBillboardSeatRecord(
        source_event_id="event-1",
        symbol="SZSE:000711",
        trade_date=TRADE_DATE,
        side=side,
        rank=1,
        seat_code=None,
        seat_name="机构专用",
        buy_amount=Decimal("120"),
        sell_amount=Decimal("20"),
        net_amount=Decimal("100"),
        buy_to_market_pct=Decimal("1.2"),
        sell_to_market_pct=Decimal("0.2"),
    )


def _record(**changes: object) -> TradingBillboardRecord:
    record = TradingBillboardRecord(
        symbol="SZSE:000711",
        trade_date=TRADE_DATE,
        source_event_id="event-1",
        reason_code="106001",
        reason_text="测试原因",
        close_price=Decimal("12.34"),
        change_rate_pct=Decimal("9.9"),
        turnover_rate_pct=Decimal("12.5"),
        market_amount=Decimal("10000"),
        buy_amount=Decimal("600"),
        sell_amount=Decimal("400"),
        net_amount=Decimal("200"),
        deal_amount=Decimal("1000"),
        deal_to_market_pct=Decimal("10"),
        net_to_market_pct=Decimal("2"),
        free_float_market_value=Decimal("50000"),
        buy_seats=(_seat(TradingBillboardSide.BUY),),
        sell_seats=(_seat(TradingBillboardSide.SELL),),
    )
    return replace(record, **changes)


class FakeProvider:
    source_code = "eastmoney"

    def __init__(
        self,
        events: list[str],
        record: TradingBillboardRecord | None = None,
        error_on: date | None = None,
    ) -> None:
        self.events = events
        self.record = record or _record()
        self.error_on = error_on

    def fetch_trading_billboard(self, trade_date: date) -> ProviderBatch[TradingBillboardRecord]:
        self.events.append(f"fetch:{trade_date.isoformat()}")
        if trade_date == self.error_on:
            raise ProviderError("source failed")
        raw_rows = (
            {"record_kind": "summary", "payload_json": "{}"},
            {"record_kind": "buy_seat", "payload_json": "{}"},
            {"record_kind": "sell_seat", "payload_json": "{}"},
        )

        def normalize() -> tuple[TradingBillboardRecord, ...]:
            self.events.append(f"normalize:{trade_date.isoformat()}")
            assert "raw" in self.events
            return (replace(self.record, trade_date=trade_date),)

        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={"trade_date": trade_date.isoformat()},
            schema_version="eastmoney.trading_billboard.v1",
            record_factory=normalize,
        )


class FakeRawStore:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def write_jsonl(self, **kwargs: object) -> StoredRawObject:
        self.events.append("raw")
        assert kwargs["dataset"] == "trading_billboard"
        assert kwargs["ingestion_id"] == INGESTION_ID
        if self.fail:
            raise OSError("disk full")
        return StoredRawObject(
            object_path=(
                f"eastmoney/trading_billboard/year=2026/month=08/day=17/{INGESTION_ID}.jsonl"
            ),
            content_sha256="a" * 64,
            byte_size=30,
            row_count=3,
            file_format="jsonl",
            schema_version=cast(str, kwargs["schema_version"]),
        )


class FakePersistence:
    def __init__(self, events: list[str], trading_days: set[date] | None = None) -> None:
        self.events = events
        self.trading_days = trading_days or {TRADE_DATE}
        self.successes: list[
            tuple[
                IngestionRun,
                RawManifest,
                tuple[QualityResult, ...],
                tuple[TradingBillboardRecord, ...],
            ]
        ] = []
        self.failures: list[tuple[IngestionRun, RawManifest | None, tuple[QualityResult, ...]]] = []

    def is_trading_day(self, trade_date: date) -> bool:
        self.events.append(f"calendar:{trade_date.isoformat()}")
        return trade_date in self.trading_days

    def known_stock_symbols(self, trade_date: date) -> frozenset[str]:
        self.events.append(f"known:{trade_date.isoformat()}")
        return frozenset({"SZSE:000711"})

    def commit_success(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: tuple[QualityResult, ...],
        records: tuple[TradingBillboardRecord, ...],
    ) -> TradingBillboardCollectionSummary:
        self.events.append("commit_success")
        self.successes.append((run, manifest, quality, records))
        return TradingBillboardCollectionSummary(
            status=run.status.value,
            ingestion_id=run.ingestion_id,
            trade_date=records[0].trade_date,
            fetched_rows=run.fetched_rows,
            accepted_entries=len(records),
            accepted_seats=sum(
                len(record.buy_seats) + len(record.sell_seats) for record in records
            ),
            filtered_rows=run.rejected_rows,
        )

    def commit_failure(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality: tuple[QualityResult, ...],
    ) -> None:
        self.events.append("commit_failure")
        self.failures.append((run, manifest, quality))


def _service(
    events: list[str],
    *,
    persistence: FakePersistence | None = None,
    provider: FakeProvider | None = None,
    raw_store: FakeRawStore | None = None,
) -> tuple[TradingBillboardService, FakePersistence]:
    actual_persistence = persistence or FakePersistence(events)
    return (
        TradingBillboardService(
            persistence=actual_persistence,
            raw_store=raw_store or FakeRawStore(events),
            provider=provider or FakeProvider(events),
            clock=lambda: NOW,
            uuid_factory=iter(
                (
                    INGESTION_ID,
                    RAW_ID,
                    UUID("00000000-0000-0000-0000-000000000103"),
                    UUID("00000000-0000-0000-0000-000000000104"),
                    UUID("00000000-0000-0000-0000-000000000105"),
                    UUID("00000000-0000-0000-0000-000000000106"),
                )
            ).__next__,
        ),
        actual_persistence,
    )


def test_collect_writes_raw_before_lazy_normalization_and_commits_once() -> None:
    events: list[str] = []
    service, persistence = _service(events)

    summary = service.collect(TRADE_DATE)

    assert events == [
        "calendar:2026-08-17",
        "fetch:2026-08-17",
        "raw",
        "normalize:2026-08-17",
        "known:2026-08-17",
        "commit_success",
    ]
    assert summary.status == "succeeded"
    assert summary.accepted_entries == 1
    assert summary.accepted_seats == 2
    assert len(persistence.successes) == 1
    run, manifest, quality, records = persistence.successes[0]
    assert run.ingestion_id == manifest.ingestion_id == INGESTION_ID
    assert run.fetched_rows == 3
    assert run.accepted_rows == 3
    assert run.rejected_rows == 0
    assert quality == ()
    assert records == (_record(),)


@pytest.mark.parametrize("failure", ["provider", "raw"])
def test_collect_commits_failed_run_without_facts_on_external_error(failure: str) -> None:
    events: list[str] = []
    provider = FakeProvider(events, error_on=TRADE_DATE if failure == "provider" else None)
    raw_store = FakeRawStore(events, fail=failure == "raw")
    service, persistence = _service(events, provider=provider, raw_store=raw_store)

    with pytest.raises((ProviderError, OSError)):
        service.collect(TRADE_DATE)

    assert persistence.successes == []
    assert len(persistence.failures) == 1
    run, manifest, quality = persistence.failures[0]
    assert run.status.value == "failed"
    assert manifest is None
    assert quality[0].rule_code.endswith("collection_error")


def test_collect_commits_whole_date_as_failed_on_hard_validation_finding() -> None:
    events: list[str] = []
    bad = _record(buy_seats=(replace(_seat(TradingBillboardSide.BUY), symbol="SSE:600000"),))
    service, persistence = _service(events, provider=FakeProvider(events, bad))

    with pytest.raises(TradingBillboardValidationError):
        service.collect(TRADE_DATE)

    assert persistence.successes == []
    run, manifest, quality = persistence.failures[0]
    assert run.status.value == "failed"
    assert manifest is not None
    assert run.rejected_rows == 3
    assert quality[0].rule_code.endswith("seat_parent_mismatch")


def test_backfill_skips_non_trading_days_and_stops_at_first_failure() -> None:
    events: list[str] = []
    day_three = TRADE_DATE + timedelta(days=2)
    persistence = FakePersistence(events, {TRADE_DATE, day_three})
    service, _ = _service(
        events,
        persistence=persistence,
        provider=FakeProvider(events, error_on=day_three),
    )

    summary = service.backfill(TRADE_DATE, day_three)

    assert isinstance(summary, TradingBillboardBackfillSummary)
    assert summary.completed_dates == (TRADE_DATE,)
    assert summary.skipped_dates == (TRADE_DATE + timedelta(days=1),)
    assert summary.failed_date == day_three


def test_backfill_rejects_invalid_or_unbounded_ranges_before_io() -> None:
    events: list[str] = []
    service, _ = _service(events)

    with pytest.raises(ValueError, match="start_date"):
        service.backfill(TRADE_DATE, TRADE_DATE - timedelta(days=1))
    with pytest.raises(ValueError, match="366"):
        service.backfill(TRADE_DATE, TRADE_DATE + timedelta(days=366))

    assert events == []
