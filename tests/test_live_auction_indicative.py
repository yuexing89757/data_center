from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.domain.auction_indicative import (
    CallAuctionIndicativeDetailRecord,
    SourceDisplayClassification,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError
from market_data_center.public_api.auction_indicative_live import (
    AuctionIndicativeLiveBusy,
    AuctionIndicativeLiveInvalid,
    AuctionIndicativeLivePersistence,
    AuctionIndicativeLiveUpstream,
    LiveAuctionIndicativeService,
)
from market_data_center.public_api.auction_indicative_write import (
    AuctionIndicativePersistenceError,
    PreparedAuctionIndicativePersistence,
)

TODAY = date(2026, 8, 14)
NOW = datetime(2026, 8, 14, 1, 26, tzinfo=UTC)


class FakeProvider:
    def __init__(self, *, error: bool = False) -> None:
        self.calls = 0
        self.error = error

    def fetch_current_day(
        self, symbol: str, trade_date: date, *, now: datetime
    ) -> ProviderBatch[CallAuctionIndicativeDetailRecord]:
        self.calls += 1
        if self.error:
            raise ProviderError("unavailable")
        record = CallAuctionIndicativeDetailRecord(
            symbol=symbol,
            trade_date=trade_date,
            observed_at=datetime(2026, 8, 14, 1, 15, 5, tzinfo=UTC),
            indicative_price=Decimal("133.99"),
            displayed_volume_shares=200,
            source_sequence=0,
            source_display_classification=SourceDisplayClassification.UNKNOWN,
        )
        return ProviderBatch(
            raw_rows=({"time": "09:15:05"},),
            request_params={"symbol": symbol},
            schema_version="test.v1",
            records=(record,),
        )


class UnsortedFakeProvider(FakeProvider):
    def fetch_current_day(
        self, symbol: str, trade_date: date, *, now: datetime
    ) -> ProviderBatch[CallAuctionIndicativeDetailRecord]:
        records = tuple(
            CallAuctionIndicativeDetailRecord(
                symbol=symbol,
                trade_date=trade_date,
                observed_at=observed_at,
                indicative_price=Decimal(price),
                displayed_volume_shares=shares,
                source_sequence=sequence,
                source_display_classification=SourceDisplayClassification.UNKNOWN,
            )
            for observed_at, price, shares, sequence in (
                (datetime(2026, 8, 14, 1, 20, tzinfo=UTC), "134.01", 300, 1),
                (datetime(2026, 8, 14, 1, 15, 5, tzinfo=UTC), "133.99", 200, 0),
            )
        )
        return ProviderBatch(
            raw_rows=({"time": "09:20:00"}, {"time": "09:15:05"}),
            request_params={"symbol": symbol},
            schema_version="test.v1",
            records=records,
        )


class FakePersistence:
    def __init__(self, *, error: bool = False) -> None:
        self.calls = 0
        self.error = error

    def prepare(self, **_: object) -> PreparedAuctionIndicativePersistence:
        self.calls += 1
        if self.error:
            raise AuctionIndicativePersistenceError("Raw unavailable")
        return PreparedAuctionIndicativePersistence(
            ingestion_id=UUID("11111111-1111-1111-1111-111111111111"),
            raw_id=UUID("22222222-2222-2222-2222-222222222222"),
            symbol="SSE:688796",
            trade_date=TODAY,
            fetched_at=NOW,
            input_hash="a" * 64,
            object_path="test.jsonl",
            content_sha256="b" * 64,
            byte_size=1,
            source_row_count=1,
            records_payload="[]",
        )

    def submit(self, _: PreparedAuctionIndicativePersistence) -> None:
        return None

    def discard(self, _: PreparedAuctionIndicativePersistence) -> None:
        return None


def _service(
    provider: FakeProvider | None = None,
    persistence: FakePersistence | None = None,
    *,
    now: datetime = NOW,
) -> LiveAuctionIndicativeService:
    return LiveAuctionIndicativeService(
        provider or FakeProvider(),  # type: ignore[arg-type]
        persistence or FakePersistence(),  # type: ignore[arg-type]
        clock=lambda: now,
        timer=lambda: 10.0,
    )


def test_fetch_captures_raw_and_queues_database_write_before_returning() -> None:
    provider, persistence = FakeProvider(), FakePersistence()
    service = _service(provider, persistence)

    first = service.fetch_current("SSE:688796", 0, 200)
    second = service.fetch_current("SSE:688796", 0, 200)

    assert first.quality.raw_captured is True
    assert first.quality.database_persistence == "queued"
    assert first.persistence_status == "queued"
    assert first.cache_hit is False
    assert second.cache_hit is True
    assert provider.calls == persistence.calls == 1


def test_fetch_sorts_all_live_records_before_pagination() -> None:
    response = _service(provider=UnsortedFakeProvider()).fetch("SSE:688796", TODAY, 0, 1)

    assert response.total_count == 2
    assert response.has_more is True
    assert [item.source_sequence for item in response.items] == [0]


def test_fetch_rejects_history_and_pre_completion_requests() -> None:
    with pytest.raises(AuctionIndicativeLiveInvalid):
        _service().fetch("SSE:688796", date(2026, 8, 13), 0, 200)
    with pytest.raises(AuctionIndicativeLiveInvalid):
        _service(now=datetime(2026, 8, 14, 1, 25, tzinfo=UTC)).fetch("SSE:688796", TODAY, 0, 200)


def test_fetch_maps_provider_and_persistence_failures_without_returning_data() -> None:
    with pytest.raises(AuctionIndicativeLiveUpstream):
        _service(provider=FakeProvider(error=True)).fetch("SSE:688796", TODAY, 0, 200)
    with pytest.raises(AuctionIndicativeLivePersistence):
        _service(persistence=FakePersistence(error=True)).fetch("SSE:688796", TODAY, 0, 200)


def test_fetch_logs_provider_failure_cause(caplog: pytest.LogCaptureFixture) -> None:
    with pytest.raises(AuctionIndicativeLiveUpstream):
        _service(provider=FakeProvider(error=True)).fetch("SSE:688796", TODAY, 0, 200)

    assert "SSE:688796" in caplog.text
    assert "unavailable" in caplog.text


def test_fetch_enforces_single_request_slot() -> None:
    service = _service()
    assert service._lock.acquire(blocking=False)  # exercise the nonblocking resource gate
    try:
        with pytest.raises(AuctionIndicativeLiveBusy):
            service.fetch("SSE:688796", TODAY, 0, 200)
    finally:
        service._lock.release()
