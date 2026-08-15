from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.domain.board_index import BoardIndexDailyBarRecord
from market_data_center.domain.records import Market
from market_data_center.providers.akshare_ths import (
    THSAnnualDailyPayload,
    THSBoardDailyBarLiveBatch,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.public_api.board_index_bias_live import (
    BoardIndexBiasLiveBusy,
    BoardIndexBiasLivePersistence,
    BoardIndexBiasLiveService,
    BoardIndexBiasLiveUpstream,
)
from market_data_center.public_api.board_index_bias_write import (
    BoardIndexPersistenceError,
    BoardIndexPersistenceQueueFull,
    PreparedBoardIndexPersistence,
)

NOW = datetime(2026, 8, 15, 1, 2, 3, tzinfo=UTC)


def _batch() -> THSBoardDailyBarLiveBatch:
    records = []
    for offset in range(35):
        value = Decimal(offset + 1)
        records.append(
            BoardIndexDailyBarRecord(
                board_id="THS:883423",
                trade_date=date(2026, 1, 1) + timedelta(days=offset),
                market=Market.CN_A_SHARE,
                open=value,
                high=value,
                low=value,
                close=value,
                volume=offset,
                amount=value * offset,
                source_code="akshare_ths",
            )
        )
    payload = THSAnnualDailyPayload(
        year=2026,
        url="https://d.10jqka.com.cn/v4/line/bk_883423/01/2026.js",
        content=b"exact",
        fetched_at=NOW,
    )
    return THSBoardDailyBarLiveBatch((payload,), tuple(records), NOW)


class FakeProvider:
    def __init__(self, *, error: bool = False) -> None:
        self.error = error
        self.calls: list[tuple[str, date]] = []

    def fetch_live_board_index_daily_bars(
        self, board_id: str, *, as_of_date: date, minimum_records: int = 34
    ) -> THSBoardDailyBarLiveBatch:
        self.calls.append((board_id, as_of_date))
        if self.error:
            raise ProviderError("THS unavailable")
        return _batch()


class FakePersistence:
    def __init__(
        self, *, prepare_error: bool = False, submit_error: Exception | None = None
    ) -> None:
        self.prepare_error = prepare_error
        self.submit_error = submit_error
        self.events: list[str] = []
        self.discarded = False

    def prepare(self, batch: THSBoardDailyBarLiveBatch) -> PreparedBoardIndexPersistence:
        self.events.append("prepare")
        if self.prepare_error:
            raise BoardIndexPersistenceError("Raw failed")
        return PreparedBoardIndexPersistence(
            ingestion_id=UUID("11111111-1111-1111-1111-111111111111"),
            raw_id=UUID("22222222-2222-2222-2222-222222222222"),
            fetched_at=NOW,
            input_hash="a" * 64,
            object_path="test.jsonl",
            content_sha256="b" * 64,
            byte_size=1,
            source_row_count=1,
            source_years_payload="[2026]",
            records_payload="[]",
        )

    def submit(self, prepared: PreparedBoardIndexPersistence) -> None:
        self.events.append("submit")
        if self.submit_error is not None:
            raise self.submit_error

    def discard(self, prepared: PreparedBoardIndexPersistence) -> None:
        self.events.append("discard")
        self.discarded = True


def _service(
    provider: FakeProvider | None = None,
    persistence: FakePersistence | None = None,
) -> BoardIndexBiasLiveService:
    return BoardIndexBiasLiveService(
        provider or FakeProvider(),  # type: ignore[arg-type]
        persistence or FakePersistence(),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )


def test_live_service_fetches_captures_raw_queues_and_returns_calculation() -> None:
    provider, persistence = FakeProvider(), FakePersistence()

    result = _service(provider, persistence).fetch_current()

    assert provider.calls == [("THS:883423", date(2026, 8, 15))]
    assert persistence.events == ["prepare", "submit"]
    assert result.data_origin == "ths_live"
    assert result.persistence_status == "queued"
    assert result.fetched_at == NOW
    assert result.moving_average_5 == Decimal("33.000000")
    assert result.bias_5_pct == Decimal("6.060606")


def test_live_service_maps_provider_raw_and_queue_failures() -> None:
    with pytest.raises(BoardIndexBiasLiveUpstream):
        _service(provider=FakeProvider(error=True)).fetch_current()
    with pytest.raises(BoardIndexBiasLivePersistence):
        _service(persistence=FakePersistence(prepare_error=True)).fetch_current()

    full = FakePersistence(submit_error=BoardIndexPersistenceQueueFull("full"))
    with pytest.raises(BoardIndexBiasLiveBusy):
        _service(persistence=full).fetch_current()
    assert full.discarded is True


def test_live_service_enforces_single_fetch_slot() -> None:
    service = _service()
    assert service._lock.acquire(blocking=False)
    try:
        with pytest.raises(BoardIndexBiasLiveBusy):
            service.fetch_current()
    finally:
        service._lock.release()
