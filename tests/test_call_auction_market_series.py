from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

import pytest

from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    MarketSeriesSnapshotRecord,
    MarketSeriesStatus,
    MarketSeriesValueSemantics,
    series_slots,
    universe_hash,
)
from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.operations import WorkflowCode

TRADE_DATE = date(2026, 8, 17)
SLOTS = tuple(
    datetime(2026, 8, 17, 1, 15, tzinfo=UTC) + timedelta(seconds=20 * seq) for seq in range(32)
)
UNIVERSE = ("SSE:600000", "SZSE:000001")


def _session(**changes: object) -> MarketSeriesSession:
    values: dict[str, object] = {
        "session_id": uuid4(),
        "workflow_run_id": uuid4(),
        "trade_date": TRADE_DATE,
        "window_start": SLOTS[0],
        "window_end": SLOTS[-1] + timedelta(seconds=20),
        "cadence_seconds": 20,
        "expected_rounds": 32,
        "universe_symbols": UNIVERSE,
        "universe_count": 2,
        "universe_hash": universe_hash(UNIVERSE),
        "status": MarketSeriesStatus.RUNNING,
        "started_at": SLOTS[0],
    }
    values.update(changes)
    return MarketSeriesSession(**values)  # type: ignore[arg-type]


def _round(**changes: object) -> MarketSeriesRound:
    values: dict[str, object] = {
        "session_id": uuid4(),
        "sample_seq": 0,
        "scheduled_at": SLOTS[0],
        "collected_at": None,
        "status": MarketSeriesStatus.RUNNING,
        "attempt_count": 0,
        "expected_quotes": 2,
        "successful_quotes": 0,
        "failed_quotes": 0,
        "selected_ingestion_id": None,
    }
    values.update(changes)
    return MarketSeriesRound(**values)  # type: ignore[arg-type]


def _snapshot(**changes: object) -> MarketSeriesSnapshotRecord:
    values: dict[str, object] = {
        "symbol": "SSE:600000",
        "trade_date": TRADE_DATE,
        "session_id": uuid4(),
        "sample_seq": 0,
        "scheduled_at": SLOTS[0],
        "observed_at": SLOTS[0] + timedelta(seconds=2),
        "source_code": "pytdx_hq",
        "last_price": Decimal("10.10"),
        "previous_close": Decimal("10.00"),
        "high_price": Decimal("10.10"),
        "low_price": Decimal("10.00"),
        "cumulative_volume": 123_400,
        "cumulative_amount": Decimal("1246340.00"),
        "value_semantics": MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    }
    values.update(changes)
    return MarketSeriesSnapshotRecord(**values)  # type: ignore[arg-type]


def test_series_slots_are_exactly_thirty_two_twenty_second_points() -> None:
    assert series_slots(TRADE_DATE) == SLOTS


def test_universe_hash_requires_ordered_unique_sse_szse_symbols() -> None:
    assert (
        universe_hash(UNIVERSE)
        == "384af5b20819f9cbe1d302a9e557de3114d4e059284b497df122c04b8317a3a1"
    )
    with pytest.raises(ValueError, match="sorted and unique"):
        universe_hash(tuple(reversed(UNIVERSE)))
    with pytest.raises(ValueError, match="sorted and unique"):
        universe_hash(("SSE:600000", "SSE:600000"))
    with pytest.raises(ValueError, match="SSE/SZSE"):
        universe_hash(("BSE:920000",))


def test_session_requires_fixed_window_universe_and_terminal_counts() -> None:
    running = _session()
    assert running.universe_symbols == UNIVERSE
    with pytest.raises(ValueError, match="window"):
        _session(window_end=SLOTS[-1])
    with pytest.raises(ValueError, match="hash"):
        _session(universe_hash="0" * 64)
    with pytest.raises(ValueError, match="finished_at"):
        _session(status=MarketSeriesStatus.SUCCEEDED)
    with pytest.raises(ValueError, match="all rounds"):
        _session(
            status=MarketSeriesStatus.SUCCEEDED,
            finished_at=SLOTS[-1] + timedelta(seconds=20),
            successful_rounds=31,
            failed_rounds=1,
            successful_quotes=62,
            failed_quotes=2,
        )


def test_round_enforces_running_and_terminal_state_counts() -> None:
    assert _round().collected_at is None
    with pytest.raises(ValueError, match="collected_at"):
        _round(status=MarketSeriesStatus.FAILED, failed_quotes=2)
    with pytest.raises(ValueError, match="expected_quotes"):
        _round(
            status=MarketSeriesStatus.PARTIAL,
            collected_at=SLOTS[0] + timedelta(seconds=3),
            attempt_count=1,
            successful_quotes=1,
            failed_quotes=0,
            selected_ingestion_id=uuid4(),
        )
    completed = _round(
        status=MarketSeriesStatus.SUCCEEDED,
        collected_at=SLOTS[0] + timedelta(seconds=3),
        attempt_count=1,
        successful_quotes=2,
        selected_ingestion_id=uuid4(),
    )
    assert completed.failed_quotes == 0


@pytest.mark.parametrize("sample_seq", [-1, 32])
def test_round_rejects_sample_sequence_outside_window(sample_seq: int) -> None:
    with pytest.raises(ValueError, match="sample_seq"):
        _round(sample_seq=sample_seq)


def test_snapshot_requires_exact_round_window_and_price_invariants() -> None:
    assert _snapshot().cumulative_volume == 123_400
    with pytest.raises(ValueError, match="scheduled_at"):
        _snapshot(sample_seq=1)
    with pytest.raises(ValueError, match="observed_at"):
        _snapshot(observed_at=SLOTS[0] + timedelta(seconds=20))
    with pytest.raises(ValueError, match="price bounds"):
        _snapshot(
            last_price=Decimal("10.20"),
            high_price=Decimal("10.10"),
            cumulative_amount=Decimal("1258680.00"),
        )
    with pytest.raises(TypeError, match="Decimal"):
        _snapshot(cumulative_amount=1.0)
    with pytest.raises(TypeError, match="integer"):
        _snapshot(cumulative_volume=True)


def test_auction_indicative_snapshot_requires_consistent_bid1_values() -> None:
    snapshot = _snapshot(
        last_price=Decimal("9.99"),
        high_price=None,
        low_price=None,
        cumulative_volume=1200,
        cumulative_amount=Decimal("11988.00"),
        value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
    )
    assert snapshot.cumulative_amount == Decimal("11988.00")
    with pytest.raises(ValueError, match="price multiplied by volume"):
        _snapshot(
            last_price=Decimal("9.99"),
            high_price=None,
            low_price=None,
            cumulative_volume=1200,
            cumulative_amount=Decimal("1"),
            value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
        )


def test_snapshot_semantics_follow_the_scheduled_0925_boundary() -> None:
    with pytest.raises(ValueError, match="before 09:25"):
        _snapshot(
            sample_seq=30,
            scheduled_at=SLOTS[30],
            observed_at=SLOTS[30] + timedelta(seconds=2),
            value_semantics=MarketSeriesValueSemantics.AUCTION_INDICATIVE,
        )


def test_series_codes_are_controlled_enums() -> None:
    assert DatasetCode.CALL_AUCTION_MARKET_SERIES.value == "call_auction_market_series"
    assert WorkflowCode.CALL_AUCTION_MARKET_SERIES.value == "call_auction_market_series"
