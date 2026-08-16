from datetime import UTC, date, datetime
from uuid import UUID
from zoneinfo import ZoneInfo

from market_data_center.domain.auction import AuctionRoundStatus, AuctionRoundSummary
from market_data_center.persistence.auction_postgres import _round_parameters, _session


def test_round_parameters_include_phase_required_by_round_insert() -> None:
    summary = AuctionRoundSummary(
        sample_seq=0,
        status=AuctionRoundStatus.SUCCEEDED,
        expected_quotes=2,
        successful_quotes=2,
        failed_quotes=0,
        scheduled_at=datetime(2026, 8, 13, 1, 15, tzinfo=UTC),
        collected_at=datetime(2026, 8, 13, 1, 15, 1, tzinfo=UTC),
        latency_ms=1_000,
    )

    parameters = _round_parameters(summary)

    assert parameters["phase"] == "cancellable"


def test_session_hydration_normalizes_postgres_timestamptz_to_utc() -> None:
    shanghai = ZoneInfo("Asia/Shanghai")
    row: dict[str, object] = {
        "session_id": UUID("00000000-0000-0000-0000-000000000001"),
        "pool_snapshot_id": UUID("00000000-0000-0000-0000-000000000002"),
        "pool_snapshot_version": 1,
        "basis_trade_date": date(2026, 8, 14),
        "effective_trade_date": date(2026, 8, 17),
        "window_start": datetime(2026, 8, 17, 9, 15, tzinfo=shanghai),
        "window_end": datetime(2026, 8, 17, 9, 25, tzinfo=shanghai),
        "cadence_seconds": 30,
        "expected_rounds": 21,
        "expected_quotes": 42,
        "provider_code": "pytdx_hq",
        "status": "succeeded",
        "started_at": datetime(2026, 8, 17, 9, 15, tzinfo=shanghai),
        "finished_at": datetime(2026, 8, 17, 9, 25, 1, tzinfo=shanghai),
        "successful_rounds": 21,
        "partial_rounds": 0,
        "failed_rounds": 0,
        "successful_quotes": 42,
        "failed_quotes": 0,
        "error_summary": None,
    }

    session = _session(row)

    assert session.window_start == datetime(2026, 8, 17, 1, 15, tzinfo=UTC)
    assert session.window_end == datetime(2026, 8, 17, 1, 25, tzinfo=UTC)
    assert session.started_at == datetime(2026, 8, 17, 1, 15, tzinfo=UTC)
    assert session.finished_at == datetime(2026, 8, 17, 1, 25, 1, tzinfo=UTC)
