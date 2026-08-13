from datetime import UTC, datetime

from market_data_center.domain.auction import AuctionRoundStatus, AuctionRoundSummary
from market_data_center.persistence.auction_postgres import _round_parameters


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
