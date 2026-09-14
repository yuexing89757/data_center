from datetime import date
from decimal import Decimal
from uuid import UUID

from market_data_center.dragon_tiger_analytics import SeatParticipation
from market_data_center.dragon_tiger_profile_service import (
    DragonTigerProfileInputs,
    DragonTigerProfileService,
    FutureSessionClose,
    SeatProfileObservation,
)

SEAT_ID = UUID("00000000-0000-0000-0000-000000000101")
CALCULATION_ID = UUID("00000000-0000-0000-0000-000000000201")


class Persistence:
    def __init__(self, inputs: DragonTigerProfileInputs) -> None:
        self.inputs = inputs
        self.written: list[object] = []

    def load_profile_inputs(self, as_of_date: date) -> DragonTigerProfileInputs:
        assert as_of_date == self.inputs.as_of_date
        return self.inputs

    def replace_profiles(self, profiles, **metadata) -> int:
        self.written.append((profiles, metadata))
        return len(profiles)


def test_materialize_builds_only_time_available_effective_buy_outcomes() -> None:
    inputs = DragonTigerProfileInputs(
        as_of_date=date(2026, 9, 11),
        input_watermark_date=date(2026, 9, 11),
        trading_dates=(date(2026, 9, 8), date(2026, 9, 9), date(2026, 9, 10), date(2026, 9, 11)),
        observations=(
            SeatProfileObservation(
                participation=SeatParticipation(
                    "event-1", SEAT_ID, date(2026, 9, 8), Decimal("100"), Decimal("20")
                ),
                event_close=Decimal("10"),
                future_closes=(
                    FutureSessionClose(1, date(2026, 9, 9), Decimal("11")),
                    FutureSessionClose(3, date(2026, 9, 11), Decimal("9")),
                ),
            ),
            SeatProfileObservation(
                participation=SeatParticipation(
                    "event-2", SEAT_ID, date(2026, 9, 10), Decimal("10"), Decimal("20")
                ),
                event_close=Decimal("10"),
                future_closes=(FutureSessionClose(1, date(2026, 9, 11), Decimal("12")),),
            ),
        ),
    )
    persistence = Persistence(inputs)
    service = DragonTigerProfileService(
        persistence,
        uuid_factory=lambda: CALCULATION_ID,
    )

    summary = service.materialize(date(2026, 9, 11))

    profiles, metadata = persistence.written[0]
    profile = profiles[0]
    assert profile.total_lhb_count == 1
    assert profile.t1_sample_count == 1
    assert profile.t1_win_rate == Decimal("1")
    assert profile.t3_sample_count == 1
    assert profile.t3_win_rate == Decimal("0")
    assert profile.t5_sample_count == 0
    assert metadata["calculation_id"] == CALCULATION_ID
    assert len(metadata["input_hash"]) == 64
    assert summary.profile_count == 1
    assert summary.skipped_outcome_count == 0


def test_materialize_preserves_missing_close_as_skipped_outcome() -> None:
    inputs = DragonTigerProfileInputs(
        as_of_date=date(2026, 9, 9),
        input_watermark_date=date(2026, 9, 9),
        trading_dates=(date(2026, 9, 8), date(2026, 9, 9)),
        observations=(
            SeatProfileObservation(
                participation=SeatParticipation(
                    "event-1", SEAT_ID, date(2026, 9, 8), Decimal("100"), None
                ),
                event_close=Decimal("10"),
                future_closes=(FutureSessionClose(1, date(2026, 9, 9), None),),
            ),
        ),
    )

    summary = DragonTigerProfileService(Persistence(inputs)).materialize(date(2026, 9, 9))

    assert summary.skipped_outcome_count == 1
