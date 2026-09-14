"""Daily point-in-time materialization of stable DragonTiger seat profiles."""

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import date
from decimal import Decimal
from hashlib import sha256
from json import dumps
from typing import Protocol
from uuid import UUID, uuid4

from market_data_center.dragon_tiger_analytics import (
    NEXT_SESSION_PARTICIPATION_DEFINITION,
    POSITIVE_RETURN_WIN_DEFINITION,
    SeatOutcome,
    SeatParticipation,
    TradingSeatProfile,
    build_trading_seat_profile,
    is_effective_buy,
)
from market_data_center.dragon_tiger_features import UNADJUSTED_CLOSE_TO_CLOSE

PROFILE_ALGORITHM_VERSION = "trading-seat-profile-v1"


@dataclass(frozen=True, slots=True)
class FutureSessionClose:
    horizon_sessions: int
    trade_date: date
    close: Decimal | None

    def __post_init__(self) -> None:
        if self.horizon_sessions not in {1, 3, 5}:
            raise ValueError("future close horizon must be 1, 3 or 5 sessions")
        if self.close is not None and self.close <= 0:
            raise ValueError("future close must be positive")


@dataclass(frozen=True, slots=True)
class SeatProfileObservation:
    participation: SeatParticipation
    event_close: Decimal | None
    future_closes: tuple[FutureSessionClose, ...]

    def __post_init__(self) -> None:
        if self.event_close is not None and self.event_close <= 0:
            raise ValueError("DragonTiger event close must be positive")
        horizons = tuple(item.horizon_sessions for item in self.future_closes)
        if len(set(horizons)) != len(horizons):
            raise ValueError("future close horizons must be unique")


@dataclass(frozen=True, slots=True)
class DragonTigerProfileInputs:
    as_of_date: date
    input_watermark_date: date
    trading_dates: tuple[date, ...]
    observations: tuple[SeatProfileObservation, ...]

    def __post_init__(self) -> None:
        if self.input_watermark_date > self.as_of_date:
            raise ValueError("profile input watermark exceeds as_of_date")
        if tuple(sorted(set(self.trading_dates))) != self.trading_dates:
            raise ValueError("profile trading dates must be unique and ascending")


@dataclass(frozen=True, slots=True)
class DragonTigerProfileSummary:
    as_of_date: date
    eligible_seat_count: int
    profile_count: int
    skipped_outcome_count: int


class DragonTigerProfilePersistence(Protocol):
    def load_profile_inputs(self, as_of_date: date) -> DragonTigerProfileInputs: ...

    def replace_profiles(
        self,
        profiles: Sequence[TradingSeatProfile],
        *,
        calculation_id: UUID,
        input_watermark_date: date,
        input_hash: str,
    ) -> int: ...


class DragonTigerProfileService:
    def __init__(
        self,
        persistence: DragonTigerProfilePersistence,
        *,
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._uuid_factory = uuid_factory

    def materialize(self, as_of_date: date) -> DragonTigerProfileSummary:
        inputs = self._persistence.load_profile_inputs(as_of_date)
        if inputs.as_of_date != as_of_date:
            raise ValueError("profile inputs do not match as_of_date")
        participations = tuple(item.participation for item in inputs.observations)
        outcomes, skipped = _build_outcomes(inputs.observations, as_of_date)
        seat_ids = tuple(
            sorted(
                {item.seat_id for item in participations if is_effective_buy(item)},
                key=str,
            )
        )
        profiles = tuple(
            build_trading_seat_profile(
                seat_id=seat_id,
                participations=participations,
                outcomes=outcomes,
                as_of_date=as_of_date,
                algorithm_version=PROFILE_ALGORITHM_VERSION,
                metric_definition=POSITIVE_RETURN_WIN_DEFINITION,
                return_definition=UNADJUSTED_CLOSE_TO_CLOSE,
                participation_definition=NEXT_SESSION_PARTICIPATION_DEFINITION,
                trading_dates=inputs.trading_dates,
            )
            for seat_id in seat_ids
        )
        calculation_id = self._uuid_factory()
        written = self._persistence.replace_profiles(
            profiles,
            calculation_id=calculation_id,
            input_watermark_date=inputs.input_watermark_date,
            input_hash=_input_hash(inputs),
        )
        if written != len(profiles):
            raise RuntimeError("DragonTiger profile publication count mismatch")
        return DragonTigerProfileSummary(
            as_of_date,
            len(seat_ids),
            len(profiles),
            skipped,
        )


def _build_outcomes(
    observations: Sequence[SeatProfileObservation], as_of_date: date
) -> tuple[tuple[SeatOutcome, ...], int]:
    outcomes: list[SeatOutcome] = []
    skipped = 0
    for observation in observations:
        for future in observation.future_closes:
            if future.trade_date > as_of_date:
                continue
            if observation.event_close is None or future.close is None:
                skipped += 1
                continue
            participation = observation.participation
            outcomes.append(
                SeatOutcome(
                    participation.event_source_record_id,
                    participation.seat_id,
                    participation.event_date,
                    future.horizon_sessions,
                    future.close / observation.event_close - Decimal(1),
                    future.trade_date,
                    UNADJUSTED_CLOSE_TO_CLOSE,
                )
            )
    return tuple(outcomes), skipped


def _input_hash(inputs: DragonTigerProfileInputs) -> str:
    payload = dumps(
        asdict(inputs), default=str, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )
    return sha256(payload.encode("utf-8")).hexdigest()
