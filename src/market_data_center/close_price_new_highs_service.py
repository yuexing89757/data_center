"""Versioned publication service for daily 120-session closing highs."""

from collections.abc import Callable
from datetime import UTC, date, datetime
from uuid import UUID, uuid4

from market_data_center.close_price_new_highs_calculator import (
    calculate_close_price_new_highs_120d,
)
from market_data_center.domain.close_price_new_highs import (
    CLOSE_PRICE_NEW_HIGHS_ALGORITHM_VERSION,
    ClosePriceNewHighBuildSummary,
    ClosePriceNewHighSnapshot,
)
from market_data_center.domain.derived import CalculationMode, CalculationRun, CalculationStatus
from market_data_center.persistence.close_price_new_highs_postgres import (
    CALCULATION_CODE,
    PostgreSQLClosePriceNewHighsPersistence,
)


class ClosePriceNewHighsService:
    def __init__(
        self,
        persistence: PostgreSQLClosePriceNewHighsPersistence,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def build(self, trade_date: date) -> ClosePriceNewHighBuildSummary:
        with self._persistence.build_lock(trade_date):
            source, watermark = self._persistence.load_input(trade_date)
            calculation = calculate_close_price_new_highs_120d(source)
            existing = self._persistence.existing_snapshot(trade_date, calculation.input_hash)
            if existing is not None:
                calculation_id, snapshot_id = existing
                return ClosePriceNewHighBuildSummary(
                    status="unchanged",
                    calculation_id=calculation_id,
                    snapshot_id=snapshot_id,
                    trade_date=trade_date,
                    candidate_count=calculation.candidate_count,
                    member_count=calculation.member_count,
                    omitted_count=calculation.omitted_count,
                )
            now = self._clock()
            run = CalculationRun(
                calculation_id=self._uuid_factory(),
                calculation_code=CALCULATION_CODE,
                algorithm_version=CLOSE_PRICE_NEW_HIGHS_ALGORITHM_VERSION,
                mode=CalculationMode.INCREMENTAL,
                start_date=trade_date,
                end_date=trade_date,
                status=CalculationStatus.RUNNING,
                input_watermark=watermark,
                input_hash=calculation.input_hash,
                requested_at=now,
            )
            self._persistence.create_calculation_run(run)
            snapshot = ClosePriceNewHighSnapshot(
                snapshot_id=self._uuid_factory(),
                calculation_id=run.calculation_id,
                trade_date=trade_date,
                version=self._persistence.next_snapshot_version(trade_date),
                candidate_count=calculation.candidate_count,
                eligible_history_count=calculation.eligible_history_count,
                omitted_count=calculation.omitted_count,
                member_count=calculation.member_count,
                incomplete_history_count=calculation.incomplete_history_count,
                non_trading_bar_count=calculation.non_trading_bar_count,
                nonpositive_price_count=calculation.nonpositive_price_count,
                missing_name_count=calculation.missing_name_count,
                input_hash=calculation.input_hash,
                content_hash=calculation.content_hash,
                algorithm_version=CLOSE_PRICE_NEW_HIGHS_ALGORITHM_VERSION,
                generated_at=now,
            )
            try:
                completed = run.succeeded(
                    finished_at=self._clock(), output_rows=1 + calculation.member_count
                )
                self._persistence.publish(completed, calculation, snapshot)
            except Exception as error:
                self._persistence.fail_calculation_run(
                    run.failed(
                        finished_at=self._clock(),
                        error_summary=(
                            f"{type(error).__name__}: closing-high snapshot calculation failed"
                        ),
                    )
                )
                raise
            return ClosePriceNewHighBuildSummary(
                status="succeeded",
                calculation_id=completed.calculation_id,
                snapshot_id=snapshot.snapshot_id,
                trade_date=trade_date,
                candidate_count=calculation.candidate_count,
                member_count=calculation.member_count,
                omitted_count=calculation.omitted_count,
            )
