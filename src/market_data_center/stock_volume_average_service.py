"""Versioned complete-market publication of fixed-calendar volume averages."""

from collections.abc import Callable
from datetime import UTC, date, datetime
from hashlib import sha256
from json import dumps
from uuid import UUID, uuid4

from market_data_center.domain.derived import CalculationMode, CalculationRun, CalculationStatus
from market_data_center.domain.stock_volume_average import (
    ALGORITHM_VERSION,
    CALCULATION_CODE,
    VolumeAverageBuildSummary,
)
from market_data_center.persistence.stock_volume_average_postgres import (
    PostgreSQLStockVolumeAveragePersistence,
    VolumeAverageDependencyNotReady,
)
from market_data_center.stock_volume_average_calculator import calculate_stock_volume_average


class StockVolumeAverageService:
    def __init__(
        self,
        persistence: PostgreSQLStockVolumeAveragePersistence,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def build(self, trade_date: date) -> VolumeAverageBuildSummary:
        trading_day = self._persistence.is_trading_day(trade_date)
        if trading_day is None:
            raise VolumeAverageDependencyNotReady("target calendar is unknown")
        if not trading_day:
            return VolumeAverageBuildSummary("skipped", trade_date, None, None, 0, 0)
        with self._persistence.build_lock(trade_date):
            source, watermark, evidence = self._persistence.load_input(trade_date)
            calculation = calculate_stock_volume_average(source)
            evidence_hash = sha256(
                dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest()
            existing = self._persistence.existing_calculation(trade_date, calculation.input_hash)
            current = self._persistence.current_publication(trade_date)
            if (
                existing is not None
                and current is not None
                and existing == current[0]
                and evidence_hash == current[2]
            ):
                return VolumeAverageBuildSummary(
                    "unchanged",
                    trade_date,
                    existing,
                    current[1],
                    len(calculation.records),
                    calculation.complete_count,
                )
            now = self._clock()
            run = None
            calculation_id = existing or self._uuid_factory()
            if existing is None:
                run = CalculationRun(
                    calculation_id,
                    CALCULATION_CODE,
                    ALGORITHM_VERSION,
                    CalculationMode.INCREMENTAL,
                    trade_date,
                    trade_date,
                    CalculationStatus.RUNNING,
                    watermark,
                    calculation.input_hash,
                    now,
                )
                self._persistence.create_calculation_run(run)
            try:
                version = self._persistence.publish(
                    run.succeeded(finished_at=self._clock(), output_rows=len(calculation.records))
                    if run is not None
                    else None,
                    calculation_id,
                    trade_date,
                    calculation,
                    evidence,
                    self._clock(),
                    watermark=watermark,
                    evidence_hash=evidence_hash,
                )
            except Exception as error:
                if run is not None:
                    self._persistence.fail_calculation_run(
                        run.failed(
                            finished_at=self._clock(), error_summary=type(error).__name__[:200]
                        )
                    )
                raise
            return VolumeAverageBuildSummary(
                "succeeded" if run is not None else "published/reused",
                trade_date,
                calculation_id,
                version,
                len(calculation.records),
                calculation.complete_count,
            )
