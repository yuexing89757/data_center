"""Application service for deterministic, immutable price-limit stock pools."""

from collections.abc import Callable
from datetime import UTC, date, datetime
from uuid import UUID, uuid4

from market_data_center.domain.derived import CalculationMode, CalculationRun, CalculationStatus
from market_data_center.domain.stock_pool import (
    PRICE_LIMIT_ALGORITHM_VERSION,
    PRICE_LIMIT_RULE_VERSION,
    STOCK_POOL_DEFINITIONS,
    StockPoolBuildSummary,
    StockPoolSnapshot,
    StockPoolSnapshotStatus,
)
from market_data_center.persistence.stock_pool_postgres import (
    CALCULATION_CODE,
    PostgreSQLStockPoolPersistence,
)
from market_data_center.stock_pool_calculator import (
    calculate_mainboard_stock_pools,
    stock_pool_content_hash,
)


class StockPoolService:
    def __init__(
        self,
        persistence: PostgreSQLStockPoolPersistence,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def build(self, basis_trade_date: date) -> StockPoolBuildSummary:
        with self._persistence.build_lock(basis_trade_date):
            source, watermark = self._persistence.load_build_input(basis_trade_date)
            output = calculate_mainboard_stock_pools(source)
            existing = self._persistence.succeeded_calculation_id(
                basis_trade_date, output.input_hash
            )
            if existing is not None:
                snapshot_ids = self._persistence.snapshot_ids_for_calculation(existing)
                return StockPoolBuildSummary(
                    "unchanged",
                    existing,
                    snapshot_ids,
                    source.basis_trade_date,
                    source.effective_trade_date,
                    output.candidate_count,
                    len(output.members),
                    output.rejected_count,
                )
            now = self._clock()
            run = CalculationRun(
                calculation_id=self._uuid_factory(),
                calculation_code=CALCULATION_CODE,
                algorithm_version=PRICE_LIMIT_ALGORITHM_VERSION,
                mode=CalculationMode.INCREMENTAL,
                start_date=basis_trade_date,
                end_date=basis_trade_date,
                status=CalculationStatus.RUNNING,
                input_watermark=watermark,
                input_hash=output.input_hash,
                requested_at=now,
            )
            self._persistence.create_calculation_run(run)
            try:
                snapshots = tuple(
                    StockPoolSnapshot(
                        snapshot_id=self._uuid_factory(),
                        calculation_id=run.calculation_id,
                        pool_code=definition.code,
                        basis_trade_date=source.basis_trade_date,
                        effective_trade_date=source.effective_trade_date,
                        version=self._persistence.next_snapshot_version(
                            definition.code, source.effective_trade_date
                        ),
                        status=StockPoolSnapshotStatus.READY,
                        member_count=sum(
                            item.pool_code == definition.code for item in output.members
                        ),
                        candidate_count=output.candidate_count,
                        rejected_count=output.rejected_count,
                        content_hash=stock_pool_content_hash(definition.code, output.members),
                        input_hash=output.input_hash,
                        rule_version=PRICE_LIMIT_RULE_VERSION,
                        algorithm_version=PRICE_LIMIT_ALGORITHM_VERSION,
                        generated_at=self._clock(),
                    )
                    for definition in STOCK_POOL_DEFINITIONS
                )
                completed = run.succeeded(
                    finished_at=self._clock(),
                    output_rows=len(output.daily_price_limits)
                    + len(output.events)
                    + len(output.members),
                )
                self._persistence.commit_build(completed, output, snapshots)
            except Exception as error:
                self._persistence.fail_calculation_run(
                    run.failed(
                        finished_at=self._clock(),
                        error_summary=f"{type(error).__name__}: stock-pool calculation failed",
                    )
                )
                raise
            return StockPoolBuildSummary(
                "succeeded",
                completed.calculation_id,
                tuple(item.snapshot_id for item in snapshots),
                source.basis_trade_date,
                source.effective_trade_date,
                output.candidate_count,
                len(output.members),
                output.rejected_count,
            )
