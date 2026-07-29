"""Application service for deterministic, versioned derived calculations."""

from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Protocol
from uuid import UUID, uuid4

from market_data_center.calculators import calculate_derived_facts, calculation_input_hash
from market_data_center.domain.derived import (
    CalculationMode,
    CalculationRun,
    CalculationStatus,
    DerivedCalculationInput,
    DerivedCalculationOutput,
)

CALCULATION_CODE = "cn_a_share_daily_derived"
DEFAULT_ALGORITHM_VERSION = "1.0.0"


class DerivedPersistence(Protocol):
    def calculation_lock(
        self, calculation_code: str, algorithm_version: str, start_date: date, end_date: date
    ) -> AbstractContextManager[None]: ...

    def load_calculation_source(
        self, start_date: date, end_date: date
    ) -> tuple[DerivedCalculationInput, dict[str, str | None]]: ...

    def succeeded_calculation_id(
        self,
        *,
        calculation_code: str,
        algorithm_version: str,
        start_date: date,
        end_date: date,
        input_hash: str,
    ) -> UUID | None: ...

    def create_calculation_run(self, run: CalculationRun) -> None: ...

    def fail_calculation_run(self, run: CalculationRun) -> None: ...

    def commit_calculation(self, run: CalculationRun, output: DerivedCalculationOutput) -> None: ...


@dataclass(frozen=True, slots=True)
class DerivationSummary:
    status: str
    calculation_id: UUID
    algorithm_version: str
    input_hash: str
    output_rows: int

    def as_json(self) -> Mapping[str, object]:
        return {
            "status": self.status,
            "calculation_id": str(self.calculation_id),
            "algorithm_version": self.algorithm_version,
            "input_hash": self.input_hash,
            "output_rows": self.output_rows,
        }


class DerivationService:
    def __init__(
        self,
        persistence: DerivedPersistence,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def recompute(
        self,
        start_date: date,
        end_date: date,
        *,
        mode: CalculationMode = CalculationMode.INCREMENTAL,
        algorithm_version: str = DEFAULT_ALGORITHM_VERSION,
    ) -> DerivationSummary:
        if end_date < start_date:
            raise ValueError("end_date must not precede start_date")
        if not algorithm_version.strip():
            raise ValueError("algorithm_version must not be blank")
        with self._persistence.calculation_lock(
            CALCULATION_CODE, algorithm_version, start_date, end_date
        ):
            inputs, watermark = self._persistence.load_calculation_source(start_date, end_date)
            if not any(start_date <= bar.trade_date <= end_date for bar in inputs.daily_bars):
                raise ValueError("requested range contains no Daily Bar inputs")
            input_hash = calculation_input_hash(inputs)
            existing = self._persistence.succeeded_calculation_id(
                calculation_code=CALCULATION_CODE,
                algorithm_version=algorithm_version,
                start_date=start_date,
                end_date=end_date,
                input_hash=input_hash,
            )
            if existing is not None:
                return DerivationSummary(
                    status="unchanged",
                    calculation_id=existing,
                    algorithm_version=algorithm_version,
                    input_hash=input_hash,
                    output_rows=0,
                )

            now = self._clock()
            run = CalculationRun(
                calculation_id=self._uuid_factory(),
                calculation_code=CALCULATION_CODE,
                algorithm_version=algorithm_version,
                mode=mode,
                start_date=start_date,
                end_date=end_date,
                status=CalculationStatus.RUNNING,
                input_watermark=watermark,
                input_hash=input_hash,
                requested_at=now,
            )
            self._persistence.create_calculation_run(run)
            try:
                output = calculate_derived_facts(inputs, start_date=start_date, end_date=end_date)
                output_rows = _output_rows(output)
                completed = run.succeeded(finished_at=self._clock(), output_rows=output_rows)
                self._persistence.commit_calculation(completed, output)
            except Exception as error:
                failed = run.failed(
                    finished_at=self._clock(),
                    error_summary=f"{type(error).__name__}: calculation failed",
                )
                self._persistence.fail_calculation_run(failed)
                raise
            return DerivationSummary(
                status="succeeded",
                calculation_id=completed.calculation_id,
                algorithm_version=completed.algorithm_version,
                input_hash=completed.input_hash,
                output_rows=completed.output_rows,
            )


def _output_rows(output: DerivedCalculationOutput) -> int:
    return sum(
        (
            len(output.adjusted_daily_bars),
            len(output.daily_metrics),
            len(output.market_capitalizations),
            len(output.classification_metrics),
        )
    )
