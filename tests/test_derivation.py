from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.derivation import DerivationService
from market_data_center.domain import (
    CalculationMode,
    CalculationRun,
    DailyBarRecord,
    DerivedCalculationInput,
    DerivedCalculationOutput,
    Market,
    TradeStatus,
)

START_DATE = date(2026, 7, 28)
END_DATE = date(2026, 7, 29)
NOW = datetime(2026, 7, 29, 8, tzinfo=UTC)
CALCULATION_ID = UUID("f71519cf-f836-42d7-87fe-edc4624a8d07")


def _inputs() -> DerivedCalculationInput:
    return DerivedCalculationInput(
        daily_bars=(
            DailyBarRecord(
                symbol="SSE:600000",
                trade_date=START_DATE,
                market=Market.CN_A_SHARE,
                open=Decimal("10"),
                high=Decimal("10"),
                low=Decimal("10"),
                close=Decimal("10"),
                previous_close=Decimal("9.5"),
                volume=100,
                amount=Decimal("1000"),
                trade_status=TradeStatus.TRADING,
                is_st=False,
                source_code="baostock",
            ),
        ),
        distributions=(),
        rights_issues=(),
        share_capital=(),
        memberships=(),
    )


class StubDerivedPersistence:
    def __init__(self) -> None:
        self.inputs = _inputs()
        self.existing: UUID | None = None
        self.created: list[CalculationRun] = []
        self.failed: list[CalculationRun] = []
        self.committed: list[tuple[CalculationRun, DerivedCalculationOutput]] = []

    def calculation_lock(
        self, calculation_code: str, algorithm_version: str, start_date: date, end_date: date
    ) -> AbstractContextManager[None]:
        return nullcontext()

    def load_calculation_source(
        self, start_date: date, end_date: date
    ) -> tuple[DerivedCalculationInput, dict[str, str | None]]:
        return self.inputs, {"daily_bar": NOW.isoformat()}

    def succeeded_calculation_id(
        self,
        *,
        calculation_code: str,
        algorithm_version: str,
        start_date: date,
        end_date: date,
        input_hash: str,
    ) -> UUID | None:
        return self.existing

    def create_calculation_run(self, run: CalculationRun) -> None:
        self.created.append(run)

    def fail_calculation_run(self, run: CalculationRun) -> None:
        self.failed.append(run)

    def commit_calculation(self, run: CalculationRun, output: DerivedCalculationOutput) -> None:
        self.committed.append((run, output))


def test_derivation_service_records_version_watermark_and_outputs() -> None:
    persistence = StubDerivedPersistence()
    service = DerivationService(persistence, clock=lambda: NOW, uuid_factory=lambda: CALCULATION_ID)

    summary = service.recompute(
        START_DATE,
        END_DATE,
        mode=CalculationMode.FULL,
        algorithm_version="1.0.0",
    )

    assert summary.status == "succeeded"
    assert summary.output_rows == 3
    assert persistence.created[0].input_watermark == {"daily_bar": NOW.isoformat()}
    run, output = persistence.committed[0]
    assert run.algorithm_version == "1.0.0"
    assert len(output.adjusted_daily_bars) == 2
    assert len(output.daily_metrics) == 1


def test_derivation_service_rejects_an_unknown_algorithm_version() -> None:
    service = DerivationService(StubDerivedPersistence())

    with pytest.raises(ValueError, match="unsupported algorithm_version"):
        service.recompute(START_DATE, END_DATE, algorithm_version="1.2.0")


def test_incremental_recompute_skips_identical_input_signature() -> None:
    persistence = StubDerivedPersistence()
    persistence.existing = CALCULATION_ID
    service = DerivationService(persistence, clock=lambda: NOW)

    summary = service.recompute(START_DATE, END_DATE)

    assert summary.status == "unchanged"
    assert summary.calculation_id == CALCULATION_ID
    assert persistence.created == []
    assert persistence.committed == []


def test_calculation_failure_is_recorded_without_error_details(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    persistence = StubDerivedPersistence()
    service = DerivationService(persistence, clock=lambda: NOW, uuid_factory=lambda: CALCULATION_ID)

    def fail_calculation(*args: object, **kwargs: object) -> DerivedCalculationOutput:
        raise RuntimeError("sensitive implementation detail")

    monkeypatch.setattr("market_data_center.derivation.calculate_derived_facts", fail_calculation)

    with pytest.raises(RuntimeError, match="sensitive implementation detail"):
        service.recompute(START_DATE, END_DATE)

    assert persistence.failed[0].error_summary == "RuntimeError: calculation failed"
