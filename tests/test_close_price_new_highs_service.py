from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

import pytest

from market_data_center.close_price_new_highs_service import ClosePriceNewHighsService
from market_data_center.domain.close_price_new_highs import (
    ClosePriceNewHighCandidate,
    ClosePriceNewHighInput,
    ClosePriceNewHighSnapshot,
)
from market_data_center.domain.derived import CalculationRun
from market_data_center.domain.records import TradeStatus

TRADE_DATE = date(2026, 8, 14)
NOW = datetime(2026, 8, 14, 13, 35, tzinfo=UTC)
CALCULATION_ID = UUID("00000000-0000-0000-0000-000000000001")
SNAPSHOT_ID = UUID("00000000-0000-0000-0000-000000000002")


def _input(close: str = "12.50") -> ClosePriceNewHighInput:
    return ClosePriceNewHighInput(
        TRADE_DATE,
        date(2026, 2, 23),
        120,
        (
            ClosePriceNewHighCandidate(
                symbol="SSE:600000",
                code="600000",
                display_name="浦发银行",
                valid_bar_count=120,
                close=Decimal(close),
                current_status=TradeStatus.UNKNOWN,
                previous_119d_high=Decimal("12.00"),
                has_non_trading_bar=False,
                has_nonpositive_price=False,
            ),
        ),
    )


class MemoryPersistence:
    def __init__(self, source: ClosePriceNewHighInput) -> None:
        self.source = source
        self.trading_day = True
        self.snapshots: list[ClosePriceNewHighSnapshot] = []
        self.runs: list[CalculationRun] = []
        self.failed_runs: list[CalculationRun] = []
        self.raise_on_publish = False
        self.raise_on_version = False

    @contextmanager
    def build_lock(self, trade_date):  # type: ignore[no-untyped-def]
        assert trade_date == TRADE_DATE
        yield

    def is_trading_day(self, trade_date):  # type: ignore[no-untyped-def]
        assert trade_date == TRADE_DATE
        return self.trading_day

    def load_input(self, trade_date):  # type: ignore[no-untyped-def]
        assert trade_date == TRADE_DATE
        return self.source, {"daily_market_workflow_run_id": "run-1"}

    def existing_snapshot(self, trade_date, input_hash):  # type: ignore[no-untyped-def]
        for snapshot in self.snapshots:
            if snapshot.trade_date == trade_date and snapshot.input_hash == input_hash:
                return snapshot.calculation_id, snapshot.snapshot_id
        return None

    def create_calculation_run(self, run):  # type: ignore[no-untyped-def]
        self.runs.append(run)

    def next_snapshot_version(self, trade_date):  # type: ignore[no-untyped-def]
        if self.raise_on_version:
            raise RuntimeError("version allocation failed")
        return 1 + max(
            (snapshot.version for snapshot in self.snapshots if snapshot.trade_date == trade_date),
            default=0,
        )

    def publish(self, run, calculation, snapshot):  # type: ignore[no-untyped-def]
        if self.raise_on_publish:
            raise RuntimeError("publish failed")
        assert run.status.value == "succeeded"
        assert snapshot.member_count == len(calculation.members)
        self.runs[-1] = run
        self.snapshots.append(snapshot)

    def fail_calculation_run(self, run):  # type: ignore[no-untyped-def]
        self.failed_runs.append(run)


def _service(persistence: MemoryPersistence) -> ClosePriceNewHighsService:
    ids = iter((CALCULATION_ID, SNAPSHOT_ID))
    return ClosePriceNewHighsService(
        persistence,  # type: ignore[arg-type]
        clock=lambda: NOW,
        uuid_factory=lambda: next(ids),
    )


def test_service_publishes_ready_snapshot_and_reuses_same_input() -> None:
    persistence = MemoryPersistence(_input())
    service = _service(persistence)

    first = service.build(TRADE_DATE)
    second = service.build(TRADE_DATE)

    assert first.status == "succeeded"
    assert first.member_count == 1
    assert second.status == "unchanged"
    assert second.snapshot_id == first.snapshot_id
    assert len(persistence.snapshots) == 1


def test_service_publishes_empty_ready_snapshot() -> None:
    persistence = MemoryPersistence(_input(close="11.50"))

    result = _service(persistence).build(TRADE_DATE)

    assert result.status == "succeeded"
    assert result.member_count == 0
    assert persistence.snapshots[0].member_count == 0


def test_revised_input_creates_next_version() -> None:
    persistence = MemoryPersistence(_input())
    first = _service(persistence).build(TRADE_DATE)
    persistence.source = replace(_input(), candidates=_input(close="13.00").candidates)
    ids = iter(
        (
            UUID("00000000-0000-0000-0000-000000000003"),
            UUID("00000000-0000-0000-0000-000000000004"),
        )
    )

    second = ClosePriceNewHighsService(
        persistence,  # type: ignore[arg-type]
        clock=lambda: NOW,
        uuid_factory=lambda: next(ids),
    ).build(TRADE_DATE)

    assert first.snapshot_id != second.snapshot_id
    assert [snapshot.version for snapshot in persistence.snapshots] == [1, 2]


def test_publish_failure_marks_calculation_failed() -> None:
    persistence = MemoryPersistence(_input())
    persistence.raise_on_publish = True

    with pytest.raises(RuntimeError, match="publish failed"):
        _service(persistence).build(TRADE_DATE)

    assert persistence.failed_runs[0].status.value == "failed"


def test_version_allocation_failure_marks_calculation_failed() -> None:
    persistence = MemoryPersistence(_input())
    persistence.raise_on_version = True

    with pytest.raises(RuntimeError, match="version allocation failed"):
        _service(persistence).build(TRADE_DATE)

    assert persistence.failed_runs[0].status.value == "failed"


def test_non_trading_day_is_a_successful_skip_without_snapshot() -> None:
    persistence = MemoryPersistence(_input())
    persistence.trading_day = False

    result = _service(persistence).build(TRADE_DATE)

    assert result.status == "skipped"
    assert result.calculation_id is None
    assert result.snapshot_id is None
    assert persistence.runs == []
    assert persistence.snapshots == []
