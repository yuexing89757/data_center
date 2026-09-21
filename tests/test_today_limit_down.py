from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from market_data_center.domain.today_limit_down import (
    TodayLimitDownDependencies,
    TodayLimitDownMember,
    UpstreamState,
)
from market_data_center.persistence.today_limit_down_postgres import (
    TodayLimitDownFillSummary,
    _missing_core,
)
from market_data_center.persistence.today_limit_down_postgres import _member as member_from_row
from market_data_center.providers.akshare_limit_down import AkshareCurrentDayLimitDownProvider
from market_data_center.providers.contracts import ProviderError
from market_data_center.raw_store import LocalRawStore
from market_data_center.scheduler import build_scheduler, run_today_limit_down_snapshot_job
from market_data_center.scheduling_catalog import TODAY_LIMIT_DOWN_SNAPSHOT_JOB_ID, job_definition
from market_data_center.settings import SchedulerSettings
from market_data_center.today_limit_down_service import (
    ManagedLimitDownProvider,
    TodayLimitDownFillService,
    TodayLimitDownPersistence,
    decide_fill,
)


class Client:
    def stock_zt_pool_dtgc_em(self, *, date: str) -> object:
        assert date == "20260918"
        return pd.DataFrame(
            [
                {
                    "代码": "000001",
                    "名称": "历史名称仅供核验",
                    "最后封板时间": "145959",
                    "封单资金": "1200000",
                    "连续跌停": 2,
                    "开板次数": 1,
                }
            ]
        )


def test_provider_maps_down_source_and_replays_raw_rows() -> None:
    batch = AkshareCurrentDayLimitDownProvider(Client()).fetch_limit_down_pool(date(2026, 9, 18))

    assert batch.raw_rows[0]["代码"] == "000001"
    assert batch.records == batch.records
    record = batch.records[0]
    assert record.symbol == "SZSE:000001"
    assert record.first_limit_down_at is None
    assert record.last_limit_down_at == datetime(
        2026, 9, 18, 14, 59, 59, tzinfo=ZoneInfo("Asia/Shanghai")
    )
    assert record.source_reported_sealed_funds_cny == Decimal("1200000")
    assert (record.open_count, record.consecutive_limit_down_days) == (1, 2)


def test_provider_rejects_negative_source_funds() -> None:
    class InvalidClient(Client):
        def stock_zt_pool_dtgc_em(self, *, date: str) -> object:
            frame = super().stock_zt_pool_dtgc_em(date=date)
            frame.loc[0, "封单资金"] = "-1"
            return frame

    batch = AkshareCurrentDayLimitDownProvider(InvalidClient()).fetch_limit_down_pool(
        date(2026, 9, 18)
    )
    with pytest.raises(ProviderError, match="nonnegative"):
        _ = batch.records


def _member(**overrides: object) -> TodayLimitDownMember:
    values: dict[str, object] = dict(
        symbol="SZSE:000001",
        code="000001",
        historical_name="平安银行",
        previous_close=Decimal("10"),
        close=Decimal("9"),
        limit_price=Decimal("9"),
        change_percent=Decimal("-10"),
        free_float_shares=100,
        free_float_market_cap_cny=Decimal("900"),
        closing_ask1_price=Decimal("9"),
        closing_ask1_volume_shares=20,
        closing_ask1_sealing_amount_cny=Decimal("180"),
    )
    values.update(overrides)
    return TodayLimitDownMember(**values)  # type: ignore[arg-type]


def test_member_requires_exact_down_limit_and_ask1_amount() -> None:
    assert _member().limit_down_duration_seconds is None
    with pytest.raises(ValueError, match="exactly equal"):
        _member(close=Decimal("9.01"))
    with pytest.raises(ValueError, match="ask-1 sealing amount"):
        _member(closing_ask1_sealing_amount_cny=Decimal("181"))
    with pytest.raises(ValueError, match="free-float market cap"):
        _member(free_float_market_cap_cny=Decimal("901"))


def test_persistence_member_freezes_ask_side_without_source() -> None:
    row = {
        "symbol": "SZSE:000001",
        "code": "000001",
        "historical_name": "平安银行",
        "previous_close": Decimal("10"),
        "close": Decimal("9"),
        "limit_price": Decimal("9"),
        "free_float_shares": 100,
        "ask1_price": Decimal("9"),
        "ask1_volume": 20,
        "ask2_price": None,
        "ask2_volume": None,
        "ask3_price": None,
        "ask3_volume": None,
        "ask4_price": None,
        "ask4_volume": None,
        "ask5_price": None,
        "ask5_volume": None,
    }

    member = member_from_row(row, None)

    assert member.closing_ask1_sealing_amount_cny == Decimal("180")
    assert member.source_reported_sealed_funds_cny is None


def test_candidate_with_non_limit_close_is_rejected_before_member_build() -> None:
    row = {
        "historical_name": "平安银行",
        "name_ingestion_id": uuid4(),
        "close": Decimal("9.01"),
        "previous_close": Decimal("10"),
        "daily_bar_ingestion_id": uuid4(),
        "limit_price": Decimal("9"),
        "pool_calculation_id": uuid4(),
        "free_float_shares": 100,
        "indicator_ingestion_id": uuid4(),
    }

    assert _missing_core(row) == "close_not_at_lower_limit"


def test_dependency_policy_does_not_call_source_without_exact_down_pool() -> None:
    decision = decide_fill(
        TodayLimitDownDependencies(
            date(2026, 9, 18),
            True,
            UpstreamState.SUCCEEDED,
            UpstreamState.SUCCEEDED,
            False,
        )
    )

    assert decision.status.value == "deferred"
    assert not decision.may_collect_source


def test_dependency_policy_marks_partial_upstream() -> None:
    decision = decide_fill(
        TodayLimitDownDependencies(
            date(2026, 9, 18),
            True,
            UpstreamState.PARTIAL,
            UpstreamState.SUCCEEDED,
            True,
        )
    )

    assert decision.status.value == "partial"
    assert decision.may_collect_source


def test_worker_catalog_schedules_down_snapshot_at_2210_disabled_by_default() -> None:
    disabled = job_definition(TODAY_LIMIT_DOWN_SNAPSHOT_JOB_ID, SchedulerSettings(_env_file=None))
    enabled = job_definition(
        TODAY_LIMIT_DOWN_SNAPSHOT_JOB_ID,
        SchedulerSettings(today_limit_down_snapshot_enabled=True, _env_file=None),
    )

    assert (disabled.hour, disabled.minute, disabled.enabled) == (22, 10, False)
    assert enabled.enabled
    assert enabled.workflow_code == "today_limit_down_snapshot"


def test_enabled_down_snapshot_registers_worker_function(tmp_path: Path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "jobs.sqlite",
            today_limit_down_snapshot_enabled=True,
            _env_file=None,
        )
    )

    job = scheduler.get_job(TODAY_LIMIT_DOWN_SNAPSHOT_JOB_ID)
    assert job is not None
    assert job.func is run_today_limit_down_snapshot_job


def test_source_failure_is_recorded_as_failed_snapshot(tmp_path: Path) -> None:
    trade_date = date(2026, 9, 18)

    class Persistence:
        run = None
        reason = None

        def dependencies(self, _trade_date: date) -> TodayLimitDownDependencies:
            return TodayLimitDownDependencies(
                trade_date, True, UpstreamState.SUCCEEDED, UpstreamState.SUCCEEDED, True
            )

        def create_ingestion_run(self, run: object) -> None:
            self.run = run

        def commit_failed(
            self, _trade_date: date, run: object, reason: str
        ) -> TodayLimitDownFillSummary:
            self.run = run
            self.reason = reason
            return TodayLimitDownFillSummary("failed", trade_date, 1, 0, 0, 0, uuid4())

    class FailingProvider:
        def __enter__(self) -> "FailingProvider":
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        def fetch_limit_down_pool(self, _trade_date: date) -> object:
            raise RuntimeError("source unavailable")

    persistence = Persistence()
    service = TodayLimitDownFillService(
        persistence=cast("TodayLimitDownPersistence", persistence),
        raw_store=LocalRawStore(tmp_path),
        provider_factory=lambda: cast("ManagedLimitDownProvider", FailingProvider()),
    )

    result = service.fill(trade_date)

    assert result.status == "failed"
    assert persistence.reason == "source_request_failed_RuntimeError"
