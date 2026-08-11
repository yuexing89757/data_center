from datetime import date, datetime
from decimal import Decimal
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.domain.today_limit_up import (
    LimitUpSourceRecord,
    TodayLimitUpDependencies,
    TodayLimitUpMember,
    UpstreamState,
)
from market_data_center.persistence.today_limit_up_postgres import TodayLimitUpFillSummary
from market_data_center.providers.akshare_limit_up import AkshareCurrentDayLimitUpProvider
from market_data_center.providers.contracts import ProviderBatch
from market_data_center.raw_store import LocalRawStore
from market_data_center.scheduler import build_scheduler, run_today_limit_up_snapshot_job
from market_data_center.scheduling_catalog import (
    TODAY_LIMIT_UP_SNAPSHOT_JOB_ID,
    job_definition,
)
from market_data_center.settings import SchedulerSettings
from market_data_center.today_limit_up_service import TodayLimitUpFillService, decide_fill


class Client:
    def stock_zt_pool_em(self, *, date: str) -> object:
        assert date == "20260811"
        return pd.DataFrame(
            [
                {
                    "代码": "000001",
                    "名称": "平安银行",
                    "首次封板时间": "093001",
                    "最后封板时间": "145959",
                    "炸板次数": 2,
                    "封板资金": "123.45",
                }
            ]
        )


def test_akshare_limit_up_mapping_preserves_source_semantics() -> None:
    batch = AkshareCurrentDayLimitUpProvider(Client()).fetch_limit_up_pool(date(2026, 8, 11))
    record = batch.records[0]
    assert record.symbol == "SZSE:000001"
    assert record.open_count == 2
    assert record.source_reported_sealed_funds_cny == Decimal("123.45")
    assert record.first_limit_up_at == datetime(
        2026, 8, 11, 9, 30, 1, tzinfo=ZoneInfo("Asia/Shanghai")
    )


def test_member_uses_exact_free_float_cap_and_bid1_amount() -> None:
    member = TodayLimitUpMember(
        symbol="SSE:600000",
        code="600000",
        historical_name="浦发银行",
        previous_close=Decimal("10"),
        close=Decimal("11"),
        limit_price=Decimal("11"),
        change_percent=Decimal("10.0"),
        free_float_shares=100,
        free_float_market_cap_cny=Decimal("1100"),
        closing_bid1_price=Decimal("11"),
        closing_bid1_volume_shares=20,
        closing_bid1_sealing_amount_cny=Decimal("220"),
    )
    assert member.limit_up_duration_seconds is None


def test_member_rejects_substituted_market_cap() -> None:
    with pytest.raises(ValueError, match="free-float market cap"):
        TodayLimitUpMember(
            symbol="SSE:600000",
            code="600000",
            historical_name="x",
            previous_close=Decimal("10"),
            close=Decimal("11"),
            limit_price=Decimal("11"),
            change_percent=Decimal("10"),
            free_float_shares=100,
            free_float_market_cap_cny=Decimal("1099"),
        )


def test_dependency_policy_defers_without_exact_pool_and_marks_partial_inputs() -> None:
    deferred = decide_fill(
        TodayLimitUpDependencies(
            date(2026, 8, 11),
            True,
            UpstreamState.SUCCEEDED,
            UpstreamState.SUCCEEDED,
            False,
        )
    )
    assert deferred.status.value == "deferred"
    assert not deferred.may_collect_source
    partial = decide_fill(
        TodayLimitUpDependencies(
            date(2026, 8, 11),
            True,
            UpstreamState.PARTIAL,
            UpstreamState.SUCCEEDED,
            True,
        )
    )
    assert partial.status.value == "partial"
    assert partial.may_collect_source


def test_fill_job_is_opt_in_and_fixed_at_2200_shanghai() -> None:
    disabled = job_definition(TODAY_LIMIT_UP_SNAPSHOT_JOB_ID, SchedulerSettings(_env_file=None))
    enabled = job_definition(
        TODAY_LIMIT_UP_SNAPSHOT_JOB_ID,
        SchedulerSettings(today_limit_up_snapshot_enabled=True, _env_file=None),
    )
    assert not disabled.enabled
    assert enabled.enabled
    assert (enabled.hour, enabled.minute, enabled.timezone) == (22, 0, "Asia/Shanghai")


def test_enabled_fill_job_registers_actual_worker_function(tmp_path) -> None:
    scheduler = build_scheduler(
        SchedulerSettings(
            scheduler_store_path=tmp_path / "jobs.sqlite",
            today_limit_up_snapshot_enabled=True,
            _env_file=None,
        )
    )
    job = scheduler.get_job(TODAY_LIMIT_UP_SNAPSHOT_JOB_ID)
    assert job is not None
    assert job.func is run_today_limit_up_snapshot_job


class FillPersistence:
    def __init__(self, dependencies: TodayLimitUpDependencies) -> None:
        self.value = dependencies
        self.created: list[IngestionRun] = []
        self.committed = False

    def dependencies(self, trade_date: date) -> TodayLimitUpDependencies:
        assert trade_date == self.value.trade_date
        return self.value

    def create_ingestion_run(self, run: IngestionRun) -> None:
        self.created.append(run)

    def fail_ingestion_run(self, run: IngestionRun) -> None:
        self.created.append(run)

    def commit_deferred(
        self, trade_date: date, reasons: tuple[str, ...]
    ) -> TodayLimitUpFillSummary:
        assert reasons
        return TodayLimitUpFillSummary("deferred", trade_date, 1, 0, 0, 0, uuid4())

    def commit_failed_source(
        self, run: IngestionRun, manifest: RawManifest, quality: tuple[QualityResult, ...]
    ) -> None:
        raise AssertionError("unexpected failed source")

    def commit_snapshot(
        self,
        *,
        trade_date: date,
        requested_status: object,
        run: IngestionRun,
        manifest: RawManifest,
        source_records: tuple[LimitUpSourceRecord, ...],
        ingestion_quality: tuple[QualityResult, ...],
    ) -> TodayLimitUpFillSummary:
        assert manifest.ingestion_id == run.ingestion_id
        assert source_records[0].symbol == "SZSE:000001"
        self.committed = True
        return TodayLimitUpFillSummary("ready", trade_date, 1, 1, 1, 0, uuid4())


class FillProvider:
    source_code = "akshare"

    def __enter__(self) -> "FillProvider":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback

    def fetch_limit_up_pool(self, trade_date: date) -> ProviderBatch[LimitUpSourceRecord]:
        record = LimitUpSourceRecord(trade_date, "SZSE:000001", "x", None, None, 0, None)
        return ProviderBatch(
            raw_rows=({"代码": "000001"},),
            request_params={},
            schema_version="test.v1",
            records=(record,),
        )


def test_fill_service_defers_before_provider_io(tmp_path) -> None:
    persistence = FillPersistence(
        TodayLimitUpDependencies(
            date(2026, 8, 11), True, UpstreamState.MISSING, UpstreamState.SUCCEEDED, False
        )
    )
    called = False

    def provider() -> FillProvider:
        nonlocal called
        called = True
        return FillProvider()

    result = TodayLimitUpFillService(
        persistence=persistence, raw_store=LocalRawStore(tmp_path), provider_factory=provider
    ).fill(date(2026, 8, 11))
    assert result.status == "deferred"
    assert not called


def test_fill_service_commits_raw_and_snapshot(tmp_path) -> None:
    persistence = FillPersistence(
        TodayLimitUpDependencies(
            date(2026, 8, 11), True, UpstreamState.SUCCEEDED, UpstreamState.SUCCEEDED, True
        )
    )
    result = TodayLimitUpFillService(
        persistence=persistence,
        raw_store=LocalRawStore(tmp_path),
        provider_factory=FillProvider,
    ).fill(date(2026, 8, 11))
    assert result.status == "ready"
    assert persistence.committed
