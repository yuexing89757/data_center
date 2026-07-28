from collections.abc import Collection, Sequence
from contextlib import AbstractContextManager, nullcontext
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import pytest

from market_data_center.domain import (
    DailyBarRecord,
    Exchange,
    IngestionRun,
    IngestionStatus,
    Market,
    QualityResult,
    RawManifest,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    TradeStatus,
    TradingDayRecord,
)
from market_data_center.domain.entities import CalculatedTradingDay
from market_data_center.pipeline import IngestionPipeline
from market_data_center.providers.contracts import ProviderBatch
from market_data_center.raw_store import LocalRawStore


def _security() -> SecurityRecord:
    return SecurityRecord(
        symbol="SSE:600000",
        code="600000",
        exchange=Exchange.SSE,
        name="浦发银行",
        security_type=SecurityType.STOCK,
        status=SecurityStatus.LISTED,
        ipo_date=date(1999, 11, 10),
        delisting_date=None,
        source_code="baostock",
    )


def _daily_bar() -> DailyBarRecord:
    return DailyBarRecord(
        symbol="SSE:600000",
        trade_date=date(2026, 7, 28),
        market=Market.CN_A_SHARE,
        open=Decimal("10"),
        high=Decimal("11"),
        low=Decimal("9"),
        close=Decimal("10.5"),
        previous_close=Decimal("9.9"),
        volume=100,
        amount=Decimal("1050"),
        trade_status=TradeStatus.TRADING,
        is_st=False,
        source_code="baostock",
    )


class StubProvider:
    fail_security = False

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        if self.fail_security:
            raise RuntimeError("provider unavailable with secret detail")
        return ProviderBatch(
            records=[_security()],
            raw_rows=[{"code": "sh.600000"}],
            request_params={},
            schema_version="security.v1",
        )

    def fetch_trading_calendar(
        self, start_date: date, end_date: date
    ) -> ProviderBatch[TradingDayRecord]:
        return ProviderBatch(
            records=[TradingDayRecord(Market.CN_A_SHARE, start_date, True, "baostock")],
            raw_rows=[{"calendar_date": start_date.isoformat(), "is_trading_day": "1"}],
            request_params={},
            schema_version="calendar.v1",
        )

    def fetch_daily_bars(
        self, source_symbol: str, start_date: date, end_date: date
    ) -> ProviderBatch[DailyBarRecord]:
        return ProviderBatch(
            records=[_daily_bar()],
            raw_rows=[{"code": source_symbol, "date": start_date.isoformat()}],
            request_params={},
            schema_version="daily.v1",
        )


class StubPersistence:
    def __init__(self) -> None:
        self.created: list[IngestionRun] = []
        self.failed: list[IngestionRun] = []
        self.security_commits: list[tuple[IngestionRun, RawManifest, Sequence[SecurityRecord]]] = []
        self.calendar_commits: list[
            tuple[IngestionRun, RawManifest, Sequence[CalculatedTradingDay]]
        ] = []
        self.daily_commits: list[
            tuple[
                IngestionRun,
                RawManifest,
                Sequence[DailyBarRecord],
                Sequence[QualityResult],
            ]
        ] = []
        self.symbols: set[str] = {"SSE:600000"}
        self.trading_dates: set[date] = {date(2026, 7, 28)}

    def task_lock(self, task_key: str) -> AbstractContextManager[None]:
        return nullcontext()

    def create_ingestion_run(self, run: IngestionRun) -> None:
        self.created.append(run)

    def fail_ingestion_run(self, run: IngestionRun) -> None:
        self.failed.append(run)

    def commit_security_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[SecurityRecord],
    ) -> None:
        self.security_commits.append((run, manifest, records))

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[CalculatedTradingDay],
    ) -> None:
        self.calendar_commits.append((run, manifest, records))

    def known_symbols(self, symbols: Collection[str]) -> set[str]:
        return self.symbols.intersection(symbols)

    def known_trading_dates(self, dates: Collection[date]) -> set[date]:
        return self.trading_dates.intersection(dates)

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[DailyBarRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.daily_commits.append((run, manifest, records, quality_results))


def _pipeline(
    tmp_path: Path, provider: StubProvider, persistence: StubPersistence
) -> IngestionPipeline:
    return IngestionPipeline(
        provider=provider,
        raw_store=LocalRawStore(tmp_path),
        persistence=persistence,
        clock=lambda: datetime(2026, 7, 28, 8, tzinfo=UTC),
        uuid_factory=uuid4,
    )


def test_security_pipeline_commits_raw_and_success_together(tmp_path: Path) -> None:
    persistence = StubPersistence()
    pipeline = _pipeline(tmp_path, StubProvider(), persistence)

    run = pipeline.ingest_securities()

    assert run.status is IngestionStatus.SUCCEEDED
    assert run.fetched_rows == 1
    assert len(persistence.security_commits) == 1
    manifest = persistence.security_commits[0][1]
    assert tmp_path.joinpath(*manifest.object_path.split("/")).exists()


def test_daily_bar_reference_failure_is_recorded_and_blocked(tmp_path: Path) -> None:
    persistence = StubPersistence()
    persistence.symbols.clear()
    pipeline = _pipeline(tmp_path, StubProvider(), persistence)

    run = pipeline.ingest_daily_bars("sh.600000", date(2026, 7, 28), date(2026, 7, 28))

    assert run.status is IngestionStatus.FAILED
    assert run.accepted_rows == 0
    assert run.rejected_rows == 1
    assert persistence.daily_commits[0][2] == []
    assert persistence.daily_commits[0][3][0].blocks_core_write


def test_provider_failure_marks_run_failed_without_leaking_message(tmp_path: Path) -> None:
    provider = StubProvider()
    provider.fail_security = True
    persistence = StubPersistence()
    pipeline = _pipeline(tmp_path, provider, persistence)

    with pytest.raises(RuntimeError, match="provider unavailable"):
        pipeline.ingest_securities()

    assert persistence.failed[0].status is IngestionStatus.FAILED
    assert persistence.failed[0].error_summary == "RuntimeError: ingestion failed"
