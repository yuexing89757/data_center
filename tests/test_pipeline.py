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
    IngestionEnvelope,
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
    source_code = "baostock"
    fail_security = False
    mismatched_source = False
    fail_normalization = False

    def source_symbol(self, symbol: str) -> str:
        return symbol

    def fetch_securities(self) -> ProviderBatch[SecurityRecord]:
        if self.fail_security:
            raise RuntimeError("provider unavailable with secret detail")
        if self.fail_normalization:
            return ProviderBatch(
                raw_rows=[{"code": "broken"}],
                request_params={},
                schema_version="security.v1",
                record_factory=self._fail_record_normalization,
            )
        security = _security()
        if self.mismatched_source:
            security = SecurityRecord(
                symbol=security.symbol,
                code=security.code,
                exchange=security.exchange,
                name=security.name,
                security_type=security.security_type,
                status=security.status,
                ipo_date=security.ipo_date,
                delisting_date=security.delisting_date,
                source_code="akshare",
            )
        return ProviderBatch(
            records=[security],
            raw_rows=[{"code": "sh.600000"}],
            request_params={},
            schema_version="security.v1",
        )

    @staticmethod
    def _fail_record_normalization() -> list[SecurityRecord]:
        raise ValueError("invalid source row with sensitive detail")

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
        self.security_commits: list[
            tuple[IngestionRun, RawManifest, Sequence[IngestionEnvelope[SecurityRecord]]]
        ] = []
        self.calendar_commits: list[
            tuple[
                IngestionRun,
                RawManifest,
                Sequence[IngestionEnvelope[CalculatedTradingDay]],
            ]
        ] = []
        self.daily_commits: list[
            tuple[
                IngestionRun,
                RawManifest,
                Sequence[IngestionEnvelope[DailyBarRecord]],
                Sequence[QualityResult],
            ]
        ] = []
        self.rejected_commits: list[tuple[IngestionRun, RawManifest, Sequence[QualityResult]]] = []
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
        records: Sequence[IngestionEnvelope[SecurityRecord]],
    ) -> None:
        self.security_commits.append((run, manifest, records))

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[CalculatedTradingDay]],
    ) -> None:
        self.calendar_commits.append((run, manifest, records))

    def known_symbols(self, symbols: Collection[str]) -> set[str]:
        return self.symbols.intersection(symbols)

    def known_trading_dates(self, dates: Collection[date]) -> set[date]:
        return self.trading_dates.intersection(dates)

    def trading_day_boundaries(
        self, start_date: date, end_date: date
    ) -> tuple[date | None, date | None]:
        return None, None

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        records: Sequence[IngestionEnvelope[DailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.daily_commits.append((run, manifest, records, quality_results))

    def commit_rejected_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.rejected_commits.append((run, manifest, quality_results))


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
    assert not persistence.daily_commits[0][2]
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


def test_mismatched_record_source_keeps_raw_but_blocks_core_write(tmp_path: Path) -> None:
    provider = StubProvider()
    provider.mismatched_source = True
    persistence = StubPersistence()
    pipeline = _pipeline(tmp_path, provider, persistence)

    with pytest.raises(RuntimeError, match="mismatched source_code"):
        pipeline.ingest_securities()

    assert persistence.security_commits == []
    assert persistence.rejected_commits[0][0].status is IngestionStatus.FAILED


def test_normalization_failure_keeps_raw_and_quality_evidence(tmp_path: Path) -> None:
    provider = StubProvider()
    provider.fail_normalization = True
    persistence = StubPersistence()
    pipeline = _pipeline(tmp_path, provider, persistence)

    with pytest.raises(RuntimeError, match="normalization failed"):
        pipeline.ingest_securities()

    failed, manifest, quality_results = persistence.rejected_commits[0]
    assert failed.status is IngestionStatus.FAILED
    assert failed.fetched_rows == 1
    assert failed.rejected_rows == 1
    assert tmp_path.joinpath(*manifest.object_path.split("/")).exists()
    assert quality_results[0].rule_code == "security.provider_normalization"
    assert "sensitive detail" not in quality_results[0].message
    assert persistence.security_commits == []
