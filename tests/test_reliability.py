from collections.abc import Collection, Mapping, Sequence
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from market_data_center.domain import (
    BoardIndexConstituentSnapshotRecord,
    BoardIndexDailyBarRecord,
    BoardIndexRecord,
    CalculatedTradingDay,
    CapitalRecord,
    ClassificationCatalogSnapshotRecord,
    ClassificationDefinition,
    ClassificationMemberSnapshotRecord,
    ClassificationType,
    DailyBarRecord,
    DatasetCode,
    IngestionEnvelope,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    RawFileFormat,
    RawManifest,
    SecurityRecord,
)
from market_data_center.domain.ingestion import ReplaySource
from market_data_center.raw_store import LocalRawStore, RawIntegrityError
from market_data_center.reliability import (
    RawReplayService,
    compare_daily_bar_sources,
    recover_stale_runs,
)

NOW = datetime(2026, 7, 29, 8, tzinfo=UTC)
SOURCE_RUN_ID = UUID("74b11082-4ec0-4ae4-826f-a80a96cb9985")
RAW_ID = UUID("0be27d94-e215-4c83-87c8-d3613e4b420e")
REPLAY_RUN_ID = UUID("f71519cf-f836-42d7-87fe-edc4624a8d07")
QUALITY_ID = UUID("38949fcb-93e6-46e9-9d54-91645fe2f98a")


class StubReliabilityPersistence:
    def __init__(self, source: ReplaySource) -> None:
        self.source = source
        self.sources: list[ReplaySource] = [source]
        self.created: list[IngestionRun] = []
        self.security_commits: list[
            tuple[
                IngestionRun,
                RawManifest | None,
                Sequence[IngestionEnvelope[SecurityRecord]],
            ]
        ] = []
        self.calendar_commits: list[
            tuple[
                IngestionRun,
                RawManifest | None,
                Sequence[IngestionEnvelope[CalculatedTradingDay]],
            ]
        ] = []
        self.daily_commits: list[
            tuple[
                IngestionRun,
                RawManifest | None,
                Sequence[IngestionEnvelope[DailyBarRecord]],
                Sequence[QualityResult],
            ]
        ] = []
        self.capital_commits: list[
            tuple[
                IngestionRun,
                RawManifest | None,
                Sequence[IngestionEnvelope[CapitalRecord]],
                Sequence[QualityResult],
            ]
        ] = []
        self.classification_catalog_commits: list[
            tuple[
                IngestionRun,
                RawManifest | None,
                IngestionEnvelope[ClassificationCatalogSnapshotRecord],
                Sequence[QualityResult],
            ]
        ] = []
        self.classification_member_commits: list[
            tuple[
                IngestionRun,
                RawManifest | None,
                IngestionEnvelope[ClassificationMemberSnapshotRecord],
                Sequence[QualityResult],
            ]
        ] = []
        self.rejected_commits: list[
            tuple[IngestionRun, RawManifest | None, Sequence[QualityResult]]
        ] = []
        self.stale_ids = [UUID("948c4e5b-97a1-4706-a1de-09c14670108a")]
        self.recovery_args: tuple[datetime, datetime, str] | None = None

    def create_ingestion_run(self, run: IngestionRun) -> None:
        self.created.append(run)

    def replay_source(self, ingestion_id: UUID) -> ReplaySource:
        assert ingestion_id == self.source.source_ingestion_id
        return self.source

    def daily_bar_replay_sources(
        self, symbol: str, start_date: date, end_date: date
    ) -> Sequence[ReplaySource]:
        return self.sources

    def known_symbols(self, symbols: Collection[str]) -> set[str]:
        return set(symbols)

    def known_trading_dates(self, dates: Collection[date]) -> set[date]:
        return set(dates)

    def known_board_ids(self, board_ids: Collection[str]) -> set[str]:
        return set(board_ids)

    def trading_day_boundaries(
        self, start_date: date, end_date: date
    ) -> tuple[date | None, date | None]:
        return None, None

    def commit_security_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[SecurityRecord]],
    ) -> None:
        self.security_commits.append((run, manifest, records))

    def commit_trading_calendar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[CalculatedTradingDay]],
    ) -> None:
        self.calendar_commits.append((run, manifest, records))

    def commit_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[DailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.daily_commits.append((run, manifest, records, quality_results))

    def commit_capital_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[CapitalRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.capital_commits.append((run, manifest, records, quality_results))

    def known_classification_snapshots(
        self, keys: Collection[tuple[str, ClassificationType, str, date]]
    ) -> set[tuple[str, ClassificationType, str, date]]:
        return set(keys)

    def commit_classification_catalog_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[ClassificationCatalogSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.classification_catalog_commits.append((run, manifest, record, quality_results))

    def commit_classification_members_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[ClassificationMemberSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.classification_member_commits.append((run, manifest, record, quality_results))

    def commit_board_index_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[BoardIndexRecord]],
    ) -> None:
        return None

    def commit_board_index_daily_bar_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        records: Sequence[IngestionEnvelope[BoardIndexDailyBarRecord]],
        quality_results: Sequence[QualityResult],
    ) -> None:
        return None

    def commit_board_index_constituents_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        record: IngestionEnvelope[BoardIndexConstituentSnapshotRecord],
        quality_results: Sequence[QualityResult],
    ) -> None:
        return None

    def commit_rejected_batch(
        self,
        run: IngestionRun,
        manifest: RawManifest | None,
        quality_results: Sequence[QualityResult],
    ) -> None:
        self.rejected_commits.append((run, manifest, quality_results))

    def stale_ingestion_run_ids(self, stale_before: datetime) -> Sequence[UUID]:
        return self.stale_ids

    def recover_stale_ingestion_runs(
        self, stale_before: datetime, finished_at: datetime, reason: str
    ) -> Sequence[UUID]:
        self.recovery_args = (stale_before, finished_at, reason)
        return self.stale_ids


def test_raw_replay_reuses_verified_raw_lineage_without_new_manifest(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.BAOSTOCK,
        dataset=DatasetCode.SECURITY,
        schema_version="baostock.security.v1",
        rows=[
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "ipoDate": "1999-11-10",
                "outDate": "",
                "type": "1",
                "status": "1",
            }
        ],
        request_params={},
    )
    persistence = StubReliabilityPersistence(source)
    service = RawReplayService(
        raw_store=store,
        persistence=persistence,
        clock=lambda: NOW,
        uuid_factory=lambda: REPLAY_RUN_ID,
    )

    summary = service.replay(SOURCE_RUN_ID)

    assert summary.status == "succeeded"
    assert persistence.created[0].replayed_from_raw_id == RAW_ID
    completed, replay_manifest, envelopes = persistence.security_commits[0]
    assert replay_manifest is None
    assert completed.ingestion_id == REPLAY_RUN_ID
    assert envelopes[0].ingestion_id == REPLAY_RUN_ID
    assert envelopes[0].record.symbol == "SSE:600000"


def test_raw_replay_dry_run_validates_without_database_writes(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.BAOSTOCK,
        dataset=DatasetCode.SECURITY,
        schema_version="baostock.security.v1",
        rows=[
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "ipoDate": "",
                "outDate": "",
                "type": "1",
                "status": "1",
            }
        ],
        request_params={},
    )
    persistence = StubReliabilityPersistence(source)

    summary = RawReplayService(raw_store=store, persistence=persistence).replay(
        SOURCE_RUN_ID, dry_run=True
    )

    assert summary.status == "valid"
    assert summary.replay_ingestion_id is None
    assert persistence.created == []
    assert persistence.security_commits == []


def test_raw_replay_normalizes_and_commits_capital_facts(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.AKSHARE,
        dataset=DatasetCode.CAPITAL,
        schema_version="akshare.capital.v1",
        rows=[
            {
                "_capital_record_type": "share_capital",
                "变更日期": "2024-01-15",
                "总股本": "1000000",
                "流通受限股份": "100000",
                "已流通股份": "900000",
                "已上市流通A股": "900000",
                "变动原因": "回购",
            }
        ],
        request_params={"source_symbol": "600000", "mode": "backfill"},
    )
    persistence = StubReliabilityPersistence(source)

    summary = RawReplayService(
        raw_store=store,
        persistence=persistence,
        clock=lambda: NOW,
        uuid_factory=lambda: REPLAY_RUN_ID,
    ).replay(SOURCE_RUN_ID)

    assert summary.accepted_rows == 1
    completed, replay_manifest, envelopes, findings = persistence.capital_commits[0]
    assert completed.status is IngestionStatus.SUCCEEDED
    assert replay_manifest is None
    assert findings == ()
    assert envelopes[0].record.symbol == "SSE:600000"


def test_raw_replay_normalizes_and_commits_classification_catalog(
    tmp_path: Path,
) -> None:
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.AKSHARE,
        dataset=DatasetCode.CLASSIFICATION_CATALOG,
        schema_version="akshare.classification_catalog.v1",
        rows=[
            {
                "\u677f\u5757\u540d\u79f0": "\u94f6\u884c",
                "\u677f\u5757\u4ee3\u7801": "BK0475",
            }
        ],
        request_params={
            "classification_type": "industry",
            "snapshot_date": "2026-07-29",
        },
    )
    persistence = StubReliabilityPersistence(source)

    summary = RawReplayService(
        raw_store=store,
        persistence=persistence,
        clock=lambda: NOW,
        uuid_factory=lambda: REPLAY_RUN_ID,
    ).replay(SOURCE_RUN_ID)

    assert summary.status == "succeeded"
    completed, replay_manifest, envelope, findings = persistence.classification_catalog_commits[0]
    assert completed.accepted_rows == 1
    assert replay_manifest is None
    assert findings == ()
    assert envelope.record.definitions == (
        ClassificationDefinition(code="BK0475", name="\u94f6\u884c"),
    )


def test_raw_replay_records_integrity_failure_without_copying_manifest(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.BAOSTOCK,
        dataset=DatasetCode.SECURITY,
        schema_version="baostock.security.v1",
        rows=[{"code": "sh.600000"}],
        request_params={},
    )
    assert source.manifest is not None
    raw_path = tmp_path.joinpath(*source.manifest.object_path.split("/"))
    raw_path.write_bytes(raw_path.read_bytes() + b"tampered")
    persistence = StubReliabilityPersistence(source)
    ids = iter((REPLAY_RUN_ID, QUALITY_ID))
    service = RawReplayService(
        raw_store=store,
        persistence=persistence,
        clock=lambda: NOW,
        uuid_factory=lambda: next(ids),
    )

    with pytest.raises(RawIntegrityError, match="byte size"):
        service.replay(SOURCE_RUN_ID)

    failed, replay_manifest, results = persistence.rejected_commits[0]
    assert failed.status is IngestionStatus.FAILED
    assert replay_manifest is None
    assert results[0].rule_code == "security.raw_replay"
    assert "tampered" not in results[0].message


def test_raw_replay_blocks_an_unsupported_schema_and_records_quality(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    source = _source(
        store,
        provider=ProviderCode.BAOSTOCK,
        dataset=DatasetCode.SECURITY,
        schema_version="baostock.security.v1",
        rows=[
            {
                "code": "sh.600000",
                "code_name": "浦发银行",
                "ipoDate": "",
                "outDate": "",
                "type": "1",
                "status": "1",
            }
        ],
        request_params={},
    )
    assert source.manifest is not None
    source = replace(
        source,
        manifest=replace(source.manifest, schema_version="baostock.security.v999"),
    )
    persistence = StubReliabilityPersistence(source)
    ids = iter((REPLAY_RUN_ID, QUALITY_ID))

    with pytest.raises(RuntimeError, match="unsupported BaoStock Raw schema"):
        RawReplayService(
            raw_store=store,
            persistence=persistence,
            clock=lambda: NOW,
            uuid_factory=lambda: next(ids),
        ).replay(SOURCE_RUN_ID)

    assert persistence.rejected_commits[0][2][0].details == {"error_type": "ProviderError"}


def test_stale_recovery_supports_dry_run_and_atomic_recovery(tmp_path: Path) -> None:
    source = ReplaySource(
        source_ingestion_id=SOURCE_RUN_ID,
        provider_code=ProviderCode.BAOSTOCK,
        dataset_code=DatasetCode.SECURITY,
        requested_at=NOW,
        request_params={},
        manifest=None,
    )
    persistence = StubReliabilityPersistence(source)

    dry_ids = recover_stale_runs(
        persistence,
        older_than=timedelta(minutes=30),
        dry_run=True,
        clock=lambda: NOW,
    )
    recovered_ids = recover_stale_runs(
        persistence,
        older_than=timedelta(minutes=30),
        dry_run=False,
        clock=lambda: NOW,
    )

    assert dry_ids == tuple(persistence.stale_ids)
    assert recovered_ids == tuple(persistence.stale_ids)
    assert persistence.recovery_args is not None
    assert persistence.recovery_args[0] == NOW - timedelta(minutes=30)
    assert persistence.recovery_args[1] == NOW


def test_cross_source_comparison_reports_differences_without_writes(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    baostock = _source(
        store,
        provider=ProviderCode.BAOSTOCK,
        dataset=DatasetCode.DAILY_BAR,
        schema_version="baostock.daily_bar.v1",
        rows=[
            {
                "date": "2026-07-28",
                "code": "sh.600000",
                "open": "10.00",
                "high": "11.00",
                "low": "9.00",
                "close": "10.50",
                "preclose": "9.90",
                "volume": "100",
                "amount": "1050.00",
                "tradestatus": "1",
                "isST": "0",
            }
        ],
        request_params={
            "source_symbol": "sh.600000",
            "start_date": "2026-07-28",
            "end_date": "2026-07-28",
        },
    )
    akshare = _source(
        store,
        provider=ProviderCode.AKSHARE,
        dataset=DatasetCode.DAILY_BAR,
        schema_version="akshare.daily_bar.v1",
        rows=[
            {
                "日期": "2026-07-28",
                "股票代码": "600000",
                "开盘": "10.00",
                "收盘": "10.60",
                "最高": "11.00",
                "最低": "9.00",
                "成交量": "100",
                "成交额": "1050.00",
                "涨跌额": "0.70",
            }
        ],
        request_params={
            "source_symbol": "600000",
            "start_date": "20260728",
            "end_date": "20260728",
        },
        source_ingestion_id=UUID("9dc013c9-d685-40e6-b986-b7e9a802d6cd"),
        raw_id=UUID("0503008e-9132-47c4-b8c9-b8672f46ff5d"),
    )
    persistence = StubReliabilityPersistence(baostock)
    persistence.sources = [baostock, akshare]

    report = compare_daily_bar_sources(
        persistence,
        store,
        symbol="SSE:600000",
        start_date=date(2026, 7, 28),
        end_date=date(2026, 7, 28),
    )

    assert report.providers == ("akshare", "baostock")
    assert report.comparable_dates == 1
    assert report.mismatched_dates == 1
    changed_fields = report.differences[0]["fields"]
    assert isinstance(changed_fields, Mapping)
    assert changed_fields["close"] == {"akshare": "10.60", "baostock": "10.50"}
    assert persistence.created == []
    assert persistence.daily_commits == []


def _source(
    store: LocalRawStore,
    *,
    provider: ProviderCode,
    dataset: DatasetCode,
    schema_version: str,
    rows: list[Mapping[str, str]],
    request_params: Mapping[str, object],
    source_ingestion_id: UUID = SOURCE_RUN_ID,
    raw_id: UUID = RAW_ID,
) -> ReplaySource:
    stored = store.write_jsonl(
        provider=provider.value,
        dataset=dataset.value,
        partition_date=NOW.date(),
        ingestion_id=source_ingestion_id,
        rows=rows,
        schema_version=schema_version,
    )
    manifest = RawManifest(
        raw_id=raw_id,
        ingestion_id=source_ingestion_id,
        object_path=stored.object_path,
        file_format=RawFileFormat.JSONL,
        content_sha256=stored.content_sha256,
        byte_size=stored.byte_size,
        row_count=stored.row_count,
        schema_version=stored.schema_version,
    )
    return ReplaySource(
        source_ingestion_id=source_ingestion_id,
        provider_code=provider,
        dataset_code=dataset,
        requested_at=NOW,
        request_params=request_params,
        manifest=manifest,
    )
