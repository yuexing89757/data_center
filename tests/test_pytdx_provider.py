from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_center.domain import (
    ClassificationCatalogSnapshotRecord,
    ClassificationMemberSnapshotRecord,
    ClassificationType,
    DailyBarRecord,
    DatasetCode,
    TradeStatus,
)
from market_data_center.providers import (
    ProviderError,
    PytdxProvider,
)
from market_data_center.providers.pytdx import LocalDailyBarRow, normalize_pytdx_raw


def _bar(trade_date: int, *, close: int = 1020, volume: int = 123_400) -> LocalDailyBarRow:
    return (trade_date, 1000, 1050, 990, close, 1_258_680.0, volume, 0)


class FakeReader:
    def __init__(self, rows: Iterable[LocalDailyBarRow]) -> None:
        self.rows = list(rows)
        self.requested_files: list[str] = []

    def parse_data_by_file(self, fname: str) -> Iterable[LocalDailyBarRow]:
        self.requested_files.append(fname)
        return iter(self.rows)


def _provider(tmp_path: Path, rows: Iterable[LocalDailyBarRow]) -> tuple[PytdxProvider, FakeReader]:
    file_path = tmp_path / "sh" / "lday" / "sh600000.day"
    file_path.parent.mkdir(parents=True)
    file_path.touch()
    (file_path.parent / "sh000001.day").touch()
    reader = FakeReader(rows)
    return PytdxProvider(reader, vipdoc_path=tmp_path), reader


def test_context_does_not_open_a_network_connection(tmp_path: Path) -> None:
    provider, _ = _provider(tmp_path, [_bar(20260728)])

    with provider as entered:
        assert entered is provider


def test_standard_symbol_maps_to_local_file_prefix() -> None:
    provider = PytdxProvider(FakeReader([]), vipdoc_path=Path("vipdoc"))

    assert provider.source_symbol("SSE:600000") == "sh.600000"
    assert provider.source_symbol("SZSE:000001") == "sz.000001"
    assert provider.source_symbol("BSE:920000") == "bj.920000"


def test_bse_daily_bars_use_local_bj_files_and_market_sentinel(tmp_path: Path) -> None:
    file_path = tmp_path / "bj" / "lday" / "bj920000.day"
    file_path.parent.mkdir(parents=True)
    file_path.touch()
    (file_path.parent / "bj899050.day").touch()
    reader = FakeReader([_bar(20260729)])
    provider = PytdxProvider(reader, vipdoc_path=tmp_path)

    batch = provider.fetch_daily_bars("bj.920000", date(2026, 7, 29), date(2026, 7, 29))

    assert [record.symbol for record in batch.records] == ["BSE:920000"]
    assert reader.requested_files == [str(file_path), str(file_path.parent / "bj899050.day")]


def test_daily_bars_read_local_file_crop_sort_and_normalize_values(tmp_path: Path) -> None:
    provider, reader = _provider(
        tmp_path,
        [_bar(20260724), _bar(20260727), _bar(20260728), _bar(20260723)],
    )

    batch = provider.fetch_daily_bars("sh.600000", date(2026, 7, 24), date(2026, 7, 28))

    assert [record.trade_date for record in batch.records] == [
        date(2026, 7, 24),
        date(2026, 7, 27),
        date(2026, 7, 28),
    ]
    record = batch.records[0]
    assert record.symbol == "SSE:600000"
    assert record.close == Decimal("10.20")
    assert record.volume == 123_400
    assert record.amount == Decimal("1258680.0")
    assert record.previous_close == Decimal("10.20")
    assert record.trade_status is TradeStatus.UNKNOWN
    assert record.is_st is None
    assert reader.requested_files[0] == str(tmp_path / "sh" / "lday" / "sh600000.day")
    assert len(batch.raw_rows) == 3
    assert batch.schema_version == "pytdx.local_daily_bar.v2"

    legacy_rows = tuple(
        {key: value for key, value in row.items() if key != "previous_close"}
        for row in batch.raw_rows
    )
    replayed = normalize_pytdx_raw(
        DatasetCode.DAILY_BAR,
        "pytdx.local_daily_bar.v1",
        legacy_rows,
        batch.request_params,
    )
    assert isinstance(replayed[0], DailyBarRecord)
    assert isinstance(replayed[1], DailyBarRecord)
    assert replayed[1].previous_close == replayed[0].close


def test_daily_bars_reject_a_stale_local_file(tmp_path: Path) -> None:
    provider, _ = _provider(tmp_path, [_bar(20260723)])

    with pytest.raises(ProviderError, match="local sh market data is stale"):
        provider.fetch_daily_bars("sh.600000", date(2026, 7, 1), date(2026, 7, 28))


def test_daily_bars_reject_a_missing_local_file(tmp_path: Path) -> None:
    provider = PytdxProvider(FakeReader([]), vipdoc_path=tmp_path)

    with pytest.raises(ProviderError, match="file does not exist"):
        provider.fetch_daily_bars("sz.000001", date(2026, 7, 1), date(2026, 7, 28))


def test_pytdx_rejects_unsupported_datasets(tmp_path: Path) -> None:
    provider, _ = _provider(tmp_path, [])

    with pytest.raises(ProviderError, match="security dataset"):
        provider.fetch_securities()
    with pytest.raises(ProviderError, match="trading calendar"):
        provider.fetch_trading_calendar(date(2026, 7, 1), date(2026, 7, 28))


def test_local_industry_catalog_and_members_are_provider_neutral(tmp_path: Path) -> None:
    provider = _classification_provider(tmp_path)

    catalog = provider.fetch_classification_catalog("industry", date(2026, 7, 29))
    members = provider.fetch_classification_members("industry", "T1001", date(2026, 7, 29))

    catalog_record = catalog.records[0]
    assert isinstance(catalog_record, ClassificationCatalogSnapshotRecord)
    assert isinstance(members.records[0], ClassificationMemberSnapshotRecord)
    assert catalog_record.namespace == "tdx"
    assert catalog_record.classification_type is ClassificationType.INDUSTRY
    assert [(item.code, item.name) for item in catalog_record.definitions] == [
        ("T1001", "银行"),
        ("X500102", "股份制银行"),
    ]
    assert members.records[0].members == (
        "SZSE:000001",
        "SSE:600000",
        "BSE:920000",
    )
    assert catalog.schema_version == "pytdx.local_classification_catalog.v1"
    assert members.schema_version == "pytdx.local_classification_members.v1"


def test_local_concept_catalog_members_and_raw_replay(tmp_path: Path) -> None:
    provider = _classification_provider(tmp_path)

    catalog = provider.fetch_classification_catalog("concept", date(2026, 7, 29))
    members = provider.fetch_classification_members("concept", "880001", date(2026, 7, 29))
    replayed = normalize_pytdx_raw(
        DatasetCode.CLASSIFICATION_MEMBERS,
        members.schema_version,
        members.raw_rows,
        members.request_params,
    )

    assert isinstance(catalog.records[0], ClassificationCatalogSnapshotRecord)
    assert isinstance(members.records[0], ClassificationMemberSnapshotRecord)
    assert [(item.code, item.name) for item in catalog.records[0].definitions] == [
        ("880001", "测试概念")
    ]
    assert members.records[0].members == (
        "SZSE:000001",
        "SSE:600000",
        "BSE:920000",
    )
    assert replayed == tuple(members.records)


def _classification_provider(tmp_path: Path) -> PytdxProvider:
    vipdoc_path = tmp_path / "vipdoc"
    vipdoc_path.mkdir()
    hq_cache = tmp_path / "T0002" / "hq_cache"
    hq_cache.mkdir(parents=True)
    (hq_cache / "tdxzs.cfg").write_text("银行|880471|2|1|1|T1001\n", encoding="gb18030")
    (hq_cache / "tdxzs3.cfg").write_text("股份制银行|881388|12|1|1|X500102\n", encoding="gb18030")
    (hq_cache / "tdxhy.cfg").write_text(
        "0|000001|T1001|||X500102\n1|600000|T1001|||X500102\n2|920000|T1001|||X500102\n",
        encoding="gb18030",
    )
    (hq_cache / "infoharbor_block.dat").write_text(
        "#GN_测试概念,3,880001,20200101,20260729,,\n0#000001,1#600000,2#920000\n",
        encoding="gb18030",
    )
    return PytdxProvider(FakeReader([]), vipdoc_path=vipdoc_path)
