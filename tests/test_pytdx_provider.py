from collections.abc import Iterable
from datetime import date
from decimal import Decimal
from pathlib import Path

import pytest

from market_data_center.domain import TradeStatus
from market_data_center.providers import ProviderError, PytdxProvider
from market_data_center.providers.pytdx import LocalDailyBarRow


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
    with pytest.raises(ProviderError, match="unsupported pytdx symbol"):
        provider.source_symbol("BSE:430047")


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
    assert record.previous_close is None
    assert record.trade_status is TradeStatus.UNKNOWN
    assert record.is_st is None
    assert reader.requested_files[0] == str(tmp_path / "sh" / "lday" / "sh600000.day")
    assert len(batch.raw_rows) == 3
    assert batch.schema_version == "pytdx.local_daily_bar.v1"


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
