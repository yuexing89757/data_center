from datetime import UTC, datetime
from decimal import Decimal
from urllib.error import URLError

import pytest

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.tencent_quote import (
    TencentQuoteProvider,
    normalize_tencent_quote_raw,
)
from market_data_center.settings import TencentQuoteSettings

ROW = (
    "1~柳钢股份~601003~3.58~3.60~3.58~99203~46429~52774~"
    "3.58~571~3.57~3753~3.56~1399~3.55~3291~3.54~1312~"
    "3.59~1862~3.60~1679~3.61~1036~3.62~1200~3.63~1683~~"
    "20260821161441~-0.02~-0.56~3.60~3.54~3.58/99203/35356540~"
    "99203~3536~0.39~21.72~~3.60~3.54~1.67~91.75~94.31~1.03~3.96~3.24"
)


def test_tencent_quote_maps_gbk_batch_without_float_or_display_amount() -> None:
    calls: list[str] = []

    def request(url: str, timeout: float) -> bytes:
        calls.append(url)
        assert timeout == 3.0
        return f'v_sh601003="{ROW}";\n'.encode("gbk")

    observed = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    provider = TencentQuoteProvider(request_bytes=request, clock=lambda: observed)

    fetched = provider.fetch_five_level_quotes(("SSE:601003",))

    assert calls == ["https://qt.gtimg.cn/q=sh601003"]
    assert fetched.failed_symbols == ()
    assert fetched.schema_version == "tencent_quote.qt_gtimg.v1"
    assert fetched.raw_rows[0]["payload"] == ROW
    quote = fetched.records[0]
    assert quote.last_price == Decimal("3.58")
    assert quote.cumulative_volume == 9_920_300
    assert quote.cumulative_amount == Decimal("35356540")
    assert quote.bid_levels[1].price == Decimal("3.57")
    assert quote.bid_levels[1].volume == 375_300
    assert quote.ask_levels[4].price == Decimal("3.63")
    assert quote.ask_levels[4].volume == 168_300
    assert quote.source_timestamp is not None
    assert quote.source_timestamp.isoformat() == "2026-08-21T16:14:41+08:00"

    replayed = normalize_tencent_quote_raw(
        DatasetCode.FIVE_LEVEL_QUOTE,
        fetched.schema_version,
        fetched.raw_rows,
        {"symbols": ["SSE:601003"]},
    )
    assert replayed == fetched.records


def test_tencent_quote_preserves_raw_and_reports_schema_drift() -> None:
    provider = TencentQuoteProvider(
        request_bytes=lambda _url, _timeout: b'v_sh601003="1~bad";',
        clock=lambda: datetime(2026, 8, 21, 8, 15, tzinfo=UTC),
    )

    fetched = provider.fetch_five_level_quotes(("SSE:601003",))

    assert len(fetched.raw_rows) == 1
    assert fetched.records == ()
    assert fetched.failed_symbols == ("SSE:601003",)
    assert fetched.normalization_errors[0].reason == "schema_drift"


def test_tencent_quote_preserves_missing_values_for_no_quote_row() -> None:
    fields = ROW.split("~")
    for index in (3, 5, 6, 33, 34):
        fields[index] = ""
    for index in range(9, 29):
        fields[index] = ""
    fields[35] = "0/0/0"
    payload = "~".join(fields)
    provider = TencentQuoteProvider(
        request_bytes=lambda _url, _timeout: f'v_sh601003="{payload}";'.encode("gbk"),
        clock=lambda: datetime(2026, 8, 21, 8, 15, tzinfo=UTC),
    )

    quote = provider.fetch_five_level_quotes(("SSE:601003",)).records[0]

    assert quote.last_price is None
    assert quote.cumulative_volume is None
    assert quote.cumulative_amount == Decimal(0)
    assert all(level.price is None and level.volume is None for level in quote.bid_levels)
    assert all(level.price is None and level.volume is None for level in quote.ask_levels)


def test_tencent_quote_rejects_duplicate_or_unsupported_symbols() -> None:
    settings = TencentQuoteSettings(tencent_quote_batch_size=1)
    provider = TencentQuoteProvider(settings, request_bytes=lambda _url, _timeout: b"")

    with pytest.raises(ProviderError, match="unique"):
        provider.fetch_five_level_quotes(("SSE:601003", "SSE:601003"))
    with pytest.raises(ProviderError, match="SSE/SZSE"):
        provider.fetch_five_level_quotes(("BSE:920000",))


def test_tencent_quote_keeps_successful_batches_when_one_request_fails() -> None:
    calls = 0

    def request(_url: str, _timeout: float) -> bytes:
        nonlocal calls
        calls += 1
        if calls == 2:
            raise URLError("unavailable")
        return f'v_sh601003="{ROW}";'.encode("gbk")

    provider = TencentQuoteProvider(
        TencentQuoteSettings(tencent_quote_batch_size=1),
        request_bytes=request,
        clock=lambda: datetime(2026, 8, 21, 8, 15, tzinfo=UTC),
    )

    fetched = provider.fetch_five_level_quotes(("SSE:601003", "SSE:600123"))

    assert tuple(record.symbol for record in fetched.records) == ("SSE:601003",)
    assert fetched.failed_symbols == ("SSE:600123",)
