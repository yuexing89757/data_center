from dataclasses import replace
from datetime import datetime, timedelta
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest
from test_tencent_quote_provider import ROW

from market_data_center.providers.regulation_quote import (
    MonitorQuote,
    benchmark_source_symbol,
    fetch_monitor_quotes,
    quote_issue,
)

NOW = datetime(2026, 9, 29, 10, 30, tzinfo=ZoneInfo("Asia/Shanghai"))


def quote(symbol="SSE:600000", observed=NOW):
    return MonitorQuote(symbol, observed, Decimal("10"), Decimal("10"), None)


@pytest.mark.parametrize(
    "symbol,expected",
    [("SSE:000002", "sh000002"), ("SZSE:399107", "sz399107"), ("SZSE:399102", "sz399102")],
)
def test_correct_benchmark_routing(symbol, expected):
    assert benchmark_source_symbol(symbol) == expected


def test_stock_code_cannot_impersonate_index():
    with pytest.raises(ValueError):
        benchmark_source_symbol("SZSE:000002")


@pytest.mark.parametrize("seconds,expected", [(59, None), (60, None), (61, "stale_quote")])
def test_quote_age_is_bounded(seconds, expected):
    timestamp = NOW - timedelta(seconds=seconds)
    assert (
        quote_issue(
            quote(observed=timestamp), quote("SSE:000002", timestamp), NOW.date(), NOW, True
        )
        == expected
    )


@pytest.mark.parametrize(
    "timestamp,expected",
    [
        (NOW + timedelta(seconds=1), "future_quote"),
        (NOW - timedelta(days=1), "quote_date_mismatch"),
    ],
)
def test_future_and_cross_date_quotes_are_rejected(timestamp, expected):
    assert (
        quote_issue(quote(observed=timestamp), quote("SSE:000002"), NOW.date(), NOW, True)
        == expected
    )


def test_lunch_preserves_only_end_of_morning_quotes_and_enforces_skew():
    lunch = NOW.replace(hour=12)
    close = NOW.replace(hour=11, minute=30)
    assert (
        quote_issue(quote(observed=close), quote("SSE:000002", close), NOW.date(), lunch, False)
        is None
    )
    assert (
        quote_issue(quote(), quote("SSE:000002"), NOW.date(), lunch, False) == "stale_session_quote"
    )
    assert (
        quote_issue(
            quote(observed=close),
            quote("SSE:000002", close - timedelta(seconds=61)),
            NOW.date(),
            lunch,
            False,
        )
        == "unsynchronised_quotes"
    )


def test_adapter_uses_separate_index_parser_and_preserves_stock_units():
    index = [""] * 31
    index[2], index[3], index[4], index[30] = "000002", "3100.00", "3000.00", "20260929103000"
    payload = (
        f'v_sh601003="{ROW.replace("20260821161441", "20260929103000")}";'
        f'v_sh000002="{"~".join(index)}";'
    ).encode("gbk")

    def request(url, timeout):
        assert url.endswith("sh601003,sh000002")
        assert 0 < timeout <= 3
        return payload

    result = fetch_monitor_quotes(
        ("SSE:601003",), ("SSE:000002",), 8, request_bytes=request, monotonic=lambda: 0
    )
    assert [q.symbol for q in result] == ["SSE:601003", "SSE:000002"]
    assert result[0].volume_shares == Decimal(9920300)
    assert result[1].price == Decimal("3100.00")
    assert result[1].volume_shares is None


def test_invalid_index_does_not_discard_valid_stock():
    payload = f'v_sh601003="{ROW}";v_sh000002="broken";'.encode("gbk")
    result = fetch_monitor_quotes(
        ("SSE:601003",), ("SSE:000002",), 8, request_bytes=lambda *_: payload, monotonic=lambda: 0
    )
    assert [q.symbol for q in result] == ["SSE:601003"]


def test_deadline_and_request_bounds_prevent_network_io():
    def forbidden(*_):
        raise AssertionError("must not request")

    assert (
        fetch_monitor_quotes(("SSE:601003",), (), 0, request_bytes=forbidden, monotonic=lambda: 1)
        == ()
    )
    with pytest.raises(ValueError):
        fetch_monitor_quotes(("SSE:601003",) * 51, (), 8, request_bytes=forbidden)


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-1", "0"])
def test_quote_rejects_nonfinite_or_nonpositive_prices(bad):
    with pytest.raises(ValueError):
        replace(quote(), price=Decimal(bad))
