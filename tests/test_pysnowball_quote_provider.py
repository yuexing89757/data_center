from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from gzip import compress
from json import dumps
from typing import ClassVar

from pydantic import SecretStr

from market_data_center.providers.pysnowball_quote import (
    PysnowballQuoteProvider,
    _NetworkPankouClient,
)
from market_data_center.settings import PysnowballSettings

TOKEN = "xq_a_token=server-secret;u=123456"
OBSERVED_AT = datetime(2026, 8, 18, 1, 15, tzinfo=UTC)


def _payload(source_symbol: str) -> bytes:
    values: dict[str, object] = {
        "symbol": source_symbol,
        "time": "Aug 18, 2026 9:15:00 AM",
        "timestamp": 1_776_647_700_000,
        "current": 10.005,
        "buypct": 40.1,
        "sellpct": 59.9,
        "diff": -100,
        "ratio": -1.23,
    }
    for level, price, volume in (
        (1, "10.00", 100),
        (2, "9.99", 200),
        (3, "9.98", 300),
        (4, "9.97", 400),
        (5, "9.96", 500),
        (6, "9.95", 600),
        (7, "9.94", 700),
        (8, "9.93", 800),
        (9, "9.92", 900),
        (10, "9.91", 1000),
    ):
        values[f"bp{level}"] = Decimal(price)
        values[f"bc{level}"] = volume
    for level, price, volume in (
        (1, "10.01", 110),
        (2, "10.02", 210),
        (3, "10.03", 310),
        (4, "10.04", 410),
        (5, "10.05", 510),
        (6, "10.06", 610),
        (7, "10.07", 710),
        (8, "10.08", 810),
        (9, "10.09", 910),
        (10, "10.10", 1010),
    ):
        values[f"sp{level}"] = Decimal(price)
        values[f"sc{level}"] = volume
    return dumps(values, default=str, separators=(",", ":")).encode()


class FakePankouClient:
    def __init__(self, responses: Mapping[str, bytes | Exception]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, str, float]] = []

    def fetch(self, source_symbol: str, token: str, timeout_seconds: float) -> bytes:
        self.calls.append((source_symbol, token, timeout_seconds))
        response = self.responses[source_symbol]
        if isinstance(response, Exception):
            raise response
        return response


def test_network_client_requests_and_decodes_gzip_pankou_payload(monkeypatch) -> None:
    body = b'{"symbol":"SZ002027"}'
    captured: dict[str, object] = {}

    class FakeResponse:
        headers: ClassVar[dict[str, str]] = {"Content-Encoding": "gzip"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_value, traceback):
            return None

        def read(self) -> bytes:
            return compress(body)

    def fake_urlopen(request, *, timeout):
        captured["accept_encoding"] = request.get_header("Accept-encoding")
        captured["timeout"] = timeout
        return FakeResponse()

    monkeypatch.setattr(
        "market_data_center.providers.pysnowball_quote.urlopen",
        fake_urlopen,
    )

    payload = _NetworkPankouClient().fetch("SZ002027", TOKEN, 2.0)

    assert payload == body
    assert captured == {"accept_encoding": "gzip, deflate", "timeout": 2.0}


def _provider(client: FakePankouClient, *, clock=lambda: OBSERVED_AT) -> PysnowballQuoteProvider:
    return PysnowballQuoteProvider(
        PysnowballSettings(pysnowball_token=SecretStr(TOKEN), _env_file=None),
        client=client,
        clock=clock,
    )


def test_maps_sse_and_szse_pankou_rows_to_exact_five_level_decimal_records() -> None:
    client = FakePankouClient({"SH600000": _payload("SH600000"), "SZ000001": _payload("SZ000001")})

    result = _provider(client).fetch_five_level_quotes(("SSE:600000", "SZSE:000001"))

    assert [call[0] for call in client.calls] == ["SH600000", "SZ000001"]
    assert result.requested_symbols == ("SSE:600000", "SZSE:000001")
    assert result.failed_symbols == ()
    assert result.schema_version == "pysnowball.pankou.v1"
    first = result.records[0]
    assert first.symbol == "SSE:600000"
    assert first.last_price == Decimal("10.005")
    assert first.previous_close is None
    assert first.cumulative_volume is None
    assert first.source_timestamp == datetime(2026, 4, 20, 1, 15, tzinfo=UTC)
    assert first.source_code == "pysnowball"
    assert [(level.price, level.volume) for level in first.bid_levels] == [
        (Decimal("10.00"), 100),
        (Decimal("9.99"), 200),
        (Decimal("9.98"), 300),
        (Decimal("9.97"), 400),
        (Decimal("9.96"), 500),
    ]
    assert [(level.price, level.volume) for level in first.ask_levels] == [
        (Decimal("10.01"), 110),
        (Decimal("10.02"), 210),
        (Decimal("10.03"), 310),
        (Decimal("10.04"), 410),
        (Decimal("10.05"), 510),
    ]
    assert all(TOKEN not in str(row) for row in result.raw_rows)


def test_zero_price_level_is_missing_even_when_source_quantity_is_nonzero() -> None:
    payload = _payload("SZ000001")
    for level, price in ((2, "9.99"), (3, "9.98"), (4, "9.97"), (5, "9.96")):
        payload = payload.replace(f'"bp{level}":"{price}"'.encode(), f'"bp{level}":0'.encode())
    client = FakePankouClient({"SZ000001": payload})

    result = _provider(client).fetch_five_level_quotes(("SZSE:000001",))

    assert result.records[0].bid_levels[1].price is None
    assert result.records[0].bid_levels[1].volume is None


def test_one_symbol_failure_does_not_stop_later_single_symbol_requests() -> None:
    client = FakePankouClient(
        {
            "SH600000": _payload("SH600000"),
            "SZ000001": TimeoutError("secret response text must not escape"),
            "SH600001": _payload("SH600001"),
        }
    )

    result = _provider(client).fetch_five_level_quotes(("SSE:600000", "SZSE:000001", "SSE:600001"))

    assert [record.symbol for record in result.records] == ["SSE:600000", "SSE:600001"]
    assert result.failed_symbols == ("SZSE:000001",)
    assert [call[0] for call in client.calls] == ["SH600000", "SZ000001", "SH600001"]


def test_deadline_marks_unattempted_symbols_failed_without_network_calls() -> None:
    client = FakePankouClient({})

    result = _provider(client).fetch_five_level_quotes(
        ("SSE:600000", "SZSE:000001"), deadline=OBSERVED_AT - timedelta(microseconds=1)
    )

    assert result.records == ()
    assert result.failed_symbols == ("SSE:600000", "SZSE:000001")
    assert client.calls == []


def test_explicitly_empty_pankou_is_preserved_as_missing_not_zero() -> None:
    payload = _payload("SH600000").replace(b'"current":10.005', b'"current":null')
    for prefix, prices in (
        ("b", ("10.00", "9.99", "9.98", "9.97", "9.96")),
        ("s", ("10.01", "10.02", "10.03", "10.04", "10.05")),
    ):
        for level, old_price in enumerate(prices, start=1):
            payload = payload.replace(
                f'"{prefix}p{level}":"{old_price}"'.encode(),
                f'"{prefix}p{level}":0'.encode(),
            )
    client = FakePankouClient({"SH600000": payload})

    result = _provider(client).fetch_five_level_quotes(("SSE:600000",))

    assert result.failed_symbols == ()
    assert result.records[0].last_price is None
    assert all(level.price is None for level in result.records[0].bid_levels)
    assert all(level.price is None for level in result.records[0].ask_levels)
