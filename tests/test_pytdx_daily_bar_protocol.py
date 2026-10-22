from datetime import datetime
from decimal import Decimal
from struct import pack
from typing import Any, cast
from zlib import compress

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx import _default_client_factory


def _price(value: int) -> bytes:
    remaining = abs(value)
    first = (remaining & 0x3F) | (0x40 if value < 0 else 0)
    remaining >>= 6
    result = bytearray([first | (0x80 if remaining else 0)])
    while remaining:
        part = remaining & 0x7F
        remaining >>= 7
        result.append(part | (0x80 if remaining else 0))
    return bytes(result)


def _bar(day: int = 20261022, *, volume: int = 0x44800000, amount: int = 0x49800000) -> bytes:
    # Integer prices / 1000; encoded quantities are exactly 1024 and 1048576.
    return (
        pack("<I", day)
        + b"".join(_price(value) for value in (10000, 200, 500, -100))
        + pack("<II", volume, amount)
    )


class ResponseSocket:
    def __init__(self, body: bytes, *, compressed: bool = False) -> None:
        payload = compress(body) if compressed else body
        self.response = pack("<IIIHH", 0, 0, 0, len(payload), len(body)) + payload
        self.sent: list[bytes] = []
        self.send_pkg_num = self.send_pkg_bytes = 0
        self.recv_pkg_num = self.recv_pkg_bytes = 0
        self.first_pkg_send_time: datetime | None = None

    def send(self, data: bytes) -> int:
        self.sent.append(data)
        return len(data)

    def recv(self, count: int) -> bytes:
        data, self.response = self.response[:count], self.response[count:]
        return data


def _fetch(body: bytes, *, compressed: bool = False) -> list[dict[str, object]]:
    client = cast(Any, _default_client_factory())
    client.client = ResponseSocket(body, compressed=compressed)
    return cast(list[dict[str, object]], client.get_security_bars(9, 1, "600000", 0, 800))


@pytest.mark.parametrize("compressed", [False, True])
def test_network_daily_bar_decodes_prices_and_quantities_without_float(compressed: bool) -> None:
    rows = _fetch(pack("<H", 1) + _bar(), compressed=compressed)

    assert rows[0]["open"] == Decimal("10.00")
    assert rows[0]["close"] == Decimal("10.20")
    assert rows[0]["high"] == Decimal("10.50")
    assert rows[0]["low"] == Decimal("9.90")
    assert rows[0]["vol"] == Decimal(1024)
    assert rows[0]["amount"] == Decimal(1048576)
    assert all(isinstance(rows[0][key], Decimal) for key in ("open", "close", "vol", "amount"))


@pytest.mark.parametrize(
    "body", [b"", b"\x00", b"\x20\x03", pack("<H", 2) + _bar(), pack("<H", 1) + _bar() + b"\x00"]
)
def test_network_daily_bar_rejects_empty_shell_and_partial_page(body: bytes) -> None:
    with pytest.raises(ProviderError, match="Daily Bar response"):
        _fetch(body)


def test_network_daily_bar_decodes_zero_quantity_as_zero() -> None:
    rows = _fetch(pack("<H", 1) + _bar(volume=0, amount=0))
    assert rows[0]["vol"] == Decimal(0)
    assert rows[0]["amount"] == Decimal(0)


def test_network_daily_bar_carries_price_deltas_across_records() -> None:
    second = (
        pack("<I", 20261023)
        + b"".join(_price(value) for value in (100, -50, 100, -200))
        + pack("<II", 0, 0)
    )
    rows = _fetch(pack("<H", 2) + _bar() + second)
    assert rows[1]["datetime"] == "2026-10-23"
    assert rows[1]["open"] == Decimal("10.30")
    assert rows[1]["close"] == Decimal("10.25")
    assert rows[1]["high"] == Decimal("10.40")
    assert rows[1]["low"] == Decimal("10.10")


def test_network_daily_bar_accepts_an_explicit_empty_page() -> None:
    assert _fetch(pack("<H", 0)) == []


def test_network_daily_bar_rejects_invalid_calendar_date() -> None:
    with pytest.raises(ProviderError, match="Daily Bar response"):
        _fetch(pack("<H", 1) + _bar(day=20260230))
