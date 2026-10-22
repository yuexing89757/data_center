"""Exact numeric decoding for the existing standard TDX wire protocol."""

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
from struct import error as StructError
from struct import unpack_from
from time import time
from typing import cast

from pytdx.helper import get_price  # type: ignore[import-untyped]
from pytdx.hq import TdxHq_API  # type: ignore[import-untyped]
from pytdx.parser.get_security_bars import GetSecurityBarsCmd  # type: ignore[import-untyped]

from market_data_center.providers.contracts import ProviderError


def decode_volume_decimal(encoded: int) -> Decimal:
    logpoint = encoded >> 24
    high = (encoded >> 16) & 0xFF
    middle = (encoded >> 8) & 0xFF
    low = encoded & 0xFF

    def power(exponent: int) -> Decimal:
        return Decimal(2) ** exponent

    first = power(logpoint * 2 - 0x7F)
    exponent = logpoint * 2 - 0x86
    if high > 0x80:
        second = power(exponent) * Decimal(128) + Decimal(high & 0x7F) * power(exponent + 1)
    else:
        second = power(exponent) * Decimal(high)
    multiplier = Decimal(2) if high & 0x80 else Decimal(1)
    third = power(logpoint * 2 - 0x8E) * Decimal(middle) * multiplier
    fourth = power(logpoint * 2 - 0x96) * Decimal(low) * multiplier
    return first + second + third + fourth


class _DecimalDailyBarsCmd(GetSecurityBarsCmd):  # type: ignore[misc]
    requested_count: int = 800

    def parseResponse(self, body_buf: bytes | bytearray) -> list[Mapping[str, object]]:
        try:
            (count,) = unpack_from("<H", body_buf)
            if count > self.requested_count or len(body_buf) < 2 + count * 16:
                raise ValueError("incomplete page")
            pos = 2
            previous = 0
            rows: list[Mapping[str, object]] = []
            for _ in range(count):
                (ymd,) = unpack_from("<I", body_buf, pos)
                day = date(ymd // 10000, ymd // 100 % 100, ymd % 100)
                pos += 4
                open_diff, pos = get_price(body_buf, pos)
                close_diff, pos = get_price(body_buf, pos)
                high_diff, pos = get_price(body_buf, pos)
                low_diff, pos = get_price(body_buf, pos)
                volume, amount = unpack_from("<II", body_buf, pos)
                pos += 8
                opening = previous + open_diff
                previous = opening + close_diff
                rows.append(
                    {
                        "datetime": day.isoformat(),
                        "open": Decimal(opening) / 1000,
                        "close": Decimal(previous) / 1000,
                        "high": Decimal(opening + high_diff) / 1000,
                        "low": Decimal(opening + low_diff) / 1000,
                        "vol": decode_volume_decimal(volume) if volume else Decimal(0),
                        "amount": decode_volume_decimal(amount) if amount else Decimal(0),
                    }
                )
            if pos != len(body_buf):
                raise ValueError("unexpected trailing bytes")
            return rows
        except (IndexError, ValueError, StructError) as error:
            raise ProviderError("pytdx malformed Daily Bar response") from error


class DecimalDailyBarClient(TdxHq_API):  # type: ignore[misc]
    def get_security_bars(
        self, category: int, market: int, code: str, start: int, count: int
    ) -> list[Mapping[str, object]]:
        if category != 9 or not 1 <= count <= 800 or not 0 <= start <= 65_535:
            raise ProviderError("pytdx Daily Bar request exceeds protocol bounds")
        if self.client is None:
            raise ProviderError("pytdx Daily Bar client is not connected")
        self.last_ack_time = time()
        command = _DecimalDailyBarsCmd(self.client, lock=self.lock)
        command.requested_count = count
        command.setParams(category, market, code, start, count)
        try:
            return cast(list[Mapping[str, object]], command.call_api())
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError("pytdx Daily Bar response unavailable") from error
