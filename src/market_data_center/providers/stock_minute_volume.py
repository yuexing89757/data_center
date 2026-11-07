"""Bounded native minute quantity reads; unknown source identities fail closed."""

import hashlib
import re
from collections.abc import Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime
from decimal import localcontext
from pathlib import Path
from struct import error as StructError
from struct import unpack_from
from time import monotonic
from typing import Any, Protocol, cast
from zoneinfo import ZoneInfo

from pytdx.helper import get_price  # type: ignore[import-untyped]
from pytdx.hq import TdxHq_API  # type: ignore[import-untyped]
from pytdx.parser.get_security_bars import GetSecurityBarsCmd  # type: ignore[import-untyped]

from market_data_center.domain.stock_max_minute_volume import MinuteVolumeBar, MinuteVolumeSource
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.pytdx_pool import PytdxCapability, load_endpoint_pool
from market_data_center.providers.pytdx_protocol import decode_volume_decimal
from market_data_center.providers.quantity_units import (
    normalize_stock_quantity,
    stock_instrument_type,
)

SOURCE_SCHEMA_VERSION = "pytdx.stock_minute_volume.v1"
UNIT_EVIDENCE_VERSION = "pytdx-minute-volume.2026-11-08.v1"
PAGE_SIZE = 800
MAX_PAGES = 6
READ_TIMEOUT = 3.0
TOTAL_TIMEOUT = 30.0
IDENTITY_FIELDS = (
    "provider",
    "endpoint",
    "schema_version",
    "instrument_type",
    "field",
    "peer",
    "category",
    "market",
    "exchange",
    "unit_evidence_version",
    "source_quantity_unit",
    "standard_quantity_unit",
    "symbol",
    "trade_date",
)
# Independent two-stock/two-date evidence per market; never infer another peer or BSE.
VERIFIED_SOURCE_SCOPES = frozenset(
    {("103.251.85.94:7709", "SSE", 1), ("103.251.85.94:7709", "SZSE", 0)}
)


def validate_minute_volume_identity(metadata: Mapping[str, object]) -> None:
    """Mandatory both before fresh normalization and before Raw certification/replay."""
    expected = {
        "provider": "pytdx",
        "endpoint": "get_security_bars",
        "schema_version": SOURCE_SCHEMA_VERSION,
        "instrument_type": "stock",
        "field": "volume",
        "category": 8,
        "unit_evidence_version": UNIT_EVIDENCE_VERSION,
        "source_quantity_unit": "share",
        "standard_quantity_unit": "share",
    }
    symbol = metadata.get("symbol")
    scope = (metadata.get("peer"), metadata.get("exchange"), metadata.get("market"))
    valid_scope = all(isinstance(value, (str, int)) for value in scope)
    trade_date = metadata.get("trade_date")
    try:
        valid_date = isinstance(trade_date, str) and date.fromisoformat(trade_date).isoformat() == (
            trade_date
        )
    except ValueError:
        valid_date = False
    if (
        any(metadata.get(key) != value for key, value in expected.items())
        or type(metadata.get("category")) is not int
        or type(metadata.get("market")) is not int
        or not valid_scope
        or scope not in VERIFIED_SOURCE_SCOPES
        or not isinstance(symbol, str)
        or stock_instrument_type(symbol) != "stock"
        or symbol.partition(":")[0] != metadata.get("exchange")
        or not valid_date
    ):
        raise ProviderError("quantity unit contract unverified for minute source identity")


def _quantity(bits: int) -> int:
    if not 0 <= bits <= 0xFFFFFFFF or bits >> 31 or (bits >> 23) & 255 == 255:
        raise ProviderError("source_response_invalid: invalid minute wire quantity")
    with localcontext() as context:
        context.prec = 160
        quantity = normalize_stock_quantity(
            decode_volume_decimal(bits) if bits else 0,
            source_unit="share",
            context="pytdx minute volume",
        )
    assert quantity is not None
    return quantity


def _timestamp(zipday: int, minutes: int) -> datetime:
    return datetime(
        (zipday >> 11) + 2004,
        (zipday % 2048) // 100,
        zipday % 2048 % 100,
        minutes // 60,
        minutes % 60,
    )


@dataclass(frozen=True, slots=True)
class _WireMinute:
    timestamp: datetime
    volume_wire_uint32: int
    timestamp_wire_day: int
    timestamp_wire_minute: int


class ExactMinuteVolumeCommand(GetSecurityBarsCmd):  # type: ignore[misc]
    """Skip integer price deltas; never decode or retain OHLC/amount values."""

    def parseResponse(self, body_buf: bytes | bytearray) -> list[_WireMinute]:
        self.body_size = len(body_buf)
        self.digest = hashlib.sha256(body_buf).hexdigest()
        try:
            (count,) = unpack_from("<H", body_buf)
            if count > PAGE_SIZE or len(body_buf) < 2 + count * 16:
                raise ValueError("incomplete page")
            rows = []
            pos = 2
            for _ in range(count):
                zipday, minutes = unpack_from("<HH", body_buf, pos)
                timestamp = _timestamp(zipday, minutes)
                pos += 4
                for _ in range(4):
                    _, pos = get_price(body_buf, pos)
                (bits,) = unpack_from("<I", body_buf, pos)
                pos += 8
                _quantity(bits)
                rows.append(_WireMinute(timestamp, bits, zipday, minutes))
            if pos != len(body_buf):
                raise ValueError("trailing bytes")
            stamps = [row.timestamp for row in rows]
            if stamps != sorted(set(stamps)):
                raise ValueError("duplicate or unordered page")
            return rows
        except (IndexError, StructError, ValueError) as error:
            raise ProviderError("source_response_invalid: malformed minute response") from error


class _MinuteClient(Protocol):
    def connect(self, host: str, port: int, *, time_out: float, deadline: float) -> bool: ...
    def disconnect(self) -> None: ...
    def read_page(
        self, market: int, code: str, start: int, timeout: float
    ) -> tuple[list[_WireMinute], dict[str, object]]: ...


class _ReadDeadlineSocket:
    """pytdx loops over recv; one timeout alone does not bound fragmented responses."""

    def __init__(self, socket: Any, deadline: float) -> None:
        object.__setattr__(self, "_socket", socket)
        object.__setattr__(self, "_deadline", deadline)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._socket, name)

    def __setattr__(self, name: str, value: object) -> None:
        setattr(self._socket, name, value)

    def _set_timeout(self) -> None:
        remaining = self._deadline - monotonic()
        if remaining <= 0:
            raise ProviderError("collection_budget_exhausted")
        self._socket.settimeout(min(READ_TIMEOUT, remaining))

    def recv(self, count: int) -> bytes:
        self._set_timeout()
        return cast(bytes, self._socket.recv(count))

    def send(self, body: bytes) -> int:
        self._set_timeout()
        return cast(int, self._socket.send(body))


class _NetworkMinuteClient(TdxHq_API):  # type: ignore[misc]
    client: Any

    def connect(self, host: str, port: int, *, time_out: float, deadline: float) -> bool:
        # The base connect invokes three setup commands before returning its socket.
        self.need_setup = False
        try:
            connected = super().connect(host, port, time_out=time_out)
        finally:
            self.need_setup = True
        if not connected:
            return False
        socket: Any = self.client
        self.client = _ReadDeadlineSocket(socket, deadline)
        try:
            self.setup()
        finally:
            self.client = socket
        return True

    def read_page(
        self, market: int, code: str, start: int, timeout: float
    ) -> tuple[list[_WireMinute], dict[str, object]]:
        bounded = _ReadDeadlineSocket(self.client, monotonic() + timeout)
        command = ExactMinuteVolumeCommand(bounded, lock=self.lock)
        command.setParams(8, market, code, start, PAGE_SIZE)
        rows = command.call_api()
        if not isinstance(rows, list):
            raise ProviderError("minute_capability_unavailable")
        return cast(list[_WireMinute], rows), {
            "start": start,
            "requested_count": PAGE_SIZE,
            "returned": len(rows),
            "wire_bytes": command.body_size,
            "wire_sha256": command.digest,
            "first": rows[0].timestamp.isoformat(" ") if rows else None,
            "last": rows[-1].timestamp.isoformat(" ") if rows else None,
        }


def _default_client_factory() -> _MinuteClient:
    return cast(
        _MinuteClient,
        _NetworkMinuteClient(
            heartbeat=False,
            auto_retry=False,
            raise_exception=True,
        ),
    )


def replay(
    raw_rows: Sequence[Mapping[str, str]], metadata: Mapping[str, object]
) -> MinuteVolumeSource:
    """Replay exact wire quantities with complete source identity checked again."""
    validate_minute_volume_identity(metadata)
    try:
        symbol = str(metadata["symbol"])
        trade_date = date.fromisoformat(str(metadata["trade_date"]))
        bars = []
        for row in raw_rows:
            if any(row.get(key) != str(metadata[key]) for key in IDENTITY_FIELDS):
                raise ProviderError("quantity unit contract conflicts with minute Raw identity")
            stamp = datetime.fromisoformat(row["timestamp"])
            zipday = int(row["timestamp_wire_day"])
            minutes = int(row["timestamp_wire_minute"])
            bits_text = row["volume_wire_uint32"]
            if not re.fullmatch(r"[0-9]{1,10}", bits_text):
                raise ValueError("invalid bits")
            if stamp != _timestamp(zipday, minutes) or stamp.date() != trade_date:
                raise ValueError("wrong Raw time")
            bars.append(MinuteVolumeBar(stamp, _quantity(int(bits_text))))
        return MinuteVolumeSource(
            symbol, trade_date, tuple(bars), tuple(raw_rows), dict(metadata), ()
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ProviderError("source_response_invalid: malformed minute Raw") from error


class PytdxMinuteVolumeProvider:
    source_code = "pytdx"

    def __init__(self, pool_path: Path) -> None:
        self._pool_path = pool_path
        self._peers: dict[str, tuple[str, int]] = {}

    def fetch(self, symbol: str, trade_date: date) -> MinuteVolumeSource:
        if stock_instrument_type(symbol) != "stock":
            raise ProviderError("quantity unit contract unverified for instrument")
        if trade_date > datetime.now(ZoneInfo("Asia/Shanghai")).date():
            raise ProviderError("source_response_invalid: future trade date")
        exchange, _, code = symbol.partition(":")
        market = 1 if exchange == "SSE" else 0
        if exchange not in self._peers:
            pool = load_endpoint_pool(self._pool_path)
            capability = {
                "SSE": PytdxCapability.DAILY_BAR_SSE,
                "SZSE": PytdxCapability.DAILY_BAR_SZSE,
                "BSE": PytdxCapability.DAILY_BAR_BSE,
            }[exchange]
            candidates = [
                (node.host, node.port)
                for node in pool.nodes
                if node.capabilities[capability]
                and (f"{node.host}:{node.port}", exchange, market) in VERIFIED_SOURCE_SCOPES
            ]
            if not candidates:
                raise ProviderError("quantity unit contract unverified for minute pool market")
            self._peers[exchange] = candidates[0]
        host, port = self._peers[exchange]
        metadata: dict[str, object] = {
            "provider": "pytdx",
            "endpoint": "get_security_bars",
            "schema_version": SOURCE_SCHEMA_VERSION,
            "instrument_type": "stock",
            "field": "volume",
            "category": 8,
            "market": market,
            "exchange": exchange,
            "peer": f"{host}:{port}",
            "unit_evidence_version": UNIT_EVIDENCE_VERSION,
            "source_quantity_unit": "share",
            "standard_quantity_unit": "share",
            "symbol": symbol,
            "trade_date": trade_date.isoformat(),
            "source_reported": True,
            "first_last_native_bars_included": True,
        }
        validate_minute_volume_identity(metadata)
        deadline = monotonic() + TOTAL_TIMEOUT
        client = _default_client_factory()
        pages: list[dict[str, object]] = []
        raw_rows: list[Mapping[str, str]] = []
        bounded = False
        try:
            if not client.connect(host, port, time_out=READ_TIMEOUT, deadline=deadline):
                raise ProviderError("minute_capability_unavailable")
            previous_oldest: datetime | None = None
            for page in range(MAX_PAGES):
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise ProviderError("collection_budget_exhausted")
                start = page * PAGE_SIZE
                rows, evidence = client.read_page(market, code, start, min(READ_TIMEOUT, remaining))
                if monotonic() > deadline:
                    raise ProviderError("collection_budget_exhausted")
                if len(rows) > PAGE_SIZE or (
                    rows and previous_oldest is not None and rows[-1].timestamp >= previous_oldest
                ):
                    raise ProviderError("source_response_invalid: overlapping minute pages")
                pages.append(evidence)
                for row in rows:
                    if row.timestamp.date() == trade_date:
                        raw_rows.append(
                            {
                                **{key: str(metadata[key]) for key in IDENTITY_FIELDS},
                                "timestamp": row.timestamp.isoformat(" "),
                                "timestamp_wire_day": str(row.timestamp_wire_day),
                                "timestamp_wire_minute": str(row.timestamp_wire_minute),
                                "volume_wire_uint32": str(row.volume_wire_uint32),
                                "page_start": str(start),
                            }
                        )
                if not rows or rows[0].timestamp.date() < trade_date or len(rows) < PAGE_SIZE:
                    bounded = True
                    break
                previous_oldest = rows[0].timestamp
        except ProviderError:
            raise
        except Exception as error:
            raise ProviderError("minute_capability_unavailable") from error
        finally:
            with suppress(Exception):
                client.disconnect()
        metadata.update(pages=pages, history_boundary_reached=bounded)
        result = replay(raw_rows, metadata)
        if not bounded:
            return MinuteVolumeSource(
                result.symbol,
                result.trade_date,
                result.bars,
                result.raw_rows,
                result.metadata,
                ("collection_budget_exhausted",),
            )
        return result
