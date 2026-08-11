"""Network pytdx five-level quote adapter with Decimal protocol decoding."""

from collections import OrderedDict
from collections.abc import Callable, Mapping, Sequence
from contextlib import AbstractContextManager
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from struct import unpack
from types import TracebackType
from typing import Protocol, Self, cast

from pytdx.helper import get_price  # type: ignore[import-untyped]
from pytdx.hq import TdxHq_API  # type: ignore[import-untyped]
from pytdx.parser.get_security_quotes import (  # type: ignore[import-untyped]
    GetSecurityQuotesCmd,
)

from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import (
    ProviderError,
    RealtimeQuoteFetch,
)
from market_data_center.providers.pytdx_pool import (
    PytdxCapability,
    endpoints_for,
    load_endpoint_pool,
)
from market_data_center.settings import PytdxHqSettings, PytdxPoolSettings


class QuoteClient(Protocol):
    def fetch(self, requests: Sequence[tuple[int, str]]) -> Sequence[Mapping[str, object]]: ...


class ManagedQuoteClient(QuoteClient, Protocol):
    def __enter__(self) -> Self: ...

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None: ...


class _DecimalSecurityQuotesCmd(GetSecurityQuotesCmd):  # type: ignore[misc]
    """Decode protocol integer prices/amount without pytdx's float conversions."""

    def parseResponse(self, body_buf: bytes | bytearray) -> list[Mapping[str, object]]:
        pos = 2
        (count,) = unpack("<H", body_buf[pos : pos + 2])
        pos += 2
        rows: list[Mapping[str, object]] = []
        for _ in range(count):
            market, code, _active1 = unpack("<B6sH", body_buf[pos : pos + 9])
            pos += 9
            price, pos = get_price(body_buf, pos)
            last_close_diff, pos = get_price(body_buf, pos)
            open_diff, pos = get_price(body_buf, pos)
            high_diff, pos = get_price(body_buf, pos)
            low_diff, pos = get_price(body_buf, pos)
            server_time_raw, pos = get_price(body_buf, pos)
            _ignored, pos = get_price(body_buf, pos)
            volume_lots, pos = get_price(body_buf, pos)
            current_volume_lots, pos = get_price(body_buf, pos)
            (amount_raw,) = unpack("<I", body_buf[pos : pos + 4])
            pos += 4
            sell_volume_lots, pos = get_price(body_buf, pos)
            buy_volume_lots, pos = get_price(body_buf, pos)
            _ignored, pos = get_price(body_buf, pos)
            _ignored, pos = get_price(body_buf, pos)
            values: OrderedDict[str, object] = OrderedDict(
                market=market,
                code=code.decode("ascii"),
                price=_price(price),
                last_close=_price(price + last_close_diff),
                open=_price(price + open_diff),
                high=_price(price + high_diff),
                low=_price(price + low_diff),
                server_time_raw=str(server_time_raw),
                volume_lots=volume_lots,
                current_volume_lots=current_volume_lots,
                amount=_decode_volume_decimal(amount_raw),
                sell_volume_lots=sell_volume_lots,
                buy_volume_lots=buy_volume_lots,
            )
            for level in range(1, 6):
                bid_delta, pos = get_price(body_buf, pos)
                ask_delta, pos = get_price(body_buf, pos)
                bid_volume, pos = get_price(body_buf, pos)
                ask_volume, pos = get_price(body_buf, pos)
                values[f"bid{level}"] = _price(price + bid_delta)
                values[f"ask{level}"] = _price(price + ask_delta)
                values[f"bid_vol{level}"] = bid_volume
                values[f"ask_vol{level}"] = ask_volume
            pos += 2
            for _ in range(4):
                _ignored, pos = get_price(body_buf, pos)
            pos += 4
            rows.append(values)
        return rows


class _NetworkQuoteClient(AbstractContextManager["_NetworkQuoteClient"]):
    def __init__(self, hosts: Sequence[tuple[str, int]], timeout_seconds: float) -> None:
        if not hosts:
            raise ProviderError("pytdx_hq has no candidate hosts")
        self._hosts = tuple(hosts)
        self._timeout = timeout_seconds
        self._api = TdxHq_API(heartbeat=True, raise_exception=True)
        self._connected_host: tuple[str, int] | None = None

    @property
    def connected_host(self) -> tuple[str, int] | None:
        return self._connected_host

    def __enter__(self) -> "_NetworkQuoteClient":
        errors: list[str] = []
        for host, port in self._hosts:
            try:
                connected = self._api.connect(host, port, time_out=self._timeout)
            except Exception as error:
                errors.append(f"{host}:{port} ({type(error).__name__})")
                continue
            if connected:
                self._connected_host = (host, port)
                return self
            errors.append(f"{host}:{port}")
        raise ProviderError("pytdx_hq connection failed; tried " + ", ".join(errors))

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._api.disconnect()
        self._connected_host = None

    def fetch(self, requests: Sequence[tuple[int, str]]) -> Sequence[Mapping[str, object]]:
        if self._api.client is None:
            raise ProviderError("pytdx_hq client is not connected")
        command = _DecimalSecurityQuotesCmd(self._api.client, lock=self._api.lock)
        command.setParams(requests)
        result = command.call_api()
        if not isinstance(result, list):
            raise ProviderError("pytdx_hq returned no quote list")
        return cast(list[Mapping[str, object]], result)


class PytdxHqProvider(AbstractContextManager["PytdxHqProvider"]):
    source_code = "pytdx_hq"

    def __init__(
        self,
        settings: PytdxHqSettings,
        *,
        endpoints: Sequence[tuple[str, int]] | None = None,
        pool_settings: PytdxPoolSettings | None = None,
        client_factory: (
            Callable[[Sequence[tuple[str, int]], float], ManagedQuoteClient] | None
        ) = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._settings = settings
        self._explicit_endpoints = _validate_endpoints(endpoints) if endpoints is not None else None
        self._pool_settings = pool_settings or PytdxPoolSettings()
        self._client_factory = client_factory or _NetworkQuoteClient
        self._clock = clock
        self._managed_client: ManagedQuoteClient | None = None

    def __enter__(self) -> "PytdxHqProvider":
        hosts = self._explicit_endpoints
        if hosts is None:
            pool = load_endpoint_pool(self._pool_settings.pytdx_pool_path)
            hosts = endpoints_for(pool, PytdxCapability.QUOTE)
            if not hosts:
                raise ProviderError("pytdx endpoint pool has no quote-capable node")
        self._managed_client = self._client_factory(
            hosts, self._settings.pytdx_hq_timeout_seconds
        ).__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if self._managed_client is not None:
            self._managed_client.__exit__(exc_type, exc_value, traceback)
            self._managed_client = None

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        if self._managed_client is None:
            raise ProviderError("pytdx_hq provider must be used as a managed context")
        _validate_deadline(deadline)
        requested = tuple(dict.fromkeys(symbols))
        if not requested or len(requested) != len(symbols):
            raise ProviderError("pytdx_hq symbols must be non-empty and unique")
        raw_rows: list[Mapping[str, str]] = []
        records: list[FiveLevelQuoteSnapshotRecord] = []
        failed: list[str] = []
        for start in range(0, len(requested), self._settings.pytdx_hq_batch_size):
            symbol_batch = requested[start : start + self._settings.pytdx_hq_batch_size]
            if deadline is not None and self._clock() >= deadline:
                failed.extend(requested[start:])
                break
            requests = tuple(_source_identity(symbol) for symbol in symbol_batch)
            try:
                response = self._managed_client.fetch(requests)
            except Exception:
                failed.extend(symbol_batch)
                continue
            collected_at = self._clock()
            by_symbol: dict[str, Mapping[str, object]] = {}
            for row in response:
                symbol = _standard_symbol(cast(int, row["market"]), str(row["code"]))
                by_symbol[symbol] = row
            for symbol in symbol_batch:
                matched_row = by_symbol.get(symbol)
                if matched_row is None:
                    failed.append(symbol)
                    continue
                raw_rows.append(_raw_row(matched_row))
                records.append(_record(symbol, matched_row, collected_at))
        return RealtimeQuoteFetch(
            tuple(raw_rows),
            tuple(records),
            requested,
            tuple(failed),
            "pytdx_hq.security_quotes.v1",
        )


def _validate_endpoints(endpoints: Sequence[tuple[str, int]]) -> tuple[tuple[str, int], ...]:
    if not endpoints:
        raise ProviderError("pytdx_hq explicit endpoints must be non-empty")
    validated: list[tuple[str, int]] = []
    for host, port in endpoints:
        if not isinstance(host, str) or not host or host != host.strip():
            raise ProviderError("pytdx_hq endpoint host is invalid")
        if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65_535:
            raise ProviderError("pytdx_hq endpoint port is invalid")
        endpoint = (host, port)
        if endpoint in validated:
            raise ProviderError("pytdx_hq explicit endpoints must be unique")
        validated.append(endpoint)
    return tuple(validated)


def _validate_deadline(deadline: datetime | None) -> None:
    if deadline is not None and (deadline.tzinfo is None or deadline.utcoffset() != timedelta()):
        raise ProviderError("pytdx_hq deadline must be an aware UTC datetime")


def _record(
    symbol: str, row: Mapping[str, object], collected_at: datetime
) -> FiveLevelQuoteSnapshotRecord:
    bids = tuple(_level(row, "bid", level) for level in range(1, 6))
    asks = tuple(_level(row, "ask", level) for level in range(1, 6))
    has_quote = any(level.price is not None for level in (*bids, *asks))
    return FiveLevelQuoteSnapshotRecord(
        symbol=symbol,
        market=Market.CN_A_SHARE,
        observed_at=collected_at,
        source_timestamp=None,
        quote_status=QuoteStatus.TRADING if has_quote else QuoteStatus.UNKNOWN,
        last_price=_optional_price(row["price"]),
        previous_close=_optional_price(row["last_close"]),
        open=_optional_price(row["open"]),
        high=_optional_price(row["high"]),
        low=_optional_price(row["low"]),
        cumulative_volume=_lots_to_shares(row["volume_lots"]),
        cumulative_amount=cast(Decimal, row["amount"]),
        bid_levels=bids,
        ask_levels=asks,
        source_code="pytdx_hq",
    )


def _level(row: Mapping[str, object], side: str, level: int) -> OrderBookLevel:
    price = _optional_price(row[f"{side}{level}"])
    volume = _lots_to_shares(row[f"{side}_vol{level}"]) if price is not None else None
    return OrderBookLevel(level, price, volume)


def _source_identity(symbol: str) -> tuple[int, str]:
    exchange, separator, code = symbol.partition(":")
    if separator != ":" or len(code) != 6 or not code.isdigit():
        raise ProviderError("invalid standard quote symbol")
    if exchange == "SSE":
        return 1, code
    if exchange == "SZSE":
        return 0, code
    raise ProviderError("pytdx_hq auction collection supports SSE/SZSE only")


def _standard_symbol(market: int, code: str) -> str:
    if market not in {0, 1} or len(code) != 6 or not code.isdigit():
        raise ProviderError("pytdx_hq returned an invalid stock identity")
    return f"{'SSE' if market == 1 else 'SZSE'}:{code}"


def _optional_price(value: object) -> Decimal | None:
    if not isinstance(value, Decimal):
        raise ProviderError("pytdx_hq Decimal decoder contract was bypassed")
    return value if value > 0 else None


def _lots_to_shares(value: object) -> int:
    if not isinstance(value, int) or value < 0:
        raise ProviderError("pytdx_hq quantity is not a nonnegative integer")
    return value * 100


def _price(cents: int) -> Decimal:
    return Decimal(cents) / Decimal(100)


def _decode_volume_decimal(encoded: int) -> Decimal:
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


def _raw_row(row: Mapping[str, object]) -> Mapping[str, str]:
    return {key: str(value) for key, value in row.items()}
