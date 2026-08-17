"""Bounded Xueqiu pankou adapter for opening-auction five-level facts."""

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from json import JSONDecodeError, loads
from typing import Protocol, cast
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import (
    ProviderError,
    RealtimeQuoteFetch,
    RealtimeQuoteNormalizationError,
)
from market_data_center.settings import PysnowballSettings

_PANKOU_URL = "https://stock.xueqiu.com/v5/stock/realtime/pankou.json"
_REQUEST_TIMEOUT_SECONDS = 2.0


class PankouClient(Protocol):
    def fetch(self, source_symbol: str, token: str, timeout_seconds: float) -> bytes: ...


class _NetworkPankouClient:
    def fetch(self, source_symbol: str, token: str, timeout_seconds: float) -> bytes:
        request = Request(
            f"{_PANKOU_URL}?{urlencode({'symbol': source_symbol})}",
            headers={
                "Accept": "application/json",
                "Cookie": token,
                "User-Agent": "Xueqiu iPhone 14.15.1",
            },
        )
        with urlopen(request, timeout=timeout_seconds) as response:
            return cast(bytes, response.read())


class _PankouNormalizationError(ProviderError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class PysnowballQuoteProvider:
    """Fetch one Xueqiu pankou response per SSE/SZSE stock."""

    source_code = "pysnowball"

    def __init__(
        self,
        settings: PysnowballSettings,
        *,
        client: PankouClient | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._token = settings.resolved_token()
        self._client = client or _NetworkPankouClient()
        self._clock = clock

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        requested = tuple(dict.fromkeys(symbols))
        if not requested or len(requested) != len(symbols):
            raise ProviderError("pysnowball symbols must be non-empty and unique")
        if deadline is not None and (
            deadline.tzinfo is None or deadline.utcoffset() != timedelta()
        ):
            raise ProviderError("pysnowball deadline must be an aware UTC datetime")

        source_symbols = {symbol: _source_symbol(symbol) for symbol in requested}
        raw_rows: list[Mapping[str, str]] = []
        raw_observed_at: list[datetime] = []
        records: list[FiveLevelQuoteSnapshotRecord] = []
        failed: set[str] = set()
        normalization_errors: list[RealtimeQuoteNormalizationError] = []

        for index, symbol in enumerate(requested):
            now = _utc_observation(self._clock())
            if deadline is not None and now >= deadline:
                failed.update(requested[index:])
                break
            timeout = _REQUEST_TIMEOUT_SECONDS
            if deadline is not None:
                timeout = min(timeout, (deadline - now).total_seconds())
            try:
                payload = self._client.fetch(source_symbols[symbol], self._token, timeout)
                row = _decode_payload(payload)
            except Exception:
                failed.add(symbol)
                continue

            observed_at = _utc_observation(self._clock())
            raw_row_index = len(raw_rows)
            raw_rows.append(_raw_row(row))
            raw_observed_at.append(observed_at)
            try:
                records.append(_record(symbol, source_symbols[symbol], row, observed_at))
            except (KeyError, TypeError, ValueError, ProviderError) as error:
                failed.add(symbol)
                normalization_errors.append(
                    RealtimeQuoteNormalizationError(
                        raw_row_index,
                        symbol,
                        (
                            error.reason
                            if isinstance(error, _PankouNormalizationError)
                            else "invalid_quote_record"
                        ),
                    )
                )

        return RealtimeQuoteFetch(
            tuple(raw_rows),
            tuple(records),
            requested,
            tuple(symbol for symbol in requested if symbol in failed),
            "pysnowball.pankou.v1",
            tuple(raw_observed_at),
            tuple(normalization_errors),
        )


def _decode_payload(payload: bytes) -> Mapping[str, object]:
    try:
        decoded = loads(payload.decode("utf-8"), parse_float=Decimal)
    except (UnicodeDecodeError, JSONDecodeError) as error:
        raise _PankouNormalizationError("invalid_json", "invalid pysnowball JSON") from error
    if not isinstance(decoded, dict):
        raise _PankouNormalizationError("invalid_schema", "pysnowball row must be an object")
    row = decoded.get("data", decoded)
    if not isinstance(row, dict):
        raise _PankouNormalizationError("invalid_schema", "pysnowball data must be an object")
    return cast(Mapping[str, object], row)


def _record(
    symbol: str,
    source_symbol: str,
    row: Mapping[str, object],
    observed_at: datetime,
) -> FiveLevelQuoteSnapshotRecord:
    if row.get("symbol") != source_symbol:
        raise _PankouNormalizationError(
            "invalid_identity", "pysnowball returned a different stock identity"
        )
    bids = tuple(_level(row, "b", level) for level in range(1, 6))
    asks = tuple(_level(row, "s", level) for level in range(1, 6))
    has_quote = any(level.price is not None for level in (*bids, *asks))
    return FiveLevelQuoteSnapshotRecord(
        symbol=symbol,
        market=Market.CN_A_SHARE,
        observed_at=observed_at,
        source_timestamp=_source_timestamp(row.get("timestamp")),
        quote_status=QuoteStatus.TRADING if has_quote else QuoteStatus.UNKNOWN,
        last_price=_optional_price(row.get("current")),
        previous_close=None,
        open=None,
        high=None,
        low=None,
        cumulative_volume=None,
        cumulative_amount=None,
        bid_levels=bids,
        ask_levels=asks,
        source_code="pysnowball",
    )


def _level(row: Mapping[str, object], prefix: str, level: int) -> OrderBookLevel:
    price = _optional_price(row[f"{prefix}p{level}"])
    if price is None:
        return OrderBookLevel(level, None, None)
    quantity = row[f"{prefix}c{level}"]
    if isinstance(quantity, bool) or not isinstance(quantity, int) or quantity < 0:
        raise _PankouNormalizationError(
            "invalid_quantity", "pysnowball quantity must be a nonnegative integer share count"
        )
    return OrderBookLevel(level, price, quantity)


def _optional_price(value: object) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, (bool, float)):
        raise _PankouNormalizationError(
            "invalid_decimal", "pysnowball price must be decoded without float"
        )
    try:
        price = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise _PankouNormalizationError(
            "invalid_decimal", "pysnowball price is not decimal"
        ) from error
    if price < 0:
        raise _PankouNormalizationError("negative_price", "pysnowball price must not be negative")
    return price if price > 0 else None


def _source_timestamp(value: object) -> datetime | None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        return None
    try:
        seconds, milliseconds = divmod(value, 1000)
        return datetime.fromtimestamp(seconds, tz=UTC) + timedelta(milliseconds=milliseconds)
    except (OverflowError, OSError, ValueError):
        return None


def _source_symbol(symbol: str) -> str:
    exchange, separator, code = symbol.partition(":")
    if separator != ":" or len(code) != 6 or not code.isdigit():
        raise ProviderError("invalid standard pysnowball symbol")
    if exchange == "SSE":
        return f"SH{code}"
    if exchange == "SZSE":
        return f"SZ{code}"
    raise ProviderError("pysnowball auction collection supports SSE/SZSE only")


def _utc_observation(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderError("pysnowball observation clock must be timezone-aware")
    return value.astimezone(UTC)


def _raw_row(row: Mapping[str, object]) -> Mapping[str, str]:
    return {key: str(value) for key, value in row.items()}
