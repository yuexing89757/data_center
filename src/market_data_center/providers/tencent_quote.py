"""Bounded Tencent GBK batch adapter for provider-neutral five-level quotes."""

from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from re import finditer, fullmatch
from typing import cast
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import (
    ProviderError,
    RawRow,
    RealtimeQuoteFetch,
    RealtimeQuoteNormalizationError,
)
from market_data_center.settings import TencentQuoteSettings

ENDPOINT = "https://qt.gtimg.cn/q="
SCHEMA_VERSION = "tencent_quote.qt_gtimg.v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
MAX_RESPONSE_BYTES = 2_000_000
_ROW_PATTERN = r'v_(sh|sz)([0-9]{6})="([^"]*)";'


class _RowError(ProviderError):
    def __init__(self, reason: str, message: str) -> None:
        super().__init__(message)
        self.reason = reason


class TencentQuoteProvider:
    source_code = "tencent_quote"

    def __init__(
        self,
        settings: TencentQuoteSettings | None = None,
        *,
        request_bytes: Callable[[str, float], bytes] | None = None,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        configured = settings or TencentQuoteSettings()
        self._timeout_seconds = configured.tencent_quote_timeout_seconds
        self._batch_size = configured.tencent_quote_batch_size
        self._request_bytes = request_bytes or _request_bytes
        self._clock = clock

    def fetch_five_level_quotes(
        self, symbols: Sequence[str], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        _validate_deadline(deadline)
        requested = tuple(symbols)
        if not 1 <= len(requested) <= 500 or len(set(requested)) != len(requested):
            raise ProviderError("Tencent quote symbols must contain 1 to 500 unique values")
        source_by_symbol = {symbol: _source_symbol(symbol) for symbol in requested}
        raw_rows: list[RawRow] = []
        raw_observed_at: list[datetime] = []
        records: list[FiveLevelQuoteSnapshotRecord] = []
        errors: list[RealtimeQuoteNormalizationError] = []
        failed: set[str] = set()

        for start in range(0, len(requested), self._batch_size):
            batch = requested[start : start + self._batch_size]
            if deadline is not None and _utc(self._clock()) >= deadline:
                failed.update(requested[start:])
                break
            url = ENDPOINT + ",".join(source_by_symbol[symbol] for symbol in batch)
            try:
                payload = self._request_bytes(url, self._timeout_seconds)
                text = payload.decode("gbk", errors="strict")
            except (OSError, UnicodeError, HTTPError, URLError, ProviderError):
                failed.update(batch)
                continue
            observed_at = _utc(self._clock())
            returned: set[str] = set()
            for match in finditer(_ROW_PATTERN, text):
                symbol = _standard_symbol(match.group(1), match.group(2))
                if symbol not in batch or symbol in returned:
                    continue
                returned.add(symbol)
                raw_index = len(raw_rows)
                raw_rows.append(
                    {
                        "source_symbol": source_by_symbol[symbol],
                        "payload": match.group(3),
                        "observed_at": observed_at.isoformat(),
                    }
                )
                raw_observed_at.append(observed_at)
                try:
                    records.append(_record(symbol, match.group(3), observed_at))
                except (KeyError, ValueError, InvalidOperation, _RowError) as error:
                    failed.add(symbol)
                    errors.append(
                        RealtimeQuoteNormalizationError(
                            raw_row_index=raw_index,
                            symbol=symbol,
                            reason=error.reason
                            if isinstance(error, _RowError)
                            else "invalid_quote_record",
                        )
                    )
            failed.update(symbol for symbol in batch if symbol not in returned)

        return RealtimeQuoteFetch(
            raw_rows=tuple(raw_rows),
            records=tuple(records),
            requested_symbols=requested,
            failed_symbols=tuple(symbol for symbol in requested if symbol in failed),
            schema_version=SCHEMA_VERSION,
            raw_observed_at=tuple(raw_observed_at),
            normalization_errors=tuple(errors),
        )


def _request_bytes(url: str, timeout: float) -> bytes:
    request = Request(
        url,
        headers={
            "User-Agent": "MarketDataCenter/0.2",
            "Referer": "https://gu.qq.com/",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        if response.status != 200:
            raise ProviderError("Tencent quote returned a non-success response")
        payload = cast(bytes, response.read(MAX_RESPONSE_BYTES + 1))
    if len(payload) > MAX_RESPONSE_BYTES:
        raise ProviderError("Tencent quote response exceeded the byte bound")
    return payload


def normalize_tencent_quote_raw(
    dataset: DatasetCode,
    schema_version: str,
    rows: Sequence[Mapping[str, str]],
    request_params: Mapping[str, object],
) -> tuple[FiveLevelQuoteSnapshotRecord, ...]:
    """Replay Tencent Raw rows into the same provider-neutral records."""
    if dataset is not DatasetCode.FIVE_LEVEL_QUOTE or schema_version != SCHEMA_VERSION:
        raise ProviderError("unsupported Tencent quote Raw replay contract")
    requested = request_params.get("symbols")
    if not isinstance(requested, list) or not all(isinstance(item, str) for item in requested):
        raise ProviderError("Tencent quote Raw replay is missing requested symbols")
    requested_symbols = set(requested)
    records: list[FiveLevelQuoteSnapshotRecord] = []
    for row in rows:
        source_symbol = row.get("source_symbol")
        payload = row.get("payload")
        observed_text = row.get("observed_at")
        if (
            source_symbol is None
            or payload is None
            or observed_text is None
            or fullmatch(r"(?:sh|sz)[0-9]{6}", source_symbol) is None
        ):
            raise ProviderError("Tencent quote Raw row has an invalid shape")
        symbol = _standard_symbol(source_symbol[:2], source_symbol[2:])
        if symbol not in requested_symbols:
            raise ProviderError("Tencent quote Raw row was not requested by the source run")
        try:
            observed_at = datetime.fromisoformat(observed_text)
        except ValueError as error:
            raise ProviderError("Tencent quote Raw row has an invalid observation time") from error
        records.append(_record(symbol, payload, _utc(observed_at)))
    return tuple(records)


def _record(symbol: str, payload: str, observed_at: datetime) -> FiveLevelQuoteSnapshotRecord:
    fields = payload.split("~")
    if len(fields) < 49:
        raise _RowError("schema_drift", "Tencent quote row has fewer than 49 fields")
    if fields[2] != symbol.partition(":")[2]:
        raise _RowError("invalid_identity", "Tencent quote payload code does not match its key")
    source_timestamp = _source_timestamp(fields[30])
    bids = tuple(_level(fields, 9 + (level - 1) * 2, level) for level in range(1, 6))
    asks = tuple(_level(fields, 19 + (level - 1) * 2, level) for level in range(1, 6))
    amount_parts = fields[35].split("/")
    if len(amount_parts) != 3:
        raise _RowError("invalid_amount", "Tencent quote composite amount field is invalid")
    return FiveLevelQuoteSnapshotRecord(
        symbol=symbol,
        market=Market.CN_A_SHARE,
        observed_at=observed_at,
        source_timestamp=source_timestamp,
        quote_status=QuoteStatus.UNKNOWN,
        last_price=_optional_decimal(fields[3]),
        previous_close=_optional_decimal(fields[4]),
        open=_optional_decimal(fields[5]),
        high=_optional_decimal(fields[33]),
        low=_optional_decimal(fields[34]),
        cumulative_volume=_optional_lots_to_shares(fields[6]),
        cumulative_amount=_nonnegative_decimal(amount_parts[2]),
        bid_levels=bids,
        ask_levels=asks,
        source_code="tencent_quote",
        name=fields[1].strip() or None,
    )


def _level(fields: Sequence[str], offset: int, level: int) -> OrderBookLevel:
    price = _optional_decimal(fields[offset])
    volume = _optional_lots_to_shares(fields[offset + 1])
    if price is None and volume == 0:
        volume = None
    return OrderBookLevel(level, price, volume)


def _optional_decimal(value: str) -> Decimal | None:
    if not value:
        return None
    result = _nonnegative_decimal(value)
    return result if result > 0 else None


def _nonnegative_decimal(value: str) -> Decimal:
    try:
        result = Decimal(value)
    except InvalidOperation as error:
        raise _RowError("invalid_decimal", "Tencent quote contains an invalid decimal") from error
    if not result.is_finite() or result < 0:
        raise _RowError("invalid_decimal", "Tencent quote decimal must be finite and nonnegative")
    return result


def _lots_to_shares(value: str) -> int:
    if not fullmatch(r"[0-9]+", value):
        raise _RowError("invalid_quantity", "Tencent quote quantity is not an integer lot count")
    return int(value) * 100


def _optional_lots_to_shares(value: str) -> int | None:
    return None if value == "" else _lots_to_shares(value)


def _source_timestamp(value: str) -> datetime:
    if fullmatch(r"[0-9]{14}", value) is None:
        raise _RowError("invalid_timestamp", "Tencent quote timestamp is invalid")
    return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=SHANGHAI)


def _source_symbol(symbol: str) -> str:
    exchange, separator, code = symbol.partition(":")
    if separator != ":" or fullmatch(r"[0-9]{6}", code) is None:
        raise ProviderError("Tencent quote symbol must use SSE:nnnnnn or SZSE:nnnnnn")
    prefix = {"SSE": "sh", "SZSE": "sz"}.get(exchange)
    if prefix is None:
        raise ProviderError("Tencent quote supports SSE/SZSE stocks only")
    return prefix + code


def _standard_symbol(prefix: str, code: str) -> str:
    return f"{'SSE' if prefix == 'sh' else 'SZSE'}:{code}"


def _validate_deadline(deadline: datetime | None) -> None:
    if deadline is not None and (deadline.tzinfo is None or deadline.utcoffset() != timedelta()):
        raise ProviderError("Tencent quote deadline must be an aware UTC datetime")


def _utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ProviderError("Tencent quote observation clock must be timezone-aware")
    return value.astimezone(UTC)
