"""Bounded Eastmoney adapter for current-day auction indicative detail."""

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from json import loads
from time import sleep
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

from market_data_center.domain.auction_indicative import (
    CallAuctionIndicativeDetailRecord,
    SourceDisplayClassification,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow

SHANGHAI = ZoneInfo("Asia/Shanghai")
SCHEMA_VERSION = "eastmoney.call_auction_indicative_detail.v1"
ENDPOINTS = (
    "https://push2delay.eastmoney.com/api/qt/stock/details/get",
    "https://push2.eastmoney.com/api/qt/stock/details/get",
)
MAX_SOURCE_ROWS = 5000


class EastmoneyAuctionIndicativeProvider:
    source_code = "eastmoney"

    def __init__(
        self,
        request_json: Callable[[str, float], Mapping[str, Any]] | None = None,
        *,
        timeout_seconds: float = 8.0,
        max_attempts: int = 2,
    ) -> None:
        if not 1 <= max_attempts <= 2 or not 1 <= timeout_seconds <= 15:
            raise ValueError("Eastmoney request bounds are invalid")
        self._request_json = request_json or _request_json
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts

    def fetch_current_day(
        self, symbol: str, trade_date: date, *, now: datetime
    ) -> ProviderBatch[CallAuctionIndicativeDetailRecord]:
        local_now = now.astimezone(SHANGHAI)
        if trade_date != local_now.date():
            raise ProviderError("Eastmoney auction detail supports the current Shanghai date only")
        if local_now.time() < time(9, 26):
            raise ProviderError(
                "auction indicative detail is not complete before 09:26 Shanghai time"
            )
        secid = _secid(symbol)
        params = {
            "secid": secid,
            "pos": f"-{MAX_SOURCE_ROWS}",
            "fields1": "f1,f2,f3,f4",
            "fields2": "f51,f52,f53,f54,f55",
            "fltt": "2",
        }
        payload: Mapping[str, Any] | None = None
        for attempt, endpoint in enumerate(ENDPOINTS[: self._max_attempts]):
            url = f"{endpoint}?{urlencode(params)}"
            try:
                payload = self._request_json(url, self._timeout_seconds)
                break
            except (OSError, ValueError, ProviderError) as error:
                if attempt + 1 == self._max_attempts:
                    raise ProviderError("Eastmoney auction indicative request failed") from error
                sleep(0.2)
        data = payload.get("data") if payload is not None else None
        if payload is None or payload.get("rc") != 0 or not isinstance(data, Mapping):
            raise ProviderError("Eastmoney auction indicative response is unavailable")
        details = data.get("details")
        if not isinstance(details, Sequence) or isinstance(details, (str, bytes)):
            raise ProviderError("Eastmoney auction indicative response has no detail list")
        if len(details) >= MAX_SOURCE_ROWS:
            raise ProviderError(
                "Eastmoney auction indicative response may be truncated at the row bound"
            )
        raw_rows = tuple(_raw_row(value, index) for index, value in enumerate(details))
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={"symbol": symbol, "trade_date": trade_date.isoformat(), "secid": secid},
            schema_version=SCHEMA_VERSION,
            record_factory=lambda: _records(raw_rows, symbol, trade_date),
        )


def _request_json(url: str, timeout: float) -> Mapping[str, Any]:
    request = Request(
        url,
        headers={"User-Agent": "MarketDataCenter/0.2", "Referer": "https://quote.eastmoney.com/"},
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            if response.status != 200:
                raise ProviderError("Eastmoney returned a non-success response")
            value = loads(response.read(2_000_000))
    except (HTTPError, URLError) as error:
        raise ProviderError("Eastmoney request failed") from error
    if not isinstance(value, Mapping):
        raise ProviderError("Eastmoney response is not an object")
    return value


def _secid(symbol: str) -> str:
    prefix, separator, code = symbol.strip().upper().partition(":")
    if separator != ":" or len(code) != 6 or not code.isdigit() or prefix not in {"SSE", "SZSE"}:
        raise ValueError("symbol must be SSE:nnnnnn or SZSE:nnnnnn")
    return f"{1 if prefix == 'SSE' else 0}.{code}"


def _raw_row(value: object, sequence: int) -> RawRow:
    if not isinstance(value, str):
        raise ProviderError("Eastmoney detail row is not a string")
    parts = value.split(",")
    if len(parts) != 5:
        raise ProviderError("Eastmoney detail row has an unexpected shape")
    return {
        "source_sequence": str(sequence),
        "time": parts[0],
        "price": parts[1],
        "volume_lots": parts[2],
        "source_auxiliary": parts[3],
        "source_display_code": parts[4],
    }


def _records(
    rows: Sequence[RawRow], symbol: str, trade_date: date
) -> tuple[CallAuctionIndicativeDetailRecord, ...]:
    result: list[CallAuctionIndicativeDetailRecord] = []
    for row in rows:
        observed_time = row["time"]
        if not "09:15:00" <= observed_time <= "09:25:59":
            continue
        try:
            price = Decimal(row["price"])
            lots = Decimal(row["volume_lots"])
            if lots != lots.to_integral_value():
                raise ValueError("volume is not an integer lot count")
            observed_at = datetime.fromisoformat(
                f"{trade_date.isoformat()}T{observed_time}"
            ).replace(tzinfo=SHANGHAI)
        except (InvalidOperation, ValueError) as error:
            raise ProviderError("Eastmoney auction detail contains invalid values") from error
        display = {
            "1": SourceDisplayClassification.INTERNAL,
            "2": SourceDisplayClassification.EXTERNAL,
        }.get(row["source_display_code"], SourceDisplayClassification.UNKNOWN)
        result.append(
            CallAuctionIndicativeDetailRecord(
                symbol=symbol,
                trade_date=trade_date,
                observed_at=observed_at,
                indicative_price=price,
                displayed_volume_shares=int(lots) * 100,
                source_sequence=int(row["source_sequence"]),
                source_display_classification=display,
            )
        )
    return tuple(result)
