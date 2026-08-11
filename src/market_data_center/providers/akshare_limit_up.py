"""AKShare/Eastmoney current-day limit-up pool adapter."""

from collections.abc import Mapping, Sequence
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from threading import Lock
from typing import Protocol
from zoneinfo import ZoneInfo

from market_data_center.domain.today_limit_up import LimitUpSourceRecord
from market_data_center.providers.contracts import ProviderBatch, ProviderError

RAW_SCHEMA_VERSION = "akshare_eastmoney.current_day_limit_up_pool.v1"
SHANGHAI = ZoneInfo("Asia/Shanghai")
_AKSHARE_REQUEST_LOCK = Lock()


class LimitUpPoolClient(Protocol):
    def stock_zt_pool_em(self, *, date: str) -> object: ...


class BoundedAkshareLimitUpClient:
    """Apply a bounded HTTP timeout to AKShare's otherwise unbounded request."""

    def __init__(self, *, timeout_seconds: float, max_attempts: int) -> None:
        self._timeout_seconds = timeout_seconds
        self._max_attempts = max_attempts

    def stock_zt_pool_em(self, *, date: str) -> object:
        import akshare  # type: ignore[import-untyped]

        function = akshare.stock_zt_pool_em
        original_requests = function.__globals__["requests"]

        class TimeoutRequests:
            @staticmethod
            def get(url: str, **kwargs: object) -> object:
                kwargs["timeout"] = self._timeout_seconds
                return original_requests.get(url, **kwargs)

        last_error: Exception | None = None
        for _attempt in range(self._max_attempts):
            try:
                with _AKSHARE_REQUEST_LOCK:
                    function.__globals__["requests"] = TimeoutRequests
                    try:
                        return function(date=date)
                    finally:
                        function.__globals__["requests"] = original_requests
            except Exception as error:
                last_error = error
        if last_error is None:  # pragma: no cover - settings require at least one attempt
            raise RuntimeError("AKShare limit-up request did not run")
        raise last_error


class AkshareCurrentDayLimitUpProvider:
    source_code = "akshare"

    def __init__(self, client: LimitUpPoolClient) -> None:
        self._client = client

    def __enter__(self) -> "AkshareCurrentDayLimitUpProvider":
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        del exc_type, exc_value, traceback

    def fetch_limit_up_pool(self, trade_date: date) -> ProviderBatch[LimitUpSourceRecord]:
        try:
            frame = self._client.stock_zt_pool_em(date=trade_date.strftime("%Y%m%d"))
            rows = _records(frame)
        except Exception as error:
            raise ProviderError("AKShare current-day limit-up request failed") from error
        raw_rows = tuple({str(key): _raw(value) for key, value in row.items()} for row in rows)
        return ProviderBatch(
            raw_rows=raw_rows,
            request_params={"trade_date": trade_date.isoformat()},
            schema_version=RAW_SCHEMA_VERSION,
            record_factory=lambda: tuple(_map_row(row, trade_date) for row in raw_rows),
        )


def _records(frame: object) -> Sequence[Mapping[object, object]]:
    to_dict = getattr(frame, "to_dict", None)
    if not callable(to_dict):
        raise ProviderError("AKShare limit-up response is not tabular")
    rows = to_dict(orient="records")
    if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
        raise ProviderError("AKShare limit-up response records are invalid")
    return rows


def _map_row(row: Mapping[str, str], trade_date: date) -> LimitUpSourceRecord:
    code = row.get("代码", "").strip().zfill(6)
    if len(code) != 6 or not code.isdigit():
        raise ProviderError("invalid limit-up source code")
    if code.startswith("6"):
        symbol = f"SSE:{code}"
    elif code.startswith(("0", "3")):
        symbol = f"SZSE:{code}"
    elif code.startswith(("4", "8", "92")):
        symbol = f"BSE:{code}"
    else:
        raise ProviderError(f"unsupported limit-up source code: {code}")
    return LimitUpSourceRecord(
        trade_date=trade_date,
        symbol=symbol,
        source_name=_optional(row.get("名称")),
        first_limit_up_at=_source_time(row.get("首次封板时间"), trade_date),
        last_limit_up_at=_source_time(row.get("最后封板时间"), trade_date),
        open_count=_integer(row.get("炸板次数")),
        source_reported_sealed_funds_cny=_decimal(row.get("封板资金")),
    )


def _source_time(value: str | None, trade_date: date) -> datetime | None:
    text = _optional(value)
    if text is None:
        return None
    digits = text.split(".", maxsplit=1)[0].zfill(6)
    if len(digits) != 6 or not digits.isdigit():
        raise ProviderError("invalid source-reported limit-up time")
    try:
        wall_time = time(int(digits[:2]), int(digits[2:4]), int(digits[4:]))
    except ValueError as error:
        raise ProviderError("invalid source-reported limit-up time") from error
    if wall_time < time(9, 15) or wall_time > time(15, 0):
        raise ProviderError("source-reported limit-up time is outside the trading session")
    return datetime.combine(trade_date, wall_time, SHANGHAI)


def _raw(value: object) -> str:
    return "" if value is None else str(value)


def _optional(value: str | None) -> str | None:
    if value is None or not value.strip() or value.strip().lower() == "nan":
        return None
    return value.strip()


def _decimal(value: str | None) -> Decimal | None:
    text = _optional(value)
    if text is None:
        return None
    try:
        result = Decimal(text)
    except InvalidOperation as error:
        raise ProviderError("invalid source-reported decimal") from error
    if result < 0:
        raise ProviderError("source-reported decimal must be nonnegative")
    return result


def _integer(value: str | None) -> int | None:
    number = _decimal(value)
    if number is None:
        return None
    if number != number.to_integral_value():
        raise ProviderError("source-reported count must be integral")
    return int(number)
