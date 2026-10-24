"""Tushare limit_list_d D-only enrichment; exact Raw and independent CNY contract."""

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal, InvalidOperation
from types import TracebackType
from typing import Self
from urllib.error import HTTPError, URLError

from market_data_center.domain.today_limit_down import LimitDownSourceRecord
from market_data_center.providers.akshare_limit_down import _source_time
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow
from market_data_center.providers.quantity_units import (
    NORMALIZER_VERSION,
    UNIT_CONTRACT_VERSION,
    quantity_unit_rule,
)
from market_data_center.providers.tushare import TushareClient, TushareHttpClient

RAW_SCHEMA_VERSION = "tushare.current_day_limit_down_pool.v1"
FIELDS = (
    "trade_date",
    "ts_code",
    "name",
    "fd_amount",
    "first_time",
    "last_time",
    "open_times",
    "limit_times",
    "limit",
)
RESPONSE_LIMIT = 2500


class TushareCurrentDayLimitDownProvider:
    source_code = "tushare"

    def __init__(self, client: TushareClient, *, max_attempts: int = 2) -> None:
        if not 1 <= max_attempts <= 3:
            raise ValueError("limit-down max_attempts must be between 1 and 3")
        self._client = client
        self._max_attempts = max_attempts

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        if isinstance(self._client, TushareHttpClient):
            self._client.close()

    def fetch_limit_down_pool(self, trade_date: date) -> ProviderBatch[LimitDownSourceRecord]:
        if type(trade_date) is not date:
            raise ValueError("trade_date must be a date")
        for attempt in range(self._max_attempts):
            try:
                result = self._client.query(
                    "limit_list_d",
                    params={"trade_date": trade_date.strftime("%Y%m%d"), "limit_type": "D"},
                    fields=FIELDS,
                )
                break
            except (ProviderError, OSError) as error:
                if attempt + 1 == self._max_attempts or not _transient(error):
                    raise ProviderError("Tushare limit_list_d request failed") from error
        if not isinstance(result, Sequence) or isinstance(result, (str, bytes)):
            raise ProviderError("Tushare limit_list_d returned invalid rows")
        raw: list[RawRow] = []
        for row in result:
            if not isinstance(row, Mapping) or any(
                isinstance(v, (float, bool, Mapping, list, tuple)) for v in row.values()
            ):
                raise ProviderError("Tushare limit_list_d requires exact scalar source values")
            raw.append({str(k): "" if v is None else str(v) for k, v in row.items()})
        return ProviderBatch(
            raw_rows=tuple(raw),
            schema_version=RAW_SCHEMA_VERSION,
            request_params={
                "trade_date": trade_date.isoformat(),
                "api_name": "limit_list_d",
                "limit_type": "D",
                "provider_schema_version": RAW_SCHEMA_VERSION,
                "quantity_unit_contract_version": UNIT_CONTRACT_VERSION,
                "quantity_normalizer_version": NORMALIZER_VERSION,
                "sealed_funds_unit": "cny",
            },
            record_factory=lambda: normalize_limit_down_raw(raw, RAW_SCHEMA_VERSION, trade_date),
        )


def normalize_limit_down_raw(
    rows: Sequence[Mapping[str, object]], schema_version: str, trade_date: date
) -> list[LimitDownSourceRecord]:
    if schema_version != RAW_SCHEMA_VERSION:
        raise ProviderError("unsupported Tushare limit-down Raw schema")
    if len(rows) >= RESPONSE_LIMIT:
        raise ProviderError("Tushare limit_list_d response limit reached; completeness unknown")
    quantity_unit_rule(
        provider="tushare",
        endpoint="limit_list_d",
        schema_version=schema_version,
        instrument_type="stock",
        field="fd_amount",
    )
    records: list[LimitDownSourceRecord] = []
    for row in rows:
        if not set(FIELDS).issubset(row):
            raise ProviderError("Tushare limit_list_d missing required fields")
        if any(isinstance(value, (float, bool, Mapping, list, tuple)) for value in row.values()):
            raise ProviderError("Tushare limit_list_d requires exact source values")
        values = {key: "" if value is None else str(value) for key, value in row.items()}
        if values["trade_date"] != trade_date.strftime("%Y%m%d") or values["limit"] != "D":
            raise ProviderError("Tushare limit_list_d date or direction mismatch")
        code, separator, exchange = values["ts_code"].partition(".")
        exchanges = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
        if (
            separator != "."
            or exchange not in exchanges
            or len(code) != 6
            or not code.isascii()
            or not code.isdigit()
        ):
            raise ProviderError("Tushare limit_list_d invalid stock code")
        if values["first_time"].strip():
            raise ProviderError("Tushare limit_list_d D first seal time is unavailable")
        count = _count(values["limit_times"])
        if count is not None and count < 1:
            raise ProviderError("consecutive limit-down days must be positive")
        records.append(
            LimitDownSourceRecord(
                trade_date=trade_date,
                symbol=f"{exchanges[exchange]}:{code}",
                source_name=values["name"].strip() or None,
                first_limit_down_at=None,
                last_limit_down_at=_source_time(values["last_time"], trade_date),
                open_count=_count(values["open_times"]),
                source_reported_sealed_funds_cny=_money(values["fd_amount"]),
                consecutive_limit_down_days=count,
                source_code="tushare",
            )
        )
    return records


def _money(value: str) -> Decimal | None:
    if not value.strip():
        return None
    try:
        number = Decimal(value)
    except InvalidOperation as error:
        raise ProviderError("invalid Tushare limit-down decimal") from error
    if not number.is_finite() or number < 0:
        raise ProviderError("Tushare limit-down decimal must be nonnegative and finite")
    parts = number.as_tuple()
    assert isinstance(parts.exponent, int)
    extra_places = max(0, -parts.exponent - 4)
    if (number != 0 and number.adjusted() >= 26) or (
        extra_places and any(parts.digits[-extra_places:])
    ):
        raise ProviderError("Tushare limit-down value cannot be stored exactly as numeric(30,4)")
    return number


def _count(value: str) -> int | None:
    number = _money(value)
    if number is None:
        return None
    if number != number.to_integral_value():
        raise ProviderError("Tushare limit-down count must be integral")
    if number > 2_147_483_647:
        raise ProviderError("Tushare limit-down count exceeds integer storage")
    return int(number)


def _transient(error: BaseException) -> bool:
    if isinstance(error, HTTPError):
        return error.code in {408, 429, 500, 502, 503, 504}
    if isinstance(error, (TimeoutError, ConnectionError, URLError)):
        return True
    return error.__cause__ is not None and _transient(error.__cause__)
