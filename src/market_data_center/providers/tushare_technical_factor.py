"""Tushare stk_factor_pro boundary, fixed source catalog and Raw replay."""

from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from math import isfinite
from time import sleep
from types import MappingProxyType
from typing import Self
from urllib.error import HTTPError, URLError

from pydantic import ValidationError

from market_data_center.domain.records import Market
from market_data_center.domain.stock_technical_factor import (
    FACTOR_CATALOG_VERSION,
    TECHNICAL_FACTOR_NAMES,
    StockTechnicalFactorSnapshotRecord,
)
from market_data_center.providers.contracts import ProviderBatch, ProviderError, RawRow
from market_data_center.providers.quantity_units import quantity_unit_rule
from market_data_center.providers.tushare import TushareClient, TushareHttpClient
from market_data_center.settings import TushareSettings

RAW_SCHEMA_VERSION = "tushare.stock_technical_factor.v1"
TECHNICAL_FACTOR_RESPONSE_LIMIT = 10_000
ADJUSTMENT_BASES = {
    "bfq": "unadjusted",
    "qfq": "forward_adjusted",
    "hfq": "backward_adjusted",
}
COMMON_SOURCE_FIELDS = {
    "downdays": "down_days",
    "updays": "up_days",
    "lowdays": "low_range_days",
    "topdays": "high_range_days",
}


def _source_field(name: str, basis: str) -> str:
    if name == "kdj_j":
        return f"kdj_{basis}"
    prefix, _, period = name.partition("_")
    if prefix in {"ema", "ma", "rsi"} and period.isdigit():
        return f"{prefix}_{basis}_{period}"
    return f"{name}_{basis}"


SOURCE_FACTOR_CATALOG = MappingProxyType(
    {
        _source_field(name, basis): (group, name)
        for basis, group in ADJUSTMENT_BASES.items()
        for name in TECHNICAL_FACTOR_NAMES
    }
)


def _default_parameters(name: str, basis: str) -> Mapping[str, int]:
    """Freeze the documented defaults per source field, including basis differences."""
    prefix, _, period = name.partition("_")
    if prefix in {"ema", "ma", "rsi"} and period.isdigit():
        return MappingProxyType({"N": int(period)})
    groups: tuple[tuple[tuple[str, ...], dict[str, int]], ...] = (
        (("asi", "asit"), {"M1": 26, "M2": 10}),
        (("atr", "cr"), {"N": 20}),
        (("bias1", "bias2", "bias3"), {"L1": 6, "L2": 12, "L3": 24}),
        (("boll_lower", "boll_mid", "boll_upper"), {"N": 20, "P": 2}),
        (("brar_ar", "brar_br", "vr"), {"M1": 26}),
        (("cci", "mfi"), {"N": 14}),
        (("dfma_dif", "dfma_difma"), {"N1": 10, "N2": 50, "M": 10}),
        (("dmi_adx", "dmi_adxr", "dmi_mdi", "dmi_pdi"), {"M1": 14, "M2": 6}),
        (("dpo", "madpo"), {"M1": 20, "M2": 10, "M3": 6}),
        (("emv", "maemv"), {"N": 14, "M": 9}),
        (("expma_12", "expma_50"), {"N1": 12, "N2": 50}),
        (("kdj_j", "kdj_d", "kdj_k"), {"N": 9, "M1": 3, "M2": 3}),
        (("ktn_down", "ktn_mid", "ktn_upper"), {"N": 20, "M": 10}),
        (("macd", "macd_dea", "macd_dif"), {"SHORT": 12, "LONG": 26, "M": 9}),
        (("mass", "ma_mass"), {"N1": 9, "N2": 25, "M": 6}),
        (("mtm", "mtmma", "psy", "psyma", "roc", "maroc"), {"N": 12, "M": 6}),
        (("obv",), {}),
        (("taq_down", "taq_mid", "taq_up"), {"N": 20}),
        (("trix", "trma"), {"M1": 12, "M2": 20}),
        (("wr", "wr1"), {"N": 10, "N1": 6}),
        (("xsii_td1", "xsii_td2", "xsii_td3", "xsii_td4"), {"N": 102, "M": 7}),
    )
    if name == "bbi":
        return MappingProxyType(
            {"M1": 3, "M2": 6, "M3": 12, "M4": {"bfq": 20, "hfq": 21, "qfq": 22}[basis]}
        )
    for names, params in groups:
        if name in names:
            return MappingProxyType(params)
    raise ValueError("factor catalog is missing documented default parameters")


SOURCE_FACTOR_PARAMETERS = MappingProxyType(
    {
        _source_field(name, basis): _default_parameters(name, basis)
        for basis in ADJUSTMENT_BASES
        for name in TECHNICAL_FACTOR_NAMES
    }
)
# Source fields outside this domain are retained in Raw, never copied into facts.
NON_TECHNICAL_FIELDS = (
    "ts_code",
    "trade_date",
    "open",
    "open_hfq",
    "open_qfq",
    "high",
    "high_hfq",
    "high_qfq",
    "low",
    "low_hfq",
    "low_qfq",
    "close",
    "close_hfq",
    "close_qfq",
    "pre_close",
    "change",
    "pct_chg",
    "vol",
    "amount",
    "turnover_rate",
    "turnover_rate_f",
    "volume_ratio",
    "pe",
    "pe_ttm",
    "pb",
    "ps",
    "ps_ttm",
    "dv_ratio",
    "dv_ttm",
    "total_share",
    "float_share",
    "free_share",
    "total_mv",
    "circ_mv",
    "adj_factor",
)
TECHNICAL_FACTOR_FIELDS = (
    *NON_TECHNICAL_FIELDS,
    *SOURCE_FACTOR_CATALOG,
    *COMMON_SOURCE_FIELDS,
)


class TushareTechnicalFactorProvider:
    source_code = "tushare"

    def __init__(
        self,
        client: TushareClient,
        *,
        max_attempts: int = 3,
        request_interval_seconds: float = 2,
        sleeper: Callable[[float], None] = sleep,
    ) -> None:
        if not 1 <= max_attempts <= 5:
            raise ValueError("max_attempts must be between 1 and 5")
        if not isfinite(request_interval_seconds) or request_interval_seconds < 2:
            raise ValueError("technical factor requests must not exceed 30/minute")
        self._client = client
        self._max_attempts = max_attempts
        self._request_interval_seconds = request_interval_seconds
        self._sleeper = sleeper
        self._request_started = False

    @classmethod
    def default(cls) -> Self:
        try:
            settings = TushareSettings()  # type: ignore[call-arg]
            token = settings.tushare_token.get_secret_value().strip()
        except ValidationError as error:
            raise ProviderError("TUSHARE_TOKEN is required for the Tushare provider") from error
        if not token:
            raise ProviderError("TUSHARE_TOKEN is required for the Tushare provider")
        return cls(TushareHttpClient(token, endpoint=settings.tushare_endpoint))

    def fetch_snapshot(self, trade_date: date) -> ProviderBatch[StockTechnicalFactorSnapshotRecord]:
        if type(trade_date) is not date:
            raise ValueError("trade_date must be a date")
        params = {"trade_date": trade_date.strftime("%Y%m%d")}
        for attempt in range(self._max_attempts):
            if self._request_started:
                self._sleeper(self._request_interval_seconds)
            self._request_started = True
            try:
                result = self._client.query(
                    "stk_factor_pro", params=params, fields=TECHNICAL_FACTOR_FIELDS
                )
                break
            except (ProviderError, OSError) as error:
                if attempt + 1 == self._max_attempts or not _transient(error):
                    raise ProviderError("Tushare stk_factor_pro request failed") from error
        rows = _raw_rows(result)

        def records() -> list[StockTechnicalFactorSnapshotRecord]:
            normalized = normalize_technical_factor_raw(rows, RAW_SCHEMA_VERSION)
            if any(record.trade_date != trade_date for record in normalized):
                raise ProviderError("Tushare stk_factor_pro date does not match request")
            return normalized

        return ProviderBatch(
            raw_rows=rows,
            request_params={"api_name": "stk_factor_pro", **params},
            schema_version=RAW_SCHEMA_VERSION,
            record_factory=records,
        )


def _transient(error: BaseException) -> bool:
    if isinstance(error, HTTPError):
        return error.code in {408, 429, 500, 502, 503, 504}
    if isinstance(error, (TimeoutError, ConnectionError, URLError)):
        return True
    return error.__cause__ is not None and _transient(error.__cause__)


def _raw_rows(rows: Sequence[Mapping[str, object]]) -> tuple[RawRow, ...]:
    if not isinstance(rows, Sequence) or isinstance(rows, (str, bytes)):
        raise ProviderError("Tushare stk_factor_pro returned invalid rows")
    result: list[RawRow] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise ProviderError("Tushare stk_factor_pro returned a malformed row")
        if any(not isinstance(key, str) for key in row):
            raise ProviderError("Tushare stk_factor_pro returned invalid field names")
        if any(isinstance(value, (float, bool, Mapping, list, tuple)) for value in row.values()):
            raise ProviderError("Tushare stk_factor_pro requires exact scalar source values")
        result.append(
            MappingProxyType(
                {key: "" if value is None else str(value) for key, value in row.items()}
            )
        )
    return tuple(result)


def normalize_technical_factor_raw(
    rows: Sequence[Mapping[str, object]],
    schema_version: str,
) -> list[StockTechnicalFactorSnapshotRecord]:
    if schema_version != RAW_SCHEMA_VERSION:
        raise ProviderError("unsupported technical factor Raw schema")
    if not 0 < len(rows) < TECHNICAL_FACTOR_RESPONSE_LIMIT:
        raise ProviderError(
            "Tushare stk_factor_pro empty market snapshot or response limit reached"
        )
    raw_rows = _raw_rows(rows)
    records: dict[tuple[str, date], StockTechnicalFactorSnapshotRecord] = {}
    for row in raw_rows:
        missing = set(TECHNICAL_FACTOR_FIELDS).difference(row)
        if missing:
            raise ProviderError(
                f"Tushare stk_factor_pro missing fields: {', '.join(sorted(missing))}"
            )
        ts_code = row["ts_code"]
        code, separator, exchange = ts_code.partition(".")
        exchanges = {"SH": "SSE", "SZ": "SZSE", "BJ": "BSE"}
        if separator != "." or exchange not in exchanges:
            raise ProviderError("Tushare stk_factor_pro has invalid ts_code")
        try:
            if (
                len(row["trade_date"]) != 8
                or not row["trade_date"].isascii()
                or not row["trade_date"].isdigit()
            ):
                raise ValueError("trade_date must use YYYYMMDD")
            trade_date = datetime.strptime(row["trade_date"], "%Y%m%d").date()
            groups: dict[str, dict[str, Decimal | None]] = {
                group: {} for group in ADJUSTMENT_BASES.values()
            }
            for source_field, (group, name) in SOURCE_FACTOR_CATALOG.items():
                groups[group][name] = _decimal(row[source_field], source_field)
            common: dict[str, int | None] = {}
            for source_field, name in COMMON_SOURCE_FIELDS.items():
                value = _decimal(row[source_field], source_field)
                if value is not None and (value < 0 or value != value.to_integral_value()):
                    raise ValueError("common factors must be nonnegative integers")
                common[name] = None if value is None else int(value)
            record = StockTechnicalFactorSnapshotRecord(
                symbol=f"{exchanges[exchange]}:{code}",
                trade_date=trade_date,
                market=Market.CN_A_SHARE,
                source_code="tushare",
                factor_catalog_version=FACTOR_CATALOG_VERSION,
                unadjusted=groups["unadjusted"],
                forward_adjusted=groups["forward_adjusted"],
                backward_adjusted=groups["backward_adjusted"],
                common=common,
            )
        except (ValueError, InvalidOperation) as error:
            raise ProviderError("Tushare stk_factor_pro invalid snapshot values") from error
        key = record.symbol, record.trade_date
        if key in records and records[key] != record:
            raise ProviderError("Tushare stk_factor_pro conflicting duplicate natural key")
        records[key] = record
    if len({record.trade_date for record in records.values()}) != 1:
        raise ProviderError("Tushare stk_factor_pro contains multiple trading dates")
    # Formula parameters do not establish output scaling (including signed OBV).
    quantity_unit_rule(
        provider="tushare",
        endpoint="stk_factor_pro",
        schema_version=schema_version,
        instrument_type="stock",
        field="obv_bfq",
    )
    return [records[key] for key in sorted(records)]


def _decimal(value: str, field: str) -> Decimal | None:
    if not value.strip():
        return None
    number = Decimal(value)
    if not number.is_finite():
        raise ValueError(f"{field} must be finite")
    return number
