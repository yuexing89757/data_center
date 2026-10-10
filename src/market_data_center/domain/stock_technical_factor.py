"""Complete, provider-neutral source technical-factor snapshots (ADR-0066)."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from types import MappingProxyType

from market_data_center.domain.records import Market

FACTOR_CATALOG_VERSION = "stock-technical-factor.v1"
TECHNICAL_FACTOR_NAMES = (
    "asi",
    "asit",
    "atr",
    "bbi",
    "bias1",
    "bias2",
    "bias3",
    "boll_lower",
    "boll_mid",
    "boll_upper",
    "brar_ar",
    "brar_br",
    "cci",
    "cr",
    "dfma_dif",
    "dfma_difma",
    "dmi_adx",
    "dmi_adxr",
    "dmi_mdi",
    "dmi_pdi",
    "dpo",
    "madpo",
    "emv",
    "maemv",
    "expma_12",
    "expma_50",
    "ema_5",
    "ema_10",
    "ema_20",
    "ema_30",
    "ema_60",
    "ema_90",
    "ema_250",
    "kdj_j",
    "kdj_d",
    "kdj_k",
    "ktn_down",
    "ktn_mid",
    "ktn_upper",
    "ma_5",
    "ma_10",
    "ma_20",
    "ma_30",
    "ma_60",
    "ma_90",
    "ma_250",
    "macd",
    "macd_dea",
    "macd_dif",
    "mass",
    "ma_mass",
    "mfi",
    "mtm",
    "mtmma",
    "obv",
    "psy",
    "psyma",
    "roc",
    "maroc",
    "rsi_6",
    "rsi_12",
    "rsi_24",
    "taq_down",
    "taq_mid",
    "taq_up",
    "trix",
    "trma",
    "vr",
    "wr",
    "wr1",
    "xsii_td1",
    "xsii_td2",
    "xsii_td3",
    "xsii_td4",
)
COMMON_FACTOR_NAMES = ("down_days", "up_days", "low_range_days", "high_range_days")


@dataclass(frozen=True, slots=True)
class StockTechnicalFactorSnapshotRecord:
    symbol: str
    trade_date: date
    market: Market
    source_code: str
    factor_catalog_version: str
    unadjusted: Mapping[str, Decimal | None]
    forward_adjusted: Mapping[str, Decimal | None]
    backward_adjusted: Mapping[str, Decimal | None]
    common: Mapping[str, int | None]

    def __post_init__(self) -> None:
        exchange, separator, code = self.symbol.partition(":")
        if (
            separator != ":"
            or exchange not in {"SSE", "SZSE", "BSE"}
            or len(code) != 6
            or not code.isascii()
            or not code.isdigit()
        ):
            raise ValueError("symbol must use a standard exchange and six-digit code")
        if self.market is not Market.CN_A_SHARE:
            raise ValueError("technical factors only support CN_A_SHARE")
        if type(self.trade_date) is not date:
            raise ValueError("trade_date must be a date")
        if not self.source_code.strip():
            raise ValueError("source_code must not be blank")
        if self.factor_catalog_version != FACTOR_CATALOG_VERSION:
            raise ValueError("unsupported factor catalog version")
        for group in ("unadjusted", "forward_adjusted", "backward_adjusted"):
            values = getattr(self, group)
            if set(values) != set(TECHNICAL_FACTOR_NAMES):
                raise ValueError("technical factors must contain the complete catalog")
            if any(
                value is not None and (not isinstance(value, Decimal) or not value.is_finite())
                for value in values.values()
            ):
                raise ValueError("technical factor values must be finite Decimal or None")
            object.__setattr__(self, group, MappingProxyType(dict(values)))
        if set(self.common) != set(COMMON_FACTOR_NAMES):
            raise ValueError("common factors must contain the complete catalog")
        if any(
            value is not None and (type(value) is not int or value < 0)
            for value in self.common.values()
        ):
            raise ValueError("common factor values must be nonnegative integers or None")
        object.__setattr__(self, "common", MappingProxyType(dict(self.common)))


def validate_technical_factor_snapshot(
    records: Sequence[StockTechnicalFactorSnapshotRecord],
    *,
    trade_date: date,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
    minimum_accepted_rows: int,
) -> tuple[StockTechnicalFactorSnapshotRecord, ...]:
    """Validate a complete market batch; the caller supplies active Security identities."""
    if minimum_accepted_rows < 0:
        raise ValueError("minimum_accepted_rows must not be negative")
    if not 0 < len(records) < 10_000:
        raise ValueError("technical factor snapshot is empty or reaches the response limit")
    if trade_date not in known_trading_dates:
        raise ValueError("technical factor snapshot requires a known trading date")
    unique: dict[tuple[str, date], StockTechnicalFactorSnapshotRecord] = {}
    for record in records:
        if record.trade_date != trade_date:
            raise ValueError("technical factor snapshot trade_date does not match request")
        if record.symbol not in known_symbols:
            raise ValueError("technical factor snapshot references an unknown or inactive Security")
        key = record.symbol, record.trade_date
        if key in unique and unique[key] != record:
            raise ValueError(
                "technical factor snapshot contains conflicting duplicate natural keys"
            )
        unique[key] = record
    if len(unique) < minimum_accepted_rows:
        raise ValueError("technical factor snapshot is below the market coverage floor")
    return tuple(unique[key] for key in sorted(unique))
