"""Provider-neutral opening call-auction virtual indicative detail facts."""

from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from enum import StrEnum
from zoneinfo import ZoneInfo

SHANGHAI = ZoneInfo("Asia/Shanghai")


class SourceDisplayClassification(StrEnum):
    """Untrusted provider display classification; never a trade direction."""

    INTERNAL = "internal"
    EXTERNAL = "external"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class CallAuctionIndicativeDetailRecord:
    symbol: str
    trade_date: date
    observed_at: datetime
    indicative_price: Decimal
    displayed_volume_shares: int
    source_sequence: int
    source_display_classification: SourceDisplayClassification
    source_code: str = "eastmoney"

    def __post_init__(self) -> None:
        if not self.symbol.startswith(("SSE:", "SZSE:")):
            raise ValueError("auction indicative detail supports SSE/SZSE symbols only")
        local = self.observed_at.astimezone(SHANGHAI)
        if local.date() != self.trade_date or not time(9, 15) <= local.time() < time(9, 26):
            raise ValueError("auction indicative observation must be in 09:15:00-09:25:59")
        if self.indicative_price <= 0:
            raise ValueError("indicative price must be positive")
        if self.displayed_volume_shares < 0 or self.displayed_volume_shares % 100:
            raise ValueError("displayed A-share volume must be nonnegative whole lots in shares")
        if self.source_sequence < 0:
            raise ValueError("source_sequence must be nonnegative")
        if self.source_code != "eastmoney":
            raise ValueError("unsupported auction indicative source")
