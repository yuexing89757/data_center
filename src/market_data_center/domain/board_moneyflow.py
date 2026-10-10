"""Provider-neutral board daily money-flow facts (Eastmoney concept/industry/region)."""

import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from enum import StrEnum


class BoardMoneyflowType(StrEnum):
    CONCEPT = "concept"
    INDUSTRY = "industry"
    REGION = "region"


@dataclass(frozen=True, slots=True)
class BoardMoneyflowRecord:
    board_code: str
    trade_date: date
    board_type: BoardMoneyflowType
    name: str
    pct_change: Decimal
    close: Decimal | None
    net_amount: Decimal
    net_amount_rate: Decimal | None
    extra_large_amount: Decimal | None
    extra_large_rate: Decimal | None
    large_amount: Decimal | None
    large_rate: Decimal | None
    medium_amount: Decimal | None
    medium_rate: Decimal | None
    small_amount: Decimal | None
    small_rate: Decimal | None
    top_small_buy_stock: str | None
    rank: int | None
    source_code: str


@dataclass(frozen=True, slots=True)
class BoardMoneyflowValidation:
    accepted: tuple[BoardMoneyflowRecord, ...]
    rejected: tuple[tuple[BoardMoneyflowRecord, str], ...]


_BOARD_CODE_PATTERN = re.compile(r"DC:BK[0-9]{4}\Z")


def validate_board_moneyflows(
    records: tuple[BoardMoneyflowRecord, ...],
) -> tuple[BoardMoneyflowRecord, ...]:
    for record in records:
        if not _BOARD_CODE_PATTERN.fullmatch(record.board_code):
            raise ValueError(f"invalid board money-flow code: {record.board_code}")
        if not isinstance(record.board_type, BoardMoneyflowType):
            raise ValueError("invalid board money-flow type")
        if not record.name.strip():
            raise ValueError("board money-flow name is required")
        if not isinstance(record.net_amount, Decimal) or not isinstance(record.pct_change, Decimal):
            raise TypeError("board money-flow amounts must use Decimal")
        if record.rank is not None and record.rank < 1:
            raise ValueError("board money-flow rank must be positive")
        if record.source_code != "tushare":
            raise ValueError("unsupported board money-flow source")
    return records


def partition_board_moneyflows(
    records: tuple[BoardMoneyflowRecord, ...],
) -> BoardMoneyflowValidation:
    """Reject invalid facts and duplicate board-day keys without aborting the batch."""
    keys = Counter((record.trade_date, record.board_code) for record in records)
    accepted: list[BoardMoneyflowRecord] = []
    rejected: list[tuple[BoardMoneyflowRecord, str]] = []
    for record in records:
        try:
            if keys[(record.trade_date, record.board_code)] > 1:
                raise ValueError("duplicate board money-flow key")
            validate_board_moneyflows((record,))
        except (ValueError, TypeError) as error:
            rejected.append((record, str(error)))
        else:
            accepted.append(record)
    return BoardMoneyflowValidation(tuple(accepted), tuple(rejected))
