"""Pure calculation for the fixed THS:883423 MA5 bias contract."""

from collections.abc import Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from typing import Final, Literal

from market_data_center.domain.board_index import BoardIndexDailyBarRecord

_BOARD_ID: Final = "THS:883423"
_BOARD_CODE: Final = "883423"
_BOARD_NAME: Final = "沪深主板昨日涨停"
_SIX_DECIMALS = Decimal("0.000001")


@dataclass(frozen=True, slots=True)
class BoardIndexBiasCalculation:
    board_id: Literal["THS:883423"]
    board_code: Literal["883423"]
    board_name: str
    trade_date: date
    close: Decimal
    moving_average_5: Decimal | None
    bias_5_pct: Decimal | None
    previous_trade_date: date | None
    previous_bias_5_pct: Decimal | None
    bias_direction: Literal["up", "down", "flat"] | None
    window_trading_days: Literal[30]
    bias_sample_count: int
    highest_bias_5_pct: Decimal | None
    highest_bias_trade_date: date | None
    lowest_bias_5_pct: Decimal | None
    lowest_bias_trade_date: date | None
    algorithm_version: Literal["board_index_bias_v1"]
    data_origin: Literal["database", "ths_live"]
    persistence_status: Literal["persisted", "queued"]
    fetched_at: datetime


def calculate_board_index_bias(
    records: Sequence[BoardIndexDailyBarRecord],
    *,
    fetched_at: datetime,
    data_origin: Literal["database", "ths_live"],
    persistence_status: Literal["persisted", "queued"],
) -> BoardIndexBiasCalculation:
    """Calculate ADR-0035 metrics without I/O or mutable state."""
    if not records:
        raise ValueError("board-index history must not be empty")
    if any(record.board_id != _BOARD_ID for record in records):
        raise ValueError("board-index history must contain only THS:883423")

    ordered = sorted(records, key=lambda record: record.trade_date)
    dates = [record.trade_date for record in ordered]
    if len(set(dates)) != len(dates):
        raise ValueError("board-index history contains a duplicate trade_date")

    scored: list[tuple[BoardIndexDailyBarRecord, Decimal | None, Decimal | None]] = []
    with localcontext() as context:
        context.prec = 38
        for index, record in enumerate(ordered):
            window = ordered[max(0, index - 4) : index + 1]
            if len(window) != 5 or any(item.close <= 0 for item in window):
                scored.append((record, None, None))
                continue
            moving_average = sum((item.close for item in window), Decimal(0)) / Decimal(5)
            bias = (record.close - moving_average) / moving_average * Decimal(100)
            scored.append((record, moving_average, bias))

    latest_record, latest_average, latest_bias = scored[-1]
    previous_record, _, previous_bias = scored[-2] if len(scored) > 1 else (None, None, None)
    observation_window = scored[-30:]
    valid_samples = [item for item in observation_window if item[2] is not None]

    highest = max(valid_samples, key=lambda item: (item[2], item[0].trade_date), default=None)
    lowest = min(
        valid_samples,
        key=lambda item: (item[2], -item[0].trade_date.toordinal()),
        default=None,
    )

    direction: Literal["up", "down", "flat"] | None = None
    if latest_bias is not None and previous_bias is not None:
        if latest_bias > previous_bias:
            direction = "up"
        elif latest_bias < previous_bias:
            direction = "down"
        else:
            direction = "flat"

    return BoardIndexBiasCalculation(
        board_id=_BOARD_ID,
        board_code=_BOARD_CODE,
        board_name=_BOARD_NAME,
        trade_date=latest_record.trade_date,
        close=latest_record.close,
        moving_average_5=_rounded(latest_average),
        bias_5_pct=_rounded(latest_bias),
        previous_trade_date=previous_record.trade_date if previous_record is not None else None,
        previous_bias_5_pct=_rounded(previous_bias),
        bias_direction=direction,
        window_trading_days=30,
        bias_sample_count=len(valid_samples),
        highest_bias_5_pct=_rounded(highest[2]) if highest is not None else None,
        highest_bias_trade_date=highest[0].trade_date if highest is not None else None,
        lowest_bias_5_pct=_rounded(lowest[2]) if lowest is not None else None,
        lowest_bias_trade_date=lowest[0].trade_date if lowest is not None else None,
        algorithm_version="board_index_bias_v1",
        data_origin=data_origin,
        persistence_status=persistence_status,
        fetched_at=fetched_at,
    )


def _rounded(value: Decimal | None) -> Decimal | None:
    if value is None:
        return None
    return value.quantize(_SIX_DECIMALS, rounding=ROUND_HALF_UP)
