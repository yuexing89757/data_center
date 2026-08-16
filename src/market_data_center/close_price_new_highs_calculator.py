"""Pure calculation of SSE/SZSE strict 120-session closing highs."""

from decimal import ROUND_HALF_UP, Decimal
from hashlib import sha256
from json import dumps

from market_data_center.domain.close_price_new_highs import (
    CLOSE_PRICE_NEW_HIGHS_ALGORITHM_VERSION,
    ClosePriceNewHighCalculation,
    ClosePriceNewHighCandidate,
    ClosePriceNewHighInput,
    ClosePriceNewHighMember,
)
from market_data_center.domain.records import TradeStatus

_PERCENT_QUANTUM = Decimal("0.0000000001")
_VALID_STATUSES = (TradeStatus.TRADING, TradeStatus.UNKNOWN)


def calculate_close_price_new_highs_120d(
    source: ClosePriceNewHighInput,
) -> ClosePriceNewHighCalculation:
    eligible: list[ClosePriceNewHighCandidate] = []
    members: list[ClosePriceNewHighMember] = []
    ordered = tuple(sorted(source.candidates, key=lambda item: item.symbol))
    for candidate in ordered:
        if not _is_eligible(candidate):
            continue
        eligible.append(candidate)
        assert candidate.close is not None
        assert candidate.previous_119d_high is not None
        assert candidate.display_name is not None
        if candidate.close <= candidate.previous_119d_high:
            continue
        members.append(
            ClosePriceNewHighMember(
                symbol=candidate.symbol,
                code=candidate.code,
                display_name=candidate.display_name,
                close=candidate.close,
                previous_119d_high=candidate.previous_119d_high,
                breakout_pct=(
                    (candidate.close / candidate.previous_119d_high - Decimal(1)) * Decimal(100)
                ).quantize(_PERCENT_QUANTUM, rounding=ROUND_HALF_UP),
            )
        )
    sorted_members = tuple(sorted(members, key=lambda item: (-item.breakout_pct, item.symbol)))
    input_hash = _digest(_input_payload(source, ordered))
    content_hash = _digest([_member_payload(item) for item in sorted_members])
    return ClosePriceNewHighCalculation(
        trade_date=source.trade_date,
        first_trade_date=source.first_trade_date,
        session_count=source.session_count,
        candidates=ordered,
        members=sorted_members,
        candidate_count=len(ordered),
        eligible_history_count=len(eligible),
        omitted_count=len(ordered) - len(eligible),
        incomplete_history_count=sum(item.valid_bar_count != 120 for item in ordered),
        non_trading_bar_count=sum(item.has_non_trading_bar for item in ordered),
        nonpositive_price_count=sum(item.has_nonpositive_price for item in ordered),
        missing_name_count=sum(item.display_name is None for item in ordered),
        input_hash=input_hash,
        content_hash=content_hash,
    )


def _is_eligible(candidate: ClosePriceNewHighCandidate) -> bool:
    return (
        candidate.valid_bar_count == 120
        and candidate.close is not None
        and candidate.close > 0
        and candidate.current_status in _VALID_STATUSES
        and candidate.previous_119d_high is not None
        and candidate.previous_119d_high > 0
        and candidate.display_name is not None
    )


def _input_payload(
    source: ClosePriceNewHighInput,
    candidates: tuple[ClosePriceNewHighCandidate, ...],
) -> dict[str, object]:
    return {
        "algorithm_version": CLOSE_PRICE_NEW_HIGHS_ALGORITHM_VERSION,
        "trade_date": source.trade_date.isoformat(),
        "first_trade_date": source.first_trade_date.isoformat(),
        "session_count": source.session_count,
        "candidates": [
            {
                "symbol": item.symbol,
                "code": item.code,
                "display_name": item.display_name,
                "valid_bar_count": item.valid_bar_count,
                "close": str(item.close) if item.close is not None else None,
                "current_status": (
                    item.current_status.value if item.current_status is not None else None
                ),
                "previous_119d_high": (
                    str(item.previous_119d_high) if item.previous_119d_high is not None else None
                ),
                "has_non_trading_bar": item.has_non_trading_bar,
                "has_nonpositive_price": item.has_nonpositive_price,
            }
            for item in candidates
        ],
    }


def _member_payload(item: ClosePriceNewHighMember) -> dict[str, str]:
    return {
        "symbol": item.symbol,
        "code": item.code,
        "display_name": item.display_name,
        "close": str(item.close),
        "previous_119d_high": str(item.previous_119d_high),
        "breakout_pct": str(item.breakout_pct),
    }


def _digest(value: object) -> str:
    canonical = dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(canonical.encode("utf-8")).hexdigest()
