"""Pure, deterministic calculators for versioned A-share derived facts."""

from collections import defaultdict
from collections.abc import Iterable, Sequence
from datetime import date
from decimal import Decimal, localcontext
from enum import Enum
from hashlib import sha256
from json import dumps
from typing import Protocol

from market_data_center.domain.derived import (
    AdjustedDailyBarRecord,
    AdjustmentType,
    ClassificationDailyMetricRecord,
    ClassificationMembershipSnapshot,
    DailyMetricRecord,
    DerivedCalculationInput,
    DerivedCalculationOutput,
    MarketCapitalizationRecord,
)
from market_data_center.domain.records import (
    CorporateActionStatus,
    DailyBarRecord,
    DistributionRecord,
    RightsIssueRecord,
    ShareCapitalRecord,
)

ZERO = Decimal(0)
ONE = Decimal(1)


class _SymbolRecord(Protocol):
    @property
    def symbol(self) -> str: ...


class CalculationInputError(ValueError):
    """Raised when Core facts cannot support a deterministic calculation."""


def calculate_derived_facts(
    inputs: DerivedCalculationInput, *, start_date: date, end_date: date
) -> DerivedCalculationOutput:
    """Calculate adjusted prices and objective daily metrics without I/O."""
    if end_date < start_date:
        raise ValueError("end_date must not precede start_date")
    bars_by_symbol = _group_bars(inputs.daily_bars)
    distributions = _group_by_symbol(inputs.distributions)
    rights_issues = _group_by_symbol(inputs.rights_issues)
    capital = _group_by_symbol(inputs.share_capital)

    adjusted: list[AdjustedDailyBarRecord] = []
    daily_metrics: list[DailyMetricRecord] = []
    market_caps: list[MarketCapitalizationRecord] = []
    raw_by_key: dict[tuple[str, date], DailyBarRecord] = {}
    market_cap_by_key: dict[tuple[str, date], MarketCapitalizationRecord] = {}

    for symbol in sorted(bars_by_symbol):
        bars = bars_by_symbol[symbol]
        all_adjusted = calculate_adjusted_daily_bars(
            bars,
            distributions=distributions.get(symbol, ()),
            rights_issues=rights_issues.get(symbol, ()),
            start_date=bars[0].trade_date,
            end_date=end_date,
        )
        symbol_adjusted = tuple(
            record for record in all_adjusted if record.trade_date >= start_date
        )
        adjusted.extend(symbol_adjusted)
        all_forward = tuple(
            record for record in all_adjusted if record.adjustment_type is AdjustmentType.FORWARD
        )
        symbol_daily_metrics = tuple(
            record
            for record in calculate_daily_metrics(all_forward)
            if record.trade_date >= start_date
        )
        daily_metrics.extend(symbol_daily_metrics)
        symbol_caps = calculate_market_capitalizations(
            bars,
            capital.get(symbol, ()),
            start_date=start_date,
            end_date=end_date,
        )
        market_caps.extend(symbol_caps)
        market_cap_by_key.update(
            {(record.symbol, record.trade_date): record for record in symbol_caps}
        )
        raw_by_key.update(
            {
                (record.symbol, record.trade_date): record
                for record in bars
                if start_date <= record.trade_date <= end_date
            }
        )

    metrics_by_key = {(record.symbol, record.trade_date): record for record in daily_metrics}
    classification_metrics = calculate_classification_metrics(
        inputs.memberships,
        raw_by_key=raw_by_key,
        daily_metrics_by_key=metrics_by_key,
        market_cap_by_key=market_cap_by_key,
        trade_dates=sorted({trade_date for _, trade_date in raw_by_key}),
    )
    return DerivedCalculationOutput(
        adjusted_daily_bars=tuple(adjusted),
        daily_metrics=tuple(daily_metrics),
        market_capitalizations=tuple(market_caps),
        classification_metrics=classification_metrics,
    )


def calculate_adjusted_daily_bars(
    bars: Sequence[DailyBarRecord],
    *,
    distributions: Sequence[DistributionRecord],
    rights_issues: Sequence[RightsIssueRecord],
    start_date: date,
    end_date: date,
) -> tuple[AdjustedDailyBarRecord, ...]:
    ordered = _validated_bars(bars)
    if not ordered:
        return ()
    symbol = ordered[0].symbol
    event_factors = _event_factors(ordered, distributions, rights_issues)
    forward_factors: dict[date, Decimal] = {}
    backward_factors: dict[date, Decimal] = {}

    with localcontext() as context:
        context.prec = 40
        cumulative = ONE
        for bar in reversed(ordered):
            forward_factors[bar.trade_date] = cumulative
            if bar.trade_date in event_factors:
                cumulative *= event_factors[bar.trade_date]

        cumulative = ONE
        for bar in ordered:
            if bar.trade_date in event_factors:
                cumulative /= event_factors[bar.trade_date]
            backward_factors[bar.trade_date] = cumulative

    output: list[AdjustedDailyBarRecord] = []
    for index, bar in enumerate(ordered):
        if not start_date <= bar.trade_date <= end_date:
            continue
        previous_date = ordered[index - 1].trade_date if index else bar.trade_date
        for adjustment_type, factors in (
            (AdjustmentType.FORWARD, forward_factors),
            (AdjustmentType.BACKWARD, backward_factors),
        ):
            factor = factors[bar.trade_date]
            previous_factor = factors[previous_date]
            output.append(
                AdjustedDailyBarRecord(
                    symbol=symbol,
                    trade_date=bar.trade_date,
                    adjustment_type=adjustment_type,
                    adjustment_factor=factor,
                    open=_multiply(bar.open, factor),
                    high=_multiply(bar.high, factor),
                    low=_multiply(bar.low, factor),
                    close=_multiply(bar.close, factor),
                    previous_close=_multiply(bar.previous_close, previous_factor),
                )
            )
    return tuple(output)


def calculate_daily_metrics(
    forward_adjusted_bars: Sequence[AdjustedDailyBarRecord],
) -> tuple[DailyMetricRecord, ...]:
    ordered = sorted(forward_adjusted_bars, key=lambda record: record.trade_date)
    if any(record.adjustment_type is not AdjustmentType.FORWARD for record in ordered):
        raise ValueError("daily metrics require forward-adjusted bars")
    closes: list[Decimal | None] = []
    output: list[DailyMetricRecord] = []
    for bar in ordered:
        closes.append(bar.close)
        total_return = None
        if bar.close is not None and bar.previous_close is not None and bar.previous_close > 0:
            total_return = bar.close / bar.previous_close - ONE
        output.append(
            DailyMetricRecord(
                symbol=bar.symbol,
                trade_date=bar.trade_date,
                total_return_1d=total_return,
                moving_average_5=_moving_average(closes, 5),
                moving_average_10=_moving_average(closes, 10),
                moving_average_20=_moving_average(closes, 20),
            )
        )
    return tuple(output)


def calculate_market_capitalizations(
    bars: Sequence[DailyBarRecord],
    capital: Sequence[ShareCapitalRecord],
    *,
    start_date: date,
    end_date: date,
) -> tuple[MarketCapitalizationRecord, ...]:
    ordered_capital = sorted(capital, key=lambda record: record.effective_date)
    output: list[MarketCapitalizationRecord] = []
    capital_index = 0
    current: ShareCapitalRecord | None = None
    for bar in _validated_bars(bars):
        while (
            capital_index < len(ordered_capital)
            and ordered_capital[capital_index].effective_date <= bar.trade_date
        ):
            current = ordered_capital[capital_index]
            capital_index += 1
        if not start_date <= bar.trade_date <= end_date or bar.close is None or current is None:
            continue
        circulating_shares = (
            current.listed_a_shares
            if current.listed_a_shares is not None
            else current.circulating_shares
        )
        output.append(
            MarketCapitalizationRecord(
                symbol=bar.symbol,
                trade_date=bar.trade_date,
                total_market_cap=bar.close * current.total_shares,
                circulating_market_cap=(
                    bar.close * circulating_shares if circulating_shares is not None else None
                ),
            )
        )
    return tuple(output)


def calculate_classification_metrics(
    memberships: Sequence[ClassificationMembershipSnapshot],
    *,
    raw_by_key: dict[tuple[str, date], DailyBarRecord],
    daily_metrics_by_key: dict[tuple[str, date], DailyMetricRecord],
    market_cap_by_key: dict[tuple[str, date], MarketCapitalizationRecord],
    trade_dates: Sequence[date],
) -> tuple[ClassificationDailyMetricRecord, ...]:
    grouped: dict[tuple[str, object, str], list[ClassificationMembershipSnapshot]] = defaultdict(
        list
    )
    for snapshot in memberships:
        grouped[
            snapshot.namespace,
            snapshot.classification_type,
            snapshot.classification_code,
        ].append(snapshot)

    output: list[ClassificationDailyMetricRecord] = []
    for identity in sorted(grouped, key=lambda item: (item[0], str(item[1]), item[2])):
        snapshots = sorted(grouped[identity], key=lambda item: item.snapshot_date)
        index = 0
        active: ClassificationMembershipSnapshot | None = None
        for trade_date in trade_dates:
            while index < len(snapshots) and snapshots[index].snapshot_date <= trade_date:
                active = snapshots[index]
                index += 1
            if active is None:
                continue
            returns: list[Decimal] = []
            total_volume = 0
            total_amount = ZERO
            total_market_cap = ZERO
            market_cap_count = 0
            for symbol in active.members:
                bar = raw_by_key.get((symbol, trade_date))
                metric = daily_metrics_by_key.get((symbol, trade_date))
                market_cap = market_cap_by_key.get((symbol, trade_date))
                if bar is not None:
                    total_volume += bar.volume or 0
                    total_amount += bar.amount or ZERO
                if metric is not None and metric.total_return_1d is not None:
                    returns.append(metric.total_return_1d)
                if market_cap is not None:
                    total_market_cap += market_cap.total_market_cap
                    market_cap_count += 1
            advancing = sum(value > 0 for value in returns)
            declining = sum(value < 0 for value in returns)
            unchanged = len(returns) - advancing - declining
            output.append(
                ClassificationDailyMetricRecord(
                    namespace=active.namespace,
                    classification_type=active.classification_type,
                    classification_code=active.classification_code,
                    membership_snapshot_date=active.snapshot_date,
                    trade_date=trade_date,
                    member_count=len(active.members),
                    priced_member_count=len(returns),
                    advancing_count=advancing,
                    declining_count=declining,
                    unchanged_count=unchanged,
                    total_volume=total_volume,
                    total_amount=total_amount,
                    equal_weight_return=(sum(returns, ZERO) / len(returns) if returns else None),
                    total_market_cap=total_market_cap if market_cap_count else None,
                    market_cap_member_count=market_cap_count,
                )
            )
    return tuple(output)


def calculation_input_hash(inputs: DerivedCalculationInput) -> str:
    """Return a stable digest of every business input, excluding ingestion metadata."""
    payload = {
        "daily_bars": [
            _daily_bar_payload(item)
            for item in sorted(inputs.daily_bars, key=lambda item: (item.symbol, item.trade_date))
        ],
        "distributions": [
            _distribution_payload(item)
            for item in sorted(
                inputs.distributions, key=lambda item: (item.symbol, item.report_period)
            )
        ],
        "rights_issues": [
            _rights_payload(item)
            for item in sorted(
                inputs.rights_issues, key=lambda item: (item.symbol, item.record_date)
            )
        ],
        "share_capital": [
            _capital_payload(item)
            for item in sorted(
                inputs.share_capital, key=lambda item: (item.symbol, item.effective_date)
            )
        ],
        "memberships": [
            _membership_payload(item)
            for item in sorted(
                inputs.memberships,
                key=lambda item: (
                    item.namespace,
                    item.classification_type.value,
                    item.classification_code,
                    item.snapshot_date,
                ),
            )
        ],
    }
    encoded = dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return sha256(encoded.encode("utf-8")).hexdigest()


def _event_factors(
    bars: Sequence[DailyBarRecord],
    distributions: Sequence[DistributionRecord],
    rights_issues: Sequence[RightsIssueRecord],
) -> dict[date, Decimal]:
    first_bar_date = bars[0].trade_date
    last_bar_date = bars[-1].trade_date
    events: dict[date, dict[str, Decimal]] = defaultdict(
        lambda: {"cash": ZERO, "bonus": ZERO, "rights_ratio": ZERO, "rights_value": ZERO}
    )
    for distribution in distributions:
        if (
            distribution.status is not CorporateActionStatus.IMPLEMENTED
            or distribution.ex_date is None
            or not first_bar_date <= distribution.ex_date <= last_bar_date
        ):
            continue
        event = events[distribution.ex_date]
        event["cash"] += distribution.cash_dividend_per_share or ZERO
        event["bonus"] += (distribution.bonus_share_ratio or ZERO) + (
            distribution.transfer_share_ratio or ZERO
        )
    for rights_issue in rights_issues:
        if (
            rights_issue.ex_date is None
            or not first_bar_date <= rights_issue.ex_date <= last_bar_date
        ):
            continue
        event = events[rights_issue.ex_date]
        event["rights_ratio"] += rights_issue.rights_ratio
        event["rights_value"] += rights_issue.rights_ratio * rights_issue.rights_price

    by_date = {bar.trade_date: bar for bar in bars}
    factors: dict[date, Decimal] = {}
    with localcontext() as context:
        context.prec = 40
        for ex_date, event in events.items():
            bar = by_date.get(ex_date)
            if bar is None:
                raise CalculationInputError(
                    f"corporate action date has no Daily Bar: {ex_date.isoformat()}"
                )
            previous_close = bar.previous_close
            if previous_close is None or previous_close <= 0:
                raise CalculationInputError(
                    f"corporate action date has no positive previous_close: {ex_date.isoformat()}"
                )
            theoretical = (previous_close - event["cash"] + event["rights_value"]) / (
                ONE + event["bonus"] + event["rights_ratio"]
            )
            if theoretical <= 0:
                message = "corporate action produces a non-positive ex-right price"
                raise CalculationInputError(f"{message}: {ex_date.isoformat()}")
            factors[ex_date] = theoretical / previous_close
    return factors


def _validated_bars(bars: Sequence[DailyBarRecord]) -> tuple[DailyBarRecord, ...]:
    ordered = tuple(sorted(bars, key=lambda item: item.trade_date))
    symbols = {item.symbol for item in ordered}
    if len(symbols) > 1:
        raise CalculationInputError("one adjusted series cannot contain multiple symbols")
    dates = [item.trade_date for item in ordered]
    if len(dates) != len(set(dates)):
        raise CalculationInputError("adjusted series contains duplicate trade dates")
    return ordered


def _group_bars(records: Iterable[DailyBarRecord]) -> dict[str, tuple[DailyBarRecord, ...]]:
    grouped = _group_by_symbol(records)
    return {symbol: _validated_bars(items) for symbol, items in grouped.items()}


def _group_by_symbol[RecordT: _SymbolRecord](
    records: Iterable[RecordT],
) -> dict[str, tuple[RecordT, ...]]:
    grouped: dict[str, list[RecordT]] = defaultdict(list)
    for record in records:
        grouped[record.symbol].append(record)
    return {symbol: tuple(items) for symbol, items in grouped.items()}


def _multiply(value: Decimal | None, factor: Decimal) -> Decimal | None:
    return value * factor if value is not None else None


def _moving_average(values: Sequence[Decimal | None], window: int) -> Decimal | None:
    if len(values) < window:
        return None
    selected = values[-window:]
    if any(value is None for value in selected):
        return None
    return sum((value for value in selected if value is not None), ZERO) / window


def _text(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, Enum):
        return str(value.value)
    if isinstance(value, (date, Decimal)):
        return str(value)
    return str(value)


def _daily_bar_payload(item: DailyBarRecord) -> list[str | None]:
    return [
        _text(getattr(item, field))
        for field in (
            "symbol",
            "trade_date",
            "market",
            "open",
            "high",
            "low",
            "close",
            "previous_close",
            "volume",
            "amount",
            "trade_status",
            "is_st",
        )
    ]


def _distribution_payload(item: DistributionRecord) -> list[str | None]:
    return [
        _text(getattr(item, field))
        for field in (
            "symbol",
            "report_period",
            "announcement_date",
            "record_date",
            "ex_date",
            "cash_dividend_per_share",
            "bonus_share_ratio",
            "transfer_share_ratio",
            "status",
        )
    ]


def _rights_payload(item: RightsIssueRecord) -> list[str | None]:
    return [
        _text(getattr(item, field))
        for field in (
            "symbol",
            "record_date",
            "announcement_date",
            "ex_date",
            "payment_start_date",
            "payment_end_date",
            "listing_date",
            "rights_ratio",
            "rights_price",
            "base_shares",
            "proceeds",
        )
    ]


def _capital_payload(item: ShareCapitalRecord) -> list[str | None]:
    return [
        _text(getattr(item, field))
        for field in (
            "symbol",
            "effective_date",
            "total_shares",
            "restricted_shares",
            "circulating_shares",
            "listed_a_shares",
            "change_reason",
        )
    ]


def _membership_payload(item: ClassificationMembershipSnapshot) -> list[object]:
    return [
        item.namespace,
        item.classification_type.value,
        item.classification_code,
        item.snapshot_date.isoformat(),
        sorted(item.members),
    ]
