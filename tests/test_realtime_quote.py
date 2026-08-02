from datetime import UTC, datetime, timedelta
from decimal import Decimal

import pytest

from market_data_center.domain import (
    FiveLevelQuoteSnapshotRecord,
    Market,
    OrderBookLevel,
    QuoteStatus,
    calculate_five_level_quote_metric,
    validate_realtime_quotes,
)

OBSERVED_AT = datetime(2026, 8, 3, 1, 30, tzinfo=UTC)


def _levels(*prices: str, volume: int = 100) -> tuple[OrderBookLevel, ...]:
    return tuple(
        OrderBookLevel(level, Decimal(price), volume) for level, price in enumerate(prices, start=1)
    )


def _quote(**changes: object) -> FiveLevelQuoteSnapshotRecord:
    values: dict[str, object] = {
        "symbol": "SSE:600000",
        "market": Market.CN_A_SHARE,
        "observed_at": OBSERVED_AT,
        "source_timestamp": OBSERVED_AT - timedelta(seconds=1),
        "quote_status": QuoteStatus.TRADING,
        "last_price": Decimal("9.51"),
        "previous_close": Decimal("9.71"),
        "open": Decimal("9.59"),
        "high": Decimal("9.59"),
        "low": Decimal("9.28"),
        "cumulative_volume": 138_263_700,
        "cumulative_amount": Decimal("1299502592.00"),
        "bid_levels": _levels("9.51", "9.50", "9.49", "9.48", "9.47"),
        "ask_levels": _levels("9.52", "9.53", "9.54", "9.55", "9.56", volume=200),
        "source_code": "pytdx_hq",
    }
    values.update(changes)
    return FiveLevelQuoteSnapshotRecord(**values)  # type: ignore[arg-type]


def test_quote_requires_decimal_and_ordered_contiguous_levels() -> None:
    with pytest.raises(TypeError, match="Decimal"):
        _quote(last_price=9.51)

    broken = (
        OrderBookLevel(1, Decimal("9.51"), 100),
        OrderBookLevel(2, None, None),
        OrderBookLevel(3, Decimal("9.49"), 100),
        OrderBookLevel(4, None, None),
        OrderBookLevel(5, None, None),
    )
    with pytest.raises(ValueError, match="contiguous"):
        _quote(bid_levels=broken)


def test_quote_requires_utc_observation_time() -> None:
    with pytest.raises(ValueError, match="UTC"):
        _quote(observed_at=datetime(2026, 8, 3, 9, 30))


def test_metrics_require_complete_five_level_depth() -> None:
    metric = calculate_five_level_quote_metric(_quote())

    assert metric.spread == Decimal("0.01")
    assert metric.mid_price == Decimal("9.515")
    assert metric.bid_depth_5 == 500
    assert metric.ask_depth_5 == 1_000
    assert metric.imbalance_5 == Decimal("-0.3333333333333333333333333333")

    incomplete_asks = (
        *_levels("9.52", "9.53", "9.54", "9.55"),
        OrderBookLevel(5, None, None),
    )
    incomplete = calculate_five_level_quote_metric(_quote(ask_levels=incomplete_asks))
    assert incomplete.ask_depth_5 is None
    assert incomplete.imbalance_5 is None


def test_validation_deduplicates_and_reports_provider_quality() -> None:
    record = _quote(source_timestamp=None)
    result = validate_realtime_quotes(
        [record, record],
        known_symbols={record.symbol},
        known_stock_symbols={record.symbol},
        now=OBSERVED_AT + timedelta(seconds=2),
    )

    assert result.accepted == (record,)
    assert result.rejected_rows == 0
    assert {finding.rule_code for finding in result.findings} == {
        "realtime_quote.missing_source_timestamp",
        "realtime_quote.lot_precision",
    }


def test_validation_rejects_conflict_unknown_and_future_observation() -> None:
    record = _quote()
    changed = _quote(last_price=Decimal("9.50"))
    result = validate_realtime_quotes(
        [record, changed],
        known_symbols=set(),
        known_stock_symbols=set(),
        now=OBSERVED_AT - timedelta(minutes=1),
    )

    assert result.accepted == ()
    assert result.rejected_rows == 2
    assert {finding.rule_code for finding in result.findings if finding.blocks_core_write} == {
        "realtime_quote.conflicting_snapshot",
        "realtime_quote.unknown_symbol",
        "realtime_quote.future_observation",
    }


def test_crossed_book_is_a_nonblocking_quality_finding() -> None:
    crossed = _quote(
        bid_levels=_levels("9.53", "9.50", "9.49", "9.48", "9.47"),
    )
    result = validate_realtime_quotes(
        [crossed],
        known_symbols={crossed.symbol},
        known_stock_symbols={crossed.symbol},
        now=OBSERVED_AT,
    )

    assert result.accepted == (crossed,)
    assert "realtime_quote.crossed_book" in {finding.rule_code for finding in result.findings}
