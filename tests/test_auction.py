from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from uuid import uuid4

from market_data_center.domain.auction import (
    AuctionPhase,
    AuctionQuoteSample,
    QuoteSemantics,
    auction_phase,
    auction_window,
    calculate_auction_quote_metric,
)
from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market


def _levels(side: str) -> tuple[OrderBookLevel, ...]:
    start = Decimal("10.00") if side == "bid" else Decimal("10.01")
    direction = Decimal("-0.01") if side == "bid" else Decimal("0.01")
    return tuple(OrderBookLevel(i, start + direction * (i - 1), i * 100) for i in range(1, 6))


def test_auction_window_has_121_five_second_samples_and_exact_phase_boundaries() -> None:
    start, end = auction_window(date(2026, 8, 3))

    assert int((end - start).total_seconds() // 5) + 1 == 121
    assert auction_phase(start) is AuctionPhase.CANCELLABLE
    assert auction_phase(start + timedelta(minutes=5)) is AuctionPhase.NON_CANCELLABLE
    assert auction_phase(end) is AuctionPhase.FINAL_MATCH


def test_unverified_auction_indicative_quote_does_not_publish_book_metrics() -> None:
    scheduled, _ = auction_window(date(2026, 8, 3))
    quote = FiveLevelQuoteSnapshotRecord(
        "SSE:600000",
        Market.CN_A_SHARE,
        scheduled,
        None,
        QuoteStatus.TRADING,
        Decimal("10.00"),
        Decimal("9.50"),
        Decimal("10.00"),
        Decimal("10.00"),
        Decimal("10.00"),
        1000,
        Decimal("10000"),
        _levels("bid"),
        _levels("ask"),
        "pytdx_hq",
    )
    sample = AuctionQuoteSample(
        uuid4(),
        uuid4(),
        0,
        scheduled,
        scheduled,
        AuctionPhase.CANCELLABLE,
        QuoteSemantics.AUCTION_INDICATIVE,
        quote,
    )

    metric = calculate_auction_quote_metric(
        sample,
        upper_limit=Decimal("10.00"),
        order_book_semantics_verified=False,
        calculated_at=datetime(2026, 8, 3, 1, 15, tzinfo=UTC),
        price_limit_rule_version="cn-mainboard-v1",
    )

    assert metric.spread is None
    assert metric.bid_depth_5 is None
    assert metric.seal_amount is None
