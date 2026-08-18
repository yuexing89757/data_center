from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest

from market_data_center.domain.dragon_tiger import (
    DragonTigerActivitySide,
    DragonTigerEvent,
    DragonTigerEventStatus,
    DragonTigerNormalizationStatus,
    DragonTigerReason,
    DragonTigerSeat,
    DragonTigerSeatActivity,
    DragonTigerSeatType,
    DragonTigerSnapshotBatch,
    DragonTigerSnapshotStatus,
    DragonTigerSourceObservation,
    calculate_dragon_tiger_summary,
    validate_dragon_tiger_batch,
)

TRADE_DATE = date(2026, 8, 18)
NOW = datetime(2026, 8, 18, 10, tzinfo=UTC)
EVENT = DragonTigerEvent(
    symbol="SSE:600000",
    trade_date=TRADE_DATE,
    historical_name="浦发银行",
    market="CN_A_SHARE",
    close=Decimal("11.0000"),
    change_percent=Decimal("10.0000000000"),
    turnover_amount_cny=Decimal("123456789.1200"),
    turnover_rate_percent=Decimal("2.5000000000"),
    status=DragonTigerEventStatus.OBSERVED,
    source_event_key="20260818:600000",
)
OBSERVATION = DragonTigerSourceObservation(
    source_event_key=EVENT.source_event_key,
    symbol=EVENT.symbol,
    trade_date=TRADE_DATE,
    observed_at=NOW,
    source_name="浦发银行",
)
INSTITUTION = DragonTigerSeat(
    identity_key="institution:generic",
    canonical_name="机构专用",
    seat_type=DragonTigerSeatType.INSTITUTION,
    valid_from=TRADE_DATE,
    source_name="机构专用",
    normalization_status=DragonTigerNormalizationStatus.MATCHED,
)
BRANCH = DragonTigerSeat(
    identity_key="broker:example:shanghai",
    canonical_name="示例证券上海营业部",
    seat_type=DragonTigerSeatType.BROKER_BRANCH,
    broker_name="示例证券",
    branch_name="上海营业部",
    region="上海",
    valid_from=TRADE_DATE,
    source_name="示例证券股份有限公司上海营业部",
    normalization_status=DragonTigerNormalizationStatus.PROVISIONAL,
)
ACTIVITIES = (
    DragonTigerSeatActivity(
        EVENT.symbol,
        TRADE_DATE,
        INSTITUTION.identity_key,
        DragonTigerActivitySide.BUY,
        Decimal("100.0000"),
        Decimal("0"),
        Decimal("100.0000"),
        INSTITUTION.source_name,
        0,
        buy_rank=1,
    ),
    DragonTigerSeatActivity(
        EVENT.symbol,
        TRADE_DATE,
        BRANCH.identity_key,
        DragonTigerActivitySide.BOTH,
        Decimal("50.0000"),
        Decimal("25.0000"),
        Decimal("25.0000"),
        BRANCH.source_name,
        1,
        buy_rank=2,
        sell_rank=1,
    ),
)


def _batch() -> DragonTigerSnapshotBatch:
    summary = calculate_dragon_tiger_summary(
        EVENT,
        ACTIVITIES,
        {INSTITUTION.identity_key: INSTITUTION, BRANCH.identity_key: BRANCH},
        calculated_at=NOW,
    )
    return DragonTigerSnapshotBatch(
        trade_date=TRADE_DATE,
        observed_at=NOW,
        status=DragonTigerSnapshotStatus.COMPLETE,
        input_hash="a" * 64,
        content_hash="b" * 64,
        observations=(OBSERVATION,),
        events=(EVENT,),
        reasons=(
            DragonTigerReason(
                EVENT.symbol,
                TRADE_DATE,
                "daily_deviation_top3",
                "日涨幅偏离值达规定阈值",
                "有价格涨跌幅限制的日涨幅偏离值达到7%的前五只证券",
                0,
            ),
        ),
        seats=(INSTITUTION, BRANCH),
        activities=ACTIVITIES,
        summaries=(summary,),
    )


def test_objective_summary_is_decimal_recomputable() -> None:
    summary = _batch().summaries[0]
    assert summary.total_buy_amount_cny == Decimal("150.0000")
    assert summary.total_sell_amount_cny == Decimal("25.0000")
    assert summary.total_net_amount_cny == Decimal("125.0000")
    assert summary.institution_buy_amount_cny == Decimal("100.0000")
    assert summary.top5_buy_concentration_ratio == Decimal("1.000000000000")
    assert summary.top5_sell_concentration_ratio == Decimal("1.000000000000")


def test_batch_validation_accepts_coherent_snapshot() -> None:
    result = validate_dragon_tiger_batch(
        _batch(),
        known_symbols={EVENT.symbol},
        known_trading_dates={TRADE_DATE},
        historical_names={EVENT.natural_key: EVENT.historical_name},
        unadjusted_closes={EVENT.natural_key: EVENT.close},
        previous_closes={EVENT.natural_key: Decimal("10")},
    )
    assert result.accepted is True
    assert result.findings == ()


def test_batch_validation_rejects_orphan_and_summary_mismatch() -> None:
    batch = _batch()
    orphan = replace(batch.reasons[0], event_symbol="SZSE:000001")
    wrong_summary = replace(
        batch.summaries[0],
        total_buy_amount_cny=Decimal("151"),
        total_net_amount_cny=Decimal("126"),
        top5_buy_concentration_ratio=Decimal("0.993377483444"),
    )
    result = validate_dragon_tiger_batch(
        replace(batch, reasons=(orphan,), summaries=(wrong_summary,)),
        known_symbols={EVENT.symbol},
        known_trading_dates={TRADE_DATE},
        historical_names={EVENT.natural_key: EVENT.historical_name},
        unadjusted_closes={EVENT.natural_key: EVENT.close},
        previous_closes={EVENT.natural_key: Decimal("10")},
    )
    assert result.accepted is False
    assert {finding.rule_code for finding in result.findings} == {
        "dragon_tiger.orphan_reason",
        "dragon_tiger.summary_mismatch",
        "dragon_tiger.complete_event_missing_reason",
    }


def test_batch_validation_requires_historical_name_and_unadjusted_daily_bar() -> None:
    result = validate_dragon_tiger_batch(
        _batch(),
        known_symbols={EVENT.symbol},
        known_trading_dates={TRADE_DATE},
        historical_names={},
        unadjusted_closes={},
        previous_closes={},
    )
    assert {
        "dragon_tiger.missing_historical_name",
        "dragon_tiger.missing_daily_close",
        "dragon_tiger.missing_previous_close",
    } <= {finding.rule_code for finding in result.findings}


def test_partial_snapshot_requires_visible_reason() -> None:
    with pytest.raises(ValueError, match="partial snapshot requires"):
        replace(_batch(), status=DragonTigerSnapshotStatus.PARTIAL)
    partial = replace(
        _batch(),
        status=DragonTigerSnapshotStatus.PARTIAL,
        partial_reasons=("source seat rows were truncated",),
    )
    assert partial.partial_reasons


def test_activity_requires_exact_net_and_objective_side() -> None:
    with pytest.raises(ValueError, match="net_amount_cny"):
        replace(ACTIVITIES[0], net_amount_cny=Decimal("99"))
    with pytest.raises(ValueError, match="buy-only"):
        replace(ACTIVITIES[0], sell_amount_cny=Decimal("1"), net_amount_cny=Decimal("99"))


def test_reason_source_numeric_value_requires_unit() -> None:
    with pytest.raises(ValueError, match="value and unit"):
        DragonTigerReason(
            EVENT.symbol,
            TRADE_DATE,
            "example_reason",
            "示例原因",
            "来源原文",
            0,
            source_numeric_value=Decimal("7"),
        )
