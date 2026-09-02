from dataclasses import replace
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    RegulationDirection,
    RegulationEventRecord,
    RegulationEventType,
    RegulationResetLevel,
    RegulationRule,
    RegulationRuleKind,
    RegulationRuleLevel,
    RegulationSegment,
    regulation_event_natural_key,
    validate_regulation_rules,
)

RULE_SET_VERSION = "cn-a-share-regulation-2026-07-06.v1"
EFFECTIVE_DATE = date(2026, 7, 6)
SHANGHAI = ZoneInfo("Asia/Shanghai")


def _deviation_rule(**changes: object) -> RegulationRule:
    values: dict[str, object] = {
        "rule_code": "SSE_MAIN_ABNORMAL_3D_DEV_UP",
        "exchange": Exchange.SSE,
        "segment": RegulationSegment.SSE_MAIN,
        "level": RegulationRuleLevel.ABNORMAL,
        "kind": RegulationRuleKind.CUMULATIVE_DEVIATION,
        "direction": RegulationDirection.UP,
        "window_days": 3,
        "threshold_pct": Decimal("20"),
        "comparison_window_days": None,
        "ratio_threshold": None,
        "secondary_threshold_pct": None,
        "count_window_days": None,
        "required_count": None,
        "counted_event_kind": None,
        "reset_level": RegulationResetLevel.ABNORMAL,
        "benchmark_symbol": "SSE:000002",
        "rule_set_version": RULE_SET_VERSION,
        "effective_date": EFFECTIVE_DATE,
        "expire_date": None,
        "source_document": "上海证券交易所交易规则（2026年修订）",  # noqa: RUF001
        "source_clause": "5.4.2(1)",
        "source_url": "https://www.sse.com.cn/official-rule",
        "enabled": True,
    }
    values.update(changes)
    return RegulationRule(**values)  # type: ignore[arg-type]


def _turnover_rule() -> RegulationRule:
    return _deviation_rule(
        rule_code="SSE_MAIN_ABNORMAL_TURNOVER",
        kind=RegulationRuleKind.TURNOVER_COMPOSITE,
        direction=RegulationDirection.NONE,
        window_days=3,
        threshold_pct=None,
        comparison_window_days=5,
        ratio_threshold=Decimal("30"),
        secondary_threshold_pct=Decimal("20"),
        benchmark_symbol=None,
        source_clause="5.4.2(2)",
    )


def _count_rule() -> RegulationRule:
    return _deviation_rule(
        rule_code="SSE_MAIN_SERIOUS_10D_COUNT_UP",
        level=RegulationRuleLevel.SERIOUS_ABNORMAL,
        kind=RegulationRuleKind.EVENT_COUNT,
        window_days=None,
        threshold_pct=None,
        count_window_days=10,
        required_count=4,
        counted_event_kind="PRICE_DEVIATION_ABNORMAL",
        reset_level=RegulationResetLevel.SERIOUS_ABNORMAL,
        benchmark_symbol=None,
        source_clause="5.4.3(1)",
    )


def test_regulation_rule_accepts_each_typed_formula_family() -> None:
    deviation = _deviation_rule()
    turnover = _turnover_rule()
    count = _count_rule()

    assert deviation.threshold_pct == Decimal("20")
    assert turnover.ratio_threshold == Decimal("30")
    assert count.required_count == 4


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"rule_code": " "}, "must not be blank"),
        ({"threshold_pct": Decimal("-20")}, "UP threshold must be positive"),
        (
            {
                "direction": RegulationDirection.DOWN,
                "threshold_pct": Decimal("20"),
            },
            "DOWN threshold must be negative",
        ),
        ({"benchmark_symbol": None}, "benchmark symbol"),
        ({"ratio_threshold": Decimal("30")}, "cumulative-deviation"),
        ({"effective_date": date(2026, 7, 5)}, "2026-07-06"),
        ({"expire_date": date(2026, 7, 5)}, "expire date"),
    ],
)
def test_deviation_rule_rejects_invalid_or_cross_kind_fields(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _deviation_rule(**changes)


def test_turnover_and_event_count_require_exact_kind_specific_parameters() -> None:
    with pytest.raises(ValueError, match="turnover-composite"):
        replace(_turnover_rule(), comparison_window_days=None)
    with pytest.raises(ValueError, match=r"Direction\.NONE"):
        replace(_turnover_rule(), direction=RegulationDirection.UP)
    with pytest.raises(ValueError, match="event-count"):
        replace(_count_rule(), required_count=None)
    with pytest.raises(ValueError, match=r"Direction\.NONE"):
        replace(_count_rule(), direction=RegulationDirection.NONE)


def test_validate_regulation_rules_selects_one_consistent_active_rule_set() -> None:
    rules = (_deviation_rule(), _turnover_rule(), _count_rule())

    assert validate_regulation_rules(rules, date(2026, 9, 2)) == rules


def test_validate_regulation_rules_rejects_duplicates_mixed_versions_and_no_coverage() -> None:
    rule = _deviation_rule()
    with pytest.raises(ValueError, match="duplicate rule code"):
        validate_regulation_rules((rule, rule), date(2026, 9, 2))
    with pytest.raises(ValueError, match="rule-set version"):
        validate_regulation_rules(
            (rule, replace(_turnover_rule(), rule_set_version="other.v1")),
            date(2026, 9, 2),
        )
    with pytest.raises(ValueError, match="no enabled regulation rule"):
        validate_regulation_rules((replace(rule, enabled=False),), date(2026, 9, 2))


def test_validate_regulation_rules_rejects_duplicate_active_dimension() -> None:
    duplicate_dimension = replace(
        _deviation_rule(),
        rule_code="SSE_MAIN_ABNORMAL_3D_DEV_UP_DUPLICATE",
        source_clause="5.4.2(1)-duplicate",
    )
    with pytest.raises(ValueError, match="active rule dimension"):
        validate_regulation_rules((_deviation_rule(), duplicate_dimension), date(2026, 9, 2))


def test_validate_regulation_rules_allows_distinct_official_windows() -> None:
    ten_day = _deviation_rule(
        rule_code="SSE_MAIN_SERIOUS_10D_DEV_UP",
        level=RegulationRuleLevel.SERIOUS_ABNORMAL,
        window_days=10,
        threshold_pct=Decimal("100"),
        reset_level=RegulationResetLevel.SERIOUS_ABNORMAL,
        source_clause="5.4.3(2)",
    )
    thirty_day = replace(
        ten_day,
        rule_code="SSE_MAIN_SERIOUS_30D_DEV_UP",
        window_days=30,
        threshold_pct=Decimal("200"),
        source_clause="5.4.3(3)",
    )

    assert validate_regulation_rules((ten_day, thirty_day), date(2026, 9, 2)) == (
        ten_day,
        thirty_day,
    )


def _event(**changes: object) -> RegulationEventRecord:
    values: dict[str, object] = {
        "symbol": "SSE:600000",
        "exchange": Exchange.SSE,
        "segment": RegulationSegment.SSE_MAIN,
        "event_type": RegulationEventType.ABNORMAL_VOLATILITY,
        "event_level": RegulationRuleLevel.ABNORMAL,
        "direction": RegulationDirection.UP,
        "period_start_date": date(2026, 8, 31),
        "period_end_date": date(2026, 9, 2),
        "published_at": datetime(2026, 9, 2, 19, 30, tzinfo=SHANGHAI),
        "effective_reset_date": date(2026, 9, 3),
        "source_event_id": "SSE-600000-20260902",
        "source_title": "股票交易异常波动公告",
        "source_url": "https://www.sse.com.cn/disclosure/event.pdf",
        "source_content_hash": "a" * 64,
        "source_code": "sse_official",
        "explicit_rule_codes": ("SSE_MAIN_ABNORMAL_3D_DEV_UP",),
        "observed_at": datetime(2026, 9, 2, 20, 0, tzinfo=SHANGHAI),
    }
    values.update(changes)
    return RegulationEventRecord(**values)  # type: ignore[arg-type]


def test_regulation_event_is_immutable_and_has_a_stable_source_natural_key() -> None:
    event = _event()

    assert regulation_event_natural_key(event) == (
        "sse_official",
        "SSE-600000-20260902",
    )
    with pytest.raises(AttributeError):
        event.source_title = "changed"  # type: ignore[misc]


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"period_start_date": date(2026, 9, 3)}, "event period"),
        ({"published_at": datetime(2026, 9, 2, 19, 30)}, "timezone-aware"),
        ({"observed_at": datetime(2026, 9, 2, 20, 0)}, "timezone-aware"),
        ({"source_content_hash": "not-a-hash"}, "SHA-256"),
        ({"source_event_id": " "}, "must not be blank"),
        ({"symbol": "600000.SH"}, "standard symbol"),
        ({"direction": RegulationDirection.NONE}, "event direction"),
    ],
)
def test_regulation_event_rejects_invalid_identity_or_time(
    changes: dict[str, object], message: str
) -> None:
    with pytest.raises(ValueError, match=message):
        _event(**changes)


def test_regulation_event_allows_unknown_direction_without_fabricating_one() -> None:
    event = _event(direction=None)

    assert event.direction is None
