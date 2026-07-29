from datetime import date
from decimal import Decimal

from market_data_center.domain import (
    CorporateActionStatus,
    DistributionRecord,
    ShareCapitalRecord,
    validate_capital,
)


def _share_capital(*, total_shares: int = 1_000) -> ShareCapitalRecord:
    return ShareCapitalRecord(
        symbol="SSE:600000",
        effective_date=date(2024, 1, 1),
        total_shares=total_shares,
        restricted_shares=100,
        circulating_shares=900,
        listed_a_shares=900,
        change_reason="test",
        source_code="akshare",
    )


def test_capital_validation_deduplicates_identical_natural_keys() -> None:
    record = _share_capital()

    result = validate_capital([record, record], known_symbols={record.symbol})

    assert result.accepted == (record,)
    assert result.findings == ()
    assert result.rejected_rows == 0


def test_capital_validation_rejects_conflicting_natural_keys() -> None:
    result = validate_capital(
        [_share_capital(), _share_capital(total_shares=2_000)],
        known_symbols={"SSE:600000"},
    )

    assert result.accepted == ()
    assert result.findings[0].rule_code == "capital.conflicting_duplicate"
    assert result.rejected_rows == 2


def test_distribution_requires_positive_allocation() -> None:
    record = DistributionRecord(
        symbol="SSE:600000",
        report_period=date(2023, 12, 31),
        announcement_date=None,
        record_date=None,
        ex_date=None,
        cash_dividend_per_share=Decimal("0.1"),
        bonus_share_ratio=None,
        transfer_share_ratio=None,
        status=CorporateActionStatus.PLANNED,
        source_code="akshare",
    )

    assert record.cash_dividend_per_share == Decimal("0.1")
