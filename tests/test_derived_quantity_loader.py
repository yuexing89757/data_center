"""Missing certification cannot revive an older share-capital fact."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from market_data_center.persistence.derived_postgres import PostgreSQLDerivedPersistence


def test_daily_bar_queries_are_bounded_by_symbol_without_changing_admission() -> None:
    """One repeatable-read connection must serve every page, including the last one."""
    from market_data_center.persistence import derived_postgres as module

    connection = MagicMock()
    symbols = [f"SSE:{index:06}" for index in range(205)]
    pages = [symbols[:100], symbols[100:200], symbols[200:], []]
    seen = []

    def execute(statement, parameters):
        result = MagicMock()
        if statement is module._SELECT_DAILY_BAR_SYMBOLS:
            page = pages.pop(0)
            assert parameters["after_symbol"] == (seen[-1][-1] if seen else "")
            assert parameters["page_size"] == 100
            result.scalars.return_value = page
        elif statement is module._SELECT_DAILY_BARS:
            seen.append(parameters["symbols"])
            assert "audit.unit_safe_daily_bar" in str(statement)
            assert "cast(:symbols as text[])" in str(statement)
            result.mappings.return_value = ()
        else:
            result.mappings.return_value = ()
        return result

    connection.execute.side_effect = execute
    PostgreSQLDerivedPersistence(MagicMock())._load_inputs(
        connection, date(2026, 9, 23), date(2026, 10, 23)
    )
    assert [symbol for page in seen for symbol in page] == symbols
    assert not pages


def test_uncertified_capital_revision_blocks_older_certified_fallback() -> None:
    older = {
        "symbol": "SSE:600000",
        "effective_date": date(2026, 1, 1),
        "total_shares": 100,
        "restricted_shares": None,
        "circulating_shares": 100,
        "listed_a_shares": 100,
        "change_reason": None,
        "source_code": "akshare",
    }
    newer = {**older, "effective_date": date(2026, 2, 1), "total_shares": None}
    connection = MagicMock()
    connection.execute.side_effect = [
        MagicMock(**{"scalars.return_value": ()}),
        *[MagicMock(**{"mappings.return_value": rows}) for rows in ((), (), (older, newer), ())],
    ]

    with pytest.raises(ValueError, match="quantity_units_unverified"):
        PostgreSQLDerivedPersistence(MagicMock())._load_inputs(
            connection, date(2026, 1, 1), date(2026, 2, 2)
        )
