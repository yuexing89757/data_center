"""Missing certification cannot revive an older share-capital fact."""

from datetime import date
from unittest.mock import MagicMock

import pytest

from market_data_center.persistence.derived_postgres import PostgreSQLDerivedPersistence


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
        MagicMock(**{"mappings.return_value": rows}) for rows in ((), (), (), (older, newer), ())
    ]

    with pytest.raises(ValueError, match="quantity_units_unverified"):
        PostgreSQLDerivedPersistence(MagicMock())._load_inputs(
            connection, date(2026, 1, 1), date(2026, 2, 2)
        )
