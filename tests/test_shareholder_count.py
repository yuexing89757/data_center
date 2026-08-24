from dataclasses import replace
from datetime import date

import pytest

from market_data_center.domain.shareholder_count import (
    ShareholderCountRecord,
    shareholder_count_revision_key,
    validate_shareholder_counts,
)


def _record() -> ShareholderCountRecord:
    values = {
        "symbol": "SSE:600000",
        "statistics_date": date(2026, 6, 30),
        "announcement_date": date(2026, 7, 15),
        "shareholder_count": 12_345,
    }
    return ShareholderCountRecord(
        **values,
        revision_key=shareholder_count_revision_key(**values),
        source_code="tushare",
    )


def test_revision_is_deterministic_and_valid_record_is_preserved() -> None:
    record = _record()

    assert validate_shareholder_counts((record,), known_symbols={record.symbol}) == (record,)
    assert record.revision_key == shareholder_count_revision_key(
        symbol=record.symbol,
        statistics_date=record.statistics_date,
        announcement_date=record.announcement_date,
        shareholder_count=record.shareholder_count,
    )


def test_nonpositive_shareholder_count_is_rejected() -> None:
    record = _record()

    with pytest.raises(ValueError, match="positive"):
        validate_shareholder_counts(
            (replace(record, shareholder_count=0),), known_symbols={record.symbol}
        )


def test_future_announcement_order_and_unknown_symbol_are_rejected() -> None:
    record = _record()

    with pytest.raises(ValueError, match="announcement precedes"):
        validate_shareholder_counts(
            (
                replace(
                    record,
                    statistics_date=date(2026, 7, 16),
                    announcement_date=date(2026, 7, 15),
                ),
            ),
            known_symbols={record.symbol},
        )
    with pytest.raises(ValueError, match="unknown"):
        validate_shareholder_counts((record,), known_symbols=set())


def test_source_hash_and_batch_uniqueness_are_enforced() -> None:
    record = _record()

    with pytest.raises(ValueError, match="source"):
        validate_shareholder_counts(
            (replace(record, source_code="akshare"),), known_symbols={record.symbol}
        )
    with pytest.raises(ValueError, match="revision key mismatch"):
        validate_shareholder_counts(
            (replace(record, revision_key="0" * 64),), known_symbols={record.symbol}
        )
    with pytest.raises(ValueError, match="duplicate"):
        validate_shareholder_counts((record, record), known_symbols={record.symbol})
