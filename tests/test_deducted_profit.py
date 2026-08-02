from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

import pytest

from market_data_center.domain.deducted_profit import (
    DeductedProfitRecord,
    deducted_profit_revision_key,
    validate_deducted_profits,
)
from market_data_center.providers.tushare import TushareProvider


class ProfitClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Mapping[str, str]]] = []

    def query(
        self, api_name: str, *, params: Mapping[str, str], fields: Sequence[str]
    ) -> Sequence[Mapping[str, object]]:
        del fields
        self.calls.append((api_name, params))
        if api_name == "disclosure_date":
            if params["end_date"] == "20250630":
                return (
                    {
                        "ts_code": "600000.SH",
                        "ann_date": "20250701",
                        "end_date": "20250630",
                        "actual_date": "20250802",
                        "modify_date": None,
                    },
                )
            return ()
        assert api_name == "fina_indicator"
        return (
            {
                "ts_code": "600000.SH",
                "ann_date": "20250802",
                "end_date": "20250630",
                "profit_dedt": Decimal("123456.78"),
                "q_dtprofit": Decimal("34567.89"),
                "update_flag": "1",
            },
        )


def test_tushare_deducted_profit_discovers_changes_without_vip_or_history_scan() -> None:
    client = ProfitClient()
    batch = TushareProvider(client).fetch_deducted_profit_updates(date(2025, 8, 2))
    records = batch.records

    assert len(records) == 1
    assert records[0].symbol == "SSE:600000"
    assert records[0].cumulative_deducted_profit == Decimal("123456.78")
    assert records[0].actual_announcement_date == date(2025, 8, 2)
    assert [call[0] for call in client.calls].count("disclosure_date") == 5
    assert [call[0] for call in client.calls].count("fina_indicator") == 1
    assert all(call[0] != "fina_indicator_vip" for call in client.calls)


def test_deducted_profit_validation_preserves_zero_and_rejects_future_knowledge() -> None:
    values = {
        "symbol": "SSE:600000",
        "report_period": date(2025, 6, 30),
        "announcement_date": date(2025, 8, 2),
        "actual_announcement_date": date(2025, 8, 2),
        "cumulative_deducted_profit": Decimal("0"),
        "quarterly_deducted_profit": None,
        "update_flag": "0",
    }
    record = DeductedProfitRecord(
        **values,
        revision_key=deducted_profit_revision_key(**values),
        source_code="tushare",
    )

    assert validate_deducted_profits((record,), known_symbols={record.symbol}) == (record,)
    invalid = DeductedProfitRecord(
        **{**values, "announcement_date": date(2025, 1, 1)},
        revision_key="invalid",
        source_code="tushare",
    )
    with pytest.raises(ValueError, match="announcement precedes"):
        validate_deducted_profits((invalid,), known_symbols={invalid.symbol})
