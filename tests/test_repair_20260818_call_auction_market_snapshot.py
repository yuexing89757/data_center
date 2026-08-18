from importlib.util import module_from_spec, spec_from_file_location
from json import dumps
from pathlib import Path
from sys import modules

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "repair_20260818_call_auction_market_snapshot.py"


def test_one_time_repair_script_is_checked_in() -> None:
    assert SCRIPT.is_file()


SPEC = spec_from_file_location("repair_20260818_call_auction_market_snapshot", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
repair = module_from_spec(SPEC)
modules[SPEC.name] = repair
SPEC.loader.exec_module(repair)


def _raw_row(**overrides: str) -> dict[str, str]:
    payload = {
        "market": "1",
        "code": "603089",
        "bid1": "11.8",
        "bid_vol1": "70656",
        "bid2": "0",
        "bid_vol2": "107432",
        "bid3": "0",
        "bid_vol3": "0",
        "bid4": "0",
        "bid_vol4": "0",
        "bid5": "0",
        "bid_vol5": "0",
        "ask1": "0",
        "ask_vol1": "0",
        "ask2": "0",
        "ask_vol2": "0",
        "ask3": "0",
        "ask_vol3": "0",
        "ask4": "0",
        "ask_vol4": "0",
        "ask5": "0",
        "ask_vol5": "0",
    }
    payload.update(overrides)
    return {
        "provider_raw_json": dumps(payload),
        "provider_schema_version": "pytdx_hq.security_quotes.v1",
        "worker_observed_at": "2026-08-18T01:26:18.815552+00:00",
    }


def test_normalize_raw_rows_preserves_volume_when_price_is_zero() -> None:
    records = repair.normalize_raw_rows((_raw_row(),))

    record = records["SSE:603089"]
    assert record.bid_prices == (repair.Decimal("11.8"), None, None, None, None)
    assert record.bid_volumes == (7065600, 10743200, None, None, None)
    assert record.ask_prices == (None, None, None, None, None)
    assert record.ask_volumes == (None, None, None, None, None)
    assert record.seal_amount == repair.Decimal("83374080.0")


def test_normalize_raw_rows_rejects_duplicate_symbol() -> None:
    with pytest.raises(repair.RepairError, match="duplicate Raw symbol"):
        repair.normalize_raw_rows((_raw_row(), _raw_row()))


def test_validate_exact_symbol_set_rejects_any_difference() -> None:
    records = repair.normalize_raw_rows((_raw_row(),))

    with pytest.raises(repair.RepairError, match="symbol set mismatch"):
        repair.validate_exact_symbol_set(records, {"SSE:600000"})
