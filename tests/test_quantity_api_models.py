from datetime import date
from decimal import Decimal

from market_data_center.public_api.models import DailyBarItem
from market_data_center.public_api.openapi_zh import localize_openapi
from market_data_center.public_api.technical_factors import StockTechnicalFactorItem

QUALITY = {
    "unit_contract_version": "stock-quantity.v1",
    "status": "unverified",
    "reason": "quantity_units_unverified",
    "field_groups": {
        "daily_bar.volume": {"status": "unverified", "reason": "quantity_units_unverified"}
    },
}


def test_openapi_quantity_fields_identify_stock_business_and_unit() -> None:
    schema = {
        "paths": {},
        "components": {
            "schemas": {
                "CallAuctionMarketSnapshotItem": {
                    "properties": {
                        "bid1_volume": {},
                        "cumulative_volume": {},
                        "cumulative_amount": {},
                    }
                },
                "SourceBarItem": {"properties": {"volume_shares": {}, "amount_cny": {}}},
            }
        },
    }
    schemas = localize_openapi(schema)["components"]["schemas"]
    volume = schemas["CallAuctionMarketSnapshotItem"]["properties"]["bid1_volume"]
    assert volume["x-unit"] == "share"
    assert volume["x-business"] == "stock_order_book"
    assert volume["examples"] == [100]
    assert schemas["SourceBarItem"]["properties"]["volume_shares"]["x-unit"] == "share"
    assert schemas["SourceBarItem"]["properties"]["amount_cny"]["x-unit"] == "CNY"


def test_daily_bar_retains_machine_readable_quantity_isolation() -> None:
    item = DailyBarItem.model_validate(
        {
            "symbol": "SSE:600000",
            "trade_date": "2026-10-22",
            "open": "10",
            "high": "11",
            "low": "9",
            "close": "10",
            "previous_close": "10",
            "volume": None,
            "amount": "100",
            "trade_status": "trading",
            "is_st": False,
            "quantity_quality": QUALITY,
        }
    )
    assert item.volume is None
    assert item.close == Decimal(10)
    assert item.model_dump(mode="json")["quantity_quality"] == QUALITY


def test_factor_response_preserves_original_catalog_and_isolated_leaves() -> None:
    item = StockTechnicalFactorItem.model_validate(
        {
            "symbol": "SSE:600000",
            "code": "600000",
            "name": "synthetic",
            "trade_date": date(2026, 10, 22),
            "source_code": "tushare",
            "factor_catalog_version": "stock-technical-factor.v2",
            "source_factor_catalog_version": "stock-technical-factor.v1",
            "unadjusted": {"obv": None},
            "forward_adjusted": {"obv": None},
            "backward_adjusted": {"obv": None},
            "common": {},
            "quantity_quality": QUALITY,
        }
    )
    assert item.model_dump(mode="json")["source_factor_catalog_version"] == (
        "stock-technical-factor.v1"
    )
    assert item.unadjusted["obv"] is None
