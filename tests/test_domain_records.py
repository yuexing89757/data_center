from datetime import date
from decimal import Decimal
from uuid import uuid4

import pytest

from market_data_center.domain import (
    DailyBarRecord,
    Exchange,
    IngestionEnvelope,
    Market,
    SecurityRecord,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)


def test_security_symbol_must_match_exchange_and_code() -> None:
    with pytest.raises(ValueError, match="symbol must equal SSE:600000"):
        SecurityRecord(
            symbol="SZSE:600000",
            code="600000",
            exchange=Exchange.SSE,
            name="浦发银行",
            security_type=SecurityType.STOCK,
            status=SecurityStatus.LISTED,
            ipo_date=date(1999, 11, 10),
            delisting_date=None,
            source_code="baostock",
        )


def test_daily_bar_rejects_invalid_ohlc() -> None:
    with pytest.raises(ValueError, match="open must be within"):
        DailyBarRecord(
            symbol="SSE:600000",
            trade_date=date(2026, 7, 24),
            market=Market.CN_A_SHARE,
            open=Decimal("12.00"),
            high=Decimal("11.00"),
            low=Decimal("10.00"),
            close=Decimal("10.50"),
            previous_close=Decimal("10.20"),
            volume=100,
            amount=Decimal("1050.00"),
            trade_status=TradeStatus.TRADING,
            is_st=False,
            source_code="baostock",
        )


def test_pipeline_adds_ingestion_id_outside_provider_record() -> None:
    record = SecurityRecord(
        symbol="SSE:600000",
        code="600000",
        exchange=Exchange.SSE,
        name="浦发银行",
        security_type=SecurityType.STOCK,
        status=SecurityStatus.LISTED,
        ipo_date=date(1999, 11, 10),
        delisting_date=None,
        source_code="baostock",
    )
    ingestion_id = uuid4()

    envelope = IngestionEnvelope(ingestion_id=ingestion_id, record=record)

    assert envelope.ingestion_id == ingestion_id
    assert not hasattr(record, "ingestion_id")
