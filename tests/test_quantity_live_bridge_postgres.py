"""The existing narrow API write bridge certifies only its newly adapted facts."""

# ruff: noqa: F811 - imported fixtures are injected by pytest.

from datetime import datetime
from json import dumps
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import text
from test_postgres_integration import (  # noqa: F401
    database_engine,
    empty_database_url,
    migrated_database_url,
)
from test_quantity_unit_gates_postgres import _facts

pytestmark = pytest.mark.integration


def test_live_bridge_certificate_is_atomic_and_reuse_does_not_certify_legacy(database_engine):
    _facts(database_engine)
    today = datetime.now(ZoneInfo("Asia/Shanghai"))
    ingestion_id, raw_id = uuid4(), uuid4()
    parameters = {
        "id": ingestion_id,
        "raw": raw_id,
        "day": today.date(),
        "fetched": today,
        "input": "d" * 64,
        "hash": "b" * 64,
        "path": f"eastmoney/call_auction_indicative_detail/year={today:%Y}/"
        f"month={today:%m}/day={today:%d}/{ingestion_id}.jsonl",
        "records": dumps(
            [
                {
                    "source_sequence": 0,
                    "indicative_price": "10.1",
                    "displayed_volume_shares": 100,
                    "observed_at": today.replace(hour=9, minute=20, second=0).isoformat(),
                    "source_display_classification": "unknown",
                }
            ]
        ),
    }
    statement = text("""
select api_v1.persist_call_auction_indicative_details(
 :id,:raw,'SSE:600000',:day,:fetched,:input,:path,:hash,1000,1,cast(:records as jsonb))
""")
    with database_engine.begin() as connection:
        connection.execute(text("set local role market_data_api"))
        result = connection.execute(statement, parameters).scalar_one()
        assert result["outcome"] == "created"
        connection.execute(text("reset role"))
        row = connection.execute(
            text("""
select displayed_volume_shares,quantity_quality->>'status' as status
from audit.unit_safe_call_auction_indicative_detail where ingestion_id=:id
"""),
            {"id": ingestion_id},
        ).one()
        assert row == (100, "certified")
        # Reusing a legacy snapshot never retroactively proves its old adapter.
        connection.execute(
            text("delete from audit.quantity_unit_certification where ingestion_id=:id"),
            {"id": ingestion_id},
        )
        connection.execute(text("set local role market_data_api"))
        assert connection.execute(statement, parameters).scalar_one()["outcome"] == "reused"
        connection.execute(text("reset role"))
        assert (
            connection.execute(
                text("""
select count(*) from audit.quantity_unit_certification where ingestion_id=:id
"""),
                {"id": ingestion_id},
            ).scalar_one()
            == 0
        )
