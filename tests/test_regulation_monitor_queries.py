from datetime import date
from unittest.mock import MagicMock
from uuid import UUID

from market_data_center.public_api.queries import PostgreSQLPublicQueryService


def test_monitor_reads_only_bounded_rpc_and_preserves_nulls():
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    payload = {"items": [{"code": "600000", "payload": None}]}
    connection.execute.return_value.mappings.return_value.all.return_value = [{"payload": payload}]
    service = PostgreSQLPublicQueryService(engine)
    day = date(2026, 9, 29)
    batch = UUID("00000000-0000-0000-0000-000000000001")
    assert service.query_regulation_monitor_candidates(day, 50, None, "600000") == payload
    assert "api_v1.query_regulation_monitor_candidates(" in str(
        connection.execute.call_args.args[0]
    )
    assert connection.execute.call_args.args[1] == {
        "trade_date": day,
        "limit": 50,
        "cursor": None,
        "query": "600000",
    }
    assert service.query_regulation_monitor_inputs(day, batch, ("600000",)) == payload
    assert "api_v1.query_regulation_monitor_inputs(" in str(connection.execute.call_args.args[0])
    assert connection.execute.call_args.args[1] == {
        "trade_date": day,
        "calculation_id": batch,
        "codes": ["600000"],
    }
