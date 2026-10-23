from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_regulation_public_api import HEADERS, client_for

from market_data_center.public_api.queries import (
    PostgreSQLPublicQueryService,
    PublicQueryConflict,
    PublicQueryUnavailable,
    _raise_safe_query_error,
)


def engine_with_rows(rows):
    engine = MagicMock()
    connection = engine.connect.return_value.__enter__.return_value
    connection.execute.return_value.mappings.return_value.all.return_value = rows
    return engine, connection


def test_public_rpc_requires_deployed_quantity_contract_without_extra_roundtrip():
    engine, connection = engine_with_rows([{"payload": {"items": []}}])
    PostgreSQLPublicQueryService(engine)._execute(
        text("select api_v1.query_recent_daily_bars(:code, :day, :limit) as payload"),
        {"code": "600000", "day": None, "limit": 5},
    )
    assert connection.execute.call_count == 1
    statement, parameters = connection.execute.call_args.args
    assert "api_v1.quantity_unit_contract_version()" in str(statement)
    assert "stock-quantity.v1" in str(statement)
    assert parameters == {"code": "600000", "day": None, "limit": 5}


def test_wrong_contract_cannot_look_like_successful_empty_payload():
    engine, _ = engine_with_rows([])
    with pytest.raises(PublicQueryUnavailable):
        PostgreSQLPublicQueryService(engine)._execute(
            text("select api_v1.query_recent_daily_bars(:code, null, 5) as payload"),
            {"code": "600000"},
        )


def test_quantity_snapshot_sqlstate_becomes_safe_conflict():
    error = DBAPIError("private SQL", {}, SimpleNamespace(sqlstate="PT409"), False)
    with pytest.raises(PublicQueryConflict, match="quantity_units_unverified"):
        _raise_safe_query_error(error)


def test_explicit_unverified_snapshot_has_documented_http_conflict():
    client, connection = client_for({})
    connection.execute.side_effect = DBAPIError(
        "private SQL", {}, SimpleNamespace(sqlstate="PT409"), False
    )
    response = client.get(
        "/api/v1/daily-limit-up-list?trade_date=2026-10-22&version=1", headers=HEADERS
    )
    assert response.status_code == 409
    assert response.json()["error"]["code"] == "quantity_units_unverified"
    assert "409" in client.app.openapi()["paths"]["/api/v1/daily-limit-up-list"]["get"]["responses"]
