"""Market-emotion chart contract and source boundary checks."""

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.kaipanla_emotion_chart import (
    MAX_RESPONSE_BYTES,
    KaipanlaEmotionChartProvider,
    KaipanlaEmotionChartUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

PATH = "/api/v1/realtime/kaipanla/market-emotion/chart"
KEY = "test-kaipanla-api-key-00000000000000"
ROW = {"Day": "2026-09-24", "strong": "42", "df_num": "5", "ztjs": "52", "lbgd": "5"}


def api(payload, calls):
    def request(req, timeout):
        calls.append(req)
        if isinstance(payload, Exception):
            raise payload
        return json.dumps(payload).encode()

    return TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
            kaipanla_emotion_chart_provider=KaipanlaEmotionChartProvider(
                request_bytes=request,
                clock=lambda: datetime(2026, 9, 27, 12, 2, 3, tzinfo=UTC),
            ),
        )
    )


def test_chart_request_and_normalized_fields():
    calls = []
    response = api({"errcode": "0", "info": [ROW]}, calls).get(
        PATH, params={"offset": 10, "limit": 1}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "trade_date": "2026-09-24",
            "emotion_index": 42,
            "large_drawdown_count": 5,
            "limit_up_count": 52,
            "consecutive_limit_up_height": 5,
        }
    ]
    assert response.json()["observed_at"] == "2026-09-27 20:02:03"
    assert response.json()["next_offset"] == 11
    assert response.json()["persisted"] is False
    assert len(calls) == 1
    assert calls[0].full_url == "https://apphis.kaipanla.com/w1/api/index.php"
    assert calls[0].method == "POST"
    assert parse_qs(calls[0].data.decode()) == {
        "c": ["HisHomeDingPan"],
        "a": ["ChangeStatistics"],
        "Index": ["10"],
        "st": ["1"],
    }


@pytest.mark.parametrize(
    "change",
    [
        {"strong": "101"},
        {"df_num": "-1"},
        {"lbgd": "1.5"},
        {"ztjs": None},
        {"strong": True},
        {"Day": "2026-02-30"},
    ],
)
def test_chart_rejects_invalid_source_fields(change):
    response = api({"errcode": "0", "info": [ROW | change]}, []).get(
        PATH, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502


@pytest.mark.parametrize(
    "payload",
    [
        {"errcode": "1", "message": "secret"},
        {},
        {"errcode": "0", "info": None},
        {"errcode": "0", "info": [ROW, ROW]},
        {"errcode": "0", "info": [ROW | {"Day": "2026-09-23"}, ROW]},
        TimeoutError("secret"),
    ],
)
def test_chart_rejects_errors_without_leaking_details(payload):
    response = api(payload, []).get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 502
    assert "secret" not in response.text


def test_chart_auth_bounds_empty_page_and_contract():
    calls = []
    client = api({"errcode": "0", "info": []}, calls)
    assert client.get(PATH).status_code == 401
    for params in ({"limit": 101}, {"limit": 0}, {"offset": -1}, {"offset": 10001}):
        assert client.get(PATH, params=params, headers={"X-API-Key": KEY}).status_code == 422
    assert calls == []
    data = client.get(PATH, headers={"X-API-Key": KEY}).json()
    assert data["items"] == [] and data["next_offset"] is None
    schema = client.get("/openapi.json").json()
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(encoding="utf8")
    )
    assert saved["paths"][PATH] == schema["paths"][PATH]
    for name, model in schema["components"]["schemas"].items():
        if name.startswith("KaipanlaEmotionChart"):
            assert saved["components"]["schemas"][name] == model


def test_chart_rejects_oversized_and_invalid_json_responses():
    for body in (b"x" * (MAX_RESPONSE_BYTES + 1), b"not json"):
        with pytest.raises(KaipanlaEmotionChartUpstream):
            KaipanlaEmotionChartProvider(request_bytes=lambda *_, body=body: body).fetch()
    response = api({"errcode": "0", "info": [ROW, ROW | {"Day": "2026-09-23"}]}, []).get(
        PATH, params={"limit": 1}, headers={"X-API-Key": KEY}
    )
    assert response.status_code == 502


def test_chart_preserves_zero_counts():
    row = ROW | {"strong": "0", "df_num": "0", "ztjs": "0", "lbgd": "0"}
    response = api({"errcode": "0", "info": [row]}, []).get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 200
    assert response.json()["items"][0]["large_drawdown_count"] == 0
