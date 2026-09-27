"""Kaipanla money-effect reads stay bounded, typed and independent of persistence."""

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers.kaipanla_money_effect import (
    MAX_RESPONSE_BYTES,
    KaipanlaMoneyEffectProvider,
    KaipanlaMoneyEffectUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

KEY = "test-kaipanla-api-key-00000000000000"
PATH = "/api/v1/realtime/kaipanla/market-emotion/money-effect"


class Transport:
    def __init__(self, payload=None):
        self.payload = payload if payload is not None else {"errcode": "0", "ZY": []}
        self.calls = []

    def __call__(self, request, timeout):
        self.calls.append((request, timeout))
        return json.dumps(self.payload, ensure_ascii=False).encode()


def provider(transport: Transport):
    return KaipanlaMoneyEffectProvider(
        request_bytes=transport,
        clock=lambda: datetime(2026, 9, 27, 12, 2, 3, tzinfo=UTC),
    )


def client(transport: Transport) -> TestClient:
    app = create_app(
        settings=ApiSettings(
            _env_file=None,
            fastapi_database_url=SecretStr("unused"),
            fastapi_api_key=SecretStr(KEY),
        ),
        query_service=object(),
        auction_indicative_service=object(),
    )
    app.state.kaipanla_money_effect_provider = provider(transport)
    return TestClient(app)


def parameters(request):
    return parse_qs(request.data.decode() if request.data else urlsplit(request.full_url).query)


def test_money_effect_route_normalizes_source_fields_and_exact_request():
    transport = Transport(
        {
            "errcode": "0",
            "ZY": [
                {"CGL": "50.00", "YLL": "0.55", "Day": "2026-09-24"},
                {"CGL": "48.72", "YLL": "-0.88", "Day": "2026-09-23"},
            ],
            "ttag": 0.013,
        }
    )

    response = client(transport).get(
        PATH,
        params={"offset": 20, "limit": 2},
        headers={"X-API-Key": KEY},
    )

    assert response.status_code == 200, response.text
    assert response.json() == {
        "source_code": "kaipanla",
        "source_layout": "kaipanla.market_emotion.money_effect.v1",
        "persisted": False,
        "observed_at": "2026-09-27 20:02:03",
        "offset": 20,
        "limit": 2,
        "returned_count": 2,
        "total": None,
        "next_offset": 22,
        "items": [
            {
                "trade_date": "2026-09-24",
                "board_success_rate_pct": "50.00",
                "board_profit_rate_pct": "0.55",
            },
            {
                "trade_date": "2026-09-23",
                "board_success_rate_pct": "48.72",
                "board_profit_rate_pct": "-0.88",
            },
        ],
    }
    request, timeout = transport.calls[0]
    assert request.method == "POST"
    assert urlsplit(request.full_url).hostname == "apphis.kaipanla.com"
    assert parameters(request) == {
        "c": ["Emotion"],
        "a": ["GetMoneyDate"],
        "index": ["20"],
        "st": ["2"],
    }
    assert 0 < timeout <= 10


def test_money_effect_requires_api_key_and_rejects_unbounded_page():
    transport = Transport()
    api = client(transport)

    assert api.get(PATH).status_code == 401
    assert api.get(PATH, params={"limit": 101}, headers={"X-API-Key": KEY}).status_code == 422
    assert transport.calls == []


@pytest.mark.parametrize(
    "payload",
    [
        {"errcode": "403", "msg": "secret"},
        {},
        {"errcode": "0"},
        {"errcode": "0", "ZY": "wrong"},
        {"errcode": "0", "ZY": [{"CGL": "NaN", "YLL": "0.55", "Day": "2026-09-24"}]},
        {"errcode": "0", "ZY": [{"CGL": "101", "YLL": "0.55", "Day": "2026-09-24"}]},
        {"errcode": "0", "ZY": [{"CGL": "50", "YLL": "0.55", "Day": "bad"}]},
        {
            "errcode": "0",
            "ZY": [
                {"CGL": "50", "YLL": "0.55", "Day": "2026-09-24"},
                {"CGL": "49", "YLL": "0.45", "Day": "2026-09-24"},
            ],
        },
        {
            "errcode": "0",
            "ZY": [
                {"CGL": "50", "YLL": "0.55", "Day": "2026-09-23"},
                {"CGL": "49", "YLL": "0.45", "Day": "2026-09-24"},
            ],
        },
    ],
)
def test_bad_money_effect_source_is_safe_502(payload):
    response = client(Transport(payload)).get(PATH, headers={"X-API-Key": KEY})

    assert response.status_code == 502
    assert response.json()["error"]["code"] == "upstream_error"
    assert "secret" not in response.text


def test_short_money_effect_page_has_no_next_offset():
    response = client(
        Transport(
            {
                "errcode": "0",
                "ZY": [{"CGL": "50", "YLL": "0.55", "Day": "2026-09-24"}],
            }
        )
    ).get(PATH, params={"limit": 20}, headers={"X-API-Key": KEY})

    assert response.status_code == 200
    assert response.json()["next_offset"] is None


def test_transport_and_response_limits_are_safe_502():
    def fail(*_):
        raise TimeoutError("source detail must not leak")

    response = client(fail).get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 502
    assert "source detail" not in response.text

    with pytest.raises(KaipanlaMoneyEffectUpstream):
        KaipanlaMoneyEffectProvider(
            request_bytes=lambda *_: b"x" * (MAX_RESPONSE_BYTES + 1)
        ).fetch()


def test_docs_and_published_contract_match_runtime():
    schema = client(Transport()).get("/openapi.json").json()
    operation = schema["paths"][PATH]["get"]
    assert operation["tags"] == ["实时接口"]
    assert operation["security"] == [{"APIKeyHeader": []}]
    assert {"401", "422", "502"} <= operation["responses"].keys()

    saved = json.loads(
        (Path(__file__).parents[1] / "contracts" / "fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert saved["paths"][PATH] == schema["paths"][PATH]
    for name, model in schema["components"]["schemas"].items():
        if name.startswith("KaipanlaMoneyEffect"):
            assert saved["components"]["schemas"][name] == model
