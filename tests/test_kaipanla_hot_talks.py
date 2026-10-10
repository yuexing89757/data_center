"""Latest topic-list authentication and validated source reads."""

import json
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr

from market_data_center.providers import kaipanla_hot_talks as source_module
from market_data_center.providers.kaipanla_hot_talks import (
    MAX_RESPONSE_BYTES,
    KaipanlaHotTalksProvider,
    KaipanlaHotTalksUpstream,
)
from market_data_center.public_api import create_app
from market_data_center.settings import ApiSettings

PATH = "/api/v1/realtime/kaipanla/hot-talks"
KEY = "test-kaipanla-api-key-00000000000000"
ROW = {"ID": "1001", "Title": "Sample topic", "IsVote": "1", "Time": "1791456170", "HotVal": 12345}


def api(payload, calls):
    def request(req, timeout):
        calls.append(req)
        assert timeout == 8
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
            kaipanla_hot_talks_provider=KaipanlaHotTalksProvider(
                request_bytes=request,
                clock=lambda: datetime(2026, 10, 10, 11, 5, tzinfo=UTC),
            ),
        )
    )


def group(day="2026-10-08", rows=None):
    return {"Day": day, "List": [ROW] if rows is None else rows}


def test_hot_talks_requires_api_key_before_source_access():
    client = TestClient(
        create_app(
            settings=ApiSettings(
                _env_file=None,
                fastapi_database_url=SecretStr("unused"),
                fastapi_api_key=SecretStr(KEY),
            ),
            query_service=object(),
            auction_indicative_service=object(),
        )
    )
    assert client.get(PATH).status_code == 401


def test_latest_group_all_items_single_credential_free_request_and_timestamps():
    calls = []
    client = api(
        {
            "errcode": "0",
            "List": [group(rows=[ROW, ROW | {"ID": "1002", "IsVote": "0"}]), group("2026-10-07")],
        },
        calls,
    )
    assert client.get(PATH).status_code == 401
    assert not calls
    response = client.get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 200
    data = response.json()
    assert data["topic_date"] == "2026-10-08"
    assert data["observed_at"] == "2026-10-10 19:05:00"
    assert data["returned_count"] == 2 and data["persisted"] is False
    assert data["items"][0] == {
        "topic_id": "1001",
        "rank": 1,
        "title": "Sample topic",
        "heat_value": 12345,
        "published_at": "2026-10-08 18:42:50",
        "has_vote": True,
    }
    assert data["items"][1]["rank"] == 2 and data["items"][1]["has_vote"] is False
    assert len(calls) == 1
    assert calls[0].full_url == "https://applhb.kaipanla.com/w1/api/index.php"
    assert calls[0].method == "POST"
    assert parse_qs(calls[0].data.decode()) == {
        "c": ["Topic"],
        "a": ["InfoList"],
        "Index": ["0"],
        "st": ["20"],
    }


@pytest.mark.parametrize(
    "change",
    [
        {"ID": True},
        {"ID": "abc"},
        {"Title": " "},
        {"Title": None},
        {"IsVote": "2"},
        {"IsVote": False},
        {"HotVal": -1},
        {"HotVal": 1.5},
        {"Time": "bad"},
        {"Time": "99999999999999999999"},
        {"Time": 0},
    ],
)
def test_rejects_invalid_latest_fields(change):
    client = api({"errcode": "0", "List": [group(rows=[ROW | change])]}, [])
    assert client.get(PATH, headers={"X-API-Key": KEY}).status_code == 502


@pytest.mark.parametrize(
    "payload",
    [
        {},
        {"errcode": "1", "message": "secret"},
        {"errcode": False, "List": []},
        {"errcode": "0", "List": None},
        {"errcode": "0", "List": [group("2026-02-30")]},
        {"errcode": "0", "List": [group("2026-10-11")]},
        {"errcode": "0", "List": [group(), group()]},
        {"errcode": "0", "List": [group("2026-10-07"), group()]},
        {"errcode": "0", "List": [group(rows=[ROW, ROW])]},
        {"errcode": "0", "List": [group(rows=[ROW] * 101)]},
        {"errcode": "0", "List": [group()] * 22},
        TimeoutError("secret"),
    ],
)
def test_source_errors_fail_without_details_or_retry(payload):
    calls = []
    response = api(payload, calls).get(PATH, headers={"X-API-Key": KEY})
    assert response.status_code == 502 and "secret" not in response.text
    assert len(calls) == 1


def test_empty_latest_group_does_not_fall_back_and_missing_values_stay_null():
    client = api({"errcode": "0", "List": [group(rows=[]), group("2026-10-07")]}, [])
    data = client.get(PATH, headers={"X-API-Key": KEY}).json()
    assert data["topic_date"] == "2026-10-08" and data["items"] == []
    data = api({"errcode": "0", "List": []}, []).get(PATH, headers={"X-API-Key": KEY}).json()
    assert data["topic_date"] is None and data["returned_count"] == 0
    data = (
        api({"errcode": "0", "List": [group(rows=[ROW | {"Time": None, "HotVal": None}])]}, [])
        .get(PATH, headers={"X-API-Key": KEY})
        .json()
    )
    assert data["items"][0]["heat_value"] is None
    assert data["items"][0]["published_at"] is None


def test_body_limits_invalid_json_and_timeout_bounds():
    for body in (b"x" * (MAX_RESPONSE_BYTES + 1), b"not-json"):
        with pytest.raises(KaipanlaHotTalksUpstream):
            KaipanlaHotTalksProvider(request_bytes=lambda *_, body=body: body).fetch()
    for timeout in (0, 11, float("nan")):
        with pytest.raises(ValueError):
            KaipanlaHotTalksProvider(timeout_seconds=timeout)


def test_openapi_matches_checked_in_contract():
    schema = api({"errcode": "0", "List": []}, []).get("/openapi.json").json()
    saved = json.loads(
        (Path(__file__).parents[1] / "contracts/fastapi-openapi-v1.json").read_text(
            encoding="utf-8"
        )
    )
    assert schema["paths"][PATH] == saved["paths"][PATH]
    assert schema["paths"][PATH]["get"]["tags"] == ["实时接口"]
    for name in ("KaipanlaHotTalkItem", "KaipanlaHotTalksResponse"):
        assert schema["components"]["schemas"][name] == saved["components"]["schemas"][name]


def test_transport_disables_proxy_and_redirect_and_bounds_read(monkeypatch):
    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

        def read(self, size):
            assert size == MAX_RESPONSE_BYTES + 1
            return b'{"errcode":"0","List":[]}'

    class Opener:
        def open(self, request, timeout):
            assert timeout == 8 and request.full_url == source_module.SOURCE_URL
            return Response()

    def build(proxy, redirects):
        assert proxy.proxies == {}
        assert redirects.redirect_request(None, None, None, None, None, None) is None
        return Opener()

    monkeypatch.setattr(source_module, "build_opener", build)
    assert KaipanlaHotTalksProvider().fetch().items == ()
