import json
from dataclasses import replace
from datetime import datetime
from decimal import Decimal
from types import SimpleNamespace
from uuid import UUID
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy.exc import DBAPIError
from test_regulation_monitor_calculator import DAYS, price_event, source
from test_regulation_public_api import HEADERS, client_for

from market_data_center.domain.regulation import MonitorState
from market_data_center.providers.regulation_quote import MonitorQuote
from market_data_center.regulation_calculator import calculate_monitor_day
from market_data_center.regulation_monitor_codec import encode_monitor

BATCH = UUID("00000000-0000-0000-0000-000000000011")
NOW = datetime(2026, 7, 7, 10, 0, tzinfo=ZoneInfo("Asia/Shanghai"))
URL = "/api/v1/regulation/monitor/query"
QUERY = {"trade_date": "2026-07-07", "calculation_id": str(BATCH), "codes": ["600000"]}


def payload():
    src = source([".1"])
    day = calculate_monitor_day(src, ())
    return {
        "schema_version": "regulation-monitor.v1",
        "trade_date": "2026-07-07",
        "base_trade_date": "2026-07-06",
        "next_trade_date": "2026-07-08",
        "calculation_id": str(BATCH),
        "algorithm_version": "regulation-monitor.v1",
        "rule_set_version": src.active_rules[0].rule_set_version,
        "completed_at": "2026-07-06T22:30:00+08:00",
        "count_cutoff_date": "2026-07-06",
        "official_coverage": "UNKNOWN",
        "official_watermark": "2026-07-06T22:30:00+08:00",
        "coverage": {
            "expected_count": 1,
            "complete_count": 1,
            "incomplete_count": 0,
            "not_applicable_count": 0,
        },
        "is_confirmed_close": False,
        "source": json.loads(encode_monitor(replace(src, candidates=()))),
        "requested_count": 1,
        "found_count": 1,
        "missing_count": 0,
        "items": [
            {
                "code": "600000",
                "symbol": "SSE:600000",
                "name": "样例",
                "next_day_reference_safe": True,
                "target_applicability": "APPLICABLE",
                "target_applicability_reason": None,
                "unrestricted_shares": None,
                "payload": json.loads(
                    encode_monitor(
                        {
                            "candidate": src.candidates[0],
                            "state": day.states[0],
                            "previous_state": None,
                            "rule_results": day.output.rule_results,
                            "warnings": (),
                        }
                    )
                ),
            }
        ],
    }


def setup(monkeypatch, raw=None):
    from market_data_center.public_api import regulation_monitor as module

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return NOW

    monkeypatch.setattr(module, "datetime", Clock)
    quotes = (
        MonitorQuote("SSE:600000", NOW, Decimal("10.50"), Decimal("10"), None),
        MonitorQuote("SSE:000002", NOW, Decimal("100"), Decimal("100"), None),
    )
    monkeypatch.setattr(module, "fetch_monitor_quotes", lambda *args, **kwargs: quotes)
    return client_for(raw or payload())


def test_query_uses_frozen_counts_and_conditional_thresholds(monkeypatch):
    client, connection = setup(monkeypatch)
    response = client.post(URL, headers=HEADERS, json=QUERY)
    assert response.status_code == 200
    body = response.json()
    stock = body["items"][0]
    assert stock["rules"][0]["today"]["trigger_price"] == "10.91"
    assert stock["calculated_price_abnormal_count_10d_up"] == 0
    assert stock["official_price_abnormal_count_10d_up"] is None
    assert body["observation_mode"] == "LIVE"
    assert body["generated_at"] == "2026-07-07 10:00:00"
    assert body["count_cutoff_date"] == "2026-07-06"
    assert "api_v1.query_regulation_monitor_inputs(" in str(connection.execute.call_args.args[0])


@pytest.mark.parametrize("codes", [[], ["1"], ["600000"] * 51])
def test_invalid_batch_and_auth_never_reach_database(monkeypatch, codes):
    client, connection = setup(monkeypatch)
    assert client.post(URL, json=QUERY).status_code == 401
    assert client.post(URL, headers=HEADERS, json={**QUERY, "codes": codes}).status_code == 422
    assert not connection.execute.called


@pytest.mark.parametrize(
    "sqlstate,expected", [("P0002", 404), ("P0004", 409), ("22023", 422), ("08006", 503)]
)
def test_database_errors_are_safe(monkeypatch, sqlstate, expected):
    client, connection = setup(monkeypatch)
    connection.execute.side_effect = DBAPIError(
        "private SQL", {}, SimpleNamespace(sqlstate=sqlstate, detail="private"), False
    )
    response = client.post(URL, headers=HEADERS, json=QUERY)
    assert response.status_code == expected
    assert "private" not in response.text


def test_unknown_stock_is_partial_and_all_upstream_failure_is_502(monkeypatch):
    from market_data_center.public_api import regulation_monitor as module

    raw = payload()
    raw["items"].append({"code": "999999", "symbol": None, "name": None, "payload": None})
    client, _ = setup(monkeypatch, raw)
    response = client.post(URL, headers=HEADERS, json={**QUERY, "codes": ["600000", "999999"]})
    assert response.status_code == 200 and response.json()["status"] == "PARTIAL"
    assert response.json()["items"][1]["calculated_price_abnormal_count_10d_up"] is None
    monkeypatch.setattr(module, "fetch_monitor_quotes", lambda *a, **k: ())
    assert client.post(URL, headers=HEADERS, json=QUERY).status_code == 502


def test_historical_query_requires_confirmed_batch_and_never_requests_today_quotes(monkeypatch):
    from market_data_center.public_api import regulation_monitor as module

    raw = payload()
    raw.update(trade_date="2026-07-06", next_trade_date="2026-07-07", is_confirmed_close=True)
    client, _ = setup(monkeypatch, raw)

    def forbidden(*a, **k):
        raise AssertionError("historical query must not fetch live quotes")

    monkeypatch.setattr(module, "fetch_monitor_quotes", forbidden)
    response = client.post(URL, headers=HEADERS, json={**QUERY, "trade_date": "2026-07-06"})
    assert response.status_code == 200
    assert response.json()["observation_mode"] == "CONFIRMED_CLOSE"
    assert response.json()["quote_observed_at"] is None


def test_candidates_are_bounded_and_future_dates_rejected(monkeypatch):
    raw = payload()
    raw.update(total=1, next_cursor=None, candidate_basis="PUBLISHED_CLOSE_OBSERVATION_LIST")
    raw["items"] = [
        {
            "code": "600000",
            "symbol": "SSE:600000",
            "name": "样例",
            "candidate_basis": ["RECENT_CALCULATED_EVENT"],
        }
    ]
    client, connection = setup(monkeypatch, raw)
    url = "/api/v1/regulation/monitor/candidates"
    response = client.get(url, headers=HEADERS, params={"trade_date": "2026-07-07"})
    assert response.status_code == 200 and response.json()["total"] == 1
    assert "api_v1.query_regulation_monitor_candidates(" in str(
        connection.execute.call_args.args[0]
    )
    assert (
        client.get(
            url, headers=HEADERS, params={"trade_date": "2026-07-07", "limit": 101}
        ).status_code
        == 422
    )
    assert client.get(url, headers=HEADERS, params={"trade_date": "2026-07-08"}).status_code == 422


def test_stale_quote_is_partial_not_a_fresh_trigger(monkeypatch):
    from datetime import timedelta

    from market_data_center.public_api import regulation_monitor as module

    client, _ = setup(monkeypatch)
    quotes = tuple(
        MonitorQuote(s, NOW - timedelta(seconds=61), Decimal("10"), Decimal("10"), None)
        for s in ("SSE:600000", "SSE:000002")
    )
    monkeypatch.setattr(module, "fetch_monitor_quotes", lambda *a: quotes)
    body = client.post(URL, headers=HEADERS, json=QUERY).json()
    assert body["status"] == "PARTIAL"
    assert body["items"][0]["rules"][0]["today"]["trigger_price"] is None
    assert "stale_quote" in body["items"][0]["rules"][0]["missing_reasons"]


def test_unknown_turnover_history_is_not_zero(monkeypatch):
    raw = payload()
    raw["items"][0]["payload"]["state"]["turnover_missing_dates"] = ["2026-07-06"]
    client, _ = setup(monkeypatch, raw)
    stock = client.post(URL, headers=HEADERS, json=QUERY).json()["items"][0]
    assert stock["calculated_price_abnormal_count_10d_up"] == 0
    assert stock["calculated_turnover_count_10d"] is None


def test_live_ten_day_count_ends_at_published_close_not_today(monkeypatch):
    from market_data_center.public_api import regulation_monitor as module

    src = source([0] * 11)
    raw = payload()
    raw.update(
        trade_date=str(DAYS[11]),
        base_trade_date=str(DAYS[10]),
        next_trade_date=str(DAYS[12]),
        count_cutoff_date=str(DAYS[10]),
        source=json.loads(encode_monitor(replace(src, candidates=()))),
    )
    raw["items"][0]["payload"]["candidate"] = json.loads(encode_monitor(src.candidates[0]))
    raw["items"][0]["payload"]["state"] = json.loads(
        encode_monitor(
            MonitorState("SSE:600000", DAYS[10], True, None, None, (price_event(DAYS[1]),), (), ())
        )
    )
    client, _ = setup(monkeypatch, raw)
    observed = datetime.combine(DAYS[11], datetime.min.time(), tzinfo=ZoneInfo("Asia/Shanghai"))
    observed = observed.replace(hour=10)

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return observed

    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(
        module,
        "fetch_monitor_quotes",
        lambda *args: (
            MonitorQuote("SSE:600000", observed, Decimal("10"), Decimal("10"), None),
            MonitorQuote("SSE:000002", observed, Decimal("100"), Decimal("100"), None),
        ),
    )

    body = client.post(URL, headers=HEADERS, json={**QUERY, "trade_date": str(DAYS[11])}).json()
    assert body["count_cutoff_date"] == str(DAYS[10])
    assert body["items"][0]["calculated_price_abnormal_count_10d_up"] == 1
    assert body["items"][0]["events"][0]["trade_date"] == str(DAYS[1])


@pytest.mark.parametrize("applicability", ["NOT_APPLICABLE", "INSUFFICIENT_DATA"])
def test_live_uses_target_day_applicability(monkeypatch, applicability):
    raw = payload()
    raw["items"][0].update(
        target_applicability=applicability, target_applicability_reason="target_status_changed"
    )
    client, _ = setup(monkeypatch, raw)
    stock = client.post(URL, headers=HEADERS, json=QUERY).json()["items"][0]
    assert stock["applicability"] == applicability
    assert stock["rules"][0]["today"]["trigger_price"] is None


def test_confirmed_close_rechecks_next_reference(monkeypatch):
    raw = payload()
    raw.update(trade_date="2026-07-06", next_trade_date="2026-07-07", is_confirmed_close=True)
    raw["items"][0]["next_day_reference_safe"] = False
    client, _ = setup(monkeypatch, raw)
    body = client.post(URL, headers=HEADERS, json={**QUERY, "trade_date": "2026-07-06"}).json()
    assert body["items"][0]["rules"][0]["next_day"]["trigger_price"] is None


def test_confirmed_close_keeps_conditional_next_day_range_when_st_unknown(monkeypatch):
    raw = payload()
    raw.update(trade_date="2026-07-06", next_trade_date="2026-07-07", is_confirmed_close=True)
    raw["items"][0]["next_day_st_verified"] = False
    client, _ = setup(monkeypatch, raw)
    body = client.post(URL, headers=HEADERS, json={**QUERY, "trade_date": "2026-07-06"}).json()
    stock = body["items"][0]
    assert stock["rules"][0]["next_day"]["trigger_price"] is not None
    assert "next_day_st_unverified" in stock["missing_reasons"]
    assert body["status"] == "PARTIAL"


def test_quote_created_during_request_is_not_future(monkeypatch):
    from datetime import timedelta

    from market_data_center.public_api import regulation_monitor as module

    client, _ = setup(monkeypatch)
    current = [NOW]

    class Clock(datetime):
        @classmethod
        def now(cls, tz=None):
            return current[0]

    def fetch(*args):
        current[0] = NOW + timedelta(seconds=2)
        return tuple(
            MonitorQuote(s, NOW + timedelta(seconds=1), Decimal(p), Decimal(p), None)
            for s, p in (("SSE:600000", "10"), ("SSE:000002", "100"))
        )

    monkeypatch.setattr(module, "datetime", Clock)
    monkeypatch.setattr(module, "fetch_monitor_quotes", fetch)
    body = client.post(URL, headers=HEADERS, json=QUERY).json()
    assert body["items"][0]["rules"][0]["today"]["trigger_price"] == "10.91"
    assert body["generated_at"] == "2026-07-07 10:00:02"
