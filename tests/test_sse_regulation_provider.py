from __future__ import annotations

import json
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import pytest

from market_data_center.domain.regulation import (
    RegulationDirection,
    RegulationRuleLevel,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.sse_regulation import (
    SSEOfficialRegulationEventProvider,
    SSEResponse,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
FROM = datetime(2026, 9, 18, 0, 0, tzinfo=SHANGHAI)
TO = datetime(2026, 9, 18, 23, 59, 59, tzinfo=SHANGHAI)
OBSERVED_AT = datetime(2026, 9, 18, 22, 0, tzinfo=SHANGHAI)


def _row(**overrides: Any) -> dict[str, object]:
    value: dict[str, object] = {
        "secCode": "600000",
        "secAbbr": "浦发银行",
        "refType": "1",
        "tradeDate": "20260918",
        "abnormalStart": "20260916",
        "abnormalEnd": "20260918",
        "secValue": "20.12",
    }
    value.update(overrides)
    return value


def _page(rows: list[dict[str, object]], *, page_no: int = 1, page_count: int = 1) -> bytes:
    payload = {
        "pageHelp": {
            "pageNo": page_no,
            "pageCount": page_count,
            "total": len(rows),
            "data": rows,
        },
        "result": rows,
    }
    return f"callback({json.dumps(payload, ensure_ascii=False)})".encode()


def _provider(
    pages: dict[int, bytes], *, final_url: str = "https://query.sse.com.cn/commonSoaQuery.do"
) -> SSEOfficialRegulationEventProvider:
    def fetch(url: str, params: dict[str, str]) -> SSEResponse:
        return SSEResponse(
            url=final_url,
            content_type="application/javascript; charset=utf-8",
            body=pages[int(params["pageHelp.pageNo"])],
        )

    return SSEOfficialRegulationEventProvider(fetch=fetch, clock=lambda: OBSERVED_AT)


def test_sse_provider_normalizes_explicit_official_reason() -> None:
    batch = _provider({1: _page([_row()])}).fetch_events(FROM, TO)

    assert batch.schema_version == "sse.regulation_event.v1"
    assert len(batch.raw_rows) == 1
    [event] = batch.records
    assert event.symbol == "SSE:600000"
    assert event.event_level is RegulationRuleLevel.ABNORMAL
    assert event.direction is RegulationDirection.UP
    assert event.explicit_rule_codes == ("SSE_MAIN_ABNORMAL_3D_DEV_UP",)


@pytest.mark.parametrize(
    ("ref_type", "direction", "rule_code"),
    [
        ("Z3", RegulationDirection.UP, "SSE_MAIN_SERIOUS_10D_4_UP"),
        ("Z4", RegulationDirection.DOWN, "SSE_MAIN_SERIOUS_10D_4_DOWN"),
        ("Z5", RegulationDirection.UP, "SSE_MAIN_SERIOUS_10D_DEV_100_UP"),
        ("Z6", RegulationDirection.DOWN, "SSE_MAIN_SERIOUS_10D_DEV_50_DOWN"),
        ("Z7", RegulationDirection.UP, "SSE_MAIN_SERIOUS_30D_DEV_200_UP"),
        ("Z8", RegulationDirection.DOWN, "SSE_MAIN_SERIOUS_30D_DEV_70_DOWN"),
    ],
)
def test_sse_provider_maps_serious_reasons(
    ref_type: str, direction: RegulationDirection, rule_code: str
) -> None:
    [event] = _provider({1: _page([_row(refType=ref_type)])}).fetch_events(FROM, TO).records
    assert event.event_level is RegulationRuleLevel.SERIOUS_ABNORMAL
    assert event.direction is direction
    assert event.explicit_rule_codes == (rule_code,)


def test_sse_provider_keeps_unrelated_reason_in_raw_without_event() -> None:
    batch = _provider({1: _page([_row(refType="14")])}).fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert tuple(batch.records) == ()


def test_sse_provider_deduplicates_repeated_pages() -> None:
    row = _row()
    batch = _provider(
        {1: _page([row], page_no=1, page_count=2), 2: _page([row], page_no=2, page_count=2)}
    ).fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert len(batch.records) == 1


def test_sse_provider_rejects_page_count_over_bound() -> None:
    provider = _provider({1: _page([], page_count=21)})
    with pytest.raises(ProviderError, match="pagination exceeds bound"):
        provider.fetch_events(FROM, TO)


def test_sse_provider_rejects_redirect_to_unapproved_host() -> None:
    provider = _provider({1: _page([])}, final_url="https://example.com/redirect")
    with pytest.raises(ProviderError, match="official URL is not allowed"):
        provider.fetch_events(FROM, TO)


def test_sse_provider_wraps_transport_timeout() -> None:
    def fetch(url: str, params: dict[str, str]) -> SSEResponse:
        raise TimeoutError("slow")

    provider = SSEOfficialRegulationEventProvider(fetch=fetch, clock=lambda: OBSERVED_AT)
    with pytest.raises(ProviderError, match="request failed"):
        provider.fetch_events(FROM, TO)


@pytest.mark.parametrize(
    "row",
    [
        _row(tradeDate="bad"),
        _row(tradeDate="20260919"),
        _row(secCode="688001"),
    ],
)
def test_sse_provider_preserves_invalid_or_out_of_scope_rows_only_in_raw(
    row: dict[str, object],
) -> None:
    batch = _provider({1: _page([row])}).fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert tuple(batch.records) == ()


def test_sse_provider_rejects_naive_observation_bounds() -> None:
    provider = _provider({1: _page([])})
    with pytest.raises(ValueError, match="timezone-aware"):
        provider.fetch_events(FROM.replace(tzinfo=None), TO)
