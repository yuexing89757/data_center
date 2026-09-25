from __future__ import annotations

import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from market_data_center.domain.ingestion import DatasetCode
from market_data_center.domain.regulation import (
    RegulationDirection,
    RegulationRuleLevel,
    RegulationSegment,
)
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.szse_regulation import (
    SZSEOfficialRegulationEventProvider,
    SZSEResponse,
    normalize_szse_regulation_raw,
)

SHANGHAI = ZoneInfo("Asia/Shanghai")
FROM = datetime(2026, 9, 18, 0, 0, tzinfo=SHANGHAI)
TO = datetime(2026, 9, 18, 23, 59, 59, tzinfo=SHANGHAI)
OBSERVED_AT = datetime(2026, 9, 18, 22, 0, tzinfo=SHANGHAI)


def _listing_row(code: str, reason: str, *, zbdm: str = "1001") -> dict[str, str]:
    return {
        "dqrq": "2026-09-18",
        "zqdm": code,
        "zqjc": "测试股票",
        "plyy": reason,
        "bz": (
            "<a a-param='/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1842_detal"
            f"&TABKEY=tab1,tab2&DQRQ=2026-09-18&ZQDM={code}&ZBDM={zbdm}'>详情</a>"
        ),
    }


def _listing(rows: list[dict[str, str]], *, page_no: int = 1, page_count: int = 1) -> bytes:
    return json.dumps(
        [
            {
                "metadata": {
                    "catalogid": "1842_xxpl_after",
                    "pageno": page_no,
                    "pagecount": page_count,
                    "recordcount": len(rows),
                },
                "data": rows,
                "error": None,
            }
        ],
        ensure_ascii=False,
    ).encode()


def _detail(code: str, reason: str, *, period: str = "2026-09-16至2026-09-18") -> bytes:
    return json.dumps(
        [
            {
                "metadata": {"catalogid": "1842_detal", "tabkey": "tab1"},
                "data": [
                    {
                        "dqrq": "2026-09-18",
                        "ycqj": period,
                        "zqjc": f"测试股票 ({code})",
                        "plyy": reason,
                    }
                ],
                "error": None,
            }
        ],
        ensure_ascii=False,
    ).encode()


def _provider(
    listing_pages: dict[int, bytes],
    details: dict[str, bytes],
    *,
    final_url: str = "https://www.szse.cn/api/report/ShowReport/data",
) -> SZSEOfficialRegulationEventProvider:
    def fetch(url: str, params: dict[str, str]) -> SZSEResponse:
        if params["CATALOGID"] == "1842_xxpl_after":
            body = listing_pages[int(params["PAGENO"])]
        else:
            body = details[params["ZQDM"]]
        return SZSEResponse(
            url=final_url,
            content_type="application/json; charset=utf-8",
            body=body,
        )

    return SZSEOfficialRegulationEventProvider(fetch=fetch, clock=lambda: OBSERVED_AT)


def test_szse_provider_distinguishes_mainboard_and_gem_rules() -> None:
    main_reason = "异常期间价格涨幅偏离值累计达到21.09%"
    gem_reason = "异常期间价格涨幅偏离值累计达到31.09%"
    provider = _provider(
        {1: _listing([_listing_row("000001", main_reason), _listing_row("300001", gem_reason)])},
        {"000001": _detail("000001", main_reason), "300001": _detail("300001", gem_reason)},
    )

    events = tuple(provider.fetch_events(FROM, TO).records)
    assert [(item.symbol, item.segment, item.explicit_rule_codes) for item in events] == [
        ("SZSE:000001", RegulationSegment.SZSE_MAIN, ("SZSE_MAIN_ABNORMAL_3D_DEV_UP",)),
        ("SZSE:300001", RegulationSegment.GEM, ("GEM_ABNORMAL_3D_DEV_UP",)),
    ]


def test_szse_raw_replay_reproduces_the_live_event() -> None:
    reason = "异常期间价格涨幅偏离值累计达到21.09%"
    batch = _provider(
        {1: _listing([_listing_row("000001", reason)])},
        {"000001": _detail("000001", reason)},
    ).fetch_events(FROM, TO)

    replayed = normalize_szse_regulation_raw(
        DatasetCode.REGULATION_EVENT,
        batch.schema_version,
        batch.raw_rows,
        batch.request_params,
    )

    assert replayed == batch.records


@pytest.mark.parametrize(
    ("code", "reason", "expected_rule"),
    [
        ("000001", "连续10个交易日内4次出现同正向异常波动", "SZSE_MAIN_SERIOUS_10D_COUNT_UP"),
        ("300001", "连续10个交易日内3次出现同负向异常波动", "GEM_SERIOUS_10D_COUNT_DOWN"),
        ("000001", "连续10个交易日内价格涨幅偏离值累计达到101%", "SZSE_MAIN_SERIOUS_10D_DEV_UP"),
        ("300001", "连续30个交易日内价格跌幅偏离值累计达到-71%", "GEM_SERIOUS_30D_DEV_DOWN"),
    ],
)
def test_szse_provider_maps_serious_reasons(code: str, reason: str, expected_rule: str) -> None:
    provider = _provider({1: _listing([_listing_row(code, reason)])}, {code: _detail(code, reason)})
    [event] = provider.fetch_events(FROM, TO).records
    assert event.event_level is RegulationRuleLevel.SERIOUS_ABNORMAL
    assert event.explicit_rule_codes == (expected_rule,)


def test_szse_provider_keeps_multiple_explicit_reasons() -> None:
    reason = "连续10个交易日内4次出现同正向异常波动;连续30个交易日内价格涨幅偏离值累计达到201%"
    provider = _provider(
        {1: _listing([_listing_row("000001", reason)])},
        {"000001": _detail("000001", reason)},
    )
    [event] = provider.fetch_events(FROM, TO).records
    assert event.direction is RegulationDirection.UP
    assert event.explicit_rule_codes == (
        "SZSE_MAIN_SERIOUS_10D_COUNT_UP",
        "SZSE_MAIN_SERIOUS_30D_DEV_UP",
    )


def test_szse_provider_accepts_exchange_recognition_with_unknown_direction() -> None:
    reason = "深圳证券交易所认定属于异常波动的其他情形"
    provider = _provider(
        {1: _listing([_listing_row("000001", reason)])},
        {"000001": _detail("000001", reason)},
    )
    [event] = provider.fetch_events(FROM, TO).records
    assert event.direction is None
    assert event.explicit_rule_codes == ()


def test_szse_provider_title_only_match_emits_no_event() -> None:
    listing_reason = "股票交易异常波动"
    body_reason = "公司经营情况正常"
    batch = _provider(
        {1: _listing([_listing_row("000001", listing_reason)])},
        {"000001": _detail("000001", body_reason)},
    ).fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert tuple(batch.records) == ()


def test_szse_provider_deduplicates_duplicate_listing_rows() -> None:
    reason = "异常期间价格涨幅偏离值累计达到21.09%"
    row = _listing_row("000001", reason)
    provider = _provider({1: _listing([row, row])}, {"000001": _detail("000001", reason)})
    batch = provider.fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert len(batch.records) == 1


def test_szse_provider_rejects_pagination_over_bound() -> None:
    provider = _provider({1: _listing([], page_count=21)}, {})
    with pytest.raises(ProviderError, match="pagination exceeds bound"):
        provider.fetch_events(FROM, TO)


def test_szse_provider_wraps_timeout() -> None:
    def fetch(url: str, params: dict[str, str]) -> SZSEResponse:
        raise TimeoutError("slow")

    provider = SZSEOfficialRegulationEventProvider(fetch=fetch, clock=lambda: OBSERVED_AT)
    with pytest.raises(ProviderError, match="request failed"):
        provider.fetch_events(FROM, TO)


def test_szse_provider_rejects_redirect_host() -> None:
    provider = _provider({1: _listing([])}, {}, final_url="https://example.com/data")
    with pytest.raises(ProviderError, match="official URL is not allowed"):
        provider.fetch_events(FROM, TO)


@pytest.mark.parametrize("period", ["bad", "2026-09-19至2026-09-18"])
def test_szse_provider_keeps_malformed_detail_only_in_raw(period: str) -> None:
    reason = "异常期间价格涨幅偏离值累计达到21.09%"
    batch = _provider(
        {1: _listing([_listing_row("000001", reason)])},
        {"000001": _detail("000001", reason, period=period)},
    ).fetch_events(FROM, TO)
    assert len(batch.raw_rows) == 1
    assert tuple(batch.records) == ()


def test_szse_provider_rejects_naive_bounds() -> None:
    provider = _provider({1: _listing([])}, {})
    with pytest.raises(ValueError, match="timezone-aware"):
        provider.fetch_events(FROM.replace(tzinfo=None), TO)
