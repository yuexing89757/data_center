from datetime import UTC, datetime

import pytest

from market_data_center.providers.tencent_quote import TencentQuoteProvider
from market_data_center.public_api.tencent_quote_live import (
    DirectTencentQuoteLiveService,
    TencentQuoteLiveUpstream,
)

ROW = (
    "1~柳钢股份~601003~3.58~3.60~3.58~99203~46429~52774~"
    "3.58~571~3.57~3753~3.56~1399~3.55~3291~3.54~1312~"
    "3.59~1862~3.60~1679~3.61~1036~3.62~1200~3.63~1683~~"
    "20260821161441~-0.02~-0.56~3.60~3.54~3.58/99203/35356540~"
    "99203~3536~0.39~21.72~~3.60~3.54~1.67~91.75~94.31~1.03~3.96~3.24"
)


def test_live_service_fetches_tencent_and_does_not_need_persistence() -> None:
    calls: list[str] = []
    now = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)

    def request(url: str, _timeout: float) -> bytes:
        calls.append(url)
        return f'v_sh601003="{ROW}";'.encode("gbk")

    service = DirectTencentQuoteLiveService(
        TencentQuoteProvider(request_bytes=request, clock=lambda: now),
        clock=lambda: now,
    )

    response = service.fetch_current(("601003", "920000"), 15)

    assert calls == ["https://qt.gtimg.cn/q=sh601003"]
    assert response.requested_count == 2
    assert response.found_count == 1
    assert response.missing_codes == ["920000"]
    assert response.items[0].name == "柳钢股份"
    assert response.items[0].cumulative_volume_shares == 9_920_300
    assert response.items[0].bid_levels[0].volume_shares == 57_100


def test_live_service_returns_stable_upstream_error_when_all_requests_fail() -> None:
    now = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    provider = TencentQuoteProvider(
        request_bytes=lambda _url, _timeout: (_ for _ in ()).throw(OSError("offline")),
        clock=lambda: now,
    )
    service = DirectTencentQuoteLiveService(provider, clock=lambda: now)

    with pytest.raises(TencentQuoteLiveUpstream):
        service.fetch_current(("601003",), 15)


def test_live_service_deduplicates_nothing_and_preserves_requested_order() -> None:
    now = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    provider = TencentQuoteProvider(
        request_bytes=lambda _url, _timeout: f'v_sh601003="{ROW}";'.encode("gbk"),
        clock=lambda: now,
    )
    service = DirectTencentQuoteLiveService(provider, clock=lambda: now)

    response = service.fetch_current(("920000", "601003"), 30)

    assert [item.code for item in response.items] == ["601003"]
    assert response.missing_codes == ["920000"]
