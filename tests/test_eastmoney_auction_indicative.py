from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pytest

from market_data_center.domain.auction_indicative import SourceDisplayClassification
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.eastmoney_auction import EastmoneyAuctionIndicativeProvider

SHANGHAI = ZoneInfo("Asia/Shanghai")
TODAY = date(2026, 8, 14)
NOW = datetime(2026, 8, 14, 10, 0, tzinfo=SHANGHAI)


def test_adapter_keeps_only_auction_window_and_converts_lots_to_shares() -> None:
    seen: list[str] = []

    def request(url: str, timeout: float) -> dict[str, object]:
        seen.append(url)
        assert timeout == 8
        return {
            "rc": 0,
            "data": {
                "details": [
                    "09:14:59,10.00,1,0,1",
                    "09:15:05,10.01,2,0,1",
                    "09:25:02,10.02,289,76,2",
                    "09:26:00,10.03,3,0,4",
                ]
            },
        }

    batch = EastmoneyAuctionIndicativeProvider(request).fetch_current_day(
        "SSE:688796", TODAY, now=NOW
    )
    records = tuple(batch.records)
    assert "secid=1.688796" in seen[0]
    assert [record.source_sequence for record in records] == [1, 2]
    assert records[0].indicative_price == Decimal("10.01")
    assert records[0].displayed_volume_shares == 200
    assert records[0].source_display_classification is SourceDisplayClassification.INTERNAL
    assert records[1].displayed_volume_shares == 28_900
    assert records[1].source_display_classification is SourceDisplayClassification.EXTERNAL
    assert batch.raw_rows[2]["source_auxiliary"] == "76"


def test_adapter_rejects_history_unknown_exchange_and_fractional_lots() -> None:
    provider = EastmoneyAuctionIndicativeProvider(
        lambda _url, _timeout: {"rc": 0, "data": {"details": ["09:20:00,10,1.5,0,4"]}}
    )
    with pytest.raises(ProviderError, match="current Shanghai date only"):
        provider.fetch_current_day("SSE:688796", date(2026, 8, 13), now=NOW)
    with pytest.raises(ValueError, match="SSE"):
        provider.fetch_current_day("BSE:920000", TODAY, now=NOW)
    with pytest.raises(ProviderError, match="invalid values"):
        tuple(provider.fetch_current_day("SZSE:000001", TODAY, now=NOW).records)
    with pytest.raises(ProviderError, match="not complete before 09:26"):
        provider.fetch_current_day(
            "SSE:688796", TODAY, now=datetime(2026, 8, 14, 9, 25, tzinfo=SHANGHAI)
        )


def test_adapter_rejects_malformed_or_potentially_truncated_response() -> None:
    malformed = EastmoneyAuctionIndicativeProvider(
        lambda _url, _timeout: {"rc": 0, "data": {"details": ["bad"]}}
    )
    with pytest.raises(ProviderError, match="unexpected shape"):
        malformed.fetch_current_day("SSE:600000", TODAY, now=NOW)

    truncated = EastmoneyAuctionIndicativeProvider(
        lambda _url, _timeout: {
            "rc": 0,
            "data": {"details": ["09:20:00,10,1,0,4"] * 5000},
        }
    )
    with pytest.raises(ProviderError, match="truncated"):
        truncated.fetch_current_day("SSE:600000", TODAY, now=NOW)
