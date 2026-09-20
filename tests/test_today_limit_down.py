from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo

import pandas as pd
import pytest

from market_data_center.domain.today_limit_down import TodayLimitDownMember
from market_data_center.providers.akshare_limit_down import AkshareCurrentDayLimitDownProvider
from market_data_center.providers.contracts import ProviderError


class Client:
    def stock_zt_pool_dtgc_em(self, *, date: str) -> object:
        assert date == "20260918"
        return pd.DataFrame(
            [
                {
                    "代码": "000001",
                    "名称": "历史名称仅供核验",
                    "最后封板时间": "145959",
                    "封单资金": "1200000",
                    "连续跌停": 2,
                    "开板次数": 1,
                }
            ]
        )


def test_provider_maps_down_source_and_replays_raw_rows() -> None:
    batch = AkshareCurrentDayLimitDownProvider(Client()).fetch_limit_down_pool(date(2026, 9, 18))

    assert batch.raw_rows[0]["代码"] == "000001"
    assert batch.records == batch.records
    record = batch.records[0]
    assert record.symbol == "SZSE:000001"
    assert record.first_limit_down_at is None
    assert record.last_limit_down_at == datetime(
        2026, 9, 18, 14, 59, 59, tzinfo=ZoneInfo("Asia/Shanghai")
    )
    assert record.source_reported_sealed_funds_cny == Decimal("1200000")
    assert (record.open_count, record.consecutive_limit_down_days) == (1, 2)


def test_provider_rejects_negative_source_funds() -> None:
    class InvalidClient(Client):
        def stock_zt_pool_dtgc_em(self, *, date: str) -> object:
            frame = super().stock_zt_pool_dtgc_em(date=date)
            frame.loc[0, "封单资金"] = "-1"
            return frame

    batch = AkshareCurrentDayLimitDownProvider(InvalidClient()).fetch_limit_down_pool(
        date(2026, 9, 18)
    )
    with pytest.raises(ProviderError, match="nonnegative"):
        _ = batch.records


def _member(**overrides: object) -> TodayLimitDownMember:
    values: dict[str, object] = dict(
        symbol="SZSE:000001",
        code="000001",
        historical_name="平安银行",
        previous_close=Decimal("10"),
        close=Decimal("9"),
        limit_price=Decimal("9"),
        change_percent=Decimal("-10"),
        free_float_shares=100,
        free_float_market_cap_cny=Decimal("900"),
        closing_ask1_price=Decimal("9"),
        closing_ask1_volume_shares=20,
        closing_ask1_sealing_amount_cny=Decimal("180"),
    )
    values.update(overrides)
    return TodayLimitDownMember(**values)  # type: ignore[arg-type]


def test_member_requires_exact_down_limit_and_ask1_amount() -> None:
    assert _member().limit_down_duration_seconds is None
    with pytest.raises(ValueError, match="exactly equal"):
        _member(close=Decimal("9.01"))
    with pytest.raises(ValueError, match="ask-1 sealing amount"):
        _member(closing_ask1_sealing_amount_cny=Decimal("181"))
    with pytest.raises(ValueError, match="free-float market cap"):
        _member(free_float_market_cap_cny=Decimal("901"))
