from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import UUID

from market_data_center.hot_money_tushare_catalog import (
    StableSeatTradeFact,
    TushareHotMoneyCatalogSource,
    TushareHotMoneyDetail,
    TushareHotMoneyRosterEntry,
    build_reviewed_hot_money_catalog,
)


class FakeClient:
    def __init__(self) -> None:
        self.detail_params: list[dict[str, str]] = []

    def query(self, api_name, *, params, fields):  # type: ignore[no-untyped-def]
        if api_name == "hm_list":
            return (
                {
                    "name": "游资甲",
                    "desc": "来源说明",
                    "orgs": "营业部甲、营业部甲二部",
                },
            )
        assert api_name == "hm_detail"
        self.detail_params.append(params)
        if params != {
            "start_date": "20260911",
            "end_date": "20260911",
            "limit": "2000",
            "offset": "0",
        }:
            return ()
        return (
            {
                "trade_date": "20260911",
                "ts_code": "600000.SH",
                "ts_name": "浦发银行",
                "buy_amount": Decimal("100"),
                "sell_amount": Decimal("20"),
                "net_amount": Decimal("80"),
                "hm_name": "游资甲",
                "hm_orgs": "营业部甲",
            },
        )


def test_tushare_source_parses_roster_and_transaction_facts() -> None:
    client = FakeClient()
    source = TushareHotMoneyCatalogSource(client)

    roster = source.fetch_roster()
    details = source.fetch_details(date(2026, 9, 11), date(2026, 9, 11))

    assert roster[0].name == "游资甲"
    assert roster[0].organizations == ("营业部甲", "营业部甲二部")
    assert details[0].trade_date == date(2026, 9, 11)
    assert details[0].symbol == "SSE:600000"
    assert details[0].buy_amount == Decimal("100")
    assert client.detail_params == [
        {"start_date": "20260911", "end_date": "20260911", "limit": "2000", "offset": "0"}
    ]


def test_tushare_details_are_fetched_in_calendar_month_windows() -> None:
    client = FakeClient()
    source = TushareHotMoneyCatalogSource(client)

    source.fetch_details(date(2026, 8, 31), date(2026, 9, 2))

    assert client.detail_params == [
        {"start_date": "20260831", "end_date": "20260831", "limit": "2000", "offset": "0"},
        {"start_date": "20260901", "end_date": "20260902", "limit": "2000", "offset": "0"},
    ]


def test_tushare_details_paginate_at_documented_source_cap() -> None:
    class CappedClient(FakeClient):
        def query(self, api_name, *, params, fields):  # type: ignore[no-untyped-def]
            if api_name == "hm_list":
                return super().query(api_name, params=params, fields=fields)
            self.detail_params.append(params)
            if params["offset"] == "0":
                row = {
                    "trade_date": "20260911",
                    "ts_code": "600000.SH",
                    "buy_amount": Decimal("100"),
                    "sell_amount": Decimal("20"),
                    "hm_name": "游资甲",
                    "hm_orgs": "营业部甲",
                }
                return (row,) * 2000
            return ()

    client = CappedClient()

    details = TushareHotMoneyCatalogSource(client).fetch_details(
        date(2026, 9, 11), date(2026, 9, 11)
    )

    assert len(details) == 2000
    assert [params["offset"] for params in client.detail_params] == ["0", "2000"]


def test_catalog_approves_only_unique_transaction_fact_matches() -> None:
    source = TushareHotMoneyCatalogSource(FakeClient())
    roster = source.fetch_roster()
    details = source.fetch_details(date(2026, 9, 11), date(2026, 9, 11))
    seat_id = UUID("00000000-0000-0000-0000-000000000001")

    result = build_reviewed_hot_money_catalog(
        roster,
        details,
        (
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SSE:600000",
                Decimal("100"),
                Decimal("20"),
                seat_id,
            ),
        ),
        catalog_version="tushare-hm-20260914-v1",
        reviewed_at=datetime(2026, 9, 14, 15, tzinfo=UTC),
    )

    assert len(result.catalog.actors) == 1
    assert result.catalog.actors[0].canonical_name == "游资甲"
    assert result.catalog.mappings[0].seat_id == seat_id
    assert result.catalog.mappings[0].actor_code == result.catalog.actors[0].actor_code
    assert result.catalog.mappings[0].valid_from == date(2026, 9, 11)
    assert result.matched_mapping_count == 1
    assert result.unmatched_detail_count == 0


def test_catalog_matches_amounts_at_tushare_hundred_yuan_precision() -> None:
    source = TushareHotMoneyCatalogSource(FakeClient())
    seat_id = UUID("00000000-0000-0000-0000-000000000001")

    result = build_reviewed_hot_money_catalog(
        source.fetch_roster(),
        source.fetch_details(date(2026, 9, 11), date(2026, 9, 11)),
        (
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SSE:600000",
                Decimal("99.51"),
                Decimal("20.49"),
                seat_id,
            ),
        ),
        catalog_version="tushare-hm-20260914-v1",
        reviewed_at=datetime(2026, 9, 14, 15, tzinfo=UTC),
    )

    assert result.catalog.mappings[0].seat_id == seat_id


def test_catalog_skips_cross_actor_seat_matches() -> None:
    client = FakeClient()
    roster = (
        *TushareHotMoneyCatalogSource(client).fetch_roster(),
        TushareHotMoneyRosterEntry("游资乙", "另一来源说明", ("营业部乙",)),
    )
    details = (
        *TushareHotMoneyCatalogSource(client).fetch_details(date(2026, 9, 11), date(2026, 9, 11)),
        TushareHotMoneyDetail(
            date(2026, 9, 11),
            "SZSE:000001",
            Decimal("50"),
            Decimal("0"),
            "游资乙",
            "营业部乙",
        ),
    )
    shared_seat = UUID("00000000-0000-0000-0000-000000000001")

    result = build_reviewed_hot_money_catalog(
        roster,
        details,
        (
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SSE:600000",
                Decimal("100"),
                Decimal("20"),
                shared_seat,
            ),
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SZSE:000001",
                Decimal("50"),
                Decimal("0"),
                shared_seat,
            ),
        ),
        catalog_version="tushare-hm-20260914-v1",
        reviewed_at=datetime(2026, 9, 14, 15, tzinfo=UTC),
    )

    assert result.catalog.mappings == ()
    assert result.conflicting_seat_count == 1


def test_catalog_skips_one_source_organization_resolving_to_multiple_seats() -> None:
    roster = (
        TushareHotMoneyRosterEntry("游资甲", "", ("共同营业部",)),
        TushareHotMoneyRosterEntry("游资乙", "", ("共同营业部",)),
    )
    details = (
        TushareHotMoneyDetail(
            date(2026, 9, 11),
            "SSE:600000",
            Decimal("100"),
            Decimal("20"),
            "游资甲",
            "共同营业部",
        ),
        TushareHotMoneyDetail(
            date(2026, 9, 11),
            "SZSE:000001",
            Decimal("50"),
            Decimal("0"),
            "游资乙",
            "共同营业部",
        ),
    )
    seat_one = UUID("00000000-0000-0000-0000-000000000001")
    seat_two = UUID("00000000-0000-0000-0000-000000000002")

    result = build_reviewed_hot_money_catalog(
        roster,
        details,
        (
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SSE:600000",
                Decimal("100"),
                Decimal("20"),
                seat_one,
            ),
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SZSE:000001",
                Decimal("50"),
                Decimal("0"),
                seat_two,
            ),
        ),
        catalog_version="tushare-hm-20260914-v1",
        reviewed_at=datetime(2026, 9, 14, 15, tzinfo=UTC),
    )

    assert result.catalog.mappings == ()
    assert result.conflicting_organization_count == 1


def test_catalog_skips_detail_matching_multiple_stable_seats() -> None:
    source = TushareHotMoneyCatalogSource(FakeClient())
    roster = source.fetch_roster()
    details = source.fetch_details(date(2026, 9, 11), date(2026, 9, 11))

    result = build_reviewed_hot_money_catalog(
        roster,
        details,
        (
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SSE:600000",
                Decimal("100"),
                Decimal("20"),
                UUID("00000000-0000-0000-0000-000000000001"),
            ),
            StableSeatTradeFact(
                date(2026, 9, 11),
                "SSE:600000",
                Decimal("100"),
                Decimal("20"),
                UUID("00000000-0000-0000-0000-000000000002"),
            ),
        ),
        catalog_version="tushare-hm-20260914-v1",
        reviewed_at=datetime(2026, 9, 14, 15, tzinfo=UTC),
    )

    assert result.catalog.mappings == ()
    assert result.ambiguous_detail_count == 1
