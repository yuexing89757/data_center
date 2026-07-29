from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal

from market_data_center.domain import (
    ClassificationCatalogSnapshotRecord,
    ClassificationMemberSnapshotRecord,
    ClassificationType,
    CorporateActionStatus,
    DistributionRecord,
    Exchange,
    RightsIssueRecord,
    SecurityStatus,
    ShareCapitalRecord,
    TradeStatus,
)
from market_data_center.providers import AKShareProvider
from market_data_center.providers.akshare import TabularResult


class FakeFrame:
    def __init__(self, rows: Sequence[Mapping[str, object]]) -> None:
        self._rows = rows
        self.columns = tuple(rows[0]) if rows else ()

    def to_dict(self, orient: str) -> object:
        assert orient == "records"
        return list(self._rows)


class _BaseFakeClient:
    def __init__(self) -> None:
        self.daily_arguments: dict[str, str] = {}

    def stock_info_a_code_name(self) -> TabularResult:
        return FakeFrame(({"code": "600000", "name": "浦发银行"},))

    def tool_trade_date_hist_sina(self) -> TabularResult:
        return FakeFrame(
            (
                {"trade_date": date(1990, 12, 19)},
                {"trade_date": date(2026, 7, 24)},
                {"trade_date": date(2026, 7, 27)},
            )
        )


class FakeClient(_BaseFakeClient):
    def stock_zh_a_hist(
        self, *, symbol: str, period: str, start_date: str, end_date: str, adjust: str
    ) -> TabularResult:
        self.daily_arguments = {
            "symbol": symbol,
            "period": period,
            "start_date": start_date,
            "end_date": end_date,
            "adjust": adjust,
        }
        return FakeFrame(
            (
                {
                    "日期": date(2026, 7, 24),
                    "股票代码": "600000",
                    "开盘": "10.00",
                    "收盘": "10.50",
                    "最高": "11.00",
                    "最低": "9.50",
                    "成交量": "100",
                    "成交额": "1050.00",
                    "涨跌额": "0.60",
                },
            )
        )

    def stock_zh_a_gbjg_em(self, *, symbol: str) -> TabularResult:
        return FakeFrame(
            (
                {
                    "变更日期": date(2024, 1, 15),
                    "总股本": 1_000_000,
                    "流通受限股份": 100_000,
                    "已流通股份": 900_000,
                    "已上市流通A股": 900_000,
                    "变动原因": "回购",
                },
            )
        )

    def stock_fhps_detail_em(self, *, symbol: str) -> TabularResult:
        return FakeFrame(
            (
                {
                    "报告期": date(2023, 12, 31),
                    "送转股份-送股比例": "1",
                    "送转股份-转股比例": "2",
                    "现金分红-现金分红比例": "3.5",
                    "预案公告日": date(2024, 3, 1),
                    "股权登记日": date(2024, 6, 5),
                    "除权除息日": date(2024, 6, 6),
                    "方案进度": "实施分配",
                    "最新公告日期": date(2024, 5, 31),
                },
            )
        )

    def stock_history_dividend_detail(self, *, symbol: str, indicator: str) -> TabularResult:
        assert indicator == "配股"
        return FakeFrame(
            (
                {
                    "公告日期": date(2020, 1, 2),
                    "配股方案": "2.5",
                    "配股价格": "8.5",
                    "基准股本": "1000000",
                    "除权日": date(2020, 1, 10),
                    "股权登记日": date(2020, 1, 9),
                    "缴款起始日": date(2020, 1, 10),
                    "缴款终止日": date(2020, 1, 16),
                    "配股上市日": date(2020, 2, 1),
                    "募集资金合计": "2125000",
                },
            )
        )

    def stock_board_industry_name_em(self) -> TabularResult:
        return FakeFrame(({"板块名称": "银行", "板块代码": "BK0475"},))

    def stock_board_concept_name_em(self) -> TabularResult:
        return FakeFrame(({"板块名称": "融资融券", "板块代码": "BK0655"},))

    def stock_board_industry_cons_em(self, *, symbol: str) -> TabularResult:
        return FakeFrame(({"代码": "600000", "名称": "浦发银行"},))

    def stock_board_concept_cons_em(self, *, symbol: str) -> TabularResult:
        return FakeFrame(({"代码": "600000", "名称": "浦发银行"},))


class EmptyRightsFakeClient(FakeClient):
    def stock_history_dividend_detail(self, *, symbol: str, indicator: str) -> TabularResult:
        return FakeFrame(())


class ZeroBaseSharesFakeClient(FakeClient):
    def stock_history_dividend_detail(self, *, symbol: str, indicator: str) -> TabularResult:
        frame = super().stock_history_dividend_detail(symbol=symbol, indicator=indicator)
        rows = frame.to_dict(orient="records")
        assert isinstance(rows, list)
        rows[0]["基准股本"] = "0"
        return FakeFrame(rows)


class MissingDistributionFakeClient(FakeClient):
    def stock_fhps_detail_em(self, *, symbol: str) -> TabularResult:
        raise TypeError("'NoneType' object is not subscriptable")


def test_security_mapping_marks_current_directory_members_as_listed() -> None:
    record = AKShareProvider(FakeClient()).fetch_securities().records[0]

    assert record.symbol == "SSE:600000"
    assert record.exchange is Exchange.SSE
    assert record.status is SecurityStatus.LISTED
    assert record.ipo_date is None
    assert record.source_code == "akshare"


def test_trading_calendar_expands_to_natural_days() -> None:
    batch = AKShareProvider(FakeClient()).fetch_trading_calendar(
        date(2026, 7, 24), date(2026, 7, 26)
    )

    assert [record.is_trading_day for record in batch.records] == [True, False, False]


def test_daily_bar_requests_unadjusted_data_and_maps_decimal_values() -> None:
    client = FakeClient()
    record = (
        AKShareProvider(client)
        .fetch_daily_bars("600000", date(2026, 7, 24), date(2026, 7, 24))
        .records[0]
    )

    assert client.daily_arguments["adjust"] == ""
    assert client.daily_arguments["period"] == "daily"
    assert record.close == Decimal("10.50")
    assert record.previous_close == Decimal("9.90")
    assert record.trade_status is TradeStatus.TRADING
    assert record.is_st is None


def test_standard_symbol_maps_to_akshare_source_symbol() -> None:
    assert AKShareProvider(FakeClient()).source_symbol("SSE:600000") == "600000"


def test_capital_mapping_normalizes_units_and_record_types() -> None:
    batch = AKShareProvider(FakeClient()).fetch_capital("SSE:600000")

    assert len(batch.raw_rows) == 3
    share_capital, distribution, rights_issue = batch.records
    assert isinstance(share_capital, ShareCapitalRecord)
    assert share_capital.total_shares == 1_000_000
    assert isinstance(distribution, DistributionRecord)
    assert distribution.cash_dividend_per_share == Decimal("0.35")
    assert distribution.bonus_share_ratio == Decimal("0.1")
    assert distribution.transfer_share_ratio == Decimal("0.2")
    assert distribution.status is CorporateActionStatus.IMPLEMENTED
    assert isinstance(rights_issue, RightsIssueRecord)
    assert rights_issue.rights_ratio == Decimal("0.25")
    assert rights_issue.rights_price == Decimal("8.5")


def test_capital_mapping_accepts_an_empty_optional_rights_table() -> None:
    records = AKShareProvider(EmptyRightsFakeClient()).fetch_capital("600000").records

    assert [type(record).__name__ for record in records] == [
        "ShareCapitalRecord",
        "DistributionRecord",
    ]


def test_capital_mapping_treats_zero_optional_base_shares_as_unknown() -> None:
    records = AKShareProvider(ZeroBaseSharesFakeClient()).fetch_capital("000001").records

    rights_issue = records[-1]
    assert isinstance(rights_issue, RightsIssueRecord)
    assert rights_issue.base_shares is None


def test_capital_mapping_treats_akshare_missing_distribution_as_empty() -> None:
    records = AKShareProvider(MissingDistributionFakeClient()).fetch_capital("688031").records

    assert [type(record).__name__ for record in records] == [
        "ShareCapitalRecord",
        "RightsIssueRecord",
    ]


def test_classification_catalog_and_members_are_complete_snapshots() -> None:
    provider = AKShareProvider(FakeClient())

    catalog = provider.fetch_classification_catalog("industry", date(2026, 7, 29)).records[0]
    members = provider.fetch_classification_members(
        "industry", "BK0475", date(2026, 7, 29)
    ).records[0]

    assert isinstance(catalog, ClassificationCatalogSnapshotRecord)
    assert catalog.classification_type is ClassificationType.INDUSTRY
    assert catalog.definitions[0].name == "银行"
    assert isinstance(members, ClassificationMemberSnapshotRecord)
    assert members.members == ("SSE:600000",)
