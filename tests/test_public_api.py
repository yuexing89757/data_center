from datetime import UTC, date, datetime
from decimal import Decimal
from types import SimpleNamespace
from typing import Literal

import pytest
from fastapi.testclient import TestClient
from pydantic import SecretStr, ValidationError
from sqlalchemy.exc import DBAPIError

from market_data_center.domain import (
    ClassificationType,
    Exchange,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)
from market_data_center.public_api import create_app
from market_data_center.public_api.board_index_bias_live import (
    BoardIndexBiasLiveBusy,
    BoardIndexBiasLivePersistence,
    BoardIndexBiasLiveUpstream,
)
from market_data_center.public_api.models import (
    AuctionIndicativeDetailItem,
    AuctionIndicativeDetailResponse,
    AuctionIndicativeQuality,
    AuctionOnePriceLimitItem,
    AuctionOnePriceLimitResponse,
    BoardIndexBiasResponse,
    CallAuctionMarketSnapshotItem,
    CallAuctionMarketSnapshotResponse,
    ClassificationMembersResponse,
    DailyBarItem,
    DailyLimitUpListItem,
    DailyLimitUpListResponse,
    DailyLimitUpQualitySummary,
    LimitUpPoolItem,
    LimitUpPoolOmissionReasons,
    LimitUpPoolResponse,
    SecurityItem,
    TopGainer20dItem,
    TopGainer20dOmissions,
    TopGainers20dResponse,
)
from market_data_center.public_api.queries import (
    BoardIndexBiasNotReady,
    PostgreSQLPublicQueryService,
    PublicQueryNotFound,
    PublicQueryUnavailable,
    _raise_safe_query_error,
)
from market_data_center.settings import ApiSettings

API_KEY = "test-api-key-00000000000000000000"


class FakeQueryService:
    def __init__(self) -> None:
        self.ready_error: Exception | None = None
        self.classification_error: Exception | None = None
        self.security_calls: list[tuple[str, int]] = []
        self.daily_bar_calls: list[tuple[str, date, date, int]] = []
        self.limit_up_calls: list[tuple[date, int | None, int]] = []
        self.daily_limit_up_calls: list[tuple[date, int | None, int, int]] = []
        self.call_auction_market_snapshot_calls: list[tuple[date, tuple[str, ...]]] = []
        self.call_auction_market_series_snapshot_calls: list[tuple[date, tuple[str, ...]]] = []
        self.top_gainer_calls: list[tuple[date | None, int]] = []
        self.close_price_new_highs_120d_calls = 0
        self.board_index_bias_calls = 0
        self.board_index_bias_error: Exception | None = None
        self.board_index_bias_live_calls = 0
        self.board_index_bias_live_error: Exception | None = None
        self.auction_one_price_limit_calls: list[date | None] = []
        self.auction_indicative_database_calls: list[tuple[str, int, int]] = []
        self.auction_indicative_database_error: Exception | None = PublicQueryNotFound("not stored")
        self.auction_indicative_calls: list[tuple[str, date, int, int]] = []

    def ready(self) -> None:
        if self.ready_error is not None:
            raise self.ready_error

    def search_securities(self, query: str, limit: int) -> tuple[SecurityItem, ...]:
        self.security_calls.append((query, limit))
        return (
            SecurityItem(
                symbol="SSE:600000",
                code="600000",
                exchange=Exchange.SSE,
                current_name="浦发银行",
                security_type=SecurityType.STOCK,
                status=SecurityStatus.LISTED,
                ipo_date=date(1999, 11, 10),
                delisting_date=None,
            ),
        )

    def daily_bars(
        self, symbol: str, start_date: date, end_date: date, limit: int
    ) -> tuple[DailyBarItem, ...]:
        self.daily_bar_calls.append((symbol, start_date, end_date, limit))
        return (
            DailyBarItem(
                symbol=symbol,
                trade_date=start_date,
                open=Decimal("10.10"),
                high=Decimal("10.30"),
                low=Decimal("10.00"),
                close=Decimal("10.20"),
                previous_close=Decimal("10.05"),
                volume=123_400,
                amount=Decimal("1258680.00"),
                trade_status=TradeStatus.TRADING,
                is_st=False,
            ),
        )

    def classification_members(
        self,
        namespace: str,
        classification_type: ClassificationType,
        classification_code: str,
        as_of_date: date,
        limit: int,
    ) -> ClassificationMembersResponse:
        if self.classification_error is not None:
            raise self.classification_error
        assert (namespace, classification_type, classification_code, as_of_date, limit) == (
            "tdx",
            ClassificationType.INDUSTRY,
            "T1001",
            date(2026, 7, 29),
            5000,
        )
        return ClassificationMembersResponse(
            snapshot_date=date(2026, 7, 29),
            member_count=2,
            returned_count=2,
            members=["SSE:600000", "SZSE:000001"],
        )

    def limit_up_pool(
        self, trade_date: date, version: int | None, limit: int
    ) -> LimitUpPoolResponse:
        self.limit_up_calls.append((trade_date, version, limit))
        return LimitUpPoolResponse(
            snapshot_id="11111111-1111-1111-1111-111111111111",
            calculation_id="22222222-2222-2222-2222-222222222222",
            trade_date=trade_date,
            effective_trade_date=date(2026, 8, 3),
            version=version or 2,
            rule_version="CN_MAINBOARD_2026_07_06",
            algorithm_version="1.0.0",
            input_hash="0" * 64,
            generated_at=datetime(2026, 8, 1, tzinfo=UTC),
            total_candidate_count=3,
            valid_count=2,
            returned_count=1,
            omitted_count=1,
            has_more=True,
            omission_reasons=LimitUpPoolOmissionReasons(
                missing_name=0,
                missing_close=1,
                missing_free_float_shares=1,
            ),
            items=[
                LimitUpPoolItem(
                    symbol="SSE:600000",
                    code="600000",
                    name="浦发银行",
                    free_float_market_cap_cny=Decimal("61200000.000000"),
                )
            ],
        )

    def daily_limit_up_list(
        self, trade_date: date, version: int | None, offset: int, limit: int
    ) -> DailyLimitUpListResponse:
        self.daily_limit_up_calls.append((trade_date, version, offset, limit))
        return DailyLimitUpListResponse(
            snapshot_id="33333333-3333-3333-3333-333333333333",
            calculation_id="44444444-4444-4444-4444-444444444444",
            trade_date=trade_date,
            version=version or 1,
            status="partial",
            rule_version="cn_a_mainboard_limit_up_v1",
            algorithm_version="today_limit_up_snapshot_v1",
            input_hash="1" * 64,
            source_ingestion_id="55555555-5555-5555-5555-555555555555",
            generated_at=datetime(2026, 8, 11, 14, 0, tzinfo=UTC),
            candidate_count=55,
            member_count=55,
            rejected_count=0,
            offset=offset,
            returned_count=1,
            has_more=True,
            quality=DailyLimitUpQualitySummary(
                total_findings=2,
                by_rule={"missing_source_observation": 2},
            ),
            items=[
                DailyLimitUpListItem(
                    symbol="SSE:600000",
                    code="600000",
                    name="浦发银行",
                    previous_close=Decimal("8.45"),
                    close=Decimal("9.29"),
                    limit_price=Decimal("9.29"),
                    change_percent=Decimal("9.9408284024"),
                    free_float_shares=100000000,
                    free_float_market_cap_cny=Decimal("929000000"),
                    first_limit_up_at=datetime(2026, 8, 11, 9, 35, tzinfo=UTC),
                    last_limit_up_at=datetime(2026, 8, 11, 14, 30, tzinfo=UTC),
                    open_count=1,
                    limit_up_duration_seconds=None,
                    duration_semantics="unavailable_without_event_stream",
                    source_reported_sealed_funds_cny=Decimal("1200000"),
                    closing_bid1_price=Decimal("9.29"),
                    closing_bid1_volume_shares=100000,
                    closing_bid2_price=Decimal("9.28"),
                    closing_bid2_volume_shares=20000,
                    closing_bid3_price=None,
                    closing_bid3_volume_shares=None,
                    closing_bid4_price=None,
                    closing_bid4_volume_shares=None,
                    closing_bid5_price=None,
                    closing_bid5_volume_shares=None,
                    closing_bid1_sealing_amount_cny=Decimal("929000"),
                    daily_bar_ingestion_id="66666666-6666-6666-6666-666666666666",
                    indicator_ingestion_id="77777777-7777-7777-7777-777777777777",
                    name_ingestion_id="88888888-8888-8888-8888-888888888888",
                    pool_calculation_id="99999999-9999-9999-9999-999999999999",
                    source_observation_ingestion_id="aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                    source_observation_raw_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
                    order_book_ingestion_id="cccccccc-cccc-cccc-cccc-cccccccccccc",
                    volume=62542540,
                    amount_cny=Decimal("580000000"),
                    free_float_turnover_rate_pct=Decimal("2.5"),
                    consecutive_limit_up_days=3,
                )
            ],
        )

    def call_auction_market_snapshots(
        self, trade_date: date, codes: tuple[str, ...]
    ) -> CallAuctionMarketSnapshotResponse:
        self.call_auction_market_snapshot_calls.append((trade_date, codes))
        return CallAuctionMarketSnapshotResponse(
            trade_date=trade_date,
            ingestion_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
            ingestion_status="partial",
            requested_count=2,
            returned_count=1,
            missing_codes=["000001"],
            items=[
                CallAuctionMarketSnapshotItem(
                    symbol="SSE:600000",
                    code="600000",
                    observed_at=datetime(2026, 8, 13, 1, 26, tzinfo=UTC),
                    last_price=Decimal("10.1200"),
                    previous_close=Decimal("10.0000"),
                    high_price=Decimal("10.1500"),
                    low_price=Decimal("9.9800"),
                    cumulative_volume=123400,
                    cumulative_amount=Decimal("1248808.0000"),
                    bid1_price=Decimal("10.1200"),
                    bid1_volume=560200,
                    bid2_price=None,
                    bid2_volume=10743200,
                    ask1_price=None,
                    ask1_volume=0,
                    ask2_price=None,
                    ask2_volume=13300,
                    seal_amount=Decimal("5673224.0000"),
                )
            ],
        )

    def call_auction_market_series_snapshots(
        self, trade_date: date, codes: tuple[str, ...]
    ) -> object:
        self.call_auction_market_series_snapshot_calls.append((trade_date, codes))
        return {
            "trade_date": trade_date,
            "session_id": "11111111-1111-1111-1111-111111111111",
            "session_status": "partial",
            "expected_rounds": 32,
            "returned_rounds": 1,
            "requested_count": 2,
            "rounds": [
                {
                    "sample_seq": 0,
                    "scheduled_at": "2026-08-14T01:15:00Z",
                    "collected_at": "2026-08-14T01:15:02Z",
                    "round_status": "partial",
                    "selected_ingestion_id": "22222222-2222-2222-2222-222222222222",
                    "requested_count": 2,
                    "returned_count": 1,
                    "missing_codes": ["000001"],
                    "items": [
                        {
                            "symbol": "SSE:600000",
                            "code": "600000",
                            "batch_code": "091500",
                            "observed_at": "2026-08-14T01:15:01Z",
                            "last_price": "10.1200",
                            "previous_close": "10.0000",
                            "high_price": "10.1500",
                            "low_price": "9.9800",
                            "cumulative_volume": 123400,
                            "cumulative_amount": "1248808.0000",
                            "value_semantics": "auction_indicative",
                            "bid1_price": "10.0000",
                            "bid1_volume": 100,
                            "bid2_price": None,
                            "bid2_volume": 10743200,
                            "bid3_price": None,
                            "bid3_volume": None,
                            "bid4_price": None,
                            "bid4_volume": None,
                            "bid5_price": None,
                            "bid5_volume": None,
                            "ask1_price": "10.0100",
                            "ask1_volume": 100,
                            "ask2_price": None,
                            "ask2_volume": 13300,
                            "ask3_price": None,
                            "ask3_volume": None,
                            "ask4_price": None,
                            "ask4_volume": None,
                            "ask5_price": None,
                            "ask5_volume": None,
                        }
                    ],
                }
            ],
        }

    def top_gainers_20d(self, end_date: date | None, limit: int) -> TopGainers20dResponse:
        self.top_gainer_calls.append((end_date, limit))
        return TopGainers20dResponse(
            start_trade_date=date(2026, 7, 16),
            end_trade_date=end_date or date(2026, 8, 13),
            trading_session_count=20,
            return_interval_count=19,
            total_candidate_count=2,
            eligible_count=1,
            omitted_count=1,
            returned_count=1,
            omissions=TopGainer20dOmissions(
                missing_start_bar=1,
                missing_end_bar=0,
                non_trading_bar=0,
                nonpositive_price=0,
                missing_name=0,
            ),
            items=[
                TopGainer20dItem(
                    symbol="SSE:600000",
                    code="600000",
                    name="浦发银行",
                    start_trade_date=date(2026, 7, 16),
                    end_trade_date=end_date or date(2026, 8, 13),
                    start_close=Decimal("10"),
                    end_close=Decimal("12"),
                    return_pct=Decimal("20"),
                )
            ],
        )

    def close_price_new_highs_120d(self) -> object:
        self.close_price_new_highs_120d_calls += 1
        return {
            "trade_date": date(2026, 8, 14),
            "window_trading_session_count": 120,
            "comparison_session_count": 119,
            "total_candidate_count": 2,
            "eligible_history_count": 1,
            "omitted_count": 1,
            "returned_count": 1,
            "omissions": {
                "incomplete_history": 1,
                "non_trading_bar": 0,
                "nonpositive_price": 0,
                "missing_name": 0,
            },
            "items": [
                {
                    "symbol": "SSE:600000",
                    "code": "600000",
                    "name": "浦发银行",
                    "close": Decimal("12.50"),
                    "previous_119d_high": Decimal("12.00"),
                    "breakout_pct": Decimal("4.1666666667"),
                }
            ],
        }

    def board_index_bias_latest(self) -> object:
        self.board_index_bias_calls += 1
        if self.board_index_bias_error is not None:
            raise self.board_index_bias_error
        return {
            "board_id": "THS:883423",
            "board_code": "883423",
            "board_name": "沪深主板昨日涨停",
            "trade_date": date(2026, 8, 14),
            "close": Decimal("1234.5600"),
            "moving_average_5": Decimal("1220.110000"),
            "bias_5_pct": Decimal("1.184319"),
            "previous_trade_date": date(2026, 8, 13),
            "previous_bias_5_pct": Decimal("0.932150"),
            "bias_direction": "up",
            "window_trading_days": 30,
            "bias_sample_count": 30,
            "highest_bias_5_pct": Decimal("4.521300"),
            "highest_bias_trade_date": date(2026, 8, 6),
            "lowest_bias_5_pct": Decimal("-2.861700"),
            "lowest_bias_trade_date": date(2026, 7, 22),
            "algorithm_version": "board_index_bias_v1",
            "data_origin": "database",
            "persistence_status": "persisted",
            "fetched_at": datetime(2026, 8, 15, 3, 26, 46, tzinfo=UTC),
        }

    def auction_one_price_limits(self, trade_date: date | None) -> AuctionOnePriceLimitResponse:
        self.auction_one_price_limit_calls.append(trade_date)
        day = trade_date or date(2026, 8, 13)
        item = AuctionOnePriceLimitItem(
            symbol="SSE:600000",
            code="600000",
            name="浦发银行",
            direction="up",
            observed_at=datetime(2026, 8, 13, 1, 26, tzinfo=UTC),
            indicated_price=Decimal("11"),
            limit_price=Decimal("11"),
            previous_close=Decimal("10"),
            cumulative_volume=100,
            cumulative_amount=Decimal("1100"),
            seal_amount=Decimal("1100"),
        )
        return AuctionOnePriceLimitResponse(
            trade_date=day,
            ingestion_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
            ingestion_status="partial",
            price_limit_calculation_id=None,
            price_limit_rule_version="CN_MAINBOARD_2026_07_06",
            price_limit_algorithm_version="1.0.0",
            calculation_mode="realtime_read",
            snapshot_window="09:25:50-09:29:59 Asia/Shanghai",
            candidate_count=2,
            omitted_incomplete_count=1,
            up_count=1,
            down_count=0,
            up=[item],
            down=[],
        )

    def auction_indicative_details(
        self, symbol: str, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse:
        self.auction_indicative_database_calls.append((symbol, offset, limit))
        if self.auction_indicative_database_error is not None:
            raise self.auction_indicative_database_error
        return _auction_indicative_response(symbol=symbol, data_origin="database")


class FakeLiveAuctionService:
    def __init__(self, query_service: FakeQueryService) -> None:
        self._query_service = query_service

    def fetch(
        self, symbol: str, trade_date: date, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse:
        self._query_service.auction_indicative_calls.append((symbol, trade_date, offset, limit))
        return AuctionIndicativeDetailResponse(
            symbol=symbol,
            trade_date=trade_date,
            fetched_at=datetime(2026, 8, 14, 1, 26, tzinfo=UTC),
            source="eastmoney",
            live_provider_derived=True,
            data_origin="eastmoney_live",
            cache_hit=False,
            persistence_status="queued",
            ingestion_id="11111111-1111-1111-1111-111111111111",
            raw_id="22222222-2222-2222-2222-222222222222",
            input_hash="a" * 64,
            semantics="auction_virtual_indicative_matching_detail",
            is_exchange_trade_tick=False,
            is_order_by_order=False,
            total_count=2,
            offset=offset,
            returned_count=2,
            has_more=False,
            quality=AuctionIndicativeQuality(
                status="complete",
                source_row_count=3,
                accepted_auction_row_count=2,
                source_display_classification_trusted=False,
                raw_captured=True,
                database_persistence="queued",
            ),
            items=[
                AuctionIndicativeDetailItem(
                    observed_at=datetime(2026, 8, 14, 1, 20, tzinfo=UTC),
                    source_sequence=1,
                    indicative_price=Decimal("134.01"),
                    displayed_volume_shares=300,
                    source_display_classification="external",
                ),
                AuctionIndicativeDetailItem(
                    observed_at=datetime(2026, 8, 14, 1, 15, 5, tzinfo=UTC),
                    source_sequence=0,
                    indicative_price=Decimal("133.99"),
                    displayed_volume_shares=200,
                    source_display_classification="unknown",
                ),
            ],
        )

    def fetch_current(
        self, symbol: str, offset: int, limit: int
    ) -> AuctionIndicativeDetailResponse:
        return self.fetch(symbol, date(2026, 8, 14), offset, limit)


class FakeBoardIndexBiasLiveService:
    def __init__(self, query_service: FakeQueryService) -> None:
        self._query_service = query_service

    def fetch_current(self) -> BoardIndexBiasResponse:
        self._query_service.board_index_bias_live_calls += 1
        if self._query_service.board_index_bias_live_error is not None:
            raise self._query_service.board_index_bias_live_error
        return BoardIndexBiasResponse(
            board_id="THS:883423",
            board_code="883423",
            board_name="沪深主板昨日涨停",
            trade_date=date(2026, 8, 14),
            close=Decimal("1234.5600"),
            moving_average_5=Decimal("1220.110000"),
            bias_5_pct=Decimal("1.184319"),
            previous_trade_date=date(2026, 8, 13),
            previous_bias_5_pct=Decimal("0.932150"),
            bias_direction="up",
            window_trading_days=30,
            bias_sample_count=30,
            highest_bias_5_pct=Decimal("4.521300"),
            highest_bias_trade_date=date(2026, 8, 6),
            lowest_bias_5_pct=Decimal("-2.861700"),
            lowest_bias_trade_date=date(2026, 7, 22),
            algorithm_version="board_index_bias_v1",
            data_origin="ths_live",
            persistence_status="queued",
            fetched_at=datetime(2026, 8, 15, 3, 26, 46, tzinfo=UTC),
        )


def _auction_indicative_response(
    *, symbol: str, data_origin: Literal["database", "eastmoney_live"]
) -> AuctionIndicativeDetailResponse:
    persisted = data_origin == "database"
    return AuctionIndicativeDetailResponse(
        symbol=symbol,
        trade_date=date(2026, 8, 14),
        fetched_at=datetime(2026, 8, 14, 1, 26, tzinfo=UTC),
        source="eastmoney",
        live_provider_derived=True,
        data_origin=data_origin,
        cache_hit=False,
        persistence_status="persisted" if persisted else "queued",
        version=1 if persisted else None,
        ingestion_status="succeeded" if persisted else None,
        ingestion_id="11111111-1111-1111-1111-111111111111",
        raw_id="22222222-2222-2222-2222-222222222222",
        input_hash="a" * 64,
        semantics="auction_virtual_indicative_matching_detail",
        is_exchange_trade_tick=False,
        is_order_by_order=False,
        total_count=2,
        offset=0,
        returned_count=2,
        has_more=False,
        quality=AuctionIndicativeQuality(
            status="complete",
            source_row_count=3,
            accepted_auction_row_count=2,
            source_display_classification_trusted=False,
            raw_captured=True,
            database_persistence="persisted" if persisted else "queued",
        ),
        items=[
            AuctionIndicativeDetailItem(
                observed_at=datetime(2026, 8, 14, 1, 20, tzinfo=UTC),
                source_sequence=1,
                indicative_price=Decimal("134.01"),
                displayed_volume_shares=300,
                source_display_classification="external",
            ),
            AuctionIndicativeDetailItem(
                observed_at=datetime(2026, 8, 14, 1, 15, 5, tzinfo=UTC),
                source_sequence=0,
                indicative_price=Decimal("133.99"),
                displayed_volume_shares=200,
                source_display_classification="unknown",
            ),
        ],
    )


def _client(service: FakeQueryService) -> TestClient:
    settings = ApiSettings(
        fastapi_database_url=SecretStr("unused"),
        fastapi_api_key=SecretStr(API_KEY),
    )
    return TestClient(
        create_app(
            settings=settings,
            query_service=service,
            auction_indicative_service=FakeLiveAuctionService(service),  # type: ignore[arg-type]
            board_index_bias_live_service=FakeBoardIndexBiasLiveService(service),  # type: ignore[arg-type]
        )
    )


def _headers() -> dict[str, str]:
    return {"X-API-Key": API_KEY}


def test_api_settings_never_fall_back_to_worker_database_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://worker-credential-must-not-be-used")
    monkeypatch.delenv("FASTAPI_DATABASE_URL", raising=False)
    monkeypatch.setenv("FASTAPI_API_KEY", API_KEY)

    with pytest.raises(ValidationError):
        ApiSettings(_env_file=None)  # type: ignore[call-arg]


def test_health_and_readiness_are_public() -> None:
    service = FakeQueryService()
    client = _client(service)

    assert client.get("/healthz").json()["status"] == "ok"
    assert client.get("/readyz").json()["status"] == "ready"


def test_readiness_hides_database_error_details() -> None:
    service = FakeQueryService()
    service.ready_error = PublicQueryUnavailable("secret database details")

    response = _client(service).get("/readyz")

    assert response.status_code == 503
    assert response.json() == {
        "error": {
            "code": "service_unavailable",
            "message": "data service is unavailable",
        }
    }
    assert "secret" not in response.text


def test_safe_query_error_logs_sqlstate_but_hides_detail(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Server logs carry sqlstate/detail for diagnostics; the raised exception does not."""
    orig = SimpleNamespace(sqlstate="08006", args=("connection refused",))
    error = DBAPIError(statement=None, params=None, orig=orig)

    with (
        caplog.at_level("WARNING", logger="market_data_center.public_api.queries"),
        pytest.raises(PublicQueryUnavailable) as exc_info,
    ):
        _raise_safe_query_error(error)

    # Raised exception keeps a fixed, detail-free message for the client.
    assert "connection refused" not in str(exc_info.value)
    assert exc_info.value.__cause__ is error
    # Server-side log records the sqlstate and original detail for operators.
    assert any("08006" in r.message for r in caplog.records)
    assert any("connection refused" in r.message for r in caplog.records)


def test_market_routes_require_the_api_key() -> None:
    client = _client(FakeQueryService())

    missing = client.get("/api/v1/securities", params={"query": "600000"})
    wrong = client.get(
        "/api/v1/securities",
        params={"query": "600000"},
        headers={"X-API-Key": "wrong-key"},
    )

    assert missing.status_code == 401
    assert wrong.status_code == 401
    assert missing.json()["error"]["code"] == "unauthorized"


def test_security_search_returns_a_bounded_envelope() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/securities",
        params={"query": "浦发", "limit": 10},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["count"] == 1
    assert response.json()["items"][0]["symbol"] == "SSE:600000"
    assert service.security_calls == [("浦发", 10)]


def test_daily_bars_keep_decimal_values_as_strings() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/daily-bars/SSE:600000",
        params={"start_date": "2026-07-29", "end_date": "2026-07-29"},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["items"][0]["close"] == "10.20"
    assert response.json()["items"][0]["amount"] == "1258680.00"
    assert service.daily_bar_calls == [("SSE:600000", date(2026, 7, 29), date(2026, 7, 29), 1000)]


def test_daily_bar_validation_does_not_call_the_service() -> None:
    service = FakeQueryService()

    invalid_symbol = _client(service).get(
        "/api/v1/daily-bars/600000",
        params={"start_date": "2026-07-29", "end_date": "2026-07-29"},
        headers=_headers(),
    )
    reversed_dates = _client(service).get(
        "/api/v1/daily-bars/SSE:600000",
        params={"start_date": "2026-07-30", "end_date": "2026-07-29"},
        headers=_headers(),
    )

    assert invalid_symbol.status_code == 422
    assert reversed_dates.status_code == 422
    assert service.daily_bar_calls == []


def test_classification_members_and_not_found_response() -> None:
    service = FakeQueryService()
    client = _client(service)
    path = "/api/v1/classifications/tdx/industry/T1001/members"
    params = {"as_of_date": "2026-07-29"}

    response = client.get(path, params=params, headers=_headers())
    service.classification_error = PublicQueryNotFound("internal details")
    missing = client.get(path, params=params, headers=_headers())

    assert response.status_code == 200
    assert response.json()["members"] == ["SSE:600000", "SZSE:000001"]
    assert missing.status_code == 404
    assert missing.json()["error"]["code"] == "not_found"
    assert "internal" not in missing.text


def test_limit_up_pool_returns_exact_decimal_market_cap_and_revision() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/limit-up-pool",
        params={"trade_date": "2026-07-31", "version": 2, "limit": 100},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["items"] == [
        {
            "symbol": "SSE:600000",
            "code": "600000",
            "name": "浦发银行",
            "free_float_market_cap_cny": "61200000.000000",
        }
    ]
    assert service.limit_up_calls == [(date(2026, 7, 31), 2, 100)]
    assert response.json()["total_candidate_count"] == 3
    assert response.json()["valid_count"] == 2
    assert response.json()["omitted_count"] == 1
    assert response.json()["has_more"] is True
    assert response.json()["omission_reasons"] == {
        "missing_name": 0,
        "missing_close": 1,
        "missing_free_float_shares": 1,
    }


def test_limit_up_pool_is_api_key_protected_and_bounded() -> None:
    service = FakeQueryService()
    client = _client(service)

    assert (
        client.get("/api/v1/limit-up-pool", params={"trade_date": "2026-07-31"}).status_code == 401
    )
    assert (
        client.get(
            "/api/v1/limit-up-pool",
            params={"trade_date": "2026-07-31", "limit": 5001},
            headers=_headers(),
        ).status_code
        == 422
    )
    assert service.limit_up_calls == []


def test_openapi_only_contains_the_active_non_derived_routes() -> None:
    schema = _client(FakeQueryService()).get("/openapi.json").json()

    assert "/api/v1/securities" in schema["paths"]
    assert "/api/v1/daily-bars/{symbol}" in schema["paths"]
    assert "/api/v1/limit-up-pool" in schema["paths"]
    assert "/api/v1/daily-limit-up-list" in schema["paths"]
    assert "/api/v1/call-auction-market-snapshots/query" in schema["paths"]
    assert "/api/v1/call-auction-market-series-snapshots/query" in schema["paths"]
    assert not any("adjusted" in path or "metric" in path for path in schema["paths"])


def test_daily_limit_up_list_returns_items() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/daily-limit-up-list",
        params={"trade_date": "2026-08-10", "version": 1, "offset": 10, "limit": 200},
        headers=_headers(),
    )

    assert response.status_code == 200
    body = response.json()
    assert body["trade_date"] == "2026-08-10"
    assert body["snapshot_id"] == "33333333-3333-3333-3333-333333333333"
    assert body["version"] == 1
    assert body["status"] == "partial"
    assert body["member_count"] == 55
    assert body["returned_count"] == 1
    assert body["offset"] == 10
    assert body["quality"] == {
        "total_findings": 2,
        "by_rule": {"missing_source_observation": 2},
    }
    item = body["items"][0]
    assert item["symbol"] == "SSE:600000"
    assert item["code"] == "600000"
    assert item["close"] == "9.29"
    assert item["free_float_market_cap_cny"] == "929000000"
    assert item["source_reported_sealed_funds_cny"] == "1200000"
    assert item["closing_bid1_sealing_amount_cny"] == "929000"
    assert item["limit_up_duration_seconds"] is None
    assert item["consecutive_limit_up_days"] == 3
    assert item["volume"] == 62542540
    assert service.daily_limit_up_calls == [(date(2026, 8, 10), 1, 10, 200)]


def test_daily_limit_up_list_version_and_pagination_are_bounded() -> None:
    service = FakeQueryService()
    client = _client(service)

    assert (
        client.get(
            "/api/v1/daily-limit-up-list",
            params={"trade_date": "2026-08-10", "version": 0},
            headers=_headers(),
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/v1/daily-limit-up-list",
            params={"trade_date": "2026-08-10", "offset": 50001},
            headers=_headers(),
        ).status_code
        == 422
    )
    assert (
        client.get(
            "/api/v1/daily-limit-up-list",
            params={"trade_date": "2026-08-10", "limit": 501},
            headers=_headers(),
        ).status_code
        == 422
    )
    assert service.daily_limit_up_calls == []


def test_call_auction_market_snapshots_deduplicate_codes_and_keep_decimals() -> None:
    service = FakeQueryService()

    response = _client(service).post(
        "/api/v1/call-auction-market-snapshots/query",
        json={"trade_date": "2026-08-13", "codes": ["600000", "000001", "600000"]},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "trade_date": "2026-08-13",
        "ingestion_id": "dddddddd-dddd-dddd-dddd-dddddddddddd",
        "ingestion_status": "partial",
        "requested_count": 2,
        "returned_count": 1,
        "missing_codes": ["000001"],
        "items": [
            {
                "symbol": "SSE:600000",
                "code": "600000",
                "observed_at": "2026-08-13T01:26:00Z",
                "last_price": "10.1200",
                "previous_close": "10.0000",
                "high_price": "10.1500",
                "low_price": "9.9800",
                "cumulative_volume": 123400,
                "cumulative_amount": "1248808.0000",
                "bid1_price": "10.1200",
                "bid1_volume": 560200,
                "bid2_price": None,
                "bid2_volume": 10743200,
                "bid3_price": None,
                "bid3_volume": None,
                "bid4_price": None,
                "bid4_volume": None,
                "bid5_price": None,
                "bid5_volume": None,
                "ask1_price": None,
                "ask1_volume": 0,
                "ask2_price": None,
                "ask2_volume": 13300,
                "ask3_price": None,
                "ask3_volume": None,
                "ask4_price": None,
                "ask4_volume": None,
                "ask5_price": None,
                "ask5_volume": None,
                "seal_amount": "5673224.0000",
            }
        ],
    }
    assert service.call_auction_market_snapshot_calls == [(date(2026, 8, 13), ("600000", "000001"))]


@pytest.mark.parametrize(
    "codes",
    [[], ["60000"], ["60000A"], [f"{value:06d}" for value in range(501)]],
)
def test_call_auction_market_snapshot_request_is_bounded(codes: list[str]) -> None:
    service = FakeQueryService()

    response = _client(service).post(
        "/api/v1/call-auction-market-snapshots/query",
        json={"trade_date": "2026-08-13", "codes": codes},
        headers=_headers(),
    )

    assert response.status_code == 422
    assert service.call_auction_market_snapshot_calls == []


def test_call_auction_market_series_snapshots_return_rounds_in_one_session() -> None:
    service = FakeQueryService()

    response = _client(service).post(
        "/api/v1/call-auction-market-series-snapshots/query",
        json={"trade_date": "2026-08-14", "codes": ["600000", "000001", "600000"]},
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "trade_date": "2026-08-14",
        "session_id": "11111111-1111-1111-1111-111111111111",
        "session_status": "partial",
        "expected_rounds": 32,
        "returned_rounds": 1,
        "requested_count": 2,
        "rounds": [
            {
                "sample_seq": 0,
                "scheduled_at": "2026-08-14T01:15:00Z",
                "collected_at": "2026-08-14T01:15:02Z",
                "round_status": "partial",
                "selected_ingestion_id": "22222222-2222-2222-2222-222222222222",
                "requested_count": 2,
                "returned_count": 1,
                "missing_codes": ["000001"],
                "items": [
                    {
                        "symbol": "SSE:600000",
                        "code": "600000",
                        "batch_code": "091500",
                        "observed_at": "2026-08-14T01:15:01Z",
                        "last_price": "10.1200",
                        "previous_close": "10.0000",
                        "high_price": "10.1500",
                        "low_price": "9.9800",
                        "cumulative_volume": 123400,
                        "cumulative_amount": "1248808.0000",
                        "value_semantics": "auction_indicative",
                        "bid1_price": "10.0000",
                        "bid1_volume": 100,
                        "bid2_price": None,
                        "bid2_volume": 10743200,
                        "bid3_price": None,
                        "bid3_volume": None,
                        "bid4_price": None,
                        "bid4_volume": None,
                        "bid5_price": None,
                        "bid5_volume": None,
                        "ask1_price": "10.0100",
                        "ask1_volume": 100,
                        "ask2_price": None,
                        "ask2_volume": 13300,
                        "ask3_price": None,
                        "ask3_volume": None,
                        "ask4_price": None,
                        "ask4_volume": None,
                        "ask5_price": None,
                        "ask5_volume": None,
                    }
                ],
            }
        ],
    }
    assert service.call_auction_market_series_snapshot_calls == [
        (date(2026, 8, 14), ("600000", "000001"))
    ]


@pytest.mark.parametrize("codes", [[], ["60000"], ["60000A"], ["000001"] * 501])
def test_call_auction_market_series_snapshot_request_is_bounded(codes: list[str]) -> None:
    service = FakeQueryService()

    response = _client(service).post(
        "/api/v1/call-auction-market-series-snapshots/query",
        json={"trade_date": "2026-08-14", "codes": codes},
        headers=_headers(),
    )

    assert response.status_code == 422
    assert service.call_auction_market_series_snapshot_calls == []


def test_top_gainers_20d_contract_and_bounds() -> None:
    service = FakeQueryService()
    response = _client(service).get(
        "/api/v1/top-gainers-20d",
        params={"end_date": "2026-08-13", "limit": 10},
        headers=_headers(),
    )
    assert response.status_code == 200
    assert response.json()["return_interval_count"] == 19
    assert response.json()["items"][0]["return_pct"] == "20"
    assert service.top_gainer_calls == [(date(2026, 8, 13), 10)]
    assert (
        _client(service)
        .get("/api/v1/top-gainers-20d", params={"limit": 11}, headers=_headers())
        .status_code
        == 422
    )


def test_close_price_new_highs_120d_returns_latest_strict_breakouts_without_inputs() -> None:
    service = FakeQueryService()
    response = _client(service).get(
        "/api/v1/close-price-new-highs-120d",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "trade_date": "2026-08-14",
        "window_trading_session_count": 120,
        "comparison_session_count": 119,
        "total_candidate_count": 2,
        "eligible_history_count": 1,
        "omitted_count": 1,
        "returned_count": 1,
        "omissions": {
            "incomplete_history": 1,
            "non_trading_bar": 0,
            "nonpositive_price": 0,
            "missing_name": 0,
        },
        "items": [
            {
                "symbol": "SSE:600000",
                "code": "600000",
                "name": "浦发银行",
                "close": "12.50",
                "previous_119d_high": "12.00",
                "breakout_pct": "4.1666666667",
            }
        ],
    }
    assert service.close_price_new_highs_120d_calls == 1


def test_close_price_new_highs_120d_sets_ten_second_timeout_before_rpc() -> None:
    calls: list[tuple[str, object]] = []
    payload = {
        "trade_date": date(2026, 8, 14),
        "window_trading_session_count": 120,
        "comparison_session_count": 119,
        "total_candidate_count": 0,
        "eligible_history_count": 0,
        "omitted_count": 0,
        "returned_count": 0,
        "omissions": {
            "incomplete_history": 0,
            "non_trading_bar": 0,
            "nonpositive_price": 0,
            "missing_name": 0,
        },
        "items": [],
    }

    class StubResult:
        def __init__(self, rows: list[dict[str, object]]) -> None:
            self._rows = rows

        def mappings(self) -> "StubResult":
            return self

        def all(self) -> list[dict[str, object]]:
            return self._rows

    class StubConnection:
        def __enter__(self) -> "StubConnection":
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, statement: object, parameters: object) -> "StubResult":
            calls.append((str(statement), parameters))
            return StubResult([{"payload": payload}])

    class StubEngine:
        def connect(self) -> StubConnection:
            return StubConnection()

    service = PostgreSQLPublicQueryService(StubEngine())  # type: ignore[arg-type]
    response = service.close_price_new_highs_120d()

    assert response.returned_count == 0
    assert "set_config('statement_timeout'" in calls[0][0]
    assert calls[0][1] == {"statement_timeout": "10000ms"}
    assert "query_close_price_new_highs_120d" in calls[1][0]


def test_board_index_bias_returns_latest_decimal_contract_without_inputs() -> None:
    service = FakeQueryService()
    client = _client(service)

    response = client.get(
        "/api/v1/board-indexes/883423/bias",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json() == {
        "board_id": "THS:883423",
        "board_code": "883423",
        "board_name": "沪深主板昨日涨停",
        "trade_date": "2026-08-14",
        "close": "1234.5600",
        "moving_average_5": "1220.110000",
        "bias_5_pct": "1.184319",
        "previous_trade_date": "2026-08-13",
        "previous_bias_5_pct": "0.932150",
        "bias_direction": "up",
        "window_trading_days": 30,
        "bias_sample_count": 30,
        "highest_bias_5_pct": "4.521300",
        "highest_bias_trade_date": "2026-08-06",
        "lowest_bias_5_pct": "-2.861700",
        "lowest_bias_trade_date": "2026-07-22",
        "algorithm_version": "board_index_bias_v1",
        "data_origin": "database",
        "persistence_status": "persisted",
        "fetched_at": "2026-08-15T03:26:46Z",
    }
    assert service.board_index_bias_calls == 1
    operation = client.get("/openapi.json").json()["paths"]["/api/v1/board-indexes/883423/bias"][
        "get"
    ]
    assert operation.get("parameters", []) == []


def test_board_index_bias_requires_api_key() -> None:
    service = FakeQueryService()

    response = _client(service).get("/api/v1/board-indexes/883423/bias")

    assert response.status_code == 401
    assert service.board_index_bias_calls == 0


def test_board_index_bias_falls_back_only_for_explicit_not_ready_error() -> None:
    service = FakeQueryService()
    service.board_index_bias_error = BoardIndexBiasNotReady("not ready")

    response = _client(service).get(
        "/api/v1/board-indexes/883423/bias",
        headers=_headers(),
    )

    assert response.status_code == 200
    assert response.json()["data_origin"] == "ths_live"
    assert response.json()["persistence_status"] == "queued"
    assert service.board_index_bias_calls == 1
    assert service.board_index_bias_live_calls == 1


def test_board_index_bias_does_not_fallback_for_database_failure() -> None:
    service = FakeQueryService()
    service.board_index_bias_error = PublicQueryUnavailable("database unavailable")

    response = _client(service).get(
        "/api/v1/board-indexes/883423/bias",
        headers=_headers(),
    )

    assert response.status_code == 503
    assert service.board_index_bias_live_calls == 0


@pytest.mark.parametrize(
    ("error", "expected_status"),
    [
        (BoardIndexBiasLiveBusy("busy"), 429),
        (BoardIndexBiasLiveUpstream("upstream"), 502),
        (BoardIndexBiasLivePersistence("persistence"), 503),
    ],
)
def test_board_index_bias_maps_live_fallback_errors(error: Exception, expected_status: int) -> None:
    service = FakeQueryService()
    service.board_index_bias_error = BoardIndexBiasNotReady("not ready")
    service.board_index_bias_live_error = error

    response = _client(service).get(
        "/api/v1/board-indexes/883423/bias",
        headers=_headers(),
    )

    assert response.status_code == expected_status
    assert service.board_index_bias_live_calls == 1


def test_auction_one_price_limits_returns_separate_sets() -> None:
    service = FakeQueryService()
    response = _client(service).get(
        "/api/v1/call-auction-one-price-limits",
        params={"trade_date": "2026-08-13"},
        headers=_headers(),
    )
    assert response.status_code == 200
    assert response.json()["up"][0]["direction"] == "up"
    assert response.json()["down"] == []
    assert response.json()["ingestion_status"] == "partial"
    assert response.json()["price_limit_calculation_id"] is None
    assert response.json()["price_limit_rule_version"] == "CN_MAINBOARD_2026_07_06"
    assert response.json()["price_limit_algorithm_version"] == "1.0.0"
    assert response.json()["calculation_mode"] == "realtime_read"
    assert response.json()["up"][0]["seal_amount"] == "1100"
    assert response.json()["up"][0]["observed_at"] == "2026-08-13 09:26:00"


def test_auction_one_price_limits_openapi_exposes_realtime_lineage() -> None:
    schema = _client(FakeQueryService()).get("/openapi.json").json()
    response = schema["components"]["schemas"]["AuctionOnePriceLimitResponse"]
    item = schema["components"]["schemas"]["AuctionOnePriceLimitItem"]

    assert {
        item["type"] for item in response["properties"]["price_limit_calculation_id"]["anyOf"]
    } == {
        "string",
        "null",
    }
    assert response["properties"]["price_limit_rule_version"]["const"] == (
        "CN_MAINBOARD_2026_07_06"
    )
    assert response["properties"]["price_limit_algorithm_version"]["const"] == "1.0.0"
    assert response["properties"]["calculation_mode"]["const"] == "realtime_read"
    assert "seal_amount" in item["properties"]
    assert "封单额" in item["properties"]["seal_amount"]["description"]
    assert item["properties"]["observed_at"]["examples"] == ["2026-08-18 14:27:46"]


def test_auction_indicative_details_falls_back_to_live_only_when_database_is_empty() -> None:
    service = FakeQueryService()
    response = _client(service).get(
        "/api/v1/call-auction-indicative-details",
        params={"code": "688796"},
        headers=_headers(),
    )
    assert response.status_code == 200
    payload = response.json()
    assert payload["semantics"] == "auction_virtual_indicative_matching_detail"
    assert payload["is_exchange_trade_tick"] is False
    assert payload["is_order_by_order"] is False
    assert payload["trade_date"] == "2026-08-14"
    assert payload["fetched_at"] == "2026-08-14 09:26:00"
    assert [item["observed_at"] for item in payload["items"]] == [
        "2026-08-14 09:15:05",
        "2026-08-14 09:20:00",
    ]
    assert payload["items"][0]["displayed_volume_shares"] == 200
    assert payload["quality"]["source_display_classification_trusted"] is False
    assert payload["quality"]["raw_captured"] is True
    assert payload["quality"]["database_persistence"] == "queued"
    assert payload["live_provider_derived"] is True
    assert payload["data_origin"] == "eastmoney_live"
    assert service.auction_indicative_database_calls == [("SSE:688796", 0, 200)]
    assert service.auction_indicative_calls == [("SSE:688796", date(2026, 8, 14), 0, 200)]


def test_auction_indicative_details_returns_database_hit_without_live_fetch() -> None:
    service = FakeQueryService()
    service.auction_indicative_database_error = None

    response = _client(service).get(
        "/api/v1/call-auction-indicative-details",
        params={"code": "000001"},
        headers=_headers(),
    )

    assert response.status_code == 200
    payload = response.json()
    assert payload["data_origin"] == "database"
    assert payload["persistence_status"] == "persisted"
    assert payload["fetched_at"] == "2026-08-14 09:26:00"
    assert [item["observed_at"] for item in payload["items"]] == [
        "2026-08-14 09:15:05",
        "2026-08-14 09:20:00",
    ]
    assert service.auction_indicative_database_calls == [("SZSE:000001", 0, 200)]
    assert service.auction_indicative_calls == []


def test_auction_indicative_database_failure_does_not_trigger_live_fetch() -> None:
    service = FakeQueryService()
    service.auction_indicative_database_error = PublicQueryUnavailable("database unavailable")

    response = _client(service).get(
        "/api/v1/call-auction-indicative-details",
        params={"code": "688796"},
        headers=_headers(),
    )

    assert response.status_code == 503
    assert service.auction_indicative_calls == []


@pytest.mark.parametrize("code", ["68879", "68879A", "920000", "200001"])
def test_auction_indicative_details_accepts_only_supported_six_digit_stock_codes(
    code: str,
) -> None:
    service = FakeQueryService()
    response = _client(service).get(
        "/api/v1/call-auction-indicative-details",
        params={"code": code},
        headers=_headers(),
    )
    assert response.status_code == 422
    assert service.auction_indicative_calls == []


def test_auction_indicative_details_no_longer_accepts_symbol_or_trade_date() -> None:
    service = FakeQueryService()

    response = _client(service).get(
        "/api/v1/call-auction-indicative-details",
        params={"symbol": "SSE:688796", "trade_date": "2026-08-14"},
        headers=_headers(),
    )

    assert response.status_code == 422
    assert service.auction_indicative_database_calls == []
    assert service.auction_indicative_calls == []
