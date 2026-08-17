"""Stable JSON models for the external read-only API."""

from datetime import date, datetime
from decimal import Decimal
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator

from market_data_center.domain import (
    Exchange,
    SecurityStatus,
    SecurityType,
    TradeStatus,
)
from market_data_center.domain.auction_indicative import SHANGHAI


class ApiModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class HealthResponse(ApiModel):
    status: str
    service: str = "market-data-center-api"
    version: str = "0.2.0"


class ErrorDetail(ApiModel):
    code: str
    message: str


class ErrorResponse(ApiModel):
    error: ErrorDetail


class SecurityItem(ApiModel):
    symbol: str
    code: str
    exchange: Exchange
    current_name: str
    security_type: SecurityType
    status: SecurityStatus
    ipo_date: date | None
    delisting_date: date | None


class SecuritySearchResponse(ApiModel):
    count: int = Field(ge=0)
    items: list[SecurityItem]


class DailyBarItem(ApiModel):
    symbol: str
    trade_date: date
    open: Decimal | None
    high: Decimal | None
    low: Decimal | None
    close: Decimal | None
    previous_close: Decimal | None
    volume: int | None
    amount: Decimal | None
    trade_status: TradeStatus
    is_st: bool | None


class DailyBarResponse(ApiModel):
    symbol: str
    start_date: date
    end_date: date
    count: int = Field(ge=0)
    items: list[DailyBarItem]


class ClassificationMembersResponse(ApiModel):
    snapshot_date: date
    member_count: int = Field(ge=0)
    returned_count: int = Field(ge=0)
    members: list[str]


class LimitUpPoolItem(ApiModel):
    symbol: str
    code: str
    name: str
    free_float_market_cap_cny: Decimal


class LimitUpPoolOmissionReasons(ApiModel):
    missing_name: int = Field(ge=0)
    missing_close: int = Field(ge=0)
    missing_free_float_shares: int = Field(ge=0)


class LimitUpPoolResponse(ApiModel):
    snapshot_id: UUID
    calculation_id: UUID
    trade_date: date
    effective_trade_date: date
    version: int = Field(ge=1)
    rule_version: str
    algorithm_version: str
    input_hash: str
    generated_at: datetime
    total_candidate_count: int = Field(ge=0)
    valid_count: int = Field(ge=0)
    returned_count: int = Field(ge=0)
    omitted_count: int = Field(ge=0)
    has_more: bool
    omission_reasons: LimitUpPoolOmissionReasons
    items: list[LimitUpPoolItem]


class DailyLimitUpListItem(ApiModel):
    symbol: str
    code: str
    name: str
    previous_close: Decimal
    close: Decimal
    limit_price: Decimal
    change_percent: Decimal
    free_float_shares: int = Field(gt=0)
    free_float_market_cap_cny: Decimal
    first_limit_up_at: datetime | None
    last_limit_up_at: datetime | None
    open_count: int | None = Field(default=None, ge=0)
    limit_up_duration_seconds: int | None = Field(default=None, ge=0)
    duration_semantics: str
    source_reported_sealed_funds_cny: Decimal | None
    closing_bid1_price: Decimal | None
    closing_bid1_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid2_price: Decimal | None
    closing_bid2_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid3_price: Decimal | None
    closing_bid3_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid4_price: Decimal | None
    closing_bid4_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid5_price: Decimal | None
    closing_bid5_volume_shares: int | None = Field(default=None, ge=0)
    closing_bid1_sealing_amount_cny: Decimal | None
    daily_bar_ingestion_id: UUID
    indicator_ingestion_id: UUID
    name_ingestion_id: UUID
    pool_calculation_id: UUID
    source_observation_ingestion_id: UUID | None
    source_observation_raw_id: UUID | None
    order_book_ingestion_id: UUID | None
    volume: int | None = Field(default=None, ge=0)
    amount_cny: Decimal | None
    free_float_turnover_rate_pct: Decimal | None
    consecutive_limit_up_days: int | None = Field(default=None, ge=1)


class DailyLimitUpQualitySummary(ApiModel):
    total_findings: int = Field(ge=0)
    by_rule: dict[str, int]


class DailyLimitUpListResponse(ApiModel):
    snapshot_id: UUID
    calculation_id: UUID | None
    trade_date: date
    version: int = Field(ge=1)
    status: Literal["ready", "partial", "deferred", "failed"]
    rule_version: str
    algorithm_version: str
    input_hash: str
    source_ingestion_id: UUID | None
    generated_at: datetime
    candidate_count: int = Field(ge=0)
    member_count: int = Field(ge=0)
    rejected_count: int = Field(ge=0)
    offset: int = Field(ge=0)
    returned_count: int = Field(ge=0)
    has_more: bool
    quality: DailyLimitUpQualitySummary
    items: list[DailyLimitUpListItem]


SixDigitCode = Annotated[str, Field(pattern=r"^[0-9]{6}$")]


class CallAuctionMarketSnapshotQuery(ApiModel):
    trade_date: date
    codes: list[SixDigitCode] = Field(min_length=1, max_length=500)

    @field_validator("codes")
    @classmethod
    def deduplicate_codes(cls, codes: list[str]) -> list[str]:
        return list(dict.fromkeys(codes))


class CallAuctionMarketSnapshotItem(ApiModel):
    symbol: str
    code: SixDigitCode
    observed_at: datetime
    last_price: Decimal | None
    previous_close: Decimal | None
    high_price: Decimal | None
    low_price: Decimal | None
    cumulative_volume: int | None = Field(default=None, ge=0)
    cumulative_amount: Decimal | None


class CallAuctionMarketSnapshotResponse(ApiModel):
    trade_date: date
    ingestion_id: UUID
    ingestion_status: Literal["succeeded", "partial"]
    requested_count: int = Field(ge=1, le=500)
    returned_count: int = Field(ge=0)
    missing_codes: list[SixDigitCode]
    items: list[CallAuctionMarketSnapshotItem]


class CallAuctionMarketSeriesSnapshotQuery(CallAuctionMarketSnapshotQuery):
    pass


class CallAuctionMarketSeriesSnapshotItem(CallAuctionMarketSnapshotItem):
    value_semantics: Literal["auction_indicative", "opening_trade", "legacy_source_quote"]


class CallAuctionMarketSeriesRound(ApiModel):
    sample_seq: int = Field(ge=0, le=31)
    scheduled_at: datetime
    collected_at: datetime
    round_status: Literal["succeeded", "partial", "failed"]
    selected_ingestion_id: UUID | None
    requested_count: int = Field(ge=1, le=500)
    returned_count: int = Field(ge=0)
    missing_codes: list[SixDigitCode]
    items: list[CallAuctionMarketSeriesSnapshotItem]


class CallAuctionMarketSeriesSnapshotResponse(ApiModel):
    trade_date: date
    session_id: UUID
    session_status: Literal["succeeded", "partial"]
    expected_rounds: Literal[32]
    returned_rounds: int = Field(ge=0, le=32)
    requested_count: int = Field(ge=1, le=500)
    rounds: list[CallAuctionMarketSeriesRound]


class TopGainer20dItem(ApiModel):
    symbol: str
    code: SixDigitCode
    name: str
    start_trade_date: date
    end_trade_date: date
    start_close: Decimal
    end_close: Decimal
    return_pct: Decimal


class TopGainer20dOmissions(ApiModel):
    missing_start_bar: int = Field(ge=0)
    missing_end_bar: int = Field(ge=0)
    non_trading_bar: int = Field(ge=0)
    nonpositive_price: int = Field(ge=0)
    missing_name: int = Field(ge=0)


class TopGainers20dResponse(ApiModel):
    start_trade_date: date
    end_trade_date: date
    trading_session_count: Literal[20]
    return_interval_count: Literal[19]
    total_candidate_count: int = Field(ge=0)
    eligible_count: int = Field(ge=0)
    omitted_count: int = Field(ge=0)
    returned_count: int = Field(ge=0, le=10)
    omissions: TopGainer20dOmissions
    items: list[TopGainer20dItem]


class ClosePriceNewHigh120dItem(ApiModel):
    symbol: str
    code: SixDigitCode
    name: str
    close: Decimal
    previous_119d_high: Decimal
    breakout_pct: Decimal


class ClosePriceNewHigh120dOmissions(ApiModel):
    incomplete_history: int = Field(ge=0)
    non_trading_bar: int = Field(ge=0)
    nonpositive_price: int = Field(ge=0)
    missing_name: int = Field(ge=0)


class ClosePriceNewHighs120dResponse(ApiModel):
    trade_date: date
    window_trading_session_count: Literal[120]
    comparison_session_count: Literal[119]
    total_candidate_count: int = Field(ge=0, le=10_000)
    eligible_history_count: int = Field(ge=0, le=10_000)
    omitted_count: int = Field(ge=0, le=10_000)
    returned_count: int = Field(ge=0, le=10_000)
    omissions: ClosePriceNewHigh120dOmissions
    items: list[ClosePriceNewHigh120dItem]


class BoardIndexBiasResponse(ApiModel):
    board_id: Literal["THS:883423"]
    board_code: Literal["883423"]
    board_name: str
    trade_date: date
    close: Decimal
    moving_average_5: Decimal | None
    bias_5_pct: Decimal | None
    previous_trade_date: date | None
    previous_bias_5_pct: Decimal | None
    bias_direction: Literal["up", "down", "flat"] | None
    window_trading_days: Literal[30]
    bias_sample_count: int = Field(ge=0, le=30)
    highest_bias_5_pct: Decimal | None
    highest_bias_trade_date: date | None
    lowest_bias_5_pct: Decimal | None
    lowest_bias_trade_date: date | None
    algorithm_version: Literal["board_index_bias_v1"]
    data_origin: Literal["database", "ths_live"]
    persistence_status: Literal["persisted", "queued"]
    fetched_at: datetime


class AuctionOnePriceLimitItem(ApiModel):
    symbol: str
    code: SixDigitCode
    name: str
    direction: Literal["up", "down"]
    observed_at: datetime
    indicated_price: Decimal
    limit_price: Decimal
    previous_close: Decimal
    cumulative_volume: int | None = Field(default=None, ge=0)
    cumulative_amount: Decimal | None


class AuctionOnePriceLimitResponse(ApiModel):
    trade_date: date
    ingestion_id: UUID
    ingestion_status: Literal["succeeded", "partial"]
    price_limit_calculation_id: UUID | None
    price_limit_rule_version: Literal["CN_MAINBOARD_2026_07_06"]
    price_limit_algorithm_version: Literal["1.0.0"]
    calculation_mode: Literal["realtime_read"]
    snapshot_window: Literal["09:26:00-09:26:59 Asia/Shanghai"]
    candidate_count: int = Field(ge=0)
    omitted_incomplete_count: int = Field(ge=0)
    up_count: int = Field(ge=0)
    down_count: int = Field(ge=0)
    up: list[AuctionOnePriceLimitItem]
    down: list[AuctionOnePriceLimitItem]


class AuctionIndicativeDetailItem(ApiModel):
    observed_at: datetime = Field(
        description="Asia/Shanghai wall-clock time formatted as YYYY-MM-DD HH:mm:ss",
        examples=["2026-08-14 09:15:05"],
    )
    source_sequence: int = Field(ge=0)
    indicative_price: Decimal
    displayed_volume_shares: int = Field(ge=0)
    source_display_classification: Literal["internal", "external", "unknown"]

    @field_serializer("observed_at", when_used="json")
    def serialize_observed_at(self, value: datetime) -> str:
        return value.astimezone(SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")


class AuctionIndicativeQuality(ApiModel):
    status: Literal["complete", "partial"]
    source_row_count: int = Field(ge=0, le=5000)
    accepted_auction_row_count: int = Field(ge=0, lt=5000)
    source_display_classification_trusted: Literal[False]
    raw_captured: Literal[True]
    database_persistence: Literal["queued", "persisted"]


class AuctionIndicativeDetailResponse(ApiModel):
    symbol: str
    trade_date: date
    fetched_at: datetime = Field(
        description="Asia/Shanghai wall-clock time formatted as YYYY-MM-DD HH:mm:ss",
        examples=["2026-08-14 11:26:46"],
    )
    source: Literal["eastmoney"]
    live_provider_derived: Literal[True]
    data_origin: Literal["database", "eastmoney_live"]
    cache_hit: bool
    persistence_status: Literal["queued", "persisted"]
    version: int | None = Field(default=None, ge=1)
    ingestion_status: Literal["succeeded", "partial"] | None = None
    ingestion_id: UUID
    raw_id: UUID
    input_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    semantics: Literal["auction_virtual_indicative_matching_detail"]
    is_exchange_trade_tick: Literal[False]
    is_order_by_order: Literal[False]
    total_count: int = Field(ge=0)
    offset: int = Field(ge=0)
    returned_count: int = Field(ge=0, le=500)
    has_more: bool
    quality: AuctionIndicativeQuality
    items: list[AuctionIndicativeDetailItem]

    @field_validator("items")
    @classmethod
    def sort_items(
        cls, items: list[AuctionIndicativeDetailItem]
    ) -> list[AuctionIndicativeDetailItem]:
        return sorted(items, key=lambda item: (item.observed_at, item.source_sequence))

    @field_serializer("fetched_at", when_used="json")
    def serialize_fetched_at(self, value: datetime) -> str:
        return value.astimezone(SHANGHAI).strftime("%Y-%m-%d %H:%M:%S")
