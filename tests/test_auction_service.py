from datetime import date, datetime, timedelta
from decimal import Decimal
from uuid import UUID

from market_data_center.auction_service import AuctionCollectionService
from market_data_center.domain.auction import (
    AuctionCollectionSession,
    AuctionPoolMember,
    AuctionSessionStatus,
    auction_window,
)
from market_data_center.domain.ingestion import ProviderCode
from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.providers.contracts import RealtimeQuoteFetch
from market_data_center.raw_store import StoredRawObject


def _levels(side: str) -> tuple[OrderBookLevel, ...]:
    start = Decimal("10.00") if side == "bid" else Decimal("10.01")
    direction = Decimal("-0.01") if side == "bid" else Decimal("0.01")
    return tuple(
        OrderBookLevel(level, start + direction * (level - 1), level * 100) for level in range(1, 6)
    )


class FakePersistence:
    def __init__(self) -> None:
        self.created_runs = []
        self.committed = []

    def create_ingestion_run(self, run) -> None:
        self.created_runs.append(run)

    def commit_round(self, *values) -> None:
        self.committed.append(values)

    def fail_ingestion_run(self, run) -> None:
        raise AssertionError(run)


class FakeProvider:
    source_code = "pysnowball"

    def __init__(self, record: FiveLevelQuoteSnapshotRecord) -> None:
        self.record = record
        self.deadlines: list[datetime | None] = []

    def fetch_five_level_quotes(self, symbols, *, deadline=None) -> RealtimeQuoteFetch:
        self.deadlines.append(deadline)
        return RealtimeQuoteFetch(
            ({"symbol": "SH600000"},),
            (self.record,),
            tuple(symbols),
            (),
            "pysnowball.pankou.v1",
            (self.record.observed_at,),
        )


class FakeRawStore:
    def __init__(self) -> None:
        self.providers: list[str] = []

    def write_jsonl(self, *, provider, dataset, partition_date, ingestion_id, rows, schema_version):
        self.providers.append(provider)
        return StoredRawObject(
            "pysnowball/five_level_quote/row.jsonl",
            "a" * 64,
            1,
            len(tuple(rows)),
            "jsonl",
            schema_version,
        )


def test_round_lineage_uses_actual_pysnowball_provider_and_fixed_deadline() -> None:
    trade_date = date(2026, 8, 18)
    scheduled_at, window_end = auction_window(trade_date)
    session_id = UUID("00000000-0000-0000-0000-000000000001")
    pool_id = UUID("00000000-0000-0000-0000-000000000002")
    session = AuctionCollectionSession(
        session_id,
        pool_id,
        1,
        date(2026, 8, 17),
        trade_date,
        scheduled_at,
        window_end,
        30,
        21,
        21,
        "pysnowball",
        AuctionSessionStatus.RUNNING,
        scheduled_at,
    )
    quote = FiveLevelQuoteSnapshotRecord(
        "SSE:600000",
        Market.CN_A_SHARE,
        scheduled_at + timedelta(seconds=1),
        scheduled_at,
        QuoteStatus.TRADING,
        Decimal("10.00"),
        None,
        None,
        None,
        None,
        None,
        None,
        _levels("bid"),
        _levels("ask"),
        "pysnowball",
    )
    persistence = FakePersistence()
    provider = FakeProvider(quote)
    raw_store = FakeRawStore()
    service = AuctionCollectionService(
        persistence,  # type: ignore[arg-type]
        provider,
        raw_store,  # type: ignore[arg-type]
        cadence_seconds=30,
        max_retries=0,
        clock=lambda: scheduled_at + timedelta(seconds=1),
        uuid_factory=iter(
            (
                UUID("00000000-0000-0000-0000-000000000003"),
                UUID("00000000-0000-0000-0000-000000000004"),
            )
        ).__next__,
    )

    service._collect_round(
        session,
        (AuctionPoolMember("SSE:600000", Decimal("10.00"), "mainboard-v1"),),
        0,
        scheduled_at,
    )

    assert persistence.created_runs[0].provider_code is ProviderCode.PYSNOWBALL
    assert raw_store.providers == ["pysnowball"]
    assert provider.deadlines == [scheduled_at + timedelta(seconds=30)]
    assert persistence.committed[0][3][0].quote.source_code == "pysnowball"
