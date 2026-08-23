from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import cast

from market_data_center.domain.ingestion import (
    IngestionRun,
    IngestionStatus,
    QualityResult,
    RawManifest,
)
from market_data_center.domain.realtime_quote import (
    FiveLevelQuoteSnapshotRecord,
    OrderBookLevel,
    QuoteStatus,
)
from market_data_center.domain.records import Market
from market_data_center.persistence.realtime_quote_postgres import (
    PostgreSQLRealtimeQuotePersistence,
)
from market_data_center.providers.contracts import RealtimeQuoteFetch, RealtimeQuoteProvider
from market_data_center.raw_store import LocalRawStore
from market_data_center.realtime_quote_service import collect_tencent_realtime_quotes


class FakePersistence:
    def __init__(self, known: set[str]) -> None:
        self.known = known
        self.created: list[IngestionRun] = []
        self.failed: list[IngestionRun] = []
        self.committed: list[
            tuple[
                IngestionRun,
                RawManifest,
                tuple[QualityResult, ...],
                tuple[FiveLevelQuoteSnapshotRecord, ...],
            ]
        ] = []

    def known_stock_symbols(self, symbols: tuple[str, ...]) -> set[str]:
        return self.known.intersection(symbols)

    def create_run(self, run: IngestionRun) -> None:
        self.created.append(run)

    def fail_run(self, run: IngestionRun) -> None:
        self.failed.append(run)

    def commit(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: tuple[QualityResult, ...],
        records: tuple[FiveLevelQuoteSnapshotRecord, ...],
    ) -> None:
        self.committed.append((run, manifest, quality, records))


class FakeProvider:
    source_code = "tencent_quote"

    def __init__(self, observed: datetime) -> None:
        bids = tuple(
            OrderBookLevel(level, Decimal("10.01") - Decimal(level) / 100, level * 100)
            for level in range(1, 6)
        )
        asks = tuple(
            OrderBookLevel(level, Decimal("10.00") + Decimal(level) / 100, level * 100)
            for level in range(1, 6)
        )
        self.fetch = RealtimeQuoteFetch(
            raw_rows=({"source_symbol": "sh600000", "payload": "raw"},),
            records=(
                FiveLevelQuoteSnapshotRecord(
                    symbol="SSE:600000",
                    market=Market.CN_A_SHARE,
                    observed_at=observed,
                    source_timestamp=observed,
                    quote_status=QuoteStatus.TRADING,
                    last_price=Decimal("10.00"),
                    previous_close=Decimal("9.90"),
                    open=Decimal("9.95"),
                    high=Decimal("10.10"),
                    low=Decimal("9.90"),
                    cumulative_volume=10_000,
                    cumulative_amount=Decimal("100000"),
                    bid_levels=bids,
                    ask_levels=asks,
                    source_code="tencent_quote",
                ),
            ),
            requested_symbols=("SSE:600000",),
            failed_symbols=(),
            schema_version="tencent_quote.qt_gtimg.v1",
            raw_observed_at=(observed,),
        )

    def fetch_five_level_quotes(
        self, symbols: tuple[str, ...], *, deadline: datetime | None = None
    ) -> RealtimeQuoteFetch:
        assert symbols == ("SSE:600000",)
        assert deadline is None
        return self.fetch


def test_tencent_collection_writes_raw_and_terminal_snapshot(tmp_path: Path) -> None:
    observed = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    persistence = FakePersistence({"SSE:600000"})

    run = collect_tencent_realtime_quotes(
        cast(PostgreSQLRealtimeQuotePersistence, persistence),
        LocalRawStore(tmp_path),
        ("SSE:600000",),
        provider=cast(RealtimeQuoteProvider, FakeProvider(observed)),
        clock=lambda: observed,
    )

    assert persistence.created[0].status is IngestionStatus.RUNNING
    assert run.status is IngestionStatus.SUCCEEDED
    terminal, manifest, quality, records = persistence.committed[0]
    assert terminal.accepted_rows == 1
    assert manifest.object_path.startswith("tencent_quote/five_level_quote/")
    assert quality == ()
    assert records[0].cumulative_volume == 10_000
    assert (tmp_path / Path(manifest.object_path)).is_file()


def test_tencent_collection_fails_before_network_for_unknown_symbol(tmp_path: Path) -> None:
    observed = datetime(2026, 8, 21, 8, 15, tzinfo=UTC)
    persistence = FakePersistence(set())

    try:
        collect_tencent_realtime_quotes(
            cast(PostgreSQLRealtimeQuotePersistence, persistence),
            LocalRawStore(tmp_path),
            ("SSE:600000",),
            provider=cast(RealtimeQuoteProvider, FakeProvider(observed)),
            clock=lambda: observed,
        )
    except ValueError as error:
        assert "unknown or non-stock" in str(error)
    else:
        raise AssertionError("unknown stock must fail")

    assert persistence.failed[0].status is IngestionStatus.FAILED
    assert persistence.committed == []
