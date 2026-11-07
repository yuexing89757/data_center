from contextlib import nullcontext
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from market_data_center import stock_max_minute_volume_service as module
from market_data_center.domain.stock_max_minute_volume import MinuteVolumeBuildSummary
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.stock_minute_volume import IDENTITY_FIELDS, replay
from market_data_center.raw_store import LocalRawStore

DAY = date(2026, 11, 3)


def source(symbol="SSE:600327", *, complete=True):
    exchange = symbol.split(":")[0]
    metadata = {
        "provider": "pytdx",
        "endpoint": "get_security_bars",
        "schema_version": "pytdx.stock_minute_volume.v1",
        "instrument_type": "stock",
        "field": "volume",
        "peer": "103.251.85.94:7709",
        "category": 8,
        "market": 1 if exchange == "SSE" else 0,
        "exchange": exchange,
        "unit_evidence_version": "pytdx-minute-volume.2026-11-08.v1",
        "source_quantity_unit": "share",
        "standard_quantity_unit": "share",
        "symbol": symbol,
        "trade_date": DAY.isoformat(),
        "history_boundary_reached": True,
    }
    labels = [
        datetime(2026, 11, 3, hour, minute) + timedelta(minutes=i)
        for hour, minute in ((9, 31), (13, 1))
        for i in range(120)
    ]
    rows = [
        {
            **{key: str(metadata[key]) for key in IDENTITY_FIELDS},
            "timestamp": stamp.isoformat(" "),
            "timestamp_wire_day": str(((2026 - 2004) << 11) + 1103),
            "timestamp_wire_minute": str(stamp.hour * 60 + stamp.minute),
            "volume_wire_uint32": str(0x41200000),
            "page_start": "0",
        }
        for stamp in labels
    ]
    return replay(rows if complete else rows[:-1], metadata)


class MemoryPersistence:
    def __init__(self, trading=True, symbols=("SSE:600327",)):
        self.trading = trading
        self.symbols = symbols
        self.recorded = []
        self.published = []

    def is_trading_day(self, day):
        return self.trading

    def candidates(self, day, symbols=None):
        return self.symbols

    def build_lock(self, day):
        return nullcontext()

    def record_source(self, *args):
        self.recorded.append(args)

    def publish(self, day, peaks, evidence):
        self.published.append((day, peaks, evidence))
        count = sum(p.max_minute_volume_shares is not None for p in peaks)
        return MinuteVolumeBuildSummary(
            "ready" if count else "not_ready", day, None, None, len(peaks), count
        )


class Provider:
    def __init__(self, result):
        self.result = result
        self.calls = []

    def fetch(self, symbol, day):
        self.calls.append(symbol)
        if isinstance(self.result, Exception):
            raise self.result
        return self.result


def test_complete_source_is_replayed_archived_and_certified_before_publication(tmp_path: Path):
    persistence = MemoryPersistence()
    service = module.StockMaxMinuteVolumeService(
        persistence, LocalRawStore(tmp_path), Provider(source())
    )
    summary = service.build(DAY, symbols=("SSE:600327",))
    assert summary.complete_count == 1
    run, manifest, certificate = persistence.recorded[0]
    assert run.accepted_rows == 240
    assert manifest.row_count == 240
    assert not (tmp_path / manifest.object_path).exists()
    assert (tmp_path / (manifest.object_path + ".gz")).exists()
    assert certificate["field_group"] == "stock_minute_volume.volume"
    day, peaks, proof = persistence.published[0]
    assert day == DAY
    assert peaks[0].max_minute_volume_shares == 10
    assert proof["coverage_scope"] == "requested_symbols"
    assert proof["stocks"]["SSE:600327"]["evidence_hash"] == certificate["evidence_hash"]


def test_missing_bar_is_raw_only_and_never_certified(tmp_path):
    persistence = MemoryPersistence()
    service = module.StockMaxMinuteVolumeService(
        persistence, LocalRawStore(tmp_path), Provider(source(complete=False))
    )
    assert service.build(DAY).complete_count == 0
    run, manifest, certificate = persistence.recorded[0]
    assert run.accepted_rows == 0
    assert manifest.row_count == 239
    assert certificate is None
    assert persistence.published[0][1][0].max_minute_volume_shares is None


def test_provider_failure_records_redacted_failure_and_gap(tmp_path):
    persistence = MemoryPersistence()
    provider = Provider(RuntimeError("token=sensitive"))
    service = module.StockMaxMinuteVolumeService(persistence, LocalRawStore(tmp_path), provider)
    assert service.build(DAY).complete_count == 0
    run, manifest, certificate = persistence.recorded[0]
    assert run.status.value == "failed"
    assert run.error_summary == "RuntimeError"
    assert manifest is certificate is None
    assert "sensitive" not in str(persistence.published)


def test_unknown_units_close_entire_market_but_do_not_borrow_another_market(tmp_path):
    persistence = MemoryPersistence(symbols=("SSE:600327", "SSE:600000", "SZSE:000001"))
    provider = Provider(ProviderError("quantity unit contract unverified"))
    module.StockMaxMinuteVolumeService(persistence, LocalRawStore(tmp_path), provider).build(DAY)
    assert provider.calls == ["SSE:600327", "SZSE:000001"]
    assert all(peak.max_minute_volume_shares is None for peak in persistence.published[0][1])


def test_closed_or_unknown_calendar_does_not_fetch(tmp_path):
    for trading in (False, None):
        persistence = MemoryPersistence(trading=trading)
        provider = Provider(source())
        service = module.StockMaxMinuteVolumeService(persistence, LocalRawStore(tmp_path), provider)
        if trading is None:
            with pytest.raises(ValueError, match="calendar"):
                service.build(DAY)
        else:
            assert service.build(DAY).status == "skipped"
        assert not provider.calls


def test_invalid_requested_universe_does_not_start_network(tmp_path):
    service = module.StockMaxMinuteVolumeService(
        MemoryPersistence(), LocalRawStore(tmp_path), Provider(source())
    )
    with pytest.raises(ValueError):
        service.build(DAY, symbols=())


def test_late_unit_failure_prevents_earlier_market_certification(tmp_path):
    persistence = MemoryPersistence(symbols=("SSE:600327", "SSE:600000", "SZSE:000001"))

    class MixedProvider:
        def fetch(self, symbol, day):
            if symbol == "SSE:600000":
                raise ProviderError("quantity unit contract unverified")
            return source(symbol)

    summary = module.StockMaxMinuteVolumeService(
        persistence, LocalRawStore(tmp_path), MixedProvider()
    ).build(DAY)
    assert summary.complete_count == 1
    peaks = persistence.published[0][1]
    assert [peak.max_minute_volume_shares for peak in peaks] == [None, None, 10]
    assert all(run.accepted_rows == 0 and cert is None for run, _, cert in persistence.recorded[:2])
    assert persistence.recorded[0][1].row_count == 240
    assert persistence.recorded[2][2]["status"] == "certified"
