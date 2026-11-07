import json
from datetime import date, datetime, timedelta
from struct import pack

import pytest

from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.stock_minute_volume import (
    SOURCE_SCHEMA_VERSION,
    UNIT_EVIDENCE_VERSION,
    ExactMinuteVolumeCommand,
    PytdxMinuteVolumeProvider,
    replay,
    validate_minute_volume_identity,
)

DAY = date(2026, 11, 3)


def metadata():
    return {
        "provider": "pytdx",
        "endpoint": "get_security_bars",
        "schema_version": SOURCE_SCHEMA_VERSION,
        "instrument_type": "stock",
        "field": "volume",
        "category": 8,
        "market": 1,
        "exchange": "SSE",
        "peer": "103.251.85.94:7709",
        "unit_evidence_version": UNIT_EVIDENCE_VERSION,
        "source_quantity_unit": "share",
        "standard_quantity_unit": "share",
        "symbol": "SSE:600327",
        "trade_date": DAY.isoformat(),
        "history_boundary_reached": True,
        "pages": [],
    }


def raw(stamp="2026-11-03 09:58:00", bits="1256034384"):
    identity = metadata()
    return {
        **{
            k: str(v) for k, v in identity.items() if k not in {"pages", "history_boundary_reached"}
        },
        "timestamp": stamp,
        "volume_wire_uint32": bits,
        "page_start": "0",
        "timestamp_wire_day": str(((2026 - 2004) << 11) + 1103),
        "timestamp_wire_minute": str(9 * 60 + 58),
    }


def wire(bits=0x4ADD9050, minutes=9 * 60 + 58):
    zipday = ((2026 - 2004) << 11) + 1103
    return pack("<HH", zipday, minutes) + bytes(4) + pack("<II", bits, 0)


def test_exact_wire_parser_and_replay_keep_native_bits_without_prices():
    command = ExactMinuteVolumeCommand(None)
    rows = command.parseResponse(pack("<H", 1) + wire())
    assert rows[0].volume_wire_uint32 == 0x4ADD9050
    result = replay((raw(bits=str(0x4ADD9050)),), metadata())
    assert result.bars[0].volume_shares == 7260200
    assert result.bars[0].timestamp == datetime(2026, 11, 3, 9, 58)
    assert set(result.raw_rows[0]).isdisjoint({"open", "close", "high", "low", "amount"})


@pytest.mark.parametrize(
    "key,value",
    [
        ("peer", "127.0.0.1:7709"),
        ("category", 9),
        ("category", True),
        ("market", 0),
        ("market", True),
        ("exchange", "BSE"),
        ("field", "amount"),
        ("schema_version", "pytdx.remote_daily_bar.v2"),
        ("unit_evidence_version", "unknown"),
        ("symbol", "SSE:000001"),
        ("source_quantity_unit", "lot"),
    ],
)
def test_unknown_identity_fails_before_normalization_or_replay(key, value):
    values = {**metadata(), key: value}
    with pytest.raises(ProviderError, match="quantity unit"):
        validate_minute_volume_identity(values)
    with pytest.raises(ProviderError, match="quantity unit"):
        replay((raw(),), values)


def test_szse_evidence_is_independently_admitted_but_not_bse():
    validate_minute_volume_identity(
        {**metadata(), "market": 0, "exchange": "SZSE", "symbol": "SZSE:000001"}
    )
    with pytest.raises(ProviderError, match="quantity unit"):
        validate_minute_volume_identity(
            {**metadata(), "market": 0, "exchange": "BSE", "symbol": "BSE:920000"}
        )


@pytest.mark.parametrize("bits", [0xBF800000, 0x7F800000, 0x7FC00000, 0x3FC00000, 0x5F000000])
def test_invalid_wire_quantity_is_rejected(bits):
    with pytest.raises(ProviderError):
        ExactMinuteVolumeCommand(None).parseResponse(pack("<H", 1) + wire(bits))
    with pytest.raises(ProviderError):
        replay((raw(bits=str(bits)),), metadata())


@pytest.mark.parametrize(
    "body", [b"", pack("<H", 801), pack("<H", 1), b"\0\0extra", pack("<H", 2) + wire() + wire()]
)
def test_bad_protocol_body_is_rejected(body):
    with pytest.raises(ProviderError, match="source_response_invalid"):
        ExactMinuteVolumeCommand(None).parseResponse(body)


def test_replay_checks_row_identity_and_exact_date():
    with pytest.raises(ProviderError):
        replay(({**raw(), "peer": "unverified:7709"},), metadata())
    with pytest.raises(ProviderError):
        replay((raw(stamp="2026-11-02 09:58:00"),), metadata())


def pool_file(tmp_path, peer="103.251.85.94"):
    path = tmp_path / "pool.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "pytdx.endpoint_pool.v1",
                "refreshed_at": "2026-11-07T16:36:32+00:00",
                "nodes": [
                    {
                        "host": peer,
                        "port": 7709,
                        "latency_ms": 1,
                        "capabilities": {
                            "quote": True,
                            "daily_bar_sse": True,
                            "daily_bar_szse": True,
                            "daily_bar_bse": False,
                        },
                    }
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def test_fetch_uses_only_configured_peer_and_raw_roundtrip(tmp_path, monkeypatch):
    from market_data_center.providers import stock_minute_volume as module

    class Client:
        def connect(self, host, port, *, time_out, deadline):
            assert (host, port, time_out) == ("103.251.85.94", 7709, 3.0)
            return True

        def disconnect(self):
            pass

        def read_page(self, market, code, start, timeout):
            assert (market, code, start) == (1, "600327", 0)
            command = ExactMinuteVolumeCommand(None)
            body = pack("<H", 240) + b"".join(
                wire(0, h * 60 + minute + i) for h, minute in ((9, 31), (13, 1)) for i in range(120)
            )
            rows = command.parseResponse(body)
            return rows, {
                "start": start,
                "requested_count": 800,
                "returned": len(rows),
                "wire_sha256": command.digest,
                "wire_bytes": len(body),
            }

    monkeypatch.setattr(module, "_default_client_factory", Client)
    path = pool_file(tmp_path)
    before = path.read_bytes()
    result = PytdxMinuteVolumeProvider(path).fetch("SSE:600327", DAY)
    assert len(result.bars) == 240
    assert result.metadata["history_boundary_reached"] is True
    assert replay(result.raw_rows, result.metadata).bars == result.bars
    assert path.read_bytes() == before


def test_unverified_pool_never_connects(tmp_path, monkeypatch):
    from market_data_center.providers import stock_minute_volume as module

    monkeypatch.setattr(module, "_default_client_factory", lambda: pytest.fail("network forbidden"))
    with pytest.raises(ProviderError, match="quantity unit"):
        PytdxMinuteVolumeProvider(pool_file(tmp_path, "127.0.0.1")).fetch("SSE:600327", DAY)


@pytest.mark.parametrize("kind", ["overlap", "read_failure", "total_budget", "page_budget"])
def test_fetch_incomplete_pages_never_produce_a_peak(tmp_path, monkeypatch, kind):
    from market_data_center.providers import stock_minute_volume as module
    from market_data_center.stock_max_minute_volume_calculator import calculate_minute_volume_peak

    calls = []

    class Client:
        def connect(self, *args, **kwargs):
            return True

        def disconnect(self):
            pass

        def read_page(self, market, code, start, timeout):
            calls.append(start)
            if kind == "read_failure":
                raise OSError("must not escape as raw error text")
            day = 20 - (start // 800 if kind == "page_budget" else 0)
            rows = [
                module._WireMinute(
                    datetime(2026, 11, day) + timedelta(minutes=i),
                    0,
                    ((2026 - 2004) << 11) + 1100 + day,
                    i,
                )
                for i in range(800)
            ]
            return rows, {"start": start, "requested_count": 800, "returned": 800}

    monkeypatch.setattr(module, "_default_client_factory", Client)
    if kind == "total_budget":
        ticks = iter([0.0, 1.0, 31.0])
        monkeypatch.setattr(module, "monotonic", lambda: next(ticks))
    provider = PytdxMinuteVolumeProvider(pool_file(tmp_path))
    if kind == "page_budget":
        result = provider.fetch("SSE:600327", DAY)
        assert calls == [0, 800, 1600, 2400, 3200, 4000]
        assert "collection_budget_exhausted" in result.quality_reasons
        assert calculate_minute_volume_peak(result).max_minute_volume_shares is None
    else:
        with pytest.raises(
            ProviderError,
            match={
                "overlap": "source_response_invalid",
                "read_failure": "minute_capability_unavailable",
                "total_budget": "collection_budget_exhausted",
            }[kind],
        ):
            provider.fetch("SSE:600327", DAY)


def test_exact_decode_is_independent_of_callers_decimal_precision():
    from decimal import localcontext

    with localcontext() as context:
        context.prec = 2
        result = replay((raw(bits=str(0x4ADD9050)),), metadata())
    assert result.bars[0].volume_shares == 7260200


def test_socket_budget_is_rechecked_for_each_received_fragment(monkeypatch):
    from market_data_center.providers import stock_minute_volume as module

    class Socket:
        def settimeout(self, value):
            assert 0 < value <= 3

        def recv(self, count):
            return b"x"

    ticks = iter([1.0, 5.0])
    monkeypatch.setattr(module, "monotonic", lambda: next(ticks))
    bounded = module._ReadDeadlineSocket(Socket(), 4.0)
    assert bounded.recv(100) == b"x"
    with pytest.raises(ProviderError, match="collection_budget_exhausted"):
        bounded.recv(100)


def test_connect_setup_fragments_share_the_collection_deadline(tmp_path, monkeypatch):
    from market_data_center.providers import stock_minute_volume as module

    clock = [0.0]
    received = []

    class Socket:
        def settimeout(self, value):
            assert 0 < value <= 3

        def recv(self, count):
            clock[0] += 2.5
            received.append(count)
            return b"x"

    def connect(client, host, port, *, time_out):
        client.client = Socket()
        if client.need_setup:
            client.setup()
        return client

    def setup(client):
        for _ in range(100):
            client.client.recv(1)

    monkeypatch.setattr(module, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module.TdxHq_API, "connect", connect)
    monkeypatch.setattr(module.TdxHq_API, "setup", setup)
    with pytest.raises(ProviderError, match="collection_budget_exhausted"):
        PytdxMinuteVolumeProvider(pool_file(tmp_path)).fetch("SSE:600327", DAY)
    assert clock[0] <= module.TOTAL_TIMEOUT
    assert len(received) == 12


@pytest.mark.parametrize("value", [None, True, "20261103", "2026-02-30", "2026-11-03T00:00:00"])
def test_source_identity_rejects_missing_or_noncanonical_trade_date(value):
    with pytest.raises(ProviderError, match="quantity unit"):
        validate_minute_volume_identity({**metadata(), "trade_date": value})
