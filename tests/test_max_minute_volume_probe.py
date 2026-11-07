"""Synthetic protocol fixtures only; this experiment never needs a database."""

import sys
from datetime import date, datetime, timedelta
from decimal import Decimal
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from struct import pack

import pytest

SCRIPT = Path(__file__).parents[1] / "scripts" / "experiments" / "max_minute_volume_probe.py"
SPEC = spec_from_file_location("max_minute_volume_probe", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
probe = module_from_spec(SPEC)
sys.modules[SPEC.name] = probe
SPEC.loader.exec_module(probe)
DAY = date(2026, 11, 3)


def sample_rows():
    labels = [
        datetime(2026, 11, 3, hour, minute) + timedelta(minutes=i)
        for hour, minute in ((9, 31), (13, 1))
        for i in range(120)
    ]
    return [probe.MinuteRow(label, Decimal(0)) for label in labels]


@pytest.mark.parametrize("peak_index", [0, 27, 239])
def test_peak_keeps_native_first_and_last_bars_and_never_certifies(peak_index):
    rows = sample_rows()
    rows[peak_index] = probe.MinuteRow(rows[peak_index].timestamp, Decimal(7260200))
    result = probe.summarize(DAY, rows, boundary_reached=True)
    assert result["status"] == "complete_experimental"
    assert result["experimental_max_source_volume"] == 7260200
    assert result["peak_at"] == rows[peak_index].timestamp.strftime("%Y-%m-%d %H:%M:%S")
    assert result["max_minute_volume_shares"] is None


def test_complete_zero_is_zero_and_tie_is_earliest():
    result = probe.summarize(DAY, list(reversed(sample_rows())), boundary_reached=True)
    assert result["experimental_max_source_volume"] == 0
    assert result["peak_at"] == "2026-11-03 09:31:00"


@pytest.mark.parametrize("kind", ["missing", "duplicate", "unexpected", "unbounded", "wrong_day"])
def test_incomplete_or_invalid_inputs_never_return_partial_peak(kind):
    rows = sample_rows()
    if kind == "missing":
        rows.pop()
    elif kind == "duplicate":
        rows.append(rows[0])
    elif kind == "unexpected":
        rows.append(probe.MinuteRow(datetime(2026, 11, 3, 9, 30), Decimal(1)))
    elif kind == "wrong_day":
        rows = [probe.MinuteRow(row.timestamp - timedelta(days=1), row.volume) for row in rows]
    result = probe.summarize(DAY, rows, boundary_reached=kind != "unbounded")
    assert result["status"] == "incomplete"
    assert result["experimental_max_source_volume"] is None
    assert result["peak_at"] is None


def wire_row(encoded_volume):
    day = ((2026 - 2004) << 11) + 1103
    return pack("<HH", day, 9 * 60 + 58) + bytes(4) + pack("<II", encoded_volume, 0)


def test_exact_parser_decodes_peak_without_float():
    command = probe.ExactMinuteBars(None)
    rows = command.parseResponse(pack("<H", 1) + wire_row(0x4ADD9050))
    assert rows == [probe.MinuteRow(datetime(2026, 11, 3, 9, 58), Decimal(7260200))]
    assert command.body_size == 18


@pytest.mark.parametrize("bits", [0xBF800000, 0x7F800000, 0x7FC00000, 0x3FC00000])
def test_parser_rejects_negative_nonfinite_and_fractional_volume(bits):
    with pytest.raises(ValueError, match="minute response"):
        probe.ExactMinuteBars(None).parseResponse(pack("<H", 1) + wire_row(bits))


@pytest.mark.parametrize("body", [b"", pack("<H", 801), pack("<H", 1), b"\0\0extra"])
def test_parser_rejects_truncated_oversized_or_trailing_body(body):
    with pytest.raises(ValueError, match="minute response"):
        probe.ExactMinuteBars(None).parseResponse(body)


def test_page_budget_cannot_yield_a_partial_maximum():
    calls = []

    def read_page(start):
        calls.append(start)
        # Complete-looking target rows still need a trustworthy history boundary.
        return sample_rows(), {"returned": 800}

    rows, _, bounded = probe.collect_pages(DAY, read_page)
    assert calls == [0, 800, 1600, 2400, 3200, 4000]
    assert not bounded
    assert (
        probe.summarize(DAY, rows, boundary_reached=bounded)["experimental_max_source_volume"]
        is None
    )


def test_run_isolated_from_database_and_dotenv(tmp_path, monkeypatch):
    import dotenv
    import sqlalchemy

    def forbidden(*args, **kwargs):
        pytest.fail("experiment must not load dotenv or connect to a database")

    monkeypatch.setenv("DATABASE_URL", "must-not-be-read")
    monkeypatch.setenv("PYTDX_POOL_PATH", "must-not-be-read")
    monkeypatch.setattr(dotenv, "load_dotenv", forbidden)
    monkeypatch.setattr(sqlalchemy, "create_engine", forbidden)
    monkeypatch.setattr(probe, "fetch", lambda *args: (sample_rows(), [], True))
    pool_file = tmp_path / "explicit-pool.json"
    pool_file.write_text(
        '{"schema_version":"pytdx.endpoint_pool.v1",'
        '"refreshed_at":"2026-11-07T16:36:32.696319+00:00",'
        '"nodes":[{"host":"103.251.85.94","port":7709,"latency_ms":140,'
        '"capabilities":{"quote":true,"daily_bar_sse":true,"daily_bar_szse":true,'
        '"daily_bar_bse":false}}]}',
        encoding="utf-8",
    )
    result = probe.run_probe("SSE:600327", DAY, pool_file)
    assert result["experiment"] is True
    assert result["persisted"] is False
    assert result["max_minute_volume_shares"] is None
    assert result["unit_research"]["status"] == "research_verified_not_registered"
