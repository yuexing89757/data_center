"""Isolated, read-only local experiment; not a production ingestion provider."""

import argparse
import hashlib
import json
import re
from collections import Counter
from collections.abc import Callable, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from struct import error as StructError
from struct import unpack_from
from time import monotonic
from zoneinfo import ZoneInfo

from pytdx.helper import get_price
from pytdx.hq import TdxHq_API
from pytdx.parser.get_security_bars import GetSecurityBarsCmd

from market_data_center.providers.pytdx_pool import PytdxCapability, load_endpoint_pool
from market_data_center.providers.pytdx_protocol import decode_volume_decimal

PAGE_SIZE = 800
MAX_PAGES = 6
READ_TIMEOUT = 3.0
TOTAL_TIMEOUT = 30.0


@dataclass(frozen=True)
class MinuteRow:
    timestamp: datetime
    volume: Decimal


def checked_volume(value: Decimal) -> int:
    if (
        not isinstance(value, Decimal)
        or not value.is_finite()
        or value < 0
        or value != value.to_integral_value()
        or value > 9_223_372_036_854_775_807
    ):
        raise ValueError("invalid experimental source quantity")
    return int(value)


class ExactMinuteBars(GetSecurityBarsCmd):
    """Read only labels and native quantities; skip prices without float decoding."""

    def parseResponse(self, body_buf):
        self.body_size = len(body_buf)
        self.digest = hashlib.sha256(body_buf).hexdigest()
        try:
            (count,) = unpack_from("<H", body_buf)
            if count > PAGE_SIZE or len(body_buf) < 2 + count * 16:
                raise ValueError("incomplete or oversized page")
            rows = []
            pos = 2
            for _ in range(count):
                zipday, minutes = unpack_from("<HH", body_buf, pos)
                timestamp = datetime(
                    (zipday >> 11) + 2004,
                    (zipday % 2048) // 100,
                    zipday % 2048 % 100,
                    minutes // 60,
                    minutes % 60,
                )
                pos += 4
                for _ in range(4):
                    _, pos = get_price(body_buf, pos)
                (bits,) = unpack_from("<I", body_buf, pos)
                pos += 8  # Amount is outside the experiment's contract.
                if bits >> 31 or (bits >> 23) & 255 == 255:
                    raise ValueError("negative or nonfinite wire volume")
                volume = decode_volume_decimal(bits) if bits else Decimal(0)
                checked_volume(volume)
                rows.append(MinuteRow(timestamp, volume))
            if pos != len(body_buf):
                raise ValueError("trailing bytes")
            stamps = [row.timestamp for row in rows]
            if stamps != sorted(set(stamps)):
                raise ValueError("unordered or duplicate page")
            return rows
        except (IndexError, StructError, ValueError) as error:
            raise ValueError("invalid pytdx minute response") from error


def collect_pages(
    day: date,
    read_page: Callable[[int], tuple[list[MinuteRow], dict]],
) -> tuple[list[MinuteRow], list[dict], bool]:
    rows = []
    pages = []
    for page in range(MAX_PAGES):
        current, evidence = read_page(page * PAGE_SIZE)
        rows.extend(current)
        pages.append(evidence)
        if (
            not current
            or any(row.timestamp.date() < day for row in current)
            or evidence["returned"] < PAGE_SIZE
        ):
            return rows, pages, True
    return rows, pages, False


def summarize(day: date, rows: Sequence[MinuteRow], *, boundary_reached: bool) -> dict:
    selected = [row for row in rows if row.timestamp.date() == day]
    expected = {
        datetime(day.year, day.month, day.day, hour, minute) + timedelta(minutes=i)
        for hour, minute in ((9, 31), (13, 1))
        for i in range(120)
    }
    counts = Counter(row.timestamp for row in selected)
    missing = sorted(expected - counts.keys())
    unexpected = sorted(counts.keys() - expected)
    duplicates = sorted(stamp for stamp, count in counts.items() if count > 1)
    for row in selected:
        checked_volume(row.volume)
    complete = boundary_reached and not missing and not unexpected and not duplicates
    peak = min(selected, key=lambda row: (-row.volume, row.timestamp)) if complete else None
    return {
        "status": "complete_experimental" if complete else "incomplete",
        "expected_count": 240,
        "observed_count": len(selected),
        "history_boundary_reached": boundary_reached,
        "missing_labels": [stamp.strftime("%H:%M:%S") for stamp in missing],
        "unexpected_labels": [stamp.strftime("%H:%M:%S") for stamp in unexpected],
        "duplicate_labels": [stamp.strftime("%H:%M:%S") for stamp in duplicates],
        "experimental_max_source_volume": checked_volume(peak.volume) if peak else None,
        "peak_at": peak.timestamp.strftime("%Y-%m-%d %H:%M:%S") if peak else None,
        "max_minute_volume_shares": None,
        "first_last_native_bars_included": True,
    }


def fetch(host: str, port: int, market: int, code: str, day: date):
    deadline = monotonic() + TOTAL_TIMEOUT
    api = TdxHq_API(heartbeat=False, auto_retry=False, raise_exception=True)
    try:
        if not api.connect(host, port, time_out=READ_TIMEOUT):
            raise ValueError("explicit pool peer unavailable")

        def read_page(start):
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise ValueError("experimental collection budget exhausted")
            api.client.settimeout(min(READ_TIMEOUT, remaining))
            command = ExactMinuteBars(api.client, lock=api.lock)
            command.setParams(8, market, code, start, PAGE_SIZE)
            rows = command.call_api()
            if monotonic() > deadline:
                raise ValueError("experimental collection budget exhausted")
            if not isinstance(rows, list):
                raise ValueError("minute response unavailable")
            return rows, {
                "start": start,
                "requested_count": PAGE_SIZE,
                "returned": len(rows),
                "first": rows[0].timestamp.isoformat(" ") if rows else None,
                "last": rows[-1].timestamp.isoformat(" ") if rows else None,
                "wire_bytes": command.body_size,
                "wire_sha256": command.digest,
            }

        return collect_pages(day, read_page)
    finally:
        with suppress(Exception):
            api.disconnect()


def run_probe(symbol: str, day: date, pool_file: Path) -> dict:
    match = re.fullmatch(r"(SSE|SZSE):([0-9]{6})", symbol)
    if match is None:
        raise ValueError("experiment supports explicit SSE/SZSE six-digit symbols only")
    if day > datetime.now(ZoneInfo("Asia/Shanghai")).date():
        raise ValueError("trade date cannot be in the future")
    pool = load_endpoint_pool(pool_file)
    capability = (
        PytdxCapability.DAILY_BAR_SSE if match[1] == "SSE" else PytdxCapability.DAILY_BAR_SZSE
    )
    # Existing flags select a candidate only, never certify minute capability or units.
    node = next((node for node in pool.nodes if node.capabilities[capability]), None)
    if node is None:
        raise ValueError("explicit pool snapshot has no candidate for this market")
    rows, pages, bounded = fetch(node.host, node.port, int(match[1] == "SSE"), match[2], day)
    result = summarize(day, rows, boundary_reached=bounded)
    researched = (
        node.host == "103.251.85.94"
        and node.port == 7709
        and symbol == "SSE:600327"
        and day == date(2026, 11, 3)
    )
    return {
        "experiment": True,
        "persisted": False,
        "symbol": symbol,
        "trade_date": day.isoformat(),
        "observed_at": datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y-%m-%d %H:%M:%S"),
        "source": {
            "provider": "pytdx",
            "endpoint": "get_security_bars",
            "category": 8,
            "market": int(match[1] == "SSE"),
            "peer": f"{node.host}:{node.port}",
            "pool_snapshot_sha256": hashlib.sha256(pool_file.read_bytes()).hexdigest(),
            "pool_refreshed_at": pool.refreshed_at.isoformat(),
            "live_production_pool_checked": False,
            "minute_capability_registered": False,
        },
        "unit_research": {
            "status": "research_verified_not_registered" if researched else "unverified",
            "proposed_source_unit": "share" if researched else None,
            "proposed_multiplier": 1 if researched else None,
            "formal_ingestion_certificate": False,
            "evidence_document": "docs/minute-volume-unit-evidence-2026-11-08.md",
        },
        **result,
        "pages": pages,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbol", required=True)
    parser.add_argument("--trade-date", required=True, type=date.fromisoformat)
    parser.add_argument("--pool-file", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="Optional local summary JSON; never minute Raw")
    args = parser.parse_args(argv)
    try:
        result = run_probe(args.symbol, args.trade_date, args.pool_file)
    except Exception as error:
        # Do not echo exception text, which can contain paths or connection settings.
        print(
            json.dumps({"experiment": True, "status": "failed", "error_type": type(error).__name__})
        )
        return 1
    serialized = json.dumps(result, ensure_ascii=False, indent=2)
    if args.output is not None:
        args.output.write_text(serialized + "\n", encoding="utf-8")
    print(serialized)
    return 0 if result["status"] == "complete_experimental" else 2


if __name__ == "__main__":
    raise SystemExit(main())
