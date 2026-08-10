"""Bulk-collect daily bars from local TDX .day files into the database.

Reads all local .day files for listed stocks and batch-UPSERTs them in
one transaction per batch (~500 rows), which is ~2000x faster than the
per-symbol pipeline on a high-latency database link.

This is a data-loading utility, not the regular ingestion path. The
worker's daily-run job still uses the full pipeline (per-symbol
ingestion_run + Raw manifest + quality checks) for traceability. Use
this script when you need to fill or backfill a database quickly
(e.g. new database setup, historical backfill).

Usage::

    uv run python scripts/bulk_collect_daily_bars.py --start-date 2026-08-03 --end-date 2026-08-07

Requires PYTDX_VIPDOC_PATH in .env (or environment). Reads DATABASE_URL
from .env via WorkerSettings.
"""

from __future__ import annotations

import os
import struct
import sys
import time
from datetime import date
from pathlib import Path
from uuid import uuid4

from sqlalchemy import create_engine, text

from market_data_center.database_urls import sqlalchemy_url
from market_data_center.settings import WorkerSettings

BATCH_SIZE = 500

UPSERT_SQL = """
    insert into core.daily_bar (
        symbol, trade_date, market, open, high, low, close,
        amount, volume, trade_status, source_code, ingestion_id
    ) values (
        :symbol, :trade_date, 'CN_A_SHARE',
        cast(:open as numeric)/100, cast(:high as numeric)/100,
        cast(:low as numeric)/100, cast(:close as numeric)/100,
        cast(:amount as numeric), :volume, 'unknown', 'pytdx', cast(:ingestion_id as uuid)
    )
    on conflict (symbol, trade_date) do update set
        market=excluded.market, open=excluded.open, high=excluded.high,
        low=excluded.low, close=excluded.close, amount=excluded.amount,
        volume=excluded.volume, trade_status=excluded.trade_status,
        source_code=excluded.source_code, ingestion_id=excluded.ingestion_id
"""

RUN_SQL = """
    insert into ingestion.ingestion_run
    (ingestion_id, provider_code, dataset_code, status, requested_at, started_at, finished_at,
     request_params, fetched_rows, accepted_rows, rejected_rows)
    values (cast(:ingestion_id as uuid), 'pytdx', 'daily_bar', 'succeeded', now(), now(), now(),
            cast(:params as jsonb), :fetched, :accepted, 0)
"""


def _parse_args() -> tuple[date, date]:
    start_str = "2026-08-03"
    end_str = "2026-08-07"
    args = sys.argv[1:]
    i = 0
    while i < len(args):
        if args[i] == "--start-date" and i + 1 < len(args):
            start_str = args[i + 1]
            i += 2
        elif args[i] == "--end-date" and i + 1 < len(args):
            end_str = args[i + 1]
            i += 2
        else:
            i += 1
    return date.fromisoformat(start_str), date.fromisoformat(end_str)


def _read_day_file(path: Path, start: date, end: date) -> list[tuple]:
    """Read a .day file, return rows within [start, end] as (date_str, o, h, l, c, amount, vol)."""
    data = path.read_bytes()
    rows: list[tuple] = []
    for offset in range(0, len(data) - 31, 32):
        raw_date, o, h, low, c, amount, vol, _ = struct.unpack_from("<IIIIIfII", data, offset)
        try:
            trade_date = date(raw_date // 10000, raw_date % 10000 // 100, raw_date % 100)
        except ValueError:
            continue
        if trade_date < start or trade_date > end:
            continue
        rows.append(
            (trade_date.isoformat(), str(o), str(h), str(low), str(c), f"{amount:.0f}", str(vol))
        )
    return rows


def main() -> None:
    start_date, end_date = _parse_args()
    vipdoc = os.environ.get("PYTDX_VIPDOC_PATH", "")
    if not vipdoc:
        print("PYTDX_VIPDOC_PATH is required (set in .env or environment)", file=sys.stderr)
        sys.exit(1)

    settings = WorkerSettings()
    engine = create_engine(sqlalchemy_url(settings.database_url.get_secret_value()))

    # Get listed stock symbols
    with engine.connect() as conn:
        symbols = [
            r[0]
            for r in conn.execute(
                text(
                    "select symbol from core.security "
                    "where security_type='stock' and status='listed' order by symbol"
                )
            ).all()
        ]
    print(f"listed stocks: {len(symbols)}")

    # Read all .day files locally
    t0 = time.perf_counter()
    all_rows: list[dict] = []
    missing = 0
    for symbol in symbols:
        exchange, code = symbol.split(":")
        ex_short = {"SSE": "sh", "SZSE": "sz", "BSE": "bj"}[exchange]
        day_file = Path(vipdoc) / ex_short / "lday" / f"{ex_short}{code}.day"
        if not day_file.is_file():
            missing += 1
            continue
        for d_str, o, h, low, c, amount, vol in _read_day_file(day_file, start_date, end_date):
            all_rows.append(
                {
                    "symbol": symbol,
                    "trade_date": d_str,
                    "open": o,
                    "high": h,
                    "low": low,
                    "close": c,
                    "amount": amount,
                    "volume": vol,
                }
            )
    read_time = time.perf_counter() - t0
    msg = (
        f"read {len(all_rows)} bar rows from .day files "
        f"({missing} symbols missing) in {read_time:.1f}s"
    )
    print(msg)

    if not all_rows:
        print("no data to write", file=sys.stderr)
        sys.exit(1)

    # Batch UPSERT
    ingestion_id = str(uuid4())
    import json

    params = json.dumps(
        {
            "mode": "bulk_local",
            "start_date": start_date.isoformat(),
            "end_date": end_date.isoformat(),
        }
    )
    db_start = time.perf_counter()
    with engine.begin() as conn:
        # Write ingestion_run first (FK target)
        conn.execute(
            text(RUN_SQL),
            {
                "ingestion_id": ingestion_id,
                "params": params,
                "fetched": len(all_rows),
                "accepted": len(all_rows),
            },
        )
        # Batch upsert daily bars
        total = len(all_rows)
        for i in range(0, total, BATCH_SIZE):
            batch = all_rows[i : i + BATCH_SIZE]
            for row in batch:
                row["ingestion_id"] = ingestion_id
            conn.execute(text(UPSERT_SQL), batch)
            done = min(i + BATCH_SIZE, total)
            if done % 5000 == 0 or done == total:
                elapsed = time.perf_counter() - db_start
                rate = done / elapsed if elapsed > 0 else 0
                print(f"  written {done}/{total} ({rate:.0f} rows/s)", flush=True)

    db_time = time.perf_counter() - db_start
    total_time = time.perf_counter() - t0
    done_msg = (
        f"\ndone: {len(all_rows)} rows in {total_time:.1f}s "
        f"(local {read_time:.1f}s + db {db_time:.1f}s)"
    )
    print(done_msg)
    engine.dispose()


if __name__ == "__main__":
    main()
