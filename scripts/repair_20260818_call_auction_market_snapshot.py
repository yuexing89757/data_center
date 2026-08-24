"""One-time repair approved by ADR-0041 and tracked by GitHub Issue #55."""

from argparse import ArgumentParser
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from json import JSONDecodeError, dumps, loads
from os import environ
from pathlib import Path
from re import fullmatch
from typing import Any
from uuid import UUID

import psycopg
from psycopg.rows import dict_row

from market_data_center.database_urls import psycopg_url
from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.raw_store import LocalRawStore

TARGET_INGESTION_ID = UUID("76e4416d-024f-45f6-8a30-cf58d7e3ad4e")
EXPECTED_ROW_COUNT = 5208
EXPECTED_DATASET = "call_auction_market_snapshot"
EXPECTED_RAW_SCHEMA = "market_data_center.call_auction_market_snapshot.raw.v1"
EXPECTED_PROVIDER_SCHEMA = "pytdx_hq.security_quotes.v1"
LEVELS = range(1, 6)
ORDER_BOOK_COLUMNS = (
    *(
        column
        for side in ("bid", "ask")
        for level in LEVELS
        for column in (f"{side}{level}_price", f"{side}{level}_volume")
    ),
    "seal_amount",
)


class RepairError(RuntimeError):
    """Raised before commit when the fixed repair boundary is not proven."""


@dataclass(frozen=True, slots=True)
class RepairRecord:
    symbol: str
    bid_prices: tuple[Decimal | None, ...]
    bid_volumes: tuple[int | None, ...]
    ask_prices: tuple[Decimal | None, ...]
    ask_volumes: tuple[int | None, ...]
    seal_amount: Decimal | None

    def database_values(self) -> tuple[object, ...]:
        values: list[object] = []
        for prices, volumes in (
            (self.bid_prices, self.bid_volumes),
            (self.ask_prices, self.ask_volumes),
        ):
            for price, volume in zip(prices, volumes, strict=True):
                values.extend((price, volume))
        values.append(self.seal_amount)
        return tuple(values)


def normalize_raw_rows(rows: Sequence[Mapping[str, str]]) -> dict[str, RepairRecord]:
    records: dict[str, RepairRecord] = {}
    for line_number, outer in enumerate(rows, start=1):
        if outer.get("provider_schema_version") != EXPECTED_PROVIDER_SCHEMA:
            raise RepairError(f"Raw row {line_number} has an unexpected provider schema")
        try:
            payload = loads(outer["provider_raw_json"])
        except (KeyError, JSONDecodeError, TypeError) as error:
            raise RepairError(f"Raw row {line_number} has invalid provider JSON") from error
        if not isinstance(payload, dict):
            raise RepairError(f"Raw row {line_number} provider JSON is not an object")

        symbol = _symbol(payload, line_number)
        if symbol in records:
            raise RepairError(f"duplicate Raw symbol: {symbol}")
        bid_prices, bid_volumes = _levels(payload, "bid", line_number)
        ask_prices, ask_volumes = _levels(payload, "ask", line_number)
        seal_amount = (
            bid_prices[0] * bid_volumes[0]
            if ask_volumes[0] in (None, 0)
            and bid_prices[0] is not None
            and bid_volumes[0] is not None
            else None
        )
        records[symbol] = RepairRecord(
            symbol=symbol,
            bid_prices=bid_prices,
            bid_volumes=bid_volumes,
            ask_prices=ask_prices,
            ask_volumes=ask_volumes,
            seal_amount=seal_amount,
        )
    return records


def validate_exact_symbol_set(
    records: Mapping[str, RepairRecord], database_symbols: set[str]
) -> None:
    raw_symbols = set(records)
    if raw_symbols != database_symbols:
        missing = sorted(database_symbols - raw_symbols)[:5]
        unexpected = sorted(raw_symbols - database_symbols)[:5]
        raise RepairError(
            f"Raw/database symbol set mismatch (missing={missing}, unexpected={unexpected})"
        )


def run_repair(*, apply: bool) -> dict[str, object]:
    database_url = environ.get("MIGRATION_DATABASE_URL") or environ.get("DATABASE_URL")
    raw_root = environ.get("RAW_DATA_ROOT")
    if not database_url or not raw_root:
        raise RepairError("DATABASE_URL/MIGRATION_DATABASE_URL and RAW_DATA_ROOT are required")

    options = "" if apply else "-c default_transaction_read_only=on"
    with (
        psycopg.connect(
            psycopg_url(database_url),
            connect_timeout=10,
            options=options,
            row_factory=dict_row,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute("set local statement_timeout = '60s'")
        manifest = _load_manifest(cursor)
        raw_rows = LocalRawStore(Path(raw_root)).read_jsonl(manifest)
        records = normalize_raw_rows(raw_rows)
        if len(records) != EXPECTED_ROW_COUNT:
            raise RepairError(
                f"normalized Raw row count is {len(records)}, expected {EXPECTED_ROW_COUNT}"
            )

        database_rows = _load_database_rows(cursor, lock=apply)
        if len(database_rows) != EXPECTED_ROW_COUNT:
            raise RepairError(
                f"database row count is {len(database_rows)}, expected {EXPECTED_ROW_COUNT}"
            )
        validate_exact_symbol_set(records, {str(row["symbol"]) for row in database_rows})
        state = _repair_state(records, database_rows)

        if apply and state == "pending":
            _apply_updates(cursor, records)
            verified_rows = _load_database_rows(cursor, lock=False)
            if _repair_state(records, verified_rows) != "already_applied":
                raise RepairError("post-update verification failed")
            state = "applied"

    return {
        "operation": "repair_20260818_call_auction_market_snapshot",
        "mode": "apply" if apply else "dry_run",
        "ingestion_id": str(TARGET_INGESTION_ID),
        "row_count": len(records),
        "status": state,
    }


def _load_manifest(cursor: Any) -> RawManifest:
    cursor.execute(
        """
        select manifest.raw_id, manifest.ingestion_id, manifest.storage_backend,
               manifest.object_path, manifest.file_format, manifest.content_sha256,
               manifest.byte_size, manifest.row_count, manifest.schema_version,
               run.dataset_code, run.status
        from ingestion.raw_manifest manifest
        join ingestion.ingestion_run run using (ingestion_id)
        where manifest.ingestion_id = %s
        """,
        (TARGET_INGESTION_ID,),
    )
    rows = cursor.fetchall()
    if len(rows) != 1:
        raise RepairError(f"expected one Manifest, found {len(rows)}")
    row = rows[0]
    if row["dataset_code"] != EXPECTED_DATASET or row["status"] != "succeeded":
        raise RepairError("target ingestion is not the expected succeeded dataset")
    if row["schema_version"] != EXPECTED_RAW_SCHEMA or row["row_count"] != EXPECTED_ROW_COUNT:
        raise RepairError("Manifest schema or row count does not match ADR-0041")
    return RawManifest(
        raw_id=row["raw_id"],
        ingestion_id=row["ingestion_id"],
        storage_backend=row["storage_backend"],
        object_path=row["object_path"],
        file_format=RawFileFormat(row["file_format"]),
        content_sha256=row["content_sha256"],
        byte_size=row["byte_size"],
        row_count=row["row_count"],
        schema_version=row["schema_version"],
    )


def _load_database_rows(cursor: Any, *, lock: bool) -> list[Mapping[str, object]]:
    columns = ", ".join(ORDER_BOOK_COLUMNS)
    suffix = " for update" if lock else ""
    cursor.execute(
        f"""select symbol, {columns}
            from realtime.call_auction_market_snapshot
            where ingestion_id = %s
            order by symbol{suffix}""",
        (TARGET_INGESTION_ID,),
    )
    return list(cursor.fetchall())


def _repair_state(
    records: Mapping[str, RepairRecord], database_rows: Sequence[Mapping[str, object]]
) -> str:
    pending = True
    already_applied = True
    for row in database_rows:
        current = tuple(row[column] for column in ORDER_BOOK_COLUMNS)
        expected = records[str(row["symbol"])].database_values()
        pending = pending and all(value is None for value in current)
        already_applied = already_applied and current == expected
    if pending:
        return "pending"
    if already_applied:
        return "already_applied"
    raise RepairError("target fields are partially populated or differ from Raw")


def _apply_updates(cursor: Any, records: Mapping[str, RepairRecord]) -> None:
    assignments = ", ".join(f"{column} = %s" for column in ORDER_BOOK_COLUMNS)
    null_guard = " and ".join(f"{column} is null" for column in ORDER_BOOK_COLUMNS)
    parameters = [
        (*record.database_values(), TARGET_INGESTION_ID, record.symbol)
        for record in records.values()
    ]
    cursor.executemany(
        f"""update realtime.call_auction_market_snapshot
            set {assignments}
            where ingestion_id = %s and symbol = %s and {null_guard}""",
        parameters,
    )
    if cursor.rowcount != EXPECTED_ROW_COUNT:
        raise RepairError(f"updated {cursor.rowcount} rows, expected {EXPECTED_ROW_COUNT}")


def _symbol(payload: Mapping[str, object], line_number: int) -> str:
    market = payload.get("market")
    code = payload.get("code")
    if (
        market not in (0, 1, "0", "1")
        or not isinstance(code, str)
        or not fullmatch(r"[0-9]{6}", code)
    ):
        raise RepairError(f"Raw row {line_number} has an invalid market/code")
    return f"{'SSE' if str(market) == '1' else 'SZSE'}:{code}"


def _levels(
    payload: Mapping[str, object], side: str, line_number: int
) -> tuple[tuple[Decimal | None, ...], tuple[int | None, ...]]:
    prices: list[Decimal | None] = []
    volumes: list[int | None] = []
    for level in LEVELS:
        price = _price(payload.get(f"{side}{level}"), line_number)
        volume: int | None = _volume(payload.get(f"{side}_vol{level}"), line_number)
        if price is None and volume == 0:
            volume = None
        prices.append(price)
        volumes.append(volume)
    return tuple(prices), tuple(volumes)


def _price(value: object, line_number: int) -> Decimal | None:
    try:
        price = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise RepairError(f"Raw row {line_number} has an invalid price") from error
    if not price.is_finite() or price < 0:
        raise RepairError(f"Raw row {line_number} has an invalid price")
    return price if price > 0 else None


def _volume(value: object, line_number: int) -> int:
    if isinstance(value, bool):
        raise RepairError(f"Raw row {line_number} has an invalid volume")
    try:
        lots = int(str(value))
    except ValueError as error:
        raise RepairError(f"Raw row {line_number} has an invalid volume") from error
    if lots < 0 or str(value) != str(lots):
        raise RepairError(f"Raw row {line_number} has an invalid volume")
    return lots * 100


def main() -> None:
    parser = ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true", help="commit the fixed ADR-0041 repair")
    args = parser.parse_args()
    try:
        result = run_repair(apply=args.apply)
    except Exception as error:
        print(
            dumps(
                {
                    "operation": "repair_20260818_call_auction_market_snapshot",
                    "status": "failed",
                    "error_type": type(error).__name__,
                    "error": str(error),
                },
                ensure_ascii=False,
                sort_keys=True,
            )
        )
        raise SystemExit(1) from None
    print(dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
