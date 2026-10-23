"""One-off owner-authorized 2026-09-23 retention, never a scheduled Worker job.

Default: read-only counts. Archive complete rows, copy off-host and verify there,
then explicitly apply using that manifest digest. No DDL, cascading deletion,
Raw removal, or changed persistent retention policy. Restore in reverse table order.
Connection comes from standard PG* environment; never prints database exceptions.
"""

import argparse
import gzip
import hashlib
import json
import os
import time
from contextlib import contextmanager
from pathlib import Path

import psycopg
from psycopg import sql

CUTOFF = "2026-09-23"
BATCH = 1000
# Child facts precede parent snapshots. Lineage and live reference data are retained.
TARGETS = dict.fromkeys(
    (
        "realtime.call_auction_market_series_snapshot_history",
        "realtime.call_auction_market_series_snapshot",
        "realtime.call_auction_indicative_detail",
        "realtime.call_auction_indicative_snapshot",
        "realtime.call_auction_market_snapshot",
        "realtime.call_auction_snapshot",
        "realtime.eod_quote_snapshot",
        "core.stock_technical_factor_snapshot",
        "core.stock_daily_indicator",
        "core.board_moneyflow",
        "core.board_index_constituent_snapshot",
        "billboard.seat_trade",
        "billboard.dragon_tiger_event",
        "regulation.warning",
        "regulation.status",
        "derived.adjusted_daily_bar",
        "derived.daily_metric",
        "derived.market_capitalization",
        "derived.price_limit_event",
        "derived.daily_price_limit",
        "metrics.classification_daily_metric",
        "convertible_bond.daily_bar",
        "today_limit_up.source_observation",
        "today_limit_down.source_observation",
    ),
    "t.trade_date < DATE '2026-09-23'",
)
TARGETS.update(
    {
        "audit.quality_result": "t.created_at < TIMESTAMPTZ '2026-09-23 00:00:00+08'",
        "billboard.trading_seat_profile_daily": "t.as_of_date < DATE '2026-09-23'",
        "regulation.rule_result": "exists(select 1 from regulation.status s where "
        "s.calculation_id=t.calculation_id and s.symbol=t.symbol "
        "and s.trade_date < DATE '2026-09-23')",
    }
)
# Insert rule children before status (and after warnings).
_rules = TARGETS.pop("regulation.rule_result")
TARGETS = {
    key: value
    for table, predicate in TARGETS.items()
    for key, value in (
        [("regulation.rule_result", _rules), (table, predicate)]
        if table == "regulation.status"
        else [(table, predicate)]
    )
}
for _schema in ("today_limit_up", "today_limit_down"):
    for _child in ("member", "calculation_quality"):
        TARGETS[f"{_schema}.{_child}"] = (
            f"exists(select 1 from {_schema}.snapshot s where s.snapshot_id=t.snapshot_id "
            "and s.trade_date < DATE '2026-09-23')"
        )
    TARGETS[f"{_schema}.snapshot"] = "t.trade_date < DATE '2026-09-23'"
TARGETS["core.daily_bar"] = """t.trade_date < (select min(trade_date) from (
    select trade_date from core.trading_calendar where market='CN_A_SHARE'
    and is_trading_day and trade_date < DATE '2026-09-23'
    order by trade_date desc limit 120) protected)
    and (select count(*) from core.trading_calendar where market='CN_A_SHARE'
         and is_trading_day and trade_date < DATE '2026-09-23') >= 120"""
TARGETS["core.board_index_daily_bar"] = TARGETS["core.daily_bar"]
_old_session = """exists(select 1 from realtime.auction_collection_session s
    where s.session_id=t.session_id and s.effective_trade_date < DATE '2026-09-23'
    and s.finished_at is not null)"""
for _table in (
    "derived.auction_quote_metric",
    "realtime.five_level_quote_snapshot",
    "realtime.auction_collection_round",
):
    TARGETS[_table] = _old_session
TARGETS["realtime.auction_collection_session"] = (
    "t.effective_trade_date < DATE '2026-09-23' and t.finished_at is not null"
)
TARGETS["realtime.call_auction_market_series_round"] = """exists(
    select 1 from realtime.call_auction_market_series_session s
    where s.session_id=t.session_id and s.trade_date < DATE '2026-09-23'
    and s.finished_at is not null)"""
TARGETS["realtime.call_auction_market_series_session"] = (
    "t.trade_date < DATE '2026-09-23' and t.finished_at is not null"
)
POOL_ELIGIBLE = """s.effective_trade_date < DATE '2026-09-23'
    and not exists(select 1 from realtime.auction_collection_session a
        where a.pool_snapshot_id=s.snapshot_id)
    and not exists(select 1 from realtime.five_level_quote_snapshot q
        where q.pool_snapshot_id=s.snapshot_id)"""
TARGETS["stock_pool.calculation_quality"] = f"""exists(
    select 1 from stock_pool.snapshot s where s.calculation_id=t.calculation_id
    and ({POOL_ELIGIBLE})) and not exists(
    select 1 from stock_pool.snapshot s where s.calculation_id=t.calculation_id
    and not ({POOL_ELIGIBLE}))"""
TARGETS["stock_pool.member"] = f"""exists(select 1 from stock_pool.snapshot s
    where s.snapshot_id=t.snapshot_id and ({POOL_ELIGIBLE}))"""
TARGETS["stock_pool.snapshot"] = POOL_ELIGIBLE.replace("s.", "t.")
# A retained effective-date snapshot reads facts on its earlier basis date.
# Protect inputs of every extant snapshot; clean snapshots first, then freshly
# archive their now-unreferenced inputs. Do not rely on date-only child ordering.
for _table in (
    "derived.daily_price_limit",
    "derived.price_limit_event",
    "core.stock_daily_indicator",
):
    _calculation = "" if _table.startswith("core.") else "and s.calculation_id=t.calculation_id"
    TARGETS[_table] += f""" and not exists(select 1 from stock_pool.snapshot s
        join stock_pool.member m on m.snapshot_id=s.snapshot_id
        where s.basis_trade_date=t.trade_date and m.symbol=t.symbol {_calculation})"""
TARGETS["derived.close_price_new_high_120d_member"] = """exists(
    select 1 from derived.close_price_new_high_120d_snapshot s
    where s.snapshot_id=t.snapshot_id and s.trade_date < DATE '2026-09-23')"""
TARGETS["derived.close_price_new_high_120d_snapshot"] = "t.trade_date < DATE '2026-09-23'"
# Keep two prior shareholder periods for the existing latest/previous comparison;
# all revisions within those periods remain available for as-of reads.
TARGETS["core.shareholder_count"] = """t.announcement_date < DATE '2026-09-23'
    and t.statistics_date < (select min(d.statistics_date) from (
        select distinct s.statistics_date from core.shareholder_count s
        where s.symbol=t.symbol and s.announcement_date < DATE '2026-09-23'
        order by s.statistics_date desc limit 2) d)"""
TARGETS["core.top10_float_holder"] = """t.announcement_date < DATE '2026-09-23'
    and t.end_date < (select max(s.end_date) from core.top10_float_holder s
        where s.symbol=t.symbol and s.announcement_date < DATE '2026-09-23')"""


def ident(table):
    return sql.Identifier(*table.split("."))


def digest_bytes(data):
    return hashlib.sha256(data).hexdigest()


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def setup(connection):
    connection.execute("set timezone='UTC'")
    connection.execute("set statement_timeout='25s'")
    connection.execute("set lock_timeout='1s'")
    connection.execute("set application_name='mdc-authorized-month-retention'")


def metadata(connection, table):
    columns = connection.execute(
        """select attname,atttypid,atttypmod,attnotnull,attidentity,attgenerated
        from pg_attribute where attrelid=%s::regclass and attnum>0
        and not attisdropped order by attnum""",
        (table,),
    ).fetchall()
    keys = connection.execute(
        """select a.attname from pg_constraint k
        cross join lateral unnest(k.conkey) with ordinality x(num,pos)
        join pg_attribute a on a.attrelid=k.conrelid and a.attnum=x.num
        where k.conrelid=%s::regclass and k.contype='p' order by x.pos""",
        (table,),
    ).fetchall()
    if not keys:
        raise RuntimeError("missing_primary_key")
    return {"schema_hash": digest_bytes(encoded(columns)), "keys": [x[0] for x in keys]}


def database_identity(connection):
    return list(
        connection.execute(
            "select system_identifier::text, "
            "(select oid from pg_database where datname=current_database()) "
            "from pg_control_system()"
        ).fetchone()
    )


def archive(connection, table, directory):
    if table not in TARGETS:
        raise RuntimeError("scope_not_allowed")
    setup(connection)
    directory.mkdir(mode=0o700, parents=False, exist_ok=False)
    plan = {"table": table, "predicate": TARGETS[table], "cutoff": CUTOFF}
    count = 0
    logical = hashlib.sha256()
    with connection.transaction():
        connection.execute("set transaction isolation level repeatable read, read only")
        plan.update(metadata(connection, table))
        plan["database"] = connection.execute("select current_database()").fetchone()[0]
        plan["database_identity"] = database_identity(connection)
        query = sql.SQL("select row_to_json(t)::text from {} t where {} order by {}").format(
            ident(table),
            sql.SQL(TARGETS[table]),
            sql.SQL(",").join(sql.Identifier("t", key) for key in plan["keys"]),
        )
        with connection.cursor(name="retention_archive") as cursor:
            cursor.itersize = BATCH
            cursor.execute(query)
            with gzip.open(directory / "rows.jsonl.gz", "xb", compresslevel=6) as output:
                for (row,) in cursor:
                    value = (row + "\n").encode()
                    output.write(value)
                    logical.update(value)
                    count += 1
    file_path = directory / "rows.jsonl.gz"
    with file_path.open("r+b") as stream:
        os.fsync(stream.fileno())
        compressed_hash = hashlib.file_digest(stream, "sha256").hexdigest()
    plan.update(rows=count, logical_hash=logical.hexdigest(), compressed_hash=compressed_hash)
    with (directory / "manifest.json").open("xb") as output:
        output.write(encoded(plan))
        output.flush()
        os.fsync(output.fileno())
    return plan


@contextmanager
def verified_archive(directory, expected_hash=None):
    if any(parent.is_symlink() for parent in (directory, *directory.parents)):
        raise RuntimeError("unsafe_archive_path")
    paths = [directory / "manifest.json", directory / "rows.jsonl.gz"]
    if any(p.is_symlink() or not p.is_file() for p in paths):
        raise RuntimeError("unsafe_archive_path")
    data = paths[0].read_bytes()
    digest = digest_bytes(data)
    if expected_hash is not None and digest != expected_hash:
        raise RuntimeError("plan_hash_mismatch")
    plan = json.loads(data)
    if plan["cutoff"] != CUTOFF or TARGETS.get(plan["table"]) != plan["predicate"]:
        raise RuntimeError("scope_changed")
    with paths[1].open("rb") as compressed:
        if hashlib.file_digest(compressed, "sha256").hexdigest() != plan["compressed_hash"]:
            raise RuntimeError("archive_hash_mismatch")
        logical = hashlib.sha256()
        count = 0
        checks = []
        compressed.seek(0)
        with gzip.GzipFile(fileobj=compressed, mode="rb") as stream:
            while chunk := list(line for _, line in zip(range(BATCH), stream, strict=False)):
                if any(not line.endswith(b"\n") for line in chunk):
                    raise RuntimeError("truncated_archive")
                value = b"".join(chunk)
                logical.update(value)
                checks.append(digest_bytes(value))
                count += len(chunk)
        if count != plan["rows"] or logical.hexdigest() != plan["logical_hash"]:
            raise RuntimeError("archive_logical_mismatch")
        compressed.seek(0)
        with gzip.GzipFile(fileobj=compressed, mode="rb") as stream:
            yield plan, digest, stream, checks


def verify_archive(directory):
    with verified_archive(directory) as (_, digest, _, _):
        return digest


def batches(stream, checks):
    for expected in checks:
        chunk = list(line for _, line in zip(range(BATCH), stream, strict=False))
        if digest_bytes(b"".join(chunk)) != expected:
            raise RuntimeError("archive_changed_during_apply")
        yield [line.decode("utf-8").rstrip("\n") for line in chunk]
    if stream.read(1):
        raise RuntimeError("archive_grew_during_apply")


def check_children(connection, table, payload):
    children = connection.execute(
        """select n.nspname,c.relname,
        array(select attname from unnest(k.conkey) with ordinality x(num,pos)
              join pg_attribute a on a.attrelid=k.conrelid and a.attnum=x.num order by pos),
        array(select attname from unnest(k.confkey) with ordinality x(num,pos)
              join pg_attribute a on a.attrelid=k.confrelid and a.attnum=x.num order by pos)
        from pg_constraint k join pg_class c on c.oid=k.conrelid
        join pg_namespace n on n.oid=c.relnamespace
        where k.confrelid=%s::regclass and k.contype='f' and k.conparentid=0""",
        (table,),
    ).fetchall()
    for schema, name, foreign, primary in children:
        matches = sql.SQL(" and ").join(
            sql.SQL("{}={}").format(sql.Identifier("c", f), sql.Identifier("p", p))
            for f, p in zip(foreign, primary, strict=True)
        )
        query = sql.SQL("""select exists(select 1 from {} c
            join jsonb_populate_recordset(null::{},%s::jsonb) p on {})""").format(
            sql.Identifier(schema, name),
            ident(table),
            matches,
        )
        if connection.execute(query, (payload,)).fetchone()[0]:
            raise RuntimeError("referenced_rows_remain")


def key_payload(rows, keys):
    # Quantities/amounts are not parsed through float, even when only keys are needed.
    documents = [json.loads(row, parse_float=str) for row in rows]
    projected = [{key: row[key] for key in keys} for row in documents]
    if len({encoded(row) for row in projected}) != len(rows):
        raise RuntimeError("duplicate_archive_keys")
    return json.dumps(projected, separators=(",", ":"))


def apply(connection, directory, expected_hash):
    acquired = connection.execute(
        "select pg_try_advisory_lock(hashtextextended('maintenance:data_cleanup',0))"
    ).fetchone()[0]
    if not acquired:
        raise RuntimeError("maintenance_already_running")
    try:
        with verified_archive(directory, expected_hash) as (plan, _, stream, checks):
            return _apply_verified(connection, directory, expected_hash, plan, stream, checks)
    finally:
        connection.execute(
            "select pg_advisory_unlock(hashtextextended('maintenance:data_cleanup',0))"
        )


def _apply_verified(connection, directory, expected_hash, plan, stream, checks):
    table = plan["table"]
    setup(connection)
    if database_identity(connection) != plan["database_identity"]:
        raise RuntimeError("database_changed")
    if metadata(connection, table) != {k: plan[k] for k in ("schema_hash", "keys")}:
        raise RuntimeError("schema_changed")
    deleted = absent = 0
    for batch_number, rows in enumerate(batches(stream, checks), 1):
        payload = key_payload(rows, plan["keys"])
        expected = set(rows)
        if len(expected) != len(rows):
            raise RuntimeError("duplicate_archive_rows")
        matches = sql.SQL(" and ").join(
            sql.SQL("{}={}").format(sql.Identifier("t", key), sql.Identifier("p", key))
            for key in plan["keys"]
        )
        with connection.transaction():
            query = sql.SQL("""select t.tableoid::oid,t.ctid::text,row_to_json(t)::text
                from {} t join jsonb_populate_recordset(null::{},%s::jsonb) p on {}
                where {} for update of t""").format(
                ident(table),
                ident(table),
                matches,
                sql.SQL(plan["predicate"]),
            )
            current = connection.execute(query, (payload,)).fetchall()
            if any(row[2] not in expected for row in current):
                raise RuntimeError("row_changed")
            # Detect an archived key moved outside the authorized date predicate.
            total = connection.execute(
                sql.SQL(
                    "select count(*) from {} t join "
                    "jsonb_populate_recordset(null::{},%s::jsonb) p on {}"
                ).format(ident(table), ident(table), matches),
                (payload,),
            ).fetchone()[0]
            if total != len(current):
                raise RuntimeError("row_changed_outside_scope")
            if current:
                check_children(connection, table, "[" + ",".join(rows) + "]")
                query = sql.SQL(
                    "delete from {} t using jsonb_populate_recordset(null::{},%s::jsonb) p "
                    "where {} and ({}) "
                    "returning row_to_json(t)::text"
                ).format(ident(table), ident(table), matches, sql.SQL(plan["predicate"]))
                returned = connection.execute(query, (payload,)).fetchall()
                if {row[0] for row in returned} != {row[2] for row in current}:
                    raise RuntimeError("delete_projection_mismatch")
        deleted += len(current)
        absent += len(rows) - len(current)
        with (directory / "apply-receipts.jsonl").open("a", encoding="utf-8") as receipt:
            receipt.write(
                json.dumps(
                    {
                        "batch": batch_number,
                        "deleted": deleted,
                        "already_absent": absent,
                        "plan_hash": expected_hash,
                    }
                )
                + "\n"
            )
            receipt.flush()
            os.fsync(receipt.fileno())
        if batch_number % 50 == 0:
            print(
                json.dumps({"table": table, "deleted": deleted, "already_absent": absent}),
                flush=True,
            )
        time.sleep(0.02)
    return {"deleted": deleted, "already_absent": absent}


def restore(connection, directory, expected_hash):
    with verified_archive(directory, expected_hash) as (plan, _, stream, checks):
        _restore_verified(connection, plan, stream, checks)


def _restore_verified(connection, plan, stream, checks):
    table = plan["table"]
    setup(connection)
    if database_identity(connection) != plan["database_identity"]:
        raise RuntimeError("database_changed")
    if metadata(connection, table) != {k: plan[k] for k in ("schema_hash", "keys")}:
        raise RuntimeError("schema_changed")
    matches = sql.SQL(" and ").join(
        sql.SQL("{}={}").format(sql.Identifier("t", key), sql.Identifier("p", key))
        for key in plan["keys"]
    )
    for rows in batches(stream, checks):
        payload = "[" + ",".join(rows) + "]"
        with connection.transaction():
            connection.execute(
                sql.SQL(
                    "insert into {} select * from jsonb_populate_recordset(null::{},%s::jsonb) "
                    "on conflict do nothing"
                ).format(ident(table), ident(table)),
                (payload,),
            )
            actual = connection.execute(
                sql.SQL(
                    "select row_to_json(t)::text from {} t join "
                    "jsonb_populate_recordset(null::{},%s::jsonb) p on {} for update of t"
                ).format(ident(table), ident(table), matches),
                (payload,),
            ).fetchall()
            if {row[0] for row in actual} != set(rows):
                raise RuntimeError("restore_existing_row_changed")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "mode",
        nargs="?",
        choices=("preview", "archive", "verify", "apply", "restore"),
        default="preview",
    )
    parser.add_argument("--table", choices=tuple(TARGETS))
    parser.add_argument("--directory", type=Path)
    parser.add_argument("--expected-hash")
    args = parser.parse_args()
    if args.mode != "preview" and not args.directory:
        parser.error("archive directory required")
    if args.mode in ("apply", "restore") and not args.expected_hash:
        parser.error("independently verified manifest digest required")
    if args.mode == "archive" and not args.table:
        parser.error("table required")
    if args.mode == "verify":
        print(json.dumps({"manifest_hash": verify_archive(args.directory)}))
        return
    with psycopg.connect(autocommit=True) as connection:
        setup(connection)
        if args.mode == "preview":
            with connection.transaction():
                connection.execute("set transaction read only")
                for table, predicate in TARGETS.items():
                    if args.table and args.table != table:
                        continue
                    count = connection.execute(
                        sql.SQL("select count(*) from {} t where {}").format(
                            ident(table), sql.SQL(predicate)
                        )
                    ).fetchone()[0]
                    print(json.dumps({"table": table, "eligible_rows": count}), flush=True)
        elif args.mode == "archive":
            result = archive(connection, args.table, args.directory)
            print(
                json.dumps(
                    {
                        "table": result["table"],
                        "rows": result["rows"],
                        "manifest_hash": verify_archive(args.directory),
                    }
                ),
                flush=True,
            )
        else:
            print(
                json.dumps(globals()[args.mode](connection, args.directory, args.expected_hash)),
                flush=True,
            )


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print(
            json.dumps(
                {
                    "failed": type(error).__name__,
                    "reason": str(error)
                    if type(error) is RuntimeError
                    else "database_or_file_error",
                }
            ),
            flush=True,
        )
        raise SystemExit(1) from None
