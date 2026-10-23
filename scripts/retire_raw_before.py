"""One-off administrator Raw retirement for ADR-0069; default is read-only preview.

Requires a separately verified, immutable off-host archive index. Raw manifests
stay unchanged. Use PG* for an administrator connection, never Worker grants.
"""

import argparse
import gzip
import hashlib
import json
import os
import re
from pathlib import Path
from uuid import UUID

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.raw_retirement import OfflineRawProof, remove_retired_raw, verify_online_raw
from market_data_center.raw_store import LocalRawStore

CUTOFF = "2026-09-23"
SCHEMAS = [
    "core",
    "capital",
    "classification",
    "derived",
    "metrics",
    "realtime",
    "stock_pool",
    "today_limit_up",
    "today_limit_down",
    "billboard",
    "regulation",
    "convertible_bond",
]
EMBEDDED = [
    ("derived.calculation_run", "input_watermark"),
    ("regulation.monitor_context", "source"),
    ("regulation.calculation_run", "input_event_keys"),
]
OLD = """m.created_at < timestamptz '2026-09-23 00:00:00+08'
    and r.finished_at < timestamptz '2026-09-23 00:00:00+08'
    and r.status in ('succeeded','partial','failed')
    and m.storage_backend='local' and m.file_format='jsonl'"""
FIELDS = (
    "m.raw_id,m.ingestion_id,m.object_path,m.file_format,m.content_sha256,"
    "m.byte_size,m.row_count,m.schema_version,m.storage_backend"
)


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()


def identity(c):
    return list(
        c.execute(
            "select system_identifier::text,(select oid::bigint from pg_database "
            "where datname=current_database()) from pg_control_system()"
        ).fetchone()
    )


def index_proofs(path, receipt):
    data = path.read_bytes()
    if hashlib.sha256(data).hexdigest() != receipt["index_sha256"]:
        raise RuntimeError("offline_index_hash_mismatch")
    logical = gzip.decompress(data)
    if hashlib.sha256(logical).hexdigest() != receipt["logical_index_sha256"]:
        raise RuntimeError("offline_index_logical_hash_mismatch")
    rows = [json.loads(line) for line in logical.splitlines()]
    if (
        len(rows) != receipt["files"]
        or sum(r["bytes"] for r in rows) != receipt["source_bytes"]
        or len({r["path"] for r in rows}) != len(rows)
    ):
        raise RuntimeError("offline_index_counts_mismatch")
    if not re.fullmatch(r"[0-9a-f]{64}", receipt["archive_sha256"]):
        raise RuntimeError("offline_archive_identity_missing")
    return {r["path"]: r for r in rows}


def reference_catalog(c):
    return c.execute(
        """select n.nspname,c.relname,a.attname from pg_class c
        join pg_namespace n on n.oid=c.relnamespace join pg_attribute a on a.attrelid=c.oid
        where c.relkind in ('r','p') and not c.relispartition and a.attnum>0
        and not a.attisdropped and a.atttypid='uuid'::regtype
        and (a.attname like '%%ingestion_id' or a.attname like '%%raw_id')
        and (n.nspname=any(%s) or n.nspname='audit' and c.relname='quantity_unit_repair_item')
        order by 1,2,3""",
        (SCHEMAS,),
    ).fetchall()


def protected_ids(c, refs, candidates=None):
    protected = set()
    for schema, table, col in refs:
        query = sql.SQL("select distinct {} from {} where {} is not null").format(
            sql.Identifier(col), sql.Identifier(schema, table), sql.Identifier(col)
        )
        if candidates is not None:
            query += sql.SQL(" and {}=any(%s::uuid[])").format(sql.Identifier(col))
        protected.update(
            str(r[0]) for r in c.execute(query, (candidates,) if candidates is not None else None)
        )
    for table, col in EMBEDDED:
        for (value,) in c.execute(
            sql.SQL("select {}::text from {}").format(
                sql.Identifier(col), sql.Identifier(*table.split("."))
            )
        ):
            protected.update(
                re.findall(
                    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", value or ""
                )
            )
    # Conservatively keep every original Raw that has ever been replayed.
    protected.update(
        str(r[0])
        for r in c.execute(
            "select distinct replayed_from_raw_id from ingestion.ingestion_run "
            "where replayed_from_raw_id is not null"
        )
    )
    return protected


def proof_matches(row, index):
    item = index.get(row["object_path"])
    return item is not None and (item["sha256"], item["bytes"], item["rows"]) == (
        row["content_sha256"],
        row["byte_size"],
        row["row_count"],
    )


def storage_identity(store):
    if not store.root.is_dir():
        raise RuntimeError("raw_root_unavailable")
    stat = store.root.stat()
    return [hashlib.sha256(str(store.root).encode()).hexdigest(), stat.st_dev, stat.st_ino]


def manifest_proof(row, plan):
    manifest = RawManifest(
        UUID(str(row["raw_id"])),
        UUID(str(row["ingestion_id"])),
        row["object_path"],
        RawFileFormat(row["file_format"]),
        row["content_sha256"],
        row["byte_size"],
        row["row_count"],
        row["schema_version"],
        storage_backend=row["storage_backend"],
    )
    proof = OfflineRawProof(
        "raw-sha256:" + plan["archive_sha256"],
        plan["index_sha256"],
        manifest.content_sha256,
        manifest.byte_size,
        manifest.row_count,
    )
    return manifest, proof


def preview(c, index, receipt, store):
    root_identity = storage_identity(store)
    with c.transaction():
        c.execute("set transaction isolation level repeatable read, read only")
        refs = reference_catalog(c)
        protected = protected_ids(c, refs)
        with c.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "select "
                + FIELDS
                + " from ingestion.raw_manifest m join ingestion.ingestion_run r "
                "using(ingestion_id) where " + OLD + " order by m.raw_id"
            )
            old = cursor.fetchall()
        unreferenced = [
            r
            for r in old
            if str(r["raw_id"]) not in protected and str(r["ingestion_id"]) not in protected
        ]
        matched = [r for r in unreferenced if proof_matches(r, index)]
        eligible = [
            r
            for r in matched
            if any(
                store.resolve_path(r["object_path"] + suffix).is_file() for suffix in ("", ".gz")
            )
        ]
        return {
            "cutoff": CUTOFF,
            "database_identity": identity(c),
            "index_sha256": receipt["index_sha256"],
            "archive_sha256": receipt["archive_sha256"],
            "references": refs,
            "storage_identity": root_identity,
            "old": len(old),
            "protected": len(old) - len(unreferenced),
            "unverified_archive": len(unreferenced) - len(matched),
            "missing_online": len(matched) - len(eligible),
            "eligible": eligible,
        }


def register_batch(c, rows, plan):
    ids = [str(r["raw_id"]) for r in rows]
    refs = reference_catalog(c)
    if encoded(refs) != encoded(plan["references"]):
        raise RuntimeError("reference_catalog_changed")
    with c.transaction():
        # Acquire before reference-table locks. Replay INSERT uses this same lock
        # in its trigger; do not SHARE-lock ingestion_run (would invert lock order).
        for raw_id in sorted(ids):
            c.execute(
                "select pg_advisory_xact_lock(hashtextextended(%s,0))",
                ("raw-retirement:" + raw_id,),
            )
        # Bounded registration only: never hold table locks while hashing/unlinking files.
        tables = sorted(
            {(s, t) for s, t, _ in refs}
            | {tuple(t.split(".")) for t, _ in EMBEDDED}
            | {("ingestion", "raw_manifest")}
        )
        c.execute(
            sql.SQL("lock table {} in share mode").format(
                sql.SQL(",").join(sql.Identifier(*t) for t in tables)
            )
        )
        candidates = list({str(r[k]) for r in rows for k in ("raw_id", "ingestion_id")})
        protected = protected_ids(c, refs, candidates)
        with c.cursor(row_factory=dict_row) as cursor:
            cursor.execute(
                "select "
                + FIELDS
                + " from ingestion.raw_manifest m join ingestion.ingestion_run r "
                "using(ingestion_id) where " + OLD + " and m.raw_id=any(%s::uuid[])",
                (ids,),
            )
            actual = {str(r["raw_id"]): r for r in cursor.fetchall()}
        if any(encoded(actual.get(str(r["raw_id"]))) != encoded(r) for r in rows):
            raise RuntimeError("raw_source_identity_changed")
        allowed = [
            r
            for r in rows
            if str(r["raw_id"]) not in protected and str(r["ingestion_id"]) not in protected
        ]
        for row in allowed:
            values = (
                str(row["raw_id"]),
                "raw-sha256:" + plan["archive_sha256"],
                plan["index_sha256"],
                row["content_sha256"],
                row["byte_size"],
                row["row_count"],
                CUTOFF,
            )
            c.execute(
                """insert into ingestion.raw_retirement(raw_id,archive_ref,archive_manifest_sha256,
                content_sha256,content_bytes,row_count,retirement_cutoff)
                values(%s,%s,%s,%s,%s,%s,%s::date) on conflict do nothing""",
                values,
            )
            stored = c.execute(
                """select raw_id::text,archive_ref,archive_manifest_sha256,content_sha256,
                content_bytes,row_count,retirement_cutoff::text,restored_at
                from ingestion.raw_retirement where raw_id=%s for update""",
                (row["raw_id"],),
            ).fetchone()
            if stored != (*values, None):
                raise RuntimeError("retirement_proof_changed_or_restored")
        return allowed


def apply(c, store, plan, index, receipt, journal):
    if (
        plan["cutoff"] != CUTOFF
        or identity(c) != plan["database_identity"]
        or any(plan[k] != receipt[k] for k in ("index_sha256", "archive_sha256"))
    ):
        raise RuntimeError("plan_identity_changed")
    if storage_identity(store) != plan["storage_identity"]:
        raise RuntimeError("raw_storage_identity_changed")
    if any(not proof_matches(r, index) for r in plan["eligible"]):
        raise RuntimeError("plan_offline_proof_changed")
    acquired = c.execute(
        "select pg_try_advisory_lock(hashtextextended('maintenance:data_cleanup',0))"
    ).fetchone()[0]
    if not acquired:
        raise RuntimeError("maintenance_already_running")
    completed = removed = protected = 0
    try:
        for start in range(0, len(plan["eligible"]), 200):
            batch = plan["eligible"][start : start + 200]
            for row in batch:
                manifest, proof = manifest_proof(row, plan)
                if not verify_online_raw(store, manifest, proof):
                    prior = c.execute(
                        "select 1 from ingestion.raw_retirement where raw_id=%s "
                        "and archive_ref=%s and archive_manifest_sha256=%s "
                        "and content_sha256=%s and content_bytes=%s and row_count=%s "
                        "and retirement_cutoff=%s::date and restored_at is null",
                        (
                            manifest.raw_id,
                            proof.archive_ref,
                            proof.index_sha256,
                            proof.content_sha256,
                            proof.content_bytes,
                            proof.row_count,
                            CUTOFF,
                        ),
                    ).fetchone()
                    if prior is None:
                        raise RuntimeError("first_retirement_online_raw_missing")
            allowed = register_batch(c, batch, plan)
            protected += len(batch) - len(allowed)
            confirmed = []
            for r in allowed:
                manifest, proof = manifest_proof(r, plan)
                removed += remove_retired_raw(
                    store, manifest, proof, register=lambda: None, confirm=lambda: None
                )
                confirmed.append(str(manifest.raw_id))
            with c.transaction():
                c.execute(
                    "update ingestion.raw_retirement set "
                    "online_removed_at=coalesce(online_removed_at,clock_timestamp()) "
                    "where raw_id=any(%s::uuid[]) and restored_at is null",
                    (confirmed,),
                )
            completed += len(confirmed)
            result = {
                "completed": completed,
                "physical_bytes_removed_this_run": removed,
                "newly_protected": protected,
                "processed": start + len(batch),
            }
            with journal.open("ab") as stream:
                stream.write(encoded(result) + b"\n")
                stream.flush()
                os.fsync(stream.fileno())
            print(json.dumps(result), flush=True)
        return {
            "completed": completed,
            "physical_bytes_removed_this_run": removed,
            "newly_protected": protected,
        }
    finally:
        c.execute("select pg_advisory_unlock(hashtextextended('maintenance:data_cleanup',0))")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", nargs="?", choices=("preview", "apply"), default="preview")
    parser.add_argument("--index", type=Path, required=True)
    parser.add_argument("--verification", type=Path, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--expected-plan-sha256")
    parser.add_argument("--raw-root", type=Path, required=True)
    args = parser.parse_args()
    receipt = json.loads(args.verification.read_bytes())
    index = index_proofs(args.index, receipt)
    with psycopg.connect(autocommit=True) as c:
        c.execute("set statement_timeout='25s'")
        c.execute("set lock_timeout='1s'")
        if args.mode == "preview":
            plan = preview(c, index, receipt, LocalRawStore(args.raw_root))
            with args.plan.open("xb") as output:
                output.write(encoded(plan))
                output.flush()
                os.fsync(output.fileno())
            print(
                json.dumps(
                    {
                        k: len(v) if k == "eligible" else v
                        for k, v in plan.items()
                        if k not in ("references", "database_identity")
                    }
                )
            )
        else:
            data = args.plan.read_bytes()
            if (
                not args.raw_root
                or not args.expected_plan_sha256
                or hashlib.sha256(data).hexdigest() != args.expected_plan_sha256
            ):
                parser.error("exact verified plan digest and Raw root required")
            print(
                json.dumps(
                    apply(
                        c,
                        LocalRawStore(args.raw_root),
                        json.loads(data),
                        index,
                        receipt,
                        args.plan.with_suffix(".receipts.jsonl"),
                    )
                )
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
