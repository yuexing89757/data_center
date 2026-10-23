"""Atomic parent/child retention transactions; archive publication precedes deletion."""

import json
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import asdict
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import Connection, Engine, text

from market_data_center.history_archive import (
    MAX_BYTES,
    MAX_ROWS,
    HistoryArchive,
    read_archive,
    write_archive,
)
from market_data_center.history_retention import POLICIES, RetentionPolicy, table_predicates
from market_data_center.quality_cleanup_service import CleanupInterrupted


class HistoryGroupFailure(RuntimeError):
    def __init__(self, cursor: str, code: str):
        super().__init__(code)
        self.cursor = cursor


def payload(rows: tuple[str, ...]) -> str:
    return "[" + ",".join(rows) + "]"


class HistoryRetentionPersistence:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def transaction(self, *, readonly: bool = False) -> Iterator[Connection]:
        with self.engine.begin() as c:
            if readonly:
                c.execute(text("set transaction read only"))
            c.execute(text("set local lock_timeout='1s'"))
            c.execute(text("set local statement_timeout='10s'"))
            c.execute(text("set local timezone='UTC'"))
            yield c

    def metadata(self, c: Connection, table: str) -> dict[str, Any]:
        columns = c.execute(
            text("""select attname,atttypid,atttypmod,attnotnull,attidentity,attgenerated
            from pg_attribute where attrelid=cast(:table as regclass) and attnum>0
            and not attisdropped order by attnum"""),
            {"table": table},
        ).all()
        keys = (
            c.execute(
                text("""select a.attname from pg_index i
            cross join lateral unnest(i.indkey) with ordinality k(num,pos)
            join pg_attribute a on a.attrelid=i.indrelid and a.attnum=k.num
            where i.indrelid=cast(:table as regclass) and i.indisprimary order by k.pos"""),
                {"table": table},
            )
            .scalars()
            .all()
        )
        if not keys:
            raise ValueError("history_primary_key_required")
        return {"columns": [list(r) for r in columns], "keys": list(keys)}

    def restore(self, archive_id: UUID, root: Path, *, execute: bool = False) -> dict[str, Any]:
        """Administrator recovery; caller holds maintenance lock. Default read-only."""
        with self.transaction(readonly=not execute) as c:
            if execute and c.scalar(text("select current_user")) == "market_data_worker":
                raise ValueError("history_restore_requires_administrator")
            receipt = (
                c.execute(
                    text("select * from audit.history_archive where archive_id=:id"),
                    {"id": archive_id},
                )
                .mappings()
                .one()
            )
            archive = HistoryArchive(
                archive_id,
                receipt["object_path"],
                receipt["compressed_sha256"],
                receipt["content_sha256"],
                receipt["compressed_bytes"],
                receipt["row_count"],
            )
            header, rows = read_archive(root, archive)
            if header.get("database_identity") != c.scalar(
                text("select audit.history_database_identity()")
            ):
                raise ValueError("history_restore_database_mismatch")
            if (
                header.get("version") != "history_archive.v1"
                or header.get("policy") != receipt["policy_table"]
            ):
                raise ValueError("history_restore_manifest_mismatch")
            for table in rows:
                if table not in table_predicates() or header["metadata"].get(
                    table
                ) != self.metadata(c, table):
                    raise ValueError("history_restore_schema_mismatch")
            for table, items in reversed(tuple(rows.items())):
                keys = header["metadata"][table]["keys"]
                match = " and ".join(f't."{k}"=p."{k}"' for k in keys)
                conflict = c.scalar(
                    text(
                        f"select exists(select 1 from {table} t join "
                        f"jsonb_populate_recordset(null::{table},cast(:rows as jsonb)) p "
                        f"on {match} "
                        "where to_jsonb(t)<>to_jsonb(p))"
                    ),
                    {"rows": payload(items)},
                )
                if conflict:
                    raise ValueError("history_restore_row_conflict")
                if execute:
                    c.execute(
                        text(
                            f"insert into {table} select * from jsonb_populate_recordset("
                            f"null::{table},cast(:rows as jsonb)) on conflict do nothing"
                        ),
                        {"rows": payload(items)},
                    )
                    matched = c.scalar(
                        text(
                            f"select count(*) from {table} t join "
                            f"jsonb_populate_recordset(null::{table},cast(:rows as jsonb)) p "
                            f"on {match} "
                            "where to_jsonb(t)=to_jsonb(p)"
                        ),
                        {"rows": payload(items)},
                    )
                    if matched != len(items):
                        raise ValueError("history_restore_verification_failed")
            return {
                "archive_id": str(archive_id),
                "rows": archive.row_count,
                "preview": not execute,
            }

    def _rows(
        self, c: Connection, query: str, params: dict[str, Any], checkpoint: Callable[[], None]
    ) -> tuple[str, ...]:
        records: list[str] = []
        size = 0
        for row in c.execution_options(stream_results=True).execute(text(query), params).scalars():
            checkpoint()
            if row is None:
                raise ValueError("history_group_byte_bound")
            size += len(row.encode("utf8")) + 1
            if size > MAX_BYTES or len(records) >= MAX_ROWS:
                raise ValueError("history_group_bound")
            records.append(row)
        c.execution_options(stream_results=False)
        return tuple(records)

    def _lock(self, c: Connection, table: str, rows: tuple[str, ...], cutoff: date) -> None:
        if rows:
            c.execute(
                text("select audit.history_lock(:table,cast(:rows as jsonb),:cutoff)"),
                {"table": table, "rows": payload(rows), "cutoff": cutoff},
            )

    def preview(self, cutoff: date) -> dict[str, Any]:
        result = {}
        for policy in POLICIES:
            try:
                with self.transaction(readonly=True) as c:
                    count = c.scalar(
                        text(
                            f"select count(*) from (select 1 from {policy.table} t "
                            f"where {policy.predicate} limit 201) candidates"
                        ),
                        {"cutoff": cutoff},
                    )
                    result[policy.table] = {
                        "candidate_roots": count,
                        "counts_complete": count < 201,
                    }
            except Exception as error:
                result[policy.table] = {"error": type(error).__name__, "counts_complete": False}
        return {"cutoff": str(cutoff), "groups": result, "preview": True}

    def clean_one(
        self,
        policy: RetentionPolicy,
        cutoff: date,
        run_id: UUID,
        root: Path,
        checkpoint: Callable[[], None],
        *,
        row_budget: int = MAX_ROWS,
        after: str | None = None,
    ) -> dict[str, Any]:
        cursor: list[str] = []
        try:
            return self._clean_one(
                policy,
                cutoff,
                run_id,
                root,
                checkpoint,
                row_budget=row_budget,
                after=after,
                cursor=cursor,
            )
        except CleanupInterrupted:
            raise
        except Exception as error:
            if cursor:
                code = str(error) if isinstance(error, ValueError) else type(error).__name__
                raise HistoryGroupFailure(cursor[-1], code) from error
            raise

    def _clean_one(
        self,
        policy: RetentionPolicy,
        cutoff: date,
        run_id: UUID,
        root: Path,
        checkpoint: Callable[[], None],
        *,
        row_budget: int = MAX_ROWS,
        after: str | None = None,
        cursor: list[str],
    ) -> dict[str, Any]:
        if policy not in POLICIES:
            raise ValueError("unknown_history_policy")
        checkpoint()
        with self.transaction() as c:
            metadata = {policy.table: self.metadata(c, policy.table)}
            order = ",".join('t."' + key + '"' for key in metadata[policy.table]["keys"])
            prior = ",".join('p."' + key + '"' for key in metadata[policy.table]["keys"])
            seek = (
                (
                    f" and ({order}) > (select {prior} from jsonb_populate_record("
                    f"null::{policy.table},cast(:after as jsonb)) p)"
                )
                if after
                else ""
            )
            projection = (
                "case when octet_length(to_jsonb(t)::text) <= :max_bytes then to_jsonb(t)::text end"
            )
            roots = self._rows(
                c,
                f"select {projection} from {policy.table} t "
                f"where ({policy.predicate}) {seek} order by {order} limit :limit",
                {
                    "cutoff": cutoff,
                    "limit": 1 if policy.children else min(200, row_budget),
                    "max_bytes": MAX_BYTES,
                    "after": after,
                },
                checkpoint,
            )
            if not roots:
                return {"deleted_rows": 0}
            cursor.append(roots[-1])
            self._lock(c, policy.table, roots, cutoff)
            rows = {}
            for table, join in policy.children:
                metadata[table] = self.metadata(c, table)
                ordering = ",".join('t."' + key + '"' for key in metadata[table]["keys"])
                children = self._rows(
                    c,
                    f"select {projection} from {table} t where exists("
                    f"select 1 from jsonb_populate_recordset(null::{policy.table},"
                    "cast(:parents as jsonb)) p "
                    f"where {join}) order by {ordering} limit :limit",
                    {"parents": payload(roots), "limit": MAX_ROWS + 1, "max_bytes": MAX_BYTES},
                    checkpoint,
                )
                if children:
                    self._lock(c, table, children, cutoff)
                    rows[table] = children
            rows[policy.table] = roots
            if sum(map(len, rows.values())) > row_budget:
                raise ValueError("history_group_row_budget")
            header = {
                "version": "history_archive.v1",
                "workflow_run_id": str(run_id),
                "policy": policy.table,
                "cutoff": str(cutoff),
                "metadata": metadata,
                "database_identity": c.scalar(text("select audit.history_database_identity()")),
            }
            archive = write_archive(root, header, rows, uuid4(), checkpoint=checkpoint)
            hashes = {
                table: c.scalar(
                    text("select md5(cast(:rows as jsonb)::text)"), {"rows": payload(items)}
                )
                for table, items in rows.items()
            }
            c.execute(
                text("""insert into audit.history_archive
                (archive_id,workflow_run_id,cutoff,policy_table,object_path,compressed_sha256,
                 content_sha256,compressed_bytes,row_count,payload_hashes)
                values(:id,:run,:cutoff,:policy,:path,:sha,:content,:bytes,:count,
                       cast(:hashes as jsonb))"""),
                {
                    "id": archive.archive_id,
                    "run": run_id,
                    "cutoff": cutoff,
                    "policy": policy.table,
                    "path": archive.path,
                    "sha": archive.sha256,
                    "content": archive.content_sha256,
                    "bytes": archive.compressed_bytes,
                    "count": archive.row_count,
                    "hashes": json.dumps(hashes),
                },
            )
            # Reread the published immutable object immediately before destructive work.
            if read_archive(root, archive) != (header, rows):
                raise ValueError("history_archive_changed")
            checkpoint()
            group = "{" + ",".join(json.dumps(t) + ":" + payload(r) for t, r in rows.items()) + "}"
            c.execute(
                text("select audit.history_delete_group(:table,cast(:rows as jsonb),:cutoff,:id)"),
                {"table": policy.table, "rows": group, "cutoff": cutoff, "id": archive.archive_id},
            )
            checkpoint()
        return {
            "deleted_rows": archive.row_count,
            "cursor": roots[-1],
            "archive": asdict(archive),
            "tables": {table: len(items) for table, items in rows.items()},
        }
