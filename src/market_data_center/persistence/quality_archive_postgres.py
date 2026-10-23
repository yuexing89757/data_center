"""Internal archive transactions. Never delete quality evidence by date alone."""

import json
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from datetime import date, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from sqlalchemy import Connection, Engine, text

from market_data_center.quality_archive import (
    MAX_ARCHIVE_BYTES,
    MAX_ARCHIVE_ROWS,
    QualityArchiveGroup,
    QualityArchiveObject,
    QualityCandidatePage,
    read_quality_archive,
    validate_rows,
)
from market_data_center.raw_store import RawIntegrityError

ELIGIBLE = """
q.dataset_code = 'call_auction_market_series'
and ((q.rule_code = 'realtime_quote.lot_precision' and q.severity = 'info')
  or (q.rule_code = 'realtime_quote.missing_source_timestamp' and q.severity = 'warning'))
and jsonb_typeof(q.details) = 'object' and not q.details ? 'aggregation_version'
"""
GROUP = ELIGIBLE + " and q.ingestion_id = :ingestion_id and q.rule_code = :rule_code"


class ArchiveUnitTooLarge(ValueError):
    pass


class QualityArchivePersistence:
    def __init__(self, engine: Engine) -> None:
        self.engine = engine

    @contextmanager
    def transaction(self, *, readonly: bool = False) -> Iterator[Connection]:
        with self.engine.begin() as connection:
            if readonly:
                connection.execute(text("set transaction read only"))
            connection.execute(text("set local lock_timeout = '2s'"))
            connection.execute(text("set local statement_timeout = '30s'"))
            connection.execute(text("set local timezone = 'UTC'"))
            yield connection

    def quality_candidate_page(
        self,
        cutoff: datetime,
        *,
        after: tuple[UUID, str] | None = None,
        page_size: int = 200,
    ) -> QualityCandidatePage:
        if not 1 <= page_size <= 200:
            raise ValueError("quality candidate page size out of bounds")
        seek = (
            "and (q.ingestion_id, q.rule_code) > (cast(:after_id as uuid), :after_rule)"
            if after is not None
            else ""
        )
        # Materialize only bounded keys before joining runs or aggregating rows.
        # A page of recent/running groups must still advance its scanned cursor.
        with self.transaction(readonly=True) as connection:
            rows = (
                connection.execute(
                    text(f"""
                with keys as materialized (
                    select distinct q.ingestion_id, q.rule_code
                    from audit.quality_result q
                    where {ELIGIBLE} {seek}
                    order by q.ingestion_id, q.rule_code limit :page_size
                )
                select k.ingestion_id, k.rule_code, i.status, i.dataset_code,
                       facts.row_count, facts.first_created_at, facts.last_created_at
                from keys k join ingestion.ingestion_run i using (ingestion_id)
                cross join lateral (
                    select count(*) row_count, min(q.created_at) first_created_at,
                           max(q.created_at) last_created_at
                    from audit.quality_result q
                    where {ELIGIBLE}
                      and q.ingestion_id=k.ingestion_id and q.rule_code=k.rule_code
                ) facts
                order by k.ingestion_id,k.rule_code
                """),
                    {
                        "page_size": page_size,
                        "after_id": after[0] if after else None,
                        "after_rule": after[1] if after else None,
                    },
                )
                .mappings()
                .all()
            )
        groups = tuple(
            QualityArchiveGroup(
                row["ingestion_id"],
                row["rule_code"],
                row["row_count"],
                row["first_created_at"],
                row["last_created_at"],
            )
            for row in rows
            if row["row_count"] > 0
            and row["status"] in ("succeeded", "partial", "failed")
            and row["dataset_code"] == "call_auction_market_series"
            and row["last_created_at"] < cutoff
        )
        cursor = (rows[-1]["ingestion_id"], rows[-1]["rule_code"]) if rows else after
        return QualityCandidatePage(groups, cursor, len(rows), len(rows) < page_size)

    def quality_candidates(
        self, cutoff: datetime, *, after: tuple[UUID, str] | None = None
    ) -> tuple[QualityArchiveGroup, ...]:
        """Compatibility helper; maintenance uses the cursor-bearing bounded page."""
        return self.quality_candidate_page(cutoff, after=after).groups

    def preview(self, cutoff: datetime) -> dict[str, object]:
        page = self.quality_candidate_page(cutoff)
        return {
            "groups": len(page.groups),
            "rows": sum(group.row_count for group in page.groups),
            "counts_complete": page.complete,
            "scanned_groups": page.scanned_groups,
            "next_cursor": (
                [str(page.after[0]), page.after[1]] if page.after is not None else None
            ),
        }

    def snapshot_preview(self, reference_date: date) -> dict[str, object]:
        from market_data_center.data_cleanup_service import (
            retention_cutoff,
            six_calendar_months_before,
        )
        from market_data_center.persistence.postgres import (
            COUNT_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS_BEFORE,
            COUNT_UNARCHIVED_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS_BEFORE,
            PostgreSQLPersistence,
        )

        dates = PostgreSQLPersistence(self.engine).latest_completed_trading_dates(reference_date, 3)
        if len(dates) != 3:
            return {"blocked": "three_completed_trading_dates_required"}
        cutoff = retention_cutoff(reference_date, dates)
        history_cutoff = six_calendar_months_before(reference_date)
        with self.transaction(readonly=True) as c:
            return {
                "online_before": cutoff.isoformat(),
                "history_before": history_cutoff.isoformat(),
                "online_rows": c.scalar(
                    COUNT_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS_BEFORE, {"reference_date": cutoff}
                ),
                "unarchived_rows": c.scalar(
                    COUNT_UNARCHIVED_CALL_AUCTION_MARKET_SERIES_SNAPSHOTS_BEFORE,
                    {"cutoff_date": cutoff},
                ),
                "history_rows": c.scalar(
                    text(
                        "select count(*) from realtime.call_auction_market_series_snapshot_history "
                        "where trade_date < :cutoff"
                    ),
                    {"cutoff": history_cutoff},
                ),
            }

    def _load(self, connection: Connection, group: QualityArchiveGroup) -> tuple[str, ...]:
        params = {"ingestion_id": group.ingestion_id, "rule_code": group.rule_code}
        size = connection.execute(
            text(f"""
            select count(*), coalesce(sum(octet_length(to_jsonb(q)::text) + 1), 0)
            from audit.quality_result q where {GROUP}
        """),
            params,
        ).one()
        if size[0] > MAX_ARCHIVE_ROWS or size[1] > MAX_ARCHIVE_BYTES:
            raise ArchiveUnitTooLarge("archive_unit_too_large")
        # Count and text are reread within one transaction; final commit compares again.
        rows = tuple(
            connection.execute(
                text(f"""
            select to_jsonb(q)::text from audit.quality_result q where {GROUP}
            order by q.quality_result_id limit :row_limit
        """),
                params | {"row_limit": MAX_ARCHIVE_ROWS + 1},
            ).scalars()
        )
        if rows:
            validate_rows(group, rows)
        return rows

    def load_quality_rows(self, group: QualityArchiveGroup) -> tuple[str, ...]:
        with self.transaction(readonly=True) as connection:
            return self._load(connection, group)

    def commit_quality_archive(
        self,
        root: Path,
        archive: QualityArchiveObject,
        rows: tuple[str, ...],
        cutoff: datetime,
        *,
        checkpoint: Callable[[], None],
    ) -> int:
        validate_rows(archive.group, rows)
        if archive.group.last_created_at >= cutoff:
            raise RawIntegrityError("archive is not expired")
        ids = [UUID(json.loads(row)["quality_result_id"]) for row in rows]
        params: dict[str, Any] = {
            "ingestion_id": archive.group.ingestion_id,
            "rule_code": archive.group.rule_code,
            "archive_id": archive.archive_id,
            "object_path": archive.object_path,
            "content_sha256": archive.content_sha256,
            "content_bytes": archive.content_bytes,
            "compressed_sha256": archive.compressed_sha256,
            "compressed_bytes": archive.compressed_bytes,
            "row_count": archive.group.row_count,
            "first_created_at": archive.group.first_created_at,
            "last_created_at": archive.group.last_created_at,
            "quality_ids": ids,
        }
        with self.transaction() as connection:
            # Worker already has UPDATE on ingestion_run. Lock its parent, not audit
            # rows (which would require a new UPDATE grant). FK inserts take KEY SHARE.
            status = connection.execute(
                text("""
                select status from ingestion.ingestion_run
                where ingestion_id = :ingestion_id and dataset_code = 'call_auction_market_series'
                for update
            """),
                params,
            ).scalar_one()
            if status not in {"succeeded", "partial", "failed"}:
                raise RawIntegrityError("ingestion is not terminal")
            checkpoint()
            if read_quality_archive(root, archive, checkpoint=checkpoint) != rows:
                raise RawIntegrityError("archive differs from source")
            existing = (
                connection.execute(
                    text("""
                select * from audit.quality_archive
                where ingestion_id = :ingestion_id and rule_code = :rule_code
            """),
                    params,
                )
                .mappings()
                .one_or_none()
            )
            current = self._load(connection, archive.group)
            if existing is not None:
                identity = {key: value for key, value in params.items() if key != "archive_id"}
                if any(existing[key] != value for key, value in identity.items()) or current:
                    raise RawIntegrityError("conflicting archive registration")
                return 0
            if current != rows:
                raise RawIntegrityError("source changed since archive")
            connection.execute(
                text("""
                insert into audit.quality_archive
                (archive_id, ingestion_id, rule_code, object_path, content_sha256, content_bytes,
                 compressed_sha256, compressed_bytes, row_count, quality_ids,
                 first_created_at, last_created_at)
                values (:archive_id, :ingestion_id, :rule_code, :object_path, :content_sha256,
                    :content_bytes, :compressed_sha256, :compressed_bytes, :row_count,
                    :quality_ids, :first_created_at, :last_created_at)
            """),
                params,
            )
            connection.execute(
                text(f"""
                insert into audit.quality_result
                  (ingestion_id, dataset_code, rule_code, severity, status, message, details)
                select q.ingestion_id, q.dataset_code, q.rule_code, q.severity, q.status, q.message,
                  jsonb_build_object('aggregation_version', 'auction_series_quality.v1',
                    'finding_count', count(*),
                    'affected_keys', jsonb_agg(q.natural_key order by q.quality_result_id),
                    'archive_id', cast(:archive_id as text),
                    'content_sha256', cast(:content_sha256 as text))
                from audit.quality_result q where {GROUP}
                  and q.quality_result_id = any(cast(:quality_ids as uuid[]))
                group by q.ingestion_id, q.dataset_code, q.rule_code,
                         q.severity, q.status, q.message
            """),
                params,
            )
            checkpoint()
            deleted = tuple(
                connection.execute(
                    text(f"""
                delete from audit.quality_result q where {GROUP}
                  and q.quality_result_id = any(cast(:quality_ids as uuid[]))
                returning to_jsonb(q)::text
            """),
                    params,
                ).scalars()
            )
            # Compare the deleted full records, not merely the count. Rollback on drift.
            if set(deleted) != set(rows) or len(deleted) != len(rows):
                raise RawIntegrityError("exact archived deletion mismatch")
            if self._load(connection, archive.group):
                raise RawIntegrityError("unexpected remaining group records")
            checkpoint()
            return len(deleted)

    def archives(self, ingestion_id: UUID) -> tuple[QualityArchiveObject, ...]:
        with self.transaction(readonly=True) as connection:
            rows = connection.execute(
                text(
                    "select * from audit.quality_archive "
                    "where ingestion_id = :id order by rule_code"
                ),
                {"id": ingestion_id},
            ).mappings()
            return tuple(
                QualityArchiveObject(
                    row["archive_id"],
                    QualityArchiveGroup(
                        row["ingestion_id"],
                        row["rule_code"],
                        row["row_count"],
                        row["first_created_at"],
                        row["last_created_at"],
                    ),
                    row["object_path"],
                    row["content_sha256"],
                    row["content_bytes"],
                    row["compressed_sha256"],
                    row["compressed_bytes"],
                )
                for row in rows
            )

    def start_report(
        self,
        run_id: UUID,
        reference_date: date,
        now: datetime,
        cutoff: datetime,
        disk: Mapping[str, object],
        *,
        raw_before: datetime | None = None,
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                text("""
                insert into operations.data_cleanup_report
                  (workflow_run_id, reference_date, started_at, cutoffs, disks, status)
                values (:id, :day, :now, cast(:cutoffs as jsonb), cast(:disks as jsonb), 'running')
            """),
                {
                    "id": run_id,
                    "day": reference_date,
                    "now": now,
                    "cutoffs": json.dumps(
                        {
                            "quality_before": cutoff.isoformat(),
                            **({"raw_before": raw_before.isoformat()} if raw_before else {}),
                        }
                    ),
                    "disks": json.dumps({"before": disk}),
                },
            )

    def register_temp(self, run_id: UUID, operation_id: UUID, path: str, now: datetime) -> None:
        self.register_temps(run_id, [(operation_id, path)], now)

    def register_temps(
        self, run_id: UUID, temps: Sequence[tuple[UUID, str]], now: datetime
    ) -> None:
        if not 1 <= len(temps) <= 200:
            raise ValueError("temporary ownership page size out of bounds")
        with self.transaction() as connection:
            count = connection.execute(
                text("""
                update operations.data_cleanup_report
                set owned_temps = owned_temps || cast(:temp as jsonb)
                where workflow_run_id = :id and status = 'running'
                returning workflow_run_id
            """),
                {
                    "id": run_id,
                    "temp": json.dumps(
                        [
                            {
                                "operation_id": str(operation_id),
                                "path": path,
                                "created_at": now.isoformat(),
                            }
                            for operation_id, path in temps
                        ]
                    ),
                },
            ).scalar_one_or_none()
            if count is None:
                raise RuntimeError("cleanup report is not running")

    def finish_report(
        self,
        run_id: UUID,
        now: datetime,
        status: str,
        result: Mapping[str, object],
        disk: Mapping[str, object],
    ) -> None:
        with self.transaction() as connection:
            connection.execute(
                text("""
                update operations.data_cleanup_report set status = :status, finished_at = :now,
                    steps = cast(:steps as jsonb), disks = disks || cast(:disk as jsonb)
                where workflow_run_id = :id and status = 'running'
            """),
                {
                    "id": run_id,
                    "now": now,
                    "status": status,
                    "steps": json.dumps(result),
                    "disk": json.dumps({"after": disk}),
                },
            )
