"""Worker consumes sealed admin approvals; it cannot register an offline archive."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import datetime
from uuid import UUID

from sqlalchemy import text

from market_data_center.domain.ingestion import RawManifest
from market_data_center.persistence.quality_archive_postgres import QualityArchivePersistence
from market_data_center.persistence.raw_compression_postgres import _manifest
from market_data_center.raw_retirement import OfflineRawProof

EXPIRED = """m.created_at < :cutoff and i.finished_at < :cutoff
and i.status in ('succeeded','partial','failed')
and m.storage_backend='local' and m.file_format='jsonl'"""


class RawRetentionPersistence(QualityArchivePersistence):
    def candidates(self, cutoff: datetime, after: UUID | None = None) -> tuple[UUID, ...]:
        with self.transaction(readonly=True) as c:
            return tuple(
                c.execute(
                    text(f"""
                select m.raw_id from ingestion.raw_manifest m
                join ingestion.ingestion_run i using(ingestion_id)
                join ingestion.raw_retirement t using(raw_id)
                where {EXPIRED} and t.restored_at is null and t.online_removed_at is null
                and t.approved_storage_sha256 is not null
                and (:after is null or m.raw_id > cast(:after as uuid))
                order by m.raw_id limit 200
            """),
                    {"cutoff": cutoff, "after": after},
                ).scalars()
            )

    def preview(self, cutoff: datetime) -> dict[str, object]:
        with self.transaction(readonly=True) as c:
            rows = (
                c.execute(
                    text(f"""
                select t.raw_id,t.restored_at,t.approved_storage_sha256
                from ingestion.raw_manifest m join ingestion.ingestion_run i using(ingestion_id)
                left join ingestion.raw_retirement t using(raw_id)
                where {EXPIRED} and (t.raw_id is null or t.restored_at is not null
                    or t.online_removed_at is null)
                order by m.created_at,m.raw_id limit 201
            """),
                    {"cutoff": cutoff},
                )
                .mappings()
                .all()
            )
        return {
            "raw_before": cutoff.isoformat(),
            "preview": True,
            "counts_complete": len(rows) < 201,
            "expired_unapproved_lower_bound": sum(
                r["raw_id"] is None
                or r["restored_at"] is not None
                or r["approved_storage_sha256"] is None
                for r in rows
            ),
            "approved_pending_lower_bound": sum(
                r["raw_id"] is not None
                and r["restored_at"] is None
                and r["approved_storage_sha256"] is not None
                for r in rows
            ),
        }

    @contextmanager
    def locked(
        self, raw_id: UUID, cutoff: datetime, root_hash: str
    ) -> Iterator[tuple[RawManifest, OfflineRawProof, Callable[[], None]] | None]:
        with self.transaction() as c:
            value = c.execute(
                text("""
                select ingestion.lock_approved_raw_retirement(:id,:cutoff,:root)
            """),
                {"id": raw_id, "cutoff": cutoff, "root": root_hash},
            ).scalar_one()
            if value is None:
                yield None
                return
            row = value["manifest"]
            row["raw_id"], row["ingestion_id"] = UUID(row["raw_id"]), UUID(row["ingestion_id"])
            manifest = _manifest(row)
            approval = value["approval"]
            proof = OfflineRawProof(
                approval["archive_ref"],
                approval["archive_manifest_sha256"],
                approval["content_sha256"],
                approval["content_bytes"],
                approval["row_count"],
            )

            def confirm() -> None:
                c.execute(
                    text("select ingestion.confirm_approved_raw_retirement(:id,:root)"),
                    {"id": raw_id, "root": root_hash},
                )

            yield manifest, proof, confirm
