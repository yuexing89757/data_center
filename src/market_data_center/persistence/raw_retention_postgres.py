"""Bounded permanent expiry; Raw manifests and business facts are never deleted."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import bindparam, text

from market_data_center.domain.ingestion import RawManifest
from market_data_center.persistence.quality_archive_postgres import QualityArchivePersistence
from market_data_center.persistence.raw_compression_postgres import _manifest

EXPIRED = """m.created_at < :cutoff and i.finished_at < :cutoff
and i.status in ('succeeded','partial','failed')
and m.storage_backend='local' and m.file_format in ('jsonl','parquet')
and (p.raw_id is null or p.online_removed_at is null)
and not exists(select 1 from ingestion.raw_retirement r where r.raw_id=m.raw_id
               and r.restored_at is null and r.online_removed_at is not null)"""


@dataclass(frozen=True)
class RawPurgeCandidate:
    manifest: RawManifest
    api_owned: bool
    declared_root_hash: str | None


class RawRetentionPersistence(QualityArchivePersistence):
    def candidates(
        self, cutoff: datetime, after: UUID | None = None
    ) -> tuple[RawPurgeCandidate, ...]:
        with self.transaction(readonly=True) as c:
            rows = (
                c.execute(
                    text(f"""
                select m.*,i.request_params->>'mode'='fastapi_live_single_symbol' api_owned,
                       p.storage_identity_sha256
                from ingestion.raw_manifest m join ingestion.ingestion_run i using(ingestion_id)
                left join ingestion.raw_purge p using(raw_id)
                where {EXPIRED}
                  and (cast(:after as uuid) is null or m.raw_id > cast(:after as uuid))
                order by m.raw_id limit 200
            """),
                    {"cutoff": cutoff, "after": after},
                )
                .mappings()
                .all()
            )
        return tuple(
            RawPurgeCandidate(_manifest(r), bool(r["api_owned"]), r["storage_identity_sha256"])
            for r in rows
        )

    def preview(self, cutoff: datetime) -> dict[str, object]:
        page = self.candidates(cutoff)
        return {
            "raw_before": cutoff.isoformat(),
            "preview": True,
            "counts_complete": len(page) < 200,
            "expired_objects_lower_bound": len(page),
            "logical_bytes_lower_bound": sum(r.manifest.byte_size for r in page),
            "archive_required": False,
        }

    def register(self, manifest: RawManifest, cutoff: datetime, root_hash: str) -> bool:
        with self.transaction() as c:
            return bool(
                c.execute(
                    text("""
                select ingestion.declare_expired_raw_purge(:id,:cutoff,:root,:hash,:bytes,:rows)
            """),
                    {
                        "id": manifest.raw_id,
                        "cutoff": cutoff,
                        "root": root_hash,
                        "hash": manifest.content_sha256,
                        "bytes": manifest.byte_size,
                        "rows": manifest.row_count,
                    },
                ).scalar_one()
            )

    def confirm(self, manifest: RawManifest, root_hash: str) -> None:
        with self.transaction() as c:
            c.execute(
                text("select ingestion.confirm_expired_raw_purge(:id,:root)"),
                {"id": manifest.raw_id, "root": root_hash},
            )

    def known_paths(self, paths: tuple[str, ...]) -> set[str]:
        if not paths:
            return set()
        if len(paths) > 200:
            raise ValueError("orphan lookup page exceeds 200")
        with self.transaction(readonly=True) as c:
            return set(
                c.execute(
                    text(
                        "select object_path from ingestion.raw_manifest "
                        "where storage_backend='local' and object_path in :paths"
                    ).bindparams(bindparam("paths", expanding=True)),
                    {"paths": paths},
                ).scalars()
            )

    def pending_orphans(self, root_hash: str, after: UUID | None) -> tuple[dict[str, Any], ...]:
        with self.transaction(readonly=True) as c:
            return tuple(
                dict(r)
                for r in c.execute(
                    text("""
                select * from ingestion.raw_orphan_purge where storage_identity_sha256=:root
                and online_removed_at is null and
                (cast(:after as uuid) is null or purge_id>cast(:after as uuid))
                order by purge_id limit 200
            """),
                    {"root": root_hash, "after": after},
                ).mappings()
            )

    def register_orphan(self, root_hash: str, row: dict[str, Any], cutoff: datetime) -> UUID | None:
        with self.transaction() as c:
            value = c.execute(
                text("""
                select ingestion.declare_orphan_raw_purge(
                    :root,:path,:hash,:bytes,:mtime,:ctime,:cutoff)
            """),
                {
                    "root": root_hash,
                    "path": row["object_path"],
                    "hash": row["physical_sha256"],
                    "bytes": row["physical_bytes"],
                    "mtime": row["mtime_ns"],
                    "ctime": row["ctime_ns"],
                    "cutoff": cutoff,
                },
            ).scalar_one()
            return UUID(str(value)) if value is not None else None

    def confirm_orphan(self, purge_id: UUID, root_hash: str) -> None:
        with self.transaction() as c:
            c.execute(
                text("select ingestion.confirm_orphan_raw_purge(:id,:root)"),
                {"id": purge_id, "root": root_hash},
            )
