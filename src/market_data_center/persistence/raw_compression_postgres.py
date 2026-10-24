"""Internal immutable Raw representation registry (no public reads or Raw updates)."""

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import text

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.persistence.quality_archive_postgres import QualityArchivePersistence
from market_data_center.raw_compression import MAX_RAW_BYTES, RawCompressionObject
from market_data_center.raw_store import RawIntegrityError

Cursor = tuple[int, int, datetime, UUID]
ELIGIBLE = """
 m.storage_backend='local' and m.file_format='jsonl'
 and m.byte_size between 0 and :max_bytes and m.created_at < :cutoff
 and i.status in ('succeeded','partial','failed') and i.finished_at < :cutoff
 and not exists(select 1 from ingestion.raw_retirement r
                where r.raw_id=m.raw_id and r.restored_at is null)
 and not exists(select 1 from ingestion.raw_purge p where p.raw_id=m.raw_id)
"""
PRIORITY = "case when c.raw_id is not null then 0 else 1 end"


def _manifest(row: Mapping[Any, Any]) -> RawManifest:
    return RawManifest(
        row["raw_id"],
        row["ingestion_id"],
        row["object_path"],
        RawFileFormat(row["file_format"]),
        row["content_sha256"],
        row["byte_size"],
        row["row_count"],
        row["schema_version"],
        row["storage_backend"],
    )


@dataclass(frozen=True)
class RawCandidatePage:
    manifests: tuple[RawManifest, ...]
    after: Cursor | None
    complete: bool


class RawCompressionPersistence(QualityArchivePersistence):
    def candidates(
        self, cutoff: datetime, *, after: Cursor | None = None, page_size: int = 200
    ) -> RawCandidatePage:
        if not 1 <= page_size <= 200:
            raise ValueError("Raw candidate page size out of bounds")
        seek = (
            f"and ({PRIORITY}, -m.byte_size, m.created_at, m.raw_id) > "
            "(:priority, :negative_bytes, :created_at, cast(:raw_id as uuid))"
            if after
            else ""
        )
        with self.transaction(readonly=True) as connection:
            rows = (
                connection.execute(
                    text(f"""
                select m.*, {PRIORITY} priority from ingestion.raw_manifest m
                join ingestion.ingestion_run i using (ingestion_id)
                left join ingestion.raw_compression c using (raw_id)
                where {ELIGIBLE} and c.plaintext_removed_at is null {seek}
                order by {PRIORITY}, -m.byte_size, m.created_at, m.raw_id limit :page_size
            """),
                    {
                        "cutoff": cutoff,
                        "max_bytes": MAX_RAW_BYTES,
                        "page_size": page_size,
                        "priority": after[0] if after else None,
                        "negative_bytes": after[1] if after else None,
                        "created_at": after[2] if after else None,
                        "raw_id": after[3] if after else None,
                    },
                )
                .mappings()
                .all()
            )
        cursor = (
            (
                rows[-1]["priority"],
                -rows[-1]["byte_size"],
                rows[-1]["created_at"],
                rows[-1]["raw_id"],
            )
            if rows
            else after
        )
        return RawCandidatePage(
            tuple(_manifest(row) for row in rows), cursor, len(rows) < page_size
        )

    def preview(self, cutoff: datetime) -> dict[str, object]:
        page = self.candidates(cutoff)
        return {
            "objects": len(page.manifests),
            "original_bytes": sum(item.byte_size for item in page.manifests),
            "counts_complete": page.complete,
            "raw_before": cutoff.isoformat(),
            "preview": True,
        }

    def register(
        self,
        manifest: RawManifest,
        representation: RawCompressionObject,
        cutoff: datetime,
        *,
        checkpoint: Callable[[], None] = lambda: None,
    ) -> None:
        with self.transaction() as connection:
            checkpoint()
            connection.execute(
                text("""
                select ingestion_id from ingestion.ingestion_run
                where ingestion_id=:id for update
            """),
                {"id": manifest.ingestion_id},
            ).scalar_one()
            row = (
                connection.execute(
                    text(f"""
                select m.* from ingestion.raw_manifest m
                join ingestion.ingestion_run i using (ingestion_id)
                where m.raw_id=:id and {ELIGIBLE}
            """),
                    {"id": manifest.raw_id, "cutoff": cutoff, "max_bytes": MAX_RAW_BYTES},
                )
                .mappings()
                .one_or_none()
            )
            if row is None or _manifest(row) != manifest:
                raise RawIntegrityError("Raw identity or eligibility changed before registration")
            if representation.object_path != manifest.object_path + ".gz":
                raise RawIntegrityError("Raw representation path does not match manifest")
            params = {
                "id": manifest.raw_id,
                "path": representation.object_path,
                "hash": representation.compressed_sha256,
                "bytes": representation.compressed_bytes,
                "original_hash": manifest.content_sha256,
                "original_bytes": manifest.byte_size,
                "original_rows": manifest.row_count,
            }
            connection.execute(
                text("""
                insert into ingestion.raw_compression
                  (raw_id, object_path, compressed_sha256, compressed_bytes,
                   content_sha256, content_bytes, row_count)
                values (:id,:path,:hash,:bytes,:original_hash,:original_bytes,:original_rows)
                on conflict (raw_id) do nothing
            """),
                params,
            )
            registered = connection.execute(
                text("""
                select object_path, compressed_sha256, compressed_bytes, content_sha256,
                       content_bytes, row_count from ingestion.raw_compression where raw_id=:id
            """),
                params,
            ).one()
            if tuple(registered) != (
                representation.object_path,
                representation.compressed_sha256,
                representation.compressed_bytes,
                manifest.content_sha256,
                manifest.byte_size,
                manifest.row_count,
            ):
                raise RawIntegrityError("Raw registry conflicts with published representation")
            checkpoint()

    def confirm_removed(self, manifest: RawManifest, representation: RawCompressionObject) -> None:
        with self.transaction() as connection:
            row = connection.execute(
                text("""
                update ingestion.raw_compression
                set plaintext_removed_at=coalesce(plaintext_removed_at,now())
                where raw_id=:id and object_path=:path and compressed_sha256=:hash
                  and compressed_bytes=:bytes and content_sha256=:original_hash
                  and content_bytes=:original_bytes and row_count=:original_rows
                returning raw_id
            """),
                {
                    "id": manifest.raw_id,
                    "path": representation.object_path,
                    "hash": representation.compressed_sha256,
                    "bytes": representation.compressed_bytes,
                    "original_hash": manifest.content_sha256,
                    "original_bytes": manifest.byte_size,
                    "original_rows": manifest.row_count,
                },
            ).scalar_one_or_none()
            if row is None:
                confirmed = connection.execute(
                    text("""
                    select raw_id from ingestion.raw_compression
                    where raw_id=:id and object_path=:path and compressed_sha256=:hash
                      and compressed_bytes=:bytes and content_sha256=:original_hash
                      and content_bytes=:original_bytes and row_count=:original_rows
                      and plaintext_removed_at is not null
                """),
                    {
                        "id": manifest.raw_id,
                        "path": representation.object_path,
                        "hash": representation.compressed_sha256,
                        "bytes": representation.compressed_bytes,
                        "original_hash": manifest.content_sha256,
                        "original_bytes": manifest.byte_size,
                        "original_rows": manifest.row_count,
                    },
                ).scalar_one_or_none()
                if confirmed is None:
                    raise RawIntegrityError("Raw registry missing during removal confirmation")
