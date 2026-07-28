"""Immutable local Raw Store implementation."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from json import dumps
from pathlib import Path, PurePosixPath
from uuid import UUID


@dataclass(frozen=True, slots=True)
class StoredRawObject:
    object_path: str
    content_sha256: str
    byte_size: int
    row_count: int
    file_format: str
    schema_version: str


class LocalRawStore:
    def __init__(self, root: Path) -> None:
        self._root = root.resolve()

    def write_jsonl(
        self,
        *,
        provider: str,
        dataset: str,
        partition_date: date,
        ingestion_id: UUID,
        rows: Iterable[Mapping[str, str]],
        schema_version: str,
    ) -> StoredRawObject:
        relative_path = PurePosixPath(
            provider,
            dataset,
            f"year={partition_date:%Y}",
            f"month={partition_date:%m}",
            f"day={partition_date:%d}",
            f"{ingestion_id}.jsonl",
        )
        destination = self._root.joinpath(*relative_path.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        digest = sha256()
        byte_size = 0
        row_count = 0
        created = False
        try:
            with destination.open("xb") as raw_file:
                created = True
                for row in rows:
                    payload = (
                        dumps(dict(row), ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                        + "\n"
                    ).encode()
                    raw_file.write(payload)
                    digest.update(payload)
                    byte_size += len(payload)
                    row_count += 1
        except Exception:
            if created:
                destination.unlink(missing_ok=True)
            raise

        return StoredRawObject(
            object_path=relative_path.as_posix(),
            content_sha256=digest.hexdigest(),
            byte_size=byte_size,
            row_count=row_count,
            file_format="jsonl",
            schema_version=schema_version,
        )
