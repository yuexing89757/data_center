"""Immutable local Raw Store implementation."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date
from hashlib import sha256
from json import JSONDecodeError, dumps, loads
from pathlib import Path, PurePosixPath
from uuid import UUID

from market_data_center.domain.ingestion import RawFileFormat, RawManifest


class RawIntegrityError(RuntimeError):
    """Raised when an immutable Raw object cannot be safely replayed."""


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

    def read_jsonl(self, manifest: RawManifest) -> tuple[Mapping[str, str], ...]:
        if manifest.storage_backend != "local" or manifest.file_format is not RawFileFormat.JSONL:
            raise RawIntegrityError("Raw object is not a supported local JSONL file")
        path = self._root.joinpath(*PurePosixPath(manifest.object_path).parts).resolve()
        if not path.is_relative_to(self._root):
            raise RawIntegrityError("Raw object path escapes the configured root")
        try:
            payload = path.read_bytes()
        except FileNotFoundError as error:
            raise RawIntegrityError("Raw object is missing") from error
        except OSError as error:
            raise RawIntegrityError("Raw object cannot be read") from error
        if len(payload) != manifest.byte_size:
            raise RawIntegrityError("Raw object byte size does not match its manifest")
        if sha256(payload).hexdigest() != manifest.content_sha256:
            raise RawIntegrityError("Raw object SHA-256 does not match its manifest")

        rows: list[Mapping[str, str]] = []
        try:
            for line_number, line in enumerate(payload.splitlines(), start=1):
                decoded = loads(line)
                if not isinstance(decoded, dict) or not all(
                    isinstance(key, str) and isinstance(value, str)
                    for key, value in decoded.items()
                ):
                    raise RawIntegrityError(f"Raw JSONL row {line_number} is not a string mapping")
                rows.append(decoded)
        except (JSONDecodeError, UnicodeDecodeError) as error:
            raise RawIntegrityError("Raw object contains invalid JSONL") from error
        if len(rows) != manifest.row_count:
            raise RawIntegrityError("Raw object row count does not match its manifest")
        return tuple(rows)
