"""Immutable, bounded archives of complete historical quality evidence.

Rows are PostgreSQL JSON text, not re-encoded Python floats. This preserves every
JSON numeric digit and field while keeping a deterministic source representation.
"""

import gzip
import json
import os
from collections.abc import Callable
from dataclasses import dataclass, replace
from datetime import datetime
from decimal import Decimal
from hashlib import sha256
from pathlib import Path
from stat import S_ISREG
from uuid import UUID, uuid4
from zlib import error as ZlibError

from market_data_center.raw_store import RawIntegrityError

MAX_ARCHIVE_BYTES = 256 * 1024 * 1024
MAX_ARCHIVE_ROWS = 100_000
QUALITY_RULES = {
    "realtime_quote.lot_precision": "info",
    "realtime_quote.missing_source_timestamp": "warning",
}
ROW_FIELDS = frozenset(
    {
        "quality_result_id",
        "ingestion_id",
        "dataset_code",
        "rule_code",
        "severity",
        "status",
        "natural_key",
        "message",
        "details",
        "created_at",
    }
)


@dataclass(frozen=True)
class QualityArchiveGroup:
    ingestion_id: UUID
    rule_code: str
    row_count: int
    first_created_at: datetime
    last_created_at: datetime


@dataclass(frozen=True)
class QualityArchiveObject:
    archive_id: UUID
    group: QualityArchiveGroup
    object_path: str
    content_sha256: str
    content_bytes: int
    compressed_sha256: str
    compressed_bytes: int


def archive_temp_path(operation_id: UUID) -> str:
    return f"_quality_archive/_tmp/{operation_id}.jsonl.gz.tmp"


def safe_archive_path(root: Path, relative: str) -> Path:
    if root.is_symlink() or root.is_junction():
        raise RawIntegrityError("archive root must not be a link")
    base = root.absolute()
    for parent in (base, *base.parents):
        if parent.is_symlink() or parent.is_junction():
            raise RawIntegrityError("archive root contains a link")
    parts = relative.split("/")
    if parts[0] != "_quality_archive" or any(p in {"", ".", ".."} for p in parts):
        raise RawIntegrityError("invalid archive path")
    if any(c in relative for c in ("\\", ":", "\x00")):
        raise RawIntegrityError("invalid archive path")
    path = base
    for part in parts:
        path = path / part
        if path.is_symlink() or path.is_junction():
            raise RawIntegrityError("archive path contains a link")
    if not path.resolve().is_relative_to(base.resolve()):
        raise RawIntegrityError("archive path escapes root")
    return path


def validate_rows(group: QualityArchiveGroup, rows: tuple[str, ...]) -> None:
    if not 0 < len(rows) == group.row_count <= MAX_ARCHIVE_ROWS:
        raise RawIntegrityError("invalid archive row count")
    if group.rule_code not in QUALITY_RULES:
        raise RawIntegrityError("unsupported archive rule")
    ids = []
    timestamps = []
    size = 0
    try:
        for line in rows:
            if "\n" in line or "\r" in line:
                raise ValueError("non JSONL row")
            size += len(line.encode("utf8")) + 1
            if size > MAX_ARCHIVE_BYTES:
                raise ValueError("archive exceeds bound")
            row = json.loads(line, parse_float=Decimal)
            if set(row) != ROW_FIELDS or row["ingestion_id"] != str(group.ingestion_id):
                raise ValueError("archive identity mismatch")
            if (
                row["dataset_code"] != "call_auction_market_series"
                or row["rule_code"] != group.rule_code
                or row["severity"] != QUALITY_RULES[group.rule_code]
                or not isinstance(row["details"], dict)
                or "aggregation_version" in row["details"]
            ):
                raise ValueError("non-whitelisted row")
            ids.append(UUID(row["quality_result_id"]))
            stamp = datetime.fromisoformat(row["created_at"])
            if stamp.tzinfo is None:
                raise ValueError("naive creation time")
            timestamps.append(stamp)
        if ids != sorted(set(ids)):
            raise ValueError("unordered or duplicate row IDs")
        if min(timestamps) != group.first_created_at or max(timestamps) != group.last_created_at:
            raise ValueError("creation range mismatch")
    except (ValueError, TypeError, KeyError) as exc:
        raise RawIntegrityError("invalid complete quality archive rows") from exc


def _sync_directory(path: Path) -> None:
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _read_bytes(path: Path, bound: int) -> bytes:
    stat = path.lstat()
    if not S_ISREG(stat.st_mode):
        raise RawIntegrityError("archive is not a regular file")
    if not 0 <= stat.st_size <= bound:
        raise RawIntegrityError("archive exceeds byte bound")
    with path.open("rb") as handle:
        content = handle.read(stat.st_size + 1)
    if len(content) != stat.st_size:
        raise RawIntegrityError("archive changed during read")
    return content


def read_quality_archive(
    root: Path,
    archive: QualityArchiveObject,
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> tuple[str, ...]:
    if (
        not 0 < archive.content_bytes <= MAX_ARCHIVE_BYTES
        or not 0 < archive.compressed_bytes <= MAX_ARCHIVE_BYTES + 1024 * 1024
    ):
        raise RawIntegrityError("invalid archive size")
    try:
        checkpoint()
        path = safe_archive_path(root, archive.object_path)
        compressed = _read_bytes(path, archive.compressed_bytes)
        if (
            len(compressed) != archive.compressed_bytes
            or sha256(compressed).hexdigest() != archive.compressed_sha256
        ):
            raise RawIntegrityError("compressed archive checksum mismatch")
        chunks = []
        size = 0
        # A bounded streaming read avoids trusting gzip's advertised expansion size.
        with gzip.open(path, "rb") as handle:
            while block := handle.read(min(1024 * 1024, archive.content_bytes - size + 1)):
                checkpoint()
                size += len(block)
                if size > archive.content_bytes:
                    raise RawIntegrityError("archive expansion exceeds bound")
                chunks.append(block)
        content = b"".join(chunks)
        if (
            size != archive.content_bytes
            or sha256(content).hexdigest() != archive.content_sha256
            or not content.endswith(b"\n")
        ):
            raise RawIntegrityError("archive content checksum mismatch")
        rows = tuple(content[:-1].decode("utf8").split("\n"))
        validate_rows(archive.group, rows)
        return rows
    except (OSError, EOFError, ValueError, ZlibError) as exc:
        raise RawIntegrityError("quality archive cannot be verified") from exc


def write_quality_archive(
    root: Path,
    group: QualityArchiveGroup,
    rows: tuple[str, ...],
    *,
    operation_id: UUID,
    checkpoint: Callable[[], None] = lambda: None,
) -> QualityArchiveObject:
    checkpoint()
    validate_rows(group, rows)
    digest = sha256()
    size = 0
    for line in rows:
        block = (line + "\n").encode("utf8")
        digest.update(block)
        size += len(block)
    relative = (
        f"_quality_archive/{group.ingestion_id}/{group.rule_code}/{digest.hexdigest()}.jsonl.gz"
    )
    target = safe_archive_path(root, relative)
    temp = safe_archive_path(root, archive_temp_path(operation_id))
    target.parent.mkdir(parents=True, exist_ok=True)
    temp.parent.mkdir(parents=True, exist_ok=True)
    safe_archive_path(root, relative)
    safe_archive_path(root, archive_temp_path(operation_id))
    with temp.open("xb") as raw:
        with gzip.GzipFile(filename="", fileobj=raw, mode="wb", compresslevel=6, mtime=0) as handle:
            for line in rows:
                checkpoint()
                handle.write((line + "\n").encode("utf8"))
        raw.flush()
        os.fsync(raw.fileno())
    compressed = _read_bytes(temp, MAX_ARCHIVE_BYTES + 1024 * 1024)
    archive = QualityArchiveObject(
        uuid4(),
        group,
        relative,
        digest.hexdigest(),
        size,
        sha256(compressed).hexdigest(),
        len(compressed),
    )
    if (
        read_quality_archive(
            root,
            replace(archive, object_path=archive_temp_path(operation_id)),
            checkpoint=checkpoint,
        )
        != rows
    ):
        raise RawIntegrityError("archive roundtrip mismatch")
    checkpoint()
    try:
        os.link(temp, target)
    except FileExistsError:
        if read_quality_archive(root, archive, checkpoint=checkpoint) != rows:
            raise RawIntegrityError("existing archive differs") from None
    # Persist every newly created directory entry, not just the leaf file.
    for directory in target.absolute().parents:
        _sync_directory(directory)
        if directory == root.absolute().parent:
            break
    temp.unlink()
    _sync_directory(temp.parent)
    if read_quality_archive(root, archive, checkpoint=checkpoint) != rows:
        raise RawIntegrityError("published archive differs")
    return archive
