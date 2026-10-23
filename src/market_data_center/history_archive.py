"""Bounded immutable full-row archives, retaining PostgreSQL numeric text verbatim."""

import gzip
import io
import json
import os
import shutil
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from typing import Any
from uuid import UUID

from market_data_center.quality_archive import _sync_directory

MAX_BYTES = 32 * 1024**2
MAX_ROWS = 10_000


@dataclass(frozen=True)
class HistoryArchive:
    archive_id: UUID
    path: str
    sha256: str
    content_sha256: str
    compressed_bytes: int
    row_count: int


def archive_path(root: Path, relative: str) -> Path:
    parts = relative.split("/")
    if parts[0] != "_history_archive" or any(p in {"", ".", ".."} for p in parts):
        raise ValueError("invalid_history_archive_path")
    if any(c in relative for c in ("\\", ":", "\x00")):
        raise ValueError("invalid_history_archive_path")
    path = root.absolute().joinpath(*parts)
    if any(p.is_symlink() or p.is_junction() for p in (path, *path.parents)):
        raise ValueError("history_archive_link")
    return path


def read_archive(
    root: Path, archive: HistoryArchive
) -> tuple[dict[str, Any], dict[str, tuple[str, ...]]]:
    path = archive_path(root, archive.path)
    if not path.is_file() or path.stat().st_size != archive.compressed_bytes:
        raise ValueError("history_archive_size")
    if not 0 < archive.compressed_bytes <= MAX_BYTES + 1024**2:
        raise ValueError("history_archive_bound")
    with path.open("rb") as handle:
        compressed = handle.read(archive.compressed_bytes + 1)
    if sha256(compressed).hexdigest() != archive.sha256:
        raise ValueError("history_archive_hash")
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as handle:
        content = handle.read(MAX_BYTES + 1)
    if len(content) > MAX_BYTES or sha256(content).hexdigest() != archive.content_sha256:
        raise ValueError("history_archive_content_hash")
    lines = content.decode("utf8").splitlines()
    header = json.loads(lines[0])
    result: dict[str, list[str]] = {}
    for line in lines[1:]:
        table, row = line.split("\t", 1)
        result.setdefault(table, []).append(row)
    if sum(map(len, result.values())) != archive.row_count:
        raise ValueError("history_archive_row_count")
    return header, {table: tuple(rows) for table, rows in result.items()}


def write_archive(
    root: Path,
    header: dict[str, Any],
    rows: dict[str, tuple[str, ...]],
    archive_id: UUID,
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> HistoryArchive:
    checkpoint()
    count = sum(map(len, rows.values()))
    if not 0 < count <= MAX_ROWS:
        raise ValueError("history_group_row_bound")
    lines = [json.dumps(header, ensure_ascii=False, sort_keys=True)]
    for table, records in rows.items():
        for row in records:
            if "\n" in row or "\r" in row or "\t" in table:
                raise ValueError("history_archive_invalid_row")
            lines.append(table + "\t" + row)
    content = ("\n".join(lines) + "\n").encode("utf8")
    if len(content) > MAX_BYTES:
        raise ValueError("history_group_byte_bound")
    relative = f"_history_archive/{archive_id}.jsonl.gz"
    path = archive_path(root, relative)
    parent = root.absolute()
    while not parent.exists():
        parent = parent.parent
    if shutil.disk_usage(parent).free < max(512 * 1024**2, 2 * len(content) + 1024**2):
        raise ValueError("history_archive_disk_low")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = archive_path(root, relative + ".tmp")
    compressed = gzip.compress(content, compresslevel=6, mtime=0)
    archive = HistoryArchive(
        archive_id,
        relative,
        sha256(compressed).hexdigest(),
        sha256(content).hexdigest(),
        len(compressed),
        count,
    )
    with temp.open("xb") as handle:
        handle.write(compressed)
        handle.flush()
        os.fsync(handle.fileno())
    checkpoint()
    archive_path(root, relative)
    os.link(temp, path)
    _sync_directory(path.parent)
    _sync_directory(root.absolute())
    if read_archive(root, archive) != (header, {t: r for t, r in rows.items() if r}):
        raise ValueError("history_archive_roundtrip")
    temp.unlink()
    _sync_directory(path.parent)
    return archive
