"""Bounded lossless representations; database registration precedes plaintext removal."""

import gzip
import os
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha256
from pathlib import Path
from stat import S_ISREG
from typing import Protocol
from uuid import UUID
from zlib import error as ZlibError

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

MAX_RAW_BYTES = 256 * 1024**2
CHUNK = 1024**2


class ByteReader(Protocol):
    def read(self, size: int = -1) -> bytes: ...


class ByteWriter(Protocol):
    def write(self, data: bytes, /) -> object: ...


@dataclass(frozen=True, slots=True)
class RawCompressionObject:
    object_path: str
    compressed_sha256: str
    compressed_bytes: int


class PlaintextRemovalSyncError(OSError):
    """Unlink completed, but its directory durability could not be confirmed."""

    def __init__(self, removed_bytes: int) -> None:
        super().__init__("Raw plaintext removal directory sync failed")
        self.removed_bytes = removed_bytes


def raw_temp_path(manifest: RawManifest, operation_id: UUID) -> str:
    return f"{manifest.object_path}.gz.{operation_id}.tmp"


def _sync_directory(path: Path) -> None:
    if os.name != "nt":
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _identity(path: Path) -> tuple[int, int, int, int]:
    info = path.lstat()
    if not S_ISREG(info.st_mode) or path.is_symlink() or path.is_junction():
        raise RawIntegrityError("Raw representation is not a regular file")
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns


def _validate_manifest(manifest: RawManifest) -> None:
    if (
        manifest.storage_backend != "local"
        or manifest.file_format is not RawFileFormat.JSONL
        or not 0 <= manifest.byte_size <= MAX_RAW_BYTES
        or manifest.row_count < 0
    ):
        raise RawIntegrityError("Raw compression requires a bounded registered local JSONL")


def _verify_stream(
    handle: ByteReader,
    manifest: RawManifest,
    checkpoint: Callable[[], None],
    sink: ByteWriter | None = None,
) -> None:
    digest = sha256()
    size = lines = 0
    tail = b""
    while True:
        checkpoint()
        chunk = handle.read(min(CHUNK, manifest.byte_size - size + 1))
        if not chunk:
            break
        size += len(chunk)
        if size > manifest.byte_size:
            raise RawIntegrityError("Raw byte size exceeds manifest")
        digest.update(chunk)
        lines += chunk.count(b"\n")
        tail = chunk[-1:]
        if sink is not None:
            sink.write(chunk)
    if (
        size != manifest.byte_size
        or digest.hexdigest() != manifest.content_sha256
        or lines + int(bool(tail) and tail != b"\n") != manifest.row_count
    ):
        raise RawIntegrityError("Raw content does not match manifest")


def verify_raw_gzip(
    store: LocalRawStore,
    manifest: RawManifest,
    *,
    checkpoint: Callable[[], None] = lambda: None,
    object_path: str | None = None,
) -> RawCompressionObject:
    _validate_manifest(manifest)
    relative = object_path or manifest.object_path + ".gz"
    path = store.resolve_path(relative)
    before = _identity(path)
    if not 0 < before[2] <= MAX_RAW_BYTES + CHUNK:
        raise RawIntegrityError("Raw gzip exceeds size bound")
    try:
        with path.open("rb") as handle, gzip.GzipFile(fileobj=handle) as decoded:
            _verify_stream(decoded, manifest, checkpoint)
        digest = sha256()
        with path.open("rb") as handle:
            size = 0
            while chunk := handle.read(CHUNK):
                checkpoint()
                size += len(chunk)
                if size > before[2]:
                    raise RawIntegrityError("Raw gzip changed during hashing")
                digest.update(chunk)
    except (OSError, EOFError, ZlibError) as error:
        raise RawIntegrityError("Raw gzip cannot be verified") from error
    if _identity(store.resolve_path(relative)) != before:
        raise RawIntegrityError("Raw gzip changed during verification")
    return RawCompressionObject(relative, digest.hexdigest(), before[2])


def publish_raw_gzip(
    store: LocalRawStore,
    manifest: RawManifest,
    operation_id: UUID,
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> RawCompressionObject:
    _validate_manifest(manifest)
    plain = store.resolve_path(manifest.object_path)
    destination = store.resolve_path(manifest.object_path + ".gz")
    # Published but unregistered representations are recoverable, never overwritten.
    if destination.exists():
        result = verify_raw_gzip(store, manifest, checkpoint=checkpoint)
        if plain.exists():
            before = _identity(plain)
            with plain.open("rb") as handle:
                _verify_stream(handle, manifest, checkpoint)
            if _identity(store.resolve_path(manifest.object_path)) != before:
                raise RawIntegrityError("Raw plaintext changed during verification")
        return result
    before = _identity(plain)
    temporary_relative = raw_temp_path(manifest, operation_id)
    temporary = store.resolve_path(temporary_relative)
    owned = False
    try:
        with temporary.open("xb") as output:
            owned = True
            with (
                plain.open("rb") as source,
                gzip.GzipFile(
                    filename="", mode="wb", fileobj=output, compresslevel=6, mtime=0
                ) as encoded,
            ):
                _verify_stream(source, manifest, checkpoint, encoded)
            output.flush()
            os.fsync(output.fileno())
        if _identity(store.resolve_path(manifest.object_path)) != before:
            raise RawIntegrityError("Raw plaintext changed during compression")
        verify_raw_gzip(store, manifest, checkpoint=checkpoint, object_path=temporary_relative)
        checkpoint()
        # Same-filesystem hard link publishes atomically without replacing a collision.
        os.link(temporary, store.resolve_path(manifest.object_path + ".gz"))
        _sync_directory(destination.parent)
    finally:
        if owned:
            temporary.unlink(missing_ok=True)
            _sync_directory(temporary.parent)
    return verify_raw_gzip(store, manifest, checkpoint=checkpoint)


def remove_verified_plaintext(
    store: LocalRawStore,
    manifest: RawManifest,
    registered: RawCompressionObject,
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> int:
    gzip_path = store.resolve_path(manifest.object_path + ".gz")
    gzip_identity = _identity(gzip_path)
    if verify_raw_gzip(store, manifest, checkpoint=checkpoint) != registered:
        raise RawIntegrityError("Raw gzip differs from registered representation")

    def check_representation() -> None:
        if _identity(store.resolve_path(manifest.object_path + ".gz")) != gzip_identity:
            raise RawIntegrityError("Raw gzip changed before plaintext removal")

    check_representation()
    path = store.resolve_path(manifest.object_path)
    try:
        before = _identity(path)
    except FileNotFoundError:
        _sync_directory(path.parent)
        check_representation()
        return 0  # crash after unlink, before database confirmation
    with path.open("rb") as handle:
        _verify_stream(handle, manifest, checkpoint)
    checkpoint()
    if _identity(store.resolve_path(manifest.object_path)) != before:
        raise RawIntegrityError("Raw plaintext changed before removal")
    check_representation()
    path.unlink()
    try:
        _sync_directory(path.parent)
    except OSError as error:
        raise PlaintextRemovalSyncError(manifest.byte_size) from error
    return manifest.byte_size
