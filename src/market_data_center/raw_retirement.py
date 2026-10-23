"""Exact online representation removal after independently verified offline archival.

Only an explicitly authorized administrator supplies registration callbacks. The
Worker never invokes this module or receives retirement-write privileges.
"""

import gzip
import re
from collections.abc import Callable
from contextlib import nullcontext
from dataclasses import dataclass
from zlib import error as ZlibError

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.raw_compression import _identity, _sync_directory, _verify_stream
from market_data_center.raw_store import LocalRawStore, RawIntegrityError


@dataclass(frozen=True, slots=True)
class OfflineRawProof:
    archive_ref: str
    index_sha256: str
    content_sha256: str
    content_bytes: int
    row_count: int


def verify_online_raw(
    store: LocalRawStore,
    manifest: RawManifest,
    proof: OfflineRawProof,
    *,
    checkpoint: Callable[[], None] = lambda: None,
) -> dict[str, tuple[int, int, int, int]]:
    """Verify all existing online representations without changing any state."""
    if (
        manifest.storage_backend != "local"
        or manifest.file_format is not RawFileFormat.JSONL
        or not 1 <= len(proof.archive_ref) <= 200
        or not re.fullmatch(r"[0-9a-f]{64}", proof.index_sha256)
        or (proof.content_sha256, proof.content_bytes, proof.row_count)
        != (manifest.content_sha256, manifest.byte_size, manifest.row_count)
    ):
        raise RawIntegrityError("offline archive proof does not match Raw manifest")
    if not store.root.is_dir():
        raise RawIntegrityError("Raw root is unavailable")
    identities: dict[str, tuple[int, int, int, int]] = {}
    for relative in (manifest.object_path, manifest.object_path + ".gz"):
        checkpoint()
        path = store.resolve_path(relative)
        if not path.exists():
            continue
        identity = _identity(path)
        try:
            with path.open("rb") as file:
                decoded = (
                    gzip.GzipFile(fileobj=file) if relative.endswith(".gz") else nullcontext(file)
                )
                with decoded as source:
                    _verify_stream(source, manifest, checkpoint)
        except (OSError, EOFError, ZlibError) as error:
            raise RawIntegrityError("online Raw representation cannot be verified") from error
        if _identity(store.resolve_path(relative)) != identity:
            raise RawIntegrityError("online Raw changed during verification")
        identities[relative] = identity
    return identities


def remove_retired_raw(
    store: LocalRawStore,
    manifest: RawManifest,
    proof: OfflineRawProof,
    *,
    register: Callable[[], None],
    confirm: Callable[[], None],
    checkpoint: Callable[[], None] = lambda: None,
) -> int:
    """Verify existing copies, commit offline evidence, then unlink exact copies.

    register must recheck source identity, cutoff, active references and independent
    archive proof. Missing copies require a prior matching retirement declaration.
    confirm records completion only after directory sync. Interrupted work resumes.
    """
    identities = verify_online_raw(store, manifest, proof, checkpoint=checkpoint)
    checkpoint()
    register()
    removed = 0
    for relative, identity in identities.items():
        checkpoint()
        path = store.resolve_path(relative)
        if _identity(path) != identity:
            raise RawIntegrityError("online Raw changed after offline registration")
        path.unlink()
        removed += identity[2]
        _sync_directory(path.parent)
    # Also flush a prior interrupted unlink before confirming an already absent object.
    parent = store.resolve_path(manifest.object_path).parent
    while not parent.exists() and parent != store.root:
        parent = parent.parent
    _sync_directory(parent)
    if any(
        store.resolve_path(p).exists() for p in (manifest.object_path, manifest.object_path + ".gz")
    ):
        raise RawIntegrityError("online Raw representation appeared during retirement")
    checkpoint()
    confirm()
    return removed
