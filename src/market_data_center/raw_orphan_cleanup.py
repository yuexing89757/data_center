"""Expire old unpublished Raw by conservative physical age and a physical audit."""

import os
import re
from calendar import timegm
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
from pathlib import Path
from typing import Any

from market_data_center.domain.ingestion import ProviderCode
from market_data_center.persistence.raw_retention_postgres import RawRetentionPersistence
from market_data_center.quality_cleanup_service import CleanupBudget, CleanupInterrupted
from market_data_center.raw_compression import _identity, _sync_directory
from market_data_center.raw_retirement import approved_storage_hash
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

PATH_PATTERN = re.compile(
    r"[a-z][a-z0-9_]*/[a-z][a-z0-9_]*/year=\d{4}/month=\d{2}/day=\d{2}/"
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.(?:jsonl|parquet)(?:\.gz)?"
)


@dataclass(frozen=True)
class OrphanCleanupResult:
    scanned: int = 0
    completed: int = 0
    removed_bytes: int = 0
    errors: tuple[str, ...] = ()


def _remove_physical(
    store: LocalRawStore, row: dict[str, Any], root_hash: str, budget: CleanupBudget
) -> int:
    if approved_storage_hash(store) != root_hash:
        raise RawIntegrityError("Raw root changed")
    path = store.resolve_path(row["object_path"])
    if not path.exists():
        parent = path.parent
        while not parent.exists() and parent != store.root:
            parent = parent.parent
        _sync_directory(parent)
        return 0
    identity = _identity(path)
    if (identity[2], identity[3], path.stat().st_ctime_ns) != (
        row["physical_bytes"],
        row["mtime_ns"],
        row["ctime_ns"],
    ):
        raise RawIntegrityError("orphan Raw identity changed")
    digest = sha256()
    with path.open("rb") as stream:
        while block := stream.read(1024 * 1024):
            budget.checkpoint()
            digest.update(block)
    if digest.hexdigest() != row["physical_sha256"] or _identity(path) != identity:
        raise RawIntegrityError("orphan Raw content changed")
    budget.checkpoint()
    if path.stat().st_ctime_ns != row["ctime_ns"]:
        raise RawIntegrityError("orphan Raw metadata changed")
    if approved_storage_hash(store) != root_hash:
        raise RawIntegrityError("Raw root changed")
    path.unlink()
    _sync_directory(path.parent)
    return identity[2]


def purge_orphan_files(
    repo: RawRetentionPersistence,
    stores: tuple[LocalRawStore, ...],
    cutoff: datetime,
    budget: CleanupBudget,
    limit: int,
) -> OrphanCleanupResult:
    scanned = completed = removed = 0
    errors: set[str] = set()
    before_ns = timegm(cutoff.utctimetuple()) * 1_000_000_000

    def consume(store: LocalRawStore, row: dict[str, Any], root_hash: str) -> None:
        nonlocal scanned, completed, removed
        budget.checkpoint()
        if scanned >= limit:
            raise CleanupInterrupted("raw_retention_object_budget")
        scanned += 1
        removed += _remove_physical(store, row, root_hash, budget)
        repo.confirm_orphan(row["purge_id"], root_hash)
        completed += 1

    try:
        for store in stores:
            root_hash = approved_storage_hash(store)
            after = None
            while True:
                pending = repo.pending_orphans(root_hash, after)
                if not pending:
                    break
                for row in pending:
                    after = row["purge_id"]
                    try:
                        consume(store, row, root_hash)
                    except (OSError, RawIntegrityError):
                        errors.add("orphan_raw_integrity_or_io_error")
            for provider in ProviderCode:
                budget.checkpoint()
                try:
                    provider_root = store.resolve_path(provider.value)
                except RawIntegrityError:
                    errors.add("orphan_raw_unsafe_directory")
                    continue
                if not provider_root.is_dir():
                    continue
                for base, dirs, files in os.walk(
                    provider_root,
                    followlinks=False,
                    onerror=lambda _: errors.add("orphan_raw_integrity_or_io_error"),
                ):
                    budget.checkpoint()
                    dirs[:] = [
                        d
                        for d in dirs
                        if not (Path(base) / d).is_symlink() and not (Path(base) / d).is_junction()
                    ]
                    eligible = []
                    for name in files:
                        budget.checkpoint()
                        path = Path(base) / name
                        relative = path.relative_to(store.root).as_posix()
                        if not PATH_PATTERN.fullmatch(relative):
                            continue
                        try:
                            path = store.resolve_path(relative)
                            identity = _identity(path)
                            info = path.stat()
                            if max(info.st_mtime_ns, info.st_ctime_ns) >= before_ns:
                                continue
                            eligible.append((relative, identity, info.st_ctime_ns))
                        except (OSError, RawIntegrityError):
                            errors.add("orphan_raw_integrity_or_io_error")
                    for start in range(0, len(eligible), 200):
                        page = eligible[start : start + 200]
                        known = repo.known_paths(tuple(p.removesuffix(".gz") for p, _, _ in page))
                        for relative, identity, ctime_ns in page:
                            if relative.removesuffix(".gz") in known:
                                continue
                            budget.checkpoint()
                            if scanned >= limit:
                                raise CleanupInterrupted("raw_retention_object_budget")
                            try:
                                path = store.resolve_path(relative)
                                digest = sha256()
                                with path.open("rb") as stream:
                                    while block := stream.read(1024 * 1024):
                                        budget.checkpoint()
                                        digest.update(block)
                                if (
                                    _identity(path) != identity
                                    or path.stat().st_ctime_ns != ctime_ns
                                ):
                                    raise RawIntegrityError("orphan Raw changed before declaration")
                                row = {
                                    "object_path": relative,
                                    "physical_sha256": digest.hexdigest(),
                                    "physical_bytes": identity[2],
                                    "mtime_ns": identity[3],
                                    "ctime_ns": ctime_ns,
                                }
                                row["purge_id"] = repo.register_orphan(root_hash, row, cutoff)
                                if row["purge_id"] is None:
                                    errors.add("orphan_raw_became_registered_or_active")
                                    continue
                                consume(store, row, root_hash)
                            except (OSError, RawIntegrityError):
                                errors.add("orphan_raw_integrity_or_io_error")
    except CleanupInterrupted as error:
        errors.add(str(error))
    return OrphanCleanupResult(scanned, completed, removed, tuple(sorted(errors)))
