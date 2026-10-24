"""Owner-authorized permanent monthly Raw expiry, without creating an archive."""

from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from functools import partial
from pathlib import Path

from market_data_center.history_retention import month_cutoff
from market_data_center.persistence.raw_retention_postgres import RawRetentionPersistence
from market_data_center.quality_cleanup_service import SHANGHAI, CleanupBudget, CleanupInterrupted
from market_data_center.raw_orphan_cleanup import purge_orphan_files
from market_data_center.raw_retirement import (
    approved_storage_hash,
    remove_purged_raw,
    verify_purge_raw,
)
from market_data_center.raw_store import LocalRawStore, RawIntegrityError


def retention_cutoff(day: date) -> datetime:
    return datetime.combine(month_cutoff(day), time.min, SHANGHAI)


@dataclass(frozen=True)
class RawRetentionResult:
    scanned: int = 0
    completed: int = 0
    removed_bytes: int = 0
    errors: tuple[str, ...] = ()

    @property
    def partial(self) -> bool:
        return bool(self.errors)

    def report(self) -> dict[str, object]:
        return asdict(self) | {"partial": self.partial}


class RawRetentionService:
    def __init__(
        self,
        persistence: RawRetentionPersistence,
        root: Path,
        *,
        additional_roots: tuple[Path, ...] = (),
    ) -> None:
        self.persistence = persistence
        self.store = LocalRawStore(root)
        self.api_store = LocalRawStore(additional_roots[0]) if additional_roots else None

    def run(self, day: date, budget: CleanupBudget) -> RawRetentionResult:
        cutoff = retention_cutoff(day)
        after = None
        scanned = completed = removed = 0
        errors: set[str] = set()
        try:
            while True:
                budget.checkpoint()
                page = self.persistence.candidates(cutoff, after)
                if not page:
                    break
                for item in page:
                    manifest = item.manifest
                    budget.checkpoint()
                    if scanned >= 10_000:
                        raise CleanupInterrupted("raw_retention_object_budget")
                    scanned += 1
                    after = manifest.raw_id
                    store = self.api_store if item.api_owned else self.store
                    if store is None:
                        errors.add("raw_retention_api_root_not_configured")
                        continue
                    try:
                        root_hash = approved_storage_hash(store)
                        if item.declared_root_hash not in (None, root_hash):
                            raise RawIntegrityError("declared Raw purge root changed")
                        identities = verify_purge_raw(store, manifest, checkpoint=budget.checkpoint)
                        if not identities and item.declared_root_hash is None:
                            errors.add("first_purge_online_raw_missing")
                            continue
                        if not self.persistence.register(manifest, cutoff, root_hash):
                            errors.add("raw_purge_active_replay")
                            continue
                        # Declaration is committed before unlink; a crash cannot reopen replay.
                        if approved_storage_hash(store) != root_hash:
                            raise RawIntegrityError("declared Raw root changed")
                        removed += remove_purged_raw(
                            store,
                            manifest,
                            register=lambda: None,
                            confirm=partial(self.persistence.confirm, manifest, root_hash),
                            checkpoint=budget.checkpoint,
                        )
                        completed += 1
                    except (OSError, RawIntegrityError):
                        errors.add("raw_retention_integrity_or_io_error")
            budget.checkpoint()
            orphan = purge_orphan_files(
                self.persistence,
                tuple(s for s in (self.store, self.api_store) if s is not None),
                cutoff,
                budget,
                10_000 - scanned,
            )
            scanned += orphan.scanned
            completed += orphan.completed
            removed += orphan.removed_bytes
            errors.update(orphan.errors)
            preview = self.persistence.preview(cutoff)
            if preview["expired_objects_lower_bound"]:
                errors.add("raw_retention_backlog")
            if not preview["counts_complete"]:
                errors.add("raw_retention_inventory_incomplete")
        except CleanupInterrupted as error:
            errors.add(str(error))
        return RawRetentionResult(scanned, completed, removed, tuple(sorted(errors)))
