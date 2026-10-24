"""Monthly online retirement of administrator-approved, independently archived Raw."""

from dataclasses import asdict, dataclass
from datetime import date, datetime, time
from pathlib import Path

from market_data_center.history_retention import month_cutoff
from market_data_center.persistence.raw_retention_postgres import RawRetentionPersistence
from market_data_center.quality_cleanup_service import SHANGHAI, CleanupBudget, CleanupInterrupted
from market_data_center.raw_retirement import approved_storage_hash, remove_retired_raw
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
        self.stores = tuple(LocalRawStore(p) for p in (root, *additional_roots))

    def run(self, day: date, budget: CleanupBudget) -> RawRetentionResult:
        cutoff = retention_cutoff(day)
        after = None
        scanned = completed = removed = 0
        errors: set[str] = set()
        stores = {approved_storage_hash(store): store for store in self.stores}
        try:
            while True:
                budget.checkpoint()
                page = self.persistence.candidates(cutoff, after)
                if not page:
                    break
                for raw_id, root_hash in page:
                    budget.checkpoint()
                    if scanned >= 10_000:
                        raise CleanupInterrupted("raw_retention_object_budget")
                    scanned += 1
                    after = raw_id
                    store = stores.get(root_hash)
                    if store is None:
                        errors.add("raw_retirement_approved_root_not_configured")
                        continue
                    try:
                        # Hold the approved row lock through hashing, unlink and confirmation:
                        # an administrator cannot restore/revoke this declaration in between.
                        with self.persistence.locked(raw_id, cutoff, root_hash) as item:
                            if item is None:
                                errors.add("raw_retirement_approval_or_references_changed")
                                continue
                            manifest, proof, confirm = item
                            if approved_storage_hash(store) != root_hash:
                                raise RawIntegrityError("approved Raw root changed")
                            removed += remove_retired_raw(
                                store,
                                manifest,
                                proof,
                                register=lambda: None,
                                confirm=confirm,
                                checkpoint=budget.checkpoint,
                            )
                            completed += 1
                    except (OSError, RawIntegrityError):
                        errors.add("raw_retention_integrity_or_io_error")
            budget.checkpoint()
            preview = self.persistence.preview(cutoff)
            if preview["expired_unapproved_lower_bound"]:
                errors.add("expired_raw_requires_verified_offline_archive")
            if not preview["counts_complete"]:
                errors.add("raw_retention_inventory_incomplete")
            if preview.get("approved_pending_lower_bound"):
                errors.add("raw_retention_pending_approvals")
        except CleanupInterrupted as error:
            errors.add(str(error))
        return RawRetentionResult(scanned, completed, removed, tuple(sorted(errors)))
