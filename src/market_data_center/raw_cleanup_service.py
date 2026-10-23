"""Crash-resumable Raw compression under the shared maintenance lock and budget."""

import shutil
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from uuid import UUID, uuid4

from market_data_center.persistence.raw_compression_postgres import RawCompressionPersistence
from market_data_center.quality_cleanup_service import SHANGHAI, CleanupBudget, CleanupInterrupted
from market_data_center.raw_compression import (
    PlaintextRemovalSyncError,
    publish_raw_gzip,
    raw_temp_path,
    remove_verified_plaintext,
)
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

MAX_RAW_OBJECTS = 10_000


def raw_cutoff(reference_date: date) -> datetime:
    return datetime.combine(reference_date, time.min, SHANGHAI) - timedelta(days=7)


@dataclass(frozen=True)
class RawCleanupResult:
    scanned: int = 0
    completed: int = 0
    failed: int = 0
    original_bytes: int = 0
    compressed_bytes: int = 0
    plaintext_bytes_removed: int = 0
    errors: tuple[str, ...] = ()

    @property
    def partial(self) -> bool:
        return bool(self.errors)

    def report(self) -> dict[str, object]:
        return asdict(self) | {"partial": self.partial}


class RawCleanupFailure(RuntimeError):
    def __init__(self, result: RawCleanupResult) -> None:
        super().__init__("raw_cleanup_failed")
        self.result = result


class RawCleanupService:
    def __init__(self, persistence: RawCompressionPersistence, root: Path) -> None:
        self.persistence = persistence
        self.store = LocalRawStore(root)

    def run(self, reference_date: date, run_id: UUID, budget: CleanupBudget) -> RawCleanupResult:
        cutoff = raw_cutoff(reference_date)
        scanned = completed = failed = original_bytes = compressed_bytes = removed_bytes = 0
        errors: list[str] = []
        after = None

        def result() -> RawCleanupResult:
            return RawCleanupResult(
                scanned,
                completed,
                failed,
                original_bytes,
                compressed_bytes,
                removed_bytes,
                tuple(sorted(set(errors))),
            )

        try:
            while True:
                budget.checkpoint()
                if scanned >= MAX_RAW_OBJECTS:
                    raise CleanupInterrupted("raw_object_budget")
                page = self.persistence.candidates(cutoff, after=after)
                operations = [(manifest, uuid4()) for manifest in page.manifests]
                budget.checkpoint()
                if operations:
                    self.persistence.register_temps(
                        run_id,
                        [(op, raw_temp_path(manifest, op)) for manifest, op in operations],
                        datetime.now(UTC),
                    )
                for manifest, operation_id in operations:
                    budget.checkpoint()
                    if scanned >= MAX_RAW_OBJECTS:
                        raise CleanupInterrupted("raw_object_budget")
                    scanned += 1
                    try:
                        path = self.store.root
                        while not path.exists():
                            path = path.parent
                        if shutil.disk_usage(path).free < max(
                            512 * 1024**2, 2 * manifest.byte_size + 1024**2
                        ):
                            raise CleanupInterrupted("raw_disk_space_low")
                        representation = publish_raw_gzip(
                            self.store, manifest, operation_id, checkpoint=budget.checkpoint
                        )
                        self.persistence.register(
                            manifest, representation, cutoff, checkpoint=budget.checkpoint
                        )  # committed before any plaintext unlink
                        original_bytes += manifest.byte_size
                        compressed_bytes += representation.compressed_bytes
                        removed_bytes += remove_verified_plaintext(
                            self.store, manifest, representation, checkpoint=budget.checkpoint
                        )
                        self.persistence.confirm_removed(manifest, representation)
                        completed += 1
                    except (OSError, RawIntegrityError) as error:
                        if isinstance(error, PlaintextRemovalSyncError):
                            removed_bytes += error.removed_bytes
                        failed += 1
                        errors.append(
                            "raw_integrity_error"
                            if isinstance(error, RawIntegrityError)
                            else "raw_io_error"
                        )
                if page.complete:
                    break
                if page.after is None or page.after == after:
                    raise RuntimeError("Raw candidate cursor did not advance")
                after = page.after
        except CleanupInterrupted as error:
            errors.append(str(error))
        except Exception as error:
            errors.append(type(error).__name__)
            raise RawCleanupFailure(result()) from error
        return result()
