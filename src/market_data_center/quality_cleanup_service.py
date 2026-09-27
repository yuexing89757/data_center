"""Bounded, fail-closed cleanup of the ADR-0058 quality whitelist only."""

import shutil
import time as timer
from collections.abc import Callable
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from market_data_center.persistence.quality_archive_postgres import (
    ArchiveUnitTooLarge,
    QualityArchivePersistence,
)
from market_data_center.quality_archive import (
    MAX_ARCHIVE_ROWS,
    archive_temp_path,
    safe_archive_path,
    write_quality_archive,
)
from market_data_center.raw_store import RawIntegrityError

SHANGHAI = ZoneInfo("Asia/Shanghai")


class CleanupInterrupted(RuntimeError):
    pass


@dataclass
class CleanupBudget:
    deadline: float
    monotonic: Callable[[], float] = timer.monotonic
    cancelled: Callable[[], bool] = lambda: False

    def checkpoint(self) -> None:
        if self.cancelled():
            raise CleanupInterrupted("maintenance_lock_lost")
        if self.monotonic() >= self.deadline:
            raise CleanupInterrupted("maintenance_budget_exhausted")


@dataclass(frozen=True)
class QualityCleanupResult:
    scanned: int = 0
    completed: int = 0
    failed: int = 0
    deleted_rows: int = 0
    archived_bytes: int = 0
    errors: tuple[str, ...] = ()

    @property
    def partial(self) -> bool:
        return bool(self.errors)

    def report(self) -> dict[str, object]:
        return asdict(self) | {"partial": self.partial}


class QualityCleanupFailure(RuntimeError):
    def __init__(self, result: QualityCleanupResult) -> None:
        super().__init__("quality_cleanup_failed")
        self.result = result


def quality_cutoff(reference_date: date) -> datetime:
    return datetime.combine(reference_date, time.min, SHANGHAI) - timedelta(days=30)


def maintenance_seconds(now: datetime, *, scheduled: bool) -> float:
    if now.tzinfo is None:
        raise ValueError("maintenance requires an aware time")
    local = now.astimezone(SHANGHAI)
    clock = local.time()
    if scheduled:
        if not time(3) <= clock < time(4):
            return 0
        end = datetime.combine(local.date(), time(4), SHANGHAI)
    else:
        if time(9, 10) <= clock < time(9, 40):
            return 0
        end = datetime.combine(local.date(), time(9, 10), SHANGHAI)
        if clock >= time(9, 40):
            end += timedelta(days=1)
    return min(1800, (end - local).total_seconds())


class DiskUsage(Protocol):
    @property
    def total(self) -> int: ...
    @property
    def used(self) -> int: ...
    @property
    def free(self) -> int: ...


class QualityCleanupService:
    def __init__(
        self,
        persistence: QualityArchivePersistence,
        root: Path,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        disk_usage: Callable[[Path], DiskUsage] = shutil.disk_usage,
    ) -> None:
        self.persistence = persistence
        self.root = root
        self.clock = clock
        self.disk_usage = disk_usage

    def disk_report(self) -> dict[str, object]:
        safe_archive_path(self.root, "_quality_archive")
        path = self.root.absolute()
        while not path.exists():
            path = path.parent
        usage = self.disk_usage(path)
        severity = (
            "critical"
            if usage.free < 2 * 1024**3 or usage.used * 100 >= usage.total * 90
            else "warning"
            if usage.used * 100 >= usage.total * 80
            else "normal"
        )
        return {
            "total_bytes": usage.total,
            "free_bytes": usage.free,
            "used_bytes": usage.used,
            "severity": severity,
        }

    def preview(self, reference_date: date) -> dict[str, object]:
        cutoff = quality_cutoff(reference_date)
        return {
            "quality_before": cutoff.isoformat(),
            **self.persistence.preview(cutoff),
            "disk": self.disk_report(),
            "preview": True,
        }

    def run(
        self, reference_date: date, run_id: UUID, budget: CleanupBudget
    ) -> QualityCleanupResult:
        cutoff = quality_cutoff(reference_date)
        scanned = completed = failed = deleted = archived_bytes = 0
        errors: list[str] = []
        after = None
        try:
            while True:
                budget.checkpoint()
                candidates = self.persistence.quality_candidates(cutoff, after=after)
                if not candidates:
                    break
                for group in candidates:
                    budget.checkpoint()
                    after = (group.ingestion_id, group.rule_code)
                    scanned += 1
                    if deleted + group.row_count > MAX_ARCHIVE_ROWS:
                        failed += 1
                        errors.append("quality_row_budget")
                        continue
                    try:
                        rows = self.persistence.load_quality_rows(group)
                        content_bytes = sum(len(row.encode("utf8")) + 1 for row in rows)
                        # Fail before even registering/creating a temporary file.
                        disk = self.disk_report()
                        if int(str(disk["free_bytes"])) < max(
                            512 * 1024**2, 2 * content_bytes + 1024**2
                        ):
                            raise CleanupInterrupted("archive_disk_space_low")
                        budget.checkpoint()
                        operation_id = uuid4()
                        self.persistence.register_temp(
                            run_id, operation_id, archive_temp_path(operation_id), self.clock()
                        )
                        archive = write_quality_archive(
                            self.root,
                            group,
                            rows,
                            operation_id=operation_id,
                            checkpoint=budget.checkpoint,
                        )
                        count = self.persistence.commit_quality_archive(
                            self.root, archive, rows, cutoff, checkpoint=budget.checkpoint
                        )
                        completed += 1
                        deleted += count
                        archived_bytes += archive.compressed_bytes
                    except ArchiveUnitTooLarge:
                        failed += 1
                        errors.append("archive_unit_too_large")
                    except (OSError, RawIntegrityError) as error:
                        failed += 1
                        errors.append(
                            "archive_integrity_error"
                            if isinstance(error, RawIntegrityError)
                            else "archive_io_error"
                        )
                    # Database failures deliberately escape: no subsequent writes.
        except CleanupInterrupted as error:
            errors.append(str(error))
        except Exception as error:
            errors.append(type(error).__name__)
            raise QualityCleanupFailure(
                QualityCleanupResult(
                    scanned, completed, failed, deleted, archived_bytes, tuple(sorted(set(errors)))
                )
            ) from error
        return QualityCleanupResult(
            scanned, completed, failed, deleted, archived_bytes, tuple(sorted(set(errors)))
        )
