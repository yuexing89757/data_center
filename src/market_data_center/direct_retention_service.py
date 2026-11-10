"""Owner-authorized 15-day deletion of quality results and monitor inputs, without archives."""

from dataclasses import asdict, dataclass
from datetime import date, timedelta
from typing import Protocol

from market_data_center.quality_cleanup_service import CleanupBudget, CleanupInterrupted

RETENTION_DAYS = 15
BATCH_ROWS = 5000
TARGETS = ("quality", "monitor_input")


def retention_cutoff(reference_date: date) -> date:
    return reference_date - timedelta(days=RETENTION_DAYS)


class DirectRetentionPersistence(Protocol):
    def delete_batch(self, target: str, cutoff: date, limit: int) -> int: ...

    def has_expired(self, target: str, cutoff: date) -> bool: ...

    def preview(self, cutoff: date) -> dict[str, object]: ...


@dataclass(frozen=True)
class DirectRetentionResult:
    deleted_rows: int = 0
    monitor_input_deleted_rows: int = 0
    errors: tuple[str, ...] = ()

    @property
    def partial(self) -> bool:
        return bool(self.errors)

    def report(self) -> dict[str, object]:
        return asdict(self) | {
            "partial": self.partial,
            "retention_days": RETENTION_DAYS,
            "archive_required": False,
        }


class DirectRetentionService:
    def __init__(self, persistence: DirectRetentionPersistence) -> None:
        self.persistence = persistence

    def preview(self, reference_date: date) -> dict[str, object]:
        return self.persistence.preview(retention_cutoff(reference_date))

    def run(self, reference_date: date, budget: CleanupBudget) -> DirectRetentionResult:
        cutoff = retention_cutoff(reference_date)
        pending = list(TARGETS)
        deleted = dict.fromkeys(TARGETS, 0)
        errors = []
        try:
            while pending:
                for target in tuple(pending):
                    budget.checkpoint()
                    try:
                        count = self.persistence.delete_batch(target, cutoff, BATCH_ROWS)
                        if not 0 <= count <= BATCH_ROWS:
                            raise ValueError("invalid committed deletion count")
                        deleted[target] += count
                        if not count:
                            pending.remove(target)
                            if self.persistence.has_expired(target, cutoff):
                                errors.append(f"{target}:expired_rows_remaining")
                    except Exception as error:
                        if target in pending:
                            pending.remove(target)
                        errors.append(f"{target}:{type(error).__name__}")
        except CleanupInterrupted as error:
            errors.append(str(error))
        return DirectRetentionResult(deleted["quality"], deleted["monitor_input"], tuple(errors))
