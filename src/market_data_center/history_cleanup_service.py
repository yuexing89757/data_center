"""Round-robin bounded history cleanup inside the existing maintenance lock."""

from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any
from uuid import UUID

from market_data_center.history_retention import POLICIES, month_cutoff
from market_data_center.persistence.history_retention_postgres import (
    HistoryGroupFailure,
    HistoryRetentionPersistence,
)
from market_data_center.quality_cleanup_service import CleanupBudget, CleanupInterrupted


@dataclass(frozen=True)
class HistoryCleanupResult:
    deleted_rows: int = 0
    groups: int = 0
    tables: dict[str, int] = field(default_factory=dict)
    errors: tuple[str, ...] = ()

    @property
    def partial(self) -> bool:
        return bool(self.errors)

    def report(self) -> dict[str, Any]:
        return asdict(self) | {"partial": self.partial}


class HistoryCleanupService:
    def __init__(self, repo: HistoryRetentionPersistence, root: Path) -> None:
        self.repo = repo
        self.root = root

    def run(self, day: date, run_id: UUID, budget: CleanupBudget) -> HistoryCleanupResult:
        active = list(POLICIES)
        deleted = groups = 0
        tables: dict[str, int] = {}
        errors = []
        cursors: dict[str, str] = {}
        try:
            while active:
                for policy in tuple(active):
                    budget.checkpoint()
                    if deleted >= 100_000:
                        raise CleanupInterrupted("history_row_budget")
                    try:
                        result = self.repo.clean_one(
                            policy,
                            month_cutoff(day),
                            run_id,
                            self.root,
                            budget.checkpoint,
                            row_budget=min(10_000, 100_000 - deleted),
                            after=cursors.get(policy.table),
                        )
                    except CleanupInterrupted:
                        raise
                    except HistoryGroupFailure as error:
                        errors.append(f"{policy.table}:{error}")
                        if cursors.get(policy.table) == error.cursor:
                            active.remove(policy)
                        cursors[policy.table] = error.cursor
                        continue
                    except Exception as error:
                        # SQL messages may contain facts or credentials: never report them.
                        code = str(error) if isinstance(error, ValueError) else type(error).__name__
                        errors.append(f"{policy.table}:{code}")
                        active.remove(policy)
                        continue
                    count = result["deleted_rows"]
                    if not count:
                        active.remove(policy)
                        continue
                    deleted += count
                    cursors[policy.table] = result["cursor"]
                    groups += 1
                    for table, count in result["tables"].items():
                        tables[table] = tables.get(table, 0) + count
        except CleanupInterrupted as error:
            errors.append(str(error))
        return HistoryCleanupResult(deleted, groups, tables, tuple(sorted(set(errors))))
