import importlib
import importlib.util
from datetime import date

from market_data_center.quality_cleanup_service import CleanupBudget


def load_module():
    name = "market_data_center.direct_retention_service"
    assert importlib.util.find_spec(name), "15-day direct cleanup is not implemented"
    return importlib.import_module(name)


class Repository:
    def __init__(self, batches=None):
        self.batches = batches or {"quality": [4, 0], "monitor_input": [2, 0]}
        self.calls = []

    def delete_batch(self, target, cutoff, limit):
        self.calls.append((target, cutoff, limit))
        return self.batches[target].pop(0)

    def has_expired(self, target, cutoff):
        return False

    def preview(self, cutoff):
        return {"cutoff": cutoff.isoformat(), "quality": 4, "monitor_input": 2}


def test_direct_retention_is_fifteen_calendar_days_including_year_boundary():
    module = load_module()
    assert module.retention_cutoff(date(2026, 11, 10)) == date(2026, 10, 26)
    assert module.retention_cutoff(date(2027, 1, 10)) == date(2026, 12, 26)


def test_deletes_both_tables_in_fair_committed_batches_without_archives():
    module = load_module()
    repo = Repository()
    result = module.DirectRetentionService(repo).run(
        date(2026, 11, 10), CleanupBudget(100, monotonic=lambda: 0)
    )
    assert result.deleted_rows == 4
    assert result.monitor_input_deleted_rows == 2
    assert not result.partial
    assert [c[0] for c in repo.calls] == ["quality", "monitor_input", "quality", "monitor_input"]
    assert all(c[1] == date(2026, 10, 26) and c[2] == 5000 for c in repo.calls)
    assert result.report()["archive_required"] is False


def test_preview_never_deletes_or_requires_a_raw_directory():
    module = load_module()
    repo = Repository()
    result = module.DirectRetentionService(repo).preview(date(2026, 11, 10))
    assert result == {"cutoff": "2026-10-26", "quality": 4, "monitor_input": 2}
    assert repo.calls == []


def test_budget_expiry_reports_partial_with_prior_commits():
    module = load_module()
    repo = Repository()
    ticks = iter([0, 0, 200])
    result = module.DirectRetentionService(repo).run(
        date(2026, 11, 10), CleanupBudget(100, monotonic=lambda: next(ticks))
    )
    assert result.deleted_rows == 4
    assert result.monitor_input_deleted_rows == 2
    assert result.partial
    assert "maintenance_budget_exhausted" in result.errors


def test_locked_expired_rows_are_partial_not_success():
    module = load_module()
    repo = Repository({"quality": [0], "monitor_input": [0]})
    repo.has_expired = lambda target, cutoff: target == "monitor_input"
    result = module.DirectRetentionService(repo).run(
        date(2026, 11, 10), CleanupBudget(100, monotonic=lambda: 0)
    )
    assert result.partial
    assert "monitor_input:expired_rows_remaining" in result.errors


def test_table_failure_does_not_skip_other_table_or_hide_commits():
    module = load_module()
    repo = Repository()
    delete = repo.delete_batch

    def fail_quality(target, cutoff, limit):
        if target == "quality":
            raise RuntimeError("unsafe detail must not enter report")
        return delete(target, cutoff, limit)

    repo.delete_batch = fail_quality
    result = module.DirectRetentionService(repo).run(
        date(2026, 11, 10), CleanupBudget(100, monotonic=lambda: 0)
    )
    assert result.deleted_rows == 0 and result.monitor_input_deleted_rows == 2
    assert result.errors == ("quality:RuntimeError",)


def test_operations_records_both_delete_counts_and_partial_status():
    module = load_module()
    from market_data_center.domain.operations import ExecutionStatus
    from market_data_center.operations_service import _result_statistics

    summary = module.DirectRetentionResult(4, 2, ("quality:expired_rows_remaining",))
    assert _result_statistics(summary) == (6, 6, 0, ExecutionStatus.PARTIAL)
