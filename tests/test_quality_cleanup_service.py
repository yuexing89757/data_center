from datetime import date, datetime
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_quality_archive import group, row


class Repository:
    def __init__(self):
        self.writes = []
        self.groups = (group(),)

    def quality_candidates(self, cutoff, *, after=None):
        return self.groups if after is None else ()

    def preview(self, cutoff):
        return {"groups": 1, "rows": 1}

    def load_quality_rows(self, candidate):
        return (row(),)

    def register_temp(self, *args):
        self.writes.append("temp")

    def commit_quality_archive(self, root, archive, rows, cutoff, *, checkpoint):
        checkpoint()
        assert (root / archive.object_path).is_file()
        self.writes.append("delete")
        return len(rows)


def service(tmp_path, repo=None, **kwargs):
    from market_data_center.quality_cleanup_service import QualityCleanupService

    return QualityCleanupService(repo or Repository(), tmp_path, **kwargs)


def test_preview_is_read_only_even_when_root_missing(tmp_path):
    repo = Repository()
    root = tmp_path / "missing"
    result = service(root, repo).preview(date(2026, 9, 27))
    assert result["quality_before"] == "2026-08-28T00:00:00+08:00"
    assert result["rows"] == 1
    assert not root.exists() and not repo.writes


def test_archive_registers_temp_before_exact_delete(tmp_path):
    from market_data_center.quality_cleanup_service import CleanupBudget

    repo = Repository()
    result = service(tmp_path, repo).run(
        date(2026, 9, 27), uuid4(), CleanupBudget(100, monotonic=lambda: 0)
    )
    assert repo.writes == ["temp", "delete"]
    assert result.deleted_rows == 1 and not result.partial


def test_space_or_time_budget_never_deletes(tmp_path):
    from market_data_center.quality_cleanup_service import CleanupBudget

    for kwargs, budget in [
        (
            {"disk_usage": lambda p: SimpleNamespace(total=100, used=99, free=1)},
            CleanupBudget(100, monotonic=lambda: 0),
        ),
        ({}, CleanupBudget(0, monotonic=lambda: 1)),
    ]:
        repo = Repository()
        result = service(tmp_path, repo, **kwargs).run(date(2026, 9, 27), uuid4(), budget)
        assert result.partial and result.deleted_rows == 0
        assert not repo.writes


def test_failed_file_candidate_does_not_delete(tmp_path, monkeypatch):
    from market_data_center import quality_cleanup_service as module

    repo = Repository()

    def broken(*args, **kwargs):
        raise OSError("private filesystem details")

    monkeypatch.setattr(module, "write_quality_archive", broken)
    result = service(tmp_path, repo).run(
        date(2026, 9, 27), uuid4(), module.CleanupBudget(100, monotonic=lambda: 0)
    )
    assert result.partial and result.errors == ("archive_io_error",)
    assert repo.writes == ["temp"]


@pytest.mark.parametrize(
    "hour,minute,scheduled,allowed",
    [
        (3, 0, True, True),
        (3, 59, True, True),
        (4, 0, True, False),
        (2, 59, True, False),
        (9, 9, False, True),
        (9, 10, False, False),
        (9, 39, False, False),
        (9, 40, False, True),
    ],
)
def test_maintenance_window(hour, minute, scheduled, allowed):
    from zoneinfo import ZoneInfo

    from market_data_center.quality_cleanup_service import maintenance_seconds

    now = datetime(2026, 9, 27, hour, minute, tzinfo=ZoneInfo("Asia/Shanghai"))
    value = maintenance_seconds(now, scheduled=scheduled)
    assert (value > 0) == allowed
    assert value <= 1800
    if hour == 9 and minute == 9:
        assert value == 60


def test_budget_detects_lock_loss():
    from market_data_center.quality_cleanup_service import CleanupBudget, CleanupInterrupted

    with pytest.raises(CleanupInterrupted, match="maintenance_lock_lost"):
        CleanupBudget(100, monotonic=lambda: 0, cancelled=lambda: True).checkpoint()


def test_later_database_failure_keeps_committed_progress(tmp_path):
    from market_data_center.quality_cleanup_service import CleanupBudget, QualityCleanupFailure

    class LaterFailure(Repository):
        def quality_candidates(self, cutoff, *, after=None):
            if after is not None:
                raise RuntimeError("private database detail")
            return self.groups

    with pytest.raises(QualityCleanupFailure) as failure:
        service(tmp_path, LaterFailure()).run(
            date(2026, 9, 27), uuid4(), CleanupBudget(100, monotonic=lambda: 0)
        )
    assert failure.value.result.completed == 1
    assert failure.value.result.deleted_rows == 1
    assert failure.value.result.archived_bytes > 0
    assert "private" not in str(failure.value)


def test_bad_group_does_not_starve_next_valid_group(tmp_path):
    from dataclasses import replace

    from market_data_center.quality_cleanup_service import CleanupBudget

    repo = Repository()
    repo.groups = (replace(group(), ingestion_id=uuid4()), group())
    result = service(tmp_path, repo).run(
        date(2026, 9, 27), uuid4(), CleanupBudget(100, monotonic=lambda: 0)
    )
    assert result.failed == 1 and result.completed == 1 and result.deleted_rows == 1
    assert result.partial
