from contextlib import contextmanager
from datetime import datetime
from types import SimpleNamespace
from uuid import uuid4
from zoneinfo import ZoneInfo

import pytest

from market_data_center import cleanup_workflow as module
from market_data_center.quality_cleanup_service import QualityCleanupResult
from market_data_center.raw_cleanup_service import RawCleanupResult


@pytest.mark.parametrize("hour", [3, 4, 9])
@pytest.mark.parametrize("fail_quality", [False, True])
@pytest.mark.parametrize("raw_partial", [False, True])
def test_workflow_window_shared_lock_and_reports(
    monkeypatch, tmp_path, hour, fail_quality, raw_partial
):
    calls = []

    class Repo:
        def __init__(self, engine):
            pass

        def start_report(self, *args, **kwargs):
            calls.append("start")

        def finish_report(self, run_id, now, status, report, disk):
            calls.append(status)
            if hour == 3 and fail_quality:
                assert report["quality"]["deleted_rows"] == 12
                assert report["snapshots"]["deleted_rows"] == 0

        def preview(self, cutoff):
            pytest.fail("finalization must not restart a candidate-count query")

    class Quality:
        def __init__(self, repo, root):
            pass

        def disk_report(self):
            return {"severity": "normal"}

        def run(self, day, run_id, budget):
            budget.checkpoint()
            calls.append("quality")
            if fail_quality:
                raise module.QualityCleanupFailure(
                    QualityCleanupResult(
                        scanned=2, completed=1, deleted_rows=12, errors=("OperationalError",)
                    )
                )
            return QualityCleanupResult()

    class Facts:
        def __init__(self, engine):
            pass

        @contextmanager
        def task_lock(self, key, *, on_lock_lost):
            assert key == "maintenance:data_cleanup"
            calls.append("lock")
            yield
            calls.append("unlock")

    class Raw:
        def __init__(self, repo, root):
            pass

        def run(self, day, run_id, budget):
            budget.checkpoint()
            calls.append("raw")
            return RawCleanupResult(errors=("raw_object_budget",) if raw_partial else ())

    class Snapshots:
        def __init__(self, repo):
            pass

        def run(self, day, *, checkpoint):
            checkpoint()
            calls.append("snapshots")
            return SimpleNamespace(deleted_rows=0, history_deleted_rows=0)

    class History:
        def __init__(self, repo, root):
            pass

        def run(self, day, run_id, budget):
            budget.checkpoint()
            calls.append("history")
            return module.HistoryCleanupResult()

    class Execution:
        run = SimpleNamespace(workflow_run_id=uuid4())

        def step(self, code, sequence, operation):
            expected = {
                "cleanup_call_auction_market_series_snapshots": 1,
                "compress_raw": 2,
                "archive_auction_quality": 3,
                "archive_history_results": 4,
                "retire_archived_raw": 5,
            }
            assert sequence == expected[code]
            calls.append(code)
            return operation()

    class Retention:
        def __init__(self, repo, root):
            pass

        def run(self, day, budget):
            calls.append("retention")
            return module.RawRetentionResult()

    for name, fake in [
        ("QualityArchivePersistence", Repo),
        ("QualityCleanupService", Quality),
        ("PostgreSQLPersistence", Facts),
        ("DataCleanupService", Snapshots),
        ("RawCompressionPersistence", Repo),
        ("RawCleanupService", Raw),
        ("HistoryRetentionPersistence", Repo),
        ("HistoryCleanupService", History),
        ("RawRetentionPersistence", Repo),
        ("RawRetentionService", Retention),
    ]:
        monkeypatch.setattr(module, name, fake)

    def run():
        return module.run_cleanup_workflow(
            None,
            SimpleNamespace(raw_data_root=tmp_path),
            Execution(),
            now=datetime(2026, 9, 27, hour, tzinfo=ZoneInfo("Asia/Shanghai")),
            scheduled=True,
        )

    if fail_quality and hour == 3:
        with pytest.raises(module.QualityCleanupFailure):
            run()
        assert calls[-1] == "failed"
        return
    result = run()
    if hour == 3:
        assert calls == [
            "start",
            "lock",
            "cleanup_call_auction_market_series_snapshots",
            "snapshots",
            "compress_raw",
            "raw",
            "archive_auction_quality",
            "quality",
            "archive_history_results",
            "history",
            "retire_archived_raw",
            "retention",
            "unlock",
            "partial" if raw_partial else "succeeded",
        ]
        assert result.partial == raw_partial
    else:
        assert calls == ["start", "archive_auction_quality", "partial"]
        assert result.errors == ("outside_maintenance_window",)


@pytest.mark.parametrize(
    "arguments", [["data-cleanup", "--execute"], ["data-cleanup", "--confirm"]]
)
def test_manual_cleanup_requires_both_flags_before_connect(monkeypatch, arguments):
    from market_data_center import cli

    monkeypatch.setattr(cli, "WorkerSettings", lambda: pytest.fail("must not connect"))
    with pytest.raises(SystemExit) as result:
        cli._run_cleanup_command(cli._parser().parse_args(arguments))
    assert result.value.code == 2
