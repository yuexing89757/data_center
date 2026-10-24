from datetime import UTC, datetime, timedelta
from time import monotonic
from uuid import uuid4

import pytest

from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.raw_orphan_cleanup import purge_orphan_files
from market_data_center.raw_store import LocalRawStore


@pytest.mark.parametrize("old,registered", [(True, False), (False, False), (True, True)])
def test_orphan_expiry_preserves_recent_and_registered_files(tmp_path, old, registered):
    relative = f"pytdx/daily_bar/year=2026/month=08/day=10/{uuid4()}.jsonl"
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_bytes(b"source bytes\n")
    protected = tmp_path / "_history_archive" / "old.jsonl"
    protected.parent.mkdir()
    protected.write_bytes(b"keep")
    audit = []

    class Repo:
        def pending_orphans(self, *args):
            return ()

        def known_paths(self, paths):
            return set(paths) if registered else set()

        def register_orphan(self, root, row, cutoff):
            audit.append(row.copy())
            return uuid4()

        def confirm_orphan(self, *args):
            assert not path.exists()

    # Future cutoff isolates physical unlink logic; real SQL independently caps monthly cutoff.
    cutoff = datetime.now(UTC) + timedelta(days=1 if old else -1)
    result = purge_orphan_files(
        Repo(), (LocalRawStore(tmp_path),), cutoff, CleanupBudget(monotonic() + 10), 10
    )
    assert path.exists() == (not old or registered)
    assert result.completed == int(old and not registered)
    assert len(audit) == result.completed
    assert protected.exists()
