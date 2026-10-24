from contextlib import contextmanager
from datetime import date
from hashlib import sha256
from time import monotonic
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.raw_retention_service import RawRetentionService, retention_cutoff
from market_data_center.raw_retirement import OfflineRawProof, approved_storage_hash
from market_data_center.raw_store import LocalRawStore


@pytest.mark.parametrize(
    "day,expected",
    [
        (date(2026, 10, 24), date(2026, 9, 24)),
        (date(2026, 3, 31), date(2026, 2, 28)),
        (date(2024, 3, 31), date(2024, 2, 29)),
    ],
)
def test_retention_uses_calendar_month_and_shanghai_midnight(day, expected):
    cutoff = retention_cutoff(day)
    assert cutoff.date() == expected
    assert cutoff.hour == 0 and cutoff.utcoffset().total_seconds() == 28800


@pytest.mark.parametrize("corrupt", [False, True])
def test_only_approved_raw_removed_and_failure_keeps_file(tmp_path, corrupt):
    content = b'{"volume":null}\n'
    path = tmp_path / "old.jsonl"
    path.write_bytes(b"corrupt" if corrupt else content)
    unapproved = tmp_path / "unapproved.jsonl"
    unapproved.write_bytes(content)
    raw_id = uuid4()
    manifest = RawManifest(
        raw_id,
        uuid4(),
        path.name,
        RawFileFormat.JSONL,
        sha256(content).hexdigest(),
        len(content),
        1,
        "test.v1",
    )
    proof = OfflineRawProof("verified-offline", "0" * 64, manifest.content_sha256, len(content), 1)
    confirmed = []

    class Repo:
        def candidates(self, cutoff, after):
            return (raw_id,) if after is None else ()

        @contextmanager
        def locked(self, candidate, cutoff, root_hash):
            assert candidate == raw_id
            assert root_hash == approved_storage_hash(LocalRawStore(tmp_path))
            yield manifest, proof, lambda: confirmed.append(raw_id)

        def preview(self, cutoff):
            return {"expired_unapproved_lower_bound": 1, "counts_complete": False}

    result = RawRetentionService(Repo(), tmp_path).run(
        date(2026, 10, 24), CleanupBudget(monotonic() + 10)
    )
    assert unapproved.exists()
    assert path.exists() == corrupt
    assert len(confirmed) == (0 if corrupt else 1)
    assert result.completed == (0 if corrupt else 1)
    assert result.partial  # Never hide unapproved expired backlog.
    assert result.removed_bytes == (0 if corrupt else len(content))


def test_late_approvals_never_report_retention_complete(tmp_path):
    class Repo:
        def candidates(self, cutoff, after):
            return ()

        def preview(self, cutoff):
            return {
                "expired_unapproved_lower_bound": 0,
                "approved_pending_lower_bound": 1,
                "counts_complete": True,
            }

    result = RawRetentionService(Repo(), tmp_path).run(
        date(2026, 10, 24), CleanupBudget(monotonic() + 10)
    )
    assert result.errors == ("raw_retention_pending_approvals",)


def test_storage_approval_changes_when_root_is_replaced(tmp_path):
    root = tmp_path / "raw"
    root.mkdir()
    original = approved_storage_hash(LocalRawStore(root))
    root.rename(tmp_path / "old-root")
    root.mkdir()
    assert approved_storage_hash(LocalRawStore(root)) != original
