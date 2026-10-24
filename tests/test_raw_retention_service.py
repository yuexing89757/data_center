from datetime import date
from hashlib import sha256
from time import monotonic
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.persistence.raw_retention_postgres import RawPurgeCandidate
from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.raw_retention_service import RawRetentionService, retention_cutoff
from market_data_center.raw_retirement import approved_storage_hash, remove_purged_raw
from market_data_center.raw_store import LocalRawStore


@pytest.fixture(autouse=True)
def isolate_registered_cleanup(monkeypatch):
    from market_data_center import raw_retention_service as module
    from market_data_center.raw_orphan_cleanup import OrphanCleanupResult

    monkeypatch.setattr(module, "purge_orphan_files", lambda *args: OrphanCleanupResult())


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
@pytest.mark.parametrize("api_owned", [False, True])
def test_purge_without_archive_uses_exact_root_and_keeps_corrupt_files(
    tmp_path, corrupt, api_owned
):
    root = tmp_path / "api" if api_owned else tmp_path
    root.mkdir(exist_ok=True)
    content = b'{"volume":null}\n'
    path = root / "old.jsonl"
    path.write_bytes(b"corrupt" if corrupt else content)
    manifest = RawManifest(
        uuid4(),
        uuid4(),
        path.name,
        RawFileFormat.JSONL,
        sha256(content).hexdigest(),
        len(content),
        1,
        "test.v1",
    )
    events = []

    class Repo:
        def candidates(self, cutoff, after):
            return (RawPurgeCandidate(manifest, api_owned, None),) if after is None else ()

        def register(self, item, cutoff, root_hash):
            assert root_hash == approved_storage_hash(LocalRawStore(root))
            events.append("declare_no_archive")
            return True

        def confirm(self, item, root_hash):
            assert not path.exists()
            events.append("confirm")

        def preview(self, cutoff):
            return {"expired_objects_lower_bound": int(corrupt), "counts_complete": True}

    result = RawRetentionService(
        Repo(), tmp_path, additional_roots=(root,) if api_owned else ()
    ).run(date(2026, 10, 24), CleanupBudget(monotonic() + 10))
    assert path.exists() == corrupt
    assert events == ([] if corrupt else ["declare_no_archive", "confirm"])
    assert result.completed == int(not corrupt)
    assert result.partial == corrupt


def test_late_candidates_never_report_retention_complete(tmp_path):
    class Repo:
        def candidates(self, cutoff, after):
            return ()

        def preview(self, cutoff):
            return {"expired_objects_lower_bound": 1, "counts_complete": True}

    result = RawRetentionService(Repo(), tmp_path).run(
        date(2026, 10, 24), CleanupBudget(monotonic() + 10)
    )
    assert result.errors == ("raw_retention_backlog",)


def test_storage_identity_changes_when_root_is_replaced(tmp_path):
    root = tmp_path / "raw"
    root.mkdir()
    before = approved_storage_hash(LocalRawStore(root))
    root.rename(tmp_path / "old-root")
    root.mkdir()
    assert approved_storage_hash(LocalRawStore(root)) != before


@pytest.mark.parametrize("declared", [False, True])
def test_missing_raw_requires_prior_purge_declaration(tmp_path, declared):
    manifest = RawManifest(
        uuid4(),
        uuid4(),
        "missing.jsonl",
        RawFileFormat.JSONL,
        sha256(b"").hexdigest(),
        0,
        0,
        "test.v1",
    )
    events = []

    class Repo:
        def candidates(self, cutoff, after):
            return (
                (
                    RawPurgeCandidate(
                        manifest,
                        False,
                        approved_storage_hash(LocalRawStore(tmp_path)) if declared else None,
                    ),
                )
                if after is None
                else ()
            )

        def register(self, *args):
            events.append("registered")
            return True

        def confirm(self, *args):
            events.append("confirmed")

        def preview(self, cutoff):
            return {"expired_objects_lower_bound": int(not declared), "counts_complete": True}

    result = RawRetentionService(Repo(), tmp_path).run(
        date(2026, 10, 24), CleanupBudget(monotonic() + 10)
    )
    assert result.completed == int(declared)
    assert result.removed_bytes == 0
    assert events == (["registered", "confirmed"] if declared else [])
    if not declared:
        assert "first_purge_online_raw_missing" in result.errors


def test_purge_helper_never_fabricates_archive_evidence(tmp_path):
    content = b'{"volume":null}\n'
    path = tmp_path / "raw.jsonl"
    path.write_bytes(content)
    manifest = RawManifest(
        uuid4(),
        uuid4(),
        path.name,
        RawFileFormat.JSONL,
        sha256(content).hexdigest(),
        len(content),
        1,
        "test.v1",
    )
    events = []
    assert remove_purged_raw(
        LocalRawStore(tmp_path),
        manifest,
        register=lambda: events.append("purge"),
        confirm=lambda: events.append("confirm"),
    ) == len(content)
    assert events == ["purge", "confirm"] and not path.exists()


def test_binary_raw_expiry_matches_original_bytes(tmp_path):
    content = b"PAR1\x00binary\ncontent\x00PAR1"
    path = tmp_path / "raw.parquet"
    path.write_bytes(content)
    manifest = RawManifest(
        uuid4(),
        uuid4(),
        path.name,
        RawFileFormat.PARQUET,
        sha256(content).hexdigest(),
        len(content),
        100,
        "test.v1",
    )
    assert remove_purged_raw(
        LocalRawStore(tmp_path), manifest, register=lambda: None, confirm=lambda: None
    ) == len(content)
    assert not path.exists()
