from datetime import date
from time import monotonic
from uuid import uuid4

import pytest
from test_raw_store import _raw_fixture

from market_data_center.persistence.raw_compression_postgres import RawCandidatePage
from market_data_center.quality_cleanup_service import CleanupBudget
from market_data_center.raw_cleanup_service import RawCleanupFailure, RawCleanupService


class Repo:
    def __init__(self, manifest, *, fail_register=False, fail_confirm=False):
        self.manifest = manifest
        self.fail_register = fail_register
        self.fail_confirm = fail_confirm
        self.registered = self.confirmed = False
        self.temps = []

    def candidates(self, cutoff, *, after=None):
        return RawCandidatePage((self.manifest,) if not self.confirmed else (), None, True)

    def register_temp(self, run_id, operation_id, path, now):
        self.temps.append(path)

    def register_temps(self, run_id, temps, now):
        self.temps.extend(path for _, path in temps)

    def register(self, manifest, representation, cutoff, *, checkpoint):
        checkpoint()
        if self.fail_register:
            raise RuntimeError("database unavailable")
        self.registered = True

    def confirm_removed(self, manifest, representation):
        assert self.registered
        if self.fail_confirm:
            raise RuntimeError("confirmation failed")
        self.confirmed = True


@pytest.mark.parametrize("failure", ["register", "confirm", None])
def test_commit_before_removal_and_crash_recovery(tmp_path, failure):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    repo = Repo(manifest, fail_register=failure == "register", fail_confirm=failure == "confirm")
    service = RawCleanupService(repo, tmp_path)
    budget = CleanupBudget(monotonic() + 60)
    if failure:
        with pytest.raises(RawCleanupFailure) as error:
            service.run(date(2026, 10, 23), uuid4(), budget)
        assert error.value.result.completed == 0
        assert plain.exists() == (failure == "register")
        assert error.value.result.plaintext_bytes_removed == (
            manifest.byte_size if failure == "confirm" else 0
        )
        assert compressed.exists() and store.read_jsonl(manifest)
    else:
        result = service.run(date(2026, 10, 23), uuid4(), budget)
        assert result.completed == 1 and not result.partial
        assert result.plaintext_bytes_removed == manifest.byte_size
    repo.fail_register = repo.fail_confirm = False
    result = service.run(date(2026, 10, 23), uuid4(), budget)
    assert repo.confirmed and not plain.exists()
    assert store.read_jsonl(manifest)
    assert result.plaintext_bytes_removed == (manifest.byte_size if failure == "register" else 0)


def test_object_limit_stops_without_querying_another_page(monkeypatch, tmp_path):
    from market_data_center import raw_cleanup_service as module

    _, manifest, _, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    repo = Repo(manifest)
    calls = 0

    def candidates(cutoff, *, after=None):
        nonlocal calls
        calls += 1
        assert calls == 1
        return RawCandidatePage((manifest,), (1, -manifest.byte_size, None, manifest.raw_id), False)

    repo.candidates = candidates
    monkeypatch.setattr(module, "MAX_RAW_OBJECTS", 1)
    result = RawCleanupService(repo, tmp_path).run(
        date(2026, 10, 23), uuid4(), CleanupBudget(monotonic() + 60)
    )
    assert result.completed == 1 and result.errors == ("raw_object_budget",)


def test_directory_sync_failure_preserves_removed_bytes_and_can_resume(monkeypatch, tmp_path):
    from market_data_center import raw_compression as module

    store, manifest, plain, _ = _raw_fixture(tmp_path)
    repo = Repo(manifest)

    def sync(path):
        raise OSError("directory fsync failed")

    monkeypatch.setattr(module, "_sync_directory", sync)
    result = RawCleanupService(repo, tmp_path).run(
        date(2026, 10, 23), uuid4(), CleanupBudget(monotonic() + 60)
    )
    assert result.partial and result.plaintext_bytes_removed == manifest.byte_size
    assert not plain.exists() and not repo.confirmed and store.read_jsonl(manifest)


@pytest.mark.parametrize("boundary", ["published", "registered"])
def test_budget_exit_between_publish_registration_and_unlink_keeps_plaintext(
    monkeypatch, tmp_path, boundary
):
    from market_data_center import raw_cleanup_service as module

    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    repo = Repo(manifest)
    budget = CleanupBudget(monotonic() + 60)
    if boundary == "published":
        publisher = module.publish_raw_gzip

        def publish(*args, **kwargs):
            result = publisher(*args, **kwargs)
            budget.deadline = 0
            return result

        monkeypatch.setattr(module, "publish_raw_gzip", publish)
    else:
        register = repo.register

        def expire(*args, **kwargs):
            register(*args, **kwargs)
            budget.deadline = 0

        repo.register = expire
    result = RawCleanupService(repo, tmp_path).run(date(2026, 10, 23), uuid4(), budget)
    assert result.errors == ("maintenance_budget_exhausted",) and result.completed == 0
    assert plain.exists() and compressed.exists() and store.read_jsonl(manifest)
    assert repo.registered == (boundary == "registered")
    monkeypatch.undo()
    if boundary == "registered":
        repo.register = register
    resumed = RawCleanupService(repo, tmp_path).run(
        date(2026, 10, 23), uuid4(), CleanupBudget(monotonic() + 60)
    )
    assert resumed.completed == 1 and repo.confirmed and not plain.exists()


def test_temp_ownership_is_registered_once_per_bounded_page(tmp_path):
    from dataclasses import replace

    _, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    second = replace(manifest, raw_id=uuid4(), object_path="second.jsonl")
    (tmp_path / second.object_path).write_bytes(plain.read_bytes())

    class PageRepo(Repo):
        batches = 0

        def candidates(self, cutoff, *, after=None):
            return RawCandidatePage((manifest, second), None, True)

        def register_temp(self, *args):
            pytest.fail("must not rewrite a growing JSON array for each of 10000 files")

        def register_temps(self, run_id, temps, now):
            self.batches += 1
            super().register_temps(run_id, temps, now)

    repo = PageRepo(manifest)
    result = RawCleanupService(repo, tmp_path).run(
        date(2026, 10, 23), uuid4(), CleanupBudget(monotonic() + 60)
    )
    assert result.completed == 2 and repo.batches == 1 and len(repo.temps) == 2
