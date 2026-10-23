import gzip
from hashlib import sha256
from uuid import uuid4

import pytest

from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.raw_store import LocalRawStore, RawIntegrityError


def fixture(tmp_path):
    data = b'{"volume":"1.5"}\n'
    manifest = RawManifest(
        uuid4(),
        uuid4(),
        "old/source.jsonl",
        RawFileFormat.JSONL,
        sha256(data).hexdigest(),
        len(data),
        1,
        "source.v1",
    )
    store = LocalRawStore(tmp_path)
    path = store.resolve_path(manifest.object_path)
    path.parent.mkdir()
    path.write_bytes(data)
    return store, manifest, path, data


def test_offline_exact_removal_registers_before_unlink_and_retries(tmp_path):
    from market_data_center.raw_retirement import OfflineRawProof, remove_retired_raw

    store, manifest, path, data = fixture(tmp_path)
    compressed = store.resolve_path(manifest.object_path + ".gz")
    compressed.write_bytes(gzip.compress(data))
    expected = path.stat().st_size + compressed.stat().st_size
    proof = OfflineRawProof("archive-set", "0" * 64, manifest.content_sha256, len(data), 1)
    calls = []

    def register():
        assert path.exists() and compressed.exists()
        calls.append("register")

    def confirm():
        assert not path.exists() and not compressed.exists()
        calls.append("confirm")

    assert (
        remove_retired_raw(store, manifest, proof, register=register, confirm=confirm) == expected
    )
    assert calls == ["register", "confirm"]
    assert remove_retired_raw(store, manifest, proof, register=lambda: None, confirm=confirm) == 0


def test_bad_proof_corrupt_gzip_and_registration_failure_keep_original(tmp_path):
    from market_data_center.raw_retirement import OfflineRawProof, remove_retired_raw

    store, manifest, path, data = fixture(tmp_path)
    proof = OfflineRawProof("archive-set", "0" * 64, manifest.content_sha256, len(data), 1)

    def forbidden():
        pytest.fail("invalid files/proof must not be registered")

    bad = OfflineRawProof("archive-set", "0" * 64, "1" * 64, len(data), 1)
    with pytest.raises(RawIntegrityError):
        remove_retired_raw(store, manifest, bad, register=forbidden, confirm=forbidden)
    compressed = store.resolve_path(manifest.object_path + ".gz")
    compressed.write_bytes(b"not gzip")
    with pytest.raises(RawIntegrityError):
        remove_retired_raw(store, manifest, proof, register=forbidden, confirm=forbidden)
    assert path.read_bytes() == data
    compressed.unlink()

    def failed():
        raise RuntimeError("synthetic_registry_failure")

    with pytest.raises(RuntimeError, match="synthetic_registry_failure"):
        remove_retired_raw(store, manifest, proof, register=failed, confirm=forbidden)
    assert path.read_bytes() == data


def test_changed_file_after_registration_is_never_removed(tmp_path):
    from market_data_center.raw_retirement import OfflineRawProof, remove_retired_raw

    store, manifest, path, data = fixture(tmp_path)
    proof = OfflineRawProof("archive-set", "0" * 64, manifest.content_sha256, len(data), 1)

    def replace():
        path.write_bytes(b"changed after registration\n")

    with pytest.raises(RawIntegrityError):
        remove_retired_raw(store, manifest, proof, register=replace, confirm=lambda: None)
    assert path.exists()


def test_new_representation_and_interrupted_confirmation_are_resumable(tmp_path):
    from market_data_center.raw_retirement import OfflineRawProof, remove_retired_raw

    store, manifest, path, data = fixture(tmp_path)
    proof = OfflineRawProof("archive-set", "0" * 64, manifest.content_sha256, len(data), 1)
    compressed = store.resolve_path(manifest.object_path + ".gz")

    def publish():
        compressed.write_bytes(gzip.compress(data))

    with pytest.raises(RawIntegrityError, match="appeared"):
        remove_retired_raw(
            store,
            manifest,
            proof,
            register=publish,
            confirm=lambda: pytest.fail("unverified new file"),
        )

    def fail_confirm():
        raise RuntimeError("interrupted_confirmation")

    with pytest.raises(RuntimeError, match="interrupted_confirmation"):
        remove_retired_raw(store, manifest, proof, register=lambda: None, confirm=fail_confirm)
    path.parent.rmdir()
    assert (
        remove_retired_raw(store, manifest, proof, register=lambda: None, confirm=lambda: None) == 0
    )
    missing = LocalRawStore(tmp_path / "unavailable")
    with pytest.raises(RawIntegrityError, match="unavailable"):
        remove_retired_raw(missing, manifest, proof, register=lambda: None, confirm=lambda: None)
