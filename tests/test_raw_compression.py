import gzip
from dataclasses import replace
from uuid import uuid4

import pytest
from test_raw_store import _raw_fixture

from market_data_center.quality_cleanup_service import CleanupInterrupted
from market_data_center.raw_compression import publish_raw_gzip, remove_verified_plaintext
from market_data_center.raw_store import RawIntegrityError


@pytest.mark.parametrize("rows", [(), ({"volume": "123", "amount": "0.1234567890123456789"},)])
def test_roundtrip_preserves_manifest_and_resume(tmp_path, rows):
    store, manifest, plain, compressed = _raw_fixture(tmp_path, rows)
    compressed.unlink()
    original = plain.read_bytes()
    representation = publish_raw_gzip(store, manifest, uuid4())
    assert plain.read_bytes() == original  # publication never removes evidence
    assert gzip.decompress(compressed.read_bytes()) == original
    assert compressed.read_bytes()[4:8] == bytes(4)  # deterministic gzip timestamp
    assert publish_raw_gzip(store, manifest, uuid4()) == representation
    assert remove_verified_plaintext(store, manifest, representation) == len(original)
    assert store.read_jsonl(manifest) == rows
    assert remove_verified_plaintext(store, manifest, representation) == 0
    assert publish_raw_gzip(store, manifest, uuid4()) == representation


@pytest.mark.parametrize(
    "field,value", [("content_sha256", "0" * 64), ("row_count", 999), ("byte_size", 0)]
)
def test_invalid_manifest_never_publishes_or_removes(tmp_path, field, value):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    with pytest.raises(RawIntegrityError):
        publish_raw_gzip(store, replace(manifest, **{field: value}), uuid4())
    assert plain.exists() and not compressed.exists()
    assert not list(tmp_path.rglob("*.tmp"))


def test_corrupt_existing_gzip_and_changed_plaintext_are_preserved(tmp_path):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.write_bytes(b"corrupt")
    with pytest.raises(RawIntegrityError):
        publish_raw_gzip(store, manifest, uuid4())
    assert plain.exists() and compressed.read_bytes() == b"corrupt"
    compressed.unlink()
    representation = publish_raw_gzip(store, manifest, uuid4())
    plain.write_bytes(b"changed")
    with pytest.raises(RawIntegrityError):
        remove_verified_plaintext(store, manifest, representation)
    assert plain.read_bytes() == b"changed"


def test_interruption_cleans_only_owned_temp_and_preserves_source(tmp_path):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    foreign = plain.with_name("foreign.tmp")
    foreign.write_bytes(b"keep")
    calls = 0

    def checkpoint():
        nonlocal calls
        calls += 1
        if calls == 3:
            raise CleanupInterrupted("budget")

    with pytest.raises(CleanupInterrupted):
        publish_raw_gzip(store, manifest, uuid4(), checkpoint=checkpoint)
    assert plain.exists() and not compressed.exists()
    assert list(tmp_path.rglob("*.tmp")) == [foreign]


def test_temp_collision_never_removes_someone_elses_file(tmp_path):
    from market_data_center.raw_compression import raw_temp_path

    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    operation = uuid4()
    temporary = tmp_path / raw_temp_path(manifest, operation)
    temporary.write_bytes(b"keep")
    with pytest.raises(FileExistsError):
        publish_raw_gzip(store, manifest, operation)
    assert temporary.read_bytes() == b"keep" and plain.exists() and not compressed.exists()


def test_gzip_expansion_is_bounded_and_cannot_replace_plaintext(tmp_path):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.write_bytes(gzip.compress(b"x" * 1_000_000))
    with pytest.raises(RawIntegrityError, match="exceeds"):
        publish_raw_gzip(store, manifest, uuid4())
    assert plain.exists()


def test_source_changed_during_stream_never_publishes(tmp_path):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    calls = 0

    def checkpoint():
        nonlocal calls
        calls += 1
        if calls == 2:
            plain.write_bytes(b"changed")

    with pytest.raises(RawIntegrityError):
        publish_raw_gzip(store, manifest, uuid4(), checkpoint=checkpoint)
    assert not compressed.exists() and plain.read_bytes() == b"changed"


def test_different_gzip_encoding_cannot_bypass_registered_hash(tmp_path):
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    compressed.unlink()
    representation = publish_raw_gzip(store, manifest, uuid4())
    compressed.write_bytes(gzip.compress(plain.read_bytes(), mtime=123))
    with pytest.raises(RawIntegrityError, match="registered"):
        remove_verified_plaintext(store, manifest, representation)
    assert plain.exists()


@pytest.mark.parametrize("change", ["remove", "truncate", "replace"])
def test_gzip_changed_during_plaintext_scan_preserves_plaintext(tmp_path, monkeypatch, change):
    from market_data_center import raw_compression as module

    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    representation = publish_raw_gzip(store, manifest, uuid4())
    verify_stream = module._verify_stream

    def change_after_verification(handle, manifest, checkpoint, sink=None):
        verify_stream(handle, manifest, checkpoint, sink)
        if getattr(handle, "name", None) == str(plain):
            if change == "remove":
                compressed.unlink()
            elif change == "truncate":
                compressed.write_bytes(b"broken")
            else:
                replacement = compressed.with_name("replacement.gz")
                replacement.write_bytes(compressed.read_bytes())
                replacement.replace(compressed)

    monkeypatch.setattr(module, "_verify_stream", change_after_verification)
    with pytest.raises((RawIntegrityError, FileNotFoundError)):
        remove_verified_plaintext(store, manifest, representation)
    assert plain.exists() and store.read_jsonl(manifest)
