import gzip
from dataclasses import replace
from datetime import date
from hashlib import sha256
from pathlib import Path
from uuid import UUID, uuid4

import pytest

from market_data_center.domain import RawFileFormat, RawManifest
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

INGESTION_ID = UUID("74b11082-4ec0-4ae4-826f-a80a96cb9985")


def test_jsonl_is_immutable_and_hash_matches_file(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    rows = [{"code": "sh.600000", "name": "浦发银行"}]

    stored = store.write_jsonl(
        provider="baostock",
        dataset="security",
        partition_date=date(2026, 7, 28),
        ingestion_id=INGESTION_ID,
        rows=rows,
        schema_version="baostock.security.v1",
    )
    raw_path = tmp_path.joinpath(*stored.object_path.split("/"))

    assert stored.content_sha256 == sha256(raw_path.read_bytes()).hexdigest()
    assert stored.row_count == 1
    assert stored.byte_size == raw_path.stat().st_size
    original_bytes = raw_path.read_bytes()
    with pytest.raises(FileExistsError):
        store.write_jsonl(
            provider="baostock",
            dataset="security",
            partition_date=date(2026, 7, 28),
            ingestion_id=INGESTION_ID,
            rows=rows,
            schema_version="baostock.security.v1",
        )
    assert raw_path.read_bytes() == original_bytes


def test_jsonl_read_verifies_manifest_before_returning_rows(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    rows = [{"code": "sh.600000", "name": "浦发银行"}]
    stored = store.write_jsonl(
        provider="baostock",
        dataset="security",
        partition_date=date(2026, 7, 28),
        ingestion_id=INGESTION_ID,
        rows=rows,
        schema_version="baostock.security.v1",
    )
    manifest = RawManifest(
        raw_id=UUID("0be27d94-e215-4c83-87c8-d3613e4b420e"),
        ingestion_id=INGESTION_ID,
        object_path=stored.object_path,
        file_format=RawFileFormat.JSONL,
        content_sha256=stored.content_sha256,
        byte_size=stored.byte_size,
        row_count=stored.row_count,
        schema_version=stored.schema_version,
    )

    assert store.read_jsonl(manifest) == tuple(rows)

    raw_path = tmp_path.joinpath(*manifest.object_path.split("/"))
    raw_path.write_bytes(raw_path.read_bytes() + b" ")
    with pytest.raises(RawIntegrityError, match="byte size"):
        store.read_jsonl(manifest)


def _raw_fixture(tmp_path: Path, rows: tuple[dict[str, str], ...] = ({"code": "sh.600000"},)):
    store = LocalRawStore(tmp_path)
    stored = store.write_jsonl(
        provider="baostock",
        dataset="security",
        partition_date=date(2026, 7, 28),
        ingestion_id=INGESTION_ID,
        rows=rows,
        schema_version="baostock.security.v1",
    )
    manifest = RawManifest(
        uuid4(),
        INGESTION_ID,
        stored.object_path,
        RawFileFormat.JSONL,
        stored.content_sha256,
        stored.byte_size,
        stored.row_count,
        stored.schema_version,
    )
    plain = tmp_path / stored.object_path
    compressed = plain.with_name(plain.name + ".gz")
    compressed.write_bytes(gzip.compress(plain.read_bytes(), mtime=0))
    return store, manifest, plain, compressed


@pytest.mark.parametrize("rows", [(), ({"code": "sh.600000", "name": "浦发银行"},)])
def test_gzip_raw_reads_using_original_manifest(tmp_path: Path, rows) -> None:
    store, manifest, plain, _ = _raw_fixture(tmp_path, rows)
    plain.unlink()

    assert store.read_jsonl(manifest) == rows


def test_compressed_logical_object_cannot_be_rewritten(tmp_path: Path) -> None:
    store, _, plain, compressed = _raw_fixture(tmp_path)
    original = compressed.read_bytes()
    plain.unlink()

    with pytest.raises(FileExistsError):
        store.write_jsonl(
            provider="baostock",
            dataset="security",
            partition_date=date(2026, 7, 28),
            ingestion_id=INGESTION_ID,
            rows=({"code": "replacement"},),
            schema_version="baostock.security.v1",
        )

    assert compressed.read_bytes() == original
    assert not plain.exists()


@pytest.mark.parametrize("damage", ["truncated", "crc", "expanded"])
def test_gzip_damage_is_rejected(tmp_path: Path, damage: str) -> None:
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    if damage == "truncated":
        compressed.write_bytes(compressed.read_bytes()[:-4])
    elif damage == "crc":
        damaged = bytearray(compressed.read_bytes())
        damaged[-8] ^= 1
        compressed.write_bytes(damaged)
    else:
        compressed.write_bytes(gzip.compress(plain.read_bytes() + b" " * 1_000_000, mtime=0))
    plain.unlink()

    with pytest.raises(RawIntegrityError):
        store.read_jsonl(manifest)


def test_corrupt_plaintext_does_not_fall_back_to_valid_gzip(tmp_path: Path) -> None:
    store, manifest, plain, _ = _raw_fixture(tmp_path)
    plain.write_bytes(plain.read_bytes().replace(b"600000", b"600001"))

    with pytest.raises(RawIntegrityError, match="SHA-256"):
        store.read_jsonl(manifest)


@pytest.mark.parametrize("field,value", [("row_count", 2), ("content_sha256", "0" * 64)])
def test_gzip_manifest_identity_is_verified(tmp_path: Path, field: str, value) -> None:
    store, manifest, plain, _ = _raw_fixture(tmp_path)
    plain.unlink()

    with pytest.raises(RawIntegrityError):
        store.read_jsonl(replace(manifest, **{field: value}))


@pytest.mark.parametrize(
    "path",
    [
        "",
        ".",
        "/outside.jsonl",
        "../outside.jsonl",
        "C:/raw.jsonl",
        "nested\\raw.jsonl",
        "./raw.jsonl",
        "nested//raw.jsonl",
    ],
)
def test_payload_reader_rejects_unsafe_logical_paths(tmp_path: Path, path: str) -> None:
    with pytest.raises(RawIntegrityError):
        LocalRawStore(tmp_path).read_payload(path, max_bytes=10)


def test_payload_reader_rejects_negative_bound(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="nonnegative"):
        LocalRawStore(tmp_path).read_payload("missing.jsonl", max_bytes=-1)


@pytest.mark.parametrize("provider", ["../escape", "C:/escape", "bad\\escape"])
def test_writer_rejects_unsafe_provider_before_creating_files(
    tmp_path: Path, provider: str, monkeypatch
) -> None:
    original_mkdir = Path.mkdir

    def guarded_mkdir(path, *args, **kwargs):
        if not path.resolve().is_relative_to(tmp_path.resolve()):
            raise AssertionError("test refuses writes outside its temporary root")
        return original_mkdir(path, *args, **kwargs)

    monkeypatch.setattr(Path, "mkdir", guarded_mkdir)
    with pytest.raises(RawIntegrityError):
        LocalRawStore(tmp_path).write_jsonl(
            provider=provider,
            dataset="security",
            partition_date=date(2026, 7, 28),
            ingestion_id=INGESTION_ID,
            rows=(),
            schema_version="baostock.security.v1",
        )
    assert list(tmp_path.iterdir()) == []


def test_plaintext_permission_error_does_not_use_gzip(tmp_path: Path, monkeypatch) -> None:
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    original_open = Path.open
    opened = []

    def guarded_open(path, *args, **kwargs):
        opened.append(path)
        if path == plain:
            raise PermissionError("private path must not reach error text")
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", guarded_open)
    with pytest.raises(RawIntegrityError, match="cannot be read") as caught:
        store.read_jsonl(manifest)
    assert compressed not in opened
    assert "private path" not in str(caught.value)


def test_reader_handles_plaintext_removed_immediately_before_open(
    tmp_path: Path, monkeypatch
) -> None:
    store, manifest, plain, _ = _raw_fixture(tmp_path)
    original_open = Path.open

    def racing_open(path, *args, **kwargs):
        if path == plain and path.exists():
            path.unlink()
        return original_open(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", racing_open)
    assert store.read_jsonl(manifest) == ({"code": "sh.600000"},)


@pytest.mark.parametrize("link_target", ["plain", "compressed", "parent"])
def test_reader_rejects_symlinks_even_within_raw_root(tmp_path: Path, link_target: str) -> None:
    store, manifest, plain, compressed = _raw_fixture(tmp_path)
    if link_target == "parent":
        original = plain.parent
        moved = original.with_name("real-day")
        original.rename(moved)
        link, target = original, moved
    else:
        link = plain if link_target == "plain" else compressed
        target = link.with_name("actual-data")
        link.rename(target)
        if link_target == "compressed":
            plain.unlink()
    try:
        link.symlink_to(target, target_is_directory=link_target == "parent")
    except OSError as error:
        pytest.skip(f"host cannot create test symlink: {type(error).__name__}")

    with pytest.raises(RawIntegrityError):
        store.read_jsonl(manifest)
