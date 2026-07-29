from datetime import date
from hashlib import sha256
from pathlib import Path
from uuid import UUID

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
