import gzip
import json
from dataclasses import replace
from datetime import UTC, datetime
from hashlib import sha256
from uuid import UUID, uuid4

import pytest


def test_invalid_gzip_deflate_is_an_integrity_error(tmp_path):
    from market_data_center.quality_archive import read_quality_archive, write_quality_archive
    from market_data_center.raw_store import RawIntegrityError

    archive = write_quality_archive(tmp_path, group(), (row(),), operation_id=uuid4())
    malformed = bytes.fromhex("1f8b0800000000000003") + b"\x07" + b"\x00" * 8
    (tmp_path / archive.object_path).write_bytes(malformed)
    archive = replace(
        archive, compressed_bytes=len(malformed), compressed_sha256=sha256(malformed).hexdigest()
    )
    with pytest.raises(RawIntegrityError):
        read_quality_archive(tmp_path, archive)


INGESTION = UUID("00000000-0000-0000-0000-000000000002")
RULE = "realtime_quote.lot_precision"


def test_synthetic_archive_volume_roundtrip(tmp_path):
    import tracemalloc
    from time import perf_counter

    from market_data_center.quality_archive import read_quality_archive, write_quality_archive

    rows = tuple(
        row(quality_result_id=str(UUID(int=i + 1)), details={"original": "x" * 256})
        for i in range(10_000)
    )
    candidate = replace(group(), row_count=len(rows))
    tracemalloc.start()
    start = perf_counter()
    try:
        archive = write_quality_archive(tmp_path, candidate, rows, operation_id=uuid4())
        assert read_quality_archive(tmp_path, archive) == rows
        _, peak = tracemalloc.get_traced_memory()
        assert peak < 64 * 1024**2
    finally:
        tracemalloc.stop()
    print(
        {
            "rows": len(rows),
            "source_bytes": archive.content_bytes,
            "compressed_bytes": archive.compressed_bytes,
            "peak_traced_bytes": peak,
            "elapsed_seconds": round(perf_counter() - start, 3),
        }
    )


def row(**overrides):
    return json.dumps(
        {
            "quality_result_id": "00000000-0000-0000-0000-000000000001",
            "ingestion_id": str(INGESTION),
            "dataset_code": "call_auction_market_series",
            "rule_code": RULE,
            "severity": "info",
            "status": "failed",
            "message": "非整数手数",
            "natural_key": {"symbol": "SSE:600000"},
            "details": {"original": None},
            "created_at": "2026-07-01T00:00:00+00:00",
        }
        | overrides,
        ensure_ascii=False,
        sort_keys=True,
    )


def group():
    from market_data_center.quality_archive import QualityArchiveGroup

    when = datetime(2026, 7, 1, tzinfo=UTC)
    return QualityArchiveGroup(INGESTION, RULE, 1, when, when)


def test_complete_archive_roundtrip_and_idempotent_publish(tmp_path):
    from market_data_center.quality_archive import read_quality_archive, write_quality_archive

    source = row().replace('"original": null', '"original": 1.234567890123456789')
    archive = write_quality_archive(tmp_path, group(), (source,), operation_id=uuid4())
    assert read_quality_archive(tmp_path, archive) == (source,)
    repeated = write_quality_archive(tmp_path, group(), (source,), operation_id=uuid4())
    assert repeated.object_path == archive.object_path
    assert repeated.content_sha256 == archive.content_sha256
    assert (
        gzip.decompress((tmp_path / archive.object_path).read_bytes()) == (source + "\n").encode()
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"severity": "error"},
        {"dataset_code": "security"},
        {"details": {"aggregation_version": "v1"}},
        {"rule_code": "other"},
        {"ingestion_id": str(uuid4())},
    ],
)
def test_archive_rejects_non_whitelisted_rows(tmp_path, changes):
    from market_data_center.quality_archive import write_quality_archive
    from market_data_center.raw_store import RawIntegrityError

    with pytest.raises(RawIntegrityError):
        write_quality_archive(tmp_path, group(), (row(**changes),), operation_id=uuid4())


def test_archive_rejects_corruption_missing_and_escape(tmp_path):
    from market_data_center.quality_archive import read_quality_archive, write_quality_archive
    from market_data_center.raw_store import RawIntegrityError

    archive = write_quality_archive(tmp_path, group(), (row(),), operation_id=uuid4())
    path = tmp_path / archive.object_path
    path.write_bytes(b"corrupt")
    with pytest.raises(RawIntegrityError):
        read_quality_archive(tmp_path, archive)
    with pytest.raises(RawIntegrityError):
        write_quality_archive(tmp_path, group(), (row(),), operation_id=uuid4())
    path.unlink()
    with pytest.raises(RawIntegrityError):
        read_quality_archive(tmp_path, archive)
    with pytest.raises(RawIntegrityError):
        read_quality_archive(tmp_path, replace(archive, object_path="../escape"))


def test_archive_cancellation_never_publishes_unverified_file(tmp_path):
    from market_data_center.quality_archive import write_quality_archive

    def stop():
        raise TimeoutError("budget")

    with pytest.raises(TimeoutError):
        write_quality_archive(tmp_path, group(), (row(),), operation_id=uuid4(), checkpoint=stop)
    assert not list(tmp_path.rglob("*.jsonl.gz"))


def test_archive_syncs_new_ancestor_directories_before_return(tmp_path, monkeypatch):
    from market_data_center import quality_archive as module

    synced = []
    monkeypatch.setattr(module, "_sync_directory", lambda path: synced.append(path.absolute()))
    archive = module.write_quality_archive(tmp_path, group(), (row(),), operation_id=uuid4())
    for parent in (tmp_path / archive.object_path).parents:
        if parent == tmp_path.parent:
            break
        assert parent.absolute() in synced


def test_archive_rejects_duplicate_ids_and_wrong_timestamp(tmp_path):
    from market_data_center.quality_archive import write_quality_archive
    from market_data_center.raw_store import RawIntegrityError

    with pytest.raises(RawIntegrityError):
        write_quality_archive(
            tmp_path, replace(group(), row_count=2), (row(), row()), operation_id=uuid4()
        )
    with pytest.raises(RawIntegrityError):
        write_quality_archive(
            tmp_path, group(), (row(created_at="2026-07-02T00:00:00+00:00"),), operation_id=uuid4()
        )
