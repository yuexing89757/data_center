import gzip
from collections.abc import Sequence
from datetime import UTC, date, datetime
from json import dumps
from pathlib import Path
from uuid import UUID

import pytest

from market_data_center.domain.ingestion import IngestionRun, QualityResult, RawManifest
from market_data_center.dragon_tiger_recovery import DragonTigerOrphanRecovery
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

INGESTION_ID = UUID("00000000-0000-0000-0000-000000000201")
NOW = datetime(2026, 9, 8, 8, tzinfo=UTC)


class Persistence:
    def __init__(self) -> None:
        self.registered: list[tuple[IngestionRun, RawManifest, Sequence[QualityResult]]] = []
        self.known: set[UUID] = set()

    def orphan_raw_state(self, ingestion_id: UUID, object_path: str, content_sha256: str) -> str:
        del object_path, content_sha256
        return "already_registered" if ingestion_id in self.known else "unregistered"

    def register_orphan_raw(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
    ) -> None:
        self.registered.append((run, manifest, quality))
        self.known.add(run.ingestion_id)


def _raw_rows(schema_version: str = "eastmoney.dragon_tiger.v3") -> tuple[dict[str, str], ...]:
    payload = dumps(
        {
            "TRADE_ID": "event-1",
            "SECUCODE": "600000.SH",
            "TRADE_DATE": "2025-01-03 00:00:00",
        },
        sort_keys=True,
    )
    return (
        {
            "raw_schema_version": schema_version,
            "record_kind": "summary",
            "source_page": "1",
            "source_index": "0",
            "payload_json": payload,
        },
    )


def test_orphan_recovery_dry_run_then_registers_once(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    store.write_jsonl(
        provider="eastmoney",
        dataset="dragon_tiger",
        partition_date=date(2025, 1, 3),
        ingestion_id=INGESTION_ID,
        rows=_raw_rows(),
        schema_version="eastmoney.dragon_tiger.v3",
    )
    persistence = Persistence()
    recovery = DragonTigerOrphanRecovery(
        raw_store=store,
        persistence=persistence,
        clock=lambda: NOW,
    )

    scan = recovery.scan()
    dry_run = recovery.register(scan, dry_run=True)
    executed = recovery.register(scan, dry_run=False)
    repeated = recovery.register(scan, dry_run=False)

    assert len(scan.candidates) == 1
    assert scan.candidates[0].trade_date == date(2025, 1, 3)
    assert dry_run.candidate_count == 1
    assert persistence.registered[0][1].content_sha256 == scan.candidates[0].content_sha256
    assert persistence.registered[0][1].row_count == 1
    assert persistence.registered[0][2][0].rule_code == "DT_RECOVERED_ORPHAN_RAW"
    assert executed.registered_count == 1
    assert repeated.already_registered_count == 1


def test_orphan_scan_rejects_unknown_schema_marker(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    store.write_jsonl(
        provider="eastmoney",
        dataset="dragon_tiger",
        partition_date=date(2025, 1, 3),
        ingestion_id=INGESTION_ID,
        rows=_raw_rows("eastmoney.dragon_tiger.v99"),
        schema_version="eastmoney.dragon_tiger.v99",
    )

    with pytest.raises(RawIntegrityError, match="schema"):
        DragonTigerOrphanRecovery(raw_store=store, persistence=Persistence()).scan()


def test_orphan_scan_rejects_partition_date_mismatch(tmp_path: Path) -> None:
    store = LocalRawStore(tmp_path)
    store.write_jsonl(
        provider="eastmoney",
        dataset="dragon_tiger",
        partition_date=date(2025, 1, 2),
        ingestion_id=INGESTION_ID,
        rows=_raw_rows(),
        schema_version="eastmoney.dragon_tiger.v3",
    )

    with pytest.raises(RawIntegrityError, match="partition"):
        DragonTigerOrphanRecovery(raw_store=store, persistence=Persistence()).scan()


@pytest.mark.parametrize("keep_plain", [False, True])
def test_orphan_scan_keeps_one_logical_identity_for_gzip(tmp_path: Path, keep_plain: bool) -> None:
    store = LocalRawStore(tmp_path)
    stored = store.write_jsonl(
        provider="eastmoney",
        dataset="dragon_tiger",
        partition_date=date(2025, 1, 3),
        ingestion_id=INGESTION_ID,
        rows=_raw_rows(),
        schema_version="eastmoney.dragon_tiger.v3",
    )
    plain = tmp_path / stored.object_path
    compressed = plain.with_name(plain.name + ".gz")
    compressed.write_bytes(gzip.compress(plain.read_bytes(), mtime=0))
    if not keep_plain:
        plain.unlink()
    recovery = DragonTigerOrphanRecovery(
        raw_store=store, persistence=Persistence(), clock=lambda: NOW
    )

    scan = recovery.scan()

    assert len(scan.candidates) == 1
    candidate = scan.candidates[0]
    assert candidate.object_path == stored.object_path
    assert candidate.content_sha256 == stored.content_sha256
    assert candidate.byte_size == stored.byte_size
    assert candidate.row_count == stored.row_count
    assert compressed.exists()
