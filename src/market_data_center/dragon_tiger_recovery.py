"""Bounded recovery of DragonTiger Raw objects missing database lineage."""

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from hashlib import sha256
from json import JSONDecodeError, loads
from pathlib import Path, PurePosixPath
from typing import Protocol
from uuid import UUID, uuid4

from market_data_center.domain.ingestion import (
    DatasetCode,
    IngestionRun,
    IngestionStatus,
    ProviderCode,
    QualityResult,
    QualitySeverity,
    QualityStatus,
    RawFileFormat,
    RawManifest,
)
from market_data_center.providers.eastmoney_dragon_tiger_normalizer import (
    SCHEMA_VERSION,
)
from market_data_center.raw_store import LocalRawStore, RawIntegrityError

_LEGACY_UNMARKED_SCHEMA = "eastmoney.dragon_tiger.v2"
_MAX_RAW_BYTES = 50 * 1024 * 1024
_MAX_RAW_ROWS = 100_000


@dataclass(frozen=True, slots=True)
class DragonTigerOrphanCandidate:
    ingestion_id: UUID
    object_path: str
    trade_date: date
    schema_version: str
    content_sha256: str
    byte_size: int
    row_count: int


@dataclass(frozen=True, slots=True)
class DragonTigerOrphanScan:
    candidates: tuple[DragonTigerOrphanCandidate, ...]


@dataclass(frozen=True, slots=True)
class DragonTigerOrphanRecoverySummary:
    candidate_count: int
    registered_count: int
    already_registered_count: int
    dry_run: bool


class DragonTigerOrphanPersistence(Protocol):
    def orphan_raw_state(
        self, ingestion_id: UUID, object_path: str, content_sha256: str
    ) -> str: ...

    def register_orphan_raw(
        self,
        run: IngestionRun,
        manifest: RawManifest,
        quality: Sequence[QualityResult],
    ) -> None: ...


class DragonTigerOrphanRecovery:
    def __init__(
        self,
        *,
        raw_store: LocalRawStore,
        persistence: DragonTigerOrphanPersistence,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        uuid_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._raw_store = raw_store
        self._persistence = persistence
        self._clock = clock
        self._uuid_factory = uuid_factory

    def scan(self) -> DragonTigerOrphanScan:
        dataset_root = self._raw_store.root / "eastmoney" / "dragon_tiger"
        if (
            dataset_root.is_symlink()
            or dataset_root.is_junction()
            or dataset_root.parent.is_symlink()
        ):
            raise RawIntegrityError("DragonTiger Raw root contains a link")
        if not dataset_root.is_relative_to(self._raw_store.root):
            raise RawIntegrityError("DragonTiger Raw root escapes configured storage")
        if not dataset_root.exists():
            return DragonTigerOrphanScan(())
        logical_paths = set(dataset_root.rglob("*.jsonl"))
        logical_paths.update(path.with_suffix("") for path in dataset_root.rglob("*.jsonl.gz"))
        candidates = tuple(self._inspect(path, dataset_root) for path in sorted(logical_paths))
        return DragonTigerOrphanScan(candidates)

    def register(
        self, scan: DragonTigerOrphanScan, *, dry_run: bool
    ) -> DragonTigerOrphanRecoverySummary:
        registered = 0
        already_registered = 0
        for candidate in scan.candidates:
            state = self._persistence.orphan_raw_state(
                candidate.ingestion_id,
                candidate.object_path,
                candidate.content_sha256,
            )
            if state == "already_registered":
                already_registered += 1
                continue
            if state != "unregistered":
                raise RawIntegrityError("DragonTiger orphan Raw conflicts with database lineage")
            if dry_run:
                continue
            now = self._aware_now()
            run = IngestionRun(
                ingestion_id=candidate.ingestion_id,
                provider_code=ProviderCode.EASTMONEY,
                dataset_code=DatasetCode.DRAGON_TIGER,
                status=IngestionStatus.FAILED,
                requested_at=now,
                started_at=now,
                finished_at=now,
                request_params={
                    "trade_date": candidate.trade_date.isoformat(),
                    "recovery": "orphan_raw",
                },
                fetched_rows=candidate.row_count,
                rejected_rows=candidate.row_count,
                error_summary="DT_RECOVERED_ORPHAN_RAW:recovery",
            )
            manifest = RawManifest(
                raw_id=self._uuid_factory(),
                ingestion_id=candidate.ingestion_id,
                object_path=candidate.object_path,
                file_format=RawFileFormat.JSONL,
                content_sha256=candidate.content_sha256,
                byte_size=candidate.byte_size,
                row_count=candidate.row_count,
                schema_version=candidate.schema_version,
            )
            quality = QualityResult(
                quality_result_id=self._uuid_factory(),
                ingestion_id=candidate.ingestion_id,
                dataset_code=DatasetCode.DRAGON_TIGER,
                rule_code="DT_RECOVERED_ORPHAN_RAW",
                severity=QualitySeverity.WARNING,
                status=QualityStatus.FAILED,
                message="DragonTiger Raw lineage was recovered from immutable local storage",
                natural_key={"trade_date": candidate.trade_date.isoformat()},
            )
            self._persistence.register_orphan_raw(run, manifest, (quality,))
            registered += 1
        return DragonTigerOrphanRecoverySummary(
            candidate_count=len(scan.candidates),
            registered_count=registered,
            already_registered_count=already_registered,
            dry_run=dry_run,
        )

    def _inspect(self, path: Path, dataset_root: Path) -> DragonTigerOrphanCandidate:
        resolved = path.resolve()
        if not resolved.is_relative_to(dataset_root) or not resolved.is_relative_to(
            self._raw_store.root
        ):
            raise RawIntegrityError("DragonTiger orphan Raw path escapes configured storage")
        relative = path.relative_to(self._raw_store.root)
        parts = relative.parts
        if len(parts) != 6 or parts[:2] != ("eastmoney", "dragon_tiger"):
            raise RawIntegrityError("DragonTiger orphan Raw path is invalid")
        try:
            partition_date = date(
                int(parts[2].removeprefix("year=")),
                int(parts[3].removeprefix("month=")),
                int(parts[4].removeprefix("day=")),
            )
            ingestion_id = UUID(Path(parts[5]).stem)
        except (ValueError, TypeError) as error:
            raise RawIntegrityError("DragonTiger orphan Raw partition is invalid") from error
        if (
            not parts[2].startswith("year=")
            or not parts[3].startswith("month=")
            or not parts[4].startswith("day=")
        ):
            raise RawIntegrityError("DragonTiger orphan Raw partition is invalid")
        payload = self._raw_store.read_payload(relative.as_posix(), max_bytes=_MAX_RAW_BYTES)
        if not payload or len(payload) > _MAX_RAW_BYTES:
            raise RawIntegrityError("DragonTiger orphan Raw byte bound is invalid")
        lines = payload.splitlines()
        if not lines or len(lines) > _MAX_RAW_ROWS:
            raise RawIntegrityError("DragonTiger orphan Raw row bound is invalid")
        trade_dates: set[date] = set()
        schema_markers: set[str] = set()
        try:
            for line in lines:
                row = loads(line)
                if not isinstance(row, dict) or not all(
                    isinstance(key, str) and isinstance(value, str) for key, value in row.items()
                ):
                    raise RawIntegrityError("DragonTiger orphan Raw row is not a string mapping")
                marker = row.get("raw_schema_version")
                if marker:
                    schema_markers.add(marker)
                source = loads(row["payload_json"])
                if not isinstance(source, dict):
                    raise RawIntegrityError("DragonTiger orphan payload is not an object")
                value = source.get("TRADE_DATE")
                if not isinstance(value, str):
                    raise RawIntegrityError("DragonTiger orphan trade date is missing")
                trade_dates.add(date.fromisoformat(value[:10]))
        except (JSONDecodeError, UnicodeDecodeError, KeyError, ValueError) as error:
            raise RawIntegrityError("DragonTiger orphan Raw contains invalid JSONL") from error
        if len(trade_dates) != 1 or next(iter(trade_dates)) != partition_date:
            raise RawIntegrityError("DragonTiger orphan Raw partition date does not match payload")
        if not schema_markers:
            schema_version = _LEGACY_UNMARKED_SCHEMA
        elif schema_markers == {SCHEMA_VERSION}:
            schema_version = SCHEMA_VERSION
        else:
            raise RawIntegrityError("DragonTiger orphan Raw schema is unsupported")
        return DragonTigerOrphanCandidate(
            ingestion_id=ingestion_id,
            object_path=PurePosixPath(*relative.parts).as_posix(),
            trade_date=partition_date,
            schema_version=schema_version,
            content_sha256=sha256(payload).hexdigest(),
            byte_size=len(payload),
            row_count=len(lines),
        )

    def _aware_now(self) -> datetime:
        value = self._clock()
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("DragonTiger recovery clock must be timezone-aware")
        return value.astimezone(UTC)
