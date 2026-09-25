from __future__ import annotations

from dataclasses import replace
from datetime import UTC, date, datetime
from typing import cast
from uuid import UUID

import pytest

from market_data_center.domain.ingestion import (
    IngestionRun,
    IngestionStatus,
    QualityResult,
    RawManifest,
)
from market_data_center.domain.records import Exchange
from market_data_center.domain.regulation import (
    RegulationDirection,
    RegulationEventRecord,
    RegulationEventType,
    RegulationRuleLevel,
    RegulationSegment,
)
from market_data_center.persistence.regulation_event_postgres import (
    RegulationEventContentConflict,
)
from market_data_center.providers.contracts import ProviderBatch
from market_data_center.raw_store import StoredRawObject
from market_data_center.regulation_event_service import (
    RegulationEventCollectionError,
    RegulationEventCollectionService,
    RegulationEventCollectionSummary,
)

FROM = datetime(2026, 9, 18, 0, 0, tzinfo=UTC)
TO = datetime(2026, 9, 19, 0, 0, tzinfo=UTC)
NOW = datetime(2026, 9, 19, 1, 0, tzinfo=UTC)
INGESTION_ID = UUID("00000000-0000-0000-0000-000000000301")
RAW_ID = UUID("00000000-0000-0000-0000-000000000302")
QUALITY_IDS = tuple(UUID(int=value) for value in range(0x303, 0x320))


def _event(**overrides: object) -> RegulationEventRecord:
    values: dict[str, object] = {
        "symbol": "SSE:600000",
        "exchange": Exchange.SSE,
        "segment": RegulationSegment.SSE_MAIN,
        "event_type": RegulationEventType.ABNORMAL_VOLATILITY,
        "event_level": RegulationRuleLevel.ABNORMAL,
        "direction": RegulationDirection.UP,
        "period_start_date": date(2026, 9, 16),
        "period_end_date": date(2026, 9, 18),
        "published_at": datetime(2026, 9, 18, 0, 0, tzinfo=UTC),
        "effective_reset_date": None,
        "source_event_id": "20260918:600000:1",
        "source_title": "浦发银行异常波动",
        "source_url": "https://www.sse.com.cn/event",
        "source_content_hash": "a" * 64,
        "source_code": "sse_official",
        "explicit_rule_codes": ("SSE_MAIN_ABNORMAL_3D_DEV_UP",),
        "observed_at": datetime(2026, 9, 18, 14, 0, tzinfo=UTC),
    }
    values.update(overrides)
    return RegulationEventRecord(**values)  # type: ignore[arg-type]


class FakeProvider:
    source_code = "sse_official"

    def __init__(self, calls: list[str], records: tuple[RegulationEventRecord, ...]) -> None:
        self.calls = calls
        self.provider_records = records

    def fetch_events(self, observed_from: datetime, observed_to: datetime) -> ProviderBatch:
        self.calls.append("fetch")

        def normalize() -> tuple[RegulationEventRecord, ...]:
            self.calls.append("records")
            return self.provider_records

        return ProviderBatch(
            raw_rows=({"payload": "{}"},),
            request_params={
                "observed_from": observed_from.isoformat(),
                "observed_to": observed_to.isoformat(),
            },
            schema_version="sse.regulation_event.v1",
            record_factory=normalize,
        )


class FakeRawStore:
    def __init__(self, calls: list[str]) -> None:
        self.calls = calls

    def write_jsonl(self, **kwargs: object) -> StoredRawObject:
        self.calls.append("write_raw")
        return StoredRawObject(
            object_path="sse_official/regulation_event/2026-09-18/test.jsonl",
            content_sha256="b" * 64,
            byte_size=10,
            row_count=1,
            file_format="jsonl",
            schema_version=cast(str, kwargs["schema_version"]),
        )


class FakePersistence:
    def __init__(
        self,
        calls: list[str],
        *,
        known_symbols: frozenset[str] = frozenset({"SSE:600000"}),
        existing_hash: str | None = None,
    ) -> None:
        self.calls = calls
        self.known_symbols_value = known_symbols
        self.existing_hash = existing_hash
        self.updated_events: list[RegulationEventRecord] = []
        self.failed_run: IngestionRun | None = None
        self.quality: tuple[QualityResult, ...] = ()

    def begin_ingestion(self, run: IngestionRun) -> None:
        self.calls.append("begin")

    def attach_raw_manifest(self, run: IngestionRun, manifest: RawManifest) -> None:
        self.calls.append("attach_manifest")

    def known_stock_symbols(self, symbols: frozenset[str]) -> frozenset[str]:
        return self.known_symbols_value.intersection(symbols)

    def publish_success(
        self,
        run: IngestionRun,
        quality: tuple[QualityResult, ...],
        records: tuple[RegulationEventRecord, ...],
    ) -> RegulationEventCollectionSummary:
        self.calls.append("publish")
        if self.existing_hash is not None:
            if any(record.source_content_hash != self.existing_hash for record in records):
                raise RegulationEventContentConflict
            unchanged = len(records)
        else:
            unchanged = 0
            self.updated_events.extend(records)
        return RegulationEventCollectionSummary(
            provider_code=run.provider_code.value,
            ingestion_id=run.ingestion_id,
            status=run.status,
            observed_from=FROM,
            observed_to=TO,
            fetched_rows=run.fetched_rows,
            accepted_events=len(records),
            unchanged_events=unchanged,
        )

    def complete_failure(self, run: IngestionRun, quality: tuple[QualityResult, ...]) -> None:
        self.calls.append("failure")
        self.failed_run = run
        self.quality = quality


def _service(
    records: tuple[RegulationEventRecord, ...] = (_event(),),
    *,
    known_symbols: frozenset[str] = frozenset({"SSE:600000"}),
    existing_hash: str | None = None,
) -> tuple[RegulationEventCollectionService, FakePersistence, list[str]]:
    calls: list[str] = []
    persistence = FakePersistence(calls, known_symbols=known_symbols, existing_hash=existing_hash)
    ids = iter((INGESTION_ID, RAW_ID, *QUALITY_IDS))
    return (
        RegulationEventCollectionService(
            persistence=persistence,
            raw_store=FakeRawStore(calls),
            provider=FakeProvider(calls, records),
            clock=lambda: NOW,
            uuid_factory=ids.__next__,
        ),
        persistence,
        calls,
    )


def test_event_service_persists_raw_before_normalizing_or_publishing() -> None:
    service, _, calls = _service()

    summary = service.collect(FROM, TO)

    assert calls == ["begin", "fetch", "write_raw", "attach_manifest", "records", "publish"]
    assert summary.accepted_events == 1


def test_identical_source_content_is_idempotent() -> None:
    service, persistence, _ = _service(existing_hash="a" * 64)
    summary = service.collect(FROM, TO)
    assert summary.unchanged_events == 1
    assert persistence.updated_events == []


def test_changed_source_hash_fails_without_overwriting_existing_event() -> None:
    service, persistence, _ = _service(existing_hash="b" * 64)
    with pytest.raises(RegulationEventCollectionError, match="REG_EVENT_CONTENT_CONFLICT"):
        service.collect(FROM, TO)
    assert persistence.updated_events == []
    assert persistence.failed_run is not None
    assert persistence.failed_run.status is IngestionStatus.FAILED


def test_unknown_security_is_error_and_blocks_all_standard_facts() -> None:
    second = replace(
        _event(),
        source_event_id="20260918:600001:1",
        symbol="SSE:600001",
    )
    service, persistence, _ = _service((_event(), second))

    with pytest.raises(RegulationEventCollectionError, match="REG_EVENT_UNKNOWN_SECURITY"):
        service.collect(FROM, TO)

    assert persistence.updated_events == []
    finding = next(
        item for item in persistence.quality if item.rule_code == "REG_EVENT_UNKNOWN_SECURITY"
    )
    assert finding.blocks_core_write


@pytest.mark.parametrize(
    "record",
    [
        _event(
            symbol="SZSE:000001",
            exchange=Exchange.SZSE,
            segment=RegulationSegment.SZSE_MAIN,
            source_code="szse_official",
            explicit_rule_codes=("SZSE_MAIN_ABNORMAL_3D_DEV_UP",),
        ),
        _event(period_end_date=date(2026, 9, 20)),
    ],
)
def test_lineage_mismatch_blocks_publish(record: RegulationEventRecord) -> None:
    service, persistence, _ = _service((record,))
    with pytest.raises(RegulationEventCollectionError, match="REG_EVENT_LINEAGE_MISMATCH"):
        service.collect(FROM, TO)
    assert persistence.updated_events == []


def test_collection_requires_strict_utc_interval() -> None:
    service, _, _ = _service()
    with pytest.raises(ValueError, match="aware UTC"):
        service.collect(FROM.replace(tzinfo=None), TO)
    with pytest.raises(ValueError, match="precede"):
        service.collect(TO, FROM)
