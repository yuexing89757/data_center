from contextlib import contextmanager
from dataclasses import replace
from datetime import UTC, datetime
from uuid import uuid4

import pytest
from test_stock_volume_average import DAYS, source

from market_data_center.stock_volume_average_service import StockVolumeAverageService


class MemoryPersistence:
    def __init__(self):
        self.source = source()
        self.trading_day = True
        self.runs = {}
        self.publications = []
        self.failed = []
        self.fail_publish = False
        self.locked = False
        self.evidence = {}
        self.publication_evidence = []

    def is_trading_day(self, trade_date):
        return self.trading_day

    @contextmanager
    def build_lock(self, trade_date):
        assert not self.locked
        self.locked = True
        try:
            yield
        finally:
            self.locked = False

    def load_input(self, trade_date):
        assert self.locked
        return self.source, {}, self.evidence

    def existing_calculation(self, trade_date, input_hash):
        return next((key for key, run in self.runs.items() if run.input_hash == input_hash), None)

    def current_publication(self, trade_date):
        return self.publications[-1] if self.publications else None

    def create_calculation_run(self, run):
        self.pending = run

    def publish(
        self, run, calculation_id, trade_date, calculation, evidence, published_at, **metadata
    ):
        assert self.locked
        if self.fail_publish:
            raise RuntimeError("synthetic publication failure")
        if run is not None:
            self.runs[calculation_id] = run
        version = len(self.publications) + 1
        self.publications.append((calculation_id, version, metadata.get("evidence_hash")))
        self.publication_evidence.append(evidence)
        return version

    def fail_calculation_run(self, run):
        self.failed.append(run)


def service(persistence):
    return StockVolumeAverageService(
        persistence, clock=lambda: datetime(2026, 11, 4, 14, tzinfo=UTC), uuid_factory=uuid4
    )


def test_first_publication_idempotence_revision_and_return_to_old_input():
    persistence = MemoryPersistence()
    builder = service(persistence)
    first = builder.build(DAYS[-1])
    again = builder.build(DAYS[-1])
    assert first.status == "succeeded"
    assert again.status == "unchanged"
    assert again.version == first.version == 1
    persistence.source = source((1, 2, 3, 4, 5))
    revised = builder.build(DAYS[-1])
    assert revised.version == 2
    assert revised.calculation_id != first.calculation_id
    persistence.source = source()
    restored = builder.build(DAYS[-1])
    assert restored.status == "published/reused"
    assert restored.version == 3
    assert restored.calculation_id == first.calculation_id
    assert len(persistence.runs) == 2
    assert builder.build(DAYS[-1]).version == 3


def test_holiday_skips_unknown_calendar_fails_and_partial_is_preserved():
    persistence = MemoryPersistence()
    persistence.trading_day = False
    assert service(persistence).build(DAYS[-1]).status == "skipped"
    assert not persistence.publications
    persistence.trading_day = None
    with pytest.raises(RuntimeError, match="calendar"):
        service(persistence).build(DAYS[-1])
    persistence.trading_day = True
    persistence.source = source((None, 1, 2, 3, 4))
    result = service(persistence).build(DAYS[-1])
    assert result.candidate_count == 1
    assert result.complete_count == 0


def test_failed_publish_keeps_previous_publication_and_records_failed_run():
    persistence = MemoryPersistence()
    builder = service(persistence)
    original = builder.build(DAYS[-1])
    persistence.source = source((0, 0, 0, 0, 0))
    persistence.fail_publish = True
    with pytest.raises(RuntimeError):
        builder.build(DAYS[-1])
    assert len(persistence.publications) == 1
    assert persistence.publications[0][:2] == (original.calculation_id, 1)
    assert len(persistence.failed) == 1
    assert persistence.failed[0].output_rows == 0
    assert "synthetic" not in persistence.failed[0].error_summary


def test_dependency_failure_does_not_publish_a_batch():
    persistence = MemoryPersistence()
    persistence.source = replace(source(), stocks=())
    with pytest.raises(ValueError):
        service(persistence).build(DAYS[-1])
    assert not persistence.publications


def test_revocation_and_same_value_new_source_preserve_current_publication_evidence():
    persistence = MemoryPersistence()
    persistence.evidence = {"SSE:600000": [{"ingestion_id": "I1", "certified": True}]}
    builder = service(persistence)
    original = builder.build(DAYS[-1])
    persistence.source = source((None, 200, 300, 400, 501))
    persistence.evidence = {"SSE:600000": [{"ingestion_id": "I1", "certified": False}]}
    assert builder.build(DAYS[-1]).version == 2
    persistence.source = source()
    persistence.evidence = {"SSE:600000": [{"ingestion_id": "I2", "certified": True}]}
    restored = builder.build(DAYS[-1])
    assert restored.calculation_id == original.calculation_id
    assert restored.version == 3
    persistence.evidence = {"SSE:600000": [{"ingestion_id": "I3", "certified": True}]}
    changed_proof = builder.build(DAYS[-1])
    assert changed_proof.status == "published/reused"
    assert changed_proof.version == 4
    assert changed_proof.calculation_id == original.calculation_id
    assert persistence.publication_evidence[-1] == persistence.evidence
    assert builder.build(DAYS[-1]).status == "unchanged"
    assert len(persistence.runs) == 2
