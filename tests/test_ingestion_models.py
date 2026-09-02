from datetime import UTC, datetime
from uuid import uuid4

import pytest

from market_data_center.domain import (
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


def test_terminal_ingestion_requires_finished_at() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValueError, match="requires finished_at"):
        IngestionRun(
            ingestion_id=uuid4(),
            provider_code=ProviderCode.BAOSTOCK,
            dataset_code=DatasetCode.SECURITY,
            status=IngestionStatus.SUCCEEDED,
            requested_at=now,
            started_at=now,
        )


def test_pysnowball_is_a_distinct_provider_identity() -> None:
    assert ProviderCode("pysnowball") is ProviderCode.PYSNOWBALL


def test_dragon_tiger_is_a_distinct_dataset_identity() -> None:
    assert DatasetCode("dragon_tiger") is DatasetCode.DRAGON_TIGER


def test_raw_manifest_rejects_path_traversal() -> None:
    with pytest.raises(ValueError, match="safe relative"):
        RawManifest(
            raw_id=uuid4(),
            ingestion_id=uuid4(),
            object_path="../secret.jsonl",
            file_format=RawFileFormat.JSONL,
            content_sha256="a" * 64,
            byte_size=1,
            row_count=1,
            schema_version="1",
        )


def test_failed_error_quality_result_blocks_core_write() -> None:
    result = QualityResult(
        quality_result_id=uuid4(),
        ingestion_id=uuid4(),
        dataset_code=DatasetCode.DAILY_BAR,
        rule_code="daily_bar.non_trading_date",
        severity=QualitySeverity.ERROR,
        status=QualityStatus.FAILED,
        message="date is not a trading day",
    )

    assert result.blocks_core_write
