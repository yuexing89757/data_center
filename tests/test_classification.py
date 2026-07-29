from datetime import date

from market_data_center.domain import (
    ClassificationCatalogSnapshotRecord,
    ClassificationDefinition,
    ClassificationMemberSnapshotRecord,
    ClassificationType,
    validate_catalog,
    validate_member_snapshot,
)

SNAPSHOT_DATE = date(2026, 7, 29)


def test_catalog_rejects_conflicting_duplicate_definitions() -> None:
    record = ClassificationCatalogSnapshotRecord(
        namespace="eastmoney",
        classification_type=ClassificationType.INDUSTRY,
        snapshot_date=SNAPSHOT_DATE,
        definitions=(
            ClassificationDefinition("BK0475", "银行"),
            ClassificationDefinition("BK0475", "银行业"),
        ),
        source_code="akshare",
    )

    findings = validate_catalog(record)

    assert {finding.rule_code for finding in findings} == {
        "classification.duplicate_definition",
        "classification.conflicting_definition",
    }


def test_member_snapshot_blocks_duplicates_and_unknown_security() -> None:
    record = ClassificationMemberSnapshotRecord(
        namespace="eastmoney",
        classification_type=ClassificationType.INDUSTRY,
        classification_code="BK0475",
        snapshot_date=SNAPSHOT_DATE,
        members=("SSE:600000", "SSE:600000", "SZSE:000001"),
        source_code="akshare",
    )
    key = ("eastmoney", ClassificationType.INDUSTRY, "BK0475", SNAPSHOT_DATE)

    findings = validate_member_snapshot(
        record,
        known_classifications={key},
        known_symbols={"SSE:600000"},
    )

    assert {finding.rule_code for finding in findings} == {
        "classification.duplicate_member",
        "classification.unknown_security",
    }


def test_member_snapshot_requires_same_date_catalog_definition() -> None:
    record = ClassificationMemberSnapshotRecord(
        namespace="eastmoney",
        classification_type=ClassificationType.CONCEPT,
        classification_code="BK0655",
        snapshot_date=SNAPSHOT_DATE,
        members=(),
        source_code="akshare",
    )

    findings = validate_member_snapshot(
        record,
        known_classifications=set(),
        known_symbols=set(),
    )

    assert findings[0].rule_code == "classification.unknown_definition"
