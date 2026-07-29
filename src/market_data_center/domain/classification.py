"""Versioned classification catalogs and complete membership snapshots."""

from collections import Counter
from collections.abc import Collection
from dataclasses import dataclass
from datetime import date
from enum import StrEnum


class ClassificationType(StrEnum):
    INDUSTRY = "industry"
    CONCEPT = "concept"
    INDEX = "index"


@dataclass(frozen=True, slots=True)
class ClassificationDefinition:
    code: str
    name: str
    level: int = 1
    parent_code: str | None = None

    def __post_init__(self) -> None:
        if not self.code.strip() or not self.name.strip():
            raise ValueError("classification code and name must not be blank")
        if self.level < 1:
            raise ValueError("classification level must be positive")
        if self.parent_code == self.code:
            raise ValueError("classification cannot be its own parent")


@dataclass(frozen=True, slots=True)
class ClassificationCatalogSnapshotRecord:
    namespace: str
    classification_type: ClassificationType
    snapshot_date: date
    definitions: tuple[ClassificationDefinition, ...]
    source_code: str

    def __post_init__(self) -> None:
        if not self.namespace.strip():
            raise ValueError("classification namespace must not be blank")


@dataclass(frozen=True, slots=True)
class ClassificationMemberSnapshotRecord:
    namespace: str
    classification_type: ClassificationType
    classification_code: str
    snapshot_date: date
    members: tuple[str, ...]
    source_code: str

    def __post_init__(self) -> None:
        if not self.namespace.strip() or not self.classification_code.strip():
            raise ValueError("classification identity must not be blank")


type ClassificationRecord = ClassificationCatalogSnapshotRecord | ClassificationMemberSnapshotRecord


@dataclass(frozen=True, slots=True)
class ClassificationFinding:
    rule_code: str
    message: str
    natural_key: dict[str, object]


def validate_catalog(
    record: ClassificationCatalogSnapshotRecord,
) -> tuple[ClassificationFinding, ...]:
    findings: list[ClassificationFinding] = []
    by_code: dict[str, ClassificationDefinition] = {}
    for definition in record.definitions:
        previous = by_code.get(definition.code)
        if previous is not None:
            findings.append(
                ClassificationFinding(
                    "classification.duplicate_definition",
                    "catalog snapshot contains a duplicate classification code",
                    _catalog_key(record, definition.code),
                )
            )
            if previous != definition:
                findings.append(
                    ClassificationFinding(
                        "classification.conflicting_definition",
                        "duplicate classification definitions have conflicting fields",
                        _catalog_key(record, definition.code),
                    )
                )
        by_code[definition.code] = definition
    for definition in record.definitions:
        if definition.parent_code is not None and definition.parent_code not in by_code:
            findings.append(
                ClassificationFinding(
                    "classification.unknown_parent",
                    "classification parent is absent from the complete catalog snapshot",
                    _catalog_key(record, definition.code),
                )
            )
    return tuple(findings)


def validate_member_snapshot(
    record: ClassificationMemberSnapshotRecord,
    *,
    known_classifications: Collection[tuple[str, ClassificationType, str, date]],
    known_symbols: Collection[str],
) -> tuple[ClassificationFinding, ...]:
    key = (
        record.namespace,
        record.classification_type,
        record.classification_code,
        record.snapshot_date,
    )
    natural_key = _catalog_key(record, record.classification_code)
    findings: list[ClassificationFinding] = []
    if key not in known_classifications:
        findings.append(
            ClassificationFinding(
                "classification.unknown_definition",
                "member snapshot has no matching catalog definition snapshot",
                natural_key,
            )
        )
    duplicates = sorted(symbol for symbol, count in Counter(record.members).items() if count > 1)
    if duplicates:
        findings.append(
            ClassificationFinding(
                "classification.duplicate_member",
                "member snapshot contains duplicate Security symbols",
                {**natural_key, "symbols": duplicates},
            )
        )
    unknown = sorted(set(record.members).difference(known_symbols))
    if unknown:
        findings.append(
            ClassificationFinding(
                "classification.unknown_security",
                "member snapshot references unknown Security symbols",
                {**natural_key, "symbols": unknown},
            )
        )
    return tuple(findings)


def _catalog_key(
    record: ClassificationCatalogSnapshotRecord | ClassificationMemberSnapshotRecord,
    code: str,
) -> dict[str, object]:
    return {
        "namespace": record.namespace,
        "classification_type": record.classification_type.value,
        "classification_code": code,
        "snapshot_date": record.snapshot_date.isoformat(),
    }
