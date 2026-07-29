"""Provider-neutral Capital validation and natural keys."""

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from datetime import date

from market_data_center.domain.records import (
    CapitalRecord,
    DistributionRecord,
    ShareCapitalRecord,
)


@dataclass(frozen=True, slots=True)
class CapitalFinding:
    rule_code: str
    message: str
    natural_key: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class CapitalValidationResult:
    accepted: tuple[CapitalRecord, ...]
    findings: tuple[CapitalFinding, ...]
    rejected_rows: int


def capital_natural_key(record: CapitalRecord) -> tuple[str, str, date]:
    if isinstance(record, ShareCapitalRecord):
        return "share_capital", record.symbol, record.effective_date
    if isinstance(record, DistributionRecord):
        return "distribution", record.symbol, record.report_period
    return "rights_issue", record.symbol, record.record_date


def capital_natural_key_json(record: CapitalRecord) -> dict[str, object]:
    record_type, symbol, effective_date = capital_natural_key(record)
    return {
        "record_type": record_type,
        "symbol": symbol,
        "effective_date": effective_date.isoformat(),
    }


def validate_capital(
    records: Sequence[CapitalRecord], *, known_symbols: Collection[str]
) -> CapitalValidationResult:
    grouped: dict[tuple[str, str, date], list[CapitalRecord]] = {}
    for record in records:
        grouped.setdefault(capital_natural_key(record), []).append(record)

    findings: list[CapitalFinding] = []
    accepted: list[CapitalRecord] = []
    rejected_rows = 0
    for grouped_records in grouped.values():
        record = grouped_records[0]
        natural_key = capital_natural_key_json(record)
        if record.symbol not in known_symbols:
            rejected_rows += len(grouped_records)
            findings.append(
                CapitalFinding(
                    rule_code="capital.unknown_symbol",
                    message="Capital fact references an unknown Security symbol",
                    natural_key=natural_key,
                )
            )
            continue
        if any(candidate != record for candidate in grouped_records[1:]):
            rejected_rows += len(grouped_records)
            findings.append(
                CapitalFinding(
                    rule_code="capital.conflicting_duplicate",
                    message="Capital batch contains conflicting facts for one natural key",
                    natural_key=natural_key,
                )
            )
            continue
        accepted.append(record)
    return CapitalValidationResult(tuple(accepted), tuple(findings), rejected_rows)
