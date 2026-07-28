"""Cross-entity validation that must run before persistence."""

from collections.abc import Collection, Iterable
from dataclasses import dataclass
from datetime import date
from enum import StrEnum

from market_data_center.domain.ingestion import QualitySeverity
from market_data_center.domain.records import DailyBarRecord, TradeStatus


class ValidationRule(StrEnum):
    UNKNOWN_SECURITY = "daily_bar.unknown_security"
    NON_TRADING_DATE = "daily_bar.non_trading_date"
    DUPLICATE_NATURAL_KEY = "daily_bar.duplicate_natural_key"
    UNKNOWN_TRADE_STATUS = "daily_bar.unknown_trade_status"
    SUSPENDED_WITH_VOLUME = "daily_bar.suspended_with_volume"


@dataclass(frozen=True, slots=True)
class ValidationFinding:
    rule_code: ValidationRule
    severity: QualitySeverity
    message: str
    symbol: str
    trade_date: date

    @property
    def blocks_core_write(self) -> bool:
        return self.severity is QualitySeverity.ERROR


def validate_daily_bars(
    records: Iterable[DailyBarRecord],
    *,
    known_symbols: Collection[str],
    known_trading_dates: Collection[date],
) -> list[ValidationFinding]:
    findings: list[ValidationFinding] = []
    seen: dict[tuple[str, date], DailyBarRecord] = {}

    for record in records:
        if record.symbol not in known_symbols:
            findings.append(
                _finding(record, ValidationRule.UNKNOWN_SECURITY, QualitySeverity.ERROR)
            )
        if record.trade_date not in known_trading_dates:
            findings.append(
                _finding(record, ValidationRule.NON_TRADING_DATE, QualitySeverity.ERROR)
            )

        natural_key = (record.symbol, record.trade_date)
        existing = seen.get(natural_key)
        if existing is not None and existing != record:
            findings.append(
                _finding(record, ValidationRule.DUPLICATE_NATURAL_KEY, QualitySeverity.ERROR)
            )
        else:
            seen[natural_key] = record

        if record.trade_status is TradeStatus.UNKNOWN:
            findings.append(
                _finding(record, ValidationRule.UNKNOWN_TRADE_STATUS, QualitySeverity.WARNING)
            )
        if record.trade_status is TradeStatus.SUSPENDED and record.volume not in (None, 0):
            findings.append(
                _finding(record, ValidationRule.SUSPENDED_WITH_VOLUME, QualitySeverity.WARNING)
            )

    return findings


def _finding(
    record: DailyBarRecord,
    rule_code: ValidationRule,
    severity: QualitySeverity,
) -> ValidationFinding:
    return ValidationFinding(
        rule_code=rule_code,
        severity=severity,
        message=rule_code.value,
        symbol=record.symbol,
        trade_date=record.trade_date,
    )
