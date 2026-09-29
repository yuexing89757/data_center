"""Versioned monitor snapshot encoding shared by Worker and the readonly RPC consumer."""

import hashlib
import json
from dataclasses import asdict, is_dataclass, replace
from datetime import date, datetime
from decimal import Decimal
from uuid import UUID

from pydantic import TypeAdapter

from market_data_center.domain.regulation import MonitorState, RegulationCalculationInput

_SOURCE = TypeAdapter(RegulationCalculationInput)
_STATE = TypeAdapter(MonitorState)


def _json_value(value: object) -> object:
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    if isinstance(value, (Decimal, date, datetime, UUID)):
        return str(value)
    raise TypeError(f"unsupported monitor snapshot type: {type(value).__name__}")


def encode_monitor(value: object) -> str:
    return json.dumps(
        value, default=_json_value, sort_keys=True, ensure_ascii=False, allow_nan=False
    )


def decode_monitor_source(value: object) -> RegulationCalculationInput:
    return _SOURCE.validate_python(value)


def decode_monitor_state(value: object) -> MonitorState:
    return _STATE.validate_python(value)


def monitor_input_hash(
    source: RegulationCalculationInput,
    states: tuple[MonitorState, ...],
    parent_id: UUID | None,
) -> str:
    stable_source = replace(
        source,
        candidates=tuple(sorted(source.candidates, key=lambda c: c.symbol)),
        active_rules=tuple(sorted(source.active_rules, key=lambda r: r.rule_code)),
    )
    payload = encode_monitor(
        {
            "source": stable_source,
            "states": tuple(sorted(states, key=lambda s: s.symbol)),
            "parent_calculation_id": parent_id,
        }
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
