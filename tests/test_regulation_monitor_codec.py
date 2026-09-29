from dataclasses import replace
from json import loads
from uuid import UUID

from test_regulation_monitor_calculator import checkpoint, source

from market_data_center.regulation_monitor_codec import (
    decode_monitor_source,
    decode_monitor_state,
    encode_monitor,
    monitor_input_hash,
)


def test_roundtrip_preserves_decimal_missing_and_checkpoint_identity():
    src = source([0, ".1"])
    state = checkpoint(src, missing_reasons=())
    assert decode_monitor_source(loads(encode_monitor(src))) == src
    assert decode_monitor_state(loads(encode_monitor(state))) == state
    with_gap = replace(state, turnover_missing_dates=(state.through_date,))
    assert decode_monitor_state(loads(encode_monitor(with_gap))) == with_gap
    assert loads(encode_monitor(src))["candidates"][0]["daily_returns"][1]["stock_return"] == "0.1"


def test_input_hash_binds_predecessor_version_and_state():
    src = source([0, ".1"])
    state = checkpoint(src, missing_reasons=())
    original = monitor_input_hash(src, (state,), UUID(int=1))
    assert original != monitor_input_hash(src, (state,), UUID(int=2))
    assert original != monitor_input_hash(src, (replace(state, complete=False),), UUID(int=1))
    assert original == monitor_input_hash(src, (state,), UUID(int=1))
