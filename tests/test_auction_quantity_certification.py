from copy import deepcopy
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from test_pytdx_hq_provider import RecordedClient

from market_data_center.auction_quantity_certification import prove_records
from market_data_center.domain.call_auction_market_series import universe_hash
from market_data_center.providers.pytdx_hq import normalize_raw_security_quote


def fixture():
    ingestion_id, session_id = uuid4(), uuid4()
    scheduled = datetime(2026, 10, 23, 1, 25, 20, tzinfo=UTC)
    session = dict(
        session_id=session_id,
        trade_date=date(2026, 10, 23),
        universe_symbols=["SSE:600000"],
        universe_count=1,
        universe_hash=universe_hash(("SSE:600000",)),
    )
    run = dict(
        ingestion_id=ingestion_id,
        accepted_rows=1,
        fetched_rows=1,
        rejected_rows=0,
        started_at=scheduled,
        finished_at=scheduled,
        request_params=dict(
            session_id=str(session_id),
            sample_seq=31,
            trade_date="2026-10-23",
            scheduled_at=scheduled.isoformat(),
            expected_rows=1,
            endpoint="fixture:7709",
        ),
    )
    round_state = dict(sample_seq=31, scheduled_at=scheduled)
    raw = {k: str(v) for k, v in RecordedClient()._row(1, "600000").items()}
    from json import dumps

    rows = [
        dict(
            ingestion_id=str(ingestion_id),
            session_id=str(session_id),
            sample_seq="31",
            scheduled_at=scheduled.isoformat(),
            trade_date="2026-10-23",
            attempt_number="1",
            endpoint="fixture:7709",
            worker_observed_at=scheduled.isoformat(),
            provider_schema_version="pytdx_hq.security_quotes.v1",
            provider_raw_json=dumps(raw),
        )
    ]
    # Independent expected storage projection: 123 lots and bid1 one lot.
    fact = dict(
        ingestion_id=ingestion_id,
        session_id=session_id,
        sample_seq=31,
        scheduled_at=scheduled,
        observed_at=scheduled,
        trade_date=date(2026, 10, 23),
        batch_code="092520",
        symbol="SSE:600000",
        source_code="pytdx_hq",
        value_semantics="opening_trade",
        last_price=Decimal("10"),
        previous_close=Decimal("9.50"),
        high_price=Decimal("10"),
        low_price=Decimal("10"),
        cumulative_volume=12300,
        cumulative_amount=Decimal("123000"),
    )
    for level in range(1, 6):
        fact[f"bid{level}_price"] = Decimal("10") - Decimal(level - 1) / 100
        fact[f"ask{level}_price"] = Decimal("10.01") + Decimal(level - 1) / 100
        fact[f"bid{level}_volume"] = level * 100
        fact[f"ask{level}_volume"] = (level + 5) * 100
    return session, run, round_state, rows, [fact]


def test_native_quote_raw_normalization_keeps_exact_shares():
    raw = {k: str(v) for k, v in RecordedClient()._row(1, "600000").items()}
    quote = normalize_raw_security_quote(
        raw,
        schema_version="pytdx_hq.security_quotes.v1",
        observed_at=datetime(2026, 10, 23, 1, 25, 20, tzinfo=UTC),
    )
    assert quote.cumulative_volume == 12300
    assert quote.bid_levels[0].volume == 100
    raw["volume_lots"] = "0.37"
    with pytest.raises(ValueError):
        normalize_raw_security_quote(
            raw, schema_version="pytdx_hq.security_quotes.v1", observed_at=quote.observed_at
        )


def test_proof_and_storage_rounding_are_exact():
    args = fixture()
    assert prove_records(*args)["verified_rows"] == 1
    import json

    raw = json.loads(args[3][0]["provider_raw_json"])
    raw["amount"] = "123000.00005"
    args[3][0]["provider_raw_json"] = json.dumps(raw)
    args[4][0]["cumulative_amount"] = Decimal("123000.0001")
    assert prove_records(*args)["verified_rows"] == 1


@pytest.mark.parametrize(
    "mutation", ["quantity", "lineage", "version", "timestamp", "duplicate", "missing", "universe"]
)
def test_proof_fails_closed(mutation):
    session, run, round_state, rows, facts = fixture()
    if mutation == "quantity":
        facts[0]["bid1_volume"] = 1
    if mutation == "lineage":
        rows[0]["ingestion_id"] = str(uuid4())
    if mutation == "version":
        rows[0]["provider_schema_version"] = "unknown"
    if mutation == "timestamp":
        rows[0]["worker_observed_at"] = "2026-10-23T02:25:20+00:00"
    if mutation == "duplicate":
        rows.append(deepcopy(rows[0]))
        run["fetched_rows"] = 2
    if mutation == "missing":
        facts.clear()
    if mutation == "universe":
        session["universe_hash"] = "0" * 64
    with pytest.raises(ValueError):
        prove_records(session, run, round_state, rows, facts)
