"""Certify unchanged auction facts only after complete, exact native-Raw proof.

This is not collection or generic replay: absent/conflicting facts remain gaps.
Each plan covers one terminal attempt and both hot/history copies, bounded at
6000 source rows. Only its certificate and audit run are written, never facts/Raw.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import ROUND_HALF_UP, Decimal, localcontext
from json import dumps, loads
from types import SimpleNamespace
from typing import Any, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from sqlalchemy import Connection, Engine, text

from market_data_center.call_auction_market_series_service import _to_snapshot
from market_data_center.domain.call_auction_market_series import (
    MarketSeriesRound,
    MarketSeriesSession,
    series_slots,
    universe_hash,
)
from market_data_center.domain.ingestion import RawFileFormat, RawManifest
from market_data_center.persistence.call_auction_market_series_postgres import _snapshot_parameters
from market_data_center.providers.pytdx_hq import normalize_raw_security_quote
from market_data_center.providers.quantity_units import (
    NORMALIZER_VERSION,
    UNIT_CONTRACT_VERSION,
    quantity_group_rules,
    validate_quantity_metadata,
)
from market_data_center.quantity_certification import write_quantity_certificates
from market_data_center.quantity_unit_repair import _database_identity, fingerprint
from market_data_center.raw_store import LocalRawStore

RULE = "auction-existing-native-proof.v1"
SCHEMA = "market_data_center.call_auction_market_series.raw.v2"
GROUP = "call_auction_market_series.quantities"
TABLES = (
    "realtime.call_auction_market_series_snapshot",
    "realtime.call_auction_market_series_snapshot_history",
)
BOUND = 6000


def prove_records(
    session: Mapping[str, Any],
    run: Mapping[str, Any],
    round_state: Mapping[str, Any],
    rows: Sequence[Mapping[str, str]],
    facts: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Reproduce the accepted set, all quantities, amounts, and other stored fields."""
    symbols = tuple(session["universe_symbols"])
    if (
        session["universe_hash"] != universe_hash(symbols)
        or len(symbols) != session["universe_count"]
    ):
        raise ValueError("frozen universe differs")
    params = run["request_params"]
    seq = round_state["sample_seq"]
    scheduled = round_state["scheduled_at"].astimezone(UTC)
    if not 0 <= seq < 32 or scheduled != series_slots(session["trade_date"])[seq]:
        raise ValueError("round schedule differs")
    expected = {
        "ingestion_id": str(run["ingestion_id"]),
        "session_id": str(session["session_id"]),
        "sample_seq": str(seq),
        "trade_date": str(session["trade_date"]),
        "scheduled_at": scheduled.isoformat(),
        "endpoint": params["endpoint"],
    }
    if any(str(params[k]) != expected[k] for k in expected if k != "ingestion_id"):
        raise ValueError("run lineage differs")
    if params["expected_rows"] != len(symbols):
        raise ValueError("run universe count differs")
    if not 0 < len(rows) <= BOUND or len(rows) != run["fetched_rows"]:
        raise ValueError("Raw count differs or exceeds bound")
    if len(facts) != run["accepted_rows"] or not facts:
        raise ValueError("complete attempt facts required")
    attempts = {row["attempt_number"] for row in rows}
    if len(attempts) != 1 or not attempts <= {"1", "2"}:
        raise ValueError("mixed or invalid attempts")
    quotes = []
    for row in rows:
        if any(row[k] != value for k, value in expected.items()):
            raise ValueError("Raw envelope lineage differs")
        observed = datetime.fromisoformat(row["worker_observed_at"])
        if observed.tzinfo is None:
            raise ValueError("Raw observation must include timezone")
        observed = observed.astimezone(UTC)
        if not run["started_at"] <= observed <= run["finished_at"]:
            raise ValueError("Raw observation outside original attempt")
        quote = normalize_raw_security_quote(
            loads(row["provider_raw_json"]),
            schema_version=row["provider_schema_version"],
            observed_at=observed,
        )
        if quote.symbol not in symbols:
            raise ValueError("Raw symbol outside frozen universe")
        quotes.append(quote)
    counts = Counter(q.symbol for q in quotes)
    duplicate = sorted(symbol for symbol, count in counts.items() if count > 1)
    projections = {}
    for quote in quotes:
        if quote.symbol in duplicate:
            continue
        record = _to_snapshot(
            quote,
            cast(MarketSeriesSession, SimpleNamespace(**session)),
            cast(MarketSeriesRound, SimpleNamespace(sample_seq=seq, scheduled_at=scheduled)),
        )
        projection = _snapshot_parameters(record, run["ingestion_id"])
        # PostgreSQL numeric(30,4) storage uses half-away-from-zero rounding.
        with localcontext() as context:
            context.prec = 50
            for key, value in projection.items():
                if isinstance(value, Decimal):
                    projection[key] = value.quantize(Decimal("0.0001"), rounding=ROUND_HALF_UP)
        projections[quote.symbol] = projection
    if set(projections) != {f["symbol"] for f in facts} or len(facts) != len(projections):
        raise ValueError("Raw accepted set differs from complete stored attempt")
    for fact in facts:
        if any(fact[key] != value for key, value in projections[fact["symbol"]].items()):
            raise ValueError("stored projection differs from native Raw")
    if run["rejected_rows"] != len(rows) - len(facts):
        raise ValueError("rejection count differs")
    return {
        "verified_rows": len(facts),
        "projection_hash": fingerprint({"rows": [projections[k] for k in sorted(projections)]}),
        "duplicate_symbols": duplicate,
        "absent_symbols": sorted(set(symbols) - set(counts)),
    }


def _evidence(
    connection: Connection, store: LocalRawStore, ingestion_id: UUID, *, lock: bool
) -> dict[str, Any]:
    suffix = " for share" if lock else ""
    run = (
        connection.execute(
            text("select * from ingestion.ingestion_run where ingestion_id=:id" + suffix),
            {"id": ingestion_id},
        )
        .mappings()
        .one()
    )
    if (
        run["dataset_code"] != "call_auction_market_series"
        or run["provider_code"] != "pytdx_hq"
        or run["status"] not in {"partial", "succeeded"}
    ):
        raise ValueError("only terminal native auction attempts are supported")
    params = run["request_params"]
    session = (
        connection.execute(
            text(
                "select * from realtime.call_auction_market_series_session where session_id=:id"
                + suffix
            ),
            {"id": UUID(params["session_id"])},
        )
        .mappings()
        .one()
    )
    round_state = (
        connection.execute(
            text(
                "select * from realtime.call_auction_market_series_round "
                "where session_id=:id and sample_seq=:seq" + suffix
            ),
            {"id": session["session_id"], "seq": params["sample_seq"]},
        )
        .mappings()
        .one()
    )
    if session["status"] not in {"partial", "succeeded"} or round_state["status"] not in {
        "partial",
        "succeeded",
    }:
        raise ValueError("session and round must be terminal")
    raw = (
        connection.execute(
            text("select * from ingestion.raw_manifest where ingestion_id=:id" + suffix),
            {"id": ingestion_id},
        )
        .mappings()
        .one()
    )
    manifest = RawManifest(
        raw_id=raw["raw_id"],
        ingestion_id=raw["ingestion_id"],
        object_path=raw["object_path"],
        file_format=RawFileFormat(raw["file_format"]),
        content_sha256=raw["content_sha256"],
        byte_size=raw["byte_size"],
        row_count=raw["row_count"],
        schema_version=raw["schema_version"],
        storage_backend=raw["storage_backend"],
    )
    if manifest.schema_version != SCHEMA or manifest.row_count > BOUND:
        raise ValueError("unsupported Raw schema or size")
    validate_quantity_metadata(
        provider="pytdx_hq", schema_version=SCHEMA, request_params=params, replay=True
    )
    rows = store.read_jsonl(manifest)
    proofs = {}
    for table in TABLES:
        # Entire ingestion is checked: no partial-date certification of sibling facts.
        facts = (
            connection.execute(
                text(
                    f"select * from {table} where ingestion_id=:id order by symbol limit :bound"
                    + suffix
                ),
                {"id": ingestion_id, "bound": BOUND + 1},
            )
            .mappings()
            .all()
        )
        if facts:
            proofs[table] = prove_records(
                dict(session), dict(run), dict(round_state), rows, [dict(fact) for fact in facts]
            )
    if not proofs:
        raise ValueError("no existing facts; collection/replay is not supported here")
    rules = quantity_group_rules(provider="pytdx_hq", schema_version=SCHEMA, field_group=GROUP)
    return {
        "rule": RULE,
        "database_identity": _database_identity(connection),
        "ingestion_id": str(ingestion_id),
        "trade_date": str(session["trade_date"]),
        "sample_seq": params["sample_seq"],
        "raw_hashes": [manifest.content_sha256],
        "lineage_hash": fingerprint(
            {"records": [dict(run), dict(session), dict(round_state), dict(raw)]}
        ),
        "proofs": proofs,
        "normalizer_version": NORMALIZER_VERSION,
        "unit_contract_version": UNIT_CONTRACT_VERSION,
        "source_rules": [
            {"identity": list(identity), "unit": rule.source_unit, "evidence": rule.evidence}
            for identity, rule in rules
        ],
    }


def certify_existing(
    engine: Engine,
    store: LocalRawStore,
    ingestion_id: UUID,
    *,
    plan: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Default read-only preview. Apply requires the exact saved plan and reproof."""
    with engine.begin() as connection:
        connection.execute(text("set transaction isolation level repeatable read"))
        if plan is None:
            connection.execute(text("set transaction read only"))
        connection.execute(text("set local statement_timeout='25s'"))
        connection.execute(text("set local lock_timeout='3s'"))
        if plan is not None:
            connection.execute(
                text("select pg_advisory_xact_lock(hashtextextended(:key,0))"),
                {"key": RULE + str(ingestion_id)},
            )
        evidence = _evidence(connection, store, ingestion_id, lock=plan is not None)
        existing = (
            connection.execute(
                text(
                    "select * from audit.quantity_unit_certification where ingestion_id=:id "
                    "and unit_contract_version=:v and field_group=:g"
                    + (" for update" if plan is not None else "")
                ),
                {"id": ingestion_id, "v": UNIT_CONTRACT_VERSION, "g": GROUP},
            )
            .mappings()
            .one_or_none()
        )
        if plan is None:
            preview = {
                "evidence": evidence,
                "before_certificate": dict(existing) if existing else None,
            }
            # Canonicalize dates/Decimals in the private plan before sealing it.
            preview = loads(dumps(preview, default=str))
            return {**preview, "plan_hash": fingerprint(preview)}
        unsigned = {k: v for k, v in plan.items() if k != "plan_hash"}
        if fingerprint(unsigned) != plan["plan_hash"] or evidence != plan["evidence"]:
            raise ValueError("sealed preview or current evidence changed")
        evidence_hash = fingerprint(evidence)
        repair_id = uuid5(NAMESPACE_URL, RULE + str(plan["plan_hash"]))
        prior = connection.execute(
            text("select status from audit.quantity_unit_repair_run where repair_run_id=:id"),
            {"id": repair_id},
        ).scalar_one_or_none()
        if (
            prior == "completed"
            and existing
            and existing["evidence_hash"] == evidence_hash
            and existing["status"] == "certified"
        ):
            return {"status": "already_applied", "ingestion_id": str(ingestion_id)}
        current = loads(dumps(dict(existing), default=str)) if existing else None
        if current != plan["before_certificate"] or existing is not None:
            raise ValueError("certificate changed or already exists; do not overwrite")
        connection.execute(
            text(
                "insert into audit.quantity_unit_repair_run "
                "(repair_run_id,plan_hash,rule_version,plan,status,next_item) "
                "values (:id,:hash,:rule,cast(:plan as jsonb),'completed',1)"
            ),
            {"id": repair_id, "hash": plan["plan_hash"], "rule": RULE, "plan": dumps(plan)},
        )
        write_quantity_certificates(
            connection,
            [
                {
                    "ingestion_id": ingestion_id,
                    "unit_contract_version": UNIT_CONTRACT_VERSION,
                    "field_group": GROUP,
                    "status": "certified",
                    "raw_hashes": dumps(evidence["raw_hashes"]),
                    "normalizer_version": RULE + "/" + NORMALIZER_VERSION,
                    "evidence_hash": evidence_hash,
                }
            ],
        )
        return {
            "status": "certified",
            "ingestion_id": str(ingestion_id),
            "evidence_hash": evidence_hash,
            "proofs": evidence["proofs"],
        }
