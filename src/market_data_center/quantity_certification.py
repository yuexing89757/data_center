"""Unit admission and atomic evidence for newly normalized facts (ADR-0068)."""

from collections.abc import Iterable
from dataclasses import fields, is_dataclass
from hashlib import sha256
from json import dumps
from uuid import UUID

from sqlalchemy import Connection, text

from market_data_center.domain.ingestion import IngestionRun, RawFileFormat, RawManifest
from market_data_center.providers.contracts import ProviderError
from market_data_center.providers.quantity_units import (
    BIGINT_MAX,
    NORMALIZER_VERSION,
    UNIT_CONTRACT_VERSION,
    quantity_group_rules,
    validate_quantity_metadata,
)

_GROUPS = {
    "daily_bar": ("daily_bar.volume", "daily_bar.amount"),
    "capital": ("capital.rights_issue.quantities",),
    "stock_daily_indicator": ("stock_daily_indicator.shares",),
    "top10_float_holder": ("top10_float_holder.hold_amount",),
    "convertible_bond": ("convertible_bond.bond.quantities",),
    "convertible_bond_daily_bar": (
        "convertible_bond_daily_bar.volume",
        "convertible_bond_daily_bar.amount",
    ),
    "today_limit_up_source": ("today_limit_up.source_observation.quantities",),
    "today_limit_down_source": ("today_limit_down.source_observation.quantities",),
    "five_level_quote": ("five_level_quote.quantities",),
    "eod_quote_snapshot": ("eod_quote_snapshot.quantities",),
    "call_auction_market_snapshot": ("call_auction_market_snapshot.quantities",),
    "call_auction_market_series": ("call_auction_market_series.quantities",),
    "call_auction_indicative_detail": ("call_auction_indicative_detail.displayed_volume_shares",),
}

_FACT_QUANTITIES = {
    "volume",
    "cumulative_volume",
    "displayed_volume_shares",
    "volume_shares",
    "total_shares",
    "restricted_shares",
    "circulating_shares",
    "listed_a_shares",
    "free_float_shares",
    "base_shares",
    "hold_amount",
}


def check_standard_quantities(records: Iterable[object]) -> None:
    """Reject fractional/bool/overflow facts; this never decides a source unit."""
    for record in records:
        if not is_dataclass(record) or isinstance(record, type):
            continue
        for field in fields(record):
            value = getattr(record, field.name)
            if (
                field.name == "hold_change"
                and value is not None
                and (type(value) is not int or not -(BIGINT_MAX + 1) <= value <= BIGINT_MAX)
            ):
                raise ProviderError("standard hold_change must be signed integral shares")
            is_quantity = field.name in _FACT_QUANTITIES or (
                field.name.endswith("_volume") or field.name.endswith("_volume_shares")
            )
            if is_quantity and value is not None:
                if type(value) is not int or not 0 <= value <= BIGINT_MAX:
                    raise ProviderError(f"standard quantity {field.name} must be integral shares")
            elif is_dataclass(value):
                check_standard_quantities((value,))
            elif isinstance(value, (tuple, list)):
                check_standard_quantities(value)


def build_quantity_certificates(
    *, ingestion_id: UUID, provider: str, dataset: str, manifest: RawManifest
) -> tuple[dict[str, object], ...]:
    """Describe a current adapter's output; historical certification must replay Raw."""
    groups = _GROUPS.get(dataset, ())
    if dataset == "five_level_quote" and provider == "tencent_quote":
        groups = ("stock_quote_snapshot.quantities",)
    result: list[dict[str, object]] = []
    for group in groups:
        rules = quantity_group_rules(
            provider=provider,
            schema_version=manifest.schema_version,
            field_group=group,
            instrument_type="convertible_bond"
            if dataset in {"convertible_bond", "convertible_bond_daily_bar"}
            else "stock",
        )
        evidence = {
            "ingestion_id": str(ingestion_id),
            "raw_id": str(manifest.raw_id),
            "raw_hashes": [manifest.content_sha256],
            "provider": provider,
            "dataset": dataset,
            "schema_version": manifest.schema_version,
            "field_group": group,
            "normalizer_version": NORMALIZER_VERSION,
            "unit_contract_version": UNIT_CONTRACT_VERSION,
            "rules": [
                {"identity": identity, "unit": rule.source_unit, "evidence": rule.evidence}
                for identity, rule in rules
            ],
        }
        evidence_hash = sha256(dumps(evidence, sort_keys=True).encode()).hexdigest()
        result.append(
            {
                "ingestion_id": ingestion_id,
                "unit_contract_version": UNIT_CONTRACT_VERSION,
                "field_group": group,
                "status": "certified",
                "raw_hashes": dumps([manifest.content_sha256]),
                "normalizer_version": NORMALIZER_VERSION,
                "evidence_hash": evidence_hash,
            }
        )
    return tuple(result)


def certify_quantity_batch(
    connection: Connection, run: IngestionRun, manifest: RawManifest | None
) -> None:
    """Use the fact transaction; a certificate never commits before its facts."""
    write_quantity_certificates(connection, quantity_certificates(connection, run, manifest))


def quantity_certificates(
    connection: Connection, run: IngestionRun, manifest: RawManifest | None
) -> tuple[dict[str, object], ...]:
    if run.dataset_code.value not in _GROUPS or run.accepted_rows == 0:
        return ()
    if manifest is None:
        if run.replayed_from_raw_id is None:
            raise ProviderError("quantity certification requires a Raw manifest")
        row = (
            connection.execute(
                text("""select raw_id,ingestion_id,object_path,file_format,content_sha256,
                           byte_size,row_count,schema_version,storage_backend
                    from ingestion.raw_manifest where raw_id=:raw_id"""),
                {"raw_id": run.replayed_from_raw_id},
            )
            .mappings()
            .one()
        )
        manifest = RawManifest(
            raw_id=row["raw_id"],
            ingestion_id=row["ingestion_id"],
            object_path=row["object_path"],
            file_format=RawFileFormat(row["file_format"]),
            content_sha256=row["content_sha256"],
            byte_size=row["byte_size"],
            row_count=row["row_count"],
            schema_version=row["schema_version"],
            storage_backend=row["storage_backend"],
        )
    elif manifest.ingestion_id != run.ingestion_id:
        raise ProviderError("quantity manifest does not match its new ingestion")
    validate_quantity_metadata(
        provider=run.provider_code.value,
        schema_version=manifest.schema_version,
        request_params=run.request_params,
    )
    return build_quantity_certificates(
        ingestion_id=run.ingestion_id,
        provider=run.provider_code.value,
        dataset=run.dataset_code.value,
        manifest=manifest,
    )


def write_quantity_certificates(
    connection: Connection, parameters: Iterable[dict[str, object]]
) -> None:
    parameters = list(parameters)
    if parameters:
        connection.execute(
            text("""
            insert into audit.quantity_unit_certification (
                ingestion_id,unit_contract_version,field_group,status,
                raw_hashes,normalizer_version,evidence_hash
            ) values (
                :ingestion_id,:unit_contract_version,:field_group,:status,
                cast(:raw_hashes as jsonb),:normalizer_version,:evidence_hash
            )
        """),
            parameters,
        )
