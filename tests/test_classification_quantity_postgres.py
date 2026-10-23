"""Classification completeness survives persistence and the public version boundary."""

# Imported fixtures are registered by pytest and injected by name.
# ruff: noqa: F811

from dataclasses import replace
from datetime import UTC, date, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy import Engine, text
from sqlalchemy.exc import IntegrityError
from test_postgres_integration import (  # noqa: F401
    database_engine,
    empty_database_url,
    migrated_database_url,
)

from market_data_center.domain import (
    CalculationMode,
    CalculationRun,
    CalculationStatus,
    ClassificationDailyMetricRecord,
    ClassificationType,
    DerivedCalculationOutput,
)
from market_data_center.persistence import PostgreSQLDerivedPersistence

pytestmark = pytest.mark.integration
DAY = date(2026, 10, 22)
NOW = datetime(2026, 10, 23, tzinfo=UTC)


def _run(version: str = "1.2.0") -> CalculationRun:
    return CalculationRun(
        calculation_id=uuid4(),
        calculation_code="cn_a_share_daily_derived",
        algorithm_version=version,
        mode=CalculationMode.INCREMENTAL,
        start_date=DAY,
        end_date=DAY,
        status=CalculationStatus.RUNNING,
        input_watermark={},
        input_hash="a" * 64,
        requested_at=NOW,
    )


def _metric() -> ClassificationDailyMetricRecord:
    return ClassificationDailyMetricRecord(
        namespace="eastmoney",
        classification_type=ClassificationType.INDUSTRY,
        classification_code="BK0475",
        membership_snapshot_date=DAY,
        trade_date=DAY,
        member_count=2,
        priced_member_count=1,
        advancing_count=1,
        declining_count=0,
        unchanged_count=0,
        total_volume=None,
        total_amount=Decimal("1000"),
        equal_weight_return=Decimal("0.1"),
        total_market_cap=None,
        market_cap_member_count=0,
        volume_member_count=1,
        amount_member_count=2,
    )


def _persist(engine: Engine, metric: ClassificationDailyMetricRecord) -> CalculationRun:
    persistence = PostgreSQLDerivedPersistence(engine)
    run = _run()
    persistence.create_calculation_run(run)
    persistence.commit_calculation(
        run.succeeded(finished_at=NOW, output_rows=1),
        DerivedCalculationOutput((), (), (), (metric,)),
    )
    return run


def test_nullable_totals_and_independent_coverage_round_trip(database_engine: Engine) -> None:
    run = _persist(database_engine, _metric())
    with database_engine.connect() as connection:
        row = (
            connection.execute(
                text("select * from api_v1.classification_daily_metrics where calculation_id=:id"),
                {"id": run.calculation_id},
            )
            .mappings()
            .one()
        )

    assert row["total_volume"] is None
    assert row["total_amount"] == Decimal("1000")
    assert row["volume_member_count"] == 1
    assert row["amount_member_count"] == 2
    assert row["equal_weight_return"] == Decimal("0.1")
    assert row["quantity_quality"]["status"] == "incomplete"


@pytest.mark.parametrize("field", ["volume_member_count", "amount_member_count"])
def test_database_rejects_coverage_above_member_count(database_engine: Engine, field: str) -> None:
    run = _persist(database_engine, _metric())
    with pytest.raises(IntegrityError), database_engine.begin() as connection:
        connection.execute(
            text(
                f"update metrics.classification_daily_metric set {field}=3 where calculation_id=:id"
            ),
            {"id": run.calculation_id},
        )


def test_explicit_zero_is_certified_when_every_member_is_covered(database_engine: Engine) -> None:
    run = _persist(
        database_engine,
        replace(
            _metric(),
            total_volume=0,
            total_amount=Decimal(0),
            volume_member_count=2,
            total_market_cap=Decimal(0),
            market_cap_member_count=2,
        ),
    )
    with database_engine.connect() as connection:
        row = (
            connection.execute(
                text("select * from api_v1.classification_daily_metrics where calculation_id=:id"),
                {"id": run.calculation_id},
            )
            .mappings()
            .one()
        )

    assert row["total_volume"] == 0
    assert row["total_amount"] == Decimal(0)
    assert row["quantity_quality"]["status"] == "certified"


def test_complete_trade_totals_do_not_certify_missing_capital(database_engine: Engine) -> None:
    run = _persist(database_engine, replace(_metric(), total_volume=100, volume_member_count=2))
    with database_engine.connect() as connection:
        row = (
            connection.execute(
                text("select * from api_v1.classification_daily_metrics where calculation_id=:id"),
                {"id": run.calculation_id},
            )
            .mappings()
            .one()
        )

    assert row["total_market_cap"] is None
    assert row["quantity_quality"]["status"] == "incomplete"
    assert row["quantity_quality"]["field_groups"]["classification_market_cap"]["status"] == (
        "incomplete"
    )


def test_legacy_results_keep_audit_values_but_public_quantities_are_unverified(
    database_engine: Engine,
) -> None:
    run = _run("1.1.0")
    persistence = PostgreSQLDerivedPersistence(database_engine)
    persistence.create_calculation_run(run)
    with database_engine.begin() as connection:
        connection.execute(
            text("""
insert into metrics.classification_daily_metric (
 calculation_id, namespace, classification_type, classification_code,
 membership_snapshot_date, trade_date, member_count, priced_member_count,
 advancing_count, declining_count, unchanged_count, total_volume, total_amount,
 market_cap_member_count
) values (:id,'eastmoney','industry','BK0475',:day,:day,2,1,1,0,0,100,1000,0)
"""),
            {"id": run.calculation_id, "day": DAY},
        )
        connection.execute(
            text("""
update derived.calculation_run set status='succeeded',calculated_at=:now,finished_at=:now
where calculation_id=:id
"""),
            {"id": run.calculation_id, "now": NOW},
        )
        audit = connection.execute(
            text(
                "select total_volume from metrics.classification_daily_metric "
                "where calculation_id=:id"
            ),
            {"id": run.calculation_id},
        ).scalar_one()
        public = (
            connection.execute(
                text("select * from api_v1.classification_daily_metrics where calculation_id=:id"),
                {"id": run.calculation_id},
            )
            .mappings()
            .one()
        )

    assert audit == 100
    assert public["total_volume"] is public["total_amount"] is None
    assert public["volume_member_count"] is public["amount_member_count"] is None
    assert public["quantity_quality"]["status"] == "unverified"
