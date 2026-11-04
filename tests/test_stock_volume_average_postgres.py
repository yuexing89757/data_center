from datetime import date, timedelta
from decimal import Decimal
from unittest.mock import MagicMock
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.exc import DBAPIError, IntegrityError
from test_postgres_integration import (  # noqa: F401
    _seed_quantity_fixture_proof,
    database_engine,
    empty_database_url,
    migrated_database_url,
)
from test_stock_volume_average import DAYS

from market_data_center.persistence.stock_volume_average_postgres import (
    PostgreSQLStockVolumeAveragePersistence,
    VolumeAverageDependencyNotReady,
)
from market_data_center.stock_volume_average_service import StockVolumeAverageService


def test_loader_fails_for_missing_middle_calendar_even_with_five_sessions():
    connection = MagicMock()
    connection.execute.side_effect = [
        MagicMock(**{"scalars.return_value.all.return_value": list(reversed(DAYS))}),
        MagicMock(**{"scalar_one.return_value": 6}),
    ]
    engine = MagicMock()
    engine.connect.return_value.execution_options.return_value.__enter__.return_value = connection
    with pytest.raises(VolumeAverageDependencyNotReady, match="calendar"):
        PostgreSQLStockVolumeAveragePersistence(engine).load_input(DAYS[-1])
    assert connection.execute.call_count == 2


def test_loader_uses_one_repeatable_snapshot_and_masks_by_volume_group_only():
    ingestion_id = uuid4()
    connection = MagicMock()
    observations = [
        {
            "symbol": "SSE:600000",
            "trade_date": day,
            "volume": volume,
            "trade_status": "trading",
            "ingestion_id": ingestion_id,
            "updated_at": "2026-11-04",
            "evidence_hash": "a" * 64,
            "normalizer_version": "test.v1",
            "volume_quality": {"status": "certified", "reason": "matching_raw_evidence"},
        }
        for day, volume in zip(DAYS, (100, 200, 300, 400, 501), strict=True)
    ]
    connection.execute.side_effect = [
        MagicMock(**{"scalars.return_value.all.return_value": list(reversed(DAYS))}),
        MagicMock(**{"scalar_one.return_value": 7}),
        MagicMock(
            **{
                "mappings.return_value.one_or_none.return_value": {
                    "workflow_run_id": uuid4(),
                    "status": "partial",
                }
            }
        ),
        MagicMock(
            **{
                "mappings.return_value.all.return_value": [
                    {
                        "symbol": "SSE:600000",
                        "ipo_date": date(1999, 11, 10),
                        "delisting_date": None,
                        "status": "listed",
                        "updated_at": "2026-11-04",
                    }
                ]
            }
        ),
        MagicMock(**{"mappings.return_value.all.return_value": observations}),
    ]
    engine = MagicMock()
    snapshot = engine.connect.return_value.execution_options
    snapshot.return_value.__enter__.return_value = connection
    source, watermark, evidence = PostgreSQLStockVolumeAveragePersistence(engine).load_input(
        DAYS[-1]
    )
    from market_data_center.stock_volume_average_calculator import calculate_stock_volume_average

    assert calculate_stock_volume_average(source).records[0].volume_average_5d == Decimal("300.2")
    snapshot.assert_called_once_with(isolation_level="REPEATABLE READ")
    assert connection.execute.call_count == 5
    assert len(evidence["SSE:600000"]) == 5
    assert watermark["unit_contract_version"] == "stock-quantity.v1"


def test_large_universe_pages_share_the_same_snapshot_and_never_load_all_symbols_at_once():
    connection = MagicMock()
    securities = [
        {
            "symbol": f"SSE:{index:06}",
            "ipo_date": date(1999, 11, 10),
            "delisting_date": None,
            "status": "listed",
            "updated_at": "2026-11-04",
        }
        for index in range(205)
    ]
    pages = []

    def execute(statement, parameters):
        result = MagicMock()
        sql = str(statement)
        if "order by trade_date desc limit 5" in sql:
            result.scalars.return_value.all.return_value = list(reversed(DAYS))
        elif "select count(*) from core.trading_calendar" in sql:
            result.scalar_one.return_value = 7
        elif "from operations.workflow_run" in sql:
            result.mappings.return_value.one_or_none.return_value = {
                "workflow_run_id": uuid4(),
                "status": "succeeded",
            }
        elif "from core.security" in sql:
            result.mappings.return_value.all.return_value = securities
        else:
            assert "from audit.unit_safe_daily_bar b" in sql
            assert len(parameters["symbols"]) <= 100
            assert parameters["days"] == list(DAYS)
            pages.append(parameters["symbols"])
            result.mappings.return_value.all.return_value = []
        return result

    connection.execute.side_effect = execute
    engine = MagicMock()
    engine.connect.return_value.execution_options.return_value.__enter__.return_value = connection
    loaded, _, _ = PostgreSQLStockVolumeAveragePersistence(engine).load_input(DAYS[-1])
    assert [len(page) for page in pages] == [100, 100, 5]
    assert [symbol for page in pages for symbol in page] == [s["symbol"] for s in securities]
    assert len(loaded.stocks) == 205
    assert connection.execute.call_count == 7


@pytest.fixture
def volume_seed(database_engine):  # noqa: F811
    ingestion_id = uuid4()
    with database_engine.begin() as c:
        c.execute(
            text("""insert into ingestion.ingestion_run
        (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
        values (:id,'pytdx','daily_bar','succeeded',now(),now(),now())"""),
            {"id": ingestion_id},
        )
        c.execute(
            text("""insert into core.security
        (symbol,code,exchange,current_name,security_type,status,ipo_date,source_code,ingestion_id)
        values ('SSE:600000','600000','SSE','synthetic','stock','listed',
                '1999-11-10','pytdx',:id)"""),
            {"id": ingestion_id},
        )
        day = DAYS[0]
        while day <= DAYS[-1]:
            c.execute(
                text("""insert into core.trading_calendar
            (market,trade_date,is_trading_day,source_code,ingestion_id)
            values ('CN_A_SHARE',:day,:open,'pytdx',:id)"""),
                {"day": day, "open": day in DAYS, "id": ingestion_id},
            )
            day += timedelta(days=1)
        for day, volume in zip(DAYS, (100, 200, 300, 400, 501), strict=True):
            c.execute(
                text("""insert into core.daily_bar
            (symbol,trade_date,market,open,high,low,close,volume,amount,
             trade_status,source_code,ingestion_id)
            values ('SSE:600000',:day,'CN_A_SHARE',10,10,10,10,:volume,10000,
                    'trading','pytdx',:id)"""),
                {"day": day, "volume": volume, "id": ingestion_id},
            )
        c.execute(
            text("""insert into operations.workflow_run
        (workflow_run_id,workflow_code,scheduled_for,trigger_source,attempt,status,started_at,finished_at)
        values (:id,'daily_market','2026-11-04 12:00:00+00',
                'scheduled',1,'partial',now(),now())"""),
            {"id": uuid4()},
        )
        # Deliberately certify only volume: amount must not gate a share average.
        _seed_quantity_fixture_proof(c, ingestion_id, ("daily_bar.volume",))
    return ingestion_id


@pytest.mark.integration
def test_certified_volume_is_independent_of_amount_and_publication_reuses_versions(
    database_engine,  # noqa: F811
    volume_seed,
):
    persistence = PostgreSQLStockVolumeAveragePersistence(database_engine)
    builder = StockVolumeAverageService(persistence)
    first = builder.build(DAYS[-1])
    assert first.complete_count == 1
    assert builder.build(DAYS[-1]).status == "unchanged"
    with database_engine.connect() as c:
        row = c.execute(text("select * from derived.stock_volume_average_5d")).mappings().one()
        assert row["volume_average_5d"] == Decimal("300.2")
        assert len(row["input_evidence"]) == 5
        assert row["input_evidence"][-1]["volume"] == 501
    with database_engine.begin() as c:
        c.execute(
            text("update core.daily_bar set volume=0 where trade_date=:day"), {"day": DAYS[-1]}
        )
    assert builder.build(DAYS[-1]).version == 2
    with database_engine.begin() as c:
        c.execute(
            text("update core.daily_bar set volume=501 where trade_date=:day"), {"day": DAYS[-1]}
        )
    restored = builder.build(DAYS[-1])
    assert restored.status == "published/reused"
    assert restored.calculation_id == first.calculation_id
    assert restored.version == 3
    with database_engine.connect() as c:
        assert c.execute(text("select count(*) from derived.calculation_run")).scalar_one() == 2
        assert (
            c.execute(text("select count(*) from derived.stock_volume_average_5d")).scalar_one()
            == 2
        )


@pytest.mark.integration
def test_unverified_volume_is_null_and_daily_dependency_must_be_terminal(
    database_engine,  # noqa: F811
    volume_seed,
):
    builder = StockVolumeAverageService(PostgreSQLStockVolumeAveragePersistence(database_engine))
    with database_engine.begin() as c:
        c.execute(text("delete from audit.quantity_unit_certification"))
    assert builder.build(DAYS[-1]).complete_count == 0
    with database_engine.connect() as c:
        row = c.execute(text("select * from derived.stock_volume_average_5d")).mappings().one()
        assert row["volume_average_5d"] is None
        assert row["quality_reasons"] == ["quantity_units_unverified"]
        assert all(item["volume"] is None for item in row["input_evidence"])
    with database_engine.begin() as c:
        c.execute(text("update operations.workflow_run set status='running',finished_at=null"))
    with pytest.raises(VolumeAverageDependencyNotReady):
        builder.build(DAYS[-1])


@pytest.mark.integration
def test_reused_calculation_keeps_new_publication_certificate_evidence(
    database_engine,  # noqa: F811
    volume_seed,
):
    builder = StockVolumeAverageService(PostgreSQLStockVolumeAveragePersistence(database_engine))
    original = builder.build(DAYS[-1])
    with database_engine.begin() as c:
        c.execute(text("delete from audit.quantity_unit_certification"))
    assert builder.build(DAYS[-1]).complete_count == 0
    for version in (3, 4):
        replacement = uuid4()
        with database_engine.begin() as c:
            c.execute(
                text("""insert into ingestion.ingestion_run
            (ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at)
            values (:id,'pytdx','daily_bar','succeeded',now(),now(),now())"""),
                {"id": replacement},
            )
            c.execute(text("update core.daily_bar set ingestion_id=:id"), {"id": replacement})
            _seed_quantity_fixture_proof(c, replacement, ("daily_bar.volume",))
        restored = builder.build(DAYS[-1])
        assert restored.calculation_id == original.calculation_id
        assert restored.status == "published/reused"
        assert restored.version == version
        assert builder.build(DAYS[-1]).status == "unchanged"
        with database_engine.connect() as c:
            publication = (
                c.execute(
                    text("""select input_evidence,input_watermark
            from derived.stock_volume_average_5d_snapshot
            order by version desc limit 1""")
                )
                .mappings()
                .one()
            )
            assert publication["input_evidence"]["SSE:600000"][0]["ingestion_id"] == str(
                replacement
            )
            assert publication["input_watermark"]["unit_contract_version"] == "stock-quantity.v1"
            original_proof = c.execute(
                text("""select input_evidence
            from derived.stock_volume_average_5d where calculation_id=:id"""),
                {"id": original.calculation_id},
            ).scalar_one()
            assert original_proof[0]["ingestion_id"] == str(volume_seed)


@pytest.mark.integration
def test_publication_failure_rolls_back_rows_and_keeps_current_version(
    database_engine,  # noqa: F811
    volume_seed,
):
    builder = StockVolumeAverageService(PostgreSQLStockVolumeAveragePersistence(database_engine))
    first = builder.build(DAYS[-1])
    with database_engine.begin() as c:
        c.execute(
            text("update core.daily_bar set volume=0 where trade_date=:day"), {"day": DAYS[-1]}
        )

    def reject_snapshot(conn, cursor, statement, parameters, context, executemany):
        if statement.lstrip().startswith("insert into derived.stock_volume_average_5d_snapshot"):
            raise RuntimeError("synthetic rollback")

    event.listen(database_engine, "before_cursor_execute", reject_snapshot)
    try:
        with pytest.raises(RuntimeError, match="rollback"):
            builder.build(DAYS[-1])
    finally:
        event.remove(database_engine, "before_cursor_execute", reject_snapshot)
    with database_engine.connect() as c:
        assert (
            c.execute(text("select count(*) from derived.stock_volume_average_5d")).scalar_one()
            == 1
        )
        assert (
            c.execute(
                text("select calculation_id from derived.stock_volume_average_5d_snapshot")
            ).scalar_one()
            == first.calculation_id
        )
        assert (
            c.execute(
                text("select count(*) from derived.calculation_run where status='failed'")
            ).scalar_one()
            == 1
        )


@pytest.mark.integration
def test_table_privileges_immutability_and_quality_check(database_engine, volume_seed):  # noqa: F811
    builder = StockVolumeAverageService(PostgreSQLStockVolumeAveragePersistence(database_engine))
    builder.build(DAYS[-1])
    for role in ("anon", "authenticated", "market_data_api"):
        with pytest.raises(DBAPIError), database_engine.begin() as c:
            c.execute(text(f"set local role {role}"))
            c.execute(text("select * from derived.stock_volume_average_5d"))
    with pytest.raises(DBAPIError), database_engine.begin() as c:
        c.execute(text("set local role market_data_worker"))
        c.execute(text("update derived.stock_volume_average_5d set covered_days=4"))
    with pytest.raises(IntegrityError), database_engine.begin() as c:
        c.execute(text("update derived.stock_volume_average_5d set covered_days=4"))
    with database_engine.begin() as c:
        c.execute(text("set local role market_data_worker"))
        assert (
            c.execute(text("select count(*) from derived.stock_volume_average_5d")).scalar_one()
            == 1
        )
