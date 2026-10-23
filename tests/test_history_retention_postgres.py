from datetime import date
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import DBAPIError
from test_postgres_integration import empty_database_url as empty_database_url
from test_postgres_integration import migrated_database_url as migrated_database_url

from market_data_center.history_retention import RetentionPolicy
from market_data_center.persistence.history_retention_postgres import (
    HistoryGroupFailure,
    HistoryRetentionPersistence,
)

pytestmark = pytest.mark.integration
MIGRATION = (
    Path(__file__).parents[1] / "supabase/migrations/20261023000800_scheduled_history_retention.sql"
)


@pytest.fixture
def repo(empty_database_url):
    with psycopg.connect(empty_database_url, autocommit=True) as c:
        for role in ("market_data_worker", "market_data_backup", "market_data_api"):
            if not c.execute("select 1 from pg_roles where rolname=%s", (role,)).fetchone():
                c.execute(f"create role {role}")
        c.execute("create schema audit; create schema core; create schema operations")
        c.execute("create table operations.workflow_run(workflow_run_id uuid primary key)")
        c.execute(
            "create table core.board_moneyflow(id int primary key, trade_date date, "
            "amount numeric, note text)"
        )
        c.execute(
            "insert into core.board_moneyflow values"
            "(1,'2020-01-01',1.123456789012345678901,null),(2,'2026-09-23',0,'boundary')"
        )
        c.execute(MIGRATION.read_text(encoding="utf8"))
    engine = create_engine(empty_database_url.replace("postgresql://", "postgresql+psycopg://"))
    yield HistoryRetentionPersistence(engine)
    engine.dispose()


def run_id(repo):
    value = uuid4()
    with repo.engine.begin() as c:
        c.execute(text("insert into operations.workflow_run values(:id)"), {"id": value})
    return value


def test_exact_archive_delete_boundary_rerun_and_precision(repo, tmp_path):
    policy = RetentionPolicy("core.board_moneyflow")
    cutoff = date(2026, 9, 23)
    batch = repo.clean_one(policy, cutoff, run_id(repo), tmp_path, lambda: None)
    assert batch["deleted_rows"] == 1
    assert repo.clean_one(policy, cutoff, run_id(repo), tmp_path, lambda: None)["deleted_rows"] == 0
    with repo.engine.connect() as c:
        assert c.execute(text("select id from core.board_moneyflow")).scalars().all() == [2]
        assert c.execute(text("select count(*) from audit.history_archive")).scalar_one() == 1
    from market_data_center.history_archive import HistoryArchive, read_archive

    archive = HistoryArchive(**batch["archive"])
    header, rows = read_archive(tmp_path, archive)
    assert "1.123456789012345678901" in rows[policy.table][0]
    assert '"note": null' in rows[policy.table][0]
    assert header["cutoff"] == "2026-09-23"
    assert repo.restore(archive.archive_id, tmp_path, execute=False)["rows"] == 1
    with repo.engine.connect() as c:
        assert c.execute(text("select count(*) from core.board_moneyflow")).scalar_one() == 1
    repo.restore(archive.archive_id, tmp_path, execute=True)
    repo.restore(archive.archive_id, tmp_path, execute=True)
    with repo.engine.connect() as c:
        assert (
            str(c.execute(text("select amount from core.board_moneyflow where id=1")).scalar_one())
            == "1.123456789012345678901"
        )


def test_unknown_target_and_recent_rows_refused(repo):
    with repo.engine.begin() as c:
        rows = c.execute(
            text("select to_jsonb(t)::text from core.board_moneyflow t where id=2")
        ).scalar_one()
    with pytest.raises(DBAPIError, match="protected"), repo.engine.begin() as c:
        c.execute(
            text(
                "select audit.history_batch('lock','core.board_moneyflow',"
                "cast(:rows as jsonb),:cutoff,null)"
            ),
            {"rows": f"[{rows}]", "cutoff": date(2026, 9, 23)},
        )
    with pytest.raises(DBAPIError, match="scope"), repo.engine.begin() as c:
        c.execute(
            text("select audit.history_batch('lock','core.security','[]',:cutoff,null)"),
            {"cutoff": date(2026, 9, 23)},
        )


def test_external_cascade_and_archive_corruption_roll_back(repo, tmp_path, monkeypatch):
    with repo.engine.begin() as c:
        c.execute(
            text(
                "create table core.other(id int primary key, "
                "parent int references core.board_moneyflow(id) on delete cascade)"
            )
        )
        c.execute(text("insert into core.other values(1,1)"))
    with pytest.raises(HistoryGroupFailure) as failure:
        repo.clean_one(
            RetentionPolicy("core.board_moneyflow"),
            date(2026, 9, 23),
            run_id(repo),
            tmp_path,
            lambda: None,
        )
    assert "referenced_rows" in str(failure.value.__cause__)
    with repo.engine.connect() as c:
        assert c.execute(text("select count(*) from core.board_moneyflow")).scalar_one() == 2
        assert c.execute(text("select count(*) from core.other")).scalar_one() == 1
        assert c.execute(text("select count(*) from audit.history_archive")).scalar_one() == 0


def test_catalog_matches_fixed_code(repo):
    from market_data_center.history_retention import table_predicates

    with repo.engine.connect() as c:
        actual = dict(
            c.execute(
                text("select table_name,predicate from audit.history_retention_catalog")
            ).all()
        )
    assert actual == table_predicates()


def test_oversized_root_retained_and_next_group_processed(repo, tmp_path, monkeypatch):
    from market_data_center.history_cleanup_service import HistoryCleanupService
    from market_data_center.history_retention import POLICIES
    from market_data_center.quality_cleanup_service import CleanupBudget

    policy = next(p for p in POLICIES if p.table == "today_limit_up.snapshot")
    with repo.engine.begin() as c:
        c.execute(text("create schema today_limit_up"))
        c.execute(
            text(
                "create table today_limit_up.snapshot(snapshot_id int primary key,trade_date date)"
            )
        )
        c.execute(
            text(
                "create table today_limit_up.member(snapshot_id int references "
                "today_limit_up.snapshot, symbol int, primary key(snapshot_id,symbol))"
            )
        )
        c.execute(
            text(
                "create table today_limit_up.calculation_quality(snapshot_id int references "
                "today_limit_up.snapshot, rule_code text,primary key(snapshot_id,rule_code))"
            )
        )
        c.execute(
            text(
                "insert into today_limit_up.snapshot values"
                "(1,'2020-01-01'),(2,'2020-01-01'),(3,'2026-09-23')"
            )
        )
        c.execute(
            text("insert into today_limit_up.member select 1,n from generate_series(1,10000) n")
        )
        c.execute(text("insert into today_limit_up.member values(2,1),(3,1)"))
    monkeypatch.setattr("market_data_center.history_cleanup_service.POLICIES", (policy,))
    result = HistoryCleanupService(repo, tmp_path).run(
        date(2026, 10, 23), run_id(repo), CleanupBudget(float("inf"))
    )
    assert result.deleted_rows == 2 and result.partial
    with repo.engine.connect() as c:
        assert c.execute(
            text("select snapshot_id from today_limit_up.snapshot order by 1")
        ).scalars().all() == [1, 3]
        assert c.execute(text("select count(*) from today_limit_up.member")).scalar_one() == 10001
        archive_id = c.scalar(text("select archive_id from audit.history_archive"))
    repo.restore(archive_id, tmp_path, execute=True)
    repo.restore(archive_id, tmp_path, execute=True)
    with repo.engine.begin() as c:
        assert c.scalar(text("select count(*) from today_limit_up.member where snapshot_id=2")) == 1
        c.execute(text("grant usage on schema audit to market_data_worker"))
    for records, expected in (
        ('{"today_limit_up.member":[{"snapshot_id":2,"symbol":1}]}', "root_required"),
        (
            '{"today_limit_up.snapshot":[{"snapshot_id":2,"trade_date":"2020-01-01"}]}',
            "incomplete_children",
        ),
    ):
        with pytest.raises(DBAPIError, match=expected), repo.engine.begin() as c:
            c.execute(text("set local role market_data_worker"))
            c.execute(
                text(
                    "select audit.history_delete_group('today_limit_up.snapshot',"
                    "cast(:rows as jsonb),'2026-09-23',:id)"
                ),
                {"rows": records, "id": archive_id},
            )


def test_archive_corruption_after_publish_rolls_back(repo, tmp_path, monkeypatch):
    import market_data_center.persistence.history_retention_postgres as module

    original = module.write_archive

    def corrupt(*args, **kwargs):
        archive = original(*args, **kwargs)
        (tmp_path / archive.path).write_bytes(b"corrupt")
        return archive

    monkeypatch.setattr(module, "write_archive", corrupt)
    with pytest.raises(HistoryGroupFailure):
        repo.clean_one(
            RetentionPolicy("core.board_moneyflow"),
            date(2026, 9, 23),
            run_id(repo),
            tmp_path,
            lambda: None,
        )
    with repo.engine.connect() as c:
        assert c.execute(text("select count(*) from core.board_moneyflow")).scalar_one() == 2
        assert c.execute(text("select count(*) from audit.history_archive")).scalar_one() == 0


def test_private_child_delete_unavailable_to_worker_and_api(repo):
    with repo.engine.begin() as c:
        c.execute(text("grant usage on schema audit to market_data_worker"))
    with pytest.raises(DBAPIError, match="permission denied"), repo.engine.begin() as c:
        c.execute(text("set local role market_data_worker"))
        c.execute(
            text("select audit.history_batch('delete','stock_pool.member','[]','2020-01-01',null)")
        )
    with repo.engine.connect() as c:
        for role in ("anon", "authenticated", "market_data_api"):
            assert not c.scalar(
                text(
                    "select has_function_privilege(:role,"
                    "'audit.history_delete_group(text,jsonb,date,uuid)','execute')"
                ),
                {"role": role},
            )


def test_all_catalog_queries_compile_against_complete_schema(migrated_database_url, tmp_path):
    from market_data_center.history_retention import POLICIES, table_predicates

    engine = create_engine(migrated_database_url.replace("postgresql://", "postgresql+psycopg://"))
    repo = HistoryRetentionPersistence(engine)
    try:
        with repo.transaction(readonly=True) as c:
            for table, predicate in table_predicates().items():
                c.execute(
                    text(f"select 1 from {table} t where {predicate} limit 1"),
                    {"cutoff": date(2026, 9, 23)},
                )
        for policy in POLICIES:
            assert (
                repo.clean_one(policy, date(2026, 9, 23), uuid4(), tmp_path, lambda: None)[
                    "deleted_rows"
                ]
                == 0
            )
    finally:
        engine.dispose()


def test_worker_cleans_via_bounded_functions_without_update_or_delete_grants(repo, tmp_path):
    from sqlalchemy import event

    workflow = run_id(repo)
    with repo.engine.begin() as c:
        c.execute(text("grant usage on schema audit,core to market_data_worker"))
        c.execute(text("grant select on core.board_moneyflow to market_data_worker"))

    def set_worker(c):
        c.exec_driver_sql("set local role market_data_worker")

    event.listen(repo.engine, "begin", set_worker)
    try:
        result = repo.clean_one(
            RetentionPolicy("core.board_moneyflow"),
            date(2026, 9, 23),
            workflow,
            tmp_path,
            lambda: None,
        )
        assert result["deleted_rows"] == 1
        with repo.engine.connect() as c:
            assert not c.scalar(
                text(
                    "select has_table_privilege(current_user,"
                    "'core.board_moneyflow','update,delete')"
                )
            )
    finally:
        event.remove(repo.engine, "begin", set_worker)
