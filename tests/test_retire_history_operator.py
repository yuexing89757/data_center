"""Operator retention must prove archived rows before deleting exact old facts."""

import importlib.util
import json
from pathlib import Path

import psycopg
import pytest
from test_postgres_integration import empty_database_url as empty_database_url

pytestmark = pytest.mark.integration


@pytest.fixture
def operator():
    path = Path(__file__).parents[1] / "scripts" / "retire_history_before.py"
    spec = importlib.util.spec_from_file_location("retire_history_operator", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def connection(empty_database_url):
    with psycopg.connect(empty_database_url, autocommit=True) as connection:
        connection.execute("create schema retention_test")
        connection.execute("""create table retention_test.fact (
            id integer primary key, trade_date date not null, amount numeric,
            note text, observed_at timestamptz)""")
        connection.execute("""insert into retention_test.fact values
            (1,'2026-09-22',1.1234567890123456789,'old',now()),
            (2,'2026-09-23',null,'boundary',now()),
            (3,'2026-10-23',0,'current',now())""")
        yield connection


def prepare(operator, connection, tmp_path):
    operator.TARGETS["retention_test.fact"] = "t.trade_date < DATE '2026-09-23'"
    return operator.archive(connection, "retention_test.fact", tmp_path / "archive")


def test_archive_readonly_apply_boundary_and_idempotence(operator, connection, tmp_path):
    plan = prepare(operator, connection, tmp_path)
    assert connection.execute("select count(*) from retention_test.fact").fetchone()[0] == 3
    assert plan["rows"] == 1
    digest = operator.verify_archive(tmp_path / "archive")
    result = operator.apply(connection, tmp_path / "archive", digest)
    assert result == {"deleted": 1, "already_absent": 0}
    assert connection.execute("select id from retention_test.fact order by id").fetchall() == [
        (2,),
        (3,),
    ]
    assert operator.apply(connection, tmp_path / "archive", digest) == {
        "deleted": 0,
        "already_absent": 1,
    }
    operator.restore(connection, tmp_path / "archive", digest)
    operator.restore(connection, tmp_path / "archive", digest)
    assert (
        str(connection.execute("select amount from retention_test.fact where id=1").fetchone()[0])
        == "1.1234567890123456789"
    )


def test_drift_and_corruption_stop_without_deleting(operator, connection, tmp_path):
    prepare(operator, connection, tmp_path)
    digest = operator.verify_archive(tmp_path / "archive")
    connection.execute("update retention_test.fact set note='changed' where id=1")
    with pytest.raises(RuntimeError, match="row_changed"):
        operator.apply(connection, tmp_path / "archive", digest)
    assert connection.execute("select count(*) from retention_test.fact").fetchone()[0] == 3


def test_archive_swapped_after_verification_is_not_used(
    operator, connection, tmp_path, monkeypatch
):
    prepare(operator, connection, tmp_path)
    digest = operator.verify_archive(tmp_path / "archive")
    original = operator.metadata

    def swap(c, table):
        path = tmp_path / "archive" / "rows.jsonl.gz"
        try:
            path.rename(path.with_suffix(".original"))
        except PermissionError:
            # Windows denies replacement of the verified, still-open handle.
            pass
        else:
            path.write_bytes(b"replacement")
        return original(c, table)

    monkeypatch.setattr(operator, "metadata", swap)
    assert operator.apply(connection, tmp_path / "archive", digest)["deleted"] == 1


def test_wrong_database_identity_refused(operator, connection, tmp_path):
    prepare(operator, connection, tmp_path)
    path = tmp_path / "archive" / "manifest.json"
    plan = json.loads(path.read_bytes())
    plan["database_identity"] = ["other-cluster", 0]
    path.write_bytes(operator.encoded(plan))
    digest = operator.verify_archive(tmp_path / "archive")
    with pytest.raises(RuntimeError, match="database_changed"):
        operator.apply(connection, tmp_path / "archive", digest)
    with (tmp_path / "archive" / "rows.jsonl.gz").open("ab") as output:
        output.write(b"corrupt")
    with pytest.raises((RuntimeError, OSError, EOFError)):
        operator.apply(connection, tmp_path / "archive", digest)


def test_cascade_is_not_implicit_authorization(operator, connection, tmp_path):
    connection.execute("""create table retention_test.child (
        id integer primary key, parent integer references retention_test.fact(id)
        on delete cascade)""")
    connection.execute("insert into retention_test.child values (1,1)")
    prepare(operator, connection, tmp_path)
    digest = operator.verify_archive(tmp_path / "archive")
    with pytest.raises(RuntimeError, match="referenced_rows"):
        operator.apply(connection, tmp_path / "archive", digest)
    assert connection.execute("select count(*) from retention_test.child").fetchone()[0] == 1


def test_plan_scope_and_hash_cannot_be_changed(operator, connection, tmp_path):
    prepare(operator, connection, tmp_path)
    digest = operator.verify_archive(tmp_path / "archive")
    plan_path = tmp_path / "archive" / "manifest.json"
    plan = json.loads(plan_path.read_text())
    plan["predicate"] = "true"
    plan_path.write_text(json.dumps(plan))
    with pytest.raises(RuntimeError, match=r"scope|hash"):
        operator.apply(connection, tmp_path / "archive", digest)
    assert connection.execute("select count(*) from retention_test.fact").fetchone()[0] == 3


def test_partitioned_cleanup_resumes_committed_batches(operator, connection, tmp_path, monkeypatch):
    connection.execute("""create table retention_test.partitioned (
        id int, trade_date date, amount numeric, primary key (trade_date,id))
        partition by range(trade_date)""")
    connection.execute("""create table retention_test.old_part
        partition of retention_test.partitioned
        for values from ('2026-08-01') to ('2026-09-23')""")
    connection.execute("""create table retention_test.current_part
        partition of retention_test.partitioned
        for values from ('2026-09-23') to ('2026-11-01')""")
    connection.execute("""insert into retention_test.partitioned
        select n,'2026-09-22',1.123456789 from generate_series(1,1003) n""")
    connection.execute("insert into retention_test.partitioned values(1,'2026-09-23',null)")
    operator.TARGETS["retention_test.partitioned"] = "t.trade_date < DATE '2026-09-23'"
    directory = tmp_path / "partitioned"
    operator.archive(connection, "retention_test.partitioned", directory)
    digest = operator.verify_archive(directory)

    def interrupt(_):
        raise RuntimeError("synthetic_interruption_after_commit")

    with monkeypatch.context() as patch:
        patch.setattr(operator.time, "sleep", interrupt)
        with pytest.raises(RuntimeError, match="synthetic_interruption"):
            operator.apply(connection, directory, digest)
    assert operator.apply(connection, directory, digest) == {"deleted": 3, "already_absent": 1000}
    assert connection.execute("select id from retention_test.current_part").fetchall() == [(1,)]
    operator.restore(connection, directory, digest)
    operator.restore(connection, directory, digest)
    assert (
        connection.execute("select count(*) from retention_test.partitioned").fetchone()[0] == 1004
    )
