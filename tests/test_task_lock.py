from contextlib import contextmanager
from threading import Event
from typing import Any, cast

import pytest
from sqlalchemy import Engine

import market_data_center.persistence.postgres as postgres


class LockConnection:
    def __init__(self, *, acquired=True, fail_ping=False, fail_unlock=False):
        self.acquired = acquired
        self.fail_ping = fail_ping
        self.fail_unlock = fail_unlock
        self.options = {}
        self.statements = []
        self.invalidated = False

    def execution_options(self, **options):
        self.options.update(options)
        return self

    def execute(self, statement, parameters=None):
        sql = str(statement)
        self.statements.append(sql)
        if ("select 1" in sql and self.fail_ping) or (
            "pg_advisory_unlock" in sql and self.fail_unlock
        ):
            raise OSError("connection lost")
        return self

    def scalar_one(self):
        return self.acquired

    def invalidate(self):
        self.invalidated = True


class LockEngine:
    def __init__(self, connection):
        self.connection = connection

    @contextmanager
    def connect(self):
        yield self.connection


def persistence(connection):
    return postgres.PostgreSQLPersistence(cast(Engine, cast(Any, LockEngine(connection))))


def test_task_lock_does_not_leave_an_idle_transaction_and_releases_session_lock():
    connection = LockConnection()
    with persistence(connection).task_lock("test"):
        assert connection.options.get("isolation_level") == "AUTOCOMMIT"
    assert sum("pg_advisory_unlock" in sql for sql in connection.statements) == 1


def test_contended_lock_is_not_unlocked_by_the_losing_worker():
    connection = LockConnection(acquired=False)
    with (
        pytest.raises(RuntimeError, match="already running"),
        persistence(connection).task_lock("test"),
    ):
        pytest.fail("must not enter")
    assert not any("pg_advisory_unlock" in sql for sql in connection.statements)


def test_lock_loss_stops_owner_and_never_reconnects_or_unlocks_a_different_session(monkeypatch):
    monkeypatch.setattr(postgres, "TASK_LOCK_HEARTBEAT_SECONDS", 0.01, raising=False)
    connection = LockConnection(fail_ping=True)
    stopped = Event()
    with (
        pytest.raises(RuntimeError, match="lock connection lost"),
        persistence(connection).task_lock("scheduler", on_lock_lost=stopped.set),
    ):
        assert stopped.wait(2)
    assert connection.invalidated
    assert not any("pg_advisory_unlock" in sql for sql in connection.statements)


def test_cleanup_failure_preserves_original_task_error():
    connection = LockConnection(fail_unlock=True)
    with pytest.raises(ValueError, match="original"), persistence(connection).task_lock("test"):
        raise ValueError("original")
    assert connection.invalidated


@pytest.mark.parametrize("phase", ["prepare", "before_start", "running"])
def test_worker_lock_loss_fails_and_cleans_up(monkeypatch, phase):
    from types import SimpleNamespace

    import market_data_center.scheduler as scheduler_module

    callback_done = Event()
    calls = []
    connection = LockConnection()
    engine = SimpleNamespace(
        connect=LockEngine(connection).connect, dispose=lambda: calls.append("dispose")
    )

    class TrackingPersistence(postgres.PostgreSQLPersistence):
        def task_lock(self, key, *, on_lock_lost=None):
            def callback():
                try:
                    on_lock_lost()
                finally:
                    callback_done.set()

            return super().task_lock(key, on_lock_lost=callback)

    def lose_lock():
        connection.fail_ping = True
        assert callback_done.wait(2)

    class Scheduler:
        running = False

        def add_listener(self, listener, mask):
            self.listener = listener
            if phase == "before_start":
                lose_lock()

        def start(self):
            calls.append("start")
            self.running = True
            self.listener(None)
            if phase == "running":
                lose_lock()
            assert not self.running

        def shutdown(self, wait):
            calls.append(("shutdown", wait))
            self.running = False

    scheduler = Scheduler()
    admin = SimpleNamespace(
        shutdown=lambda: calls.append("admin.shutdown"),
        server_close=lambda: calls.append("admin.close"),
    )

    def prepare(*args):
        if phase == "prepare":
            lose_lock()
        return scheduler, admin

    monkeypatch.setattr(postgres, "TASK_LOCK_HEARTBEAT_SECONDS", 0.001)
    monkeypatch.setattr(scheduler_module, "SchedulerSettings", lambda: object())
    monkeypatch.setattr(
        scheduler_module,
        "WorkerSettings",
        lambda: SimpleNamespace(database_url=SimpleNamespace(get_secret_value=lambda: "unused")),
    )
    monkeypatch.setattr(scheduler_module, "sqlalchemy_url", lambda value: value)
    monkeypatch.setattr(scheduler_module, "create_engine", lambda *args, **kwargs: engine)
    monkeypatch.setattr(scheduler_module, "PostgreSQLPersistence", TrackingPersistence)
    monkeypatch.setattr(scheduler_module, "signal", lambda *args: None)
    monkeypatch.setattr(scheduler_module, "run_stale_recovery_job", lambda: None)
    monkeypatch.setattr(scheduler_module, "prepare_locked_worker", prepare)

    with pytest.raises(RuntimeError, match="lock connection lost"):
        scheduler_module.run_worker()
    assert connection.invalidated
    assert not any("pg_advisory_unlock" in sql for sql in connection.statements)
    assert calls.count("dispose") == 1
    assert "admin.close" in calls
    if phase == "prepare":
        assert "start" not in calls
    else:
        assert calls.count(("shutdown", True)) == 1
