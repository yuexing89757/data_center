from datetime import date, datetime
from pathlib import Path
from sqlite3 import connect
from typing import cast
from urllib.error import HTTPError
from urllib.request import Request, urlopen

import pytest

from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.scheduler import read_job_store_snapshot
from market_data_center.scheduling_catalog import (
    DAILY_RUN_JOB_ID,
    DEDUCTED_PROFIT_JOB_ID,
    STALE_RUN_RECOVERY_JOB_ID,
    STOCK_DAILY_INDICATOR_JOB_ID,
    STOCK_POOL_JOB_ID,
)
from market_data_center.settings import SchedulerSettings
from market_data_center.worker_admin import (
    ADMIN_PATH,
    LOOPBACK_HOST,
    render_scheduled_tasks_page,
    start_worker_admin_server,
)


class AdminPersistence:
    def stale_ingestion_run_ids(self, stale_before: datetime) -> list[str]:
        del stale_before
        return []

    def latest_stock_daily_indicator_snapshot(self) -> tuple[date, int]:
        return date.today(), 5_522


class AdminOperationsPersistence:
    def recent_workflows(self, limit: int = 10) -> tuple[object, ...]:
        del limit
        return ()


def _job_store(path: Path) -> None:
    with connect(path) as connection:
        connection.execute(
            "create table apscheduler_jobs "
            "(id varchar primary key, next_run_time float, job_state blob)"
        )
        connection.executemany(
            "insert into apscheduler_jobs values (?, ?, ?)",
            [
                (DAILY_RUN_JOB_ID, 1_785_666_600.0, b"must-not-be-read"),
                (DEDUCTED_PROFIT_JOB_ID, 1_785_672_000.0, b"must-not-be-read"),
                (STALE_RUN_RECOVERY_JOB_ID, 1_785_670_200.0, b"must-not-be-read"),
                (STOCK_DAILY_INDICATOR_JOB_ID, None, b"must-not-be-read"),
                (STOCK_POOL_JOB_ID, 1_785_672_600.0, b"must-not-be-read"),
            ],
        )


def test_job_store_snapshot_is_read_only_and_does_not_expose_job_state(tmp_path: Path) -> None:
    store_path = tmp_path / "jobs.sqlite"
    _job_store(store_path)
    settings = SchedulerSettings(scheduler_store_path=store_path)

    snapshot = read_job_store_snapshot(settings)

    assert snapshot.available
    assert [task.task_id for task in snapshot.tasks] == [
        DAILY_RUN_JOB_ID,
        DEDUCTED_PROFIT_JOB_ID,
        STOCK_POOL_JOB_ID,
        STALE_RUN_RECOVERY_JOB_ID,
        STOCK_DAILY_INDICATOR_JOB_ID,
    ]
    assert snapshot.tasks[0].next_run_time is not None
    assert snapshot.tasks[-1].next_run_time is None


def test_admin_page_distinguishes_persistence_from_worker_liveness(tmp_path: Path) -> None:
    store_path = tmp_path / "jobs.sqlite"
    _job_store(store_path)
    settings = SchedulerSettings(scheduler_store_path=store_path)

    page = render_scheduled_tasks_page(
        settings,
        cast(PostgreSQLPersistence, AdminPersistence()),
        worker_running=False,
    ).decode()

    assert "Worker: <strong>未运行</strong>" in page
    assert "日 K 与基础数据更新" in page
    assert "周一至周五 18:30 (Asia/Shanghai)" in page
    assert "已持久化" in page
    assert "job_state" not in page
    assert str(store_path) not in page
    assert "must-not-be-read" not in page
    assert "立即执行" not in page
    assert "删除" not in page


def test_admin_http_is_loopback_read_only_and_sets_security_headers(tmp_path: Path) -> None:
    store_path = tmp_path / "jobs.sqlite"
    _job_store(store_path)
    settings = SchedulerSettings(scheduler_store_path=store_path)
    server = start_worker_admin_server(
        settings,
        cast(PostgreSQLPersistence, AdminPersistence()),
        cast(PostgreSQLOperationsPersistence, AdminOperationsPersistence()),
        port=0,
    )
    try:
        host = str(server.server_address[0])
        port = int(server.server_address[1])
        assert host == LOOPBACK_HOST
        with urlopen(f"http://{host}:{port}{ADMIN_PATH}") as response:
            assert response.status == 200
            assert response.headers["Cache-Control"] == "no-store"
            assert response.headers["X-Frame-Options"] == "DENY"
            assert "Worker: <strong>正在运行</strong>" in response.read().decode()
        with pytest.raises(HTTPError) as error:
            urlopen(Request(f"http://{host}:{port}{ADMIN_PATH}", method="POST"))
        assert error.value.code == 405
    finally:
        server.shutdown()
        server.server_close()
