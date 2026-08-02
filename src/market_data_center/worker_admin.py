"""Loopback-only, read-only HTML administration for the collection Worker."""

from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging import getLogger
from threading import Thread

from market_data_center.persistence import PostgreSQLPersistence
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.scheduler import (
    check_scheduler_health,
    read_job_store_snapshot,
)
from market_data_center.scheduling_catalog import job_definitions, workflow_definition
from market_data_center.settings import SchedulerSettings

ADMIN_PATH = "/admin/scheduled-tasks"
LOOPBACK_HOST = "127.0.0.1"
LOGGER = getLogger(__name__)


def render_scheduled_tasks_page(
    settings: SchedulerSettings,
    persistence: PostgreSQLPersistence,
    *,
    worker_running: bool,
    operations_persistence: PostgreSQLOperationsPersistence | None = None,
) -> bytes:
    definitions = job_definitions(settings)
    job_store = read_job_store_snapshot(settings)
    persisted = {task.task_id: task for task in job_store.tasks}
    try:
        health = check_scheduler_health(settings, persistence)
    except Exception:
        LOGGER.warning("worker admin health query unavailable")
        health = None
    rows = []
    for definition in definitions:
        state = persisted.get(definition.code)
        workflow = workflow_definition(definition.workflow_code)
        rows.append(
            "<tr>"
            f"<td><code>{escape(definition.code)}</code></td>"
            f"<td>{escape(definition.display_name)}</td>"
            f"<td>{escape(definition.description)}</td>"
            f"<td>{escape(definition.workflow_code)}</td>"
            f"<td>{escape(' → '.join(workflow.step_codes))}</td>"
            f"<td>{escape(definition.trigger_type)}</td>"
            f"<td>{escape(definition.schedule_description)} (Asia/Shanghai)</td>"
            f"<td>{'启用' if definition.enabled else '停用'}</td>"
            f"<td>{definition.timeout_seconds}s; {escape(definition.recovery_policy)}</td>"
            f"<td>{escape(state.next_run_time) if state and state.next_run_time else '—'}</td>"
            f"<td>{'是' if state else '否'}</td>"
            "</tr>"
        )
    worker_label = "正在运行" if worker_running else "未运行"
    health_label = "健康" if health is not None and health.healthy else "需要检查"
    store_label = "可读取" if job_store.available else "不可读取"
    stale_run_count = str(health.stale_run_count) if health is not None else "不可用"
    latest_snapshot = health.latest_snapshot_date if health is not None else None
    latest_snapshot = latest_snapshot or "无"
    try:
        recent_runs = (
            operations_persistence.recent_workflows(10)
            if operations_persistence is not None
            else ()
        )
    except Exception:
        LOGGER.warning("worker admin operations history unavailable")
        recent_runs = ()
    history_rows = "".join(
        "<tr>"
        f"<td><code>{escape(run.workflow_code.value)}</code></td>"
        f"<td>{escape(run.status.value)}</td>"
        f"<td>{run.attempt}</td>"
        f"<td>{escape(run.trigger_source.value)}</td>"
        f"<td>{escape(run.started_at.isoformat())}</td>"
        f"<td>{escape(run.finished_at.isoformat()) if run.finished_at else '—'}</td>"
        f"<td>{run.accepted_rows}/{run.rejected_rows}</td>"
        f"<td>{escape(run.error_summary) if run.error_summary else '—'}</td>"
        "</tr>"
        for run in recent_runs
    )
    html = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Data Center 定时任务</title>
<style>
body{{font-family:system-ui,sans-serif;max-width:1120px;margin:36px auto;
padding:0 20px;color:#17202a}}
h1{{margin-bottom:8px}} .summary{{display:flex;gap:12px;flex-wrap:wrap;margin:20px 0}}
.card{{border:1px solid #d9e1e8;border-radius:8px;padding:12px 16px;background:#f8fafc}}
table{{width:100%;border-collapse:collapse}}
th,td{{padding:10px;border-bottom:1px solid #d9e1e8;text-align:left}}
th{{background:#eef3f7}} code{{font-size:.92em}} .note{{color:#536471;margin-top:20px}}
</style></head><body>
<h1>定时任务</h1><p>只读本地管理页</p>
<div class="summary">
<div class="card">Worker: <strong>{worker_label}</strong></div>
<div class="card">调度健康: <strong>{health_label}</strong></div>
<div class="card">JobStore: <strong>{store_label}</strong></div>
<div class="card">陈旧运行: <strong>{stale_run_count}</strong></div>
<div class="card">最近指标快照: <strong>{escape(latest_snapshot)}</strong></div>
</div>
<table><thead><tr>
<th>任务 ID</th><th>名称</th><th>说明</th><th>Workflow</th><th>步骤顺序</th>
<th>触发类型</th><th>计划</th><th>状态</th><th>超时/恢复</th>
<th>下次运行</th><th>已持久化</th>
</tr></thead>
<tbody>{"".join(rows)}</tbody></table>
<h2>最近工作流执行</h2>
<table><thead><tr><th>Workflow</th><th>状态</th><th>Attempt</th><th>触发来源</th>
<th>开始</th><th>结束</th><th>接受/拒绝</th><th>错误类别</th></tr></thead>
<tbody>{history_rows or '<tr><td colspan="8">暂无可用执行记录</td></tr>'}</tbody></table>
<p class="note">“已持久化”只表示任务存在于 JobStore; “Worker 正在运行”表示本页面由
持有单实例锁的统一 Worker 提供。APScheduler JobStore 不保存可靠的任务执行历史,
本页不推断最近成功状态。</p>
</body></html>"""
    return html.encode("utf-8")


class WorkerAdminServer(ThreadingHTTPServer):
    daemon_threads = True


def start_worker_admin_server(
    settings: SchedulerSettings,
    persistence: PostgreSQLPersistence,
    operations_persistence: PostgreSQLOperationsPersistence,
    *,
    port: int | None = None,
) -> WorkerAdminServer:
    class Handler(BaseHTTPRequestHandler):
        server_version = "MarketDataCenter"
        sys_version = ""

        def _security_headers(self) -> None:
            self.send_header("Cache-Control", "no-store")
            self.send_header(
                "Content-Security-Policy", "default-src 'none'; style-src 'unsafe-inline'"
            )
            self.send_header("X-Content-Type-Options", "nosniff")
            self.send_header("X-Frame-Options", "DENY")
            self.send_header("Referrer-Policy", "no-referrer")

        def _empty_error(self, status: HTTPStatus) -> None:
            self.send_response(status)
            self.send_header("Content-Length", "0")
            self._security_headers()
            self.end_headers()

        def do_GET(self) -> None:
            if self.path != ADMIN_PATH:
                self._empty_error(HTTPStatus.NOT_FOUND)
                return
            payload = render_scheduled_tasks_page(
                settings,
                persistence,
                worker_running=True,
                operations_persistence=operations_persistence,
            )
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self._security_headers()
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:
            self._empty_error(HTTPStatus.METHOD_NOT_ALLOWED)

        def log_message(self, format: str, *args: object) -> None:
            LOGGER.info("worker admin: " + format, *args)

    server = WorkerAdminServer(
        (LOOPBACK_HOST, settings.worker_admin_port if port is None else port), Handler
    )
    Thread(target=server.serve_forever, name="worker-admin", daemon=True).start()
    LOGGER.info("worker admin listening on http://%s:%d%s", *server.server_address, ADMIN_PATH)
    return server
