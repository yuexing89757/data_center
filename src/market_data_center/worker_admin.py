"""Loopback-only, read-only HTML administration for the collection Worker."""

from datetime import datetime
from html import escape
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from logging import getLogger
from threading import Thread
from zoneinfo import ZoneInfo

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
SHANGHAI_TZ = ZoneInfo("Asia/Shanghai")
LOGGER = getLogger(__name__)


def _iso(value: datetime | None) -> str | None:
    """Return the ISO string of a datetime, or None when missing."""
    return value.isoformat() if value is not None else None


def _format_local_time(value: str | None) -> str:
    """Render an ISO timestamp as a concise Asia/Shanghai wall-clock string.

    Accepts the ISO strings produced by ``read_job_store_snapshot`` (already
    localized) and by ``WorkflowRun.started_at/finished_at.isoformat()``
    (UTC-aware). Returns ``—`` for missing values. Falls back to the raw
    string when parsing fails so the page never breaks on an unexpected
    timestamp shape.
    """
    if not value:
        return "—"
    try:
        return datetime.fromisoformat(value).astimezone(SHANGHAI_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return escape(value)


def _status_cell(status: str) -> str:
    """Wrap a workflow status in a colored badge cell."""
    classes = {
        "succeeded": "badge badge-ok",
        "failed": "badge badge-err",
        "partial": "badge badge-warn",
        "running": "badge badge-run",
    }
    cls = classes.get(status, "badge")
    return f'<td><span class="{cls}">{escape(status)}</span></td>'


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
        state_badge = "启用" if definition.enabled else "停用"
        state_class = "badge badge-on" if definition.enabled else "badge badge-off"
        recovery = f"{definition.timeout_seconds}s; {escape(definition.recovery_policy)}"
        next_run = _format_local_time(state.next_run_time) if state else "—"
        rows.append(
            "<tr>"
            f"<td><code>{escape(definition.code)}</code></td>"
            f"<td>{escape(definition.display_name)}</td>"
            f'<td class="desc">{escape(definition.description)}</td>'
            f"<td><code>{escape(definition.workflow_code)}</code></td>"
            f'<td class="steps">{escape(" → ".join(workflow.step_codes))}</td>'
            f"<td>{escape(definition.trigger_type)}</td>"
            f"<td>{escape(definition.schedule_description)} (Asia/Shanghai)</td>"
            f'<td><span class="{state_class}">{state_badge}</span></td>'
            f'<td class="desc">{recovery}</td>'
            f'<td class="mono">{next_run}</td>'
            f"<td>{'是' if state else '否'}</td>"
            "</tr>"
        )
    worker_label = "正在运行" if worker_running else "未运行"
    worker_cls = "ok" if worker_running else "err"
    health_ok = health is not None and health.healthy
    health_label = "健康" if health_ok else "需要检查"
    health_cls = "ok" if health_ok else "warn"
    store_label = "可读取" if job_store.available else "不可读取"
    store_cls = "ok" if job_store.available else "err"
    stale_run_count = str(health.stale_run_count) if health is not None else "不可用"
    stale_ok = health is not None and health.stale_run_count == 0
    stale_cls = "ok" if stale_ok else "warn"
    latest_snapshot = health.latest_snapshot_date if health is not None else None
    latest_snapshot = latest_snapshot or "无"
    summary_cards = [
        (worker_cls, "Worker", worker_label),
        (health_cls, "调度健康", health_label),
        (store_cls, "JobStore", store_label),
        (stale_cls, "陈旧运行", stale_run_count),
        ("", "最近指标快照", escape(latest_snapshot)),
    ]
    summary_html = "".join(
        f'<div class="card s-{cls}"><span class="k">{key}</span><strong>{val}</strong></div>'
        for cls, key, val in summary_cards
    )
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
        (
            "<tr>"
            f"<td><code>{escape(run.workflow_code.value)}</code></td>"
            + _status_cell(run.status.value)
            + f"<td>{run.attempt}</td>"
            f"<td>{escape(run.trigger_source.value)}</td>"
            f'<td class="mono">{_format_local_time(run.started_at.isoformat())}</td>'
            f'<td class="mono">{_format_local_time(_iso(run.finished_at))}</td>'
            f"<td>{run.accepted_rows}/{run.rejected_rows}</td>"
            f'<td class="desc">{escape(run.error_summary) if run.error_summary else "—"}</td>'
            "</tr>"
        )
        for run in recent_runs
    )
    html = f"""<!doctype html>
<html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Data Center 定时任务</title>
<style>
:root{{--ok:#16a34a;--err:#dc2626;--warn:#d97706;--run:#2563eb;--line:#e5e7eb;--bg:#f8fafc;
--head:#f1f5f9;--mut:#64748b}}
*{{box-sizing:border-box}}
body{{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;max-width:1180px;margin:0 auto;
padding:28px 20px 48px;color:#0f172a;background:#fff}}
header{{margin-bottom:8px}}
h1{{margin:0 0 4px;font-size:1.45rem}}
.subtitle{{color:var(--mut);font-size:.9rem;margin:0 0 4px}}
.summary{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));
gap:10px;margin:18px 0 26px}}
.card{{border:1px solid var(--line);border-left-width:3px;border-radius:8px;padding:10px 14px;
background:var(--bg);font-size:.9rem}}
.card .k{{color:var(--mut);display:block;margin-bottom:2px}}
.card strong{{font-size:1.02rem}}
.card.s-ok{{border-left-color:var(--ok)}} .card.s-ok strong{{color:var(--ok)}}
.card.s-err{{border-left-color:var(--err)}} .card.s-err strong{{color:var(--err)}}
.card.s-warn{{border-left-color:var(--warn)}} .card.s-warn strong{{color:var(--warn)}}
h2{{font-size:1.1rem;margin:28px 0 10px;border-bottom:2px solid var(--head);padding-bottom:6px}}
.table-wrap{{overflow-x:auto;border:1px solid var(--line);border-radius:8px;margin-bottom:8px}}
table{{width:100%;border-collapse:collapse;font-size:.86rem}}
th,td{{padding:8px 10px;border-bottom:1px solid var(--line);text-align:left;vertical-align:top}}
thead th{{background:var(--head);font-weight:600;white-space:nowrap;position:sticky;top:0}}
tbody tr:hover{{background:#fafbfc}}
td.desc,td.steps{{color:var(--mut);font-size:.82rem}}
td.mono,code{{font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:.82rem}}
.badge{{display:inline-block;padding:2px 9px;border-radius:999px;font-size:.76rem;
font-weight:600;line-height:1.5}}
.badge-on{{background:#dcfce7;color:#15803d}} .badge-off{{background:#f1f5f9;color:#64748b}}
.badge-ok{{background:#dcfce7;color:#15803d}} .badge-err{{background:#fee2e2;color:#b91c1c}}
.badge-warn{{background:#fef3c7;color:#b45309}} .badge-run{{background:#dbeafe;color:#1d4ed8}}
.note{{color:var(--mut);font-size:.8rem;margin-top:18px;line-height:1.6}}
</style></head><body>
<header>
<h1>定时任务</h1>
<p class="subtitle">只读本地管理页 · http://127.0.0.1:{settings.worker_admin_port}{ADMIN_PATH}</p>
</header>
<div class="summary">
{summary_html}
</div>
<h2>定时任务</h2>
<div class="table-wrap"><table><thead><tr>
<th>任务 ID</th><th>名称</th><th>说明</th><th>Workflow</th><th>步骤顺序</th>
<th>触发</th><th>计划</th><th>状态</th><th>超时/恢复</th>
<th>下次运行</th><th>已持久化</th>
</tr></thead>
<tbody>{"".join(rows)}</tbody></table></div>
<h2>最近工作流执行</h2>
<div class="table-wrap"><table><thead><tr>
<th>Workflow</th><th>状态</th><th>Attempt</th><th>触发来源</th>
<th>开始</th><th>结束</th><th>接受/拒绝</th><th>错误</th></tr></thead>
<tbody>{history_rows or '<tr><td colspan="8">暂无可用执行记录</td></tr>'}</tbody></table></div>
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
