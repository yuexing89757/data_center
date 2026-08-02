"""Controlled workflow and job-definition catalog."""

from dataclasses import dataclass

from market_data_center.settings import SchedulerSettings

DAILY_RUN_JOB_ID = "daily-run"
STOCK_DAILY_INDICATOR_JOB_ID = "stock-daily-indicators-daily"
STALE_RUN_RECOVERY_JOB_ID = "recover-stale-ingestion-runs"
DEDUCTED_PROFIT_JOB_ID = "deducted-profit-daily"
STOCK_POOL_JOB_ID = "mainboard-price-limit-stock-pools-daily"


@dataclass(frozen=True, slots=True)
class WorkflowDefinition:
    code: str
    display_name: str
    description: str
    step_codes: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class JobDefinition:
    code: str
    display_name: str
    description: str
    workflow_code: str
    trigger_type: str
    schedule_description: str
    timezone: str
    enabled: bool
    timeout_seconds: int
    recovery_policy: str
    day_of_week: str | None = None
    hour: int | None = None
    minute: int | None = None
    interval_hours: int | None = None


WORKFLOW_DEFINITIONS = (
    WorkflowDefinition(
        "daily_market",
        "日 K 与基础数据更新",
        "同步证券、交易日历和本地 pytdx 未复权日 K。",
        ("security", "trading_calendar", "daily_bar"),
    ),
    WorkflowDefinition(
        "stock_daily_indicator",
        "股票每日指标更新",
        "同步交易日历、全市场每日指标并执行安全保留。",
        ("trading_calendar", "stock_daily_indicator", "retention"),
    ),
    WorkflowDefinition(
        "stale_run_recovery",
        "陈旧运行恢复",
        "恢复超时停留在 running 的采集和工作流记录。",
        ("recover_ingestion_runs", "recover_workflow_runs"),
    ),
    WorkflowDefinition(
        "deducted_profit",
        "扣非净利润增量同步",
        "发现新公告或修订并同步扣非净利润点时事实。",
        ("deducted_profit",),
    ),
    WorkflowDefinition(
        "stock_pool",
        "沪深主板昨日涨跌停股票池",
        "在日 K 与每日指标成功后构建下一交易日生效的不可变涨跌停股票池。",
        ("build_stock_pools",),
    ),
)


def job_definitions(settings: SchedulerSettings) -> tuple[JobDefinition, ...]:
    timezone = settings.scheduler_timezone
    timeout = settings.scheduler_misfire_grace_seconds
    return (
        JobDefinition(
            DAILY_RUN_JOB_ID,
            "日 K 与基础数据更新",
            "运行 daily_market 工作流。",
            "daily_market",
            "cron",
            f"周一至周五 {settings.daily_run_hour:02d}:{settings.daily_run_minute:02d}",
            timezone,
            True,
            timeout,
            "启动及每小时恢复超过 60 分钟的 running 记录",
            day_of_week="mon-fri",
            hour=settings.daily_run_hour,
            minute=settings.daily_run_minute,
        ),
        JobDefinition(
            STOCK_DAILY_INDICATOR_JOB_ID,
            "股票每日指标更新",
            "运行 stock_daily_indicator 工作流。",
            "stock_daily_indicator",
            "cron",
            f"周一至周五 {settings.stock_daily_indicator_hour:02d}:"
            f"{settings.stock_daily_indicator_minute:02d}",
            timezone,
            True,
            timeout,
            "启动及每小时恢复超过 60 分钟的 running 记录",
            day_of_week="mon-fri",
            hour=settings.stock_daily_indicator_hour,
            minute=settings.stock_daily_indicator_minute,
        ),
        JobDefinition(
            STOCK_POOL_JOB_ID,
            "沪深主板昨日涨跌停股票池",
            "构建涨停与跌停两份对称的不可变股票池快照。",
            "stock_pool",
            "cron",
            f"周一至周五 {settings.stock_pool_hour:02d}:{settings.stock_pool_minute:02d}",
            timezone,
            True,
            timeout,
            "缺少同日成功的日 K/每日指标 workflow 时失败; 下一次调度或手工命令重试",
            day_of_week="mon-fri",
            hour=settings.stock_pool_hour,
            minute=settings.stock_pool_minute,
        ),
        JobDefinition(
            DEDUCTED_PROFIT_JOB_ID,
            "扣非净利润增量同步",
            "按披露变化增量同步累计和单季度扣非净利润。",
            "deducted_profit",
            "cron",
            f"每天 {settings.deducted_profit_hour:02d}:{settings.deducted_profit_minute:02d}",
            timezone,
            True,
            timeout,
            "启动及每小时恢复超过 60 分钟的 running 记录",
            hour=settings.deducted_profit_hour,
            minute=settings.deducted_profit_minute,
        ),
        JobDefinition(
            STALE_RUN_RECOVERY_JOB_ID,
            "陈旧运行恢复",
            "恢复中断的采集与 operations 记录。",
            "stale_run_recovery",
            "interval",
            "每 1 小时",
            timezone,
            True,
            timeout,
            "失败后等待下一小时重试",
            interval_hours=1,
        ),
    )


def workflow_definition(code: str) -> WorkflowDefinition:
    return next(item for item in WORKFLOW_DEFINITIONS if item.code == code)
