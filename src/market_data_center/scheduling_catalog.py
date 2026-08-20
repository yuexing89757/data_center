"""Controlled workflow and job-definition catalog."""

from dataclasses import dataclass

from market_data_center.settings import SchedulerSettings

DAILY_RUN_JOB_ID = "daily-run"
STOCK_DAILY_INDICATOR_JOB_ID = "stock-daily-indicators-daily"
STALE_RUN_RECOVERY_JOB_ID = "recover-stale-ingestion-runs"
DEDUCTED_PROFIT_JOB_ID = "deducted-profit-daily"
STOCK_POOL_JOB_ID = "mainboard-price-limit-stock-pools-daily"
EOD_QUOTE_SNAPSHOT_JOB_ID = "eod-quote-snapshot-daily"
CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID = "call-auction-market-snapshot-daily"
CALL_AUCTION_MARKET_SERIES_JOB_ID = "call-auction-market-series"
TODAY_LIMIT_UP_SNAPSHOT_JOB_ID = "today-limit-up-snapshot-daily"
PYTDX_POOL_REFRESH_JOB_ID = "pytdx-pool-refresh"
CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID = "close-price-new-highs-120d-daily"
BOARD_INDEX_DAILY_BAR_JOB_ID = "board-index-883423-daily-bar"
SCHEDULER_TIMEZONE = "Asia/Shanghai"
JOB_TIMEOUT_SECONDS = 21_600


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
    hour: int | str | None = None
    minute: int | None = None
    second: int | None = None
    interval_hours: int | None = None
    cadence_seconds: int | None = None


WORKFLOW_DEFINITIONS = (
    WorkflowDefinition(
        "daily_market",
        "日 K 与基础数据更新",
        "同步证券、交易日历和远程 pytdx 未复权日 K。",
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
        (
            "recover_ingestion_runs",
            "recover_workflow_runs",
            "recover_auction_sessions",
            "recover_call_auction_market_series_sessions",
        ),
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
    WorkflowDefinition(
        "auction_collection",
        "集合竞价涨停池五档采集",
        "冻结当日精确涨停池快照, 并在 09:15-09:25 内按固定节奏采集。",
        ("collect_auction_quotes",),
    ),
    WorkflowDefinition(
        "eod_quote_snapshot",
        "收盘五档快照",
        "当日日 K、每日指标和涨停池完成后采集收盘五档快照, 计算封单金额。",
        ("collect_eod_quotes",),
    ),
    WorkflowDefinition(
        "call_auction_market_snapshot",
        "沪深全市场开盘竞价快照",
        "在开盘集合竞价结束后采集沪深上市股票的完整来源快照。",
        ("collect_call_auction_market_snapshot",),
    ),
    WorkflowDefinition(
        "call_auction_market_series",
        "沪深全市场开盘竞价序列快照",
        "在开盘集合竞价期间按固定轮次采集沪深上市股票的完整来源快照。",
        ("collect_call_auction_market_series",),
    ),
    WorkflowDefinition(
        "call_auction_snapshot",
        "今日竞价量",
        "保留数据库最终化的历史 operations 定义, Worker 不再自动调度。",
        ("finalize_call_auction_snapshot",),
    ),
    WorkflowDefinition(
        "today_limit_up_snapshot",
        "Same-day immutable limit-up snapshot",
        "Build a versioned snapshot after exact-date upstream dependency checks.",
        ("fill_today_limit_up_snapshot",),
    ),
    WorkflowDefinition(
        "pytdx_pool_refresh",
        "PYTDX 节点池刷新",
        "探测候选节点能力并原子发布最后有效节点池。",
        ("refresh_pytdx_pool",),
    ),
    WorkflowDefinition(
        "close_price_new_highs_120d",
        "沪深120交易日收盘新高快照",
        "在日 K 完成后构建版本化沪深120交易日收盘新高快照。",
        ("build_close_price_new_highs_120d_snapshot",),
    ),
    WorkflowDefinition(
        "board_index_daily_bar",
        "883423 板块日线收盘采集",
        "收盘后采集固定同花顺板块 THS:883423 日线, 并补齐尾部缺口。",
        ("collect_board_index_daily_bars",),
    ),
)


def job_definitions(settings: SchedulerSettings) -> tuple[JobDefinition, ...]:
    timezone = SCHEDULER_TIMEZONE
    timeout = JOB_TIMEOUT_SECONDS
    return (
        JobDefinition(
            DAILY_RUN_JOB_ID,
            "日 K 与基础数据更新",
            "运行 daily_market 工作流。",
            "daily_market",
            "cron",
            "周一至周五 20:00",
            timezone,
            True,
            timeout,
            "启动及每小时恢复超过 60 分钟的 running 记录",
            day_of_week="mon-fri",
            hour=20,
            minute=0,
        ),
        JobDefinition(
            STOCK_DAILY_INDICATOR_JOB_ID,
            "股票每日指标更新",
            "运行 stock_daily_indicator 工作流。",
            "stock_daily_indicator",
            "cron",
            "周一至周五 20:30",
            timezone,
            True,
            timeout,
            "启动及每小时恢复超过 60 分钟的 running 记录",
            day_of_week="mon-fri",
            hour=20,
            minute=30,
        ),
        JobDefinition(
            STOCK_POOL_JOB_ID,
            "沪深主板昨日涨跌停股票池",
            "构建涨停与跌停两份对称的不可变股票池快照。",
            "stock_pool",
            "cron",
            "周一至周五 21:00",
            timezone,
            True,
            timeout,
            "缺少同日成功的日 K/每日指标 workflow 时失败; 下一次调度或手工命令重试",
            day_of_week="mon-fri",
            hour=21,
            minute=0,
        ),
        JobDefinition(
            DEDUCTED_PROFIT_JOB_ID,
            "扣非净利润增量同步",
            "按披露变化增量同步累计和单季度扣非净利润。",
            "deducted_profit",
            "cron",
            "每天 20:00",
            timezone,
            True,
            timeout,
            "启动及每小时恢复超过 60 分钟的 running 记录",
            hour=20,
            minute=0,
        ),
        JobDefinition(
            EOD_QUOTE_SNAPSHOT_JOB_ID,
            "收盘五档快照",
            "当日涨停池 ready 后采集其成员的收盘五档行情, 计算涨停封单金额。",
            "eod_quote_snapshot",
            "cron",
            "周一至周五 21:10",
            timezone,
            settings.eod_quote_snapshot_enabled,
            timeout,
            "当日 ready 涨停池缺失时失败; 不使用旧池或当前报价补历史数据",
            day_of_week="mon-fri",
            hour=21,
            minute=10,
        ),
        JobDefinition(
            CALL_AUCTION_MARKET_SNAPSHOT_JOB_ID,
            "沪深全市场开盘竞价快照",
            "采集沪深上市股票在开盘集合竞价结束后、连续竞价前的完整五档来源快照。",
            "call_auction_market_snapshot",
            "cron",
            "周一至周五 09:25:30",
            timezone,
            settings.call_auction_snapshot_enabled,
            timeout,
            "只在当日 09:25-09:30 窗口内采集; 失败保持显式缺口, 不盘后补采。",
            day_of_week="mon-fri",
            hour=9,
            minute=25,
            second=30,
        ),
        JobDefinition(
            CALL_AUCTION_MARKET_SERIES_JOB_ID,
            "沪深全市场开盘竞价序列快照",
            "09:15-09:25:20 每20秒保存一次沪深上市股票全集来源事实。",
            "call_auction_market_series",
            "cron",
            "周一至周五 09:15",
            timezone,
            settings.call_auction_market_series_enabled,
            timeout,
            "错过轮次显式失败, 不补采。",
            day_of_week="mon-fri",
            hour=9,
            minute=15,
        ),
        JobDefinition(
            TODAY_LIMIT_UP_SNAPSHOT_JOB_ID,
            "Same-day limit-up snapshot fill",
            "Freeze a versioned snapshot after exact-date bar, share and pool checks.",
            "today_limit_up_snapshot",
            "cron",
            "Monday-Friday 22:00",
            timezone,
            settings.today_limit_up_snapshot_enabled,
            timeout,
            "Record deferred/partial for incomplete upstreams; never publish false ready state",
            day_of_week="mon-fri",
            hour=22,
            minute=0,
        ),
        JobDefinition(
            CLOSE_PRICE_NEW_HIGHS_120D_JOB_ID,
            "沪深120交易日收盘新高快照",
            "物化最近交易日严格突破此前119日最高收盘的沪深股票。",
            "close_price_new_highs_120d",
            "cron",
            "周一至周五 21:30",
            timezone,
            settings.close_price_new_highs_120d_enabled,
            timeout,
            "同日 daily_market 未终态时失败; 下一次调度或显式日期手工命令重试",
            day_of_week="mon-fri",
            hour=21,
            minute=30,
        ),
        JobDefinition(
            BOARD_INDEX_DAILY_BAR_JOB_ID,
            "883423 板块日线收盘采集",
            "在三个收盘后时点幂等采集 THS:883423 日线。",
            "board_index_daily_bar",
            "cron",
            "周一至周五 15:30、16:30、17:30",
            timezone,
            settings.board_index_daily_bar_enabled,
            timeout,
            "每轮最多三次 Provider 短重试; 后续时点及下一交易日继续补采缺口",
            day_of_week="mon-fri",
            hour="15-17",
            minute=30,
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
        JobDefinition(
            PYTDX_POOL_REFRESH_JOB_ID,
            "PYTDX 节点池刷新",
            "有界探测节点能力, 成功时原子发布, 失败时保留 last-good。",
            "pytdx_pool_refresh",
            "interval",
            "每 12 小时",
            timezone,
            True,
            timeout,
            "刷新失败保留 last-good; 新旧池均无效时 Worker 启动失败",
            interval_hours=12,
        ),
    )


def job_definition(code: str, settings: SchedulerSettings) -> JobDefinition:
    return next(item for item in job_definitions(settings) if item.code == code)


def workflow_definition(code: str) -> WorkflowDefinition:
    return next(item for item in WORKFLOW_DEFINITIONS if item.code == code)
