alter table ingestion.ingestion_run
    drop constraint ingestion_run_provider_check,
    add constraint ingestion_run_provider_check
        check (provider_code in ('baostock', 'akshare'));

alter table core.security
    drop constraint security_source_check,
    add constraint security_source_check
        check (source_code in ('baostock', 'akshare'));

alter table core.security_name_history
    drop constraint security_name_history_source_check,
    add constraint security_name_history_source_check
        check (source_code in ('baostock', 'akshare'));

alter table core.trading_calendar
    drop constraint trading_calendar_source_check,
    add constraint trading_calendar_source_check
        check (source_code in ('baostock', 'akshare'));

alter table core.daily_bar
    drop constraint daily_bar_source_check,
    add constraint daily_bar_source_check
        check (source_code in ('baostock', 'akshare'));
