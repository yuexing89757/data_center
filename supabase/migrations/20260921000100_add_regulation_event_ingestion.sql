-- ADR-0048: official SSE/SZSE regulation-event ingestion and immutable Raw replay.
alter table ingestion.ingestion_run drop constraint ingestion_run_provider_check;
alter table ingestion.ingestion_run add constraint ingestion_run_provider_check check (
    provider_code in (
        'baostock','akshare','akshare_ths','pytdx','tushare','pytdx_hq','eastmoney',
        'pysnowball','tencent_quote','sse_official','szse_official'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_provider_check;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source','today_limit_down_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard','dragon_tiger',
        'regulation_event'
    )
) not valid;
alter table ingestion.ingestion_run validate constraint ingestion_run_dataset_check;

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'shareholder_count','five_level_quote','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series',
        'call_auction_indicative_detail','today_limit_up_source','today_limit_down_source',
        'convertible_bond','convertible_bond_daily_bar','trading_billboard','dragon_tiger',
        'regulation_event'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','today_limit_down_snapshot',
        'close_price_new_highs_120d','board_index_daily_bar','trading_billboard_daily',
        'security_bse_daily','dragon_tiger_daily','dragon_tiger_seat_profile',
        'regulation_daily_calculation','regulation_event_reconciliation','data_cleanup',
        'call_auction_market_series_archive'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

alter table regulation.event drop constraint regulation_event_direction_check;
alter table regulation.event add constraint regulation_event_direction_check check (
    direction is null or direction in ('UP', 'DOWN')
);

create index regulation_event_period_symbol_idx
    on regulation.event (period_end_date desc, symbol);
