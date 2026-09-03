alter table operations.workflow_run drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run add constraint workflow_run_workflow_code_check check (
    workflow_code in (
        'daily_market','stock_daily_indicator','stale_run_recovery','deducted_profit',
        'shareholder_count_daily','shareholder_count_backfill','stock_pool',
        'auction_collection','eod_quote_snapshot','call_auction_snapshot',
        'call_auction_market_snapshot','call_auction_market_series','pytdx_pool_refresh',
        'today_limit_up_snapshot','close_price_new_highs_120d','board_index_daily_bar',
        'trading_billboard_daily','dragon_tiger_daily','regulation_daily_calculation',
        'data_cleanup'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create policy call_auction_market_series_snapshot_worker_delete
    on realtime.call_auction_market_series_snapshot
    for delete to market_data_worker using (true);

grant delete on realtime.call_auction_market_series_snapshot to market_data_worker;
