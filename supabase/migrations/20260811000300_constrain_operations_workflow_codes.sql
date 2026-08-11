alter table operations.workflow_run
    add constraint workflow_run_workflow_code_check
    check (workflow_code in (
        'daily_market',
        'stock_daily_indicator',
        'stale_run_recovery',
        'deducted_profit',
        'stock_pool',
        'auction_collection',
        'eod_quote_snapshot',
        'call_auction_snapshot',
        'pytdx_pool_refresh'
    )) not valid;

alter table operations.workflow_run
    validate constraint workflow_run_workflow_code_check;
