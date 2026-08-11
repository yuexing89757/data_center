create table realtime.call_auction_market_snapshot (
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    symbol text not null references core.security (symbol),
    trade_date date not null,
    observed_at timestamptz not null,
    last_price numeric(18, 4),
    previous_close numeric(18, 4),
    high_price numeric(18, 4),
    low_price numeric(18, 4),
    cumulative_volume bigint,
    cumulative_amount numeric(30, 4),
    source_code text not null check (source_code = 'pytdx_hq'),
    created_at timestamptz not null default now(),
    primary key (ingestion_id, symbol),
    constraint call_auction_market_price_range check (
        (high_price is null or high_price >= 0)
        and (low_price is null or low_price >= 0)
        and (high_price is null or low_price is null or high_price >= low_price)
        and (last_price is null or low_price is null or last_price >= low_price)
        and (last_price is null or high_price is null or last_price <= high_price)
    ),
    constraint call_auction_market_nonnegative check (
        (last_price is null or last_price >= 0)
        and (previous_close is null or previous_close >= 0)
        and (cumulative_volume is null or cumulative_volume >= 0)
        and (cumulative_amount is null or cumulative_amount >= 0)
    ),
    constraint call_auction_market_observation_window check (
        (observed_at at time zone 'Asia/Shanghai')::date = trade_date
        and (observed_at at time zone 'Asia/Shanghai')::time >= time '09:25:00'
        and (observed_at at time zone 'Asia/Shanghai')::time < time '09:30:00'
    )
);

create index call_auction_market_snapshot_trade_date_ingestion_symbol_idx
    on realtime.call_auction_market_snapshot (trade_date, ingestion_id, symbol);

alter table realtime.call_auction_market_snapshot enable row level security;
create policy call_auction_market_snapshot_worker_select
    on realtime.call_auction_market_snapshot
    for select to market_data_worker using (true);
create policy call_auction_market_snapshot_worker_insert
    on realtime.call_auction_market_snapshot
    for insert to market_data_worker with check (true);
grant select, insert on realtime.call_auction_market_snapshot to market_data_worker;

alter table realtime.call_auction_snapshot
    add column observed_at timestamptz;
grant delete on realtime.call_auction_snapshot to market_data_worker;

alter table ingestion.ingestion_run
    drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run
    add constraint ingestion_run_dataset_check check (
        dataset_code in (
            'security','trading_calendar','daily_bar','capital','classification_catalog',
            'classification_members','board_index','board_index_daily_bar',
            'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
            'five_level_quote','convertible_bond','convertible_bond_daily_bar',
            'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot'
        )
    ) not valid;
alter table ingestion.ingestion_run
    validate constraint ingestion_run_dataset_check;

alter table audit.quality_result
    drop constraint quality_result_dataset_check;
alter table audit.quality_result
    add constraint quality_result_dataset_check check (
        dataset_code in (
            'security','trading_calendar','daily_bar','capital','classification_catalog',
            'classification_members','board_index','board_index_daily_bar',
            'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
            'five_level_quote','convertible_bond','convertible_bond_daily_bar',
            'eod_quote_snapshot','call_auction_snapshot','call_auction_market_snapshot'
        )
    ) not valid;
alter table audit.quality_result
    validate constraint quality_result_dataset_check;

alter table operations.workflow_run
    drop constraint workflow_run_workflow_code_check;
alter table operations.workflow_run
    add constraint workflow_run_workflow_code_check check (
        workflow_code in (
            'daily_market',
            'stock_daily_indicator',
            'stale_run_recovery',
            'deducted_profit',
            'stock_pool',
            'auction_collection',
            'eod_quote_snapshot',
            'call_auction_snapshot',
            'call_auction_market_snapshot',
            'pytdx_pool_refresh'
        )
    ) not valid;
alter table operations.workflow_run
    validate constraint workflow_run_workflow_code_check;
