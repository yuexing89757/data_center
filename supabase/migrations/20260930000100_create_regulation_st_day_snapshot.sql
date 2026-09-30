-- ADR-0061 / Issue #85: complete Tushare ST set per trading day.
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
        'regulation_event','regulation_st_snapshot'
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
        'regulation_event','regulation_st_snapshot'
    )
) not valid;
alter table audit.quality_result validate constraint quality_result_dataset_check;

create table regulation.st_day_snapshot (
    trade_date date primary key,
    symbols jsonb not null check (jsonb_typeof(symbols) = 'array'),
    symbol_count integer not null check (symbol_count between 1 and 999),
    source_code text not null check (source_code = 'tushare'),
    ingestion_id uuid not null references ingestion.ingestion_run(ingestion_id),
    created_at timestamptz not null default now(),
    check (jsonb_array_length(symbols) = symbol_count)
);
alter table regulation.st_day_snapshot enable row level security;
create policy st_day_snapshot_worker_select on regulation.st_day_snapshot
    for select to market_data_worker using (true);
create policy st_day_snapshot_worker_insert on regulation.st_day_snapshot
    for insert to market_data_worker with check (true);
create policy st_day_snapshot_worker_update on regulation.st_day_snapshot
    for update to market_data_worker using (true) with check (true);
revoke all on regulation.st_day_snapshot from public, anon, authenticated, market_data_api;
grant select, insert, update on regulation.st_day_snapshot to market_data_worker;
