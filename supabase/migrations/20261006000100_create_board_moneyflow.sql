-- ADR-0065: Eastmoney board daily money-flow facts, per trading day sync.
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
        'regulation_event','regulation_st_snapshot','top10_float_holder','board_moneyflow'
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
        'regulation_event','regulation_st_snapshot','top10_float_holder','board_moneyflow'
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
        'call_auction_market_series_archive','top10_float_holders_weekly',
        'board_moneyflow_daily'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table core.board_moneyflow (
    trade_date date not null,
    board_code text not null,
    board_type text not null,
    name text not null,
    pct_change numeric(10, 4) not null,
    close numeric(20, 4),
    net_amount numeric(20, 2) not null,
    net_amount_rate numeric(10, 4),
    extra_large_amount numeric(20, 2),
    extra_large_rate numeric(10, 4),
    large_amount numeric(20, 2),
    large_rate numeric(10, 4),
    medium_amount numeric(20, 2),
    medium_rate numeric(10, 4),
    small_amount numeric(20, 2),
    small_rate numeric(10, 4),
    top_small_buy_stock text,
    rank integer,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    first_observed_at timestamptz not null default now(),
    primary key (trade_date, board_code),
    constraint board_moneyflow_code_check check (board_code ~ '^DC:BK[0-9]{4}$'),
    constraint board_moneyflow_type_check check (
        board_type in ('concept', 'industry', 'region')
    ),
    constraint board_moneyflow_name_check check (btrim(name) <> ''),
    constraint board_moneyflow_rank_check check (rank is null or rank >= 1),
    constraint board_moneyflow_source_check check (source_code = 'tushare')
);

create index board_moneyflow_board_idx
    on core.board_moneyflow (board_code, trade_date desc);
create index board_moneyflow_ingestion_idx
    on core.board_moneyflow (ingestion_id);

alter table core.board_moneyflow enable row level security;
create policy board_moneyflow_worker_select on core.board_moneyflow
    for select to market_data_worker using (true);
create policy board_moneyflow_worker_insert on core.board_moneyflow
    for insert to market_data_worker with check (true);
revoke all on core.board_moneyflow from public, anon, authenticated, market_data_api;
grant select, insert on core.board_moneyflow to market_data_worker;

comment on table core.board_moneyflow is
    'Append-only Eastmoney board daily money-flow facts (ADR-0065).';
