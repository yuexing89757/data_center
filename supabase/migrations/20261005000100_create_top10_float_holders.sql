-- ADR-0064: Tushare top-10 float-holder point-in-time facts, weekly Saturday sync.
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
        'regulation_event','regulation_st_snapshot','top10_float_holder'
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
        'regulation_event','regulation_st_snapshot','top10_float_holder'
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
        'call_auction_market_series_archive','top10_float_holders_weekly'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table core.top10_float_holder (
    symbol text not null references core.security (symbol),
    end_date date not null,
    announcement_date date not null,
    holder_rank smallint not null,
    holder_name text not null,
    hold_amount bigint not null,
    hold_ratio numeric(10, 4),
    hold_float_ratio numeric(10, 4),
    hold_change bigint,
    holder_type text,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    first_observed_at timestamptz not null default now(),
    primary key (symbol, end_date, announcement_date, holder_rank),
    constraint top10_float_holder_unique_name
        unique (symbol, end_date, announcement_date, holder_name),
    constraint top10_float_holder_rank_check check (holder_rank between 1 and 10),
    constraint top10_float_holder_amount_check check (hold_amount > 0),
    constraint top10_float_holder_date_order_check check (end_date <= announcement_date),
    constraint top10_float_holder_ratio_check check (
        (hold_ratio is null or hold_ratio >= 0)
        and (hold_float_ratio is null or hold_float_ratio >= 0)
    ),
    constraint top10_float_holder_name_check check (btrim(holder_name) <> ''),
    constraint top10_float_holder_source_check check (source_code = 'tushare')
);

create index top10_float_holder_announcement_idx
    on core.top10_float_holder (announcement_date);
create index top10_float_holder_symbol_idx
    on core.top10_float_holder (symbol, end_date desc, announcement_date desc);
create index top10_float_holder_ingestion_idx
    on core.top10_float_holder (ingestion_id);

alter table core.top10_float_holder enable row level security;
create policy top10_float_holder_worker_select on core.top10_float_holder
    for select to market_data_worker using (true);
create policy top10_float_holder_worker_insert on core.top10_float_holder
    for insert to market_data_worker with check (true);
revoke all on core.top10_float_holder from public, anon, authenticated, market_data_api;
grant select, insert on core.top10_float_holder to market_data_worker;

comment on table core.top10_float_holder is
    'Append-only top-10 float-holder facts per announcement revision (ADR-0064).';
