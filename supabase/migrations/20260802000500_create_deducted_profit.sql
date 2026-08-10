alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security', 'trading_calendar', 'daily_bar', 'capital',
        'classification_catalog', 'classification_members', 'board_index',
        'board_index_daily_bar', 'board_index_constituent_snapshot',
        'stock_daily_indicator', 'deducted_profit'
    )
);

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security', 'trading_calendar', 'daily_bar', 'capital',
        'classification_catalog', 'classification_members', 'board_index',
        'board_index_daily_bar', 'board_index_constituent_snapshot',
        'stock_daily_indicator', 'deducted_profit'
    )
);

create table core.deducted_profit (
    symbol text not null references core.security (symbol),
    report_period date not null,
    announcement_date date not null,
    actual_announcement_date date,
    effective_announcement_date date generated always as (
        coalesce(actual_announcement_date, announcement_date)
    ) stored,
    cumulative_deducted_profit numeric(30, 4),
    quarterly_deducted_profit numeric(30, 4),
    update_flag text,
    revision_key text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    first_observed_at timestamptz not null default now(),
    primary key (symbol, report_period, revision_key),
    constraint deducted_profit_announcement_order check (
        announcement_date >= report_period
        and (actual_announcement_date is null or actual_announcement_date >= report_period)
    ),
    constraint deducted_profit_value_present check (
        cumulative_deducted_profit is not null or quarterly_deducted_profit is not null
    ),
    constraint deducted_profit_revision_key_check check (revision_key ~ '^[0-9a-f]{64}$'),
    constraint deducted_profit_source_check check (source_code = 'tushare')
);

create index deducted_profit_as_of_idx on core.deducted_profit (
    symbol, effective_announcement_date desc, first_observed_at desc
);
create index deducted_profit_ingestion_idx on core.deducted_profit (ingestion_id);

alter table core.deducted_profit enable row level security;
create policy deducted_profit_worker_all on core.deducted_profit
    for all to market_data_worker using (true) with check (true);
grant select, insert on core.deducted_profit to market_data_worker;

create or replace function api_v1.query_deducted_profits_as_of(
    p_as_of_date date,
    p_symbols text[] default null,
    p_limit integer default 500
)
returns table (
    symbol text,
    report_period date,
    announcement_date date,
    actual_announcement_date date,
    cumulative_deducted_profit numeric,
    quarterly_deducted_profit numeric,
    cumulative_deducted_profit_positive boolean,
    quarterly_deducted_profit_positive boolean,
    update_flag text
)
language sql stable security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
    select distinct on (profit.symbol)
        profit.symbol,
        profit.report_period,
        profit.announcement_date,
        profit.actual_announcement_date,
        profit.cumulative_deducted_profit,
        profit.quarterly_deducted_profit,
        profit.cumulative_deducted_profit > 0,
        profit.quarterly_deducted_profit > 0,
        profit.update_flag
    from core.deducted_profit as profit
    where profit.effective_announcement_date <= p_as_of_date
      and profit.first_observed_at < ((p_as_of_date + 1)::timestamp at time zone 'Asia/Shanghai')
      and (p_symbols is null or profit.symbol = any(p_symbols))
    order by profit.symbol, profit.effective_announcement_date desc,
             profit.first_observed_at desc, profit.revision_key desc
    limit least(greatest(p_limit, 1), 2000)
$$;

revoke all on function api_v1.query_deducted_profits_as_of(date, text[], integer)
    from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant execute on function api_v1.query_deducted_profits_as_of(date, text[], integer)
            to anon, authenticated;
    end if;
end
$$;
