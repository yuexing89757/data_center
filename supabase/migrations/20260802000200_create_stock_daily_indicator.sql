alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security',
        'trading_calendar',
        'daily_bar',
        'capital',
        'classification_catalog',
        'classification_members',
        'board_index',
        'board_index_daily_bar',
        'board_index_constituent_snapshot',
        'stock_daily_indicator'
    )
);

alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security',
        'trading_calendar',
        'daily_bar',
        'capital',
        'classification_catalog',
        'classification_members',
        'board_index',
        'board_index_daily_bar',
        'board_index_constituent_snapshot',
        'stock_daily_indicator'
    )
);

create table core.stock_daily_indicator (
    symbol text not null references core.security (symbol),
    trade_date date not null,
    market text not null,
    close numeric(18, 4),
    turnover_rate_pct numeric(24, 10),
    free_float_turnover_rate_pct numeric(24, 10),
    volume_ratio numeric(24, 10),
    pe numeric(24, 10),
    pe_ttm numeric(24, 10),
    pb numeric(24, 10),
    ps numeric(24, 10),
    ps_ttm numeric(24, 10),
    dividend_yield_pct numeric(24, 10),
    dividend_yield_ttm_pct numeric(24, 10),
    total_shares bigint,
    circulating_shares bigint,
    free_float_shares bigint,
    total_market_value numeric(30, 4),
    circulating_market_value numeric(30, 4),
    price_limit_status text not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, trade_date),
    foreign key (market, trade_date) references core.trading_calendar (market, trade_date),
    constraint stock_daily_indicator_market_check check (market = 'CN_A_SHARE'),
    constraint stock_daily_indicator_nonnegative_check check (
        (close is null or close >= 0)
        and (turnover_rate_pct is null or turnover_rate_pct >= 0)
        and (free_float_turnover_rate_pct is null or free_float_turnover_rate_pct >= 0)
        and (volume_ratio is null or volume_ratio >= 0)
        and (dividend_yield_pct is null or dividend_yield_pct >= 0)
        and (dividend_yield_ttm_pct is null or dividend_yield_ttm_pct >= 0)
        and (total_shares is null or total_shares > 0)
        and (circulating_shares is null or circulating_shares >= 0)
        and (free_float_shares is null or free_float_shares >= 0)
        and (total_market_value is null or total_market_value >= 0)
        and (circulating_market_value is null or circulating_market_value >= 0)
    ),
    constraint stock_daily_indicator_share_order_check check (
        (circulating_shares is null or total_shares is null or circulating_shares <= total_shares)
        and (
            free_float_shares is null or circulating_shares is null
            or free_float_shares <= circulating_shares
        )
    ),
    constraint stock_daily_indicator_market_value_order_check check (
        circulating_market_value is null or total_market_value is null
        or circulating_market_value <= total_market_value
    ),
    constraint stock_daily_indicator_limit_status_check check (
        price_limit_status in (
            'flat', 'rise', 'limit_up', 'one_price_limit_up',
            'fall', 'limit_down', 'one_price_limit_down', 'unknown'
        )
    ),
    constraint stock_daily_indicator_source_check check (source_code = 'tushare')
);

create index stock_daily_indicator_trade_date_idx
    on core.stock_daily_indicator (trade_date, symbol);
create index stock_daily_indicator_ingestion_idx
    on core.stock_daily_indicator (ingestion_id);

create trigger stock_daily_indicator_ensure_trading_day
before insert or update of market, trade_date on core.stock_daily_indicator
for each row execute function core.ensure_daily_bar_trading_day();

create trigger stock_daily_indicator_set_updated_at
before update on core.stock_daily_indicator
for each row execute function ingestion.set_updated_at();

alter table core.stock_daily_indicator enable row level security;
create policy stock_daily_indicator_worker_all on core.stock_daily_indicator
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on core.stock_daily_indicator to market_data_worker;

create view api_v1.stock_daily_indicators as
select
    symbol,
    trade_date,
    close,
    turnover_rate_pct,
    free_float_turnover_rate_pct,
    volume_ratio,
    pe,
    pe_ttm,
    pb,
    ps,
    ps_ttm,
    dividend_yield_pct,
    dividend_yield_ttm_pct,
    total_shares,
    circulating_shares,
    free_float_shares,
    total_market_value,
    circulating_market_value,
    price_limit_status
from core.stock_daily_indicator;

revoke all on api_v1.stock_daily_indicators from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on api_v1.stock_daily_indicators to authenticated;
    end if;
end
$$;
