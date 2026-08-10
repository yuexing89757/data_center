-- ADR: daily limit-up list interface
-- Two new tables: end-of-day 5-level quote snapshot and call-auction snapshot.
-- Plus the query_daily_limit_up_list RPC for the FastAPI endpoint.

create table realtime.eod_quote_snapshot (
    symbol text not null references core.security (symbol),
    trade_date date not null,
    last_price numeric(18, 4),
    previous_close numeric(18, 4),
    bid1_price numeric(18, 4), bid1_volume bigint,
    bid2_price numeric(18, 4), bid2_volume bigint,
    bid3_price numeric(18, 4), bid3_volume bigint,
    bid4_price numeric(18, 4), bid4_volume bigint,
    bid5_price numeric(18, 4), bid5_volume bigint,
    ask1_price numeric(18, 4), ask1_volume bigint,
    ask2_price numeric(18, 4), ask2_volume bigint,
    ask3_price numeric(18, 4), ask3_volume bigint,
    ask4_price numeric(18, 4), ask4_volume bigint,
    ask5_price numeric(18, 4), ask5_volume bigint,
    seal_amount numeric(30, 4),
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (symbol, trade_date),
    constraint eod_quote_nonnegative check (
        (last_price is null or last_price >= 0)
        and seal_amount is null or seal_amount >= 0
    )
);

create table realtime.call_auction_snapshot (
    symbol text not null references core.security (symbol),
    trade_date date not null,
    last_price numeric(18, 4),
    previous_close numeric(18, 4),
    cumulative_volume bigint,
    cumulative_amount numeric(30, 4),
    auction_premium_pct numeric(24, 10),
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (symbol, trade_date),
    constraint call_auction_nonnegative check (
        (cumulative_volume is null or cumulative_volume >= 0)
        and (cumulative_amount is null or cumulative_amount >= 0)
    )
);

create index eod_quote_ingestion_idx on realtime.eod_quote_snapshot (ingestion_id);
create index call_auction_ingestion_idx on realtime.call_auction_snapshot (ingestion_id);

alter table realtime.eod_quote_snapshot enable row level security;
alter table realtime.call_auction_snapshot enable row level security;
create policy eod_quote_worker_all on realtime.eod_quote_snapshot
    for all to market_data_worker using (true) with check (true);
create policy call_auction_worker_all on realtime.call_auction_snapshot
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on realtime.eod_quote_snapshot to market_data_worker;
grant select, insert, update on realtime.call_auction_snapshot to market_data_worker;

-- Extend dataset_code for the new datasets
alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security','trading_calendar','daily_bar','capital','classification_catalog',
        'classification_members','board_index','board_index_daily_bar',
        'board_index_constituent_snapshot','stock_daily_indicator','deducted_profit',
        'five_level_quote','convertible_bond','convertible_bond_daily_bar',
        'eod_quote_snapshot','call_auction_snapshot'
    )
);

-- RPC: daily limit-up list
create or replace function api_v1.query_daily_limit_up_list(
    p_trade_date date,
    p_limit integer default 200
)
returns jsonb
language sql stable security definer
set search_path = pg_catalog, api_v1, stock_pool, core, realtime, derived
set statement_timeout = '5s'
as $$
    with limit_up_members as (
        select m.symbol, s.basis_trade_date
        from stock_pool.snapshot s
        join stock_pool.member m on m.snapshot_id = s.snapshot_id
        where s.pool_code = 'CN_A_PREVIOUS_DAY_MAINBOARD_LIMIT_UP'
          and s.basis_trade_date = p_trade_date
          and s.status = 'ready'
        order by s.version desc
        limit 1
    ),
    enriched as (
        select
            sec.code,
            sec.current_name as name,
            bar.close,
            bar.volume,
            case
                when bar.close is not null and ind.free_float_shares is not null
                then bar.close * ind.free_float_shares
            end as free_float_market_cap,
            ind.free_float_turnover_rate_pct,
            eod.seal_amount,
            case
                when eod.bid1_volume is not null and bar.volume is not null and bar.volume > 0
                then eod.bid1_volume::numeric / bar.volume
            end as seal_volume_ratio,
            (
                select count(*)::int
                from core.stock_daily_indicator si
                where si.symbol = m.symbol
                  and si.price_limit_status in ('limit_up', 'one_price_limit_up')
                  and si.trade_date <= p_trade_date
                  and si.trade_date > (
                      select max(si2.trade_date)
                      from core.stock_daily_indicator si2
                      where si2.symbol = m.symbol
                        and si2.price_limit_status not in ('limit_up', 'one_price_limit_up')
                        and si2.trade_date <= p_trade_date
                  )
            ) as consecutive_limit_up_days,
            ca.cumulative_volume as auction_volume,
            ca.cumulative_amount as auction_amount,
            ca.auction_premium_pct
        from limit_up_members m
        join core.security sec on sec.symbol = m.symbol
        left join core.daily_bar bar on bar.symbol = m.symbol and bar.trade_date = p_trade_date
        left join core.stock_daily_indicator ind on ind.symbol = m.symbol and ind.trade_date = p_trade_date
        left join realtime.eod_quote_snapshot eod on eod.symbol = m.symbol and eod.trade_date = p_trade_date
        left join realtime.call_auction_snapshot ca on ca.symbol = m.symbol and ca.trade_date = p_trade_date
        order by m.symbol
        limit least(greatest(p_limit, 1), 500)
    )
    select jsonb_build_object(
        'trade_date', p_trade_date,
        'count', count(*),
        'items', coalesce(jsonb_agg(to_jsonb(e) order by e.close desc nulls last), '[]'::jsonb)
    )
    from enriched
$$;

revoke all on function api_v1.query_daily_limit_up_list(date, integer) from public;
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_daily_limit_up_list(date, integer) to market_data_api;
    end if;
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant execute on function api_v1.query_daily_limit_up_list(date, integer) to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant execute on function api_v1.query_daily_limit_up_list(date, integer) to authenticated;
    end if;
end
$$;
