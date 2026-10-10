-- ADR-0066 / Issue #88: independent provider technical-factor snapshots.
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
        'regulation_event','regulation_st_snapshot','top10_float_holder','board_moneyflow',
        'stock_technical_factor'
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
        'regulation_event','regulation_st_snapshot','top10_float_holder','board_moneyflow',
        'stock_technical_factor'
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
        'call_auction_market_series_archive','top10_float_holders_weekly','board_moneyflow_daily',
        'stock_technical_factor'
    )
) not valid;
alter table operations.workflow_run validate constraint workflow_run_workflow_code_check;

create table core.stock_technical_factor_snapshot (
    symbol text not null references core.security(symbol),
    trade_date date not null,
    market text not null check (market = 'CN_A_SHARE'),
    source_code text not null check (source_code = 'tushare'),
    factor_catalog_version text not null check (factor_catalog_version = 'stock-technical-factor.v1'),
    unadjusted jsonb not null check (jsonb_typeof(unadjusted) = 'object'),
    forward_adjusted jsonb not null check (jsonb_typeof(forward_adjusted) = 'object'),
    backward_adjusted jsonb not null check (jsonb_typeof(backward_adjusted) = 'object'),
    common jsonb not null check (jsonb_typeof(common) = 'object'),
    ingestion_id uuid not null references ingestion.ingestion_run(ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (symbol, trade_date),
    foreign key (market, trade_date) references core.trading_calendar(market, trade_date)
);
create index stock_technical_factor_date_idx on core.stock_technical_factor_snapshot(trade_date, symbol);
create index stock_technical_factor_ingestion_idx on core.stock_technical_factor_snapshot(ingestion_id);
create trigger stock_technical_factor_ensure_trading_day
before insert or update of market, trade_date on core.stock_technical_factor_snapshot
for each row execute function core.ensure_daily_bar_trading_day();
create trigger stock_technical_factor_updated_at before update on core.stock_technical_factor_snapshot
for each row execute function ingestion.set_updated_at();
alter table core.stock_technical_factor_snapshot enable row level security;
create policy stock_technical_factor_worker_all on core.stock_technical_factor_snapshot
for all to market_data_worker using (true) with check (true);
revoke all on core.stock_technical_factor_snapshot from public, anon, authenticated, market_data_api;
grant select, insert, update, delete on core.stock_technical_factor_snapshot to market_data_worker;
grant select on core.stock_technical_factor_snapshot to market_data_backup;
-- Preserve application backup coverage for the preceding new source-fact tables.
grant select on core.top10_float_holder, core.board_moneyflow to market_data_backup;

create function api_v1.query_stock_technical_factors(p_symbols text[], p_trade_date date)
returns jsonb language plpgsql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
declare v_payload jsonb;
begin
    if p_trade_date is null or p_symbols is null or cardinality(p_symbols) not between 1 and 100
        or exists (select 1 from unnest(p_symbols) s where s is null or s !~ '^(SSE|SZSE|BSE):[0-9]{6}$') then
        raise exception 'An exact trade date and 1 to 100 standard stock symbols are required' using errcode = '22023';
    end if;
    with requested as (
        select symbol, min(ordinality) as request_order
        from unnest(p_symbols) with ordinality input(symbol, ordinality) group by symbol
    ), matched as (
        select r.symbol, r.request_order, f.trade_date,
            jsonb_build_object(
                'symbol',f.symbol,'code',s.code,'name',s.current_name,
                'trade_date',f.trade_date,'source_code',f.source_code,
                'factor_catalog_version',f.factor_catalog_version,
                'unadjusted',(select jsonb_object_agg(key, case when value = 'null'::jsonb then 'null'::jsonb else to_jsonb(value #>> '{}') end) from jsonb_each(f.unadjusted)),
                'forward_adjusted',(select jsonb_object_agg(key, case when value = 'null'::jsonb then 'null'::jsonb else to_jsonb(value #>> '{}') end) from jsonb_each(f.forward_adjusted)),
                'backward_adjusted',(select jsonb_object_agg(key, case when value = 'null'::jsonb then 'null'::jsonb else to_jsonb(value #>> '{}') end) from jsonb_each(f.backward_adjusted)),
                'common',f.common
            ) as item
        from requested r left join core.security s on s.symbol = r.symbol and s.security_type = 'stock'
        left join core.stock_technical_factor_snapshot f on f.symbol = s.symbol and f.trade_date = p_trade_date
    )
    select jsonb_build_object('trade_date',p_trade_date,
        'items',coalesce(jsonb_agg(item order by request_order) filter (where trade_date is not null),'[]'::jsonb),
        'missing_symbols',coalesce(jsonb_agg(symbol order by request_order) filter (where trade_date is null),'[]'::jsonb)
    ) into v_payload from matched;
    return v_payload;
end;
$$;

create function api_v1.query_stock_technical_factors_by_codes(p_codes text[], p_trade_date date)
returns jsonb language plpgsql stable security definer
set search_path = pg_catalog, pg_temp set statement_timeout = '5s'
as $$
declare v_payload jsonb; v_symbols text[]; v_missing jsonb;
begin
    if p_trade_date is null or p_codes is null or cardinality(p_codes) not between 1 and 100
        or exists (select 1 from unnest(p_codes) c where c is null or c !~ '^[0-9]{6}$') then
        raise exception 'An exact trade date and 1 to 100 six-digit stock codes are required' using errcode = '22023';
    end if;
    if exists (select s.code from core.security s join (select distinct unnest(p_codes) code) r using(code)
        where s.security_type = 'stock' group by s.code having count(*) > 1) then
        raise exception 'stock code is ambiguous across exchanges' using errcode = 'P0003';
    end if;
    with requested as (
        select code,min(ordinality) as request_order
        from unnest(p_codes) with ordinality input(code,ordinality) group by code
    ) select array_agg(s.symbol order by r.request_order) filter (where s.symbol is not null)
        into v_symbols from requested r left join core.security s using(code) where s.security_type = 'stock';
    if v_symbols is null then
        v_payload := jsonb_build_object('trade_date',p_trade_date,'items','[]'::jsonb);
    else
        v_payload := api_v1.query_stock_technical_factors(v_symbols,p_trade_date) - 'missing_symbols';
    end if;
    with requested as (
        select code,min(ordinality) as request_order
        from unnest(p_codes) with ordinality input(code,ordinality) group by code
    ) select coalesce(jsonb_agg(r.code order by r.request_order),'[]'::jsonb) into v_missing
        from requested r where not exists (
            select 1 from jsonb_array_elements(v_payload->'items') item where item->>'code' = r.code
        );
    return v_payload || jsonb_build_object('missing_codes',v_missing);
end;
$$;
revoke all on function api_v1.query_stock_technical_factors(text[],date) from public,anon,authenticated;
revoke all on function api_v1.query_stock_technical_factors_by_codes(text[],date) from public,anon,authenticated;
grant execute on function api_v1.query_stock_technical_factors(text[],date) to authenticated,market_data_api;
grant execute on function api_v1.query_stock_technical_factors_by_codes(text[],date) to market_data_api;
