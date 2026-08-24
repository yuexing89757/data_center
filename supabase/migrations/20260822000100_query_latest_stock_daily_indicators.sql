create function api_v1.query_latest_stock_daily_indicators(
    p_codes text[]
)
returns jsonb
language plpgsql
stable
security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
declare
    v_payload jsonb;
begin
    if p_codes is null
       or cardinality(p_codes) < 1
       or cardinality(p_codes) > 500
       or exists (
           select 1
           from unnest(p_codes) as requested(code)
           where requested.code is null or requested.code !~ '^[0-9]{6}$'
       ) then
        raise exception 'p_codes must contain between 1 and 500 six-digit stock codes'
            using errcode = '22023';
    end if;

    if exists (
        select 1
        from (
            select requested.code
            from (select distinct code from unnest(p_codes) as input(code)) requested
            join core.security security
              on security.code = requested.code
             and security.security_type = 'stock'
            group by requested.code
            having count(*) > 1
        ) ambiguous
    ) then
        raise exception 'stock code is ambiguous across exchanges'
            using errcode = 'P0003';
    end if;

    with requested_codes as (
        select requested.code, min(requested.ordinality)::integer as request_order
        from unnest(p_codes) with ordinality as requested(code, ordinality)
        group by requested.code
    ), resolved as (
        select
            requested.code,
            requested.request_order,
            security.symbol
        from requested_codes requested
        left join core.security security
          on security.code = requested.code
         and security.security_type = 'stock'
    ), latest as (
        select
            resolved.code,
            resolved.request_order,
            resolved.symbol,
            indicator.trade_date,
            indicator.close,
            indicator.turnover_rate_pct,
            indicator.free_float_turnover_rate_pct,
            indicator.volume_ratio,
            indicator.pe,
            indicator.pe_ttm,
            indicator.pb,
            indicator.ps,
            indicator.ps_ttm,
            indicator.dividend_yield_pct,
            indicator.dividend_yield_ttm_pct,
            indicator.total_shares,
            indicator.circulating_shares,
            indicator.free_float_shares,
            indicator.total_market_value,
            indicator.circulating_market_value,
            indicator.price_limit_status
        from resolved
        left join lateral (
            select value.*
            from core.stock_daily_indicator value
            where value.symbol = resolved.symbol
            order by value.trade_date desc
            limit 1
        ) indicator on true
    )
    select jsonb_build_object(
        'requested_count', count(*)::integer,
        'found_count', count(*) filter (where latest.trade_date is not null)::integer,
        'missing_codes', coalesce(
            jsonb_agg(latest.code order by latest.request_order)
                filter (where latest.trade_date is null),
            '[]'::jsonb
        ),
        'items', coalesce(
            jsonb_agg(
                jsonb_build_object(
                    'symbol', latest.symbol,
                    'code', latest.code,
                    'trade_date', latest.trade_date,
                    'close', latest.close,
                    'turnover_rate_pct', latest.turnover_rate_pct,
                    'free_float_turnover_rate_pct', latest.free_float_turnover_rate_pct,
                    'volume_ratio', latest.volume_ratio,
                    'pe', latest.pe,
                    'pe_ttm', latest.pe_ttm,
                    'pb', latest.pb,
                    'ps', latest.ps,
                    'ps_ttm', latest.ps_ttm,
                    'dividend_yield_pct', latest.dividend_yield_pct,
                    'dividend_yield_ttm_pct', latest.dividend_yield_ttm_pct,
                    'total_shares', latest.total_shares,
                    'circulating_shares', latest.circulating_shares,
                    'free_float_shares', latest.free_float_shares,
                    'total_market_value', latest.total_market_value,
                    'circulating_market_value', latest.circulating_market_value,
                    'price_limit_status', latest.price_limit_status
                ) order by latest.request_order
            ) filter (where latest.trade_date is not null),
            '[]'::jsonb
        )
    ) into v_payload
    from latest;

    return v_payload;
end
$$;

comment on function api_v1.query_latest_stock_daily_indicators(text[])
is 'Returns each requested stock code latest retained daily indicator, preserving request order.';

revoke all on function api_v1.query_latest_stock_daily_indicators(text[]) from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        revoke all on function api_v1.query_latest_stock_daily_indicators(text[]) from anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        revoke all on function api_v1.query_latest_stock_daily_indicators(text[])
            from authenticated;
    end if;
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_latest_stock_daily_indicators(text[])
            to market_data_api;
    end if;
end
$$;
