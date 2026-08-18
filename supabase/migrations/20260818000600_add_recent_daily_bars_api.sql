create function api_v1.query_recent_daily_bars(
    p_code text,
    p_trade_date date,
    p_limit integer default 20
)
returns jsonb
language plpgsql
stable
security invoker
set search_path = pg_catalog, api_v1
set statement_timeout = '5s'
as $$
declare
    v_symbol text;
    v_symbol_count integer;
    v_items jsonb;
begin
    if p_code is null or p_code !~ '^[0-9]{6}$' then
        raise exception 'p_code must be exactly six digits' using errcode = '22023';
    end if;
    if p_trade_date is null then
        raise exception 'p_trade_date is required' using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 5000 then
        raise exception 'p_limit must be between 1 and 5000' using errcode = '22023';
    end if;

    select count(*), min(security.symbol)
      into v_symbol_count, v_symbol
    from api_v1.securities security
    where security.code = p_code
      and security.security_type = 'stock';

    if v_symbol_count = 0 then
        raise exception 'stock code was not found' using errcode = 'P0002';
    end if;
    if v_symbol_count > 1 then
        raise exception 'stock code is ambiguous' using errcode = '22023';
    end if;

    select coalesce(jsonb_agg(to_jsonb(selected_bar) order by selected_bar.trade_date desc), '[]')
      into v_items
    from (
        select bar.symbol,
               bar.trade_date,
               bar.open,
               bar.high,
               bar.low,
               bar.close,
               bar.previous_close,
               bar.volume,
               bar.amount,
               bar.trade_status,
               bar.is_st
        from api_v1.daily_bars bar
        where bar.symbol = v_symbol
          and bar.trade_date <= p_trade_date
        order by bar.trade_date desc
        limit p_limit
    ) selected_bar;

    return jsonb_build_object(
        'code', p_code,
        'symbol', v_symbol,
        'trade_date', p_trade_date,
        'limit', p_limit,
        'count', jsonb_array_length(v_items),
        'items', v_items
    );
end
$$;

comment on function api_v1.query_recent_daily_bars(text, date, integer)
is 'Returns at most p_limit stored unadjusted daily bars on or before p_trade_date, newest first.';

revoke all on function api_v1.query_recent_daily_bars(text, date, integer) from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        revoke all on function api_v1.query_recent_daily_bars(text, date, integer) from anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        revoke all on function api_v1.query_recent_daily_bars(text, date, integer) from authenticated;
    end if;
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_recent_daily_bars(text, date, integer)
            to market_data_api;
    end if;
end
$$;
