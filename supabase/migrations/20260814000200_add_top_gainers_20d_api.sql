create function api_v1.query_top_gainers_20d(
    p_end_date date default null,
    p_limit integer default 10
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '5s'
as $$
declare
    selected_end date;
    selected_start date;
    session_count integer;
    payload jsonb;
begin
    if p_limit < 1 or p_limit > 10 then
        raise exception 'limit is out of bounds' using errcode = '22023';
    end if;

    if p_end_date is null then
        select max(c.trade_date) into selected_end
        from core.trading_calendar c
        where c.market = 'CN_A_SHARE' and c.is_trading_day
          and exists (
              select 1 from core.daily_bar b
              where b.trade_date = c.trade_date and b.trade_status = 'trading'
                and b.close > 0
          );
    else
        select c.trade_date into selected_end
        from core.trading_calendar c
        where c.market = 'CN_A_SHARE' and c.is_trading_day
          and c.trade_date = p_end_date;
    end if;
    if selected_end is null then
        raise exception 'eligible end date not found' using errcode = 'P0002';
    end if;

    with window_days as (
        select c.trade_date from core.trading_calendar c
        where c.market='CN_A_SHARE' and c.is_trading_day and c.trade_date <= selected_end
        order by c.trade_date desc limit 20
    )
    select min(trade_date), count(*) into selected_start, session_count from window_days;
    if session_count <> 20 then
        raise exception '20 trading sessions are not available' using errcode = 'P0002';
    end if;

    with candidates as (
        select s.symbol, s.code
        from core.security s
        where s.security_type='stock'
          and (s.ipo_date is null or s.ipo_date <= selected_end)
          and (s.delisting_date is null or s.delisting_date >= selected_end)
    ), facts as (
        select c.*, sb.close start_close, sb.trade_status start_status,
               eb.close end_close, eb.trade_status end_status, nh.name
        from candidates c
        left join core.daily_bar sb on sb.symbol=c.symbol and sb.trade_date=selected_start
        left join core.daily_bar eb on eb.symbol=c.symbol and eb.trade_date=selected_end
        left join core.security_name_history nh on nh.symbol=c.symbol
          and nh.effective_from <= selected_end
          and (nh.effective_to is null or nh.effective_to >= selected_end)
    ), eligible as (
        select *, round((end_close/start_close-1)*100, 10) return_pct
        from facts where start_close > 0 and end_close > 0
          and start_status='trading' and end_status='trading' and name is not null
    ), ranked as (
        select * from eligible order by return_pct desc, symbol limit p_limit
    ), counts as (
        select count(*)::integer total_candidate_count,
          count(*) filter(where start_close is null)::integer missing_start_bar,
          count(*) filter(where end_close is null)::integer missing_end_bar,
          count(*) filter(where (start_status is not null and start_status<>'trading')
                            or (end_status is not null and end_status<>'trading'))::integer non_trading_bar,
          count(*) filter(where (start_close is not null and start_close<=0)
                            or (end_close is not null and end_close<=0))::integer nonpositive_price,
          count(*) filter(where name is null)::integer missing_name
        from facts
    )
    select jsonb_build_object(
      'start_trade_date', selected_start, 'end_trade_date', selected_end,
      'trading_session_count', 20, 'return_interval_count', 19,
      'total_candidate_count', c.total_candidate_count,
      'eligible_count', (select count(*) from eligible),
      'omitted_count', c.total_candidate_count-(select count(*) from eligible),
      'returned_count', (select count(*) from ranked),
      'omissions', jsonb_build_object(
        'missing_start_bar',c.missing_start_bar,'missing_end_bar',c.missing_end_bar,
        'non_trading_bar',c.non_trading_bar,'nonpositive_price',c.nonpositive_price,
        'missing_name',c.missing_name),
      'items', coalesce((select jsonb_agg(jsonb_build_object(
        'symbol',r.symbol,'code',r.code,'name',r.name,
        'start_trade_date',selected_start,'end_trade_date',selected_end,
        'start_close',r.start_close,'end_close',r.end_close,'return_pct',r.return_pct
      ) order by r.return_pct desc,r.symbol) from ranked r),'[]'::jsonb)
    ) into payload from counts c;
    return payload;
end
$$;
revoke all on function api_v1.query_top_gainers_20d(date,integer)
    from public, anon, authenticated;
grant execute on function api_v1.query_top_gainers_20d(date,integer) to market_data_api;
