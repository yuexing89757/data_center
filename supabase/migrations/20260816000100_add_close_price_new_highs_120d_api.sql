create function api_v1.query_close_price_new_highs_120d()
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, core
set statement_timeout = '10s'
as $$
declare
    selected_end date;
    session_count integer;
    candidate_count integer;
    payload jsonb;
begin
    select max(c.trade_date) into selected_end
    from core.trading_calendar c
    where c.market = 'CN_A_SHARE'
      and c.is_trading_day
      and exists (
          select 1
          from core.daily_bar b
          join core.security s on s.symbol = b.symbol
          where b.trade_date = c.trade_date
            and s.exchange in ('SSE', 'SZSE')
            and s.security_type = 'stock'
            and b.trade_status in ('trading', 'unknown')
            and b.close > 0
      );

    if selected_end is null then
        raise exception 'eligible trade date not found' using errcode = 'P0002';
    end if;

    with window_days as (
        select c.trade_date
        from core.trading_calendar c
        where c.market = 'CN_A_SHARE'
          and c.is_trading_day
          and c.trade_date <= selected_end
        order by c.trade_date desc
        limit 120
    )
    select count(*) into session_count from window_days;

    if session_count <> 120 then
        raise exception '120 trading sessions are not available' using errcode = 'P0002';
    end if;

    select count(*) into candidate_count
    from core.security s
    where s.exchange in ('SSE', 'SZSE')
      and s.security_type = 'stock'
      and (s.ipo_date is null or s.ipo_date <= selected_end)
      and (s.delisting_date is null or s.delisting_date >= selected_end);

    if candidate_count > 10000 then
        raise exception 'candidate universe exceeds bound' using errcode = '54000';
    end if;

    with window_days as (
        select c.trade_date
        from core.trading_calendar c
        where c.market = 'CN_A_SHARE'
          and c.is_trading_day
          and c.trade_date <= selected_end
        order by c.trade_date desc
        limit 120
    ), candidates as (
        select s.symbol, s.code
        from core.security s
        where s.exchange in ('SSE', 'SZSE')
          and s.security_type = 'stock'
          and (s.ipo_date is null or s.ipo_date <= selected_end)
          and (s.delisting_date is null or s.delisting_date >= selected_end)
    ), bars_in_window as (
        select b.symbol, b.trade_date, b.close, b.trade_status
        from core.daily_bar b
        join window_days wd on wd.trade_date = b.trade_date
        where b.market = 'CN_A_SHARE'
    ), bar_stats as (
        select
            c.symbol,
            c.code,
            count(*) filter (
                where b.close > 0
                  and b.trade_status in ('trading', 'unknown')
            )::integer as valid_bar_count,
            max(b.close) filter (where b.trade_date = selected_end) as close,
            max(b.trade_status) filter (where b.trade_date = selected_end)
                as current_status,
            max(b.close) filter (
                where b.trade_date < selected_end
                  and b.close > 0
                  and b.trade_status in ('trading', 'unknown')
            ) as previous_119d_high,
            bool_or(
                b.trade_status is not null
                and b.trade_status not in ('trading', 'unknown')
            ) as has_non_trading_bar,
            bool_or(b.close is not null and b.close <= 0) as has_nonpositive_price
        from candidates c
        left join bars_in_window b on b.symbol = c.symbol
        group by c.symbol, c.code
    ), observations as (
        select bs.*, nh.name
        from bar_stats bs
        left join core.security_name_history nh
          on nh.symbol = bs.symbol
         and nh.effective_from <= selected_end
         and (nh.effective_to is null or nh.effective_to >= selected_end)
    ), eligible_history as (
        select *
        from observations
        where valid_bar_count = 120
          and close > 0
          and current_status in ('trading', 'unknown')
          and previous_119d_high > 0
          and name is not null
    ), breakouts as (
        select
            *,
            round((close / previous_119d_high - 1) * 100, 10) as breakout_pct
        from eligible_history
        where close > previous_119d_high
    ), counts as (
        select
            count(*) filter (where valid_bar_count <> 120)::integer
                as incomplete_history,
            count(*) filter (where has_non_trading_bar)::integer
                as non_trading_bar,
            count(*) filter (where has_nonpositive_price)::integer
                as nonpositive_price,
            count(*) filter (where name is null)::integer as missing_name
        from observations
    )
    select jsonb_build_object(
        'trade_date', selected_end,
        'window_trading_session_count', 120,
        'comparison_session_count', 119,
        'total_candidate_count', candidate_count,
        'eligible_history_count', (select count(*) from eligible_history),
        'omitted_count',
            candidate_count - (select count(*) from eligible_history),
        'returned_count', (select count(*) from breakouts),
        'omissions', jsonb_build_object(
            'incomplete_history', counts.incomplete_history,
            'non_trading_bar', counts.non_trading_bar,
            'nonpositive_price', counts.nonpositive_price,
            'missing_name', counts.missing_name
        ),
        'items', coalesce(
            (
                select jsonb_agg(
                    jsonb_build_object(
                        'symbol', b.symbol,
                        'code', b.code,
                        'name', b.name,
                        'close', b.close,
                        'previous_119d_high', b.previous_119d_high,
                        'breakout_pct', b.breakout_pct
                    )
                    order by b.breakout_pct desc, b.symbol
                )
                from breakouts b
            ),
            '[]'::jsonb
        )
    )
    into payload
    from counts;

    return payload;
end
$$;

revoke all on function api_v1.query_close_price_new_highs_120d()
    from public, anon, authenticated;
grant execute on function api_v1.query_close_price_new_highs_120d()
    to market_data_api;
