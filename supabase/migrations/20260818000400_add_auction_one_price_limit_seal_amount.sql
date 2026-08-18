create or replace function api_v1.query_auction_one_price_limits(
    p_trade_date date default null
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, ingestion, realtime, core
set statement_timeout = '5s'
as $$
declare
    selected_date date;
    selected_ingestion uuid;
    selected_status text;
    payload jsonb;
begin
    select snapshot.trade_date
    into selected_date
    from realtime.call_auction_market_snapshot snapshot
    join ingestion.ingestion_run run using (ingestion_id)
    where run.dataset_code = 'call_auction_market_snapshot'
      and run.status in ('succeeded', 'partial')
      and (p_trade_date is null or snapshot.trade_date = p_trade_date)
      and (snapshot.observed_at at time zone 'Asia/Shanghai')::time >= time '09:25:50'
      and (snapshot.observed_at at time zone 'Asia/Shanghai')::time < time '09:30:00'
    order by snapshot.trade_date desc
    limit 1;

    if selected_date is null then
        raise exception '09:25:50 auction snapshot not found' using errcode = 'P0002';
    end if;

    select run.ingestion_id, run.status
    into selected_ingestion, selected_status
    from ingestion.ingestion_run run
    where run.dataset_code = 'call_auction_market_snapshot'
      and run.status in ('succeeded', 'partial')
      and exists (
          select 1
          from realtime.call_auction_market_snapshot snapshot
          where snapshot.ingestion_id = run.ingestion_id
            and snapshot.trade_date = selected_date
            and (snapshot.observed_at at time zone 'Asia/Shanghai')::time
                >= time '09:25:50'
            and (snapshot.observed_at at time zone 'Asia/Shanghai')::time
                < time '09:30:00'
      )
    order by case run.status when 'succeeded' then 0 else 1 end,
             run.finished_at desc,
             run.requested_at desc,
             run.ingestion_id desc
    limit 1;

    with calendar_ordinals as materialized (
        select calendar.trade_date,
               row_number() over (order by calendar.trade_date) as trading_day_number
        from core.trading_calendar calendar
        where calendar.market = 'CN_A_SHARE'
          and calendar.is_trading_day
          and calendar.trade_date <= selected_date
    ), target_calendar as materialized (
        select trading_day_number
        from calendar_ordinals
        where trade_date = selected_date
    ), prior_five_dates as materialized (
        select trade_date
        from calendar_ordinals
        where trade_date < selected_date
        order by trade_date desc
        limit 5
    ), mainboard_universe as materialized (
        select snapshot.symbol,
               security.code,
               security.exchange,
               security.ipo_date,
               name_history.name,
               snapshot.observed_at,
               snapshot.last_price,
               snapshot.previous_close,
               snapshot.high_price,
               snapshot.low_price,
               snapshot.cumulative_volume,
               snapshot.cumulative_amount,
               snapshot.seal_amount,
               case
                   when listing_calendar.trading_day_number is null then null
                   else target_calendar.trading_day_number
                        - listing_calendar.trading_day_number + 1
               end as listing_trading_day_number
        from realtime.call_auction_market_snapshot snapshot
        join core.security security on security.symbol = snapshot.symbol
        cross join target_calendar
        left join calendar_ordinals listing_calendar
          on listing_calendar.trade_date = security.ipo_date
        left join lateral (
            select history.name
            from core.security_name_history history
            where history.symbol = security.symbol
              and history.effective_from <= selected_date
              and (history.effective_to is null
                   or history.effective_to >= selected_date)
            order by history.effective_from desc
            limit 1
        ) name_history on true
        where snapshot.ingestion_id = selected_ingestion
          and snapshot.trade_date = selected_date
          and (snapshot.observed_at at time zone 'Asia/Shanghai')::time
              >= time '09:25:50'
          and (snapshot.observed_at at time zone 'Asia/Shanghai')::time
              < time '09:30:00'
          and security.security_type = 'stock'
          and security.status = 'listed'
          and security.code ~ '^[0-9]{6}$'
          and (
              (security.exchange = 'SSE' and (
                  security.code between '600000' and '603999'
                  or security.code between '605000' and '605999'
              ))
              or (
                  security.exchange = 'SZSE'
                  and security.code between '000001' and '004999'
                  and security.code not between '001001' and '001199'
              )
          )
    ), prior_bar_counts as materialized (
        select bar.symbol, count(*) as prior_five_bar_count
        from core.daily_bar bar
        join prior_five_dates prior_date using (trade_date)
        join mainboard_universe universe using (symbol)
        where bar.market = 'CN_A_SHARE'
          and bar.trade_status in ('trading', 'unknown')
        group by bar.symbol
    ), rounded as (
        select universe.*,
               coalesce(bar_counts.prior_five_bar_count, 0) as prior_five_bar_count,
               round(universe.previous_close * 1.10::numeric, 2) as raw_upper_limit,
               round(universe.previous_close * 0.90::numeric, 2) as raw_lower_limit
        from mainboard_universe universe
        left join prior_bar_counts bar_counts using (symbol)
    ), calculated as (
        select rounded.*,
               case
                   when abs(raw_upper_limit - previous_close) < 0.01::numeric
                       then previous_close + 0.01::numeric
                   else raw_upper_limit
               end as upper_limit,
               greatest(
                   case
                       when abs(raw_lower_limit - previous_close) < 0.01::numeric
                           then previous_close - 0.01::numeric
                       else raw_lower_limit
                   end,
                   0.01::numeric
               ) as lower_limit,
               coalesce(
                   rounded.name is not null
                   and rounded.ipo_date is not null
                   and rounded.listing_trading_day_number > 5
                   and rounded.prior_five_bar_count = 5
                   and rounded.previous_close > 0
                   and rounded.last_price > 0
                   and rounded.high_price > 0
                   and rounded.low_price > 0,
                   false
               ) as evidence_complete
        from rounded
    ), classified as (
        select calculated.*,
               case
                   when evidence_complete
                     and last_price = high_price
                     and last_price = low_price
                     and last_price = upper_limit then 'up'
                   when evidence_complete
                     and last_price = high_price
                     and last_price = low_price
                     and last_price = lower_limit then 'down'
                   else null
               end as direction
        from calculated
    )
    select jsonb_build_object(
        'trade_date', selected_date,
        'ingestion_id', selected_ingestion,
        'ingestion_status', selected_status,
        'price_limit_calculation_id', null::uuid,
        'price_limit_rule_version', 'CN_MAINBOARD_2026_07_06',
        'price_limit_algorithm_version', '1.0.0',
        'calculation_mode', 'realtime_read',
        'snapshot_window', '09:25:50-09:29:59 Asia/Shanghai',
        'candidate_count', count(*),
        'omitted_incomplete_count', count(*) filter (where not evidence_complete),
        'up_count', count(*) filter (where direction = 'up'),
        'down_count', count(*) filter (where direction = 'down'),
        'up', coalesce(jsonb_agg(jsonb_build_object(
            'symbol', symbol,
            'code', code,
            'name', name,
            'direction', 'up',
            'observed_at', observed_at,
            'indicated_price', last_price,
            'limit_price', upper_limit,
            'previous_close', previous_close,
            'cumulative_volume', cumulative_volume,
            'cumulative_amount', cumulative_amount,
            'seal_amount', seal_amount
        ) order by symbol) filter (where direction = 'up'), '[]'::jsonb),
        'down', coalesce(jsonb_agg(jsonb_build_object(
            'symbol', symbol,
            'code', code,
            'name', name,
            'direction', 'down',
            'observed_at', observed_at,
            'indicated_price', last_price,
            'limit_price', lower_limit,
            'previous_close', previous_close,
            'cumulative_volume', cumulative_volume,
            'cumulative_amount', cumulative_amount,
            'seal_amount', seal_amount
        ) order by symbol) filter (where direction = 'down'), '[]'::jsonb)
    )
    into payload
    from classified;

    return payload;
end
$$;

revoke all on function api_v1.query_auction_one_price_limits(date)
    from public, anon, authenticated;
grant execute on function api_v1.query_auction_one_price_limits(date)
    to market_data_api;
