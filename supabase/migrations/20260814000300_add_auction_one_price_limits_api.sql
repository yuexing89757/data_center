create function api_v1.query_auction_one_price_limits(p_trade_date date default null)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, ingestion, realtime, derived, core
set statement_timeout = '5s'
as $$
declare
    selected_date date;
    selected_ingestion uuid;
    selected_status text;
    selected_calculation uuid;
    payload jsonb;
begin
    select s.trade_date into selected_date
    from realtime.call_auction_market_snapshot s
    join ingestion.ingestion_run r on r.ingestion_id=s.ingestion_id
    where r.dataset_code='call_auction_market_snapshot'
      and r.status in ('succeeded','partial')
      and (p_trade_date is null or s.trade_date=p_trade_date)
      and (s.observed_at at time zone 'Asia/Shanghai')::time >= time '09:26:00'
      and (s.observed_at at time zone 'Asia/Shanghai')::time < time '09:27:00'
    order by s.trade_date desc limit 1;
    if selected_date is null then
        raise exception '09:26 auction snapshot not found' using errcode='P0002';
    end if;

    select r.ingestion_id,r.status into selected_ingestion,selected_status
    from ingestion.ingestion_run r
    where r.dataset_code='call_auction_market_snapshot' and r.status in ('succeeded','partial')
      and exists(select 1 from realtime.call_auction_market_snapshot s
        where s.ingestion_id=r.ingestion_id and s.trade_date=selected_date
          and (s.observed_at at time zone 'Asia/Shanghai')::time >= time '09:26:00'
          and (s.observed_at at time zone 'Asia/Shanghai')::time < time '09:27:00')
    order by case r.status when 'succeeded' then 0 else 1 end,
      r.finished_at desc,r.requested_at desc,r.ingestion_id desc limit 1;

    select r.calculation_id into selected_calculation
    from derived.calculation_run r
    where r.calculation_code='cn_a_mainboard_price_limit_pools'
      and r.status='succeeded' and r.start_date<=selected_date and r.end_date>=selected_date
      and exists(select 1 from derived.daily_price_limit p
        where p.calculation_id=r.calculation_id and p.trade_date=selected_date)
    order by r.calculated_at desc,r.calculation_id desc limit 1;
    if selected_calculation is null then
        raise exception 'price limit calculation not found' using errcode='P0002';
    end if;

    with facts as (
      select s.symbol,sec.code,nh.name,s.observed_at,s.last_price,s.previous_close,
        s.high_price,s.low_price,s.cumulative_volume,s.cumulative_amount,
        p.upper_limit,p.lower_limit
      from realtime.call_auction_market_snapshot s
      join core.security sec on sec.symbol=s.symbol
      left join core.security_name_history nh on nh.symbol=s.symbol
        and nh.effective_from<=selected_date
        and (nh.effective_to is null or nh.effective_to>=selected_date)
      left join derived.daily_price_limit p on p.calculation_id=selected_calculation
        and p.symbol=s.symbol and p.trade_date=selected_date
      where s.ingestion_id=selected_ingestion and s.trade_date=selected_date
        and (s.observed_at at time zone 'Asia/Shanghai')::time >= time '09:26:00'
        and (s.observed_at at time zone 'Asia/Shanghai')::time < time '09:27:00'
    ), classified as (
      select *, case
        when name is not null and last_price>0 and previous_close>0 and high_price=last_price
          and low_price=last_price and last_price=upper_limit then 'up'
        when name is not null and last_price>0 and previous_close>0 and high_price=last_price
          and low_price=last_price and last_price=lower_limit then 'down'
        else null end direction
      from facts
    )
    select jsonb_build_object(
      'trade_date',selected_date,'ingestion_id',selected_ingestion,
      'ingestion_status',selected_status,'price_limit_calculation_id',selected_calculation,
      'snapshot_window','09:26:00-09:26:59 Asia/Shanghai',
      'candidate_count',count(*),
      'omitted_incomplete_count',count(*) filter(where direction is null),
      'up_count',count(*) filter(where direction='up'),
      'down_count',count(*) filter(where direction='down'),
      'up',coalesce(jsonb_agg(jsonb_build_object(
        'symbol',symbol,'code',code,'name',name,'direction','up','observed_at',observed_at,
        'indicated_price',last_price,'limit_price',upper_limit,'previous_close',previous_close,
        'cumulative_volume',cumulative_volume,'cumulative_amount',cumulative_amount
      ) order by symbol) filter(where direction='up'),'[]'::jsonb),
      'down',coalesce(jsonb_agg(jsonb_build_object(
        'symbol',symbol,'code',code,'name',name,'direction','down','observed_at',observed_at,
        'indicated_price',last_price,'limit_price',lower_limit,'previous_close',previous_close,
        'cumulative_volume',cumulative_volume,'cumulative_amount',cumulative_amount
      ) order by symbol) filter(where direction='down'),'[]'::jsonb)
    ) into payload from classified;
    return payload;
end
$$;
revoke all on function api_v1.query_auction_one_price_limits(date)
    from public, anon, authenticated;
grant execute on function api_v1.query_auction_one_price_limits(date) to market_data_api;
