alter table realtime.call_auction_market_snapshot
    add column bid1_price numeric(18, 4),
    add column bid1_volume bigint,
    add column bid2_price numeric(18, 4),
    add column bid2_volume bigint,
    add column bid3_price numeric(18, 4),
    add column bid3_volume bigint,
    add column bid4_price numeric(18, 4),
    add column bid4_volume bigint,
    add column bid5_price numeric(18, 4),
    add column bid5_volume bigint,
    add column ask1_price numeric(18, 4),
    add column ask1_volume bigint,
    add column ask2_price numeric(18, 4),
    add column ask2_volume bigint,
    add column ask3_price numeric(18, 4),
    add column ask3_volume bigint,
    add column ask4_price numeric(18, 4),
    add column ask4_volume bigint,
    add column ask5_price numeric(18, 4),
    add column ask5_volume bigint,
    add column seal_amount numeric(30, 4);

alter table realtime.call_auction_market_snapshot
    add constraint call_auction_market_order_book_nonnegative check (
        (bid1_price is null or bid1_price > 0)
        and (bid2_price is null or bid2_price > 0)
        and (bid3_price is null or bid3_price > 0)
        and (bid4_price is null or bid4_price > 0)
        and (bid5_price is null or bid5_price > 0)
        and (ask1_price is null or ask1_price > 0)
        and (ask2_price is null or ask2_price > 0)
        and (ask3_price is null or ask3_price > 0)
        and (ask4_price is null or ask4_price > 0)
        and (ask5_price is null or ask5_price > 0)
        and (bid1_volume is null or bid1_volume >= 0)
        and (bid2_volume is null or bid2_volume >= 0)
        and (bid3_volume is null or bid3_volume >= 0)
        and (bid4_volume is null or bid4_volume >= 0)
        and (bid5_volume is null or bid5_volume >= 0)
        and (ask1_volume is null or ask1_volume >= 0)
        and (ask2_volume is null or ask2_volume >= 0)
        and (ask3_volume is null or ask3_volume >= 0)
        and (ask4_volume is null or ask4_volume >= 0)
        and (ask5_volume is null or ask5_volume >= 0)
        and (seal_amount is null or seal_amount >= 0)
    ),
    add constraint call_auction_market_order_book_price_requires_volume check (
        (bid1_price is null or bid1_volume is not null)
        and (bid2_price is null or bid2_volume is not null)
        and (bid3_price is null or bid3_volume is not null)
        and (bid4_price is null or bid4_volume is not null)
        and (bid5_price is null or bid5_volume is not null)
        and (ask1_price is null or ask1_volume is not null)
        and (ask2_price is null or ask2_volume is not null)
        and (ask3_price is null or ask3_volume is not null)
        and (ask4_price is null or ask4_volume is not null)
        and (ask5_price is null or ask5_volume is not null)
    ),
    add constraint call_auction_market_order_book_volume_only_positive check (
        (bid1_price is not null or bid1_volume is null or bid1_volume > 0)
        and (bid2_price is not null or bid2_volume is null or bid2_volume > 0)
        and (bid3_price is not null or bid3_volume is null or bid3_volume > 0)
        and (bid4_price is not null or bid4_volume is null or bid4_volume > 0)
        and (bid5_price is not null or bid5_volume is null or bid5_volume > 0)
        and (ask1_price is not null or ask1_volume is null or ask1_volume > 0)
        and (ask2_price is not null or ask2_volume is null or ask2_volume > 0)
        and (ask3_price is not null or ask3_volume is null or ask3_volume > 0)
        and (ask4_price is not null or ask4_volume is null or ask4_volume > 0)
        and (ask5_price is not null or ask5_volume is null or ask5_volume > 0)
    ),
    add constraint call_auction_market_seal_amount_rule check (
        seal_amount is not distinct from (
            case
                when ask1_volume is null or ask1_volume = 0
                    then bid1_price * bid1_volume
                else null
            end
        )
    );

create or replace function api_v1.query_call_auction_market_snapshots(
    p_trade_date date,
    p_codes text[]
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, ingestion, realtime, core
set statement_timeout = '5s'
as $$
declare
    selected_ingestion_id uuid;
    selected_status text;
    payload jsonb;
begin
    if p_trade_date is null
       or p_codes is null
       or cardinality(p_codes) < 1
       or cardinality(p_codes) > 500
       or exists (
           select 1
           from unnest(p_codes) as requested(code)
           where requested.code is null or requested.code !~ '^[0-9]{6}$'
       ) then
        raise exception 'invalid call-auction market snapshot query boundary'
            using errcode = '22023';
    end if;

    select run.ingestion_id, run.status
      into selected_ingestion_id, selected_status
    from ingestion.ingestion_run run
    where run.dataset_code = 'call_auction_market_snapshot'
      and run.status in ('succeeded', 'partial')
      and exists (
          select 1
          from realtime.call_auction_market_snapshot snapshot
          where snapshot.ingestion_id = run.ingestion_id
            and snapshot.trade_date = p_trade_date
      )
    order by
        case run.status when 'succeeded' then 0 else 1 end,
        run.finished_at desc,
        run.requested_at desc,
        run.ingestion_id desc
    limit 1;

    if selected_ingestion_id is null then
        raise exception 'call-auction market snapshot not found'
            using errcode = 'P0002';
    end if;

    with requested_codes as (
        select distinct requested.code
        from unnest(p_codes) as requested(code)
    ), matched as (
        select
            snapshot.symbol,
            security.code,
            snapshot.observed_at,
            snapshot.last_price,
            snapshot.previous_close,
            snapshot.high_price,
            snapshot.low_price,
            snapshot.cumulative_volume,
            snapshot.cumulative_amount,
            snapshot.bid1_price,
            snapshot.bid1_volume,
            snapshot.bid2_price,
            snapshot.bid2_volume,
            snapshot.bid3_price,
            snapshot.bid3_volume,
            snapshot.bid4_price,
            snapshot.bid4_volume,
            snapshot.bid5_price,
            snapshot.bid5_volume,
            snapshot.ask1_price,
            snapshot.ask1_volume,
            snapshot.ask2_price,
            snapshot.ask2_volume,
            snapshot.ask3_price,
            snapshot.ask3_volume,
            snapshot.ask4_price,
            snapshot.ask4_volume,
            snapshot.ask5_price,
            snapshot.ask5_volume,
            snapshot.seal_amount
        from realtime.call_auction_market_snapshot snapshot
        join core.security security on security.symbol = snapshot.symbol
        join requested_codes requested on requested.code = security.code
        where snapshot.ingestion_id = selected_ingestion_id
          and snapshot.trade_date = p_trade_date
          and security.exchange in ('SSE', 'SZSE')
    ), item_payload as (
        select
            count(*)::integer as returned_count,
            coalesce(
                jsonb_agg(
                    jsonb_build_object(
                        'symbol', matched.symbol,
                        'code', matched.code,
                        'observed_at', matched.observed_at,
                        'last_price', matched.last_price,
                        'previous_close', matched.previous_close,
                        'high_price', matched.high_price,
                        'low_price', matched.low_price,
                        'cumulative_volume', matched.cumulative_volume,
                        'cumulative_amount', matched.cumulative_amount,
                        'bid1_price', matched.bid1_price,
                        'bid1_volume', matched.bid1_volume,
                        'bid2_price', matched.bid2_price,
                        'bid2_volume', matched.bid2_volume,
                        'bid3_price', matched.bid3_price,
                        'bid3_volume', matched.bid3_volume,
                        'bid4_price', matched.bid4_price,
                        'bid4_volume', matched.bid4_volume,
                        'bid5_price', matched.bid5_price,
                        'bid5_volume', matched.bid5_volume,
                        'ask1_price', matched.ask1_price,
                        'ask1_volume', matched.ask1_volume,
                        'ask2_price', matched.ask2_price,
                        'ask2_volume', matched.ask2_volume,
                        'ask3_price', matched.ask3_price,
                        'ask3_volume', matched.ask3_volume,
                        'ask4_price', matched.ask4_price,
                        'ask4_volume', matched.ask4_volume,
                        'ask5_price', matched.ask5_price,
                        'ask5_volume', matched.ask5_volume,
                        'seal_amount', matched.seal_amount
                    ) order by matched.code, matched.symbol
                ),
                '[]'::jsonb
            ) as items
        from matched
    ), missing_payload as (
        select coalesce(
            jsonb_agg(requested.code order by requested.code),
            '[]'::jsonb
        ) as missing_codes
        from requested_codes requested
        where not exists (
            select 1 from matched where matched.code = requested.code
        )
    )
    select jsonb_build_object(
        'trade_date', p_trade_date,
        'ingestion_id', selected_ingestion_id,
        'ingestion_status', selected_status,
        'requested_count', (select count(*) from requested_codes),
        'returned_count', item_payload.returned_count,
        'missing_codes', missing_payload.missing_codes,
        'items', item_payload.items
    )
      into payload
    from item_payload
    cross join missing_payload;

    return payload;
end
$$;

revoke all on function api_v1.query_call_auction_market_snapshots(date, text[])
    from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_market_snapshots(date, text[])
    to market_data_api;

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
            'cumulative_amount', cumulative_amount
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
            'cumulative_amount', cumulative_amount
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
