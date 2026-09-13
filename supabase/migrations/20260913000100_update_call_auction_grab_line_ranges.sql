drop function if exists api_v1.query_call_auction_grab_lines(date, numeric);

create or replace function api_v1.query_call_auction_grab_lines(
    p_trade_date date,
    p_min_grab_line_pct numeric default 0,
    p_max_grab_line_pct numeric default null,
    p_min_change_pct numeric default 0,
    p_max_change_pct numeric default null
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, ingestion, realtime, core, operations
set statement_timeout = '10s'
as $$
declare
    selected_session_id uuid;
    selected_session_status text;
    first_ingestion_id uuid;
    final_ingestion_id uuid;
    payload jsonb;
    candidate_count integer;
begin
    if p_trade_date is null
       or p_min_grab_line_pct is null
       or p_min_change_pct is null then
        raise exception 'trade date and minimum bounds are required'
            using errcode = '22023';
    end if;
    if p_max_grab_line_pct is not null
       and p_max_grab_line_pct <= p_min_grab_line_pct then
        raise exception 'maximum grab-line percentage must exceed minimum'
            using errcode = '22023';
    end if;
    if p_max_change_pct is not null
       and p_max_change_pct <= p_min_change_pct then
        raise exception 'maximum change percentage must exceed minimum'
            using errcode = '22023';
    end if;

    select count(*) into candidate_count
    from core.security security
    where security.security_type = 'stock'
      and security.exchange in ('SSE', 'SZSE')
      and security.code ~ '^[0-9]{6}$'
      and (security.ipo_date is null or security.ipo_date <= p_trade_date)
      and (security.delisting_date is null or security.delisting_date >= p_trade_date);

    if candidate_count > 10000 then
        raise exception 'candidate universe exceeds bound' using errcode = '54000';
    end if;

    select session.session_id, session.status,
           first_round.selected_ingestion_id, final_round.selected_ingestion_id
      into selected_session_id, selected_session_status,
           first_ingestion_id, final_ingestion_id
    from realtime.call_auction_market_series_session session
    join realtime.call_auction_market_series_round first_round
      on first_round.session_id = session.session_id
     and first_round.sample_seq = 30
     and first_round.status = 'succeeded'
     and first_round.successful_quotes = first_round.expected_quotes
     and first_round.selected_ingestion_id is not null
    join ingestion.ingestion_run first_run
      on first_run.ingestion_id = first_round.selected_ingestion_id
     and first_run.dataset_code = 'call_auction_market_series'
     and first_run.status = 'succeeded'
    join realtime.call_auction_market_series_round final_round
      on final_round.session_id = session.session_id
     and final_round.sample_seq = 31
     and final_round.status = 'succeeded'
     and final_round.successful_quotes = final_round.expected_quotes
     and final_round.selected_ingestion_id is not null
    join ingestion.ingestion_run final_run
      on final_run.ingestion_id = final_round.selected_ingestion_id
     and final_run.dataset_code = 'call_auction_market_series'
     and final_run.status = 'succeeded'
    where session.trade_date = p_trade_date
      and session.status in ('succeeded', 'partial')
      and exists (
          select 1 from realtime.call_auction_market_series_snapshot snapshot
          where snapshot.trade_date = p_trade_date
            and snapshot.session_id = session.session_id
            and snapshot.sample_seq = 30
            and snapshot.batch_code = '092453'
            and snapshot.ingestion_id = first_run.ingestion_id
      )
      and exists (
          select 1 from realtime.call_auction_market_series_snapshot snapshot
          where snapshot.trade_date = p_trade_date
            and snapshot.session_id = session.session_id
            and snapshot.sample_seq = 31
            and snapshot.batch_code = '092520'
            and snapshot.ingestion_id = final_run.ingestion_id
      )
    order by case session.status when 'succeeded' then 0 else 1 end,
             session.started_at desc, session.session_id desc
    limit 1;

    if selected_session_id is null then
        raise exception 'complete 09:24:53 and 09:25:20 auction pair not found'
            using errcode = 'P0002';
    end if;

    with source_prices as materialized (
        select snapshot.symbol, snapshot.last_price, snapshot.previous_close,
               1 as round_position
        from realtime.call_auction_market_series_snapshot snapshot
        where snapshot.trade_date = p_trade_date
          and snapshot.session_id = selected_session_id
          and snapshot.sample_seq = 30
          and snapshot.batch_code = '092453'
          and snapshot.ingestion_id = first_ingestion_id
          and snapshot.value_semantics = 'auction_indicative'
        union all
        select snapshot.symbol, snapshot.last_price, snapshot.previous_close,
               2 as round_position
        from realtime.call_auction_market_series_snapshot snapshot
        where snapshot.trade_date = p_trade_date
          and snapshot.session_id = selected_session_id
          and snapshot.sample_seq = 31
          and snapshot.batch_code = '092520'
          and snapshot.ingestion_id = final_ingestion_id
          and snapshot.value_semantics = 'opening_trade'
    ), paired as materialized (
        select source_prices.symbol,
               max(source_prices.last_price) filter (where round_position = 1) as first_price,
               max(source_prices.last_price) filter (where round_position = 2) as final_price,
               max(source_prices.previous_close) filter (where round_position = 1)
                   as first_previous_close,
               max(source_prices.previous_close) filter (where round_position = 2)
                   as final_previous_close
        from source_prices
        group by source_prices.symbol
        having count(*) filter (where round_position = 1) = 1
           and count(*) filter (where round_position = 2) = 1
    ), calculated as materialized (
        select paired.symbol,
               ((paired.final_price - paired.first_price)
                   / paired.final_previous_close) * 100::numeric as exact_grab_line_pct,
               ((paired.final_price - paired.final_previous_close)
                   / paired.final_previous_close) * 100::numeric as exact_change_pct
        from paired
        where paired.first_price is not null and paired.first_price > 0
          and paired.final_price is not null and paired.final_price > 0
          and paired.first_previous_close is not null
          and paired.first_previous_close = paired.final_previous_close
          and paired.final_previous_close > 0
    ), filtered as materialized (
        select * from calculated
        where exact_grab_line_pct > p_min_grab_line_pct
          and (p_max_grab_line_pct is null
               or exact_grab_line_pct < p_max_grab_line_pct)
          and exact_change_pct > p_min_change_pct
          and (p_max_change_pct is null or exact_change_pct < p_max_change_pct)
    ), matched as materialized (
        select security.code, name_history.name,
               filtered.exact_grab_line_pct, filtered.exact_change_pct
        from filtered
        join core.security security using (symbol)
        left join lateral (
            select history.name
            from core.security_name_history history
            where history.symbol = security.symbol
              and history.effective_from <= p_trade_date
              and (history.effective_to is null or history.effective_to >= p_trade_date)
            order by history.effective_from desc
            limit 1
        ) name_history on true
        where security.security_type = 'stock'
          and security.exchange in ('SSE', 'SZSE')
          and security.code ~ '^[0-9]{6}$'
          and (security.ipo_date is null or security.ipo_date <= p_trade_date)
          and (security.delisting_date is null or security.delisting_date >= p_trade_date)
    )
    select jsonb_build_object(
        'trade_date', p_trade_date,
        'session_id', selected_session_id,
        'session_status', selected_session_status,
        'min_grab_line_pct', p_min_grab_line_pct,
        'max_grab_line_pct', p_max_grab_line_pct,
        'min_change_pct', p_min_change_pct,
        'max_change_pct', p_max_change_pct,
        'first_batch_code', '092453',
        'final_batch_code', '092520',
        'count', count(*),
        'items', coalesce(jsonb_agg(jsonb_build_object(
            'code', code,
            'name', name,
            'grab_line_pct', round(exact_grab_line_pct, 10),
            'change_pct_092520', round(exact_change_pct, 10),
            'trade_date', p_trade_date
        ) order by exact_grab_line_pct desc, code), '[]'::jsonb)
    ) into payload
    from matched;

    return payload;
end
$$;

revoke all on function api_v1.query_call_auction_grab_lines(
    date, numeric, numeric, numeric, numeric
) from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_grab_lines(
    date, numeric, numeric, numeric, numeric
) to market_data_api;
