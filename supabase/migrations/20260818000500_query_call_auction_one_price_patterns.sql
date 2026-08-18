create index call_auction_market_series_snapshot_session_symbol_idx
    on realtime.call_auction_market_series_snapshot
       (trade_date, session_id, symbol, sample_seq);

create function api_v1.query_call_auction_one_price_patterns(
    p_trade_date date default null
)
returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, api_v1, realtime, core
set statement_timeout = '10s'
as $$
declare
    selected_session realtime.call_auction_market_series_session%rowtype;
    payload jsonb;
begin
    select session.*
      into selected_session
    from realtime.call_auction_market_series_session session
    where session.status in ('succeeded', 'partial')
      and (p_trade_date is null or session.trade_date = p_trade_date)
      and 29 = (
          select count(*)
          from realtime.call_auction_market_series_round round
          where round.session_id = session.session_id
            and round.sample_seq between 1 and 29
            and round.status = 'succeeded'
            and round.successful_quotes = round.expected_quotes
            and round.selected_ingestion_id is not null
      )
    order by session.trade_date desc, session.started_at desc, session.session_id desc
    limit 1;

    if selected_session.session_id is null then
        raise exception 'call-auction one-price pattern session not found'
            using errcode = 'P0002';
    end if;

    with grouped as materialized (
        select
            snapshot.symbol,
            min(snapshot.last_price) as one_price,
            min(snapshot.previous_close) as previous_close,
            count(*)::integer as sample_count
        from realtime.call_auction_market_series_snapshot snapshot
        where snapshot.trade_date = selected_session.trade_date
          and snapshot.session_id = selected_session.session_id
          and snapshot.sample_seq between 1 and 29
        group by snapshot.symbol
        having count(*) = 29
           and count(distinct snapshot.sample_seq) = 29
           and bool_and(snapshot.value_semantics = 'auction_indicative')
           and bool_and(
               snapshot.last_price is not null
               and snapshot.previous_close is not null
               and snapshot.last_price > 0
               and snapshot.previous_close > 0
           )
           and min(snapshot.last_price) = max(snapshot.last_price)
           and min(snapshot.previous_close) = max(snapshot.previous_close)
    ), calculated as (
        select
            grouped.*,
            (one_price / previous_close - 1) * 100 as exact_change_pct
        from grouped
    ), matched as (
        select
            calculated.*,
            security.code,
            security.exchange,
            name_history.name
        from calculated
        join core.security security using (symbol)
        left join lateral (
            select history.name
            from core.security_name_history history
            where history.symbol = calculated.symbol
              and history.effective_from <= selected_session.trade_date
              and (
                  history.effective_to is null
                  or history.effective_to >= selected_session.trade_date
              )
            order by history.effective_from desc
            limit 1
        ) name_history on true
        where security.security_type = 'stock'
          and security.exchange in ('SSE', 'SZSE')
          and exact_change_pct between -4 and 4
    )
    select jsonb_build_object(
        'trade_date', selected_session.trade_date,
        'session_id', selected_session.session_id,
        'session_status', selected_session.status,
        'window_start',
            (selected_session.trade_date + time '09:15:20')
                at time zone 'Asia/Shanghai',
        'window_end',
            (selected_session.trade_date + time '09:24:40')
                at time zone 'Asia/Shanghai',
        'round_count', 29,
        'candidate_count', count(*),
        'items', coalesce(
            jsonb_agg(
                jsonb_build_object(
                    'symbol', symbol,
                    'code', code,
                    'name', name,
                    'exchange', exchange,
                    'one_price', one_price,
                    'previous_close', previous_close,
                    'change_pct', round(exact_change_pct, 10),
                    'sample_count', sample_count
                )
                order by exact_change_pct desc, code, symbol
            ),
            '[]'::jsonb
        )
    )
      into payload
    from matched;

    return payload;
end
$$;

revoke all on function api_v1.query_call_auction_one_price_patterns(date)
    from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_one_price_patterns(date)
    to market_data_api;
