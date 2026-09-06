create or replace function api_v1.query_call_auction_grab_lines(
    p_trade_date date,
    p_threshold_n numeric default 0
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
    if p_trade_date is null or p_threshold_n is null then
        raise exception 'trade date and threshold are required'
            using errcode = '22023';
    end if;

    select count(*)
      into candidate_count
    from core.security security
    where security.security_type = 'stock'
      and security.exchange in ('SSE', 'SZSE')
      and security.code ~ '^[0-9]{6}$'
      and (security.ipo_date is null or security.ipo_date <= p_trade_date)
      and (
          security.delisting_date is null
          or security.delisting_date >= p_trade_date
      );

    if candidate_count > 10000 then
        raise exception 'candidate universe exceeds bound' using errcode = '54000';
    end if;

    select
        session.session_id,
        session.status,
        first_round.selected_ingestion_id,
        final_round.selected_ingestion_id
    into
        selected_session_id,
        selected_session_status,
        first_ingestion_id,
        final_ingestion_id
    from realtime.call_auction_market_series_session session
    join realtime.call_auction_market_series_round first_round
      on first_round.session_id = session.session_id
     and first_round.sample_seq = 29
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
          select 1
          from realtime.call_auction_market_series_snapshot snapshot
          where snapshot.trade_date = p_trade_date
            and snapshot.session_id = session.session_id
            and snapshot.sample_seq = 29
            and snapshot.batch_code = '092440'
            and snapshot.ingestion_id = first_run.ingestion_id
      )
      and exists (
          select 1
          from realtime.call_auction_market_series_snapshot snapshot
          where snapshot.trade_date = p_trade_date
            and snapshot.session_id = session.session_id
            and snapshot.sample_seq = 31
            and snapshot.batch_code = '092520'
            and snapshot.ingestion_id = final_run.ingestion_id
      )
    order by case session.status when 'succeeded' then 0 else 1 end,
             session.started_at desc,
             session.session_id desc
    limit 1;

    if selected_session_id is null then
        raise exception 'complete 09:24:40 and 09:25:20 auction pair not found'
            using errcode = 'P0002';
    end if;

    -- Read each selected ingestion once and pair the two rounds by aggregation.
    -- A self-join is deliberately avoided: a newly-created ingestion UUID has
    -- no useful column statistics yet and can otherwise trigger a quadratic
    -- nested-loop plan for the roughly 5,000-symbol universe.
    with source_prices as materialized (
        select
            snapshot.symbol,
            snapshot.last_price,
            snapshot.previous_close,
            1 as round_position
        from realtime.call_auction_market_series_snapshot snapshot
        where snapshot.trade_date = p_trade_date
          and snapshot.session_id = selected_session_id
          and snapshot.sample_seq = 29
          and snapshot.batch_code = '092440'
          and snapshot.ingestion_id = first_ingestion_id
          and snapshot.value_semantics = 'auction_indicative'

        union all

        select
            snapshot.symbol,
            snapshot.last_price,
            snapshot.previous_close,
            2 as round_position
        from realtime.call_auction_market_series_snapshot snapshot
        where snapshot.trade_date = p_trade_date
          and snapshot.session_id = selected_session_id
          and snapshot.sample_seq = 31
          and snapshot.batch_code = '092520'
          and snapshot.ingestion_id = final_ingestion_id
          and snapshot.value_semantics = 'opening_trade'
    ), paired as materialized (
        select
            source_prices.symbol,
            max(source_prices.last_price)
                filter (where source_prices.round_position = 1) as first_price,
            max(source_prices.last_price)
                filter (where source_prices.round_position = 2) as final_price,
            max(source_prices.previous_close)
                filter (where source_prices.round_position = 1) as first_previous_close,
            max(source_prices.previous_close)
                filter (where source_prices.round_position = 2) as final_previous_close
        from source_prices
        group by source_prices.symbol
        having count(*) filter (where source_prices.round_position = 1) = 1
           and count(*) filter (where source_prices.round_position = 2) = 1
    ), filtered as materialized (
        select
            paired.symbol,
            (
                (paired.final_price - paired.first_price)
                / paired.final_previous_close
            ) * 100::numeric as exact_grab_line_pct
        from paired
        where paired.first_price is not null
          and paired.first_price > 0
          and paired.final_price is not null
          and paired.final_price > 0
          and paired.first_previous_close is not null
          and paired.first_previous_close = paired.final_previous_close
          and paired.final_previous_close > 0
          and (
              (
                  (paired.final_price - paired.first_price)
                  / paired.final_previous_close
              ) * 100::numeric
          ) > p_threshold_n
    ), matched as materialized (
        select
            security.code,
            name_history.name,
            filtered.exact_grab_line_pct
        from filtered
        join core.security security using (symbol)
        left join lateral (
            select history.name
            from core.security_name_history history
            where history.symbol = security.symbol
              and history.effective_from <= p_trade_date
              and (
                  history.effective_to is null
                  or history.effective_to >= p_trade_date
              )
            order by history.effective_from desc
            limit 1
        ) name_history on true
        where security.security_type = 'stock'
          and security.exchange in ('SSE', 'SZSE')
          and security.code ~ '^[0-9]{6}$'
          and (security.ipo_date is null or security.ipo_date <= p_trade_date)
          and (
              security.delisting_date is null
              or security.delisting_date >= p_trade_date
          )
    )
    select jsonb_build_object(
        'trade_date', p_trade_date,
        'session_id', selected_session_id,
        'session_status', selected_session_status,
        'threshold_n', p_threshold_n,
        'first_batch_code', '092440',
        'final_batch_code', '092520',
        'count', count(*),
        'items', coalesce(
            jsonb_agg(
                jsonb_build_object(
                    'code', code,
                    'name', name,
                    'grab_line_pct', round(exact_grab_line_pct, 10),
                    'trade_date', p_trade_date
                )
                order by exact_grab_line_pct desc, code
            ),
            '[]'::jsonb
        )
    )
      into payload
    from matched;

    return payload;
end
$$;

revoke all on function api_v1.query_call_auction_grab_lines(date, numeric)
    from public, anon, authenticated;
grant execute on function api_v1.query_call_auction_grab_lines(date, numeric)
    to market_data_api;
