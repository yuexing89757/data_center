-- ADR-0048 / Issue #83: exact-date, calculation-coherent, bounded public reads.
-- NULL deliberately leaves legacy input membership unknown; never infer it at migration time.
alter table regulation.calculation_run add column input_event_keys jsonb
    check (input_event_keys is null or jsonb_typeof(input_event_keys) = 'array');
comment on column regulation.calculation_run.input_event_keys is
    'Actual immutable event natural keys and content hashes from calculation input; NULL=legacy unknown, []=captured empty.';

-- Shared validation stays private. The cursor is a keyset position, not an authorization token.
create function regulation.query_context(
    p_trade_date date, p_cursor text, p_limit integer, p_endpoint text
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, pg_temp
set statement_timeout = '5s'
as $$
declare
    v_cursor jsonb;
    v_run regulation.calculation_run%rowtype;
    v_id uuid;
    v_key_date date;
begin
    if p_trade_date is null or p_trade_date < date '2026-07-06'
       or p_limit is null or p_limit not between 1 and 500
       or p_endpoint not in ('triggers', 'recent-next')
       or not exists (
           select 1 from core.trading_calendar
           where market = 'CN_A_SHARE' and trade_date = p_trade_date and is_trading_day
       ) then
        raise exception using errcode = '22023', message = 'invalid regulation query parameters';
    end if;

    if p_cursor is not null then
        begin
            if length(p_cursor) not between 1 and 2048 or p_cursor !~ '^[A-Za-z0-9_-]+$' then
                raise exception 'invalid cursor encoding';
            end if;
            v_cursor := convert_from(decode(
                translate(p_cursor, '-_', '+/') || repeat('=', (4 - length(p_cursor) % 4) % 4),
                'base64'
            ), 'UTF8')::jsonb;
            if jsonb_typeof(v_cursor) is distinct from 'object'
               or v_cursor->'v' is distinct from '1'::jsonb
               or v_cursor->>'endpoint' is distinct from p_endpoint
               or v_cursor->>'trade_date' is distinct from p_trade_date::text
               or v_cursor->'limit' is distinct from to_jsonb(p_limit)
               or jsonb_typeof(v_cursor->'symbol') is distinct from 'string'
               or v_cursor->>'symbol' !~ '^(SSE|SZSE):[0-9]{6}$'
               or jsonb_typeof(v_cursor->'calculation_id') is distinct from 'string'
               or (select count(*) from jsonb_object_keys(v_cursor)) <> 7 then
                raise exception 'invalid cursor identity';
            end if;
            v_id := (v_cursor->>'calculation_id')::uuid;
            if p_endpoint = 'triggers' then
                if v_cursor->'state_rank' not in ('1'::jsonb, '2'::jsonb)
                   or not (v_cursor ? 'state_rank') then
                    raise exception 'invalid trigger position';
                end if;
            else
                if jsonb_typeof(v_cursor->'latest_event_date') is distinct from 'string'
                   or v_cursor->>'latest_event_date' !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' then
                    raise exception 'invalid event position';
                end if;
                v_key_date := (v_cursor->>'latest_event_date')::date;
                if v_key_date > p_trade_date then
                    raise exception 'future event position';
                end if;
            end if;
        exception when others then
            raise exception using errcode = '22023', message = 'invalid regulation cursor';
        end;
    end if;

    select * into v_run from regulation.calculation_run r
    where r.trade_date = p_trade_date and r.status in ('SUCCEEDED', 'PARTIAL')
      and r.completed_at is not null and (v_id is null or r.calculation_id = v_id)
    order by r.completed_at desc, r.calculation_id desc limit 1;
    if not found then
        if p_cursor is not null then
            raise exception using errcode = '22023', message = 'cursor calculation is unavailable';
        end if;
        raise exception using errcode = 'P0002', message = 'exact-date calculation is unavailable';
    end if;
    return jsonb_build_object(
        'cursor', v_cursor,
        'next_trade_date', v_run.next_trade_date,
        'metadata', jsonb_build_object(
            'trade_date', v_run.trade_date,
            'calculation_id', v_run.calculation_id,
            'calculation_status', v_run.status,
            'completed_at', v_run.completed_at,
            'event_watermark', v_run.event_watermark,
            'algorithm_version', v_run.algorithm_version,
            'rule_set_version', v_run.rule_set_version,
            'coverage', jsonb_build_object(
                'expected_count', v_run.expected_count,
                'complete_count', v_run.complete_count,
                'incomplete_count', v_run.incomplete_count,
                'not_applicable_count', v_run.not_applicable_count
            )
        )
    );
end;
$$;
revoke all on function regulation.query_context(date, text, integer, text)
    from public, anon, authenticated, market_data_api;

create function api_v1.query_regulation_triggers(
    p_trade_date date, p_cursor text default null, p_limit integer default 100
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, pg_temp
set statement_timeout = '5s'
as $$
declare
    v_context jsonb := regulation.query_context(p_trade_date, p_cursor, p_limit, 'triggers');
    v_meta jsonb := v_context->'metadata';
    v_id uuid := (v_meta->>'calculation_id')::uuid;
    v_cursor jsonb := v_context->'cursor';
    v_items jsonb;
    v_last jsonb;
    v_more boolean;
    v_next text;
begin
    with candidates as (
        select s.symbol, s.exchange, s.segment, s.announced_state,
               max(case when r.level = 'SERIOUS_ABNORMAL' then 2 else 1 end) as state_rank
        from regulation.status s
        join regulation.rule_result rr
          on rr.calculation_id = s.calculation_id and rr.symbol = s.symbol
        join regulation.rule r on r.rule_id = rr.rule_id
        where s.calculation_id = v_id and s.applicability = 'APPLICABLE'
          and s.data_completeness = 'COMPLETE' and rr.triggered
          and rr.data_completeness = 'COMPLETE'
        group by s.symbol, s.exchange, s.segment, s.announced_state
    ), bounded as materialized (
        select * from candidates c
        where p_cursor is null
           or c.state_rank < (v_cursor->>'state_rank')::integer
           or (c.state_rank = (v_cursor->>'state_rank')::integer
               and c.symbol > v_cursor->>'symbol')
        order by state_rank desc, symbol limit p_limit + 1
    ), page as (
        select * from bounded order by state_rank desc, symbol limit p_limit
    ), items as (
        select p.state_rank, p.symbol, jsonb_build_object(
            'code', split_part(p.symbol, ':', 2), 'symbol', p.symbol,
            'name', nh.name, 'exchange', p.exchange, 'segment', p.segment,
            'calculated_state', case when p.state_rank = 2 then 'SERIOUS_TRIGGERED'
                                     else 'ABNORMAL_TRIGGERED' end,
            'announced_state', p.announced_state,
            'triggered_rules', (
                select jsonb_agg(jsonb_build_object(
                    'rule_code', r.rule_code, 'level', r.level, 'direction', r.direction,
                    'kind', r.kind, 'window_start_date', rr.window_start_date,
                    'window_end_date', rr.window_end_date,
                    'observed_window_days', rr.observed_window_days,
                    'current_value', rr.current_value::text, 'threshold', rr.threshold::text,
                    'secondary_current_value', rr.secondary_current_value::text,
                    'secondary_threshold', rr.secondary_threshold::text,
                    'event_count', rr.event_count, 'required_count', rr.required_count,
                    'selected_reset_date', rr.selected_reset_date
                ) order by (r.level = 'SERIOUS_ABNORMAL') desc, r.rule_code)
                from regulation.rule_result rr join regulation.rule r using (rule_id)
                where rr.calculation_id = v_id and rr.symbol = p.symbol
                  and rr.triggered and rr.data_completeness = 'COMPLETE'
            )
        ) as item
        from page p
        left join core.security_name_history nh
          on nh.symbol = p.symbol and nh.effective_from <= p_trade_date
         and (nh.effective_to is null or nh.effective_to >= p_trade_date)
    )
    select coalesce(jsonb_agg(item order by state_rank desc, symbol), '[]'::jsonb),
           (select count(*) > p_limit from bounded)
    into v_items, v_more from items;

    if v_more then
        v_last := v_items->-1;
        v_next := rtrim(translate(replace(encode(convert_to(jsonb_build_object(
            'v', 1, 'endpoint', 'triggers', 'trade_date', p_trade_date,
            'calculation_id', v_id, 'limit', p_limit, 'symbol', v_last->>'symbol',
            'state_rank', case when v_last->>'calculated_state' = 'SERIOUS_TRIGGERED'
                               then 2 else 1 end
        )::text, 'UTF8'), 'base64'), E'\n', ''), '+/', '-_'), '=');
    end if;
    return v_meta || jsonb_build_object(
        'items', v_items, 'returned_count', jsonb_array_length(v_items),
        'has_more', v_more, 'next_cursor', v_next
    );
end;
$$;

create function api_v1.query_regulation_recent_event_next_triggers(
    p_trade_date date, p_cursor text default null, p_limit integer default 100
) returns jsonb
language plpgsql stable security definer
set search_path = pg_catalog, pg_temp
set statement_timeout = '5s'
as $$
declare
    v_context jsonb := regulation.query_context(p_trade_date, p_cursor, p_limit, 'recent-next');
    v_meta jsonb := v_context->'metadata';
    v_id uuid := (v_meta->>'calculation_id')::uuid;
    v_watermark timestamptz := (v_meta->>'event_watermark')::timestamptz;
    v_event_keys jsonb;
    v_cursor jsonb := v_context->'cursor';
    v_dates date[];
    v_items jsonb;
    v_last jsonb;
    v_more boolean;
    v_next text;
begin
    select input_event_keys into v_event_keys
    from regulation.calculation_run where calculation_id = v_id;
    if v_event_keys is null then
        raise exception using errcode = 'P0002', message = 'calculation event membership is unavailable';
    end if;
    select array_agg(trade_date order by trade_date) into v_dates
    from (
        select trade_date from core.trading_calendar
        where market = 'CN_A_SHARE' and is_trading_day and trade_date <= p_trade_date
        order by trade_date desc limit 30
    ) days;
    if coalesce(cardinality(v_dates), 0) <> 30 then
        raise exception using errcode = 'P0002', message = '30-session calendar is unavailable';
    end if;
    if p_cursor is not null and not ((v_cursor->>'latest_event_date')::date = any(v_dates)) then
        raise exception using errcode = '22023', message = 'cursor event is outside the window';
    end if;

    with events as (
        select e.*, count(*) over (partition by e.symbol) as event_count,
               row_number() over (
                   partition by e.symbol
                   order by e.period_end_date desc, e.published_at desc,
                            e.source_code, e.source_event_id
               ) as position
        from jsonb_to_recordset(v_event_keys)
            as k(source_code text, source_event_id text, source_content_hash text)
        join regulation.event e using (source_code, source_event_id, source_content_hash)
        where e.period_end_date = any(v_dates) and e.observed_at <= v_watermark
          and e.source_code in ('sse_official', 'szse_official')
          and e.event_type in ('ABNORMAL_VOLATILITY', 'SERIOUS_ABNORMAL_VOLATILITY')
    ), bounded as materialized (
        select e.*, s.data_completeness, s.applicability, s.benchmark_symbol
        from events e join regulation.status s
          on s.calculation_id = v_id and s.symbol = e.symbol
        where e.position = 1 and s.applicability <> 'NOT_APPLICABLE'
          and (p_cursor is null
               or e.period_end_date < (v_cursor->>'latest_event_date')::date
               or (e.period_end_date = (v_cursor->>'latest_event_date')::date
                   and e.symbol > v_cursor->>'symbol'))
        order by e.period_end_date desc, e.symbol limit p_limit + 1
    ), page as (
        select * from bounded order by period_end_date desc, symbol limit p_limit
    ), items as (
        select p.period_end_date, p.symbol, jsonb_build_object(
            'code', split_part(p.symbol, ':', 2), 'symbol', p.symbol, 'name', nh.name,
            'exchange', p.exchange, 'segment', p.segment,
            'official_event_count_30d', p.event_count,
            'latest_source_event_id', p.source_event_id,
            'latest_event_date', p.period_end_date, 'latest_event_published_at', p.published_at,
            'latest_event_level', p.event_level, 'latest_event_direction', p.direction,
            'latest_event_source_title', p.source_title, 'latest_event_source_url', p.source_url,
            'next_triggers', coalesce((
                select jsonb_agg(jsonb_build_object(
                    'level', w.level, 'direction', w.direction, 'rule_code', r.rule_code,
                    'benchmark_symbol', coalesce(r.benchmark_symbol, p.benchmark_symbol),
                    'scenario_code', w.scenario_code, 'scenario_index_pct', w.scenario_index_pct::text,
                    'next_day_reference_price', w.next_day_reference_price::text,
                    'raw_trigger_price', w.raw_trigger_price::text,
                    'trigger_price', w.next_day_trigger_price::text,
                    'trigger_change_pct', w.next_day_trigger_pct::text,
                    'lower_limit_price', w.lower_limit_price::text,
                    'upper_limit_price', w.upper_limit_price::text,
                    'reachability', case w.scenario_code
                        when 'CURRENT' then 'CURRENTLY_TRIGGERED'
                        when 'NONE' then 'NOT_PRICE_CALCULABLE' else w.reachability end,
                    'window_start_date', w.window_start_date, 'window_end_date', w.window_end_date,
                    'requires_official_event_confirmation', w.requires_official_event_confirmation
                ) order by (w.level = 'SERIOUS_ABNORMAL') desc, r.rule_code,
                           case w.scenario_code when 'CURRENT' then 0 when 'NONE' then 1
                               when 'INDEX_DOWN_2' then 2 when 'INDEX_FLAT' then 3 else 4 end)
                from regulation.warning w join regulation.rule r using (rule_id)
                join regulation.rule_result rr
                  on rr.calculation_id = w.calculation_id and rr.symbol = w.symbol
                 and rr.rule_id = w.rule_id
                where w.calculation_id = v_id and w.symbol = p.symbol
                  and p.applicability = 'APPLICABLE' and p.data_completeness = 'COMPLETE'
                  and rr.data_completeness = 'COMPLETE'
            ), '[]'::jsonb)
        ) as item
        from page p
        left join core.security_name_history nh
          on nh.symbol = p.symbol and nh.effective_from <= p_trade_date
         and (nh.effective_to is null or nh.effective_to >= p_trade_date)
    )
    select coalesce(jsonb_agg(item order by period_end_date desc, symbol), '[]'::jsonb),
           (select count(*) > p_limit from bounded)
    into v_items, v_more from items;

    if v_more then
        v_last := v_items->-1;
        v_next := rtrim(translate(replace(encode(convert_to(jsonb_build_object(
            'v', 1, 'endpoint', 'recent-next', 'trade_date', p_trade_date,
            'calculation_id', v_id, 'limit', p_limit, 'symbol', v_last->>'symbol',
            'latest_event_date', v_last->>'latest_event_date'
        )::text, 'UTF8'), 'base64'), E'\n', ''), '+/', '-_'), '=');
    end if;
    return v_meta || jsonb_build_object(
        'next_trade_date', v_context->'next_trade_date',
        'lookback_trading_days', 30, 'lookback_start_date', v_dates[1],
        'items', v_items, 'returned_count', jsonb_array_length(v_items),
        'has_more', v_more, 'next_cursor', v_next
    );
end;
$$;

revoke all on function api_v1.query_regulation_triggers(date, text, integer)
    from public, anon, authenticated;
revoke all on function api_v1.query_regulation_recent_event_next_triggers(date, text, integer)
    from public, anon, authenticated;
grant execute on function api_v1.query_regulation_triggers(date, text, integer)
    to market_data_api;
grant execute on function api_v1.query_regulation_recent_event_next_triggers(date, text, integer)
    to market_data_api;

comment on function api_v1.query_regulation_triggers(date, text, integer)
    is 'Exact-date calculated triggers; keyset pagination pins one published calculation. Not an official announcement.';
comment on function api_v1.query_regulation_recent_event_next_triggers(date, text, integer)
    is 'Official events in 30 trading sessions and calculation-coherent T+1 conditions. Not a price prediction.';
