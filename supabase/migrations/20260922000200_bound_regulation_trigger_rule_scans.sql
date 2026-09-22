-- Keep the published contract and five-second timeout unchanged.
-- New batches may be absent from ANALYZE statistics: materialize matching rules once
-- instead of allowing a nested loop to rescan the entire batch for every stock.
create or replace function api_v1.query_regulation_triggers(
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
    with triggered as materialized (
        select rr.* from regulation.rule_result rr
        where rr.calculation_id = v_id and rr.triggered
          and rr.data_completeness = 'COMPLETE'
    ), candidates as (
        select s.symbol, s.exchange, s.segment, s.announced_state,
               max(case when r.level = 'SERIOUS_ABNORMAL' then 2 else 1 end) as state_rank
        from regulation.status s
        join triggered rr
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
                from triggered rr join regulation.rule r using (rule_id)
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
