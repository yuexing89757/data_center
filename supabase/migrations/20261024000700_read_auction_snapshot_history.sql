-- ADR-0074 / Issue #97. Read one physical copy of each original attempt.
-- Probe the complete attempt, never the requested symbols: a partial hot copy
-- must not be silently filled from history on a code-by-code basis.
create view audit.unit_safe_auction_snapshot_read as
select snapshot.* from audit.unit_safe_call_auction_market_series_snapshot snapshot
union all
select history.* from audit.unit_safe_call_auction_market_series_snapshot_history history
where not exists (
    select 1 from realtime.call_auction_market_series_snapshot online
    where online.trade_date = history.trade_date
      and online.session_id = history.session_id
      and online.sample_seq = history.sample_seq
      and online.ingestion_id = history.ingestion_id
);

-- Bound facts before projection and check the same durable certificate once per
-- attempt. Repeated per-field proofs make 500-code / 32-round reads too costly.
do $attempt_reader$
declare
    projection text;
    read_sql text;
begin
    select string_agg(
        case when attribute.attname = any(array[
            'cumulative_volume','cumulative_amount',
            'bid1_volume','bid2_volume','bid3_volume','bid4_volume','bid5_volume',
            'ask1_volume','ask2_volume','ask3_volume','ask4_volume','ask5_volume'
        ]) then format(
            '(case when $6 then snapshot.%I end)::%s as %I',
            attribute.attname,format_type(attribute.atttypid,attribute.atttypmod),attribute.attname
        ) else format('snapshot.%I',attribute.attname) end, ', ' order by attribute.attnum
    ) into projection
    from pg_attribute attribute
    where attribute.attrelid='realtime.call_auction_market_series_snapshot'::regclass
      and attribute.attnum>0 and not attribute.attisdropped;
    read_sql := format('select %s, $7::jsonb as quantity_quality from %%s snapshot '
        'where snapshot.trade_date=$1 and snapshot.session_id=$2 and snapshot.sample_seq=$3 '
        'and snapshot.ingestion_id=$4 and snapshot.symbol=any($5)', projection);
    execute format($function$
        create function audit.read_auction_snapshot_attempt(
            p_date date,p_session uuid,p_seq integer,p_ingestion uuid,p_codes text[]
        ) returns setof audit.unit_safe_call_auction_market_series_snapshot
        language plpgsql stable security invoker
        set search_path=pg_catalog,pg_temp set statement_timeout='5s'
        as $body$
        declare
            quality jsonb;
            certified boolean;
            symbols text[];
            source_relation text;
        begin
            quality := audit.quantity_quality(p_ingestion,'call_auction_market_series.quantities');
            certified := quality->>'status'='certified';
            select array_agg(exchange.prefix || code.value) into symbols
            from unnest(p_codes) code(value)
            cross join (values ('SSE:'),('SZSE:')) exchange(prefix);
            if exists (
                select 1 from realtime.call_auction_market_series_snapshot online
                where online.trade_date=p_date and online.session_id=p_session
                  and online.sample_seq=p_seq and online.ingestion_id=p_ingestion
            ) then
                source_relation := 'realtime.call_auction_market_series_snapshot';
            else
                source_relation := 'realtime.call_auction_market_series_snapshot_history';
            end if;
            return query execute format(%L,source_relation)
                using p_date,p_session,p_seq,p_ingestion,symbols,certified,quality;
        end;
        $body$;
    $function$,read_sql);
end;
$attempt_reader$;
revoke all on function audit.read_auction_snapshot_attempt(date,uuid,integer,uuid,text[])
    from public,anon,authenticated,market_data_api,market_data_worker;
comment on function audit.read_auction_snapshot_attempt(date,uuid,integer,uuid,text[]) is
    'Private exact-attempt reader; applies the durable quantity gate once and never mixes physical copies';

revoke all on audit.unit_safe_auction_snapshot_read
    from public, anon, authenticated, market_data_api, market_data_worker;
comment on view audit.unit_safe_auction_snapshot_read is
    'Internal certified auction read source: whole online attempt, otherwise its retained historical copy';

-- Keep the current definitions, including ADR-0068 masks, quantity_quality,
-- candidate ranking, ACLs, security mode, fixed search_path and five-second limit.
do $history_reads$
declare
    signature text;
    original text;
    rewritten text;
    reader text;
begin
    foreach signature in array array[
        'api_v1.query_call_auction_market_snapshots(date,text[])',
        'api_v1.query_call_auction_market_series_snapshots(date,text[],text)'
    ] loop
        original := pg_get_functiondef(signature::regprocedure);
        rewritten := regexp_replace(original,
            '\maudit\.unit_safe_call_auction_market_series_snapshot\M',
            'audit.unit_safe_auction_snapshot_read', 'g');
        if rewritten = original then
            raise exception 'expected certified auction source absent in %', signature;
        end if;
        reader := case when signature like '%series%' then
            'audit.read_auction_snapshot_attempt(p_trade_date,selected_session_id,'
                'round.sample_seq,round.selected_ingestion_id,p_codes)'
            else 'audit.read_auction_snapshot_attempt(p_trade_date,selected_session_id,'
                '31,selected_ingestion_id,p_codes)' end;
        rewritten := regexp_replace(rewritten,
            '\maudit\.unit_safe_auction_snapshot_read(?= snapshot[[:space:]]+join core\.security)',
            reader,'g');
        if position('audit.read_auction_snapshot_attempt(' in rewritten)=0 then
            raise exception 'expected bounded auction projection absent in %',signature;
        end if;
        execute rewritten;
    end loop;
end;
$history_reads$;
