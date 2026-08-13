alter table realtime.call_auction_indicative_snapshot
    add column fetched_at timestamptz,
    add column input_hash text;
update realtime.call_auction_indicative_snapshot s
set fetched_at = r.requested_at,
    input_hash = encode(extensions.digest(s.ingestion_id::text, 'sha256'), 'hex')
from ingestion.ingestion_run r where r.ingestion_id=s.ingestion_id;
alter table realtime.call_auction_indicative_snapshot
    alter column fetched_at set not null,
    alter column input_hash set not null;
alter table realtime.call_auction_indicative_snapshot
    add constraint call_auction_indicative_input_hash_check
        check (input_hash ~ '^[0-9a-f]{64}$'),
    add constraint call_auction_indicative_input_unique unique (symbol,trade_date,input_hash);

create function api_v1.persist_call_auction_indicative_details(
    p_ingestion_id uuid, p_raw_id uuid, p_symbol text, p_trade_date date,
    p_fetched_at timestamptz, p_input_hash text, p_object_path text,
    p_content_sha256 text, p_byte_size bigint, p_source_row_count integer,
    p_records jsonb
) returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, api_v1, realtime, ingestion, audit, core
set statement_timeout = '5s'
as $$
declare existing realtime.call_auction_indicative_snapshot%rowtype;
declare next_version integer;
declare accepted_count integer;
begin
    if p_symbol !~ '^(SSE|SZSE):[0-9]{6}$'
       or p_trade_date <> (now() at time zone 'Asia/Shanghai')::date
       or (p_fetched_at at time zone 'Asia/Shanghai')::date <> p_trade_date
       or p_input_hash !~ '^[0-9a-f]{64}$'
       or p_content_sha256 !~ '^[0-9a-f]{64}$'
       or p_object_path !~ '^eastmoney/call_auction_indicative_detail/year=[0-9]{4}/month=[0-9]{2}/day=[0-9]{2}/[0-9a-f-]{36}[.]jsonl$'
       or p_byte_size < 1 or p_byte_size > 2000000
       or p_source_row_count < 1 or p_source_row_count >= 5000
       or jsonb_typeof(p_records) <> 'array'
       or jsonb_array_length(p_records) < 1
       or jsonb_array_length(p_records) > p_source_row_count then
        raise exception 'invalid live auction persistence boundary' using errcode='22023';
    end if;
    if not exists (select 1 from core.security s where s.symbol=p_symbol) then
        raise exception 'unknown security' using errcode='22023';
    end if;
    select * into existing from realtime.call_auction_indicative_snapshot s
    where s.symbol=p_symbol and s.trade_date=p_trade_date and s.input_hash=p_input_hash;
    if found then
        return jsonb_build_object('outcome','reused','version',existing.version,
          'ingestion_id',existing.ingestion_id,'raw_id',existing.raw_id,
          'input_hash',existing.input_hash);
    end if;
    select count(*) into accepted_count from jsonb_array_elements(p_records) item
    where (item->>'source_sequence')::integer >= 0
      and (item->>'indicative_price')::numeric > 0
      and (item->>'displayed_volume_shares')::bigint >= 0
      and (item->>'displayed_volume_shares')::bigint % 100 = 0
      and item->>'source_display_classification' in ('internal','external','unknown')
      and ((item->>'observed_at')::timestamptz at time zone 'Asia/Shanghai')::date=p_trade_date
      and ((item->>'observed_at')::timestamptz at time zone 'Asia/Shanghai')::time
          between time '09:15:00' and time '09:25:59.999999';
    if accepted_count <> jsonb_array_length(p_records) then
        raise exception 'invalid live auction record' using errcode='22023';
    end if;
    perform pg_advisory_xact_lock(hashtextextended(p_symbol||p_trade_date::text,0));
    select * into existing from realtime.call_auction_indicative_snapshot s
    where s.symbol=p_symbol and s.trade_date=p_trade_date and s.input_hash=p_input_hash;
    if found then
        return jsonb_build_object('outcome','reused','version',existing.version,
          'ingestion_id',existing.ingestion_id,'raw_id',existing.raw_id,
          'input_hash',existing.input_hash);
    end if;
    select coalesce(max(version),0)+1 into next_version
    from realtime.call_auction_indicative_snapshot
    where symbol=p_symbol and trade_date=p_trade_date;
    insert into ingestion.ingestion_run (
      ingestion_id,provider_code,dataset_code,status,requested_at,started_at,finished_at,
      request_params,fetched_rows,accepted_rows,rejected_rows
    ) values (
      p_ingestion_id,'eastmoney','call_auction_indicative_detail','succeeded',p_fetched_at,
      p_fetched_at,now(),jsonb_build_object('symbol',p_symbol,'trade_date',p_trade_date,
      'mode','fastapi_live_single_symbol'),p_source_row_count,jsonb_array_length(p_records),0
    );
    insert into ingestion.raw_manifest (
      raw_id,ingestion_id,storage_backend,object_path,file_format,content_sha256,
      byte_size,row_count,schema_version
    ) values (p_raw_id,p_ingestion_id,'local',p_object_path,'jsonl',p_content_sha256,
      p_byte_size,p_source_row_count,'eastmoney.call_auction_indicative_detail.v1');
    insert into realtime.call_auction_indicative_snapshot (
      ingestion_id,raw_id,symbol,trade_date,version,status,source_code,record_count,
      fetched_at,input_hash
    ) values (p_ingestion_id,p_raw_id,p_symbol,p_trade_date,next_version,'succeeded',
      'eastmoney',jsonb_array_length(p_records),p_fetched_at,p_input_hash);
    insert into realtime.call_auction_indicative_detail (
      ingestion_id,symbol,trade_date,source_sequence,observed_at,indicative_price,
      displayed_volume_shares,source_display_classification
    ) select p_ingestion_id,p_symbol,p_trade_date,(item->>'source_sequence')::integer,
      (item->>'observed_at')::timestamptz,(item->>'indicative_price')::numeric,
      (item->>'displayed_volume_shares')::bigint,item->>'source_display_classification'
      from jsonb_array_elements(p_records) item;
    insert into audit.quality_result (
      quality_result_id,ingestion_id,dataset_code,rule_code,severity,status,message,details
    ) values (extensions.gen_random_uuid(),p_ingestion_id,'call_auction_indicative_detail',
      'auction_indicative.live_response_boundary','info','passed',
      'bounded single-symbol live response persisted',
      jsonb_build_object('source_rows',p_source_row_count,'accepted_rows',accepted_count));
    return jsonb_build_object('outcome','created','version',next_version,
      'ingestion_id',p_ingestion_id,'raw_id',p_raw_id,'input_hash',p_input_hash);
end $$;

revoke all on function api_v1.persist_call_auction_indicative_details(
 uuid,uuid,text,date,timestamptz,text,text,text,bigint,integer,jsonb)
 from public,anon,authenticated;
grant execute on function api_v1.persist_call_auction_indicative_details(
 uuid,uuid,text,date,timestamptz,text,text,text,bigint,integer,jsonb)
 to market_data_api;
