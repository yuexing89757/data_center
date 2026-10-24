-- ADR-0072: consume only admin-sealed approvals; never grant Worker INSERT/UPDATE.
alter table ingestion.raw_retirement add column approved_storage_sha256 text
    check (approved_storage_sha256 ~ '^[0-9a-f]{64}$');
comment on column ingestion.raw_retirement.approved_storage_sha256 is
    'SHA-256 of admin-approved canonical path hash/device/inode; null legacy declarations cannot be consumed by Worker';
create index raw_retirement_pending_idx on ingestion.raw_retirement(raw_id)
    where restored_at is null and online_removed_at is null
      and approved_storage_sha256 is not null;

create function ingestion.lock_approved_raw_retirement(p_id uuid, p_cutoff timestamptz, p_root text)
returns jsonb language plpgsql security definer set search_path=pg_catalog as $$
declare result jsonb; ref record; used boolean; source_id uuid;
begin
    if p_cutoff > ((date_trunc('day',now() at time zone 'Asia/Shanghai')
                       - interval '1 month') at time zone 'Asia/Shanghai') then
        raise exception 'Raw retirement cutoff exceeds monthly policy' using errcode='22023';
    end if;
    perform pg_advisory_xact_lock(hashtextextended('raw-retirement:' || p_id::text,0));
    perform 1 from ingestion.raw_retirement where raw_id=p_id for update;
    select ingestion_id into source_id from ingestion.raw_manifest where raw_id=p_id;
    -- Approvals may wait for the next scheduled run. Protect facts created since sealing,
    -- using the same closed schema/lineage catalog as the administrator operator.
    for ref in
        select n.nspname,c.relname,a.attname from pg_class c
        join pg_namespace n on n.oid=c.relnamespace join pg_attribute a on a.attrelid=c.oid
        where c.relkind in ('r','p') and not c.relispartition and a.attnum>0
          and not a.attisdropped and a.atttypid='uuid'::regtype
          and (a.attname like '%ingestion_id' or a.attname like '%raw_id')
          and (n.nspname=any(array['core','capital','classification','derived','metrics',
               'realtime','stock_pool','today_limit_up','today_limit_down','billboard',
               'regulation','convertible_bond'])
               or n.nspname='audit' and c.relname='quantity_unit_repair_item')
        order by 1,2,3
    loop
        execute format('lock table %I.%I in share mode',ref.nspname,ref.relname);
        execute format('select exists(select 1 from %I.%I where %I=any($1))',
                       ref.nspname,ref.relname,ref.attname)
            into used using array[p_id,source_id];
        if used then return null; end if;
    end loop;
    lock table derived.calculation_run,regulation.monitor_context,
               regulation.calculation_run in share mode;
    if exists(select 1 from derived.calculation_run
              where strpos(input_watermark::text,p_id::text)>0
                 or strpos(input_watermark::text,source_id::text)>0)
       or exists(select 1 from regulation.monitor_context
                 where strpos(source::text,p_id::text)>0
                    or strpos(source::text,source_id::text)>0)
       or exists(select 1 from regulation.calculation_run
                 where strpos(input_event_keys::text,p_id::text)>0
                    or strpos(input_event_keys::text,source_id::text)>0)
       or exists(select 1 from ingestion.ingestion_run where replayed_from_raw_id=p_id)
    then return null; end if;
    select jsonb_build_object('manifest',to_jsonb(m),'approval',to_jsonb(t)) into result
    from ingestion.raw_retirement t join ingestion.raw_manifest m using(raw_id)
    join ingestion.ingestion_run i using(ingestion_id)
    where t.raw_id=p_id and t.restored_at is null and t.online_removed_at is null
      and t.approved_storage_sha256=p_root
      and t.retirement_cutoff <= (p_cutoff at time zone 'Asia/Shanghai')::date
      and m.created_at < p_cutoff and i.finished_at < p_cutoff
      and i.status in ('succeeded','partial','failed')
      and m.storage_backend='local' and m.file_format='jsonl'
      and (t.content_sha256,t.content_bytes,t.row_count)
          = (m.content_sha256,m.byte_size,m.row_count);
    return result;
end;
$$;
create function ingestion.confirm_approved_raw_retirement(p_id uuid,p_root text)
returns void language plpgsql security definer set search_path=pg_catalog as $$
begin
    if ingestion.lock_approved_raw_retirement(p_id,
          (date_trunc('day',now() at time zone 'Asia/Shanghai')-interval '1 month')
          at time zone 'Asia/Shanghai',p_root) is null then
        raise exception 'Raw retirement approval changed' using errcode='55000';
    end if;
    update ingestion.raw_retirement set online_removed_at=clock_timestamp() where raw_id=p_id;
end;
$$;
revoke all on function ingestion.lock_approved_raw_retirement(uuid,timestamptz,text)
    from public,anon,authenticated,market_data_api;
revoke all on function ingestion.confirm_approved_raw_retirement(uuid,text)
    from public,anon,authenticated,market_data_api;
grant execute on function ingestion.lock_approved_raw_retirement(uuid,timestamptz,text)
    to market_data_worker;
grant execute on function ingestion.confirm_approved_raw_retirement(uuid,text)
    to market_data_worker;
comment on function ingestion.lock_approved_raw_retirement(uuid,timestamptz,text) is
    'ADR-0072 bounded exact approved Raw lock; locks survive until caller transaction ends';
comment on function ingestion.confirm_approved_raw_retirement(uuid,text) is
    'ADR-0072 only confirms approved removal; never registers proof or changes immutable Raw';
