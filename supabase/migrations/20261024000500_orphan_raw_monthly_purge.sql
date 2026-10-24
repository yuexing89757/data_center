-- ADR-0073: old unpublished Raw files need their own physical deletion audit.
create table ingestion.raw_orphan_purge (
    purge_id uuid primary key default gen_random_uuid(),
    storage_identity_sha256 text not null check(storage_identity_sha256 ~ '^[0-9a-f]{64}$'),
    object_path text not null,
    physical_sha256 text not null check(physical_sha256 ~ '^[0-9a-f]{64}$'),
    physical_bytes bigint not null check(physical_bytes>=0),
    mtime_ns bigint not null check(mtime_ns>=0),
    ctime_ns bigint not null check(ctime_ns>=0),
    cutoff date not null,
    declared_at timestamptz not null default clock_timestamp(),
    online_removed_at timestamptz,
    unique(storage_identity_sha256,object_path),
    check(online_removed_at is null or online_removed_at>=declared_at)
);
alter table ingestion.raw_orphan_purge enable row level security;
revoke all on ingestion.raw_orphan_purge from public,anon,authenticated,market_data_api;
grant select on ingestion.raw_orphan_purge to market_data_worker,market_data_backup;
create policy raw_orphan_purge_read on ingestion.raw_orphan_purge for select
    to market_data_worker,market_data_backup using(true);
comment on table ingestion.raw_orphan_purge is 'ADR-0073 physical audit for old unregistered Raw, no fabricated source manifest or archive';
comment on column ingestion.raw_orphan_purge.purge_id is 'Immutable physical purge audit identity';
comment on column ingestion.raw_orphan_purge.storage_identity_sha256 is 'Exact canonical root path/device/inode SHA-256';
comment on column ingestion.raw_orphan_purge.object_path is 'Restricted relative Raw representation path, not a public field';
comment on column ingestion.raw_orphan_purge.physical_sha256 is 'SHA-256 of exact physical file bytes; not a logical source certification';
comment on column ingestion.raw_orphan_purge.physical_bytes is 'Exact physical file size in bytes';
comment on column ingestion.raw_orphan_purge.mtime_ns is 'Verified file mtime in integer Unix nanoseconds';
comment on column ingestion.raw_orphan_purge.ctime_ns is 'Verified file ctime in integer Unix nanoseconds';
comment on column ingestion.raw_orphan_purge.cutoff is 'Exclusive Shanghai online file-age boundary';
comment on column ingestion.raw_orphan_purge.declared_at is 'Committed before unlink; no new source manifest may claim this path';
comment on column ingestion.raw_orphan_purge.online_removed_at is 'Confirmed exact file removal after directory fsync';
create index raw_orphan_purge_pending_idx on ingestion.raw_orphan_purge(storage_identity_sha256,purge_id)
    where online_removed_at is null;
create index raw_orphan_purge_logical_path_idx on ingestion.raw_orphan_purge
    ((regexp_replace(object_path,'\.gz$','')));

create function ingestion.declare_orphan_raw_purge(p_root text,p_path text,p_hash text,
    p_bytes bigint,p_mtime bigint,p_ctime bigint,p_cutoff timestamptz)
returns uuid language plpgsql security definer set search_path=pg_catalog as $$
declare existing ingestion.raw_orphan_purge; logical_path text; run_id uuid;
begin
    if p_cutoff>((date_trunc('day',now() at time zone 'Asia/Shanghai')-interval '1 month')
                  at time zone 'Asia/Shanghai')
       or greatest(p_mtime,p_ctime)>=extract(epoch from p_cutoff)*1000000000 then
        raise exception 'Orphan Raw is not expired' using errcode='55000';
    end if;
    if p_path !~ '^[a-z][a-z0-9_]*/[a-z][a-z0-9_]*/year=[0-9]{4}/month=[0-9]{2}/day=[0-9]{2}/[0-9a-f-]{36}\.(jsonl|parquet)(\.gz)?$' then
        raise exception 'Unsupported orphan Raw path' using errcode='22023';
    end if;
    logical_path=regexp_replace(p_path,'\.gz$','');
    run_id=regexp_replace(split_part(logical_path,'/',6),'\.(jsonl|parquet)$','')::uuid;
    perform pg_advisory_xact_lock(hashtextextended('raw-orphan:'||logical_path,0));
    if exists(select 1 from ingestion.raw_manifest where storage_backend='local' and object_path=logical_path)
       or exists(select 1 from ingestion.ingestion_run where ingestion_id=run_id and status='running') then
        return null;
    end if;
    insert into ingestion.raw_orphan_purge(storage_identity_sha256,object_path,physical_sha256,
         physical_bytes,mtime_ns,ctime_ns,cutoff)
    values(p_root,p_path,p_hash,p_bytes,p_mtime,p_ctime,(p_cutoff at time zone 'Asia/Shanghai')::date)
    on conflict(storage_identity_sha256,object_path) do nothing;
    select * into existing from ingestion.raw_orphan_purge
        where storage_identity_sha256=p_root and object_path=p_path for update;
    if (existing.physical_sha256,existing.physical_bytes,existing.mtime_ns,existing.ctime_ns)
         is distinct from (p_hash,p_bytes,p_mtime,p_ctime) then
        raise exception 'Orphan Raw identity changed' using errcode='55000';
    end if;
    return existing.purge_id;
end;
$$;
create function ingestion.confirm_orphan_raw_purge(p_id uuid,p_root text)
returns void language plpgsql security definer set search_path=pg_catalog as $$
begin
    update ingestion.raw_orphan_purge set online_removed_at=coalesce(online_removed_at,clock_timestamp())
    where purge_id=p_id and storage_identity_sha256=p_root;
    if not found then raise exception 'Orphan purge declaration missing' using errcode='55000'; end if;
end;
$$;
create function ingestion.reject_purged_orphan_manifest() returns trigger
language plpgsql security definer set search_path=pg_catalog as $$
begin
    perform pg_advisory_xact_lock(hashtextextended('raw-orphan:'||new.object_path,0));
    if new.storage_backend='local' and exists(select 1 from ingestion.raw_orphan_purge
         where regexp_replace(object_path,'\.gz$','')=new.object_path) then
        raise exception 'Raw path permanently expired before registration' using errcode='55000';
    end if;
    return new;
end;
$$;
create trigger reject_purged_orphan_manifest before insert on ingestion.raw_manifest
    for each row execute function ingestion.reject_purged_orphan_manifest();
revoke all on function ingestion.declare_orphan_raw_purge(text,text,text,bigint,bigint,bigint,timestamptz)
    from public,anon,authenticated,market_data_api;
revoke all on function ingestion.confirm_orphan_raw_purge(uuid,text)
    from public,anon,authenticated,market_data_api;
revoke all on function ingestion.reject_purged_orphan_manifest() from public;
grant execute on function ingestion.declare_orphan_raw_purge(text,text,text,bigint,bigint,bigint,timestamptz)
    to market_data_worker;
grant execute on function ingestion.confirm_orphan_raw_purge(uuid,text) to market_data_worker;
comment on function ingestion.declare_orphan_raw_purge(text,text,text,bigint,bigint,bigint,timestamptz) is
    'ADR-0073 exact old unpublished Raw physical declaration; serializes late registration';
comment on function ingestion.confirm_orphan_raw_purge(uuid,text) is 'ADR-0073 exact idempotent orphan unlink confirmation';
comment on function ingestion.reject_purged_orphan_manifest() is 'ADR-0073 rejects late source manifest for a permanently purged orphan';
