-- ADR-0073: owner explicitly cancelled archival of expired Raw on 2026-10-24.
-- Permanent purge is a separate fact, never a forged offline-archive declaration.
create table ingestion.raw_purge (
    raw_id uuid primary key references ingestion.raw_manifest(raw_id),
    storage_identity_sha256 text not null check(storage_identity_sha256 ~ '^[0-9a-f]{64}$'),
    content_sha256 text not null check(content_sha256 ~ '^[0-9a-f]{64}$'),
    content_bytes bigint not null check(content_bytes >= 0),
    row_count bigint not null check(row_count >= 0),
    cutoff date not null,
    declared_at timestamptz not null default clock_timestamp(),
    online_removed_at timestamptz,
    check(online_removed_at is null or online_removed_at >= declared_at)
);
alter table ingestion.raw_purge enable row level security;
revoke all on ingestion.raw_purge from public,anon,authenticated,market_data_api;
grant select on ingestion.raw_purge to market_data_worker,market_data_backup;
create policy raw_purge_read on ingestion.raw_purge for select
    to market_data_worker,market_data_backup using(true);
comment on table ingestion.raw_purge is 'ADR-0073 permanent Raw expiry without archival; immutable manifest/standard facts remain';
comment on column ingestion.raw_purge.raw_id is 'Original immutable Raw identity; records remain after file deletion';
comment on column ingestion.raw_purge.storage_identity_sha256 is 'Exact canonical path/device/inode identity SHA-256';
comment on column ingestion.raw_purge.content_sha256 is 'Original logical content SHA-256, not an archive proof';
comment on column ingestion.raw_purge.content_bytes is 'Original logical content size in bytes';
comment on column ingestion.raw_purge.row_count is 'Original logical row count';
comment on column ingestion.raw_purge.cutoff is 'Exclusive Shanghai collection-age cutoff, one calendar month';
comment on column ingestion.raw_purge.declared_at is 'Permanent purge declared before unlink; blocks new replay';
comment on column ingestion.raw_purge.online_removed_at is 'Verified online representations removed and directories synced';
create index raw_purge_pending_idx on ingestion.raw_purge(raw_id) where online_removed_at is null;
create index raw_manifest_expiry_idx on ingestion.raw_manifest(created_at,raw_id)
    where storage_backend='local' and file_format in ('jsonl','parquet');

create function ingestion.declare_expired_raw_purge(
    p_id uuid,p_cutoff timestamptz,p_root text,p_hash text,p_bytes bigint,p_rows bigint
) returns boolean language plpgsql security definer set search_path=pg_catalog as $$
declare m ingestion.raw_manifest; existing ingestion.raw_purge;
begin
    if p_cutoff > ((date_trunc('day',now() at time zone 'Asia/Shanghai')-interval '1 month')
                   at time zone 'Asia/Shanghai') then
        raise exception 'Raw purge cutoff exceeds monthly policy' using errcode='22023';
    end if;
    perform pg_advisory_xact_lock(hashtextextended('raw-retirement:'||p_id::text,0));
    select manifest.* into m from ingestion.raw_manifest manifest
    join ingestion.ingestion_run run using(ingestion_id)
    where manifest.raw_id=p_id and manifest.created_at<p_cutoff and run.finished_at<p_cutoff
      and run.status in ('succeeded','partial','failed')
      and manifest.storage_backend='local' and manifest.file_format in ('jsonl','parquet');
    if not found or (m.content_sha256,m.byte_size,m.row_count) is distinct from
                    (p_hash,p_bytes,p_rows) then
        raise exception 'Raw purge identity or age changed' using errcode='55000';
    end if;
    if exists(select 1 from ingestion.ingestion_run
              where replayed_from_raw_id=p_id and status='running') then return false; end if;
    insert into ingestion.raw_purge(raw_id,storage_identity_sha256,content_sha256,
         content_bytes,row_count,cutoff)
    values(p_id,p_root,p_hash,p_bytes,p_rows,(p_cutoff at time zone 'Asia/Shanghai')::date)
    on conflict(raw_id) do nothing;
    select * into existing from ingestion.raw_purge where raw_id=p_id for update;
    if (existing.storage_identity_sha256,existing.content_sha256,existing.content_bytes,
        existing.row_count) is distinct from (p_root,p_hash,p_bytes,p_rows) then
        raise exception 'Raw purge declaration changed' using errcode='55000';
    end if;
    return true;
end;
$$;
create function ingestion.confirm_expired_raw_purge(p_id uuid,p_root text)
returns void language plpgsql security definer set search_path=pg_catalog as $$
begin
    update ingestion.raw_purge set online_removed_at=coalesce(online_removed_at,clock_timestamp())
    where raw_id=p_id and storage_identity_sha256=p_root;
    if not found then raise exception 'Raw purge declaration missing' using errcode='55000'; end if;
end;
$$;
revoke all on function ingestion.declare_expired_raw_purge(uuid,timestamptz,text,text,bigint,bigint)
    from public,anon,authenticated,market_data_api;
revoke all on function ingestion.confirm_expired_raw_purge(uuid,text)
    from public,anon,authenticated,market_data_api;
grant execute on function ingestion.declare_expired_raw_purge(uuid,timestamptz,text,text,bigint,bigint)
    to market_data_worker;
grant execute on function ingestion.confirm_expired_raw_purge(uuid,text) to market_data_worker;
comment on function ingestion.declare_expired_raw_purge(uuid,timestamptz,text,text,bigint,bigint) is
    'ADR-0073 bounded exact aged Raw declaration; active replays protected; no archive implied';
comment on function ingestion.confirm_expired_raw_purge(uuid,text) is
    'ADR-0073 idempotent exact online unlink confirmation, no arbitrary metadata update';

create or replace function ingestion.reject_offline_raw_replay() returns trigger
language plpgsql set search_path=pg_catalog,ingestion as $$
begin
    if new.replayed_from_raw_id is null then return new; end if;
    perform pg_advisory_xact_lock(hashtextextended('raw-retirement:'||new.replayed_from_raw_id::text,0));
    if exists(select 1 from ingestion.raw_purge where raw_id=new.replayed_from_raw_id) then
        raise exception 'Raw object permanently removed by monthly retention policy; no archive guaranteed'
            using errcode='55000';
    end if;
    if exists(select 1 from ingestion.raw_retirement
              where raw_id=new.replayed_from_raw_id and restored_at is null) then
        raise exception 'Raw object is archived offline; restore and verify it first'
            using errcode='55000';
    end if;
    return new;
end;
$$;
-- Superseded archived-approval queue is not a scheduled write capability any more.
revoke execute on function ingestion.lock_approved_raw_retirement(uuid,timestamptz,text)
    from market_data_worker;
revoke execute on function ingestion.confirm_approved_raw_retirement(uuid,text)
    from market_data_worker;

create function ingestion.assert_raw_restorable(p_id uuid) returns void
language plpgsql set search_path=pg_catalog as $$
begin
    perform pg_advisory_xact_lock(hashtextextended('raw-retirement:'||p_id::text,0));
    if exists(select 1 from ingestion.raw_purge where raw_id=p_id) then
        raise exception 'Raw permanently expired; restoration requires a new explicit retention decision'
            using errcode='55000';
    end if;
end;
$$;
revoke all on function ingestion.assert_raw_restorable(uuid) from public,anon,authenticated,
    market_data_api,market_data_worker;
comment on function ingestion.assert_raw_restorable(uuid) is
    'Admin restore must call before touching files, holding transaction lock through publication';
create function ingestion.reject_purged_raw_restore() returns trigger
language plpgsql set search_path=pg_catalog as $$
begin
    if new.restored_at is distinct from old.restored_at and new.restored_at is not null then
        perform ingestion.assert_raw_restorable(new.raw_id);
    end if;
    return new;
end;
$$;
create trigger reject_purged_raw_restore before update of restored_at on ingestion.raw_retirement
    for each row execute function ingestion.reject_purged_raw_restore();
comment on function ingestion.reject_purged_raw_restore() is
    'Rejects legacy archive restore confirmation for permanently purged Raw';
