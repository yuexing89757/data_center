-- ADR-0069: explicitly authorized offline retirement; original manifests remain immutable.
create table ingestion.raw_retirement (
    raw_id uuid primary key references ingestion.raw_manifest(raw_id),
    archive_ref text not null check (length(archive_ref) between 1 and 200),
    archive_manifest_sha256 text not null check (archive_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    content_sha256 text not null check (content_sha256 ~ '^[0-9a-f]{64}$'),
    content_bytes bigint not null check (content_bytes >= 0),
    row_count bigint not null check (row_count >= 0),
    retirement_cutoff date not null,
    retired_at timestamptz not null default now(),
    online_removed_at timestamptz,
    restored_at timestamptz,
    check (online_removed_at is null or online_removed_at >= retired_at),
    check (restored_at is null or restored_at >= retired_at)
);
alter table ingestion.raw_retirement enable row level security;
revoke all on ingestion.raw_retirement from public, anon, authenticated, market_data_api;
grant select on ingestion.raw_retirement to market_data_worker, market_data_backup;
create policy raw_retirement_worker_read on ingestion.raw_retirement
    for select to market_data_worker using (true);
comment on table ingestion.raw_retirement is 'ADR-0069 admin-only offline retirement evidence; Worker read-only; original Raw manifest is unchanged';
comment on column ingestion.raw_retirement.raw_id is 'Immutable original Raw manifest identifier';
comment on column ingestion.raw_retirement.archive_ref is 'Restricted logical offline archive set reference; never an API field';
comment on column ingestion.raw_retirement.archive_manifest_sha256 is 'SHA-256 of verified sealed offline index covering this Raw';
comment on column ingestion.raw_retirement.content_sha256 is 'Original logical Raw SHA-256, must match immutable manifest during retirement';
comment on column ingestion.raw_retirement.content_bytes is 'Original logical content size, bytes';
comment on column ingestion.raw_retirement.row_count is 'Original logical JSONL row count, records';
comment on column ingestion.raw_retirement.retirement_cutoff is 'Authorized exclusive Shanghai business-date boundary';
comment on column ingestion.raw_retirement.retired_at is 'Offline declaration committed before exact online file removal';
comment on column ingestion.raw_retirement.online_removed_at is 'Confirmed matching online representations removed and parent directories synchronized';
comment on column ingestion.raw_retirement.restored_at is 'Admin confirmation after exact original content has been restored and verified';

-- Serialize replay creation with administrator retirement, including callers that
-- read replay eligibility before retirement committed. No schema-owner escalation.
create function ingestion.reject_offline_raw_replay() returns trigger
language plpgsql set search_path = pg_catalog, ingestion as $$
begin
    if new.replayed_from_raw_id is null then return new; end if;
    perform pg_advisory_xact_lock(hashtextextended('raw-retirement:' || new.replayed_from_raw_id::text, 0));
    if exists(select 1 from ingestion.raw_retirement
              where raw_id=new.replayed_from_raw_id and restored_at is null) then
        raise exception 'Raw object is archived offline; restore and verify it first'
            using errcode='55000';
    end if;
    return new;
end;
$$;
create trigger reject_offline_raw_replay
    before insert on ingestion.ingestion_run
    for each row execute function ingestion.reject_offline_raw_replay();
comment on function ingestion.reject_offline_raw_replay() is 'ADR-0069 atomic replay creation guard; same Raw advisory lock as administrator retirement';
