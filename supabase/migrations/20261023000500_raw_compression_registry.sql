-- ADR-0058: representation metadata only; no Raw content or manifest is changed here.
create table ingestion.raw_compression (
    raw_id uuid primary key references ingestion.raw_manifest(raw_id),
    object_path text not null unique,
    encoding text not null default 'gzip' check (encoding = 'gzip'),
    content_sha256 text not null check (content_sha256 ~ '^[0-9a-f]{64}$'),
    content_bytes bigint not null check (content_bytes between 0 and 268435456),
    row_count bigint not null check (row_count >= 0),
    compressed_sha256 text not null check (compressed_sha256 ~ '^[0-9a-f]{64}$'),
    compressed_bytes bigint not null check (compressed_bytes between 1 and 269484032),
    verified_at timestamptz not null default now(),
    plaintext_removed_at timestamptz,
    check (plaintext_removed_at is null or plaintext_removed_at >= verified_at)
);
alter table ingestion.raw_compression enable row level security;
grant select, insert on ingestion.raw_compression to market_data_worker;
grant update (plaintext_removed_at) on ingestion.raw_compression to market_data_worker;
grant select on ingestion.raw_compression to market_data_backup;
create policy raw_compression_worker_select on ingestion.raw_compression
    for select to market_data_worker using (true);
create policy raw_compression_worker_insert on ingestion.raw_compression
    for insert to market_data_worker with check (
        plaintext_removed_at is null and exists (
            select 1 from ingestion.raw_manifest m
            join ingestion.ingestion_run i using (ingestion_id)
            where m.raw_id=raw_compression.raw_id and m.storage_backend='local'
              and m.file_format='jsonl' and m.object_path || '.gz'=raw_compression.object_path
              and m.content_sha256=raw_compression.content_sha256
              and m.byte_size=raw_compression.content_bytes and m.row_count=raw_compression.row_count
              and m.created_at < ((current_timestamp at time zone 'Asia/Shanghai')::date-7)
                    ::timestamp at time zone 'Asia/Shanghai'
              and i.status in ('succeeded','partial','failed')
              and i.finished_at < ((current_timestamp at time zone 'Asia/Shanghai')::date-7)
                    ::timestamp at time zone 'Asia/Shanghai'
        )
    );
create policy raw_compression_worker_confirm on ingestion.raw_compression
    for update to market_data_worker using (plaintext_removed_at is null)
    with check (plaintext_removed_at is not null);
create index raw_manifest_compression_candidates_idx
    on ingestion.raw_manifest ((-byte_size), created_at, raw_id)
    where storage_backend='local' and file_format='jsonl' and byte_size <= 268435456;
comment on table ingestion.raw_compression is 'ADR-0058 verified lossless Raw representation; immutable original manifest remains authoritative; no public access';
comment on column ingestion.raw_compression.raw_id is 'Original immutable Raw manifest identity';
comment on column ingestion.raw_compression.object_path is 'Relative representation path: original object_path plus .gz';
comment on column ingestion.raw_compression.encoding is 'Lossless representation encoding, gzip';
comment on column ingestion.raw_compression.content_sha256 is 'Original uncompressed content SHA-256, identical to manifest';
comment on column ingestion.raw_compression.content_bytes is 'Original content size in bytes, identical to manifest';
comment on column ingestion.raw_compression.row_count is 'Original JSONL row count, identical to manifest';
comment on column ingestion.raw_compression.compressed_sha256 is 'Verified gzip file SHA-256';
comment on column ingestion.raw_compression.compressed_bytes is 'Verified gzip file size in bytes';
comment on column ingestion.raw_compression.verified_at is 'Timestamp of committed verification and registration';
comment on column ingestion.raw_compression.plaintext_removed_at is 'Confirmation that the matching plaintext representation was removed after registration';
