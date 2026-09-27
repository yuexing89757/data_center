-- ADR-0058: immutable evidence before exact-ID deletion. No data is deleted here.
create table audit.quality_archive (
    archive_id uuid primary key,
    ingestion_id uuid not null references ingestion.ingestion_run(ingestion_id),
    rule_code text not null check (rule_code in (
        'realtime_quote.lot_precision', 'realtime_quote.missing_source_timestamp')),
    schema_version text not null default 'quality_archive.v1'
        check (schema_version = 'quality_archive.v1'),
    object_path text not null unique,
    content_sha256 text not null check (content_sha256 ~ '^[0-9a-f]{64}$'),
    content_bytes bigint not null check (content_bytes between 1 and 268435456),
    compressed_sha256 text not null check (compressed_sha256 ~ '^[0-9a-f]{64}$'),
    compressed_bytes bigint not null check (compressed_bytes between 1 and 269484032),
    row_count integer not null check (row_count between 1 and 100000),
    quality_ids uuid[] not null,
    first_created_at timestamptz not null,
    last_created_at timestamptz not null,
    archived_at timestamptz not null default now(),
    unique (ingestion_id, rule_code),
    check (cardinality(quality_ids) = row_count and array_position(quality_ids, null) is null),
    check (first_created_at <= last_created_at),
    check (object_path = '_quality_archive/' || ingestion_id::text || '/' || rule_code
           || '/' || content_sha256 || '.jsonl.gz')
);
alter table audit.quality_archive enable row level security;
grant select, insert on audit.quality_archive to market_data_worker;
create policy quality_archive_worker_select on audit.quality_archive
    for select to market_data_worker using (true);
create policy quality_archive_worker_insert on audit.quality_archive
    for insert to market_data_worker with check (
        last_created_at < ((current_timestamp at time zone 'Asia/Shanghai')::date - 30)
            ::timestamp at time zone 'Asia/Shanghai'
        and exists (select 1 from ingestion.ingestion_run i
            where i.ingestion_id = quality_archive.ingestion_id
              and i.dataset_code = 'call_auction_market_series'
              and i.status in ('succeeded', 'partial', 'failed'))
    );

drop policy quality_result_worker_all on audit.quality_result;
create policy quality_result_worker_select on audit.quality_result
    for select to market_data_worker using (true);
create policy quality_result_worker_insert on audit.quality_result
    for insert to market_data_worker with check (true);
-- Keep INSERT/SELECT unchanged. DELETE is a narrow capability, never UPDATE/DDL.
create policy quality_result_worker_archived_delete on audit.quality_result
    for delete to market_data_worker using (
        dataset_code = 'call_auction_market_series'
        and ((rule_code = 'realtime_quote.lot_precision' and severity = 'info')
             or (rule_code = 'realtime_quote.missing_source_timestamp' and severity = 'warning'))
        and jsonb_typeof(details) = 'object' and not details ? 'aggregation_version'
        and created_at < ((current_timestamp at time zone 'Asia/Shanghai')::date - 30)
            ::timestamp at time zone 'Asia/Shanghai'
        and exists (select 1 from ingestion.ingestion_run i
            where i.ingestion_id = quality_result.ingestion_id
              and i.dataset_code = 'call_auction_market_series'
              and i.status in ('succeeded', 'partial', 'failed'))
        and exists (select 1 from audit.quality_archive a
            where a.ingestion_id = quality_result.ingestion_id
              and a.rule_code = quality_result.rule_code
              and quality_result.quality_result_id = any(a.quality_ids)
              and quality_result.created_at between a.first_created_at and a.last_created_at)
    );

create index quality_result_archive_candidate_idx
    on audit.quality_result (ingestion_id, rule_code, quality_result_id)
    where dataset_code = 'call_auction_market_series'
      and ((rule_code = 'realtime_quote.lot_precision' and severity = 'info')
           or (rule_code = 'realtime_quote.missing_source_timestamp' and severity = 'warning'))
      and jsonb_typeof(details) = 'object' and not details ? 'aggregation_version';

create table operations.data_cleanup_report (
    workflow_run_id uuid primary key references operations.workflow_run(workflow_run_id),
    reference_date date not null,
    started_at timestamptz not null,
    finished_at timestamptz,
    cutoffs jsonb not null check (jsonb_typeof(cutoffs) = 'object'),
    steps jsonb not null default '{}' check (jsonb_typeof(steps) = 'object'),
    disks jsonb not null default '{}' check (jsonb_typeof(disks) = 'object'),
    owned_temps jsonb not null default '[]' check (jsonb_typeof(owned_temps) = 'array'),
    status text not null check (status in ('running', 'succeeded', 'partial', 'failed')),
    check ((status = 'running' and finished_at is null)
        or (status <> 'running' and finished_at is not null and finished_at >= started_at))
);
alter table operations.data_cleanup_report enable row level security;
grant select, insert on operations.data_cleanup_report to market_data_worker;
grant update (finished_at, steps, disks, owned_temps, status)
    on operations.data_cleanup_report to market_data_worker;
create policy data_cleanup_report_worker_select on operations.data_cleanup_report
    for select to market_data_worker using (true);
create policy data_cleanup_report_worker_insert on operations.data_cleanup_report
    for insert to market_data_worker with check (status = 'running' and exists (
        select 1 from operations.workflow_run w where w.workflow_run_id = data_cleanup_report.workflow_run_id
        and w.workflow_code = 'data_cleanup' and w.status = 'running'));
create policy data_cleanup_report_worker_update on operations.data_cleanup_report
    for update to market_data_worker using (status = 'running')
    with check (exists (select 1 from operations.workflow_run w
        where w.workflow_run_id = data_cleanup_report.workflow_run_id and w.workflow_code = 'data_cleanup'));
