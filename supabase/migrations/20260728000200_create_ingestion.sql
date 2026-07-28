create table ingestion.ingestion_run (
    ingestion_id uuid primary key default extensions.gen_random_uuid(),
    provider_code text not null,
    dataset_code text not null,
    status text not null default 'pending',
    requested_at timestamptz not null default now(),
    started_at timestamptz,
    finished_at timestamptz,
    request_params jsonb not null default '{}'::jsonb,
    fetched_rows bigint not null default 0,
    accepted_rows bigint not null default 0,
    rejected_rows bigint not null default 0,
    error_summary text,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    constraint ingestion_run_provider_check check (provider_code = 'baostock'),
    constraint ingestion_run_dataset_check check (
        dataset_code in ('security', 'trading_calendar', 'daily_bar')
    ),
    constraint ingestion_run_status_check check (
        status in ('pending', 'running', 'succeeded', 'failed', 'partial')
    ),
    constraint ingestion_run_counts_nonnegative_check check (
        fetched_rows >= 0 and accepted_rows >= 0 and rejected_rows >= 0
    ),
    constraint ingestion_run_counts_consistent_check check (
        accepted_rows + rejected_rows <= fetched_rows
    ),
    constraint ingestion_run_terminal_time_check check (
        status not in ('succeeded', 'failed', 'partial') or finished_at is not null
    ),
    constraint ingestion_run_time_order_check check (
        (started_at is null or started_at >= requested_at)
        and (finished_at is null or (started_at is not null and finished_at >= started_at))
    )
);

create index ingestion_run_dataset_requested_idx
    on ingestion.ingestion_run (dataset_code, requested_at desc);
create index ingestion_run_status_requested_idx
    on ingestion.ingestion_run (status, requested_at desc);

create trigger ingestion_run_set_updated_at
before update on ingestion.ingestion_run
for each row execute function ingestion.set_updated_at();

create table ingestion.raw_manifest (
    raw_id uuid primary key default extensions.gen_random_uuid(),
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    storage_backend text not null default 'local',
    object_path text not null,
    file_format text not null,
    content_sha256 text not null,
    byte_size bigint not null,
    row_count bigint not null,
    schema_version text not null,
    created_at timestamptz not null default now(),
    constraint raw_manifest_storage_check check (storage_backend = 'local'),
    constraint raw_manifest_path_check check (
        object_path <> ''
        and object_path !~ '^/'
        and object_path !~ '(^|/)\.\.(/|$)'
    ),
    constraint raw_manifest_format_check check (file_format in ('parquet', 'jsonl')),
    constraint raw_manifest_sha256_check check (content_sha256 ~ '^[0-9a-f]{64}$'),
    constraint raw_manifest_size_check check (byte_size >= 0 and row_count >= 0),
    constraint raw_manifest_schema_version_check check (btrim(schema_version) <> ''),
    constraint raw_manifest_object_unique unique (storage_backend, object_path)
);

create index raw_manifest_ingestion_idx on ingestion.raw_manifest (ingestion_id);
create index raw_manifest_sha256_idx on ingestion.raw_manifest (content_sha256);

create table audit.quality_result (
    quality_result_id uuid primary key default extensions.gen_random_uuid(),
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    dataset_code text not null,
    rule_code text not null,
    severity text not null,
    status text not null,
    natural_key jsonb,
    message text not null,
    details jsonb not null default '{}'::jsonb,
    created_at timestamptz not null default now(),
    constraint quality_result_dataset_check check (
        dataset_code in ('security', 'trading_calendar', 'daily_bar')
    ),
    constraint quality_result_rule_check check (btrim(rule_code) <> ''),
    constraint quality_result_severity_check check (severity in ('info', 'warning', 'error')),
    constraint quality_result_status_check check (status in ('passed', 'failed')),
    constraint quality_result_message_check check (btrim(message) <> '')
);

create index quality_result_ingestion_idx on audit.quality_result (ingestion_id);
create index quality_result_failed_idx
    on audit.quality_result (dataset_code, rule_code, created_at desc)
    where status = 'failed';

alter table ingestion.ingestion_run enable row level security;
alter table ingestion.raw_manifest enable row level security;
alter table audit.quality_result enable row level security;

create policy ingestion_run_worker_all on ingestion.ingestion_run
    for all to market_data_worker using (true) with check (true);
create policy raw_manifest_worker_all on ingestion.raw_manifest
    for all to market_data_worker using (true) with check (true);
create policy quality_result_worker_all on audit.quality_result
    for all to market_data_worker using (true) with check (true);

grant select, insert, update on ingestion.ingestion_run to market_data_worker;
grant select, insert on ingestion.raw_manifest to market_data_worker;
grant select, insert on audit.quality_result to market_data_worker;
