create schema if not exists operations;
grant usage on schema operations to market_data_worker;

create table operations.workflow_run (
    workflow_run_id uuid primary key,
    workflow_code text not null,
    scheduled_for timestamptz not null,
    trigger_source text not null check (trigger_source in ('scheduled', 'manual', 'recovery')),
    attempt integer not null check (attempt > 0),
    status text not null check (status in ('running', 'succeeded', 'failed', 'partial')),
    started_at timestamptz not null,
    finished_at timestamptz,
    accepted_rows bigint not null default 0 check (accepted_rows >= 0),
    rejected_rows bigint not null default 0 check (rejected_rows >= 0),
    error_summary text check (char_length(error_summary) <= 200),
    constraint workflow_run_attempt_unique unique (workflow_code, scheduled_for, attempt),
    constraint workflow_run_terminal_time check (
        (status = 'running' and finished_at is null)
        or (status <> 'running' and finished_at is not null)
    ),
    constraint workflow_run_time_order check (finished_at is null or finished_at >= started_at)
);

create table operations.job_execution (
    job_execution_id uuid primary key,
    workflow_run_id uuid not null references operations.workflow_run (workflow_run_id),
    job_code text not null,
    sequence_no integer not null check (sequence_no > 0),
    attempt integer not null check (attempt > 0),
    status text not null check (status in ('running', 'succeeded', 'failed', 'partial')),
    started_at timestamptz not null,
    finished_at timestamptz,
    fetched_rows bigint not null default 0 check (fetched_rows >= 0),
    accepted_rows bigint not null default 0 check (accepted_rows >= 0),
    rejected_rows bigint not null default 0 check (rejected_rows >= 0),
    error_summary text check (char_length(error_summary) <= 200),
    constraint job_execution_attempt_unique unique (workflow_run_id, job_code, attempt),
    constraint job_execution_counts check (accepted_rows + rejected_rows <= fetched_rows),
    constraint job_execution_terminal_time check (
        (status = 'running' and finished_at is null)
        or (status <> 'running' and finished_at is not null)
    ),
    constraint job_execution_time_order check (finished_at is null or finished_at >= started_at)
);

create index workflow_run_recent_idx on operations.workflow_run (started_at desc);
create index workflow_run_status_idx on operations.workflow_run (status, started_at);
create index job_execution_workflow_idx
    on operations.job_execution (workflow_run_id, sequence_no, attempt);
create index job_execution_status_idx on operations.job_execution (status, started_at);

alter table operations.workflow_run enable row level security;
alter table operations.job_execution enable row level security;
create policy workflow_run_worker_all on operations.workflow_run
    for all to market_data_worker using (true) with check (true);
create policy job_execution_worker_all on operations.job_execution
    for all to market_data_worker using (true) with check (true);
grant select, insert, update on operations.workflow_run to market_data_worker;
grant select, insert, update on operations.job_execution to market_data_worker;
