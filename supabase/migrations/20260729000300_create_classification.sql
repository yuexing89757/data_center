create schema if not exists classification;
revoke all on schema classification from public;
grant usage on schema classification to market_data_worker;

alter table ingestion.ingestion_run drop constraint ingestion_run_dataset_check;
alter table ingestion.ingestion_run add constraint ingestion_run_dataset_check check (
    dataset_code in (
        'security', 'trading_calendar', 'daily_bar', 'capital',
        'classification_catalog', 'classification_members'
    )
);
alter table audit.quality_result drop constraint quality_result_dataset_check;
alter table audit.quality_result add constraint quality_result_dataset_check check (
    dataset_code in (
        'security', 'trading_calendar', 'daily_bar', 'capital',
        'classification_catalog', 'classification_members'
    )
);

create table classification.catalog_snapshot (
    namespace text not null,
    classification_type text not null,
    snapshot_date date not null,
    definition_count integer not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (namespace, classification_type, snapshot_date),
    constraint catalog_snapshot_namespace_check check (btrim(namespace) <> ''),
    constraint catalog_snapshot_type_check check (
        classification_type in ('industry', 'concept', 'index')
    ),
    constraint catalog_snapshot_count_check check (definition_count >= 0)
);

create table classification.definition_snapshot (
    namespace text not null,
    classification_type text not null,
    snapshot_date date not null,
    classification_code text not null,
    name text not null,
    level integer not null,
    parent_code text,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (
        namespace, classification_type, snapshot_date, classification_code
    ),
    foreign key (namespace, classification_type, snapshot_date)
        references classification.catalog_snapshot
            (namespace, classification_type, snapshot_date)
        on delete cascade,
    foreign key (namespace, classification_type, snapshot_date, parent_code)
        references classification.definition_snapshot
            (namespace, classification_type, snapshot_date, classification_code)
        deferrable initially deferred,
    constraint definition_snapshot_code_check check (btrim(classification_code) <> ''),
    constraint definition_snapshot_name_check check (btrim(name) <> ''),
    constraint definition_snapshot_level_check check (level > 0),
    constraint definition_snapshot_parent_check check (
        parent_code is null or parent_code <> classification_code
    )
);

create table classification.member_snapshot (
    namespace text not null,
    classification_type text not null,
    classification_code text not null,
    snapshot_date date not null,
    member_count integer not null,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (
        namespace, classification_type, classification_code, snapshot_date
    ),
    foreign key (
        namespace, classification_type, snapshot_date, classification_code
    ) references classification.definition_snapshot (
        namespace, classification_type, snapshot_date, classification_code
    ) on delete cascade,
    constraint member_snapshot_count_check check (member_count >= 0)
);

create table classification.member_snapshot_item (
    namespace text not null,
    classification_type text not null,
    classification_code text not null,
    snapshot_date date not null,
    symbol text not null references core.security (symbol),
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    primary key (
        namespace, classification_type, classification_code, snapshot_date, symbol
    ),
    foreign key (
        namespace, classification_type, classification_code, snapshot_date
    ) references classification.member_snapshot (
        namespace, classification_type, classification_code, snapshot_date
    ) on delete cascade
);

create table classification.member_interval (
    namespace text not null,
    classification_type text not null,
    classification_code text not null,
    symbol text not null references core.security (symbol),
    valid_from date not null,
    valid_to date,
    source_code text not null,
    ingestion_id uuid not null references ingestion.ingestion_run (ingestion_id),
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    primary key (
        namespace, classification_type, classification_code, symbol, valid_from
    ),
    constraint member_interval_order_check check (
        valid_to is null or valid_to >= valid_from
    ),
    constraint member_interval_type_check check (
        classification_type in ('industry', 'concept', 'index')
    ),
    exclude using gist (
        namespace with =,
        classification_type with =,
        classification_code with =,
        symbol with =,
        daterange(valid_from, coalesce(valid_to, 'infinity'::date), '[]') with &&
    )
);

create index catalog_snapshot_ingestion_idx
    on classification.catalog_snapshot (ingestion_id);
create index definition_snapshot_date_idx
    on classification.definition_snapshot (snapshot_date desc, classification_code);
create index member_snapshot_item_symbol_idx
    on classification.member_snapshot_item (symbol, snapshot_date desc);
create index member_snapshot_item_ingestion_idx
    on classification.member_snapshot_item (ingestion_id);
create index member_interval_as_of_idx
    on classification.member_interval (symbol, valid_from, valid_to);

create trigger catalog_snapshot_set_updated_at
before update on classification.catalog_snapshot
for each row execute function ingestion.set_updated_at();
create trigger member_snapshot_set_updated_at
before update on classification.member_snapshot
for each row execute function ingestion.set_updated_at();
create trigger member_interval_set_updated_at
before update on classification.member_interval
for each row execute function ingestion.set_updated_at();

alter table classification.catalog_snapshot enable row level security;
alter table classification.definition_snapshot enable row level security;
alter table classification.member_snapshot enable row level security;
alter table classification.member_snapshot_item enable row level security;
alter table classification.member_interval enable row level security;

create policy catalog_snapshot_worker_all on classification.catalog_snapshot
    for all to market_data_worker using (true) with check (true);
create policy definition_snapshot_worker_all on classification.definition_snapshot
    for all to market_data_worker using (true) with check (true);
create policy member_snapshot_worker_all on classification.member_snapshot
    for all to market_data_worker using (true) with check (true);
create policy member_snapshot_item_worker_all on classification.member_snapshot_item
    for all to market_data_worker using (true) with check (true);
create policy member_interval_worker_all on classification.member_interval
    for all to market_data_worker using (true) with check (true);

grant select, insert, update, delete on classification.catalog_snapshot to market_data_worker;
grant select, insert, delete on classification.definition_snapshot to market_data_worker;
grant select, insert, update, delete on classification.member_snapshot to market_data_worker;
grant select, insert, delete on classification.member_snapshot_item to market_data_worker;
grant select, insert, update on classification.member_interval to market_data_worker;

create view api_v1.classification_catalog_snapshots as
select namespace, classification_type, snapshot_date, classification_code,
       name, level, parent_code
from classification.definition_snapshot;

create view api_v1.classification_member_snapshots as
select namespace, classification_type, classification_code, snapshot_date, symbol
from classification.member_snapshot_item;

create view api_v1.classification_member_intervals as
select namespace, classification_type, classification_code, symbol,
       valid_from, valid_to
from classification.member_interval;

revoke all on
    api_v1.classification_catalog_snapshots,
    api_v1.classification_member_snapshots,
    api_v1.classification_member_intervals
from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        grant select on
            api_v1.classification_catalog_snapshots,
            api_v1.classification_member_snapshots,
            api_v1.classification_member_intervals
        to anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        grant select on
            api_v1.classification_catalog_snapshots,
            api_v1.classification_member_snapshots,
            api_v1.classification_member_intervals
        to authenticated;
    end if;
end
$$;
