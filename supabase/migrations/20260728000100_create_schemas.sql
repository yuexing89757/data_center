create schema if not exists extensions;

create extension if not exists btree_gist with schema extensions;
create extension if not exists pgcrypto with schema extensions;

create schema if not exists ingestion;
create schema if not exists audit;
create schema if not exists core;
create schema if not exists api_v1;

revoke all on schema ingestion, audit, core from public;
revoke all on schema api_v1 from public;

do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'market_data_worker') then
        create role market_data_worker nologin;
    end if;
end
$$;

grant usage on schema ingestion, audit, core to market_data_worker;

create or replace function ingestion.set_updated_at()
returns trigger
language plpgsql
set search_path = pg_catalog
as $$
begin
    new.updated_at = now();
    return new;
end
$$;

revoke all on function ingestion.set_updated_at() from public;
