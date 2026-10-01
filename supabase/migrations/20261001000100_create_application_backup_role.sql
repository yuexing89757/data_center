-- Complete application dumps must see every row without widening Worker/API access.
-- Credentials are configured on the operations host, never in migrations.
do $$
declare
    schema_name text;
begin
    if not exists (select 1 from pg_roles where rolname = 'market_data_backup') then
        create role market_data_backup login bypassrls
            nosuperuser nocreatedb nocreaterole noinherit noreplication;
    elsif exists (
        select 1 from pg_roles where rolname = 'market_data_backup'
          and (rolsuper or rolcreatedb or rolcreaterole or rolinherit or rolreplication
               or not rolcanlogin or not rolbypassrls)
    ) then
        raise exception 'existing market_data_backup role has unexpected attributes';
    end if;

    foreach schema_name in array array[
        'audit', 'billboard', 'capital', 'classification', 'convertible_bond',
        'core', 'derived', 'ingestion', 'metrics', 'operations', 'realtime',
        'regulation', 'stock_pool', 'today_limit_down', 'today_limit_up'
    ] loop
        execute format('grant usage on schema %I to market_data_backup', schema_name);
        execute format('grant select on all tables in schema %I to market_data_backup', schema_name);
        execute format('grant select on all sequences in schema %I to market_data_backup', schema_name);
    end loop;
end
$$;

grant usage on schema supabase_migrations to market_data_backup;
grant select on supabase_migrations.schema_migrations to market_data_backup;
alter role market_data_backup set default_transaction_read_only = on;
