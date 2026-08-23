begin;

revoke all on function api_v1.query_latest_stock_quotes(text[], integer) from public;

do $$
declare
    role_name text;
begin
    foreach role_name in array array['anon', 'authenticated', 'market_data_api'] loop
        if exists (select 1 from pg_roles where rolname = role_name) then
            execute format(
                'revoke all on function api_v1.query_latest_stock_quotes(text[], integer) from %I',
                role_name
            );
        end if;
    end loop;
end
$$;

drop function api_v1.query_latest_stock_quotes(text[], integer);

notify pgrst, 'reload schema';

commit;
