-- Restore ADR-0030's FastAPI-only execution boundary after the RPC rebuilds.
-- The FastAPI login inherits market_data_api; PostgREST client roles must not
-- execute this contract unless a later accepted decision explicitly permits it.

revoke all on function api_v1.query_daily_limit_up_list(
    date, integer, integer, integer
) from public;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'anon') then
        revoke all on function api_v1.query_daily_limit_up_list(
            date, integer, integer, integer
        ) from anon;
    end if;
    if exists (select 1 from pg_roles where rolname = 'authenticated') then
        revoke all on function api_v1.query_daily_limit_up_list(
            date, integer, integer, integer
        ) from authenticated;
    end if;
    if exists (select 1 from pg_roles where rolname = 'market_data_api') then
        grant execute on function api_v1.query_daily_limit_up_list(
            date, integer, integer, integer
        ) to market_data_api;
    end if;
end
$$;

