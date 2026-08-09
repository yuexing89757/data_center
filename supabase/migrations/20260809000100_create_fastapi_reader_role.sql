do $$
begin
    if not exists (select 1 from pg_roles where rolname = 'market_data_api') then
        create role market_data_api nologin;
    elsif exists (
        select 1
        from pg_roles
        where rolname = 'market_data_api'
          and (rolsuper or rolcreaterole or rolcreatedb or rolcanlogin or rolreplication or rolbypassrls)
    ) then
        raise exception 'existing market_data_api role has unsafe attributes';
    end if;
end
$$;

revoke all on schema api_v1 from market_data_api;
grant usage on schema api_v1 to market_data_api;

grant select on api_v1.securities,
    api_v1.daily_bars,
    api_v1.classification_member_snapshot_status,
    api_v1.classification_member_snapshots
    to market_data_api;

grant execute on function api_v1.query_securities(text, integer)
    to market_data_api;
grant execute on function api_v1.query_daily_bars(text, date, date, integer)
    to market_data_api;
grant execute on function api_v1.query_classification_members_as_of(
    text, text, text, date, integer
) to market_data_api;
