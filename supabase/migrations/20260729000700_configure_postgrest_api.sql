-- Keep the self-hosted PostgREST exposure aligned with the api_v1 contract.
-- The conditional keeps plain PostgreSQL CI databases compatible when the
-- Supabase authenticator role is not installed.
do $$
begin
    if exists (select 1 from pg_roles where rolname = 'authenticator') then
        alter role authenticator set pgrst.db_schemas = 'api_v1';
        alter role authenticator set pgrst.db_extra_search_path = 'extensions';
    end if;
end
$$;

notify pgrst, 'reload config';
notify pgrst, 'reload schema';
