"""Read-only preflight for the standalone PostgreSQL FastAPI process."""

from argparse import ArgumentParser

import psycopg

from market_data_center.database_urls import psycopg_url
from market_data_center.settings import ApiSettings

PUBLISHED_FUNCTIONS = (
    "api_v1.query_securities(text,integer)",
    "api_v1.query_daily_bars(text,date,date,integer)",
    "api_v1.query_classification_members_as_of(text,text,text,date,integer)",
)


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument("--require-loopback", action="store_true")
    args = parser.parse_args()
    settings = ApiSettings()  # type: ignore[call-arg]
    if args.require_loopback and settings.fastapi_host != "127.0.0.1":
        raise SystemExit("FASTAPI_HOST must remain 127.0.0.1 before reverse-proxy approval")

    options = "-c default_transaction_read_only=on -c statement_timeout=5000"
    with psycopg.connect(
        psycopg_url(settings.resolved_database_url()), connect_timeout=10, options=options
    ) as connection:
        state = connection.execute(
            """
            select
                current_setting('transaction_read_only') = 'on',
                current_setting('statement_timeout') = '5s',
                pg_has_role(current_user, 'market_data_api', 'member')
            """
        ).fetchone()
        if state != (True, True, True):
            raise SystemExit("API connection is not the accepted read-only role")
        for function_name in PUBLISHED_FUNCTIONS:
            allowed = connection.execute(
                "select has_function_privilege(current_user, %s, 'EXECUTE')",
                (function_name,),
            ).fetchone()
            if not allowed or not allowed[0]:
                raise SystemExit("API role cannot execute the published v1 contract")
        forbidden = connection.execute(
            """
            select
                has_table_privilege(current_user, 'core.security', 'INSERT'),
                has_table_privilege(current_user, 'ingestion.ingestion_run', 'UPDATE'),
                has_schema_privilege(current_user, 'core', 'CREATE')
            """
        ).fetchone()
        if forbidden != (False, False, False):
            raise SystemExit("API role has forbidden internal write privileges")
    print("fastapi_release_preflight=passed")


if __name__ == "__main__":
    main()
