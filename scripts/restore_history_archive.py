"""ADR-0070 operator recovery. Defaults to verification/preview; never schedules recovery."""

import argparse
import json
from pathlib import Path
from uuid import UUID

import psycopg
from sqlalchemy import create_engine

from market_data_center.persistence.history_retention_postgres import HistoryRetentionPersistence
from market_data_center.persistence.postgres import PostgreSQLPersistence


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--archive-id", required=True, type=UUID)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("--confirm", action="store_true")
    args = parser.parse_args()
    if args.execute != args.confirm:
        parser.error("execution requires --execute and --confirm")
    # Standard PG* admin environment, no URL or secret printed.
    engine = create_engine("postgresql+psycopg://", creator=lambda: psycopg.connect(""))
    try:
        repo = HistoryRetentionPersistence(engine)
        if args.execute:
            with PostgreSQLPersistence(engine).task_lock("maintenance:data_cleanup"):
                result = repo.restore(args.archive_id, args.root, execute=True)
        else:
            result = repo.restore(args.archive_id, args.root)
        print(json.dumps(result))
    except Exception as error:
        print(json.dumps({"error": type(error).__name__}))
        raise SystemExit(1) from None
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
