"""Default-preview certification of one unchanged, terminal auction attempt."""

import argparse
import json
from pathlib import Path
from uuid import UUID

from sqlalchemy import create_engine

from market_data_center.auction_quantity_certification import certify_existing
from market_data_center.database_urls import sqlalchemy_url
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import WorkerSettings


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ingestion-id", type=UUID, required=True)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    settings = WorkerSettings()
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), hide_parameters=True
    )
    try:
        plan = json.loads(args.plan.read_text(encoding="utf-8")) if args.apply else None
        result = certify_existing(
            engine, LocalRawStore(settings.raw_data_root), args.ingestion_id, plan=plan
        )
        if not args.apply:
            with args.plan.open("x", encoding="utf-8") as output:
                json.dump(result, output, ensure_ascii=False, sort_keys=True)
            print(
                json.dumps(
                    {
                        "status": "preview",
                        "plan_hash": result["plan_hash"],
                        "proofs": result["evidence"]["proofs"],
                    }
                )
            )
        else:
            print(json.dumps(result))
    finally:
        engine.dispose()


if __name__ == "__main__":
    main()
