"""Controlled stock-volume certification/repair. Defaults to a read-only preview.

Examples (PowerShell; credentials and Raw root come from WorkerSettings):
  uv run python scripts/repair_stock_quantity_units.py --start 2026-08-01 \
      --end 2026-10-22 --provider pytdx --output D:/restricted/quantity-preview.json
  uv run python scripts/repair_stock_quantity_units.py --apply \
      --plan D:/restricted/quantity-preview.json --expected-hash <reviewed_sha256>
  uv run python scripts/repair_stock_quantity_units.py --rollback \
      --plan D:/restricted/quantity-preview.json --expected-hash <reviewed_sha256>

The preview is protected operational evidence, not a file to commit to Git.
Apply/rollback are explicit operator actions; this script is never a scheduled job.
Repair write batches are at most 500 facts; correct-history certification separately
locks/rechecks complete ingestion groups up to 5000 current facts. Larger groups
require a separately governed certification protocol; narrowing dates cannot prove
the completeness of an existing ingestion group.
"""

from argparse import ArgumentParser
from datetime import date
from json import dumps, loads
from pathlib import Path

from sqlalchemy import create_engine

from market_data_center.database_urls import sqlalchemy_url
from market_data_center.quantity_unit_repair import (
    RepairScope,
    apply_plan,
    certification_status,
    preview,
    rollback_plan,
    summary,
    verify_plan,
)
from market_data_center.raw_store import LocalRawStore
from market_data_center.settings import WorkerSettings


def main() -> int:
    parser = ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true", help="apply the exact reviewed preview")
    mode.add_argument(
        "--rollback", action="store_true", help="compare-and-set rollback of that plan"
    )
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--provider", action="append", choices=("pytdx", "akshare"))
    parser.add_argument("--symbol", action="append", default=[])
    parser.add_argument("--max-rows", type=int, default=50_000)
    parser.add_argument("--batch-size", type=int, default=500)
    parser.add_argument("--output", type=Path, help="new protected preview file; never overwritten")
    parser.add_argument("--plan", type=Path)
    parser.add_argument("--expected-hash", help="SHA256 copied from the reviewed preview summary")
    args = parser.parse_args()
    if args.apply or args.rollback:
        if not args.plan or not args.expected_hash:
            parser.error("--apply/--rollback require --plan and --expected-hash")
        if args.start or args.end or args.provider or args.symbol or args.output:
            parser.error("apply/rollback use the scope in the sealed preview only")
        plan = loads(args.plan.read_text(encoding="utf-8"))
        verify_plan(plan, args.expected_hash)
    elif not (args.start and args.end and args.provider and args.output):
        parser.error("preview requires --start, --end, --provider and --output")
    elif args.plan or args.expected_hash:
        parser.error("--plan and --expected-hash are for explicit apply/rollback only")
    if args.output and args.output.resolve().is_relative_to(Path(__file__).resolve().parents[1]):
        parser.error("protected preview evidence must be saved outside the repository")
    settings = WorkerSettings()
    # Never echo the URL or pass it as an argument shown in process listings.
    engine = create_engine(
        sqlalchemy_url(settings.database_url.get_secret_value()), hide_parameters=True
    )
    raw_store = LocalRawStore(settings.raw_data_root)
    try:
        if args.apply:
            repair_items = apply_plan(
                engine, raw_store, plan, args.expected_hash, batch_size=args.batch_size
            )
            result = {
                "plan_hash": plan["plan_hash"],
                "repair_items": repair_items,
                "field_group_certifications": certification_status(engine, plan),
            }
        elif args.rollback:
            repair_items = rollback_plan(
                engine, plan, args.expected_hash, batch_size=args.batch_size
            )
            result = {
                "plan_hash": plan["plan_hash"],
                "repair_items": repair_items,
                "field_group_certifications": certification_status(engine, plan),
            }
        else:
            scope = RepairScope(
                args.start,
                args.end,
                tuple(sorted(set(args.provider))),
                tuple(sorted(set(args.symbol))),
                args.max_rows,
            )
            plan = preview(engine, raw_store, scope, page_size=args.batch_size)
            # Exclusive creation prevents silently replacing the artifact an operator reviewed.
            with args.output.open("x", encoding="utf-8") as output:
                output.write(dumps(plan, ensure_ascii=False, sort_keys=True, indent=2) + "\n")
            result = summary(plan)
        print(dumps(result, ensure_ascii=False, sort_keys=True))
        if args.apply or args.rollback:
            return (
                2
                if any(repair_items.get(status, 0) for status in ("conflict", "rollback_conflict"))
                else 0
            )
        return 0
    finally:
        engine.dispose()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        # SQL/driver exceptions can contain credentials, paths or Raw values.
        print(
            f"quantity repair stopped ({type(error).__name__}); "
            "inspect the restricted audit ledger before resuming the reviewed plan"
        )
        raise SystemExit(1) from None
