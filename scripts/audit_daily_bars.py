"""Generate a read-only Daily Bar quality acceptance report."""

from argparse import ArgumentParser
from datetime import date
from os import environ
from pathlib import Path

from market_data_center.quality_audit import (
    audit_daily_bars,
    render_json,
    render_markdown,
)


def main() -> None:
    args = _parser().parse_args()
    database_url = environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")

    report = audit_daily_bars(
        database_url,
        args.start_date,
        args.end_date,
        top_gap_limit=args.top_gaps,
    )
    rendered = render_json(report) if args.format == "json" else render_markdown(report)
    if args.output is None:
        print(rendered, end="" if rendered.endswith("\n") else "\n")
    else:
        _write_new_report(args.output, rendered)
        print(f"report_file={args.output}")

    if report.has_errors:
        raise SystemExit("Daily Bar audit failed")
    if args.fail_on_warning and report.has_warnings:
        raise SystemExit("Daily Bar audit contains warnings")


def _parser() -> ArgumentParser:
    parser = ArgumentParser(prog="audit-daily-bars")
    parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    parser.add_argument("--format", choices=("json", "markdown"), default="markdown")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--top-gaps", type=int, default=20)
    parser.add_argument("--fail-on-warning", action="store_true")
    return parser


def _write_new_report(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8", newline="\n") as report_file:
            report_file.write(content)
    except FileExistsError as error:
        raise SystemExit(f"report already exists: {path}") from error


if __name__ == "__main__":
    main()
