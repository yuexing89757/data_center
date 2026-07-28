"""Backup, restore, and compare Market Data Center application data."""

from argparse import ArgumentParser
from dataclasses import asdict
from json import dumps
from os import environ
from pathlib import Path

from market_data_center.recovery import (
    backup_application_data,
    capture_database_snapshot,
    restore_application_data,
    verify_restored_snapshot,
)


def main() -> None:
    parser = _parser()
    args = parser.parse_args()
    if args.command == "backup":
        source_url = _required_environment("SOURCE_DATABASE_URL")
        digest = backup_application_data(source_url, args.file)
        print(f"backup_file={args.file}")
        print(f"backup_sha256={digest}")
        return
    if args.command == "restore":
        target_url = _required_environment("TARGET_DATABASE_URL")
        restore_application_data(target_url, args.file)
        print(f"restored_file={args.file}")
        return
    if args.command == "snapshot":
        source_url = _required_environment("SOURCE_DATABASE_URL")
        print(dumps(asdict(capture_database_snapshot(source_url)), ensure_ascii=False))
        return
    source_url = _required_environment("SOURCE_DATABASE_URL")
    target_url = _required_environment("TARGET_DATABASE_URL")
    source = capture_database_snapshot(source_url)
    restored = capture_database_snapshot(target_url)
    verify_restored_snapshot(source, restored)
    print(dumps(asdict(restored), ensure_ascii=False))


def _parser() -> ArgumentParser:
    parser = ArgumentParser(prog="backup-restore")
    subparsers = parser.add_subparsers(dest="command", required=True)
    backup = subparsers.add_parser("backup")
    backup.add_argument("--file", type=Path, required=True)
    restore = subparsers.add_parser("restore")
    restore.add_argument("--file", type=Path, required=True)
    subparsers.add_parser("snapshot")
    subparsers.add_parser("verify")
    return parser


def _required_environment(name: str) -> str:
    value = environ.get(name)
    if not value:
        raise SystemExit(f"{name} is required")
    return value


if __name__ == "__main__":
    main()
