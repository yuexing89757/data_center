"""Dump the Kaipanla "What to trade today/tomorrow" list from a local MuMu emulator.

This is a manual, on-demand UI extraction tool. It reads the published topic
ranking through Android UI Automation (``uiautomator dump``) — the upstream
HTTP endpoint requires a logged-in account token and applies certificate
pinning, so it cannot be integrated as a credential-free read-only provider
(ADR-0059 style). The script sends no account credentials, writes no database
rows, saves no Raw objects, and registers no schedule.

Requirements:
- MuMu emulator running locally with developer mode adb enabled
  (default ``127.0.0.1:16384``; override with ``--adb``/``--serial``);
- Kaipanla app ``com.aiyu.kaipanla`` installed.

Usage::

    uv run python scripts/kaipanla_hottalk_dump.py
    uv run python scripts/kaipanla_hottalk_dump.py --pages 6 --out hottalk.json

The app shows "明天炒什么" after market close and "今天炒什么" intraday; both
are the same ``HotTalkListActivity`` ranking. Output is a JSON document with
``observed_at`` (Asia/Shanghai), the screen title, and ``items`` grouped by
trade date, each item carrying rank, read count text, and title. Reading
counts are kept as the displayed string (for example ``14.41万``); nothing is
converted or fabricated.
"""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

PACKAGE = "com.aiyu.kaipanla"
ACTIVITY = f"{PACKAGE}/.newindex.topic.activity.HotTalkListActivity"
DEFAULT_ADB = r"D:\Program Files\Netease\MuMu\nx_main\adb.exe"
DEFAULT_SERIAL = "127.0.0.1:16384"
LOAD_WAIT_SECONDS = 12.0
PAGE_SETTLE_SECONDS = 2.0
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
RANK_RE = re.compile(r"^(\d{1,2})\.$")
VIEWS_RE = re.compile(r"^([\d.]+)万$")

Shanghai = ZoneInfo("Asia/Shanghai")


class DumpError(RuntimeError):
    """The emulator or the app did not produce a usable ranking."""


def run_adb(adb: str, serial: str, *args: str, timeout: float = 30.0) -> str:
    command = [adb, "-s", serial, *args]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )
    if result.returncode != 0:
        raise DumpError(f"adb failed: {' '.join(args)}\n{result.stderr.strip()}")
    return result.stdout


def dump_ui(adb: str, serial: str, attempts: int = 3) -> ET.Element:
    """Dump the current window hierarchy.

    On this emulator ``uiautomator dump`` may crash (SIGSEGV) after the file
    has already been written, so a non-zero exit code is not fatal as long as
    ``cat`` returns a valid hierarchy.
    """
    last_error = ""
    for attempt in range(attempts):
        run_adb(adb, serial, "shell", "rm", "-f", "/sdcard/kpl_dump_tool.xml")
        subprocess.run(
            [adb, "-s", serial, "shell", "uiautomator", "dump", "/sdcard/kpl_dump_tool.xml"],
            capture_output=True,
            text=True,
            timeout=30,
        )
        try:
            xml = run_adb(adb, serial, "shell", "cat", "/sdcard/kpl_dump_tool.xml")
            if "<hierarchy" in xml:
                return ET.fromstring(xml)
            last_error = "uiautomator dump returned no hierarchy"
        except DumpError as error:
            last_error = str(error)
        time.sleep(1.5 + attempt)
    raise DumpError(f"uiautomator dump failed after {attempts} attempts: {last_error}")


def visible_texts(root: ET.Element) -> list[str]:
    texts: list[str] = []
    for node in root.iter("node"):
        text = (node.get("text") or "").strip()
        if text:
            texts.append(text)
    return texts


ID_DATE = f"{PACKAGE}:id/tvDate"
ID_RANK = f"{PACKAGE}:id/tvNum"
ID_READS = f"{PACKAGE}:id/tvHotNum"
ID_TITLE = f"{PACKAGE}:id/tvHeadContent"


def parse_ranking(root: ET.Element) -> list[dict[str, str]]:
    """Read ranking rows by stable view ids; fall back to text heuristics."""
    rows: list[dict[str, str]] = []
    current_date = ""
    pending: dict[str, str] = {}
    for node in root.iter("node"):
        text = (node.get("text") or "").strip()
        resource_id = node.get("resource-id") or ""
        if not text:
            continue
        if resource_id == ID_DATE and DATE_RE.match(text):
            current_date = text
        elif resource_id == ID_RANK:
            pending["rank"] = text.rstrip(".")
        elif resource_id == ID_READS:
            pending["reads"] = text
        elif resource_id == ID_TITLE:
            pending["title"] = text
        if {"rank", "reads", "title"} <= pending.keys():
            rows.append(
                {
                    "trade_date": current_date,
                    "rank": pending["rank"],
                    "reads": pending["reads"],
                    "title": pending["title"],
                }
            )
            pending = {}
    if rows:
        return rows
    return parse_ranking_by_text(root)


def parse_ranking_by_text(root: ET.Element) -> list[dict[str, str]]:
    """Legacy fallback: group texts by rank/views/title ordering."""
    rows: list[dict[str, str]] = []
    current_date = ""
    pending_rank = ""
    pending_views = ""
    for node in root.iter("node"):
        text = (node.get("text") or "").strip()
        if not text:
            continue
        if DATE_RE.match(text):
            current_date = text
        elif not pending_rank and (match := RANK_RE.match(text)):
            pending_rank = match.group(1)
        elif not pending_views and VIEWS_RE.match(text):
            pending_views = text
        elif pending_rank and pending_views and len(text) >= 6:
            rows.append(
                {
                    "trade_date": current_date,
                    "rank": pending_rank,
                    "reads": pending_views,
                    "title": text,
                }
            )
            pending_rank = ""
            pending_views = ""
    return rows


def merge_rows(rounds: list[list[dict[str, str]]]) -> list[dict[str, str]]:
    """Keep one row per title; rows observed with a visible date header win."""
    best: dict[str, dict[str, str]] = {}
    for rows in rounds:
        for row in rows:
            key = row["title"].strip()
            current = best.get(key)
            if current is None:
                best[key] = dict(row)
                continue
            if row["trade_date"] and not current["trade_date"]:
                # A dated observation replaces a boundary partial row entirely.
                best[key] = dict(row)

    def sort_key(row: dict[str, str]) -> tuple[str, int]:
        try:
            rank = int(row["rank"]) if row["rank"] else 99
        except ValueError:
            rank = 99
        return (row["trade_date"] or "0000-00-00", rank)

    return [best[key] for key in sorted(best, key=lambda k: sort_key(best[k]))]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--adb", default=DEFAULT_ADB, help="path to adb.exe")
    parser.add_argument("--serial", default=DEFAULT_SERIAL, help="device serial")
    parser.add_argument("--pages", type=int, default=8, help="scroll steps (1-30)")
    parser.add_argument("--out", type=Path, default=None, help="write JSON here")
    args = parser.parse_args()
    if not 1 <= args.pages <= 30:
        parser.error("--pages must be between 1 and 30")

    adb = args.adb
    serial = args.serial

    def list_devices() -> list[str]:
        result = subprocess.run(
            [adb, "devices"],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=20,
        )
        return result.stdout.splitlines()

    connected = any(
        line.startswith(serial) and line.strip().endswith("device") for line in list_devices()
    )
    if not connected:
        subprocess.run([adb, "connect", serial], capture_output=True, text=True, timeout=20)
        connected = any(
            line.startswith(serial) and line.strip().endswith("device") for line in list_devices()
        )
    if not connected:
        raise DumpError(f"device not connected: {serial}")

    # HotTalkListActivity is not exported; MuMu's adbd allows root, which can
    # start it directly. Without root the script falls back to a launch via
    # the app's main entry and fails fast with a clear message.
    id_out = run_adb(adb, serial, "shell", "id").strip()
    if "uid=0" not in id_out:
        subprocess.run([adb, "-s", serial, "root"], capture_output=True, text=True, timeout=20)
        time.sleep(3)
        subprocess.run([adb, "connect", serial], capture_output=True, text=True, timeout=20)
        id_out = run_adb(adb, serial, "shell", "id").strip()
    if "uid=0" not in id_out:
        raise DumpError("adbd is not root; enable ADB root in MuMu settings and retry")

    packages = run_adb(adb, serial, "shell", "pm", "list", "packages")
    if PACKAGE not in packages:
        raise DumpError(f"{PACKAGE} is not installed on {serial}")

    run_adb(adb, serial, "shell", "am", "force-stop", PACKAGE)
    time.sleep(2)
    run_adb(adb, serial, "shell", "am", "start", "-n", ACTIVITY)
    time.sleep(LOAD_WAIT_SECONDS)

    root = dump_ui(adb, serial)
    texts = visible_texts(root)
    if any("出错" in text for text in texts):
        raise DumpError("app page reported a load error; open the page manually once, then retry")
    title = next((text for text in texts if "炒什么" in text), "")

    rounds = [parse_ranking(root)]
    for _ in range(args.pages):
        run_adb(
            adb,
            serial,
            "shell",
            "input",
            "swipe",
            "540",
            "1400",
            "540",
            "700",
            "400",
        )
        time.sleep(PAGE_SETTLE_SECONDS)
        rounds.append(parse_ranking(dump_ui(adb, serial)))

    items = merge_rows(rounds)
    payload = {
        "source": "kaipanla_app_ui",
        "activity": "HotTalkListActivity",
        "title": title,
        "observed_at": datetime.now(Shanghai).strftime("%Y-%m-%d %H:%M:%S"),
        "pages": args.pages,
        "item_count": len(items),
        "items": items,
    }
    rendered = json.dumps(payload, ensure_ascii=False, indent=2)
    if args.out is not None:
        args.out.write_text(rendered + "\n", encoding="utf-8")
        print(f"wrote {len(items)} items -> {args.out}")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except DumpError as error:
        print(f"error: {error}", file=sys.stderr)
        sys.exit(1)
