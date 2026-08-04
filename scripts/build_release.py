"""Build a reproducible Windows source-distribution zip.

The archive contains exactly the files ``git ls-files`` reports for the
current commit, so the package always matches a committed snapshot:
``__pycache__``, ``.venv``, ``dist``, ``.env`` and raw data never enter
it. Entry names are written with the UTF-8 flag set so the Chinese docs
filenames survive on every Windows codepage.

Usage::

    uv run python scripts/build_release.py

The output overwrites ``dist/market-data-center-{version}-windows.zip``.
The ``dist/`` directory stays git-ignored; the archive is a build
artifact, not a committed file.
"""

from __future__ import annotations

import subprocess
import sys
import tomllib
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
UTF8_FLAG = 0x800


def _read_version() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["version"]


def _require_clean_tree() -> None:
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    if status:
        sys.exit("Working tree is not clean. Commit or stash changes before building:\n" + status)


def _tracked_files() -> list[str]:
    output = subprocess.check_output(["git", "ls-files"], cwd=ROOT, text=True).splitlines()
    return [line for line in (line.strip() for line in output) if line]


def _add_file(archive: zipfile.ZipFile, relative: str) -> None:
    source = ROOT / relative
    info = zipfile.ZipInfo.from_file(source, arcname=relative)
    # Mark the name as UTF-8 so non-ASCII (Chinese docs) decode everywhere.
    info.flag_bits |= UTF8_FLAG
    info.compress_type = zipfile.ZIP_DEFLATED
    with source.open("rb") as handle:
        archive.writestr(info, handle.read())


def main() -> None:
    _require_clean_tree()
    version = _read_version()
    files = _tracked_files()
    DIST.mkdir(exist_ok=True)
    output = DIST / f"market-data-center-{version}-windows.zip"
    with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
        for relative in files:
            _add_file(archive, relative)
    print(f"Wrote {output.relative_to(ROOT)} ({len(files)} files, version {version}).")


if __name__ == "__main__":
    main()
