"""Build a source release from the current committed snapshot.

The archive contains exactly the files ``git ls-files`` reports for the
current commit, so the package always matches a committed snapshot:
``__pycache__``, ``.venv``, ``dist``, ``.env`` and raw data never enter
it. Entry names are written with the UTF-8 flag set so the Chinese docs
filenames survive on every Windows codepage.

Usage::

    uv run python scripts/build_release.py --platform linux

Pass ``--allow-dirty`` only for local experimentation; official builds
must run against a clean tree so the package matches a real commit.

The output overwrites the platform-specific archive in ``dist/``. That
directory stays git-ignored; archives are build artifacts, not committed files.
"""

from __future__ import annotations

import argparse
import hashlib
import subprocess
import sys
import tarfile
import tomllib
import zipfile
from io import BytesIO
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DIST = ROOT / "dist"
UTF8_FLAG = 0x800


def _read_version() -> str:
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    return data["project"]["version"]


def _require_clean_tree(*, allow_dirty: bool) -> None:
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=ROOT, text=True).strip()
    if status and not allow_dirty:
        sys.exit("Working tree is not clean. Commit or stash changes before building:\n" + status)


def _tracked_files() -> list[str]:
    # Use -z so git emits raw UTF-8 bytes separated by NUL, bypassing
    # core.quotePath escaping of non-ASCII filenames on Windows.
    output = subprocess.check_output(["git", "ls-files", "-z"], cwd=ROOT)
    return [entry.decode("utf-8") for entry in output.split(b"\x00") if entry]


def _add_file(archive: zipfile.ZipFile, relative: str) -> None:
    source = ROOT / relative
    # Force forward slashes; the zip spec uses them, and Windows backslashes
    # would create single-name entries instead of nested paths on unzip.
    info = zipfile.ZipInfo.from_file(source, arcname=relative.replace("\\", "/"))
    # Mark the name as UTF-8 so non-ASCII (Chinese docs) decode everywhere.
    info.flag_bits |= UTF8_FLAG
    info.compress_type = zipfile.ZIP_DEFLATED
    with source.open("rb") as handle:
        archive.writestr(info, handle.read())


def _build_linux_tar(version: str, files: list[str]) -> Path:
    output = DIST / f"market-data-center-{version}-linux.tar.gz"
    prefix = f"market-data-center-{version}"
    with tarfile.open(output, "w:gz") as archive:
        for relative in files:
            source = ROOT / relative
            archive_name = f"{prefix}/{relative}"
            if source.suffix == ".sh":
                content = source.read_bytes().replace(b"\r\n", b"\n")
                info = archive.gettarinfo(str(source), arcname=archive_name)
                info.size = len(content)
                archive.addfile(info, BytesIO(content))
            else:
                archive.add(source, arcname=archive_name, recursive=False)
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--platform", choices=("linux", "windows"), default="windows")
    parser.add_argument("--allow-dirty", action="store_true")
    args = parser.parse_args()
    allow_dirty = args.allow_dirty
    _require_clean_tree(allow_dirty=allow_dirty)
    version = _read_version()
    files = _tracked_files()
    DIST.mkdir(exist_ok=True)
    if args.platform == "linux":
        output = _build_linux_tar(version, files)
    else:
        output = DIST / f"market-data-center-{version}-windows.zip"
        with zipfile.ZipFile(output, "w", zipfile.ZIP_DEFLATED) as archive:
            for relative in files:
                _add_file(archive, relative)
    digest = hashlib.sha256(output.read_bytes()).hexdigest()
    checksum = output.with_name(output.name + ".sha256")
    checksum.write_text(f"{digest}  {output.name}\n", encoding="ascii")
    print(
        f"Wrote {output.relative_to(ROOT)} and {checksum.relative_to(ROOT)} "
        f"({len(files)} files, version {version})."
    )


if __name__ == "__main__":
    main()
