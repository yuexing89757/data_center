from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from tarfile import open as open_tar

SCRIPT = Path(__file__).parents[1] / "scripts" / "build_release.py"
SPEC = spec_from_file_location("build_release", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
build_release = module_from_spec(SPEC)
SPEC.loader.exec_module(build_release)


def test_linux_release_normalizes_shell_scripts_to_lf(
    tmp_path: Path,
    monkeypatch,
) -> None:
    root = tmp_path / "root"
    dist = tmp_path / "dist"
    script = root / "deploy" / "linux" / "smoke-check.sh"
    script.parent.mkdir(parents=True)
    script.write_bytes(b"#!/usr/bin/env sh\r\nset -eu\r\necho ok\r\n")
    monkeypatch.setattr(build_release, "ROOT", root)
    monkeypatch.setattr(build_release, "DIST", dist)
    dist.mkdir()

    archive = build_release._build_linux_tar("test", ["deploy/linux/smoke-check.sh"])

    with open_tar(archive, "r:gz") as release:
        extracted = release.extractfile("market-data-center-test/deploy/linux/smoke-check.sh")
        assert extracted is not None
        assert extracted.read() == b"#!/usr/bin/env sh\nset -eu\necho ok\n"
