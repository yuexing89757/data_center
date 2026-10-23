from runpy import run_path
from types import SimpleNamespace
from unittest.mock import Mock

from pydantic import SecretStr


def test_default_preview_uses_installed_postgresql_driver_and_never_applies(tmp_path, monkeypatch):
    module = run_path("scripts/repair_stock_quantity_units.py")
    main = module["main"]
    engine_factory = Mock()
    settings = SimpleNamespace(
        database_url=SecretStr("postgresql://synthetic@localhost/test"), raw_data_root=tmp_path
    )
    monkeypatch.setitem(main.__globals__, "WorkerSettings", lambda: settings)
    monkeypatch.setitem(main.__globals__, "create_engine", engine_factory)
    monkeypatch.setitem(main.__globals__, "preview", Mock(return_value={"plan_hash": "a" * 64}))
    monkeypatch.setitem(main.__globals__, "summary", Mock(return_value={"quarantined": 1}))
    apply = Mock(side_effect=AssertionError("default preview must not write facts"))
    monkeypatch.setitem(main.__globals__, "apply_plan", apply)
    monkeypatch.setattr(
        "sys.argv",
        [
            "repair",
            "--start",
            "2026-10-21",
            "--end",
            "2026-10-21",
            "--provider",
            "pytdx",
            "--output",
            str(tmp_path / "plan.json"),
        ],
    )
    assert main() == 0
    assert engine_factory.call_args.args[0] == "postgresql+psycopg://synthetic@localhost/test"
    assert engine_factory.call_args.kwargs["hide_parameters"] is True
    apply.assert_not_called()
    engine_factory.return_value.dispose.assert_called_once()
