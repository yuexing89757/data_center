from pathlib import Path

from market_data_center import recovery


def test_backup_includes_regulation_and_snapshot_counts(monkeypatch, tmp_path):
    calls = []

    def run_backup(arguments, database_url, operation):
        calls.append(arguments)
        Path(arguments[arguments.index("--file") + 1]).write_bytes(b"dump")

    monkeypatch.setattr(recovery, "_run_postgres_command", run_backup)
    recovery.backup_application_data("unused", tmp_path / "application.dump")
    assert ["--schema", "regulation"] in [
        calls[0][index : index + 2] for index in range(len(calls[0]) - 1)
    ]
    for table in (
        "rule",
        "event",
        "calculation_run",
        "status",
        "rule_result",
        "warning",
        "monitor_context",
        "monitor_input",
        "calculated_event",
        "st_day_snapshot",
    ):
        assert f"select count(*) from regulation.{table}" in recovery.COUNT_QUERIES.values()
