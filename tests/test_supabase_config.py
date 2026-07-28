from pathlib import Path
from tomllib import loads


def test_postgrest_exposes_only_versioned_api_schema() -> None:
    config_path = Path(__file__).resolve().parents[1] / "supabase" / "config.toml"
    config = loads(config_path.read_text(encoding="utf-8"))

    assert config["api"]["schemas"] == ["api_v1"]
    assert "core" not in config["api"]["extra_search_path"]
    assert "ingestion" not in config["api"]["extra_search_path"]
    assert "audit" not in config["api"]["extra_search_path"]
