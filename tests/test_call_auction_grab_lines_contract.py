from json import loads
from pathlib import Path
from runpy import run_path
from typing import cast

PROJECT_ROOT = Path(__file__).parents[1]
CONTRACT = PROJECT_ROOT / "contracts" / "fastapi-openapi-v1.json"
FASTAPI_CHECKS = run_path(str(PROJECT_ROOT / "scripts" / "check_fastapi_release.py"))
PUBLISHED_FUNCTIONS = cast(tuple[str, ...], FASTAPI_CHECKS["PUBLISHED_FUNCTIONS"])


def test_checked_in_fastapi_contract_exposes_grab_lines() -> None:
    schema = loads(CONTRACT.read_text(encoding="utf-8"))
    operation = schema["paths"]["/api/v1/call-auction-grab-lines"]["get"]
    parameters = {parameter["name"]: parameter for parameter in operation["parameters"]}
    response = schema["components"]["schemas"]["CallAuctionGrabLineResponse"]
    item = schema["components"]["schemas"]["CallAuctionGrabLineItem"]

    assert parameters["trade_date"]["required"] is True
    assert parameters["trade_date"]["schema"]["format"] == "date"
    assert "n" not in parameters
    assert parameters["min_grab_line_pct"]["required"] is False
    assert parameters["min_grab_line_pct"]["schema"]["default"] == "0"
    assert parameters["max_grab_line_pct"]["required"] is False
    assert parameters["min_change_pct"]["required"] is False
    assert parameters["min_change_pct"]["schema"]["default"] == "0"
    assert parameters["max_change_pct"]["required"] is False
    assert response["properties"]["count"]["maximum"] == 10_000
    assert response["properties"]["first_batch_code"]["const"] == "092453"
    assert item["properties"]["code"]["pattern"] == "^[0-9]{6}$"
    assert item["properties"]["grab_line_pct"]["type"] == "string"
    assert item["properties"]["change_pct_092520"]["type"] == "string"
    assert response["properties"]["min_change_pct"]["type"] == "string"
    assert "09:24:53" in operation["description"]
    assert {"401", "404", "422", "503"}.issubset(operation["responses"])


def test_fastapi_release_preflight_requires_grab_line_rpc() -> None:
    assert (
        "api_v1.query_call_auction_grab_lines(date,numeric,numeric,numeric,numeric)"
        in PUBLISHED_FUNCTIONS
    )
