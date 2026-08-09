"""Bounded HTTP smoke check for an already-running loopback FastAPI service."""

from json import loads
from os import environ
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from market_data_center.settings import ApiSettings


def _get(url: str, *, api_key: str | None = None) -> dict[str, object]:
    headers = {"X-API-Key": api_key} if api_key is not None else {}
    with urlopen(Request(url, headers=headers), timeout=10) as response:
        if response.status != 200:
            raise RuntimeError("FastAPI smoke request failed")
        return loads(response.read())


def main() -> None:
    settings = ApiSettings()  # type: ignore[call-arg]
    if settings.fastapi_host != "127.0.0.1":
        raise SystemExit("smoke check only targets the loopback deployment")
    base = f"http://127.0.0.1:{settings.fastapi_port}"
    if _get(base + "/healthz").get("status") != "ok":
        raise SystemExit("health check failed")
    if _get(base + "/readyz").get("status") != "ready":
        raise SystemExit("readiness check failed")
    query = urlencode({"query": "600000", "limit": 1})
    api_key = settings.fastapi_api_key.get_secret_value()
    payload = _get(base + "/api/v1/securities?" + query, api_key=api_key)
    if int(payload.get("count", -1)) not in (0, 1):
        raise SystemExit("bounded security query returned an invalid envelope")
    if environ.get("DATABASE_URL"):
        raise SystemExit("Worker DATABASE_URL must not be present in the API environment")
    print("fastapi_http_smoke=passed")


if __name__ == "__main__":
    main()
