"""Probe pytdx HQ quote servers and persist the reachable ones as a pool.

pytdx ships 100+ candidate HQ hosts, but only a subset is reachable from
a given network at a given time. This script connects to every built-in
host, issues one five-level quote request, and writes the hosts that
answered to ``data/pytdx_hq_pool.json`` so the realtime quote adapter
can pick a healthy endpoint instead of a fixed IP.

Usage::

    uv run python scripts/probe_pytdx_hq_hosts.py

The output is a JSON array of ``{"name", "ip", "port", "latency_ms"}``
sorted by latency (fastest first). The file is a local artifact; it is
not committed (``data/`` is git-ignored).
"""

from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from time import perf_counter

from pytdx.config.hosts import hq_hosts
from pytdx.hq import TdxHq_API

POOL_PATH = Path(__file__).resolve().parents[1] / "data" / "pytdx_hq_pool.json"
CONNECT_TIMEOUT = 4.0
# 0 = Shenzhen, 1 = Shanghai; SSE:600000 is a liquid name every host serves.
PROBE_MARKET = 1
PROBE_CODE = "600000"


def _probe(host: tuple[str, str, int]) -> dict[str, object] | None:
    """Return host metadata if it serves a quote, else None."""
    name, ip, port = host
    api = TdxHq_API()
    start = perf_counter()
    try:
        if not api.connect(ip, port, time_out=CONNECT_TIMEOUT):
            return None
        quotes = api.get_security_quotes([(PROBE_MARKET, PROBE_CODE)])
        api.disconnect()
    except Exception:
        return None
    if not quotes:
        return None
    latency_ms = int((perf_counter() - start) * 1000)
    return {"name": name, "ip": ip, "port": port, "latency_ms": latency_ms}


def main() -> None:
    print(f"Probing {len(hq_hosts)} pytdx HQ hosts (timeout {CONNECT_TIMEOUT}s)...")
    reachable: list[dict[str, object]] = []
    with ThreadPoolExecutor(max_workers=32) as pool:
        futures = {pool.submit(_probe, host): host for host in hq_hosts}
        for future in as_completed(futures):
            result = future.result()
            if result is not None:
                reachable.append(result)
                line = (
                    f"  OK  {result['ip']}:{result['port']}"
                    f"  {result['latency_ms']}ms  {result['name']}"
                )
                print(line)
    reachable.sort(key=lambda item: item["latency_ms"])
    POOL_PATH.parent.mkdir(parents=True, exist_ok=True)
    POOL_PATH.write_text(
        json.dumps(reachable, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    rel = POOL_PATH.relative_to(POOL_PATH.parents[1])
    print(f"\nWrote {len(reachable)}/{len(hq_hosts)} reachable hosts to {rel}")
    if reachable:
        best = reachable[0]
        print(f"Fastest: {best['ip']}:{best['port']} ({best['latency_ms']}ms) — {best['name']}")


if __name__ == "__main__":
    main()
