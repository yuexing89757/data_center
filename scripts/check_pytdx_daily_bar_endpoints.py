"""Read-only connectivity check for configured remote TDX Daily Bar endpoints."""

from argparse import ArgumentParser

from pytdx.hq import TdxHq_API  # type: ignore[import-untyped]

from market_data_center.providers.pytdx import parse_daily_bar_endpoints
from market_data_center.settings import PytdxDailyBarSettings


def main() -> None:
    parser = ArgumentParser()
    parser.add_argument(
        "--require-all",
        action="store_true",
        help="fail unless every configured endpoint is reachable",
    )
    args = parser.parse_args()
    settings = PytdxDailyBarSettings()
    endpoints = parse_daily_bar_endpoints(settings.pytdx_daily_bar_endpoints)
    reachable = 0
    for index, (host, port) in enumerate(endpoints, start=1):
        api = TdxHq_API(heartbeat=False, auto_retry=False, raise_exception=True)
        try:
            connected = api.connect(
                host,
                port,
                time_out=settings.pytdx_daily_bar_timeout_seconds,
            )
        except Exception as error:
            print(f"endpoint_{index}=failed error_type={type(error).__name__}")
        else:
            if connected:
                reachable += 1
                print(f"endpoint_{index}=reachable")
            else:
                print(f"endpoint_{index}=failed")
        finally:
            api.disconnect()
    print(f"reachable_endpoints={reachable}/{len(endpoints)}")
    if reachable == 0:
        raise SystemExit("no configured TDX endpoint is reachable")
    if args.require_all and reachable != len(endpoints):
        raise SystemExit("one or more configured TDX endpoints are unreachable")


if __name__ == "__main__":
    main()
