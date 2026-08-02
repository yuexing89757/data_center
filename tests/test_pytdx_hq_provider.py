from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from types import TracebackType

from market_data_center.providers.pytdx_hq import PytdxHqProvider
from market_data_center.settings import PytdxHqSettings


class RecordedClient:
    def __enter__(self) -> "RecordedClient":
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        return None

    def fetch(self, requests: Sequence[tuple[int, str]]) -> Sequence[Mapping[str, object]]:
        market, code = requests[0]
        row: dict[str, object] = {
            "market": market,
            "code": code,
            "price": Decimal("10.00"),
            "last_close": Decimal("9.50"),
            "open": Decimal("10.00"),
            "high": Decimal("10.00"),
            "low": Decimal("10.00"),
            "server_time_raw": "91500",
            "volume_lots": 123,
            "current_volume_lots": 1,
            "amount": Decimal("123000"),
            "sell_volume_lots": 2,
            "buy_volume_lots": 3,
        }
        for level in range(1, 6):
            row[f"bid{level}"] = Decimal("10.00") - Decimal(level - 1) / 100
            row[f"ask{level}"] = Decimal("10.01") + Decimal(level - 1) / 100
            row[f"bid_vol{level}"] = level
            row[f"ask_vol{level}"] = level + 5
        return [row]


def test_pytdx_hq_contract_keeps_decimal_and_converts_lots_to_shares() -> None:
    observed = datetime(2026, 8, 3, 1, 15, tzinfo=UTC)
    provider = PytdxHqProvider(
        PytdxHqSettings(pytdx_hq_host="recorded.invalid"),
        client_factory=RecordedClient,
        clock=lambda: observed,
    )

    with provider:
        result = provider.fetch_five_level_quotes(("SSE:600000",))

    quote = result.records[0]
    assert quote.last_price == Decimal("10.00")
    assert quote.cumulative_volume == 12_300
    assert quote.bid_levels[0].volume == 100
    assert quote.source_timestamp is None
    assert result.failed_symbols == ()
