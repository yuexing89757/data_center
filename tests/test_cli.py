from market_data_center.cli import AUTO_PROVIDER_CODE, _parser


def test_cli_uses_automatic_routing_by_default() -> None:
    args = _parser().parse_args(["security"])

    assert args.provider == AUTO_PROVIDER_CODE


def test_cli_still_accepts_an_explicit_provider() -> None:
    args = _parser().parse_args(["--provider", "pytdx", "security"])

    assert args.provider == "pytdx"
