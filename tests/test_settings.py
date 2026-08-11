from pathlib import Path

import pytest
from pydantic import ValidationError

from market_data_center.settings import PytdxPoolSettings


def test_pytdx_pool_settings_have_safe_defaults() -> None:
    settings = PytdxPoolSettings(_env_file=None)

    assert settings.pytdx_pool_path == Path("data/pytdx_pool.json")
    assert settings.pytdx_pool_refresh_hours == 12


@pytest.mark.parametrize("refresh_hours", [0, 169])
def test_pytdx_pool_refresh_interval_is_bounded(refresh_hours: int) -> None:
    with pytest.raises(ValidationError):
        PytdxPoolSettings(pytdx_pool_refresh_hours=refresh_hours, _env_file=None)
