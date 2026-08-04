"""External read-only FastAPI service."""

from market_data_center.public_api.app import create_app, run

__all__ = ["create_app", "run"]
