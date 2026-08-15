"""Export the deterministic FastAPI OpenAPI contract without contacting a database."""

from json import dumps
from pathlib import Path
from typing import cast

from pydantic import SecretStr

from market_data_center.public_api import create_app
from market_data_center.public_api.queries import PublicQueryService
from market_data_center.settings import ApiSettings

CONTRACT_PATH = Path(__file__).parents[1] / "contracts" / "fastapi-openapi-v1.json"


def main() -> None:
    settings = ApiSettings(
        fastapi_database_url=SecretStr("unused"),
        fastapi_api_key=SecretStr("contract-api-key-0000000000000000"),
    )
    schema = create_app(
        settings=settings,
        query_service=cast(PublicQueryService, object()),
        auction_indicative_service=cast(object, object()),  # type: ignore[arg-type]
        board_index_bias_live_service=cast(object, object()),  # type: ignore[arg-type]
    ).openapi()
    CONTRACT_PATH.write_text(
        dumps(schema, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
