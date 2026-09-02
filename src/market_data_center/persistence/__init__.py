"""PostgreSQL persistence adapters."""

from market_data_center.persistence.derived_postgres import PostgreSQLDerivedPersistence
from market_data_center.persistence.dragon_tiger_postgres import (
    PostgreSQLDragonTigerPersistence,
)
from market_data_center.persistence.operations_postgres import PostgreSQLOperationsPersistence
from market_data_center.persistence.postgres import PostgreSQLPersistence
from market_data_center.persistence.regulation_postgres import PostgreSQLRegulationPersistence
from market_data_center.persistence.stock_pool_postgres import PostgreSQLStockPoolPersistence

__all__ = [
    "PostgreSQLDerivedPersistence",
    "PostgreSQLDragonTigerPersistence",
    "PostgreSQLOperationsPersistence",
    "PostgreSQLPersistence",
    "PostgreSQLRegulationPersistence",
    "PostgreSQLStockPoolPersistence",
]
