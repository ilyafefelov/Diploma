"""Bronze ingestion assets for smart_arbitrage."""

from smart_arbitrage.assets.bronze.market_weather import BRONZE_INGESTION_ASSETS
from smart_arbitrage.assets.bronze.poland_neighbor_snapshot import (
    POLAND_NEIGHBOR_MARKET_SNAPSHOT_BRONZE_ASSETS,
)
from smart_arbitrage.assets.bronze.tenant_load import TENANT_LOAD_BRONZE_ASSETS

__all__ = [
    "BRONZE_INGESTION_ASSETS",
    "POLAND_NEIGHBOR_MARKET_SNAPSHOT_BRONZE_ASSETS",
    "TENANT_LOAD_BRONZE_ASSETS",
]
