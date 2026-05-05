"""Silver-layer assets for smart_arbitrage."""

from smart_arbitrage.assets.silver.grid_events import GRID_EVENT_SILVER_ASSETS as GRID_EVENT_SILVER_ASSETS
from smart_arbitrage.assets.silver.neural_forecasts import NEURAL_FORECAST_SILVER_ASSETS as NEURAL_FORECAST_SILVER_ASSETS
from smart_arbitrage.assets.silver.real_data_benchmark import (
    REAL_DATA_BENCHMARK_SILVER_ASSETS as REAL_DATA_BENCHMARK_SILVER_ASSETS,
)
from smart_arbitrage.assets.silver.tenant_load import TENANT_LOAD_SILVER_ASSETS as TENANT_LOAD_SILVER_ASSETS

__all__ = [
    "GRID_EVENT_SILVER_ASSETS",
    "NEURAL_FORECAST_SILVER_ASSETS",
    "REAL_DATA_BENCHMARK_SILVER_ASSETS",
    "TENANT_LOAD_SILVER_ASSETS",
]
