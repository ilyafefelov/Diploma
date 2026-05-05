"""Dagster definitions package for the smart_arbitrage project."""

from dagster import Definitions

from smart_arbitrage.assets.bronze.grid_events import GRID_EVENT_BRONZE_ASSETS
from smart_arbitrage.assets.bronze.market_weather import REAL_DATA_BENCHMARK_BRONZE_ASSETS
from smart_arbitrage.assets.mvp_demo import MVP_DEMO_ASSETS
from smart_arbitrage.assets.gold.forecast_strategy import (
    FORECAST_STRATEGY_GOLD_ASSETS,
    FORECAST_STRATEGY_GOLD_SCHEDULES,
)
from smart_arbitrage.assets.gold.dfl_research import DFL_RESEARCH_GOLD_ASSETS
from smart_arbitrage.assets.gold.simulated_trades import SIMULATED_TRADE_TRAINING_ASSETS
from smart_arbitrage.assets.silver import (
    GRID_EVENT_SILVER_ASSETS,
    NEURAL_FORECAST_SILVER_ASSETS,
    REAL_DATA_BENCHMARK_SILVER_ASSETS,
)
from smart_arbitrage.assets.telemetry import (
    BATTERY_TELEMETRY_ASSETS,
    BATTERY_TELEMETRY_SCHEDULES,
)

defs = Definitions(
    assets=[
        *MVP_DEMO_ASSETS,
        *REAL_DATA_BENCHMARK_BRONZE_ASSETS,
        *GRID_EVENT_BRONZE_ASSETS,
        *GRID_EVENT_SILVER_ASSETS,
        *REAL_DATA_BENCHMARK_SILVER_ASSETS,
        *NEURAL_FORECAST_SILVER_ASSETS,
        *BATTERY_TELEMETRY_ASSETS,
        *FORECAST_STRATEGY_GOLD_ASSETS,
        *DFL_RESEARCH_GOLD_ASSETS,
        *SIMULATED_TRADE_TRAINING_ASSETS,
    ],
    schedules=[*BATTERY_TELEMETRY_SCHEDULES, *FORECAST_STRATEGY_GOLD_SCHEDULES],
)

__all__ = ["defs"]
