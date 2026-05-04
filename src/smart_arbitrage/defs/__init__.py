"""Dagster definitions package for the smart_arbitrage project."""

from dagster import Definitions

from smart_arbitrage.assets.mvp_demo import MVP_DEMO_ASSETS
from smart_arbitrage.assets.gold.simulated_trades import SIMULATED_TRADE_TRAINING_ASSETS
from smart_arbitrage.assets.silver import NEURAL_FORECAST_SILVER_ASSETS
from smart_arbitrage.assets.telemetry import BATTERY_TELEMETRY_ASSETS, BATTERY_TELEMETRY_SCHEDULES

defs = Definitions(
    assets=[
        *MVP_DEMO_ASSETS,
        *NEURAL_FORECAST_SILVER_ASSETS,
        *BATTERY_TELEMETRY_ASSETS,
        *SIMULATED_TRADE_TRAINING_ASSETS,
    ],
    schedules=[*BATTERY_TELEMETRY_SCHEDULES],
)

__all__ = ["defs"]
