"""Dagster definitions package for the smart_arbitrage project."""

from dagster import Definitions

from smart_arbitrage.assets.mvp_demo import MVP_DEMO_ASSETS
from smart_arbitrage.assets.silver import NEURAL_FORECAST_SILVER_ASSETS

defs = Definitions(assets=[*MVP_DEMO_ASSETS, *NEURAL_FORECAST_SILVER_ASSETS])

__all__ = ["defs"]
