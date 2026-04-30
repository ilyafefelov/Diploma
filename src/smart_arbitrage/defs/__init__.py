"""Dagster definitions package for the smart_arbitrage project."""

from dagster import Definitions

from smart_arbitrage.assets.mvp_demo import MVP_DEMO_ASSETS

defs = Definitions(assets=MVP_DEMO_ASSETS)

__all__ = ["defs"]