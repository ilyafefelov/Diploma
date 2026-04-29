"""Dagster definitions entrypoint for the smart_arbitrage package."""

from dagster import Definitions

from smart_arbitrage.assets.mvp_demo import MVP_DEMO_ASSETS

defs = Definitions(assets=MVP_DEMO_ASSETS)