"""Decision-focused learning pilot utilities.

Keep this package import lightweight. Some downstream contract validators are
used by CLI smoke checks and should not import torch/NBEATSx just because they
live under ``smart_arbitrage.dfl``.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "HORIZON_REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND",
    "REGRET_WEIGHTED_CALIBRATION_STRATEGY_KIND",
    "build_horizon_regret_weighted_forecast_calibration_frame",
    "build_horizon_regret_weighted_forecast_strategy_benchmark_frame",
    "build_regret_weighted_forecast_calibration_frame",
    "build_regret_weighted_forecast_strategy_benchmark_frame",
    "run_regret_weighted_dfl_pilot",
]

_REGRET_WEIGHTED_EXPORTS = frozenset(__all__)


def __getattr__(name: str) -> Any:
    if name in _REGRET_WEIGHTED_EXPORTS:
        from smart_arbitrage.dfl import regret_weighted

        return getattr(regret_weighted, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
