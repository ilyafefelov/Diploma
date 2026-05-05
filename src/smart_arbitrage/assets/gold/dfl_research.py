from typing import Any

import dagster as dg
import polars as pl

from smart_arbitrage.dfl.regret_weighted import run_regret_weighted_dfl_pilot
from smart_arbitrage.resources.dfl_training_store import get_dfl_training_store
from smart_arbitrage.resources.strategy_evaluation_store import get_strategy_evaluation_store
from smart_arbitrage.strategy.ensemble_gate import build_value_aware_ensemble_frame
from smart_arbitrage.training.dfl_training import build_dfl_training_frame


class DflTrainingAssetConfig(dg.Config):
    """DFL training-table behavior for thesis-grade benchmark rows."""

    require_thesis_grade: bool = True


class RegretWeightedDflPilotAssetConfig(dg.Config):
    """Small regret-weighted DFL pilot scope."""

    tenant_id: str = "client_003_dnipro_factory"
    forecast_model_name: str = "tft_silver_v0"
    validation_fraction: float = 0.2


@dg.asset(group_name="gold")
def real_data_value_aware_ensemble_frame(
    context,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Gold value-aware ensemble gate using prior-anchor validation regret only."""

    ensemble_frame = build_value_aware_ensemble_frame(real_data_rolling_origin_benchmark_frame)
    get_strategy_evaluation_store().upsert_evaluation_frame(ensemble_frame)
    _add_metadata(
        context,
        {
            "rows": ensemble_frame.height,
            "tenant_count": ensemble_frame.select("tenant_id").n_unique() if ensemble_frame.height else 0,
            "anchor_count": ensemble_frame.select("anchor_timestamp").n_unique() if ensemble_frame.height else 0,
            "strategy_kind": "value_aware_ensemble_gate",
            "selection_policy": "prior_anchor_validation_regret_only",
        },
    )
    return ensemble_frame


@dg.asset(group_name="gold")
def dfl_training_frame(
    context,
    config: DflTrainingAssetConfig,
    real_data_rolling_origin_benchmark_frame: pl.DataFrame,
    real_data_value_aware_ensemble_frame: pl.DataFrame,
) -> pl.DataFrame:
    """DFL-ready supervised examples from benchmark and ensemble rows."""

    source_frame = pl.concat(
        [real_data_rolling_origin_benchmark_frame, real_data_value_aware_ensemble_frame],
        how="diagonal_relaxed",
    )
    training_frame = build_dfl_training_frame(
        source_frame,
        require_thesis_grade=config.require_thesis_grade,
    )
    get_dfl_training_store().upsert_training_frame(training_frame)
    _add_metadata(
        context,
        {
            "rows": training_frame.height,
            "tenant_count": training_frame.select("tenant_id").n_unique() if training_frame.height else 0,
            "model_count": training_frame.select("forecast_model_name").n_unique() if training_frame.height else 0,
            "scope": "dfl_ready_training_examples_not_full_dfl",
        },
    )
    return training_frame


@dg.asset(group_name="gold")
def regret_weighted_dfl_pilot_frame(
    context,
    config: RegretWeightedDflPilotAssetConfig,
    dfl_training_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Small regret-weighted forecast-calibration pilot for one tenant/model."""

    pilot_frame = run_regret_weighted_dfl_pilot(
        dfl_training_frame,
        tenant_id=config.tenant_id,
        forecast_model_name=config.forecast_model_name,
        validation_fraction=config.validation_fraction,
    )
    get_dfl_training_store().upsert_pilot_frame(pilot_frame)
    _add_metadata(
        context,
        {
            "rows": pilot_frame.height,
            "tenant_id": config.tenant_id,
            "forecast_model_name": config.forecast_model_name,
            "scope": "pilot_not_full_dfl",
        },
    )
    return pilot_frame


DFL_RESEARCH_GOLD_ASSETS = [
    real_data_value_aware_ensemble_frame,
    dfl_training_frame,
    regret_weighted_dfl_pilot_frame,
]


def _add_metadata(context: dg.AssetExecutionContext | None, metadata: dict[str, Any]) -> None:
    if context is not None:
        context.add_output_metadata(metadata)
