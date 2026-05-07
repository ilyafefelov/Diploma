from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.assets.gold.dfl_research import (
    DFL_RESEARCH_GOLD_ASSETS,
    DflTrainingAssetConfig,
    HorizonRegretWeightedForecastCalibrationAssetConfig,
    OfflineDflExperimentAssetConfig,
    RelaxedDflPilotAssetConfig,
    RegretWeightedForecastCalibrationAssetConfig,
    RegretWeightedDflPilotAssetConfig,
    calibrated_value_aware_ensemble_frame,
    dfl_training_frame,
    dfl_relaxed_lp_pilot_frame,
    forecast_dispatch_sensitivity_frame,
    horizon_regret_weighted_forecast_calibration_frame,
    horizon_regret_weighted_forecast_strategy_benchmark_frame,
    offline_dfl_experiment_frame,
    real_data_value_aware_ensemble_frame,
    regret_weighted_forecast_calibration_frame,
    regret_weighted_forecast_strategy_benchmark_frame,
    regret_weighted_dfl_pilot_frame,
    risk_adjusted_value_gate_frame,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.resources.dfl_training_store import InMemoryDflTrainingStore
from smart_arbitrage.resources.strategy_evaluation_store import InMemoryStrategyEvaluationStore


def _benchmark_frame() -> pl.DataFrame:
    first_anchor = datetime(2026, 5, 1, 23)
    rows: list[dict[str, object]] = []
    for anchor_index in range(5):
        anchor = first_anchor + timedelta(days=anchor_index)
        for model_name, regret in [
            ("strict_similar_day", 100.0),
            ("nbeatsx_silver_v0", 150.0),
            ("tft_silver_v0", 120.0),
        ]:
            rows.append(
                {
                    "evaluation_id": f"{anchor_index}:{model_name}",
                    "tenant_id": "client_003_dnipro_factory",
                    "forecast_model_name": model_name,
                    "strategy_kind": "real_data_rolling_origin_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": anchor,
                    "generated_at": datetime(2026, 5, 5),
                    "horizon_hours": 2,
                    "starting_soc_fraction": 0.5,
                    "starting_soc_source": "tenant_default",
                    "decision_value_uah": 1000.0 - regret,
                    "forecast_objective_value_uah": 950.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": regret,
                    "regret_ratio": regret / 1000.0,
                    "total_degradation_penalty_uah": 10.0,
                    "total_throughput_mwh": 0.1,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": 1,
                    "evaluation_payload": {
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "forecast_diagnostics": {
                            "mae_uah_mwh": regret,
                            "rmse_uah_mwh": regret,
                            "smape": 0.1,
                        },
                        "horizon": [
                            {
                                "step_index": 0,
                                "interval_start": (anchor + timedelta(hours=1)).isoformat(),
                                "forecast_price_uah_mwh": 1000.0,
                                "actual_price_uah_mwh": 1100.0,
                                "net_power_mw": 0.0,
                                "degradation_penalty_uah": 0.0,
                            },
                            {
                                "step_index": 1,
                                "interval_start": (anchor + timedelta(hours=2)).isoformat(),
                                "forecast_price_uah_mwh": 1050.0,
                                "actual_price_uah_mwh": 1150.0,
                                "net_power_mw": 0.0,
                                "degradation_penalty_uah": 0.0,
                            },
                        ],
                    },
                }
            )
    return pl.DataFrame(rows)


def test_dfl_research_assets_are_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in DFL_RESEARCH_GOLD_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}

    assert {
        "real_data_value_aware_ensemble_frame",
        "dfl_training_frame",
        "regret_weighted_dfl_pilot_frame",
        "regret_weighted_forecast_calibration_frame",
        "regret_weighted_forecast_strategy_benchmark_frame",
        "horizon_regret_weighted_forecast_calibration_frame",
        "horizon_regret_weighted_forecast_strategy_benchmark_frame",
        "calibrated_value_aware_ensemble_frame",
        "forecast_dispatch_sensitivity_frame",
        "risk_adjusted_value_gate_frame",
        "dfl_relaxed_lp_pilot_frame",
        "offline_dfl_experiment_frame",
    }.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
    tags_by_key = {
        asset_key.to_user_string(): tags
        for asset in DFL_RESEARCH_GOLD_ASSETS
        for asset_key, tags in asset.tags_by_key.items()
    }
    groups_by_key = {
        asset_key.to_user_string(): group
        for asset in DFL_RESEARCH_GOLD_ASSETS
        for asset_key, group in asset.group_names_by_key.items()
    }
    assert tags_by_key["dfl_relaxed_lp_pilot_frame"]["medallion"] == "gold"
    assert tags_by_key["offline_dfl_experiment_frame"]["evidence_scope"] == "not_market_execution"
    assert groups_by_key["dfl_training_frame"] == "gold_dfl_training"
    assert groups_by_key["offline_dfl_experiment_frame"] == "gold_dfl_training"
    assert groups_by_key["regret_weighted_forecast_calibration_frame"] == "gold_calibration"
    assert groups_by_key["horizon_regret_weighted_forecast_calibration_frame"] == "gold_calibration"
    assert groups_by_key["regret_weighted_forecast_strategy_benchmark_frame"] == "gold_calibration"
    assert groups_by_key["horizon_regret_weighted_forecast_strategy_benchmark_frame"] == "gold_calibration"
    assert groups_by_key["calibrated_value_aware_ensemble_frame"] == "gold_selector_diagnostics"
    assert groups_by_key["forecast_dispatch_sensitivity_frame"] == "gold_selector_diagnostics"
    assert groups_by_key["risk_adjusted_value_gate_frame"] == "gold_selector_diagnostics"


def test_dfl_research_assets_persist_ensemble_training_and_pilot(monkeypatch) -> None:
    strategy_store = InMemoryStrategyEvaluationStore()
    dfl_store = InMemoryDflTrainingStore()
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.get_strategy_evaluation_store",
        lambda: strategy_store,
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.get_dfl_training_store",
        lambda: dfl_store,
    )
    benchmark = _benchmark_frame()

    ensemble = real_data_value_aware_ensemble_frame(None, benchmark)
    training = dfl_training_frame(
        None,
        DflTrainingAssetConfig(),
        benchmark,
        ensemble,
    )
    pilot = regret_weighted_dfl_pilot_frame(
        None,
        RegretWeightedDflPilotAssetConfig(
            tenant_id="client_003_dnipro_factory",
            forecast_model_name="tft_silver_v0",
        ),
        training,
    )
    calibration = regret_weighted_forecast_calibration_frame(
        None,
        RegretWeightedForecastCalibrationAssetConfig(
            min_prior_anchors=1,
            rolling_calibration_window_anchors=3,
        ),
        training,
    )
    calibrated_benchmark = regret_weighted_forecast_strategy_benchmark_frame(
        None,
        benchmark,
        calibration,
    )
    horizon_calibration = horizon_regret_weighted_forecast_calibration_frame(
        None,
        HorizonRegretWeightedForecastCalibrationAssetConfig(
            min_prior_anchors=1,
            rolling_calibration_window_anchors=3,
        ),
        benchmark,
    )
    horizon_calibrated_benchmark = horizon_regret_weighted_forecast_strategy_benchmark_frame(
        None,
        benchmark,
        horizon_calibration,
    )
    calibrated_ensemble = calibrated_value_aware_ensemble_frame(
        None,
        horizon_calibrated_benchmark,
    )
    sensitivity = forecast_dispatch_sensitivity_frame(
        None,
        horizon_calibrated_benchmark,
    )
    risk_gate = risk_adjusted_value_gate_frame(
        None,
        horizon_calibrated_benchmark,
    )
    relaxed_pilot = dfl_relaxed_lp_pilot_frame(None, RelaxedDflPilotAssetConfig(max_examples=4), benchmark)
    offline_experiment = offline_dfl_experiment_frame(
        None,
        OfflineDflExperimentAssetConfig(
            forecast_model_names_csv="tft_silver_v0",
            validation_fraction=0.4,
            max_train_anchors=3,
            max_validation_anchors=2,
            epoch_count=2,
        ),
        benchmark,
    )

    assert ensemble.height == 5
    assert strategy_store.evaluation_frame.height == 65
    assert training.height == 20
    assert dfl_store.training_frame.height == 20
    assert pilot.height == 1
    assert dfl_store.pilot_frame.height == 1
    assert calibration.height == 10
    assert calibrated_benchmark.height == 25
    assert horizon_calibration.height == 10
    assert horizon_calibrated_benchmark.height == 25
    assert set(calibrated_benchmark["forecast_model_name"].unique().to_list()) == {
        "strict_similar_day",
        "nbeatsx_silver_v0",
        "tft_silver_v0",
        "nbeatsx_regret_weighted_calibrated_v0",
        "tft_regret_weighted_calibrated_v0",
    }
    assert {
        "nbeatsx_horizon_regret_weighted_calibrated_v0",
        "tft_horizon_regret_weighted_calibrated_v0",
    }.issubset(set(horizon_calibrated_benchmark["forecast_model_name"].unique().to_list()))
    assert calibrated_ensemble.height == 5
    assert set(calibrated_ensemble["forecast_model_name"].unique().to_list()) == {
        "calibrated_value_aware_ensemble_v0"
    }
    assert sensitivity.height == 25
    assert "diagnostic_bucket" in sensitivity.columns
    assert risk_gate.height == 5
    assert set(risk_gate["forecast_model_name"].unique().to_list()) == {
        "risk_adjusted_value_gate_v0"
    }
    assert relaxed_pilot.height > 0
    assert dfl_store.relaxed_pilot_frame.height == relaxed_pilot.height
    assert relaxed_pilot.select("academic_scope").to_series().unique().to_list() == [
        "differentiable_relaxed_lp_pilot_not_final_dfl"
    ]
    assert offline_experiment.height == 1
    assert offline_experiment.select("claim_scope").to_series().unique().to_list() == [
        "offline_dfl_experiment_not_full_dfl"
    ]
    assert offline_experiment.select("not_market_execution").to_series().unique().to_list() == [True]
