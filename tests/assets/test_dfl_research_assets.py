from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.assets.gold.dfl_research import (
    DFL_RESEARCH_GOLD_ASSETS,
    DflActionClassifierBaselineAssetConfig,
    DflActionClassifierStrictLpProjectionAssetConfig,
    DflValueAwareActionClassifierStrictLpProjectionAssetConfig,
    DflForecastDflV1AssetConfig,
    DflForecastPipelineTruthAuditAssetConfig,
    DflOfficialGlobalPanelScheduleValueProductionGateAssetConfig,
    DflPolandLag24ExperimentalRollingStrictAssetConfig,
    DflRegretSurrogateV1AssetConfig,
    DflTrainingAssetConfig,
    HorizonRegretWeightedForecastCalibrationAssetConfig,
    OfflineDflActionTargetAssetConfig,
    DflTrajectoryFeatureRankerAssetConfig,
    DflStrictFailureSelectorAssetConfig,
    DflStrictFailureSelectorRobustnessAssetConfig,
    AflTrainingPanelAssetConfig,
    DflStrictChallengerAssetConfig,
    OfflineDflTrajectoryValueSelectorAssetConfig,
    DflActionLabelPanelAssetConfig,
    DflDataCoverageAuditAssetConfig,
    OfflineDflExperimentAssetConfig,
    OfflineDflDecisionTargetAssetConfig,
    OfflineDflPanelExperimentAssetConfig,
    OfflineDflPanelStrictLpBenchmarkAssetConfig,
    RelaxedDflPilotAssetConfig,
    RegretWeightedForecastCalibrationAssetConfig,
    RegretWeightedDflPilotAssetConfig,
    calibrated_value_aware_ensemble_frame,
    dfl_action_classifier_baseline_frame,
    dfl_action_classifier_failure_analysis_frame,
    dfl_action_classifier_strict_lp_benchmark_frame,
    dfl_value_aware_action_classifier_strict_lp_benchmark_frame,
    dfl_forecast_dfl_v1_panel_frame,
    dfl_forecast_dfl_v1_strict_lp_benchmark_frame,
    dfl_training_frame,
    dfl_action_label_panel_frame,
    dfl_data_coverage_audit_frame,
    dfl_relaxed_lp_pilot_frame,
    forecast_dispatch_sensitivity_frame,
    horizon_regret_weighted_forecast_calibration_frame,
    horizon_regret_weighted_forecast_strategy_benchmark_frame,
    offline_dfl_experiment_frame,
    offline_dfl_action_target_panel_frame,
    offline_dfl_action_target_strict_lp_benchmark_frame,
    dfl_trajectory_value_candidate_panel_frame,
    dfl_schedule_candidate_library_frame,
    dfl_trajectory_feature_ranker_frame,
    dfl_trajectory_feature_ranker_strict_lp_benchmark_frame,
    dfl_pipeline_integrity_audit_frame,
    forecast_pipeline_truth_audit_frame,
    dfl_schedule_candidate_library_v2_frame,
    dfl_non_strict_oracle_upper_bound_frame,
    dfl_strict_baseline_autopsy_frame,
    dfl_strict_failure_selector_frame,
    dfl_strict_failure_selector_strict_lp_benchmark_frame,
    dfl_strict_failure_selector_robustness_frame,
    forecast_candidate_forensics_frame,
    afl_training_panel_frame,
    dfl_trajectory_value_selector_frame,
    dfl_trajectory_value_selector_strict_lp_benchmark_frame,
    offline_dfl_decision_target_panel_frame,
    offline_dfl_decision_target_strict_lp_benchmark_frame,
    offline_dfl_panel_experiment_frame,
    offline_dfl_panel_strict_lp_benchmark_frame,
    official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame,
    dfl_training_example_frame,
    real_data_value_aware_ensemble_frame,
    regret_weighted_forecast_calibration_frame,
    regret_weighted_forecast_strategy_benchmark_frame,
    regret_weighted_dfl_pilot_frame,
    risk_adjusted_value_gate_frame,
    _concat_feature_candidate_frames,
    _normalize_asset_metadata,
)
from smart_arbitrage.defs import defs
from smart_arbitrage.dfl.promotion_gate import PromotionGateResult
from smart_arbitrage.resources.dfl_training_store import InMemoryDflTrainingStore
from smart_arbitrage.resources.strategy_evaluation_store import (
    InMemoryStrategyEvaluationStore,
)
from smart_arbitrage.strategy.official_global_panel import (
    POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND,
)


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
                                "interval_start": (
                                    anchor + timedelta(hours=1)
                                ).isoformat(),
                                "forecast_price_uah_mwh": 1000.0,
                                "actual_price_uah_mwh": 1100.0,
                                "net_power_mw": 0.0,
                                "degradation_penalty_uah": 0.0,
                            },
                            {
                                "step_index": 1,
                                "interval_start": (
                                    anchor + timedelta(hours=2)
                                ).isoformat(),
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


def test_feature_candidate_concat_relaxes_null_lagged_columns() -> None:
    primary = pl.DataFrame({"feature_name": ["base"]}).with_columns(
        pl.lit(None).alias("source_delivery_timestamp_utc")
    )
    lagged = pl.DataFrame(
        {
            "feature_name": ["lagged"],
            "source_delivery_timestamp_utc": ["2026-01-01T00:00:00+00:00"],
        }
    )

    merged = _concat_feature_candidate_frames(primary, lagged)

    assert merged["feature_name"].to_list() == ["base", "lagged"]
    assert merged["source_delivery_timestamp_utc"].to_list() == [
        None,
        "2026-01-01T00:00:00+00:00",
    ]


def test_dfl_research_asset_metadata_normalizes_tuple_values() -> None:
    metadata = _normalize_asset_metadata(
        {
            "source_model_names": ("nbeatsx", "tft"),
            "nested": {"candidate_profiles": ("strict", "risk")},
        }
    )

    assert metadata == {
        "source_model_names": ["nbeatsx", "tft"],
        "nested": {"candidate_profiles": ["strict", "risk"]},
    }


def _silver_feature_frame() -> pl.DataFrame:
    first_timestamp = datetime(2026, 4, 25)
    rows: list[dict[str, object]] = []
    for hour_index in range(121):
        rows.append(
            {
                "tenant_id": "client_003_dnipro_factory",
                "timestamp": first_timestamp + timedelta(hours=hour_index),
                "price_uah_mwh": 1000.0 + hour_index,
                "source_kind": "observed",
                "weather_source_kind": "observed",
            }
        )
    return pl.DataFrame(rows)


def test_global_panel_schedule_value_production_gate_uses_full_promotion_thresholds() -> (
    None
):
    config = DflOfficialGlobalPanelScheduleValueProductionGateAssetConfig()

    assert config.min_validation_tenant_anchor_count_per_source_model == 90
    assert config.min_rolling_window_count == 4
    assert config.min_rolling_strict_pass_windows == 3


def test_regret_surrogate_v1_config_keeps_v2_plus_gate_and_shadow_sources() -> None:
    config = DflRegretSurrogateV1AssetConfig()

    assert config.source_model_names_csv == (
        "nbeatsx_official_global_panel_horizon_calibrated_v1"
    )
    assert config.min_oracle_improvement_ratio_vs_v2_plus == 0.05
    assert config.material_switch_delta_uah == 25.0
    assert config.high_v2_regret_uah == 500.0
    assert config.min_context_prior_support_count == 1
    assert config.max_prior_neighbor_distance == 1.5
    assert config.min_neighbor_safe_win_count == 1
    assert config.max_neighbor_tail_risk_probability == 0.25
    assert config.nearest_neighbor_count == 5
    assert "poland_shadow_candidate" in config.allowed_candidate_sources_csv
    assert "v7_generated_candidate" in config.allowed_candidate_sources_csv
    assert config.min_prior_material_safe_switch_examples_for_dt == 20
    assert config.use_cuda_if_available is True


def test_poland_lag24_rolling_asset_merges_persisted_batches(monkeypatch) -> None:
    store = InMemoryStrategyEvaluationStore()
    generated_at = datetime(2026, 5, 21, 12)

    def poland_row(*, evaluation_id: str, anchor_hour: int) -> dict[str, object]:
        return {
            "evaluation_id": evaluation_id,
            "tenant_id": "client_003_dnipro_factory",
            "forecast_model_name": (
                "nbeatsx_official_global_panel_poland_lag24_experimental_v1"
            ),
            "strategy_kind": POLAND_LAG24_EXPERIMENTAL_ROLLING_STRATEGY_KIND,
            "anchor_timestamp": datetime(2026, 4, 2, anchor_hour),
            "generated_at": generated_at,
            "rank_by_regret": 1,
            "regret_uah": 10.0,
        }

    store.upsert_evaluation_frame(
        pl.DataFrame([poland_row(evaluation_id="batch-0", anchor_hour=1)])
    )

    def fake_builder(
        real_data_benchmark_silver_feature_frame: pl.DataFrame,
        **kwargs: object,
    ) -> pl.DataFrame:
        assert real_data_benchmark_silver_feature_frame.height == 1
        assert kwargs["tenant_ids"] == ("client_003_dnipro_factory",)
        assert kwargs["anchor_batch_start_index"] == 4
        assert kwargs["anchor_batch_size"] == 4
        assert kwargs["generated_at"] == generated_at
        return pl.DataFrame([poland_row(evaluation_id="batch-4", anchor_hour=2)])

    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.get_strategy_evaluation_store",
        lambda: store,
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research."
        "build_official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame",
        fake_builder,
    )

    frame = official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame(
        None,
        DflPolandLag24ExperimentalRollingStrictAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            max_eval_windows=365,
            anchor_batch_start_index=4,
            anchor_batch_size=4,
            anchor_batch_order="chronological",
            resume_generated_at_iso=generated_at.isoformat(),
            merge_persisted_batches=True,
        ),
        pl.DataFrame({"tenant_id": ["client_003_dnipro_factory"]}),
        pl.DataFrame({"feature_name": ["entsoe_pl_lag24_day_ahead_price_uah_mwh"]}),
        pl.DataFrame({"feature_name": ["entsoe_pl_lag24_day_ahead_price_uah_mwh"]}),
    )

    assert frame["evaluation_id"].to_list() == ["batch-0", "batch-4"]


def test_dfl_research_assets_are_registered() -> None:
    asset_keys = {asset.key.to_user_string() for asset in DFL_RESEARCH_GOLD_ASSETS}
    registered_asset_keys = {asset.key.to_user_string() for asset in defs.assets or []}
    registered_all_asset_keys = {
        asset.key.to_user_string() for asset in defs.assets or []
    }

    assert {
        "real_data_value_aware_ensemble_frame",
        "dfl_training_frame",
        "dfl_training_example_frame",
        "dfl_data_coverage_audit_frame",
        "dfl_ua_coverage_repair_audit_frame",
        "dfl_action_label_panel_frame",
        "dfl_action_classifier_baseline_frame",
        "dfl_action_classifier_strict_lp_benchmark_frame",
        "dfl_value_aware_action_classifier_strict_lp_benchmark_frame",
        "dfl_action_classifier_failure_analysis_frame",
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
        "offline_dfl_panel_experiment_frame",
        "offline_dfl_panel_strict_lp_benchmark_frame",
        "offline_dfl_decision_target_panel_frame",
        "offline_dfl_decision_target_strict_lp_benchmark_frame",
        "offline_dfl_action_target_panel_frame",
        "offline_dfl_action_target_strict_lp_benchmark_frame",
        "dfl_trajectory_value_candidate_panel_frame",
        "dfl_trajectory_value_selector_frame",
        "dfl_trajectory_value_selector_strict_lp_benchmark_frame",
        "dfl_schedule_candidate_library_frame",
        "dfl_trajectory_feature_ranker_frame",
        "dfl_trajectory_feature_ranker_strict_lp_benchmark_frame",
        "dfl_pipeline_integrity_audit_frame",
        "forecast_pipeline_truth_audit_frame",
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_schedule_candidate_library_v2_plus_frame",
        "dfl_schedule_value_regret_decomposition_frame",
        "dfl_non_strict_oracle_upper_bound_frame",
        "dfl_strict_baseline_autopsy_frame",
        "dfl_strict_failure_selector_frame",
        "dfl_strict_failure_selector_strict_lp_benchmark_frame",
        "dfl_strict_failure_selector_robustness_frame",
        "dfl_strict_failure_prior_feature_panel_frame",
        "dfl_strict_failure_feature_audit_frame",
        "dfl_feature_aware_strict_failure_selector_frame",
        "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame",
        "dfl_regime_gated_tft_selector_v2_frame",
        "dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame",
        "dfl_forecast_dfl_v1_panel_frame",
        "dfl_forecast_dfl_v1_strict_lp_benchmark_frame",
        "dfl_real_data_trajectory_dataset_frame",
        "dfl_residual_schedule_value_model_frame",
        "dfl_residual_schedule_value_strict_lp_benchmark_frame",
        "dfl_offline_dt_candidate_frame",
        "dfl_offline_dt_candidate_strict_lp_benchmark_frame",
        "dfl_residual_dt_fallback_strict_lp_benchmark_frame",
        "dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame",
        "dfl_source_specific_research_challenger_frame",
        "dfl_schedule_value_learner_v2_frame",
        "dfl_schedule_value_learner_v2_strict_lp_benchmark_frame",
        "dfl_schedule_value_learner_v2_robustness_frame",
        "dfl_schedule_value_learner_v3_frame",
        "dfl_schedule_value_learner_v3_strict_lp_benchmark_frame",
        "dfl_schedule_value_learner_v2_plus_frame",
        "dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_schedule_value_production_gate_frame",
        "dfl_official_schedule_candidate_library_frame",
        "dfl_official_schedule_candidate_library_v2_frame",
        "dfl_official_schedule_value_learner_v2_frame",
        "dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame",
        "dfl_official_schedule_value_learner_v2_robustness_frame",
        "dfl_official_schedule_value_production_gate_frame",
        "dfl_official_global_panel_schedule_candidate_library_frame",
        "dfl_official_global_panel_schedule_candidate_library_v2_frame",
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_regret_decomposition_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_robustness_frame",
        "dfl_official_global_panel_schedule_value_learner_v3_frame",
        "dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_oracle_gap_audit_frame",
        "dfl_ua_calendar_publication_context_frame",
        "dfl_ua_weather_load_context_frame",
        "dfl_ua_grid_event_context_frame",
        "dfl_ua_context_oracle_gap_feature_panel_frame",
        "dfl_ua_context_safe_switch_separability_audit_frame",
        "dfl_ua_context_safe_switch_scorer_frame",
        "dfl_ua_context_safe_switch_strict_lp_benchmark_frame",
        "dfl_ua_context_safe_switch_rolling_robustness_frame",
        "dfl_ua_context_lava_teacher_frame",
        "dfl_ua_context_lava_sequence_training_frame",
        "dfl_ua_context_lava_candidate_policy_frame",
        "dfl_ua_context_lava_strict_lp_benchmark_frame",
        "dfl_ua_context_lava_rolling_robustness_frame",
        "dfl_v2_plus_learning_limit_audit_frame",
        "dfl_expanded_schedule_value_teacher_label_panel_v1_frame",
        "dfl_regret_surrogate_forecast_correction_v1_frame",
        "dfl_regret_surrogate_candidate_value_v1_frame",
        "dfl_regret_surrogate_strict_lp_benchmark_frame",
        "dfl_regret_surrogate_rolling_robustness_frame",
        "dfl_regret_surrogate_safe_switch_context_audit_frame",
        "dfl_regret_surrogate_teacher_label_panel_v2_frame",
        "dfl_regret_surrogate_contextual_candidate_value_v2_frame",
        "dfl_regret_surrogate_contextual_strict_lp_benchmark_frame",
        "dfl_regret_surrogate_contextual_rolling_robustness_frame",
        "dfl_sparse_safe_switch_feature_contract_audit_frame",
        "dfl_sparse_safe_switch_candidate_library_v6_frame",
        "dfl_sparse_safe_switch_opportunity_audit_frame",
        "dfl_sparse_safe_switch_teacher_label_panel_v6_frame",
        "dfl_sparse_safe_switch_abstention_model_v6_frame",
        "dfl_sparse_safe_switch_strict_lp_benchmark_frame",
        "dfl_sparse_safe_switch_rolling_robustness_frame",
        "dfl_v2_plus_opportunity_backfill_requirements_frame",
        "dfl_backfilled_context_feature_panel_v7_frame",
        "dfl_feasible_schedule_candidate_library_v7_frame",
        "dfl_candidate_value_teacher_label_panel_v7_frame",
        "dfl_candidate_value_regret_surrogate_v7_frame",
        "dfl_candidate_value_v7_strict_lp_benchmark_frame",
        "dfl_candidate_value_v7_rolling_robustness_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame",
        "official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame",
        "official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame",
        "official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame",
        "official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame",
        "dfl_poland_lag24_calibrated_schedule_candidate_library_frame",
        "dfl_poland_lag24_calibrated_schedule_candidate_library_v2_frame",
        "dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame",
        "dfl_poland_lag24_calibrated_schedule_value_learner_v2_frame",
        "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_frame",
        "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_poland_lag24_calibrated_vs_v2_plus_comparison_frame",
        "dfl_poland_lag24_prior_tail_risk_veto_frame",
        "dfl_poland_lag24_feature_consumption_audit_frame",
        "dfl_poland_lag24_candidate_value_label_panel_frame",
        "dfl_poland_lag24_candidate_value_ranker_frame",
        "dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame",
        "dfl_v2_plus_schedule_neighbor_teacher_label_frame",
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_candidate_value_scorer_frame",
        "dfl_lava_candidate_value_strict_lp_benchmark_frame",
        "dfl_lava_tail_risk_diagnostic_frame",
        "dfl_lava_tail_risk_aware_target_frame",
        "dfl_lava_tail_risk_aware_strict_lp_benchmark_frame",
        "dfl_lava_tail_risk_safe_switch_scorer_frame",
        "dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame",
        "dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame",
        "dfl_lava_tail_risk_safe_switch_scorer_v2_frame",
        "dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame",
        "dfl_lava_tail_risk_avoidance_label_frame",
        "dfl_lava_tail_risk_avoidance_scorer_v3_frame",
        "dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame",
        "dfl_lava_schedule_neighbor_dt_training_frame",
        "dfl_lava_schedule_neighbor_dt_policy_frame",
        "dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame",
        "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame",
        "dfl_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame",
        "dfl_poland_lag24_experimental_schedule_candidate_library_frame",
        "dfl_poland_lag24_experimental_schedule_candidate_library_v2_frame",
        "dfl_poland_lag24_experimental_schedule_candidate_library_v2_plus_frame",
        "dfl_poland_lag24_experimental_schedule_value_learner_v2_frame",
        "dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_frame",
        "dfl_poland_lag24_experimental_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_poland_lag24_experimental_vs_v2_plus_comparison_frame",
        "dfl_tft_quantile_schedule_candidate_library_frame",
        "dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame",
        "dfl_tft_combined_v2_plus_strict_lp_benchmark_frame",
        "dfl_tft_calibrated_quantile_schedule_candidate_library_frame",
        "dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame",
        "dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame",
        "dfl_nbeatsx_tft_complementarity_audit_frame",
        "dfl_nbeatsx_tft_candidate_portfolio_v1_frame",
        "dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame",
        "dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame",
        "dfl_nbeatsx_tft_meta_selector_rolling_strict_lp_benchmark_frame",
        "dfl_nbeatsx_tft_meta_selector_prior_rolling_robustness_frame",
        "dfl_nbeatsx_tft_meta_selector_robustness_frame",
        "dfl_official_global_panel_schedule_candidate_library_v3_frame",
        "dfl_official_global_panel_candidate_value_label_panel_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame",
        "dfl_official_global_panel_v2_v3_plateau_autopsy_frame",
        "dfl_official_global_panel_plateau_data_quality_audit_frame",
        "dfl_official_global_panel_schedule_candidate_library_v4_frame",
        "dfl_official_global_panel_candidate_value_label_panel_v4_frame",
        "dfl_official_global_panel_candidate_value_dfl_v4_frame",
        "dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame",
        "dfl_point_in_time_context_repair_audit_frame",
        "dfl_point_in_time_context_feature_panel_frame",
        "dfl_context_enriched_schedule_candidate_library_v5_frame",
        "dfl_context_enriched_candidate_value_label_panel_v5_frame",
        "dfl_context_enriched_candidate_value_dfl_v5_frame",
        "dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame",
        "dfl_official_global_panel_v2_plus_trajectory_dataset_frame",
        "dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame",
        "dfl_official_global_panel_v2_plus_offline_dt_candidate_frame",
        "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame",
        "dfl_official_v2_plus_bridge_failure_audit_frame",
        "dfl_market_coupled_schedule_value_learner_v2_plus_frame",
        "dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame",
        "dfl_market_coupling_v2_plus_ablation_frame",
        "dfl_official_global_panel_schedule_value_production_gate_frame",
        "dfl_production_promotion_gate_frame",
        "forecast_afe_feature_catalog_frame",
        "market_coupling_temporal_availability_frame",
        "entsoe_neighbor_market_query_spec_frame",
        "entsoe_neighbor_market_sample_audit_frame",
        "entsoe_neighbor_market_feature_candidate_frame",
        "nbu_eur_uah_fx_metadata_frame",
        "entsoe_poland_lagged_feature_candidate_frame",
        "poland_neighbor_market_snapshot_feature_candidate_frame",
        "poland_neighbor_market_hourly_feature_frame",
        "entsoe_poland_governance_closure_frame",
        "entsoe_poland_feature_governance_frame",
        "entsoe_neighbor_market_aligned_feature_panel_frame",
        "dfl_semantic_event_strict_failure_audit_frame",
        "afl_forecast_error_audit_frame",
        "forecast_candidate_forensics_frame",
        "afl_training_panel_frame",
    }.issubset(asset_keys)
    assert asset_keys.issubset(registered_asset_keys)
    assert "poland_neighbor_market_snapshot_bronze" in registered_all_asset_keys
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
    deps_by_key = {
        asset.key.to_user_string(): {
            dependency.to_user_string() for dependency in asset.dependency_keys
        }
        for asset in DFL_RESEARCH_GOLD_ASSETS
    }
    assert tags_by_key["dfl_relaxed_lp_pilot_frame"]["medallion"] == "gold"
    assert (
        tags_by_key["offline_dfl_experiment_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert groups_by_key["dfl_training_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_training_example_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_data_coverage_audit_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_ua_coverage_repair_audit_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_action_label_panel_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_action_classifier_baseline_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_action_classifier_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_value_aware_action_classifier_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_action_classifier_failure_analysis_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["offline_dfl_experiment_frame"] == "gold_dfl_training"
    assert groups_by_key["offline_dfl_panel_experiment_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["offline_dfl_panel_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["offline_dfl_decision_target_panel_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["offline_dfl_decision_target_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["offline_dfl_action_target_panel_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["offline_dfl_action_target_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_trajectory_value_candidate_panel_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_trajectory_value_selector_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_trajectory_value_selector_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_schedule_candidate_library_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_trajectory_feature_ranker_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_trajectory_feature_ranker_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_pipeline_integrity_audit_frame"] == "gold_dfl_training"
    assert groups_by_key["forecast_pipeline_truth_audit_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_schedule_candidate_library_v2_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_non_strict_oracle_upper_bound_frame"] == "gold_dfl_training"
    )
    assert groups_by_key["dfl_strict_baseline_autopsy_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_strict_failure_selector_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_strict_failure_selector_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_strict_failure_selector_robustness_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_strict_failure_prior_feature_panel_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_strict_failure_feature_audit_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_feature_aware_strict_failure_selector_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_regime_gated_tft_selector_v2_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_forecast_dfl_v1_panel_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_forecast_dfl_v1_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_real_data_trajectory_dataset_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_residual_schedule_value_model_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_residual_schedule_value_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_offline_dt_candidate_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_offline_dt_candidate_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_residual_dt_fallback_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_v2_plus_trajectory_dataset_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_v2_plus_offline_dt_candidate_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_v2_plus_bridge_failure_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_schedule_value_dfl_v2_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_schedule_candidate_library_v3_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_candidate_value_label_panel_v3_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_candidate_value_dfl_v3_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_v2_v3_plateau_autopsy_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_plateau_data_quality_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_schedule_candidate_library_v4_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_candidate_value_label_panel_v4_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_global_panel_candidate_value_dfl_v4_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_nbeatsx_tft_complementarity_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_nbeatsx_tft_candidate_portfolio_v1_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_nbeatsx_tft_meta_selector_robustness_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_poland_lag24_prior_tail_risk_veto_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_poland_lag24_feature_consumption_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_poland_lag24_candidate_value_label_panel_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_poland_lag24_candidate_value_ranker_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_v2_plus_schedule_neighbor_teacher_label_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_schedule_neighbor_candidate_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_lava_candidate_value_scorer_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_lava_candidate_value_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_lava_tail_risk_diagnostic_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_lava_tail_risk_aware_target_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_lava_tail_risk_aware_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_safe_switch_scorer_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_safe_switch_scorer_v2_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_avoidance_label_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_avoidance_scorer_v3_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_schedule_neighbor_dt_training_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_schedule_neighbor_dt_policy_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_calendar_publication_context_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_ua_weather_load_context_frame"] == "gold_dfl_training"
    assert groups_by_key["dfl_ua_grid_event_context_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_ua_context_oracle_gap_feature_panel_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_safe_switch_separability_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_safe_switch_scorer_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_safe_switch_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_safe_switch_rolling_robustness_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_ua_context_lava_teacher_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_ua_context_lava_sequence_training_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_lava_candidate_policy_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_lava_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_ua_context_lava_rolling_robustness_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_v2_plus_learning_limit_audit_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_expanded_schedule_value_teacher_label_panel_v1_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_regret_surrogate_forecast_correction_v1_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_regret_surrogate_candidate_value_v1_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_regret_surrogate_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_regret_surrogate_rolling_robustness_frame"]
        == "gold_dfl_training"
    )
    for asset_key in (
        "dfl_sparse_safe_switch_feature_contract_audit_frame",
        "dfl_sparse_safe_switch_candidate_library_v6_frame",
        "dfl_sparse_safe_switch_opportunity_audit_frame",
        "dfl_sparse_safe_switch_teacher_label_panel_v6_frame",
        "dfl_sparse_safe_switch_abstention_model_v6_frame",
        "dfl_sparse_safe_switch_strict_lp_benchmark_frame",
        "dfl_sparse_safe_switch_rolling_robustness_frame",
    ):
        assert groups_by_key[asset_key] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_point_in_time_context_repair_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_point_in_time_context_feature_panel_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_context_enriched_schedule_candidate_library_v5_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_context_enriched_candidate_value_label_panel_v5_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_context_enriched_candidate_value_dfl_v5_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_source_specific_research_challenger_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_schedule_value_learner_v2_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_schedule_value_learner_v2_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_schedule_value_learner_v2_robustness_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_schedule_value_learner_v3_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["dfl_schedule_value_learner_v3_strict_lp_benchmark_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_schedule_value_production_gate_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_schedule_candidate_library_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_schedule_candidate_library_v2_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_schedule_value_learner_v2_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key[
            "dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame"
        ]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_schedule_value_learner_v2_robustness_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_official_schedule_value_production_gate_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["dfl_production_promotion_gate_frame"] == "gold_dfl_training"
    assert groups_by_key["forecast_afe_feature_catalog_frame"] == "gold_dfl_training"
    assert (
        groups_by_key["market_coupling_temporal_availability_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["entsoe_neighbor_market_query_spec_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["entsoe_neighbor_market_sample_audit_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["entsoe_neighbor_market_feature_candidate_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["entsoe_poland_lagged_feature_candidate_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["poland_neighbor_market_snapshot_feature_candidate_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["poland_neighbor_market_hourly_feature_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["entsoe_poland_governance_closure_frame"] == "gold_dfl_training"
    )
    assert (
        groups_by_key["entsoe_neighbor_market_aligned_feature_panel_frame"]
        == "gold_dfl_training"
    )
    assert (
        groups_by_key["dfl_semantic_event_strict_failure_audit_frame"]
        == "gold_dfl_training"
    )
    assert groups_by_key["afl_forecast_error_audit_frame"] == "gold_dfl_training"
    assert groups_by_key["forecast_candidate_forensics_frame"] == "gold_dfl_training"
    assert groups_by_key["afl_training_panel_frame"] == "gold_dfl_training"
    assert (
        tags_by_key["offline_dfl_panel_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["offline_dfl_panel_strict_lp_benchmark_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["offline_dfl_decision_target_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["offline_dfl_decision_target_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["offline_dfl_action_target_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["offline_dfl_action_target_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_trajectory_value_candidate_panel_frame"]["ml_stage"]
        == "evaluation"
    )
    assert tags_by_key["dfl_trajectory_value_selector_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["dfl_trajectory_value_selector_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_trajectory_value_selector_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_schedule_candidate_library_frame"]["ml_stage"]
        == "training_data"
    )
    assert tags_by_key["dfl_trajectory_feature_ranker_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["dfl_trajectory_feature_ranker_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_trajectory_feature_ranker_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_pipeline_integrity_audit_frame"]["ml_stage"] == "diagnostics"
    )
    assert (
        tags_by_key["forecast_pipeline_truth_audit_frame"]["ml_stage"] == "diagnostics"
    )
    assert (
        tags_by_key["forecast_pipeline_truth_audit_frame"]["evidence_scope"]
        == "research_only"
    )
    assert (
        tags_by_key["dfl_schedule_candidate_library_v2_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_non_strict_oracle_upper_bound_frame"]["ml_stage"]
        == "evaluation"
    )
    assert tags_by_key["dfl_strict_baseline_autopsy_frame"]["ml_stage"] == "diagnostics"
    assert tags_by_key["dfl_strict_failure_selector_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["dfl_strict_failure_selector_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_strict_failure_selector_robustness_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_strict_failure_prior_feature_panel_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_strict_failure_feature_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_feature_aware_strict_failure_selector_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key[
            "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame"
        ]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_regime_gated_tft_selector_v2_frame"]["ml_stage"] == "selection"
    )
    assert (
        tags_by_key["dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert tags_by_key["dfl_forecast_dfl_v1_panel_frame"]["ml_stage"] == "training_data"
    assert (
        tags_by_key["dfl_forecast_dfl_v1_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_real_data_trajectory_dataset_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_residual_schedule_value_model_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_residual_schedule_value_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert tags_by_key["dfl_offline_dt_candidate_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["dfl_offline_dt_candidate_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_residual_dt_fallback_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_official_global_panel_v2_plus_trajectory_dataset_frame"][
            "ml_stage"
        ]
        == "training_data"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"
        ]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_official_v2_plus_bridge_failure_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_source_specific_research_challenger_frame"]["ml_stage"]
        == "evaluation"
    )
    assert tags_by_key["dfl_schedule_value_learner_v2_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["dfl_schedule_value_learner_v2_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_schedule_value_learner_v2_robustness_frame"]["ml_stage"]
        == "evaluation"
    )
    assert tags_by_key["dfl_schedule_value_learner_v3_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["dfl_schedule_value_learner_v3_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_schedule_value_production_gate_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_official_schedule_candidate_library_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_official_schedule_candidate_library_v2_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_learner_v2_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_learner_v2_robustness_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_production_gate_frame"]["ml_stage"]
        == "selection"
    )
    assert tags_by_key["dfl_production_promotion_gate_frame"]["ml_stage"] == "selection"
    assert (
        tags_by_key["forecast_afe_feature_catalog_frame"]["ml_stage"]
        == "feature_engineering"
    )
    assert (
        tags_by_key["market_coupling_temporal_availability_frame"]["ml_stage"]
        == "feature_engineering"
    )
    assert (
        tags_by_key["entsoe_neighbor_market_query_spec_frame"]["ml_stage"]
        == "feature_engineering"
    )
    assert (
        tags_by_key["entsoe_neighbor_market_sample_audit_frame"]["ml_stage"]
        == "feature_engineering"
    )
    assert (
        tags_by_key["entsoe_neighbor_market_aligned_feature_panel_frame"]["ml_stage"]
        == "feature_engineering"
    )
    assert tags_by_key["nbu_eur_uah_fx_metadata_frame"]["ml_stage"] == (
        "feature_engineering"
    )
    assert (
        tags_by_key["entsoe_poland_lagged_feature_candidate_frame"]["ml_stage"]
        == "feature_engineering"
    )
    assert tags_by_key["entsoe_poland_feature_governance_frame"]["ml_stage"] == (
        "feature_engineering"
    )
    assert tags_by_key["poland_neighbor_market_hourly_feature_frame"]["ml_stage"] == (
        "feature_engineering"
    )
    assert tags_by_key["entsoe_poland_governance_closure_frame"]["ml_stage"] == (
        "feature_engineering"
    )
    assert (
        tags_by_key["dfl_semantic_event_strict_failure_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert tags_by_key["afl_forecast_error_audit_frame"]["ml_stage"] == "diagnostics"
    assert (
        tags_by_key["forecast_candidate_forensics_frame"]["ml_stage"] == "diagnostics"
    )
    assert tags_by_key["afl_training_panel_frame"]["ml_stage"] == "training_data"
    assert (
        tags_by_key["dfl_strict_failure_selector_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_strict_failure_selector_robustness_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_strict_failure_prior_feature_panel_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_strict_failure_feature_audit_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_semantic_event_strict_failure_audit_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_forecast_dfl_v1_strict_lp_benchmark_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_source_specific_research_challenger_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_production_promotion_gate_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_schedule_value_learner_v2_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_schedule_value_learner_v2_robustness_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_schedule_value_learner_v3_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_schedule_value_production_gate_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_learner_v2_robustness_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_official_schedule_value_production_gate_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame"
        ]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_official_global_panel_candidate_value_label_panel_v3_frame"][
            "ml_stage"
        ]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_official_global_panel_v2_v3_plateau_autopsy_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_official_global_panel_plateau_data_quality_audit_frame"][
            "ml_stage"
        ]
        == "diagnostics"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_official_global_panel_candidate_value_label_panel_v4_frame"][
            "ml_stage"
        ]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_point_in_time_context_repair_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_point_in_time_context_feature_panel_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_ua_context_safe_switch_separability_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_ua_context_oracle_gap_feature_panel_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_ua_context_safe_switch_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert tags_by_key["dfl_ua_context_lava_teacher_frame"]["ml_stage"] == (
        "training_data"
    )
    assert tags_by_key["dfl_ua_context_lava_candidate_policy_frame"]["ml_stage"] == (
        "selection"
    )
    assert (
        tags_by_key["dfl_ua_context_lava_strict_lp_benchmark_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_ua_context_lava_rolling_robustness_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_v2_plus_learning_limit_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_expanded_schedule_value_teacher_label_panel_v1_frame"][
            "ml_stage"
        ]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_regret_surrogate_forecast_correction_v1_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_regret_surrogate_strict_lp_benchmark_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_feature_contract_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_candidate_library_v6_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_opportunity_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_teacher_label_panel_v6_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_abstention_model_v6_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_sparse_safe_switch_rolling_robustness_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_v2_plus_opportunity_backfill_requirements_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_feasible_schedule_candidate_library_v7_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_candidate_value_regret_surrogate_v7_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_candidate_value_v7_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_candidate_value_v7_rolling_robustness_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key[
            "dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_tft_combined_v2_plus_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_nbeatsx_tft_complementarity_audit_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_nbeatsx_tft_candidate_portfolio_v1_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame"][
            "ml_stage"
        ]
        == "selection"
    )
    assert (
        tags_by_key["dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_nbeatsx_tft_meta_selector_robustness_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_market_coupling_v2_plus_ablation_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key[
            "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"
        ]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_official_v2_plus_bridge_failure_audit_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_lava_candidate_value_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_lava_tail_risk_aware_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_lava_schedule_neighbor_dt_training_frame"]["ml_stage"]
        == "training_data"
    )
    assert (
        tags_by_key["dfl_lava_schedule_neighbor_dt_policy_frame"]["ml_stage"]
        == "selection"
    )
    assert (
        tags_by_key["dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["afl_forecast_error_audit_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        "real_data_benchmark_silver_feature_frame"
        in deps_by_key["dfl_semantic_event_strict_failure_audit_frame"]
    )
    assert (
        "ukrenergo_grid_events_bronze"
        in deps_by_key["dfl_semantic_event_strict_failure_audit_frame"]
    )
    assert (
        "grid_event_signal_silver"
        not in deps_by_key["dfl_semantic_event_strict_failure_audit_frame"]
    )
    assert deps_by_key["entsoe_neighbor_market_aligned_feature_panel_frame"] == {
        "real_data_benchmark_silver_feature_frame",
        "entsoe_neighbor_market_feature_candidate_frame",
    }
    assert deps_by_key["entsoe_poland_lagged_feature_candidate_frame"] == {
        "real_data_benchmark_silver_feature_frame",
        "entsoe_neighbor_market_feature_candidate_frame",
        "nbu_eur_uah_fx_metadata_frame",
        "poland_neighbor_market_snapshot_feature_candidate_frame",
    }
    assert deps_by_key["entsoe_poland_feature_governance_frame"] == {
        "entsoe_neighbor_market_feature_candidate_frame",
        "entsoe_poland_lagged_feature_candidate_frame",
        "poland_neighbor_market_snapshot_feature_candidate_frame",
    }
    assert deps_by_key["poland_neighbor_market_hourly_feature_frame"] == {
        "poland_neighbor_market_snapshot_bronze"
    }
    assert deps_by_key["entsoe_poland_governance_closure_frame"] == {
        "poland_neighbor_market_hourly_feature_frame"
    }
    assert deps_by_key["dfl_real_data_trajectory_dataset_frame"] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_strict_failure_prior_feature_panel_frame",
    }
    assert deps_by_key["forecast_pipeline_truth_audit_frame"] == {
        "real_data_rolling_origin_benchmark_frame"
    }
    assert deps_by_key["dfl_residual_schedule_value_model_frame"] == {
        "dfl_real_data_trajectory_dataset_frame"
    }
    assert deps_by_key["dfl_residual_schedule_value_strict_lp_benchmark_frame"] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_residual_schedule_value_model_frame",
    }
    assert deps_by_key["dfl_offline_dt_candidate_frame"] == {
        "dfl_real_data_trajectory_dataset_frame"
    }
    assert deps_by_key["dfl_offline_dt_candidate_strict_lp_benchmark_frame"] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_offline_dt_candidate_frame",
    }
    assert deps_by_key["dfl_residual_dt_fallback_strict_lp_benchmark_frame"] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_residual_schedule_value_model_frame",
        "dfl_offline_dt_candidate_frame",
    }
    assert deps_by_key["dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"] == {
        "dfl_residual_dt_fallback_strict_lp_benchmark_frame",
        "dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_v2_plus_trajectory_dataset_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v2_plus_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame"
    ] == {"dfl_official_global_panel_v2_plus_trajectory_dataset_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_v2_plus_offline_dt_candidate_frame"
    ] == {"dfl_official_global_panel_v2_plus_trajectory_dataset_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_v2_plus_residual_schedule_value_model_frame",
        "dfl_official_global_panel_v2_plus_offline_dt_candidate_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_official_global_panel_schedule_value_dfl_v2_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_dfl_v2_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_dfl_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_candidate_library_v3_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v2_plus_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_candidate_value_label_panel_v3_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v3_frame"}
    assert deps_by_key["dfl_official_global_panel_candidate_value_dfl_v3_frame"] == {
        "dfl_official_global_panel_candidate_value_label_panel_v3_frame",
        "dfl_official_global_panel_schedule_candidate_library_v3_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_candidate_value_dfl_v3_failure_audit_frame"
    ] == {
        "dfl_official_global_panel_candidate_value_label_panel_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_official_global_panel_v2_v3_plateau_autopsy_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v3_frame",
        "dfl_official_global_panel_candidate_value_label_panel_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_frame",
        "dfl_official_global_panel_candidate_value_dfl_v3_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_plateau_data_quality_audit_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v3_frame",
        "real_data_benchmark_silver_feature_frame",
        "dfl_official_global_panel_v2_v3_plateau_autopsy_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_candidate_library_v4_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v3_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_candidate_value_label_panel_v4_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v4_frame"}
    assert deps_by_key["dfl_official_global_panel_candidate_value_dfl_v4_frame"] == {
        "dfl_official_global_panel_candidate_value_label_panel_v4_frame",
        "dfl_official_global_panel_schedule_candidate_library_v4_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_candidate_value_dfl_v4_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v4_frame",
        "dfl_official_global_panel_candidate_value_dfl_v4_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_point_in_time_context_repair_audit_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v4_frame",
        "real_data_benchmark_silver_feature_frame",
        "dfl_official_global_panel_v2_v3_plateau_autopsy_frame",
    }
    assert deps_by_key["dfl_point_in_time_context_feature_panel_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v4_frame",
        "dfl_point_in_time_context_repair_audit_frame",
        "real_data_benchmark_silver_feature_frame",
    }
    assert deps_by_key["dfl_ua_calendar_publication_context_frame"] == {
        "dfl_oracle_gap_safe_switch_feature_panel_frame",
        "real_data_benchmark_silver_feature_frame",
    }
    assert deps_by_key["dfl_ua_weather_load_context_frame"] == {
        "dfl_oracle_gap_safe_switch_feature_panel_frame",
        "real_data_benchmark_silver_feature_frame",
        "tenant_historical_net_load_silver",
    }
    assert deps_by_key["dfl_ua_grid_event_context_frame"] == {
        "dfl_oracle_gap_safe_switch_feature_panel_frame",
        "grid_event_signal_silver",
    }
    assert deps_by_key["dfl_ua_context_oracle_gap_feature_panel_frame"] == {
        "dfl_oracle_gap_safe_switch_feature_panel_frame",
        "dfl_ua_calendar_publication_context_frame",
        "dfl_ua_weather_load_context_frame",
        "dfl_ua_grid_event_context_frame",
    }
    assert deps_by_key["dfl_ua_context_safe_switch_separability_audit_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame"
    }
    assert deps_by_key["dfl_ua_context_safe_switch_scorer_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame"
    }
    assert deps_by_key["dfl_ua_context_safe_switch_strict_lp_benchmark_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame",
        "dfl_ua_context_safe_switch_scorer_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_ua_context_safe_switch_rolling_robustness_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame"
    }
    assert deps_by_key["dfl_ua_context_lava_teacher_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame",
        "dfl_lava_tail_risk_avoidance_label_frame",
    }
    assert deps_by_key["dfl_ua_context_lava_sequence_training_frame"] == {
        "dfl_ua_context_lava_teacher_frame"
    }
    assert deps_by_key["dfl_ua_context_lava_candidate_policy_frame"] == {
        "dfl_ua_context_lava_sequence_training_frame"
    }
    assert deps_by_key["dfl_ua_context_lava_strict_lp_benchmark_frame"] == {
        "dfl_ua_context_lava_sequence_training_frame",
        "dfl_ua_context_lava_candidate_policy_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_ua_context_lava_rolling_robustness_frame"] == {
        "dfl_ua_context_lava_sequence_training_frame"
    }
    assert deps_by_key["dfl_v2_plus_learning_limit_audit_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame"
    }
    assert deps_by_key["dfl_expanded_schedule_value_teacher_label_panel_v1_frame"] == {
        "dfl_ua_context_oracle_gap_feature_panel_frame",
        "dfl_v2_plus_learning_limit_audit_frame",
    }
    assert deps_by_key["dfl_regret_surrogate_forecast_correction_v1_frame"] == {
        "dfl_expanded_schedule_value_teacher_label_panel_v1_frame"
    }
    assert deps_by_key["dfl_regret_surrogate_candidate_value_v1_frame"] == {
        "dfl_expanded_schedule_value_teacher_label_panel_v1_frame",
        "dfl_regret_surrogate_forecast_correction_v1_frame",
    }
    assert deps_by_key["dfl_regret_surrogate_strict_lp_benchmark_frame"] == {
        "dfl_expanded_schedule_value_teacher_label_panel_v1_frame",
        "dfl_regret_surrogate_candidate_value_v1_frame",
    }
    assert deps_by_key["dfl_regret_surrogate_rolling_robustness_frame"] == {
        "dfl_expanded_schedule_value_teacher_label_panel_v1_frame"
    }
    assert deps_by_key["dfl_sparse_safe_switch_feature_contract_audit_frame"] == {
        "dfl_regret_surrogate_teacher_label_panel_v2_frame"
    }
    assert deps_by_key["dfl_sparse_safe_switch_candidate_library_v6_frame"] == {
        "dfl_regret_surrogate_teacher_label_panel_v2_frame"
    }
    assert deps_by_key["dfl_sparse_safe_switch_opportunity_audit_frame"] == {
        "dfl_sparse_safe_switch_candidate_library_v6_frame"
    }
    assert deps_by_key["dfl_sparse_safe_switch_teacher_label_panel_v6_frame"] == {
        "dfl_sparse_safe_switch_candidate_library_v6_frame",
        "dfl_sparse_safe_switch_opportunity_audit_frame",
    }
    assert deps_by_key["dfl_sparse_safe_switch_abstention_model_v6_frame"] == {
        "dfl_sparse_safe_switch_teacher_label_panel_v6_frame"
    }
    assert deps_by_key["dfl_sparse_safe_switch_strict_lp_benchmark_frame"] == {
        "dfl_sparse_safe_switch_teacher_label_panel_v6_frame",
        "dfl_sparse_safe_switch_abstention_model_v6_frame",
    }
    assert deps_by_key["dfl_sparse_safe_switch_rolling_robustness_frame"] == {
        "dfl_sparse_safe_switch_candidate_library_v6_frame"
    }
    assert deps_by_key["dfl_v2_plus_opportunity_backfill_requirements_frame"] == {
        "dfl_sparse_safe_switch_candidate_library_v6_frame",
        "dfl_sparse_safe_switch_opportunity_audit_frame",
    }
    assert deps_by_key["dfl_backfilled_context_feature_panel_v7_frame"] == {
        "dfl_sparse_safe_switch_candidate_library_v6_frame",
        "dfl_v2_plus_opportunity_backfill_requirements_frame",
    }
    assert deps_by_key["dfl_feasible_schedule_candidate_library_v7_frame"] == {
        "dfl_backfilled_context_feature_panel_v7_frame",
        "dfl_v2_plus_opportunity_backfill_requirements_frame",
    }
    assert deps_by_key["dfl_candidate_value_teacher_label_panel_v7_frame"] == {
        "dfl_feasible_schedule_candidate_library_v7_frame",
        "dfl_v2_plus_opportunity_backfill_requirements_frame",
    }
    assert deps_by_key["dfl_candidate_value_regret_surrogate_v7_frame"] == {
        "dfl_candidate_value_teacher_label_panel_v7_frame"
    }
    assert deps_by_key["dfl_candidate_value_v7_strict_lp_benchmark_frame"] == {
        "dfl_candidate_value_teacher_label_panel_v7_frame",
        "dfl_candidate_value_regret_surrogate_v7_frame",
    }
    assert deps_by_key["dfl_candidate_value_v7_rolling_robustness_frame"] == {
        "dfl_feasible_schedule_candidate_library_v7_frame"
    }
    assert deps_by_key["dfl_context_enriched_schedule_candidate_library_v5_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v4_frame",
        "dfl_point_in_time_context_feature_panel_frame",
    }
    assert deps_by_key["dfl_context_enriched_candidate_value_label_panel_v5_frame"] == {
        "dfl_context_enriched_schedule_candidate_library_v5_frame",
        "dfl_point_in_time_context_feature_panel_frame",
    }
    assert deps_by_key["dfl_context_enriched_candidate_value_dfl_v5_frame"] == {
        "dfl_context_enriched_schedule_candidate_library_v5_frame",
        "dfl_context_enriched_candidate_value_label_panel_v5_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
    }
    assert deps_by_key[
        "dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame"
    ] == {
        "dfl_context_enriched_schedule_candidate_library_v5_frame",
        "dfl_context_enriched_candidate_value_dfl_v5_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_official_v2_plus_bridge_failure_audit_frame"] == {
        "dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame"
    }
    assert deps_by_key["dfl_source_specific_research_challenger_frame"] == {
        "dfl_residual_dt_fallback_strict_lp_benchmark_frame",
        "dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame",
        "dfl_strict_failure_selector_robustness_frame",
        "dfl_strict_failure_feature_audit_frame",
    }
    assert deps_by_key["dfl_schedule_value_learner_v2_frame"] == {
        "dfl_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key["dfl_schedule_value_learner_v2_strict_lp_benchmark_frame"] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_schedule_value_learner_v2_frame",
    }
    assert deps_by_key["dfl_schedule_value_learner_v2_robustness_frame"] == {
        "dfl_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key["dfl_schedule_value_learner_v3_frame"] == {
        "dfl_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key["dfl_schedule_value_learner_v3_strict_lp_benchmark_frame"] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_schedule_value_learner_v2_frame",
        "dfl_schedule_value_learner_v3_frame",
    }
    assert deps_by_key["dfl_schedule_value_production_gate_frame"] == {
        "dfl_schedule_value_learner_v2_strict_lp_benchmark_frame",
        "dfl_schedule_value_learner_v2_robustness_frame",
    }
    assert deps_by_key["dfl_official_schedule_candidate_library_frame"] == {
        "official_forecast_rolling_origin_benchmark_frame"
    }
    assert deps_by_key["dfl_official_schedule_candidate_library_v2_frame"] == {
        "dfl_official_schedule_candidate_library_frame"
    }
    assert deps_by_key["dfl_official_schedule_value_learner_v2_frame"] == {
        "dfl_official_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key[
        "dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_schedule_candidate_library_v2_frame",
        "dfl_official_schedule_value_learner_v2_frame",
    }
    assert deps_by_key["dfl_official_schedule_value_learner_v2_robustness_frame"] == {
        "dfl_official_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key["dfl_official_schedule_value_production_gate_frame"] == {
        "dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame",
        "dfl_official_schedule_value_learner_v2_robustness_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_candidate_library_frame"
    ] == {"nbeatsx_official_global_panel_rolling_calibrated_strict_lp_benchmark_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_schedule_candidate_library_v2_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_frame"}
    assert deps_by_key["dfl_official_global_panel_schedule_value_learner_v2_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_learner_v2_robustness_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v2_frame"}
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame"
    ] == {"dfl_official_global_panel_schedule_candidate_library_v2_plus_frame"}
    assert deps_by_key["dfl_tft_quantile_schedule_candidate_library_frame"] == {
        "tft_official_global_panel_rolling_strict_lp_benchmark_frame"
    }
    assert deps_by_key["dfl_tft_augmented_v2_plus_strict_lp_benchmark_frame"] == {
        "dfl_tft_quantile_schedule_candidate_library_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_tft_combined_v2_plus_strict_lp_benchmark_frame"] == {
        "dfl_tft_quantile_schedule_candidate_library_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_tft_calibrated_quantile_schedule_candidate_library_frame"
    ] == {
        "tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame"
    }
    assert deps_by_key[
        "dfl_tft_calibrated_augmented_v2_plus_strict_lp_benchmark_frame"
    ] == {
        "dfl_tft_calibrated_quantile_schedule_candidate_library_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame"
    ] == {
        "dfl_tft_calibrated_quantile_schedule_candidate_library_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_nbeatsx_tft_complementarity_audit_frame"] == {
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_tft_calibrated_quantile_schedule_candidate_library_frame",
    }
    assert deps_by_key["dfl_nbeatsx_tft_candidate_portfolio_v1_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_tft_calibrated_quantile_schedule_candidate_library_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_nbeatsx_tft_complementarity_audit_frame",
    }
    assert deps_by_key["dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame"] == {
        "dfl_nbeatsx_tft_candidate_portfolio_v1_frame",
        "dfl_nbeatsx_tft_complementarity_audit_frame",
    }
    assert deps_by_key["dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame"] == {
        "dfl_nbeatsx_tft_candidate_portfolio_v1_frame",
        "dfl_nbeatsx_tft_candidate_value_meta_selector_v1_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_nbeatsx_tft_meta_selector_robustness_frame"] == {
        "dfl_nbeatsx_tft_meta_selector_strict_lp_benchmark_frame"
    }
    assert deps_by_key["dfl_market_coupling_v2_plus_ablation_frame"] == {
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_robustness_frame",
        "official_forecast_exogenous_feature_route_frame",
        "dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame",
    }
    assert deps_by_key["dfl_v2_plus_schedule_neighbor_teacher_label_frame"] == {
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
        "dfl_poland_lag24_prior_tail_risk_veto_frame",
        "dfl_poland_lag24_candidate_value_ranker_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_schedule_neighbor_candidate_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_poland_lag24_calibrated_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_learner_v2_plus_oracle_gap_audit_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_candidate_value_scorer_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame"
    }
    assert deps_by_key["dfl_lava_candidate_value_strict_lp_benchmark_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_candidate_value_scorer_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_diagnostic_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_candidate_value_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_aware_target_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_tail_risk_diagnostic_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_aware_strict_lp_benchmark_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_tail_risk_aware_target_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_safe_switch_scorer_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_tail_risk_diagnostic_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_tail_risk_safe_switch_scorer_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame"] == {
        "dfl_lava_schedule_neighbor_candidate_frame",
        "dfl_lava_tail_risk_diagnostic_frame",
        "entsoe_poland_lagged_feature_candidate_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_safe_switch_scorer_v2_frame"] == {
        "dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame",
    }
    assert deps_by_key[
        "dfl_lava_tail_risk_safe_switch_v2_strict_lp_benchmark_frame"
    ] == {
        "dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame",
        "dfl_lava_tail_risk_safe_switch_scorer_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_avoidance_label_frame"] == {
        "dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_avoidance_scorer_v3_frame"] == {
        "dfl_lava_tail_risk_avoidance_label_frame",
    }
    assert deps_by_key["dfl_lava_tail_risk_avoidance_v3_strict_lp_benchmark_frame"] == {
        "dfl_lava_tail_risk_avoidance_label_frame",
        "dfl_lava_tail_risk_avoidance_scorer_v3_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_lava_schedule_neighbor_dt_training_frame"] == {
        "dfl_lava_tail_risk_avoidance_label_frame",
    }
    assert deps_by_key["dfl_lava_schedule_neighbor_dt_policy_frame"] == {
        "dfl_lava_schedule_neighbor_dt_training_frame",
    }
    assert deps_by_key["dfl_lava_schedule_neighbor_dt_strict_lp_benchmark_frame"] == {
        "dfl_lava_schedule_neighbor_dt_training_frame",
        "dfl_lava_schedule_neighbor_dt_policy_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_strict_lp_benchmark_frame",
    }
    assert deps_by_key["dfl_market_coupled_schedule_value_learner_v2_plus_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
        "official_forecast_exogenous_feature_route_frame",
        "entsoe_poland_lagged_feature_candidate_frame",
    }
    assert deps_by_key[
        "dfl_market_coupled_schedule_value_learner_v2_plus_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_plus_frame",
        "dfl_market_coupled_schedule_value_learner_v2_plus_frame",
        "entsoe_poland_lagged_feature_candidate_frame",
    }
    assert deps_by_key[
        "dfl_market_coupled_schedule_value_learner_v2_plus_robustness_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_plus_frame",
        "official_forecast_exogenous_feature_route_frame",
        "entsoe_poland_lagged_feature_candidate_frame",
    }
    assert deps_by_key["dfl_official_global_panel_schedule_value_learner_v3_frame"] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_frame"
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_learner_v3_strict_lp_benchmark_frame"
    ] == {
        "dfl_official_global_panel_schedule_candidate_library_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_frame",
        "dfl_official_global_panel_schedule_value_learner_v3_frame",
    }
    assert deps_by_key[
        "dfl_official_global_panel_schedule_value_production_gate_frame"
    ] == {
        "dfl_official_global_panel_schedule_value_learner_v2_strict_lp_benchmark_frame",
        "dfl_official_global_panel_schedule_value_learner_v2_robustness_frame",
    }
    assert deps_by_key["dfl_ua_coverage_repair_audit_frame"] == {
        "real_data_benchmark_silver_feature_frame",
        "dfl_data_coverage_audit_frame",
    }
    assert deps_by_key["dfl_regime_gated_tft_selector_v2_frame"] == {
        "dfl_strict_failure_prior_feature_panel_frame",
        "dfl_strict_failure_feature_audit_frame",
    }
    assert deps_by_key[
        "dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame"
    ] == {
        "dfl_schedule_candidate_library_v2_frame",
        "dfl_regime_gated_tft_selector_v2_frame",
        "dfl_strict_failure_prior_feature_panel_frame",
        "dfl_strict_failure_feature_audit_frame",
    }
    assert deps_by_key["dfl_production_promotion_gate_frame"] == {
        "dfl_source_specific_research_challenger_frame",
        "dfl_strict_failure_selector_robustness_frame",
        "dfl_strict_failure_feature_audit_frame",
        "dfl_data_coverage_audit_frame",
        "dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame",
    }
    assert tags_by_key["dfl_data_coverage_audit_frame"]["ml_stage"] == "diagnostics"
    assert (
        tags_by_key["dfl_ua_coverage_repair_audit_frame"]["ml_stage"] == "diagnostics"
    )
    assert tags_by_key["dfl_action_label_panel_frame"]["ml_stage"] == "training_data"
    assert (
        tags_by_key["dfl_action_classifier_baseline_frame"]["ml_stage"] == "evaluation"
    )
    assert (
        tags_by_key["dfl_action_classifier_baseline_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_action_classifier_strict_lp_benchmark_frame"]["ml_stage"]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_action_classifier_strict_lp_benchmark_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_value_aware_action_classifier_strict_lp_benchmark_frame"][
            "ml_stage"
        ]
        == "evaluation"
    )
    assert (
        tags_by_key["dfl_value_aware_action_classifier_strict_lp_benchmark_frame"][
            "evidence_scope"
        ]
        == "not_market_execution"
    )
    assert (
        tags_by_key["dfl_action_classifier_failure_analysis_frame"]["ml_stage"]
        == "diagnostics"
    )
    assert (
        tags_by_key["dfl_action_classifier_failure_analysis_frame"]["evidence_scope"]
        == "not_market_execution"
    )
    assert (
        groups_by_key["regret_weighted_forecast_calibration_frame"]
        == "gold_calibration"
    )
    assert (
        groups_by_key["horizon_regret_weighted_forecast_calibration_frame"]
        == "gold_calibration"
    )
    assert (
        groups_by_key["regret_weighted_forecast_strategy_benchmark_frame"]
        == "gold_calibration"
    )
    assert (
        groups_by_key["horizon_regret_weighted_forecast_strategy_benchmark_frame"]
        == "gold_calibration"
    )
    assert (
        groups_by_key["calibrated_value_aware_ensemble_frame"]
        == "gold_selector_diagnostics"
    )
    assert (
        groups_by_key["forecast_dispatch_sensitivity_frame"]
        == "gold_selector_diagnostics"
    )
    assert (
        groups_by_key["risk_adjusted_value_gate_frame"] == "gold_selector_diagnostics"
    )


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
    training_examples = dfl_training_example_frame(None, benchmark)
    coverage_audit = dfl_data_coverage_audit_frame(
        None,
        DflDataCoverageAuditAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            target_anchor_count_per_tenant=2,
            required_past_hours=72,
            horizon_hours=24,
        ),
        _silver_feature_frame(),
        benchmark,
    )
    action_labels = dfl_action_label_panel_frame(
        None,
        DflActionLabelPanelAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_holdout_anchor_count_per_tenant=2,
        ),
        benchmark,
        coverage_audit,
    )
    action_classifier = dfl_action_classifier_baseline_frame(
        None,
        DflActionClassifierBaselineAssetConfig(),
        action_labels,
    )
    action_classifier_strict = dfl_action_classifier_strict_lp_benchmark_frame(
        None,
        DflActionClassifierStrictLpProjectionAssetConfig(),
        action_labels,
        action_classifier,
        benchmark,
    )
    value_aware_action_classifier_strict = (
        dfl_value_aware_action_classifier_strict_lp_benchmark_frame(
            None,
            DflValueAwareActionClassifierStrictLpProjectionAssetConfig(
                value_weight_scale_uah=100.0
            ),
            action_labels,
            benchmark,
        )
    )
    failure_analysis = dfl_action_classifier_failure_analysis_frame(
        None,
        action_labels,
        action_classifier_strict,
        value_aware_action_classifier_strict,
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
    horizon_calibrated_benchmark = (
        horizon_regret_weighted_forecast_strategy_benchmark_frame(
            None,
            benchmark,
            horizon_calibration,
        )
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
    relaxed_pilot = dfl_relaxed_lp_pilot_frame(
        None, RelaxedDflPilotAssetConfig(max_examples=4), benchmark
    )
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
    offline_panel = offline_dfl_panel_experiment_frame(
        None,
        OfflineDflPanelExperimentAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            inner_validation_fraction=0.34,
            epoch_count=2,
        ),
        benchmark,
    )
    strict_panel = offline_dfl_panel_strict_lp_benchmark_frame(
        None,
        OfflineDflPanelStrictLpBenchmarkAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
        ),
        benchmark,
        offline_panel,
    )
    decision_target_panel = offline_dfl_decision_target_panel_frame(
        None,
        OfflineDflDecisionTargetAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            inner_validation_fraction=0.34,
            spread_scale_grid_csv="1.0",
            mean_shift_grid_uah_mwh_csv="0.0",
            include_panel_v2_bias_options_csv="true",
        ),
        benchmark,
        offline_panel,
    )
    decision_target_strict = offline_dfl_decision_target_strict_lp_benchmark_frame(
        None,
        OfflineDflDecisionTargetAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            inner_validation_fraction=0.34,
            spread_scale_grid_csv="1.0",
            mean_shift_grid_uah_mwh_csv="0.0",
            include_panel_v2_bias_options_csv="true",
        ),
        benchmark,
        offline_panel,
        decision_target_panel,
    )
    action_target_panel = offline_dfl_action_target_panel_frame(
        None,
        OfflineDflActionTargetAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            inner_validation_fraction=0.34,
            charge_hour_count_grid_csv="1",
            discharge_hour_count_grid_csv="1",
            action_spread_grid_uah_mwh_csv="1000.0",
            include_panel_v2_bias_options_csv="false",
            include_decision_v3_correction_options_csv="false",
        ),
        benchmark,
        offline_panel,
        decision_target_panel,
    )
    action_target_strict = offline_dfl_action_target_strict_lp_benchmark_frame(
        None,
        OfflineDflActionTargetAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            inner_validation_fraction=0.34,
            charge_hour_count_grid_csv="1",
            discharge_hour_count_grid_csv="1",
            action_spread_grid_uah_mwh_csv="1000.0",
            include_panel_v2_bias_options_csv="false",
            include_decision_v3_correction_options_csv="false",
        ),
        benchmark,
        offline_panel,
        decision_target_panel,
        action_target_panel,
    )
    trajectory_value_panel = dfl_trajectory_value_candidate_panel_frame(
        None,
        OfflineDflTrajectoryValueSelectorAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
        ),
        benchmark,
        strict_panel,
        decision_target_strict,
        action_target_strict,
        offline_panel,
        decision_target_panel,
        action_target_panel,
    )
    trajectory_value_selector = dfl_trajectory_value_selector_frame(
        None,
        OfflineDflTrajectoryValueSelectorAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            min_final_holdout_tenant_anchor_count_per_source_model=2,
        ),
        trajectory_value_panel,
    )
    trajectory_value_strict = dfl_trajectory_value_selector_strict_lp_benchmark_frame(
        None,
        OfflineDflTrajectoryValueSelectorAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            final_validation_anchor_count_per_tenant=2,
            max_train_anchors_per_tenant=3,
            min_final_holdout_tenant_anchor_count_per_source_model=2,
        ),
        trajectory_value_panel,
        trajectory_value_selector,
    )
    trajectory_ranker_config = DflTrajectoryFeatureRankerAssetConfig(
        tenant_ids_csv="client_003_dnipro_factory",
        forecast_model_names_csv="tft_silver_v0",
        final_validation_anchor_count_per_tenant=2,
        perturb_spread_scale_grid_csv="1.0",
        perturb_mean_shift_grid_uah_mwh_csv="100.0",
        min_final_holdout_tenant_anchor_count_per_source_model=2,
    )
    schedule_library = dfl_schedule_candidate_library_frame(
        None,
        trajectory_ranker_config,
        benchmark,
        trajectory_value_panel,
    )
    trajectory_ranker = dfl_trajectory_feature_ranker_frame(
        None,
        trajectory_ranker_config,
        schedule_library,
    )
    trajectory_ranker_strict = dfl_trajectory_feature_ranker_strict_lp_benchmark_frame(
        None,
        trajectory_ranker_config,
        schedule_library,
        trajectory_ranker,
    )
    strict_challenger_config = DflStrictChallengerAssetConfig(
        blend_weights_csv="0.5",
        residual_min_prior_anchors=1,
        min_final_holdout_tenant_anchor_count_per_source_model=1,
    )
    pipeline_audit = dfl_pipeline_integrity_audit_frame(
        None, benchmark, schedule_library
    )
    truth_audit = forecast_pipeline_truth_audit_frame(
        None,
        DflForecastPipelineTruthAuditAssetConfig(price_cap_uah_mwh=16_000.0),
        benchmark,
    )
    schedule_library_v2 = dfl_schedule_candidate_library_v2_frame(
        None,
        strict_challenger_config,
        schedule_library,
    )
    non_strict_upper_bound = dfl_non_strict_oracle_upper_bound_frame(
        None,
        strict_challenger_config,
        schedule_library_v2,
    )
    strict_autopsy = dfl_strict_baseline_autopsy_frame(
        None,
        schedule_library_v2,
        non_strict_upper_bound,
    )
    strict_failure_selector_config = DflStrictFailureSelectorAssetConfig(
        tenant_ids_csv="client_003_dnipro_factory",
        forecast_model_names_csv="tft_silver_v0",
        switch_threshold_grid_uah_csv="0.0,50.0",
        min_prior_anchor_count=1,
        min_final_holdout_tenant_anchor_count_per_source_model=2,
    )
    strict_failure_selector = dfl_strict_failure_selector_frame(
        None,
        strict_failure_selector_config,
        schedule_library_v2,
        strict_autopsy,
    )
    strict_failure_selector_strict = (
        dfl_strict_failure_selector_strict_lp_benchmark_frame(
            None,
            strict_failure_selector_config,
            schedule_library_v2,
            strict_failure_selector,
        )
    )
    strict_failure_selector_robustness = dfl_strict_failure_selector_robustness_frame(
        None,
        DflStrictFailureSelectorRobustnessAssetConfig(
            tenant_ids_csv="client_003_dnipro_factory",
            forecast_model_names_csv="tft_silver_v0",
            validation_window_count=1,
            validation_anchor_count=2,
            min_prior_anchors_before_window=1,
            min_prior_anchor_count=1,
            switch_threshold_grid_uah_csv="0.0,50.0",
            min_robust_passing_windows=1,
            min_validation_tenant_anchor_count_per_source_model=2,
        ),
        schedule_library_v2,
    )
    forecast_forensics = forecast_candidate_forensics_frame(None, benchmark)
    afl_panel = afl_training_panel_frame(
        None,
        AflTrainingPanelAssetConfig(final_holdout_anchor_count_per_tenant=2),
        pl.DataFrame(),
        benchmark,
        pl.DataFrame(),
    )

    assert ensemble.height == 5
    assert strategy_store.evaluation_frame.height == 117
    assert training.height == 20
    assert dfl_store.training_frame.height == 20
    assert training_examples.height == 15
    assert dfl_store.training_example_frame.height == 15
    assert coverage_audit.height == 1
    assert action_labels.height == 5
    assert dfl_store.action_label_frame.height == 5
    assert action_classifier.height == 4
    assert action_classifier.select("claim_scope").to_series().unique().to_list() == [
        "dfl_action_classifier_baseline_not_full_dfl"
    ]
    assert action_classifier.select(
        "promotion_status"
    ).to_series().unique().to_list() == [
        "blocked_classification_only_no_strict_lp_value"
    ]
    assert action_classifier_strict.height == 4
    assert action_classifier_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == ["dfl_action_classifier_strict_lp_projection"]
    assert (
        "dfl_action_classifier_v0_tft_silver_v0"
        in action_classifier_strict["forecast_model_name"].unique().to_list()
    )
    assert value_aware_action_classifier_strict.height == 4
    assert value_aware_action_classifier_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == [
        "dfl_value_aware_action_classifier_strict_lp_projection"
    ]
    assert (
        "dfl_value_aware_action_classifier_v1_tft_silver_v0"
        in value_aware_action_classifier_strict["forecast_model_name"]
        .unique()
        .to_list()
    )
    assert failure_analysis.height == 2
    assert failure_analysis.select("claim_scope").to_series().unique().to_list() == [
        "dfl_action_classifier_failure_analysis_not_full_dfl"
    ]
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
    }.issubset(
        set(horizon_calibrated_benchmark["forecast_model_name"].unique().to_list())
    )
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
    assert offline_experiment.select(
        "not_market_execution"
    ).to_series().unique().to_list() == [True]
    assert offline_panel.height == 1
    assert offline_panel.select("claim_scope").to_series().unique().to_list() == [
        "offline_dfl_panel_experiment_not_full_dfl"
    ]
    assert offline_panel.select(
        "not_market_execution"
    ).to_series().unique().to_list() == [True]
    assert strict_panel.height == 6
    assert strict_panel.select("strategy_kind").to_series().unique().to_list() == [
        "offline_dfl_panel_strict_lp_benchmark"
    ]
    assert (
        "offline_dfl_panel_v2_tft_silver_v0"
        in strict_panel["forecast_model_name"].unique().to_list()
    )
    assert decision_target_panel.height == 1
    assert decision_target_panel.select(
        "claim_scope"
    ).to_series().unique().to_list() == ["offline_dfl_decision_target_v3_not_full_dfl"]
    assert decision_target_strict.height == 8
    assert decision_target_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == [
        "offline_dfl_decision_target_strict_lp_benchmark"
    ]
    assert (
        "offline_dfl_decision_target_v3_tft_silver_v0"
        in decision_target_strict["forecast_model_name"].unique().to_list()
    )
    assert action_target_panel.height == 1
    assert action_target_panel.select("claim_scope").to_series().unique().to_list() == [
        "offline_dfl_action_target_v4_not_full_dfl"
    ]
    assert action_target_strict.height == 10
    assert action_target_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == [
        "offline_dfl_action_target_strict_lp_benchmark"
    ]
    assert (
        "offline_dfl_action_target_v4_tft_silver_v0"
        in action_target_strict["forecast_model_name"].unique().to_list()
    )
    assert trajectory_value_panel.height == 10
    assert trajectory_value_panel.select(
        "claim_scope"
    ).to_series().unique().to_list() == [
        "dfl_trajectory_value_candidate_panel_not_full_dfl"
    ]
    assert trajectory_value_selector.height == 1
    assert trajectory_value_selector.select(
        "claim_scope"
    ).to_series().unique().to_list() == [
        "dfl_trajectory_value_selector_v1_not_full_dfl"
    ]
    assert trajectory_value_strict.height >= 4
    assert trajectory_value_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == [
        "dfl_trajectory_value_selector_strict_lp_benchmark"
    ]
    assert schedule_library.height > 0
    assert schedule_library.select("claim_scope").to_series().unique().to_list() == [
        "dfl_schedule_candidate_library_not_full_dfl"
    ]
    assert trajectory_ranker.height == 1
    assert trajectory_ranker.select("claim_scope").to_series().unique().to_list() == [
        "dfl_trajectory_feature_ranker_v1_not_full_dfl"
    ]
    assert trajectory_ranker_strict.height == 6
    assert trajectory_ranker_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == [
        "dfl_trajectory_feature_ranker_strict_lp_benchmark"
    ]
    assert (
        "dfl_trajectory_feature_ranker_v1_tft_silver_v0"
        in trajectory_ranker_strict["forecast_model_name"].unique().to_list()
    )
    assert pipeline_audit.height == 1
    assert pipeline_audit.select("claim_scope").to_series().unique().to_list() == [
        "dfl_pipeline_integrity_audit_not_full_dfl"
    ]
    assert truth_audit.height == 3
    assert truth_audit.select("claim_scope").to_series().unique().to_list() == [
        "dfl_forecast_pipeline_truth_audit_not_full_dfl"
    ]
    assert schedule_library_v2.height > schedule_library.height
    assert (
        "strict_raw_blend_v2"
        in schedule_library_v2["candidate_family"].unique().to_list()
    )
    assert non_strict_upper_bound.height >= 2
    assert non_strict_upper_bound.select(
        "claim_scope"
    ).to_series().unique().to_list() == [
        "dfl_non_strict_oracle_upper_bound_not_full_dfl"
    ]
    assert strict_autopsy.height == non_strict_upper_bound.height
    assert strict_autopsy.select("claim_scope").to_series().unique().to_list() == [
        "dfl_strict_baseline_autopsy_not_full_dfl"
    ]
    assert strict_failure_selector.height == 1
    assert strict_failure_selector.select(
        "claim_scope"
    ).to_series().unique().to_list() == ["dfl_strict_failure_selector_v1_not_full_dfl"]
    assert strict_failure_selector_strict.height >= 4
    assert strict_failure_selector_strict.select(
        "strategy_kind"
    ).to_series().unique().to_list() == [
        "dfl_strict_failure_selector_strict_lp_benchmark"
    ]
    assert (
        "dfl_strict_failure_selector_v1_tft_silver_v0"
        in strict_failure_selector_strict["forecast_model_name"].unique().to_list()
    )
    assert strict_failure_selector_robustness.height == 1
    assert strict_failure_selector_robustness.select(
        "claim_scope"
    ).to_series().unique().to_list() == [
        "dfl_strict_failure_selector_robustness_not_full_dfl"
    ]
    assert forecast_forensics.height == 3
    assert (
        "compact_silver_candidate"
        in forecast_forensics["candidate_kind"].unique().to_list()
    )
    assert afl_panel.height == benchmark.height
    assert afl_panel.select("claim_scope").to_series().unique().to_list() == [
        "arbitrage_focused_learning_panel_not_full_dfl"
    ]


def test_dfl_forecast_v1_assets_materialize_panel_and_strict_rows(monkeypatch) -> None:
    strategy_store = InMemoryStrategyEvaluationStore()
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.get_strategy_evaluation_store",
        lambda: strategy_store,
    )
    benchmark = _benchmark_frame()
    config = DflForecastDflV1AssetConfig(
        tenant_ids_csv="client_003_dnipro_factory",
        forecast_model_names_csv="tft_silver_v0",
        final_validation_anchor_count_per_tenant=2,
        max_train_anchors_per_tenant=3,
        inner_validation_fraction=0.34,
        epoch_count=1,
        learning_rate=10.0,
    )

    panel = dfl_forecast_dfl_v1_panel_frame(None, config, benchmark)
    strict = dfl_forecast_dfl_v1_strict_lp_benchmark_frame(
        None, config, benchmark, panel
    )

    assert panel.height == 1
    assert (
        strict.filter(
            pl.col("forecast_model_name").str.starts_with("dfl_forecast_dfl_v1_")
        ).height
        == 2
    )
    assert strategy_store.evaluation_frame.height == strict.height


def test_trajectory_value_strict_asset_uses_fresh_generated_at(monkeypatch) -> None:
    captured: dict[str, object] = {}
    strategy_store = InMemoryStrategyEvaluationStore()

    def fake_builder(
        candidate_panel: pl.DataFrame,
        selector_frame: pl.DataFrame,
        *,
        generated_at: datetime | None = None,
    ) -> pl.DataFrame:
        captured["generated_at"] = generated_at
        return pl.DataFrame(
            [
                {
                    "evaluation_id": "trajectory-value-selector:test",
                    "tenant_id": "client_003_dnipro_factory",
                    "forecast_model_name": "dfl_trajectory_value_selector_v1_tft_silver_v0",
                    "strategy_kind": "dfl_trajectory_value_selector_strict_lp_benchmark",
                    "market_venue": "DAM",
                    "anchor_timestamp": datetime(2026, 4, 29, 23),
                    "generated_at": datetime(2026, 5, 8, 11),
                    "horizon_hours": 2,
                    "starting_soc_fraction": 0.5,
                    "starting_soc_source": "tenant_default",
                    "decision_value_uah": 900.0,
                    "forecast_objective_value_uah": 900.0,
                    "oracle_value_uah": 1000.0,
                    "regret_uah": 100.0,
                    "regret_ratio": 0.1,
                    "total_degradation_penalty_uah": 1.0,
                    "total_throughput_mwh": 0.1,
                    "committed_action": "HOLD",
                    "committed_power_mw": 0.0,
                    "rank_by_regret": 1,
                    "evaluation_payload": {
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                    },
                }
            ]
        )

    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.build_dfl_trajectory_value_selector_strict_lp_benchmark_frame",
        fake_builder,
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.evaluate_dfl_trajectory_value_selector_gate",
        lambda strict_frame, **kwargs: PromotionGateResult(
            passed=False,
            decision="diagnostic_pass_production_blocked",
            description="test",
            metrics={"development_gate_passed": True},
        ),
    )
    monkeypatch.setattr(
        "smart_arbitrage.assets.gold.dfl_research.get_strategy_evaluation_store",
        lambda: strategy_store,
    )

    dfl_trajectory_value_selector_strict_lp_benchmark_frame(
        None,
        OfflineDflTrajectoryValueSelectorAssetConfig(
            forecast_model_names_csv="tft_silver_v0"
        ),
        pl.DataFrame({"generated_at": [datetime(2026, 1, 1)]}),
        pl.DataFrame(),
    )

    assert captured["generated_at"] is None
