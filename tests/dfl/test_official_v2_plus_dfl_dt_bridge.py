from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.offline_dt_candidate import build_dfl_offline_dt_candidate_frame
from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    build_dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame,
    build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame,
)
from smart_arbitrage.dfl.residual_schedule_value import (
    build_dfl_residual_schedule_value_model_frame,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    build_dfl_schedule_value_learner_v2_frame,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    build_dfl_schedule_value_learner_v2_plus_frame,
    build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame,
)
from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import (
    evaluate_dfl_v2_plus_dfl_dt_bridge_gate,
    validate_dfl_v2_plus_dfl_dt_bridge_evidence,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)
GENERATED_AT = datetime(2026, 5, 17, 13)


def test_official_v2_plus_trajectory_uses_train_prior_teacher_labels_only() -> None:
    library = _candidate_library(
        residual_train_regret=75.0,
        residual_final_regret=210.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    trajectory = build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    mutated = build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame(
        _mutate_final_rows(library, regret=90.0),
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )

    assert set(
        trajectory.filter(pl.col("split_name") == "final_holdout")[
            "teacher_label_allowed"
        ].unique()
    ) == {False}
    assert set(
        trajectory.filter(pl.col("split_name") != "final_holdout")[
            "teacher_label_allowed"
        ].unique()
    ) == {True}
    feature_columns = [
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "candidate_family",
        "candidate_model_name",
        "horizon_step",
        "feature_prior_anchor_count",
        "feature_prior_strict_mean_regret_uah",
        "feature_prior_raw_mean_regret_uah",
        "feature_prior_best_non_strict_mean_regret_uah",
    ]
    assert (
        trajectory.filter(pl.col("split_name") == "final_holdout")
        .select(feature_columns)
        .to_dicts()
        == mutated.filter(pl.col("split_name") == "final_holdout")
        .select(feature_columns)
        .to_dicts()
    )
    assert (
        trajectory.filter(pl.col("split_name") == "final_holdout")[
            "episode_regret_uah"
        ].mean()
        != mutated.filter(pl.col("split_name") == "final_holdout")[
            "episode_regret_uah"
        ].mean()
    )


def test_official_v2_plus_bridge_blocks_when_challengers_do_not_beat_v2_plus() -> None:
    bridge_frame = _official_bridge_frame(
        residual_train_regret=75.0,
        residual_final_regret=210.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )

    evidence = validate_dfl_v2_plus_dfl_dt_bridge_evidence(
        bridge_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )
    gate = evaluate_dfl_v2_plus_dfl_dt_bridge_gate(
        bridge_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )

    assert evidence.passed is True
    assert gate.passed is False
    assert gate.decision == "blocked"
    assert bridge_frame["strategy_kind"].unique().to_list() == [
        DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND
    ]
    assert "filtered_behavior_cloning_reference" in set(
        bridge_frame["selection_role"].unique().to_list()
    )
    assert set(bridge_frame["not_market_execution"].unique().to_list()) == {True}


def test_official_v2_plus_bridge_passes_only_when_challenger_beats_v2_plus() -> None:
    bridge_frame = _official_bridge_frame(
        residual_train_regret=75.0,
        residual_final_regret=210.0,
        v2_plus_train_regret=30.0,
        v2_plus_final_regret=180.0,
    )
    bridge_frame = _mutate_bridge_role_regret(
        bridge_frame,
        roles=("residual_dfl_reference", "residual_dt_fallback_reference"),
        regret=120.0,
    )

    gate = evaluate_dfl_v2_plus_dfl_dt_bridge_gate(
        bridge_frame,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        min_validation_tenant_anchor_count=90,
    )

    assert gate.passed is True
    assert gate.metrics["best_mean_regret_improvement_ratio_vs_v2_plus"] > 0.0
    assert gate.metrics["market_execution_enabled"] is False


def _official_bridge_frame(
    *,
    residual_train_regret: float,
    residual_final_regret: float,
    v2_plus_train_regret: float,
    v2_plus_final_regret: float,
) -> pl.DataFrame:
    library = _candidate_library(
        residual_train_regret=residual_train_regret,
        residual_final_regret=residual_final_regret,
        v2_plus_train_regret=v2_plus_train_regret,
        v2_plus_final_regret=v2_plus_final_regret,
    )
    trajectory = build_dfl_official_global_panel_v2_plus_trajectory_dataset_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    residual_model = build_dfl_residual_schedule_value_model_frame(
        trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    dt_candidate = build_dfl_offline_dt_candidate_frame(
        trajectory,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        max_epochs=1,
        random_seed=17,
    )
    v2_model = build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    v2_plus_model = build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
    )
    v2_plus_strict = build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=GENERATED_AT,
    )
    return build_dfl_official_global_panel_v2_plus_dfl_dt_bridge_strict_lp_benchmark_frame(
        library,
        residual_model,
        dt_candidate,
        v2_plus_strict,
        source_model_names=OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
        generated_at=GENERATED_AT,
    )


def _candidate_library(
    *,
    residual_train_regret: float,
    residual_final_regret: float,
    v2_plus_train_regret: float,
    v2_plus_final_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    train_anchor_count = 3
    final_anchor_count = 18
    for tenant_id in TENANTS:
        for source_model_name in OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS:
            for anchor_index in range(train_anchor_count + final_anchor_count):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                split_name = (
                    "final_holdout"
                    if anchor_index >= train_anchor_count
                    else "train_selection"
                )
                rows.extend(
                    [
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_control",
                            candidate_model_name="strict_similar_day",
                            anchor=anchor,
                            split_name=split_name,
                            regret=310.0,
                            forecast_prices=(1000.0, 5000.0),
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=310.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="raw_source",
                            candidate_model_name=source_model_name,
                            anchor=anchor,
                            split_name=split_name,
                            regret=620.0,
                            forecast_prices=(5000.0, 1000.0),
                            dispatch=(0.0, 1.0),
                            prior_family_mean_regret=620.0,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="strict_prior_residual_v2",
                            candidate_model_name=f"dfl_residual_family_{source_model_name}",
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                residual_final_regret
                                if split_name == "final_holdout"
                                else residual_train_regret
                            ),
                            forecast_prices=(900.0, 6500.0),
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=residual_train_regret,
                        ),
                        _candidate_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            candidate_family="rank_extrema_perturbation_v2_plus",
                            candidate_model_name=(
                                "dfl_schedule_library_v2_plus_rank_extrema_"
                                f"{source_model_name}"
                            ),
                            anchor=anchor,
                            split_name=split_name,
                            regret=(
                                v2_plus_final_regret
                                if split_name == "final_holdout"
                                else v2_plus_train_regret
                            ),
                            forecast_prices=(700.0, 7000.0),
                            dispatch=(1.0, -1.0),
                            prior_family_mean_regret=v2_plus_train_regret,
                        ),
                    ]
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    forecast_prices: tuple[float, ...],
    dispatch: tuple[float, ...],
    prior_family_mean_regret: float,
) -> dict[str, object]:
    actual_prices = (1000.0, 5000.0)
    horizon = [
        {
            "step_index": step_index,
            "interval_start": (anchor + timedelta(hours=step_index + 1)).isoformat(),
            "forecast_price_uah_mwh": forecast_prices[step_index],
            "actual_price_uah_mwh": actual_prices[step_index],
            "net_power_mw": dispatch[step_index],
            "soc_fraction": 0.5 + step_index * 0.01,
            "degradation_penalty_uah": 0.0,
        }
        for step_index in range(len(forecast_prices))
    ]
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": len(forecast_prices),
        "forecast_price_uah_mwh_vector": list(forecast_prices),
        "actual_price_uah_mwh_vector": list(actual_prices),
        "dispatch_mw_vector": list(dispatch),
        "soc_fraction_vector": [0.5, 0.51],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 950.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "forecast_spread_uah_mwh": max(forecast_prices) - min(forecast_prices),
        "actual_spread_uah_mwh": max(actual_prices) - min(actual_prices),
        "forecast_top_k_actual_overlap": 1.0,
        "forecast_bottom_k_actual_overlap": 1.0,
        "peak_index_abs_error": 0.0,
        "trough_index_abs_error": 0.0,
        "soc_min_slack_fraction": 0.4,
        "prior_family_mean_regret_uah": prior_family_mean_regret,
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "claim_scope": "dfl_schedule_candidate_library_v2_plus_not_full_dfl",
        "evaluation_payload": {
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "source_forecast_model_name": source_model_name,
            "candidate_family": candidate_family,
            "candidate_model_name": candidate_model_name,
            "safety_violation_count": 0,
            "horizon": horizon,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        },
    }


def _mutate_final_rows(frame: pl.DataFrame, *, regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if str(row["split_name"]) == "final_holdout":
            copied["regret_uah"] = regret
            copied["decision_value_uah"] = 1000.0 - regret
            copied["actual_price_uah_mwh_vector"] = [1200.0, 4500.0]
        rows.append(copied)
    return pl.DataFrame(rows)


def _mutate_bridge_role_regret(
    frame: pl.DataFrame,
    *,
    roles: tuple[str, ...],
    regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for row in frame.iter_rows(named=True):
        copied = dict(row)
        if str(row["selection_role"]) in roles:
            copied["regret_uah"] = regret
            copied["decision_value_uah"] = 1000.0 - regret
            payload = dict(copied["evaluation_payload"])
            payload["regret_uah"] = regret
            payload["decision_value_uah"] = 1000.0 - regret
            copied["evaluation_payload"] = payload
        rows.append(copied)
    return pl.DataFrame(rows)
