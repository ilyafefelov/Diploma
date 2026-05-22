from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.oracle_gap_safe_switch import (
    ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE,
    build_dfl_oracle_gap_safe_switch_feature_panel_frame,
    build_dfl_oracle_gap_safe_switch_label_frame,
    build_dfl_oracle_gap_safe_switch_rolling_robustness_frame,
    build_dfl_oracle_gap_safe_switch_scorer_frame,
    build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame,
    evaluate_dfl_oracle_gap_safe_switch_gate,
)

TENANTS: tuple[str, ...] = ("tenant_a", "tenant_b")
SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
GENERATED_AT = datetime(2026, 5, 22, 9)
FIRST_ANCHOR = datetime(2026, 1, 1, 12)


def test_oracle_gap_labels_identify_safe_switch_and_tail_risk() -> None:
    frames = _oracle_gap_inputs(
        train_alt_regret=70.0,
        final_alt_regret=80.0,
        tail_alt_regret=290.0,
    )

    labels = build_dfl_oracle_gap_safe_switch_label_frame(**frames)

    classes = set(labels["oracle_gap_teacher_class"].unique().to_list())
    assert {"v2_plus_best", "safe_switch_win", "tail_risk_loss"}.issubset(
        classes
    )
    assert labels.filter(pl.col("oracle_gap_teacher_class") == "safe_switch_win")[
        "label_regret_delta_vs_v2_plus_uah"
    ].max() < 0.0
    assert set(labels["market_execution_enabled"].unique().to_list()) == {False}


def test_oracle_gap_feature_panel_is_prior_safe_when_final_labels_mutate() -> None:
    base = _oracle_gap_inputs(train_alt_regret=70.0, final_alt_regret=80.0)
    mutated = _oracle_gap_inputs(train_alt_regret=70.0, final_alt_regret=400.0)
    base_panel = build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        build_dfl_oracle_gap_safe_switch_label_frame(**base)
    )
    mutated_panel = build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        build_dfl_oracle_gap_safe_switch_label_frame(**mutated)
    )

    feature_columns = sorted(
        column for column in base_panel.columns if column.startswith("selector_feature_")
    )
    assert feature_columns
    assert base_panel.select(feature_columns).to_dicts() == mutated_panel.select(
        feature_columns
    ).to_dicts()
    assert base_panel.select("label_regret_delta_vs_v2_plus_uah").to_dicts() != (
        mutated_panel.select("label_regret_delta_vs_v2_plus_uah").to_dicts()
    )


def test_oracle_gap_scorer_switches_only_when_prior_signal_is_safe() -> None:
    frames = _oracle_gap_inputs(train_alt_regret=70.0, final_alt_regret=80.0)
    panel = build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        build_dfl_oracle_gap_safe_switch_label_frame(**frames)
    )
    scorer = build_dfl_oracle_gap_safe_switch_scorer_frame(
        panel,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.25,
    )
    strict = build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame(
        panel,
        scorer,
        frames["schedule_value_v2_plus_strict_frame"],
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_oracle_gap_safe_switch_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert set(scorer["fallback_to_v2_plus"].to_list()) == {False}
    assert set(
        strict.filter(pl.col("selection_role") == ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE)[
            "regret_uah"
        ].to_list()
    ) == {80.0}
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_oracle_gap_scorer_falls_back_for_tail_risk_profile() -> None:
    frames = _oracle_gap_inputs(train_alt_regret=290.0, final_alt_regret=20.0)
    panel = build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        build_dfl_oracle_gap_safe_switch_label_frame(**frames)
    )
    scorer = build_dfl_oracle_gap_safe_switch_scorer_frame(
        panel,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.25,
    )
    strict = build_dfl_oracle_gap_safe_switch_strict_lp_benchmark_frame(
        panel,
        scorer,
        frames["schedule_value_v2_plus_strict_frame"],
        generated_at=GENERATED_AT,
    )

    assert set(scorer["fallback_to_v2_plus"].to_list()) == {True}
    assert strict.filter(pl.col("selection_role") == ORACLE_GAP_SAFE_SWITCH_SELECTION_ROLE)[
        "regret_uah"
    ].to_list() == [120.0] * (len(TENANTS) * 2)


def test_oracle_gap_rolling_robustness_retrains_before_each_window() -> None:
    frames = _oracle_gap_inputs(
        train_alt_regret=70.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )
    panel = build_dfl_oracle_gap_safe_switch_feature_panel_frame(
        build_dfl_oracle_gap_safe_switch_label_frame(**frames)
    )

    rolling = build_dfl_oracle_gap_safe_switch_rolling_robustness_frame(
        panel,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert set(rolling["market_execution_enabled"].to_list()) == {False}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2


def _oracle_gap_inputs(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    tail_alt_regret: float = 290.0,
    train_anchor_count: int = 3,
    final_anchor_count: int = 2,
) -> dict[str, pl.DataFrame]:
    library = _candidate_library(
        train_alt_regret=train_alt_regret,
        final_alt_regret=final_alt_regret,
        tail_alt_regret=tail_alt_regret,
        train_anchor_count=train_anchor_count,
        final_anchor_count=final_anchor_count,
    )
    v2_model = v2.build_dfl_schedule_value_learner_v2_frame(
        library,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=final_anchor_count,
    )
    v2_plus_model = v2_plus.build_dfl_schedule_value_learner_v2_plus_frame(
        library,
        v2_model,
        tenant_ids=TENANTS,
        forecast_model_names=(SOURCE,),
        final_validation_anchor_count_per_tenant=final_anchor_count,
        min_prior_mean_improvement_ratio_vs_v2=0.01,
    )
    strict = v2_plus.build_dfl_schedule_value_learner_v2_plus_strict_lp_benchmark_frame(
        library,
        v2_plus_model,
        v2_model,
        generated_at=GENERATED_AT,
    )
    audit = v2_plus.build_dfl_schedule_value_learner_v2_plus_oracle_gap_audit_frame(
        library,
        strict,
        source_model_names=(SOURCE,),
    )
    return {
        "schedule_candidate_library_frame": library,
        "schedule_value_v2_plus_frame": v2_plus_model,
        "schedule_value_v2_frame": v2_model,
        "schedule_value_v2_plus_strict_frame": strict,
        "oracle_gap_audit_frame": audit,
    }


def _candidate_library(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    tail_alt_regret: float,
    train_anchor_count: int,
    final_anchor_count: int,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant in TENANTS:
        anchors = [
            (FIRST_ANCHOR + timedelta(hours=index), "train_selection")
            for index in range(train_anchor_count)
        ] + [
            (
                FIRST_ANCHOR + timedelta(hours=train_anchor_count + index),
                "final_holdout",
            )
            for index in range(final_anchor_count)
        ]
        for anchor, split_name in anchors:
            alt_regret = train_alt_regret if split_name == "train_selection" else final_alt_regret
            for family, model_name, regret, prior_mean, dispatch in (
                (
                    "strict_control",
                    "strict_similar_day",
                    300.0,
                    130.0,
                    [0.0, 0.0, 0.0, 0.0],
                ),
                (
                    v2.CANDIDATE_FAMILY_RAW,
                    "raw_nbeatsx",
                    500.0,
                    140.0,
                    [0.1, 0.0, -0.1, 0.0],
                ),
                (
                    "rank_extrema_perturbation_v2_plus",
                    "rank_extrema_safe",
                    120.0,
                    60.0,
                    [0.1, -0.1, 0.0, 0.0],
                ),
                (
                    "robust_spread_penalty_v2_plus",
                    "robust_safe_switch",
                    alt_regret,
                    80.0,
                    [0.2, -0.2, 0.0, 0.0],
                ),
                (
                    "strict_neighborhood_shift_v2_plus",
                    "tail_risk_shift",
                    tail_alt_regret,
                    90.0,
                    [0.4, -0.4, 0.0, 0.0],
                ),
            ):
                rows.append(
                    _candidate_row(
                        tenant=tenant,
                        anchor=anchor,
                        split_name=split_name,
                        family=family,
                        model_name=model_name,
                        regret=regret,
                        prior_family_mean_regret=prior_mean,
                        dispatch=dispatch,
                    )
                )
    return pl.DataFrame(rows)


def _candidate_row(
    *,
    tenant: str,
    anchor: datetime,
    split_name: str,
    family: str,
    model_name: str,
    regret: float,
    prior_family_mean_regret: float,
    dispatch: list[float],
) -> dict[str, object]:
    forecast = [1000.0, 3000.0, 1500.0, 4000.0]
    actual = [1100.0, 2800.0, 1600.0, 3900.0]
    soc = [0.5 + sum(dispatch[: index + 1]) * 0.05 for index in range(len(dispatch))]
    return {
        "tenant_id": tenant,
        "source_model_name": SOURCE,
        "candidate_family": family,
        "candidate_model_name": model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 4,
        "forecast_price_uah_mwh_vector": forecast,
        "actual_price_uah_mwh_vector": actual,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "prior_family_mean_regret_uah": prior_family_mean_regret,
        "forecast_spread_uah_mwh": max(forecast) - min(forecast),
        "total_degradation_penalty_uah": abs(sum(dispatch)) * 10.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "soc_min_slack_fraction": min(soc),
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": {
            "dispatch_mw": dispatch,
            "soc_fraction": soc,
            "candidate_family": family,
            "candidate_model_name": model_name,
        },
    }
