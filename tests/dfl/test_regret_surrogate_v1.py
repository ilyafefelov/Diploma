from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.regret_surrogate_v1 import (
    REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE,
    REGRET_SURROGATE_SELECTION_ROLE,
    STRICT_REFERENCE_ROLE,
    V2_PLUS_REFERENCE_ROLE,
    build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame,
    build_dfl_regret_surrogate_contextual_candidate_value_v2_frame,
    build_dfl_regret_surrogate_contextual_rolling_robustness_frame,
    build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame,
    build_dfl_regret_surrogate_safe_switch_context_audit_frame,
    build_dfl_regret_surrogate_candidate_value_v1_frame,
    build_dfl_regret_surrogate_forecast_correction_v1_frame,
    build_dfl_regret_surrogate_rolling_robustness_frame,
    build_dfl_regret_surrogate_strict_lp_benchmark_frame,
    build_dfl_regret_surrogate_teacher_label_panel_v2_frame,
    build_dfl_v2_plus_learning_limit_audit_frame,
    evaluate_dfl_regret_surrogate_gate,
)

TENANTS = ("tenant_a", "tenant_b")
SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
GENERATED_AT = datetime(2026, 5, 22, 12)
FIRST_ANCHOR = datetime(2026, 1, 1, 12)


def test_learning_limit_audit_reports_candidate_ceiling() -> None:
    panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)

    audit = build_dfl_v2_plus_learning_limit_audit_frame(panel)

    assert audit.height == len(TENANTS) * 5
    assert set(audit["learning_limit_failure_mode"].to_list()) == {
        "selector_too_conservative"
    }
    assert set(audit["candidate_universe_can_beat_v2_plus_gate"].to_list()) == {True}
    assert set(audit["recommended_next_branch"].to_list()) == {
        "regret_surrogate_dfl"
    }
    assert set(audit["market_execution_enabled"].to_list()) == {False}


def test_learning_limit_audit_stops_when_oracle_switch_cannot_clear_gate() -> None:
    panel = _candidate_panel(train_alt_regret=117.0, final_alt_regret=117.0)

    audit = build_dfl_v2_plus_learning_limit_audit_frame(panel)

    assert set(audit["candidate_universe_can_beat_v2_plus_gate"].to_list()) == {False}
    assert set(audit["recommended_next_branch"].to_list()) == {
        "data_or_candidate_backfill"
    }


def test_teacher_panel_keeps_final_features_prior_only_when_labels_mutate() -> None:
    base_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    mutated_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=360.0)

    base = build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        base_panel,
        build_dfl_v2_plus_learning_limit_audit_frame(base_panel),
    )
    mutated = build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        mutated_panel,
        build_dfl_v2_plus_learning_limit_audit_frame(mutated_panel),
    )
    feature_columns = sorted(
        column for column in base.columns if column.startswith("selector_feature_")
    )

    assert feature_columns
    assert base.select(feature_columns).to_dicts() == mutated.select(
        feature_columns
    ).to_dicts()
    assert base.select("label_regret_delta_vs_v2_plus_uah").to_dicts() != (
        mutated.select("label_regret_delta_vs_v2_plus_uah").to_dicts()
    )
    assert base.filter(pl.col("split_name") == "final_holdout").select(
        pl.col("is_training_row").any()
    ).item() is False
    assert set(base["raw_hourly_action_imitation"].unique().to_list()) == {False}


def test_regret_surrogate_can_select_learnable_safe_candidate() -> None:
    panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher = _teacher_panel(panel)
    correction = build_dfl_regret_surrogate_forecast_correction_v1_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
    )
    candidate_value = build_dfl_regret_surrogate_candidate_value_v1_frame(
        teacher,
        correction,
    )
    strict = build_dfl_regret_surrogate_strict_lp_benchmark_frame(
        teacher,
        candidate_value,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_regret_surrogate_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert set(correction["fallback_to_v2_plus"].to_list()) == {False}
    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {2}
    assert {
        STRICT_REFERENCE_ROLE,
        V2_PLUS_REFERENCE_ROLE,
        REGRET_SURROGATE_SELECTION_ROLE,
    }.issubset(set(strict["selection_role"].unique().to_list()))
    assert {
        "starting_soc_fraction",
        "starting_soc_source",
        "committed_action",
        "committed_power_mw",
        "rank_by_regret",
    }.issubset(set(strict.columns))
    assert strict.filter(pl.col("selection_role") == REGRET_SURROGATE_SELECTION_ROLE)[
        "regret_uah"
    ].to_list() == [80.0] * (len(TENANTS) * 2)
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_regret_surrogate_falls_back_on_tail_risk_even_if_final_would_win() -> None:
    panel = _candidate_panel(train_alt_regret=320.0, final_alt_regret=20.0)
    teacher = _teacher_panel(panel)
    correction = build_dfl_regret_surrogate_forecast_correction_v1_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_prior_safe_win_count=1,
        max_predicted_tail_risk_probability=0.25,
    )
    candidate_value = build_dfl_regret_surrogate_candidate_value_v1_frame(
        teacher,
        correction,
    )
    strict = build_dfl_regret_surrogate_strict_lp_benchmark_frame(
        teacher,
        candidate_value,
        generated_at=GENERATED_AT,
    )

    assert set(correction["fallback_to_v2_plus"].to_list()) == {True}
    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {0}
    assert strict.filter(pl.col("selection_role") == REGRET_SURROGATE_SELECTION_ROLE)[
        "regret_uah"
    ].to_list() == [120.0] * (len(TENANTS) * 2)


def test_regret_surrogate_rolling_uses_prior_windows_only() -> None:
    panel = _candidate_panel(
        train_alt_regret=80.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )
    teacher = _teacher_panel(panel)

    rolling = build_dfl_regret_surrogate_rolling_robustness_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        min_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_predicted_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def test_safe_switch_context_audit_reports_missing_prior_context() -> None:
    panel = _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher = _teacher_panel(panel)

    audit = build_dfl_regret_surrogate_safe_switch_context_audit_frame(teacher)

    final = audit.filter(pl.col("split_name") == "final_holdout")
    assert set(final["safe_switch_context_failure_mode"].to_list()) == {
        "context_without_prior_support"
    }
    assert set(final["recommended_next_branch"].to_list()) == {
        "data_context_backfill"
    }
    assert set(final["material_safe_switch_available"].to_list()) == {True}
    assert set(final["market_execution_enabled"].to_list()) == {False}


def test_teacher_panel_v2_keeps_final_context_features_prior_only() -> None:
    base_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    mutated_panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=360.0)

    base_v2 = _teacher_panel_v2(base_panel)
    mutated_v2 = _teacher_panel_v2(mutated_panel)
    feature_columns = sorted(
        column for column in base_v2.columns if column.startswith("selector_feature_")
    )

    assert feature_columns
    assert base_v2.select(feature_columns).to_dicts() == mutated_v2.select(
        feature_columns
    ).to_dicts()
    assert base_v2.select("label_context_material_safe_switch").to_dicts() != (
        mutated_v2.select("label_context_material_safe_switch").to_dicts()
    )
    assert base_v2.filter(pl.col("split_name") == "final_holdout").select(
        pl.col("is_training_row").any()
    ).item() is False


def test_contextual_regret_surrogate_selects_prior_supported_safe_candidate() -> None:
    panel = _candidate_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher_v2 = _teacher_panel_v2(panel)

    candidate_value = build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
        teacher_v2,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_context_prior_support_count=1,
        min_context_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_context_tail_risk_probability=0.30,
    )
    strict = build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
        teacher_v2,
        candidate_value,
        generated_at=GENERATED_AT,
    )

    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {2}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE
    )["regret_uah"].to_list() == [80.0] * (len(TENANTS) * 2)
    assert (
        strict.filter(pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE)[
            "regret_uah"
        ].mean()
        < strict.filter(pl.col("selection_role") == V2_PLUS_REFERENCE_ROLE)[
            "regret_uah"
        ].mean()
    )


def test_contextual_regret_surrogate_falls_back_without_prior_context_support() -> None:
    panel = _final_context_shifted_panel(train_alt_regret=80.0, final_alt_regret=80.0)
    teacher_v2 = _teacher_panel_v2(panel)

    candidate_value = build_dfl_regret_surrogate_contextual_candidate_value_v2_frame(
        teacher_v2,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        min_context_prior_support_count=1,
        min_context_prior_safe_win_count=1,
    )
    strict = build_dfl_regret_surrogate_contextual_strict_lp_benchmark_frame(
        teacher_v2,
        candidate_value,
        generated_at=GENERATED_AT,
    )

    assert set(candidate_value["selected_final_candidate_count"].to_list()) == {0}
    assert strict.filter(
        pl.col("selection_role") == REGRET_SURROGATE_CONTEXTUAL_SELECTION_ROLE
    )["regret_uah"].to_list() == [120.0] * (len(TENANTS) * 2)


def test_contextual_regret_surrogate_rolling_uses_prior_context_only() -> None:
    panel = _candidate_panel(
        train_alt_regret=80.0,
        final_alt_regret=80.0,
        train_anchor_count=4,
        final_anchor_count=4,
    )
    teacher = _teacher_panel(panel)

    rolling = build_dfl_regret_surrogate_contextual_rolling_robustness_frame(
        teacher,
        tenant_ids=TENANTS,
        source_model_names=(SOURCE,),
        validation_window_count=2,
        validation_anchor_count=2,
        min_prior_anchors_before_window=2,
        min_context_prior_support_count=1,
        min_context_prior_safe_win_count=1,
        min_predicted_improvement_uah=1.0,
        max_context_tail_risk_probability=0.30,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert rolling.height == 2
    assert set(rolling["rolling_window_passed"].to_list()) == {True}
    assert rolling["minimum_prior_anchor_count_before_window"].min() >= 2
    assert set(rolling["market_execution_enabled"].to_list()) == {False}


def _teacher_panel(panel: pl.DataFrame) -> pl.DataFrame:
    return build_dfl_expanded_schedule_value_teacher_label_panel_v1_frame(
        panel,
        build_dfl_v2_plus_learning_limit_audit_frame(panel),
    )


def _teacher_panel_v2(panel: pl.DataFrame) -> pl.DataFrame:
    teacher = _teacher_panel(panel)
    audit = build_dfl_regret_surrogate_safe_switch_context_audit_frame(teacher)
    return build_dfl_regret_surrogate_teacher_label_panel_v2_frame(teacher, audit)


def _final_context_shifted_panel(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
) -> pl.DataFrame:
    return _candidate_panel(
        train_alt_regret=train_alt_regret,
        final_alt_regret=final_alt_regret,
    ).with_columns(
        pl.when(pl.col("split_name") == "final_holdout")
        .then(0.0)
        .otherwise(pl.col("selector_feature_grid_event_context_ready"))
        .alias("selector_feature_grid_event_context_ready")
    )


def _candidate_panel(
    *,
    train_alt_regret: float,
    final_alt_regret: float,
    train_anchor_count: int = 3,
    final_anchor_count: int = 2,
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
            alt_regret = (
                train_alt_regret
                if split_name == "train_selection"
                else final_alt_regret
            )
            for source, family, model_name, regret, dispatch in (
                (
                    "strict_fallback",
                    "strict_control",
                    "strict_similar_day",
                    300.0,
                    [0.0, 0.0, 0.0, 0.0],
                ),
                (
                    "v2_plus_default",
                    "frozen_v2_plus_fallback",
                    "schedule_value_learner_v2_plus",
                    120.0,
                    [0.1, -0.1, 0.0, 0.0],
                ),
                (
                    "tft_shadow_candidate",
                    "tft_quantile_risk",
                    "tft_safe_candidate",
                    alt_regret,
                    [0.2, -0.2, 0.0, 0.0],
                ),
                (
                    "poland_shadow_candidate",
                    "poland_lag24_tail_risk",
                    "poland_tail_candidate",
                    360.0,
                    [0.4, -0.4, 0.0, 0.0],
                ),
            ):
                rows.append(
                    _candidate_row(
                        tenant=tenant,
                        candidate_source=source,
                        family=family,
                        model_name=model_name,
                        anchor=anchor,
                        split_name=split_name,
                        regret=regret,
                        dispatch=dispatch,
                    )
                )
    return pl.DataFrame(rows, infer_schema_length=None)


def _candidate_row(
    *,
    tenant: str,
    candidate_source: str,
    family: str,
    model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    dispatch: list[float],
) -> dict[str, object]:
    forecast = [1000.0, 3000.0, 1500.0, 4000.0]
    actual = [1100.0, 2800.0, 1600.0, 3900.0]
    soc = [0.5 + sum(dispatch[: index + 1]) * 0.05 for index in range(len(dispatch))]
    baseline_regret = 120.0
    delta = regret - baseline_regret
    return {
        "tenant_id": tenant,
        "source_model_name": SOURCE,
        "candidate_source": candidate_source,
        "candidate_family": family,
        "candidate_model_name": model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 4,
        "eligible_for_final_selection": candidate_source != "strict_fallback",
        "forecast_price_uah_mwh_vector": forecast,
        "actual_price_uah_mwh_vector": actual,
        "dispatch_mw_vector": dispatch,
        "soc_fraction_vector": soc,
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0 - regret,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "v2_plus_baseline_regret_uah": baseline_regret,
        "label_regret_delta_vs_v2_plus_uah": delta,
        "label_safe_switch_win": candidate_source not in {
            "v2_plus_default",
            "strict_fallback",
        }
        and delta < 0.0,
        "label_tail_risk_loss": candidate_source not in {
            "v2_plus_default",
            "strict_fallback",
        }
        and delta >= 150.0,
        "label_best_candidate_family": family if delta < 0.0 else "frozen_v2_plus",
        "label_best_candidate_model_name": (
            model_name if delta < 0.0 else "schedule_value_learner_v2_plus"
        ),
        "label_is_anchor_best_candidate": delta < 0.0,
        "selector_feature_schedule_distance_from_v2_plus": (
            0.0 if candidate_source == "v2_plus_default" else abs(dispatch[0] - 0.1)
        ),
        "selector_feature_total_throughput_delta_mwh": sum(abs(value) for value in dispatch)
        - 0.2,
        "selector_feature_terminal_soc_delta_fraction": soc[-1] - 0.5,
        "selector_feature_forecast_spread_uah_mwh": max(forecast) - min(forecast),
        "selector_feature_total_degradation_penalty_uah": abs(sum(dispatch)) * 10.0,
        "selector_feature_poland_shadow_candidate": float(
            candidate_source == "poland_shadow_candidate"
        ),
        "selector_feature_tft_shadow_candidate": float(
            candidate_source == "tft_shadow_candidate"
        ),
        "selector_feature_weather_load_context_ready": 1.0,
        "selector_feature_calendar_publication_context_ready": 1.0,
        "selector_feature_grid_event_context_ready": 1.0,
        "selector_feature_hour_of_day": float(anchor.hour),
        "selector_feature_weekend": float(anchor.weekday() >= 5),
        "total_degradation_penalty_uah": abs(sum(dispatch)) * 10.0,
        "total_throughput_mwh": sum(abs(value) for value in dispatch),
        "safety_violation_count": 0,
        "target_label_space": "schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": {
            "candidate_source": candidate_source,
            "candidate_family": family,
            "candidate_model_name": model_name,
            "dispatch_mw": dispatch,
            "soc_fraction": soc,
        },
    }
