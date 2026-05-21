from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.lava_schedule_neighbor_bridge import (
    DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND,
    build_dfl_lava_candidate_value_scorer_frame,
    build_dfl_lava_candidate_value_strict_lp_benchmark_frame,
    build_dfl_lava_schedule_neighbor_candidate_frame,
    build_dfl_v2_plus_schedule_neighbor_teacher_label_frame,
    evaluate_dfl_lava_candidate_value_gate,
)
from smart_arbitrage.dfl.lava_tail_risk_target import (
    DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE,
    DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_SAFE_SWITCH_V2_SELECTION_ROLE,
    DFL_LAVA_SAFE_SWITCH_V2_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_SELECTION_ROLE,
    DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_STRICT_LP_STRATEGY_KIND,
    DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE,
    DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND,
    build_dfl_lava_tail_risk_avoidance_label_frame,
    build_dfl_lava_tail_risk_avoidance_scorer_v3_frame,
    build_dfl_lava_tail_risk_avoidance_strict_lp_benchmark_v3_frame,
    build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame,
    build_dfl_lava_tail_risk_safe_switch_scorer_v2_frame,
    build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_v2_frame,
    build_dfl_lava_tail_risk_safe_switch_scorer_frame,
    build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame,
    build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame,
    build_dfl_lava_tail_risk_aware_target_frame,
    build_dfl_lava_tail_risk_diagnostic_frame,
    evaluate_dfl_lava_tail_risk_avoidance_v3_gate,
    evaluate_dfl_lava_tail_risk_safe_switch_v2_gate,
    evaluate_dfl_lava_tail_risk_safe_switch_gate,
    evaluate_dfl_lava_tail_risk_aware_gate,
)

TENANTS: tuple[str, ...] = ("client_001_kyiv_mall", "client_002_lviv_office")
BASELINE_SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"
POLAND_SOURCE = "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1"
GENERATED_AT = datetime(2026, 5, 21, 21)
FIRST_ANCHOR = datetime(2026, 1, 1, 23)


def test_teacher_labels_classify_poland_wins_and_overreach() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    poland = _poland_strict_frame(regrets=(80.0, 300.0))
    ranker = _ranker_strict_frame(regrets=(90.0, 320.0))

    labels = build_dfl_v2_plus_schedule_neighbor_teacher_label_frame(
        baseline,
        poland,
        _prior_veto_frame(selected_regrets=(100.0, 120.0)),
        ranker,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )

    classes = set(labels["teacher_class"].to_list())
    assert {
        "v2_plus_best",
        "poland_safe_win",
        "poland_tail_risk_loss",
        "selector_overreach",
    }.issubset(classes)
    assert {-40.0, -30.0}.issubset(
        set(
            labels.filter(pl.col("teacher_class") == "poland_safe_win")[
                "label_regret_delta_vs_v2_plus_uah"
            ].to_list()
        )
    )
    assert set(labels["market_execution_enabled"].unique().to_list()) == {False}


def test_teacher_selector_features_do_not_change_when_final_labels_mutate() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    poland = _poland_strict_frame(regrets=(80.0, 260.0))
    mutated = _poland_strict_frame(regrets=(400.0, 500.0))

    labels = build_dfl_v2_plus_schedule_neighbor_teacher_label_frame(
        baseline,
        poland,
        _prior_veto_frame(selected_regrets=(100.0, 120.0)),
        _ranker_strict_frame(regrets=(90.0, 320.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    mutated_labels = build_dfl_v2_plus_schedule_neighbor_teacher_label_frame(
        baseline,
        mutated,
        _prior_veto_frame(selected_regrets=(100.0, 120.0)),
        _ranker_strict_frame(regrets=(90.0, 320.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )

    selector_columns = sorted(
        column for column in labels.columns if column.startswith("selector_feature_")
    )
    assert selector_columns
    assert labels.select(selector_columns).to_dicts() == mutated_labels.select(
        selector_columns
    ).to_dicts()
    assert labels.select("label_regret_delta_vs_v2_plus_uah").to_dicts() != (
        mutated_labels.select("label_regret_delta_vs_v2_plus_uah").to_dicts()
    )


def test_lava_candidate_frame_keeps_oracle_neighbors_train_only() -> None:
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_candidate_library(train_regret=70.0, final_regret=80.0),
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )

    assert {
        "v2_plus_default",
        "strict_fallback",
        "poland_shadow_candidate",
        "oracle_neighbor_train_diagnostic",
    }.issubset(set(candidates["candidate_source"].unique().to_list()))
    oracle_rows = candidates.filter(
        pl.col("candidate_source") == "oracle_neighbor_train_diagnostic"
    )
    assert oracle_rows.height > 0
    assert set(oracle_rows["split_name"].unique().to_list()) == {"train_selection"}
    assert set(oracle_rows["eligible_for_final_selection"].unique().to_list()) == {
        False
    }
    assert set(candidates["market_execution_enabled"].unique().to_list()) == {False}


def test_lava_selection_is_prior_only_and_final_scoring_changes_with_labels() -> None:
    baseline_library = _baseline_candidate_library(train_regret=100.0, final_regret=120.0)
    poland_library = _poland_candidate_library(train_regret=70.0, final_regret=80.0)
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        baseline_library,
        poland_library,
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    scorer = build_dfl_lava_candidate_value_scorer_frame(
        candidates,
        tenant_ids=TENANTS,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )
    strict = build_dfl_lava_candidate_value_strict_lp_benchmark_frame(
        candidates,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    mutated_candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        baseline_library,
        _poland_candidate_library(train_regret=70.0, final_regret=400.0),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    mutated_strict = build_dfl_lava_candidate_value_strict_lp_benchmark_frame(
        mutated_candidates,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )

    selected = strict.filter(pl.col("selection_role") == "lava_candidate_value_scorer")
    mutated_selected = mutated_strict.filter(
        pl.col("selection_role") == "lava_candidate_value_scorer"
    )
    gate = evaluate_dfl_lava_candidate_value_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    assert set(scorer["fallback_to_v2_plus"].to_list()) == {False}
    assert scorer["selected_final_candidate_source_counts"].to_list() == [
        {"poland_shadow_candidate": 2},
        {"poland_shadow_candidate": 2},
    ]
    assert selected["selected_candidate_family"].to_list() == (
        mutated_selected["selected_candidate_family"].to_list()
    )
    assert selected["regret_uah"].to_list() != mutated_selected["regret_uah"].to_list()
    assert "behavior_cloning_reference" in set(strict["selection_role"].to_list())
    assert set(strict["strategy_kind"].unique().to_list()) == {
        DFL_LAVA_CANDIDATE_VALUE_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_lava_scorer_falls_back_when_prior_signal_is_weak() -> None:
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_candidate_library(train_regret=105.0, final_regret=20.0),
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    scorer = build_dfl_lava_candidate_value_scorer_frame(
        candidates,
        tenant_ids=TENANTS,
        min_prior_mean_improvement_ratio_vs_v2_plus=0.01,
    )
    strict = build_dfl_lava_candidate_value_strict_lp_benchmark_frame(
        candidates,
        scorer,
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        generated_at=GENERATED_AT,
    )
    selected = strict.filter(pl.col("selection_role") == "lava_candidate_value_scorer")

    assert set(scorer["fallback_to_v2_plus"].to_list()) == {True}
    assert selected["regret_uah"].to_list() == [120.0] * (len(TENANTS) * 2)


def test_tail_risk_diagnostic_blocks_unsafe_perturbation_families() -> None:
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )

    by_family = {
        row["candidate_family"]: row["tail_risk_diagnostic_class"]
        for row in diagnostic.filter(pl.col("split_name") == "train_selection")
        .group_by("candidate_family")
        .agg(pl.col("tail_risk_diagnostic_class").first())
        .to_dicts()
    }

    assert by_family["rank_extrema_perturbation_v2_plus"] == (
        "tail_risk_perturbation_loss"
    )
    assert by_family["poland_safe_value_candidate"] == "safe_neighbor_candidate"
    assert set(diagnostic["market_execution_enabled"].unique().to_list()) == {False}


def test_tail_risk_diagnostic_accepts_persisted_strict_schema_without_candidate_family() -> None:
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    persisted_shape = _failed_lava_tail_risk_strict_frame().drop("candidate_family")

    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        persisted_shape,
        tail_risk_delta_uah=150.0,
    )

    assert "tail_risk_perturbation_loss" in set(
        diagnostic["tail_risk_diagnostic_class"].to_list()
    )


def test_tail_risk_aware_target_uses_safe_family_not_raw_hourly_actions() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    target = build_dfl_lava_tail_risk_aware_target_frame(
        candidates,
        diagnostic,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
    )
    strict = build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
        candidates,
        target,
        baseline,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_lava_tail_risk_aware_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )

    selected = strict.filter(
        pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE
    )
    assert set(target["raw_hourly_action_imitation"].to_list()) == {False}
    assert target["blocked_candidate_families"].to_list() == [
        ["rank_extrema_perturbation_v2_plus"],
        ["rank_extrema_perturbation_v2_plus"],
    ]
    assert selected["selected_candidate_family"].to_list() == [
        "poland_safe_value_candidate"
    ] * (len(TENANTS) * 2)
    assert set(strict["strategy_kind"].unique().to_list()) == {
        DFL_LAVA_TAIL_RISK_AWARE_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_tail_risk_target_selection_is_prior_only_when_final_labels_mutate() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    mutated_candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=900.0,
            tail_train_regret=420.0,
            tail_final_regret=1000.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    mutated_diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        mutated_candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )

    target = build_dfl_lava_tail_risk_aware_target_frame(
        candidates,
        diagnostic,
        tenant_ids=TENANTS,
    )
    mutated_target = build_dfl_lava_tail_risk_aware_target_frame(
        mutated_candidates,
        mutated_diagnostic,
        tenant_ids=TENANTS,
    )
    strict = build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
        candidates,
        target,
        baseline,
        generated_at=GENERATED_AT,
    )
    mutated_strict = build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
        mutated_candidates,
        target,
        baseline,
        generated_at=GENERATED_AT,
    )

    assert target["selected_final_candidate_keys"].to_list() == (
        mutated_target["selected_final_candidate_keys"].to_list()
    )
    assert strict.filter(
        pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE
    )["regret_uah"].to_list() != mutated_strict.filter(
        pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE
    )[
        "regret_uah"
    ].to_list()


def test_tail_risk_target_falls_back_to_v2_plus_when_only_tail_risk_exists() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_only_candidate_library(
            train_regret=420.0,
            final_regret=20.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    target = build_dfl_lava_tail_risk_aware_target_frame(
        candidates,
        diagnostic,
        tenant_ids=TENANTS,
    )
    strict = build_dfl_lava_tail_risk_aware_strict_lp_benchmark_frame(
        candidates,
        target,
        baseline,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_lava_tail_risk_aware_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )
    selected = strict.filter(
        pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AWARE_SELECTION_ROLE
    )

    assert set(target["fallback_to_v2_plus"].to_list()) == {True}
    assert selected["regret_uah"].to_list() == [120.0] * (len(TENANTS) * 2)
    assert gate.passed is False
    assert "mean_not_improved_vs_v2_plus" in gate.description


def test_safe_switch_scorer_uses_prior_profiles_and_per_anchor_fallback() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
            safe_final_dispatch_by_anchor={
                3: (0.2, -0.2),
                4: (0.7, -0.7),
            },
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )

    scorer = build_dfl_lava_tail_risk_safe_switch_scorer_frame(
        candidates,
        diagnostic,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
    )
    strict = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame(
        candidates,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_lava_tail_risk_safe_switch_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )
    selected = strict.filter(pl.col("selection_role") == DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE)

    assert scorer["target_label_space"].to_list() == [
        "schedule_candidate_index",
        "schedule_candidate_index",
    ]
    assert set(scorer["raw_hourly_action_imitation"].to_list()) == {False}
    assert scorer["selected_final_candidate_source_counts"].to_list() == [
        {"poland_shadow_candidate": 1, "frozen_v2_plus_fallback": 1},
        {"poland_shadow_candidate": 1, "frozen_v2_plus_fallback": 1},
    ]
    assert "strict_fallback" not in set(selected["selected_strategy_source"].to_list())
    assert selected["selected_candidate_family"].to_list() == [
        "poland_safe_value_candidate",
        "frozen_v2_plus_fallback",
        "poland_safe_value_candidate",
        "frozen_v2_plus_fallback",
    ]
    assert selected["regret_uah"].to_list() == [80.0, 120.0, 80.0, 120.0]
    assert set(strict["strategy_kind"].unique().to_list()) == {
        DFL_LAVA_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True
    assert gate.metrics["market_execution_enabled"] is False


def test_safe_switch_selection_is_unchanged_when_final_labels_mutate() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    mutated_candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=900.0,
            tail_train_regret=420.0,
            tail_final_regret=1000.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    mutated_diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        mutated_candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )

    scorer = build_dfl_lava_tail_risk_safe_switch_scorer_frame(
        candidates,
        diagnostic,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
    )
    mutated_scorer = build_dfl_lava_tail_risk_safe_switch_scorer_frame(
        mutated_candidates,
        mutated_diagnostic,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
    )
    strict = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame(
        candidates,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    mutated_strict = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_frame(
        mutated_candidates,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )

    assert scorer["selected_final_candidate_keys"].to_list() == (
        mutated_scorer["selected_final_candidate_keys"].to_list()
    )
    assert strict.filter(
        pl.col("selection_role") == DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE
    )["regret_uah"].to_list() != mutated_strict.filter(
        pl.col("selection_role") == DFL_LAVA_SAFE_SWITCH_SELECTION_ROLE
    )[
        "regret_uah"
    ].to_list()


def test_safe_switch_v2_repairs_null_poland_context_features() -> None:
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    lagged_features = _lagged_poland_feature_frame_with_null_context()

    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        candidates,
        diagnostic,
        lagged_features,
    )

    rich_columns = [
        column
        for column in feature_panel.columns
        if column.startswith("selector_feature_poland_lag24_")
    ]
    assert rich_columns
    assert feature_panel.select(pl.sum_horizontal(pl.col(rich_columns).null_count())).item() == 0
    assert set(feature_panel["feature_repair_status"].unique().to_list()) == {
        "repaired_prior_safe_neutral_context"
    }
    assert feature_panel["selector_feature_repaired_null_count"].max() > 0
    assert set(feature_panel["market_execution_enabled"].unique().to_list()) == {False}


def test_safe_switch_v2_uses_rich_prior_context_and_anchor_fallback() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
            safe_final_dispatch_by_anchor={
                3: (0.2, -0.2),
                4: (0.7, -0.7),
            },
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        candidates,
        diagnostic,
        _lagged_poland_feature_frame_with_null_context(),
    )

    scorer = build_dfl_lava_tail_risk_safe_switch_scorer_v2_frame(
        feature_panel,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
        min_predicted_improvement_uah=1.0,
    )
    strict = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_v2_frame(
        feature_panel,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    gate = evaluate_dfl_lava_tail_risk_safe_switch_v2_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )
    selected = strict.filter(
        pl.col("selection_role") == DFL_LAVA_SAFE_SWITCH_V2_SELECTION_ROLE
    )

    assert scorer["selected_scorer_type"].to_list() == [
        "tabular_rich_prior_safe_switch_v2",
        "tabular_rich_prior_safe_switch_v2",
    ]
    assert "selector_feature_poland_lag24_ua_spread_ratio" in (
        scorer.row(0, named=True)["selected_feature_names"]
    )
    assert scorer["selected_final_candidate_source_counts"].to_list() == [
        {"poland_shadow_candidate": 1, "frozen_v2_plus_fallback": 1},
        {"poland_shadow_candidate": 1, "frozen_v2_plus_fallback": 1},
    ]
    assert selected["selected_candidate_family"].to_list() == [
        "poland_safe_value_candidate",
        "frozen_v2_plus_fallback",
        "poland_safe_value_candidate",
        "frozen_v2_plus_fallback",
    ]
    assert selected["regret_uah"].to_list() == [80.0, 120.0, 80.0, 120.0]
    assert set(strict["strategy_kind"].unique().to_list()) == {
        DFL_LAVA_SAFE_SWITCH_V2_STRICT_LP_STRATEGY_KIND
    }
    assert gate.passed is True


def test_safe_switch_v2_selection_is_unchanged_when_final_labels_mutate() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    mutated_candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=900.0,
            tail_train_regret=420.0,
            tail_final_regret=1000.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    mutated_diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        mutated_candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        candidates,
        diagnostic,
        _lagged_poland_feature_frame_with_null_context(),
    )
    mutated_feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        mutated_candidates,
        mutated_diagnostic,
        _lagged_poland_feature_frame_with_null_context(),
    )

    scorer = build_dfl_lava_tail_risk_safe_switch_scorer_v2_frame(
        feature_panel,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
    )
    mutated_scorer = build_dfl_lava_tail_risk_safe_switch_scorer_v2_frame(
        mutated_feature_panel,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
    )
    strict = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_v2_frame(
        feature_panel,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    mutated_strict = build_dfl_lava_tail_risk_safe_switch_strict_lp_benchmark_v2_frame(
        mutated_feature_panel,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )

    assert scorer["selected_final_candidate_keys"].to_list() == (
        mutated_scorer["selected_final_candidate_keys"].to_list()
    )
    assert strict.filter(
        pl.col("selection_role") == DFL_LAVA_SAFE_SWITCH_V2_SELECTION_ROLE
    )["regret_uah"].to_list() != mutated_strict.filter(
        pl.col("selection_role") == DFL_LAVA_SAFE_SWITCH_V2_SELECTION_ROLE
    )[
        "regret_uah"
    ].to_list()


def test_tail_risk_avoidance_label_frame_marks_safe_and_risky_switches() -> None:
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        _v2_plus_strict_frame(v2_regrets=(120.0, 120.0)),
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        candidates,
        diagnostic,
        _lagged_poland_feature_frame_with_null_context(),
    )

    labels = build_dfl_lava_tail_risk_avoidance_label_frame(
        feature_panel,
        tail_risk_delta_uah=150.0,
    )

    assert {
        "v2_plus_default",
        "safe_switch_win",
        "tail_risk_switch",
    }.issubset(set(labels["tail_risk_avoidance_class"].to_list()))
    assert set(labels["label_tail_risk_switch"].unique().to_list()) == {False, True}
    assert set(labels["raw_hourly_action_imitation"].unique().to_list()) == {False}
    assert set(labels["market_execution_enabled"].unique().to_list()) == {False}


def test_tail_risk_avoidance_v3_blocks_profiles_with_prior_tail_losses() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        candidates,
        diagnostic,
        _lagged_poland_feature_frame_with_null_context(),
    )
    first_anchor = FIRST_ANCHOR
    feature_panel = feature_panel.with_columns(
        pl.when(
            (pl.col("candidate_family") == "poland_safe_value_candidate")
            & (pl.col("anchor_timestamp") == first_anchor)
        )
        .then(pl.lit(300.0))
        .otherwise(pl.col("label_regret_delta_vs_v2_plus_uah"))
        .alias("label_regret_delta_vs_v2_plus_uah")
    )
    labels = build_dfl_lava_tail_risk_avoidance_label_frame(
        feature_panel,
        tail_risk_delta_uah=150.0,
    )

    scorer = build_dfl_lava_tail_risk_avoidance_scorer_v3_frame(
        labels,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        max_prior_tail_loss_count=99,
        min_prior_precision=0.0,
        max_predicted_tail_risk_probability=0.0,
    )
    strict = build_dfl_lava_tail_risk_avoidance_strict_lp_benchmark_v3_frame(
        labels,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    selected = strict.filter(
        pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_SELECTION_ROLE
    )

    assert scorer["selector_gate_blocker"].to_list() == [
        "no_candidate_after_tail_risk_probability_filter",
        "no_candidate_after_tail_risk_probability_filter",
    ]
    assert set(selected["selected_candidate_family"].to_list()) == {
        "frozen_v2_plus_fallback"
    }
    assert selected["regret_uah"].to_list() == [120.0, 120.0, 120.0, 120.0]


def test_tail_risk_avoidance_v3_can_pass_when_safe_candidates_are_prior_supported() -> None:
    baseline = _v2_plus_strict_frame(v2_regrets=(120.0, 120.0))
    candidates = build_dfl_lava_schedule_neighbor_candidate_frame(
        _baseline_candidate_library(train_regret=100.0, final_regret=120.0),
        _poland_tail_risk_candidate_library(
            safe_train_regret=70.0,
            safe_final_regret=80.0,
            tail_train_regret=420.0,
            tail_final_regret=600.0,
        ),
        baseline,
        baseline_source_model_name=BASELINE_SOURCE,
        poland_source_model_names=(POLAND_SOURCE,),
    )
    diagnostic = build_dfl_lava_tail_risk_diagnostic_frame(
        candidates,
        _failed_lava_tail_risk_strict_frame(),
        tail_risk_delta_uah=150.0,
    )
    feature_panel = build_dfl_lava_tail_risk_safe_switch_feature_panel_v2_frame(
        candidates,
        diagnostic,
        _lagged_poland_feature_frame_with_null_context(),
    )
    labels = build_dfl_lava_tail_risk_avoidance_label_frame(
        feature_panel,
        tail_risk_delta_uah=150.0,
    )

    scorer = build_dfl_lava_tail_risk_avoidance_scorer_v3_frame(
        labels,
        tenant_ids=TENANTS,
        min_prior_safe_win_count=1,
        min_prior_precision=0.5,
        max_predicted_tail_risk_probability=0.35,
    )
    strict = build_dfl_lava_tail_risk_avoidance_strict_lp_benchmark_v3_frame(
        labels,
        scorer,
        baseline,
        generated_at=GENERATED_AT,
    )
    strict_with_many_references = (
        build_dfl_lava_tail_risk_avoidance_strict_lp_benchmark_v3_frame(
            labels,
            scorer,
            pl.concat([baseline] * 60),
            generated_at=GENERATED_AT,
        )
    )
    gate = evaluate_dfl_lava_tail_risk_avoidance_v3_gate(
        strict,
        min_validation_tenant_anchor_count=len(TENANTS) * 2,
        min_mean_regret_improvement_ratio_vs_v2_plus=0.05,
    )
    selected = strict.filter(
        pl.col("selection_role") == DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_SELECTION_ROLE
    )

    assert selected["selected_candidate_family"].to_list() == [
        "poland_safe_value_candidate",
        "poland_safe_value_candidate",
        "poland_safe_value_candidate",
        "poland_safe_value_candidate",
    ]
    assert set(strict["strategy_kind"].unique().to_list()) == {
        DFL_LAVA_TAIL_RISK_AVOIDANCE_V3_STRICT_LP_STRATEGY_KIND
    }
    assert "selected_candidate_family" in strict_with_many_references.columns
    assert gate.passed is True


def _v2_plus_strict_frame(*, v2_regrets: tuple[float, float]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index, regret in enumerate(v2_regrets):
            anchor = FIRST_ANCHOR + timedelta(days=3 + anchor_index)
            rows.extend(
                [
                    _strict_row(
                        tenant_id=tenant_id,
                        source_model_name=BASELINE_SOURCE,
                        forecast_model_name="strict_similar_day",
                        selection_role="strict_reference",
                        anchor=anchor,
                        regret=310.0,
                    ),
                    _strict_row(
                        tenant_id=tenant_id,
                        source_model_name=BASELINE_SOURCE,
                        forecast_model_name=(
                            "dfl_schedule_value_learner_v2_plus_"
                            f"{BASELINE_SOURCE}"
                        ),
                        selection_role="schedule_value_learner_v2_plus",
                        anchor=anchor,
                        regret=regret,
                    ),
                ]
            )
    return pl.DataFrame(rows)


def _poland_strict_frame(*, regrets: tuple[float, float]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index, regret in enumerate(regrets):
            anchor = FIRST_ANCHOR + timedelta(days=3 + anchor_index)
            rows.append(
                _strict_row(
                    tenant_id=tenant_id,
                    source_model_name=POLAND_SOURCE,
                    forecast_model_name=f"dfl_schedule_value_learner_v2_plus_{POLAND_SOURCE}",
                    selection_role="schedule_value_learner_v2_plus",
                    anchor=anchor,
                    regret=regret,
                    candidate_family="poland_value_candidate",
                )
            )
    return pl.DataFrame(rows)


def _ranker_strict_frame(*, regrets: tuple[float, float]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index, regret in enumerate(regrets):
            anchor = FIRST_ANCHOR + timedelta(days=3 + anchor_index)
            rows.append(
                _strict_row(
                    tenant_id=tenant_id,
                    source_model_name=POLAND_SOURCE,
                    forecast_model_name=(
                        "dfl_poland_lag24_candidate_value_ranker_v1_"
                        f"{POLAND_SOURCE}"
                    ),
                    selection_role="poland_lag24_candidate_value_ranker_v1",
                    anchor=anchor,
                    regret=regret,
                    candidate_family="poland_ranker_candidate",
                )
            )
    return pl.DataFrame(rows)


def _prior_veto_frame(*, selected_regrets: tuple[float, float]) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index, regret in enumerate(selected_regrets):
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "anchor_timestamp": FIRST_ANCHOR + timedelta(days=3 + anchor_index),
                    "selected_strategy_name": (
                        "poland_lag24_prior_tail_risk_veto"
                        if regret < 120.0
                        else "frozen_ukrainian_v2_plus_fallback"
                    ),
                    "selected_regret_uah": regret,
                    "baseline_regret_uah": 120.0,
                    "challenger_regret_uah": 80.0 if regret < 120.0 else 260.0,
                    "selected_uses_challenger": regret < 120.0,
                    "market_execution_enabled": False,
                    "not_market_execution": True,
                    "not_full_dfl": True,
                }
            )
    return pl.DataFrame(rows)


def _baseline_candidate_library(*, train_regret: float, final_regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(5):
            split_name = "train_selection" if anchor_index < 3 else "final_holdout"
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            regret = train_regret if split_name == "train_selection" else final_regret
            rows.extend(
                [
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=BASELINE_SOURCE,
                        candidate_family="strict_control",
                        candidate_model_name="strict_similar_day",
                        anchor=anchor,
                        split_name=split_name,
                        regret=310.0,
                        dispatch=(0.0, 0.0),
                    ),
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=BASELINE_SOURCE,
                        candidate_family="rank_extrema_perturbation_v2_plus",
                        candidate_model_name=f"v2_plus_{anchor_index}",
                        anchor=anchor,
                        split_name=split_name,
                        regret=regret,
                        dispatch=(0.2, -0.2),
                    ),
                ]
            )
    return pl.DataFrame(rows)


def _poland_candidate_library(*, train_regret: float, final_regret: float) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(5):
            split_name = "train_selection" if anchor_index < 3 else "final_holdout"
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            regret = train_regret if split_name == "train_selection" else final_regret
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name=POLAND_SOURCE,
                    candidate_family="poland_value_candidate",
                    candidate_model_name=f"poland_{anchor_index}",
                    anchor=anchor,
                    split_name=split_name,
                    regret=regret,
                    dispatch=(0.4, -0.4),
                )
            )
    return pl.DataFrame(rows)


def _poland_tail_risk_candidate_library(
    *,
    safe_train_regret: float,
    safe_final_regret: float,
    tail_train_regret: float,
    tail_final_regret: float,
    safe_final_dispatch_by_anchor: dict[int, tuple[float, float]] | None = None,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(5):
            split_name = "train_selection" if anchor_index < 3 else "final_holdout"
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            safe_regret = (
                safe_train_regret if split_name == "train_selection" else safe_final_regret
            )
            safe_dispatch = (
                (0.2, -0.2)
                if split_name == "train_selection"
                else (safe_final_dispatch_by_anchor or {}).get(
                    anchor_index,
                    (0.2, -0.2),
                )
            )
            tail_regret = (
                tail_train_regret if split_name == "train_selection" else tail_final_regret
            )
            rows.extend(
                [
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=POLAND_SOURCE,
                        candidate_family="poland_safe_value_candidate",
                        candidate_model_name=f"poland_safe_{anchor_index}",
                        anchor=anchor,
                        split_name=split_name,
                        regret=safe_regret,
                        dispatch=safe_dispatch,
                    ),
                    _candidate_row(
                        tenant_id=tenant_id,
                        source_model_name=POLAND_SOURCE,
                        candidate_family="rank_extrema_perturbation_v2_plus",
                        candidate_model_name=f"poland_tail_{anchor_index}",
                        anchor=anchor,
                        split_name=split_name,
                        regret=tail_regret,
                        dispatch=(0.6, -0.6),
                    ),
                ]
            )
    return pl.DataFrame(rows)


def _poland_tail_risk_only_candidate_library(
    *,
    train_regret: float,
    final_regret: float,
) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(5):
            split_name = "train_selection" if anchor_index < 3 else "final_holdout"
            anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
            regret = train_regret if split_name == "train_selection" else final_regret
            rows.append(
                _candidate_row(
                    tenant_id=tenant_id,
                    source_model_name=POLAND_SOURCE,
                    candidate_family="rank_extrema_perturbation_v2_plus",
                    candidate_model_name=f"poland_tail_{anchor_index}",
                    anchor=anchor,
                    split_name=split_name,
                    regret=regret,
                    dispatch=(0.6, -0.6),
                )
            )
    return pl.DataFrame(rows)


def _failed_lava_tail_risk_strict_frame() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for tenant_id in TENANTS:
        for anchor_index in range(2):
            anchor = FIRST_ANCHOR + timedelta(days=3 + anchor_index)
            rows.append(
                _strict_row(
                    tenant_id=tenant_id,
                    source_model_name="lava_schedule_neighbor_bridge_v1",
                    forecast_model_name="dfl_lava_candidate_value_scorer_v1",
                    selection_role="lava_candidate_value_scorer",
                    anchor=anchor,
                    regret=600.0,
                    candidate_family="rank_extrema_perturbation_v2_plus",
                )
            )
    return pl.DataFrame(rows)


def _lagged_poland_feature_frame_with_null_context() -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for anchor_index in range(5):
        anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
        rows.append(
            {
                "delivery_timestamp_utc": anchor.isoformat() + "+00:00",
                "coverage_status": "full_lagged_feature_coverage",
                "source_backed": True,
                "entsoe_pl_lag24_ua_spread_uah_mwh": 200.0 + anchor_index * 5.0,
                "entsoe_pl_lag24_ua_spread_delta_24h_uah_mwh": None
                if anchor_index == 0
                else 5.0,
                "entsoe_pl_lag24_ua_spread_ratio": None if anchor_index == 1 else 0.2,
                "entsoe_pl_lag24_ua_rank_disagreement": 0.1,
                "entsoe_pl_lag24_ua_peak_hour_delta": 2.0,
                "entsoe_pl_lag24_ua_trough_hour_delta": 1.0,
                "entsoe_pl_lag24_ua_spread_momentum_sign": None
                if anchor_index == 2
                else 1.0,
                "entsoe_pl_lag24_evening_morning_spread_uah_mwh": 400.0,
                "entsoe_pl_lag24_price_rank_centered": 0.25,
                "entsoe_pl_lag24_peak_trough_span_hours": 18.0,
            }
        )
    return pl.DataFrame(rows)


def _strict_row(
    *,
    tenant_id: str,
    source_model_name: str,
    forecast_model_name: str,
    selection_role: str,
    anchor: datetime,
    regret: float,
    candidate_family: str | None = None,
) -> dict[str, object]:
    return {
        "evaluation_id": f"{tenant_id}:{forecast_model_name}:{anchor:%Y%m%dT%H%M}",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": "test",
        "market_venue": "DAM",
        "selection_role": selection_role,
        "candidate_family": candidate_family,
        "candidate_model_name": forecast_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 2,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "test",
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 0.2,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "evaluation_payload": {
            "market_execution_enabled": False,
            "horizon": [
                {
                    "step_index": 0,
                    "net_power_mw": 0.2,
                    "soc_fraction": 0.5,
                },
                {
                    "step_index": 1,
                    "net_power_mw": -0.2,
                    "soc_fraction": 0.45,
                },
            ],
        },
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _candidate_row(
    *,
    tenant_id: str,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    anchor: datetime,
    split_name: str,
    regret: float,
    dispatch: tuple[float, float],
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "candidate_family": candidate_family,
        "candidate_model_name": candidate_model_name,
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "split_name": split_name,
        "horizon_hours": 2,
        "forecast_price_uah_mwh_vector": [1000.0, 3500.0],
        "actual_price_uah_mwh_vector": [900.0, 3600.0],
        "dispatch_mw_vector": list(dispatch),
        "soc_fraction_vector": [0.5, 0.5 + dispatch[0] * 0.1],
        "decision_value_uah": 1000.0 - regret,
        "forecast_objective_value_uah": 900.0,
        "oracle_value_uah": 1000.0,
        "regret_uah": regret,
        "regret_ratio": regret / 1000.0,
        "forecast_spread_uah_mwh": 2500.0,
        "total_degradation_penalty_uah": 5.0,
        "total_throughput_mwh": abs(dispatch[0]) + abs(dispatch[1]),
        "safety_violation_count": 0,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "evaluation_payload": {"market_execution_enabled": False},
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }
