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
