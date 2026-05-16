from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl

from smart_arbitrage.dfl.market_coupling_ablation import (
    build_dfl_market_coupling_v2_plus_ablation_frame,
    validate_dfl_market_coupling_v2_plus_ablation_evidence,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND,
    schedule_value_learner_v2_plus_model_name,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
)
from smart_arbitrage.forecasting.market_coupling_features import (
    MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE,
)

SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"


def test_market_coupling_ablation_blocks_without_approved_features() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_status"] == "blocked_by_governance"
    assert row["did_train_market_coupled_variant"] is False
    assert row["ablation_passed"] is False
    assert row["ablation_blocker"] == "no_approved_external_features"
    assert row["baseline_mean_regret_uah"] == 100.0
    assert row["market_execution_enabled"] is False

    outcome = validate_dfl_market_coupling_v2_plus_ablation_evidence(
        frame,
        source_model_names=(SOURCE,),
    )
    assert outcome.passed is True


def test_market_coupling_ablation_passes_only_when_market_coupled_variant_beats_v2_plus() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=True),
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0],
            selected_regrets=[80.0, 80.0, 80.0, 80.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_status"] == "comparison_complete"
    assert row["did_train_market_coupled_variant"] is True
    assert row["ablation_passed"] is True
    assert row["approved_external_feature_columns_csv"] == (
        "entsoe_neighbor_day_ahead_price_context"
    )
    assert row["mean_regret_improvement_ratio_vs_ukrainian_v2_plus"] == 0.2
    assert row["rolling_robustness_preserved"] is True


def test_market_coupling_ablation_blocks_median_degradation() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[20.0, 20.0, 20.0, 220.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=True),
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[20.0, 20.0, 20.0, 220.0],
            selected_regrets=[0.0, 0.0, 100.0, 100.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    row = frame.row(0, named=True)
    assert row["ablation_passed"] is False
    assert "median_degraded" in row["ablation_blocker"]


def test_market_coupling_ablation_scoring_changes_do_not_change_approved_route() -> None:
    route = _feature_route(approved=True)
    base = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        route,
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0],
            selected_regrets=[80.0, 80.0, 80.0, 80.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )
    mutated = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        route,
        market_coupled_strict_frame=_strict_frame(
            baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0],
            selected_regrets=[140.0, 140.0, 140.0, 140.0],
        ),
        market_coupled_robustness_frame=_robustness_frame(passed_windows=4),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    )

    base_row = base.row(0, named=True)
    mutated_row = mutated.row(0, named=True)
    assert base_row["approved_external_feature_columns_csv"] == mutated_row[
        "approved_external_feature_columns_csv"
    ]
    assert base_row["ablation_passed"] is True
    assert mutated_row["ablation_passed"] is False


def test_market_coupling_ablation_validation_rejects_false_claim_flags() -> None:
    frame = build_dfl_market_coupling_v2_plus_ablation_frame(
        _strict_frame(baseline_selected_regrets=[100.0, 100.0, 100.0, 100.0]),
        _robustness_frame(passed_windows=4),
        _feature_route(approved=False),
        source_model_names=(SOURCE,),
        min_tenant_count=2,
        min_validation_tenant_anchor_count=4,
    ).with_columns(pl.lit(False).alias("not_market_execution"))

    outcome = validate_dfl_market_coupling_v2_plus_ablation_evidence(
        frame,
        source_model_names=(SOURCE,),
    )

    assert outcome.passed is False
    assert "research-only claim flags" in outcome.description


def _feature_route(*, approved: bool) -> pl.DataFrame:
    status = "approved_for_training" if approved else "blocked_by_governance"
    blockers = "" if approved else "publication_time,prior_fx,licensing"
    return pl.DataFrame(
        [
            {
                "feature_name": "entsoe_neighbor_day_ahead_price_context",
                "source_name": "ENTSO_E",
                "source_kind": "neighbor_market_api",
                "approved_feature_column": "entsoe_neighbor_day_ahead_price_context",
                "feature_route_status": status,
                "source_backed_row_count": 24 if approved else 0,
                "training_use_allowed": approved,
                "feature_use_allowed": approved,
                "approved_for_official_training": approved,
                "training_blockers_csv": blockers,
                "readiness_status": "training_ready" if approved else "blocked",
                "licensing_status": "ready" if approved else "blocked",
                "timezone_status": "ready" if approved else "blocked",
                "currency_status": "ready" if approved else "blocked",
                "market_rules_status": "ready" if approved else "blocked",
                "temporal_availability_status": "ready" if approved else "blocked",
                "domain_shift_status": "ready" if approved else "blocked",
                "publication_time_policy": "must_be_published_before_ua_anchor",
                "decision_cutoff_policy": "ua_dam_day_ahead_cutoff",
                "external_feature_role": "future_market_coupling_covariate",
                "claim_scope": MARKET_COUPLING_FEATURE_ROUTE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        ]
    )


def _strict_frame(
    *,
    baseline_selected_regrets: list[float],
    selected_regrets: list[float] | None = None,
) -> pl.DataFrame:
    selected_values = selected_regrets or baseline_selected_regrets
    rows: list[dict[str, object]] = []
    tenants = ["client_001_kyiv_mall", "client_002_lviv_office"]
    first_anchor = datetime(2026, 4, 1, 23)
    role_regrets = {
        "strict_reference": [200.0] * len(selected_values),
        "raw_reference": [240.0] * len(selected_values),
        "schedule_value_learner_v2_reference": baseline_selected_regrets,
        "schedule_value_learner_v2_plus": selected_values,
    }
    for tenant_index, tenant_id in enumerate(tenants):
        for anchor_index, anchor_offset in enumerate(range(2)):
            anchor = first_anchor + timedelta(days=anchor_offset)
            value_index = tenant_index * 2 + anchor_index
            for role, regrets in role_regrets.items():
                regret = regrets[value_index]
                forecast_model_name = (
                    schedule_value_learner_v2_plus_model_name(SOURCE)
                    if role == "schedule_value_learner_v2_plus"
                    else f"{role}_{SOURCE}"
                )
                payload = {
                    "data_quality_tier": "thesis_grade",
                    "observed_coverage_ratio": 1.0,
                    "safety_violation_count": 0,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "selection_role": role,
                }
                rows.append(
                    {
                        "evaluation_id": f"{tenant_id}:{anchor_index}:{role}",
                        "tenant_id": tenant_id,
                        "source_model_name": SOURCE,
                        "forecast_model_name": forecast_model_name,
                        "strategy_kind": (
                            DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_LP_STRATEGY_KIND
                        ),
                        "market_venue": "DAM",
                        "anchor_timestamp": anchor,
                        "generated_at": datetime(2026, 5, 15),
                        "horizon_hours": 24,
                        "starting_soc_fraction": 0.5,
                        "starting_soc_source": "test_fixture",
                        "decision_value_uah": 1000.0 - regret,
                        "forecast_objective_value_uah": 1000.0 - regret,
                        "oracle_value_uah": 1000.0,
                        "regret_uah": regret,
                        "regret_ratio": regret / 1000.0,
                        "total_degradation_penalty_uah": 0.0,
                        "total_throughput_mwh": 0.0,
                        "committed_action": "HOLD",
                        "committed_power_mw": 0.0,
                        "rank_by_regret": 1,
                        "data_quality_tier": "thesis_grade",
                        "observed_coverage_ratio": 1.0,
                        "safety_violation_count": 0,
                        "selection_role": role,
                        "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_STRICT_CLAIM_SCOPE,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "evaluation_payload": payload,
                    }
                )
    return pl.DataFrame(rows)


def _robustness_frame(*, passed_windows: int) -> pl.DataFrame:
    rows: list[dict[str, object]] = []
    for window_index in range(1, 5):
        passed = window_index <= passed_windows
        rows.append(
            {
                "source_model_name": SOURCE,
                "window_index": window_index,
                "tenant_count": 2,
                "validation_anchor_count_per_tenant": 2,
                "validation_tenant_anchor_count": 4,
                "minimum_prior_anchor_count_before_window": 30,
                "strict_mean_regret_uah": 200.0,
                "raw_mean_regret_uah": 240.0,
                "v2_mean_regret_uah": 100.0,
                "selected_mean_regret_uah": 80.0 if passed else 210.0,
                "strict_median_regret_uah": 200.0,
                "v2_median_regret_uah": 100.0,
                "selected_median_regret_uah": 80.0 if passed else 210.0,
                "mean_regret_improvement_ratio_vs_raw": (
                    (240.0 - (80.0 if passed else 210.0)) / 240.0
                ),
                "mean_regret_improvement_ratio_vs_strict": (
                    (200.0 - (80.0 if passed else 210.0)) / 200.0
                ),
                "mean_regret_improvement_ratio_vs_v2": (
                    (100.0 - (80.0 if passed else 210.0)) / 100.0
                ),
                "development_passed": passed,
                "source_specific_strict_passed": passed,
                "v2_non_degradation_passed": passed,
                "v2_plus_window_passed": passed,
                "passing_window_count_for_source": passed_windows,
                "robust_research_challenger": passed_windows >= 3,
                "production_promote": False,
                "gate_label": "v2_plus_window_pass" if passed else "blocked",
                "claim_scope": DFL_SCHEDULE_VALUE_LEARNER_V2_PLUS_ROBUSTNESS_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
            }
        )
    return pl.DataFrame(rows)
