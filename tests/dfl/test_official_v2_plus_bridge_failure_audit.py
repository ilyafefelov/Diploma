from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.official_v2_plus_bridge_failure_audit import (
    build_dfl_official_v2_plus_bridge_failure_audit_frame,
    validate_dfl_official_v2_plus_bridge_failure_audit_evidence,
)
from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE,
    DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
    OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
FIRST_ANCHOR = datetime(2026, 4, 13)
GENERATED_AT = datetime(2026, 5, 17, 17, 37)


def test_official_bridge_failure_audit_reconciles_bridge_rows() -> None:
    bridge_frame = _bridge_frame(
        v2_plus_regret=180.0,
        residual_regret=330.0,
        dt_regret=340.0,
        behavior_cloning_regret=500.0,
        fallback_regret=330.0,
    )

    audit = build_dfl_official_v2_plus_bridge_failure_audit_frame(bridge_frame)
    evidence = validate_dfl_official_v2_plus_bridge_failure_audit_evidence(audit)

    assert evidence.passed is True
    assert audit.height == 720
    assert set(audit["analysis_only_challenger_role"].unique().to_list()) == {
        "residual_dfl_reference",
        "offline_dt_reference",
        "filtered_behavior_cloning_reference",
        "residual_dt_fallback_reference",
    }
    assert set(audit["analysis_only_failure_mode"].unique().to_list()) <= {
        "bad_teacher_target",
        "candidate_family_collapse",
        "dt_imitation_weaker_than_v2_selector",
        "horizon_credit_assignment_issue",
        "reward_scaling_issue",
        "weak_trajectory_objective",
    }
    assert audit["not_market_execution"].unique().to_list() == [True]
    assert audit["market_execution_enabled"].unique().to_list() == [False]


def test_official_bridge_failure_audit_score_mutation_does_not_change_families() -> None:
    bridge_frame = _bridge_frame(
        v2_plus_regret=180.0,
        residual_regret=330.0,
        dt_regret=340.0,
        behavior_cloning_regret=500.0,
        fallback_regret=330.0,
    )
    mutated = bridge_frame.with_columns(
        pl.when(pl.col("selection_role") == "offline_dt_reference")
        .then(pl.lit(125.0))
        .otherwise(pl.col("regret_uah"))
        .alias("regret_uah")
    )

    audit = build_dfl_official_v2_plus_bridge_failure_audit_frame(bridge_frame)
    mutated_audit = build_dfl_official_v2_plus_bridge_failure_audit_frame(mutated)

    stable_columns = [
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "analysis_only_challenger_role",
        "analysis_only_v2_plus_candidate_family",
        "analysis_only_challenger_candidate_family",
    ]
    assert audit.select(stable_columns).to_dicts() == mutated_audit.select(
        stable_columns
    ).to_dicts()
    assert (
        audit.filter(pl.col("analysis_only_challenger_role") == "offline_dt_reference")[
            "analysis_only_regret_delta_vs_v2_plus_uah"
        ].mean()
        != mutated_audit.filter(
            pl.col("analysis_only_challenger_role") == "offline_dt_reference"
        )["analysis_only_regret_delta_vs_v2_plus_uah"].mean()
    )


def test_official_bridge_failure_audit_requires_v2_plus_and_behavior_cloning() -> None:
    bridge_frame = _bridge_frame(
        v2_plus_regret=180.0,
        residual_regret=330.0,
        dt_regret=340.0,
        behavior_cloning_regret=500.0,
        fallback_regret=330.0,
    )

    with pytest.raises(ValueError, match="missing required bridge role"):
        build_dfl_official_v2_plus_bridge_failure_audit_frame(
            bridge_frame.filter(
                pl.col("selection_role") != "schedule_value_learner_v2_plus_reference"
            )
        )
    with pytest.raises(ValueError, match="missing required bridge role"):
        build_dfl_official_v2_plus_bridge_failure_audit_frame(
            bridge_frame.filter(
                pl.col("selection_role") != "filtered_behavior_cloning_reference"
            )
        )


def test_official_bridge_failure_audit_rejects_safety_and_claim_violations() -> None:
    bridge_frame = _bridge_frame(
        v2_plus_regret=180.0,
        residual_regret=330.0,
        dt_regret=340.0,
        behavior_cloning_regret=500.0,
        fallback_regret=330.0,
    )

    with pytest.raises(ValueError, match="safety violations"):
        build_dfl_official_v2_plus_bridge_failure_audit_frame(
            bridge_frame.with_columns(pl.lit(1).alias("safety_violation_count"))
        )
    with pytest.raises(ValueError, match="non-thesis rows"):
        build_dfl_official_v2_plus_bridge_failure_audit_frame(
            bridge_frame.with_columns(pl.lit("coverage_gap").alias("data_quality_tier"))
        )
    with pytest.raises(ValueError, match="claim boundary"):
        build_dfl_official_v2_plus_bridge_failure_audit_frame(
            bridge_frame.with_columns(pl.lit(False).alias("not_market_execution"))
        )


def _bridge_frame(
    *,
    v2_plus_regret: float,
    residual_regret: float,
    dt_regret: float,
    behavior_cloning_regret: float,
    fallback_regret: float,
) -> pl.DataFrame:
    role_regrets = {
        "strict_reference": 310.0,
        "schedule_value_learner_v2_plus_reference": v2_plus_regret,
        "residual_dfl_reference": residual_regret,
        "offline_dt_reference": dt_regret,
        "filtered_behavior_cloning_reference": behavior_cloning_regret,
        "residual_dt_fallback_reference": fallback_regret,
    }
    role_families = {
        "strict_reference": "strict_control",
        "schedule_value_learner_v2_plus_reference": "strict_prior_residual_v2",
        "residual_dfl_reference": "strict_raw_blend_v2",
        "offline_dt_reference": "strict_raw_blend_v2",
        "filtered_behavior_cloning_reference": "forecast_perturbation",
        "residual_dt_fallback_reference": "strict_raw_blend_v2",
    }
    rows: list[dict[str, object]] = []
    for source_model_name in OFFICIAL_GLOBAL_PANEL_V2_PLUS_SOURCE_MODELS:
        for tenant_id in TENANTS:
            for anchor_index in range(18):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                for role, regret in role_regrets.items():
                    rows.append(
                        _bridge_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            anchor=anchor,
                            role=role,
                            regret=regret,
                            candidate_family=role_families[role],
                        )
                    )
    return pl.DataFrame(rows)


def _bridge_row(
    *,
    tenant_id: str,
    source_model_name: str,
    anchor: datetime,
    role: str,
    regret: float,
    candidate_family: str,
) -> dict[str, object]:
    oracle_value = 1000.0
    return {
        "evaluation_id": f"{tenant_id}:{source_model_name}:{role}:{anchor.isoformat()}",
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "forecast_model_name": role,
        "strategy_kind": DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor,
        "generated_at": GENERATED_AT,
        "horizon_hours": 24,
        "starting_soc_fraction": 0.5,
        "starting_soc_source": "official_v2_plus_candidate_library",
        "decision_value_uah": oracle_value - regret,
        "forecast_objective_value_uah": oracle_value - regret - 10.0,
        "oracle_value_uah": oracle_value,
        "regret_uah": regret,
        "regret_ratio": regret / oracle_value,
        "total_degradation_penalty_uah": 0.0,
        "total_throughput_mwh": 1.0 if role != "schedule_value_learner_v2_plus_reference" else 0.7,
        "committed_action": "HOLD",
        "committed_power_mw": 0.0,
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": 0,
        "selection_role": role,
        "claim_scope": DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "selected_strategy_source": role,
        "evaluation_payload": {
            "candidate_family": candidate_family,
            "market_execution_enabled": False,
            "not_full_dfl": True,
            "not_market_execution": True,
            "safety_violation_count": 0,
            "forecast_diagnostics": {
                "spread_ranking_quality": 0.85,
                "top_k_price_recall": 0.5 if role != "schedule_value_learner_v2_plus_reference" else 0.8,
            },
            "horizon": _horizon(role),
        },
    }


def _horizon(role: str) -> list[dict[str, float | int | str]]:
    high_price_hours = {8, 9, 18, 19}
    if role == "schedule_value_learner_v2_plus_reference":
        discharge_hours = {8, 18}
    elif role == "filtered_behavior_cloning_reference":
        discharge_hours = {2, 3}
    else:
        discharge_hours = {4, 5}
    return [
        {
            "step_index": hour,
            "interval_start": (FIRST_ANCHOR + timedelta(hours=hour)).isoformat(),
            "forecast_price_uah_mwh": 4000.0 if hour in high_price_hours else 1000.0,
            "actual_price_uah_mwh": 5000.0 if hour in high_price_hours else 900.0,
            "net_power_mw": 0.14 if hour in discharge_hours else -0.04,
            "degradation_penalty_uah": 10.0 if hour in discharge_hours else 1.0,
        }
        for hour in range(24)
    ]
