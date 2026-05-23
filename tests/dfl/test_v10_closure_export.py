from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from smart_arbitrage.dfl.v10_closure_export import (
    build_dfl_v10_tail_risk_transfer_closure_packet,
    write_dfl_v10_tail_risk_transfer_closure_packet,
)


def test_v10_closure_packet_exports_negative_evidence(tmp_path) -> None:
    audit = _audit_frame(market_execution_enabled=False)
    decision = _decision_frame(market_execution_enabled=False)

    packet = build_dfl_v10_tail_risk_transfer_closure_packet(
        run_slug="week3-v10-closure",
        tail_risk_audit_frame=audit,
        learning_ceiling_decision_frame=decision,
        dagster_run_id="9e16fa67-566c-41c3-9de4-82a1dfb972a9",
        asset_check_status="passed",
    )
    export_dir = write_dfl_v10_tail_risk_transfer_closure_packet(
        packet,
        output_root=tmp_path,
        tail_risk_audit_frame=audit,
        learning_ceiling_decision_frame=decision,
    )

    assert packet["negative_evidence"] is True
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["learning_ceiling_decision"]["dt_lava_ready"] is False
    assert packet["learning_ceiling_decision"][
        "v10_learning_ceiling_decision"
    ] == "stop_modeling_current_candidate_space"
    assert (
        export_dir / "dfl_v10_tail_risk_transfer_closure_summary.json"
    ).exists()
    assert (
        export_dir / "dfl_v10_tail_risk_transfer_closure_summary.md"
    ).exists()
    assert (export_dir / "dfl_v10_tail_risk_transfer_audit_rows.csv").exists()
    assert (export_dir / "dfl_v10_learning_ceiling_decision_rows.csv").exists()
    markdown = (
        export_dir / "dfl_v10_tail_risk_transfer_closure_summary.md"
    ).read_text(encoding="utf-8")
    assert "Negative Evidence Result" in markdown
    assert "stop_modeling_current_candidate_space" in markdown
    assert "market_execution_enabled=false" in markdown
    assert "0 final non-tail-risk safe switches" in markdown


def test_v10_closure_packet_refuses_market_execution() -> None:
    with pytest.raises(ValueError, match="market execution"):
        build_dfl_v10_tail_risk_transfer_closure_packet(
            run_slug="invalid",
            tail_risk_audit_frame=_audit_frame(market_execution_enabled=True),
            learning_ceiling_decision_frame=_decision_frame(
                market_execution_enabled=False
            ),
        )


def _audit_frame(*, market_execution_enabled: bool) -> pl.DataFrame:
    anchor = datetime(2026, 5, 22, 12)
    return pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "anchor_timestamp": anchor,
                "split_name": "final_holdout",
                "anchor_key": "tenant_a|source|2026-05-22T12:00:00",
                "candidate_key": "candidate-a",
                "candidate_source": "oracle_template_v10_generated_candidate",
                "candidate_family": "forecast_extrema_shift_template",
                "candidate_model_name": "template-a",
                "candidate_regret_uah": 360.0,
                "v2_plus_regret_uah": 120.0,
                "candidate_delta_vs_v2_plus_uah": 240.0,
                "label_v10_material_safe_switch": False,
                "label_v10_tail_risk_loss": True,
                "v10_transfer_failure_class": "forecast_extrema_shift",
                "diagnostic_template_source_anchor_timestamp": anchor,
                "diagnostic_template_source_split_name": "train_selection",
                "diagnostic_template_candidate_key": "prior-candidate",
                "diagnostic_template_profile_key": "profile-a",
                "selector_feature_v10_template_safe_win_count": 1.0,
                "selector_feature_v10_template_tail_risk_count": 0.0,
                "selector_feature_v10_template_mean_delta_uah": -80.0,
                "selector_feature_v10_template_best_delta_uah": -90.0,
                "market_execution_enabled": market_execution_enabled,
            }
        ],
        infer_schema_length=None,
    )


def _decision_frame(*, market_execution_enabled: bool) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "generated_candidate_count": 1,
                "final_generated_candidate_count": 1,
                "prior_generated_candidate_count": 0,
                "prior_generated_material_safe_switch_count": 0,
                "final_generated_material_safe_switch_count": 0,
                "final_generated_non_tail_risk_material_safe_switch_count": 0,
                "final_generated_tail_risk_count": 1,
                "final_generated_missing_prior_context_count": 0,
                "v2_plus_final_mean_regret_uah": 120.0,
                "v10_non_tail_risk_oracle_final_mean_regret_uah": 120.0,
                "v10_non_tail_risk_oracle_upper_bound_improvement_ratio_vs_v2_plus": 0.0,
                "min_oracle_improvement_ratio_vs_v2_plus": 0.05,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "v10_learning_ceiling_decision": "stop_modeling_current_candidate_space",
                "dt_lava_ready": False,
                "recommended_next_branch": "thesis_ml_closure_and_data_acquisition",
                "market_execution_enabled": market_execution_enabled,
            }
        ],
        infer_schema_length=None,
    )
