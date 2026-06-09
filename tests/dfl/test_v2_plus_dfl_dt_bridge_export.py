from __future__ import annotations

from datetime import datetime, timedelta

import polars as pl
import pytest

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge_export import (
    build_dfl_v2_plus_dfl_dt_bridge_packet,
    write_dfl_v2_plus_dfl_dt_bridge_packet,
)
from smart_arbitrage.dfl.official_v2_plus_dfl_dt_bridge import (
    DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE,
    DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
)

TENANTS: tuple[str, ...] = (
    "client_001_kyiv_mall",
    "client_002_lviv_office",
    "client_003_dnipro_factory",
    "client_004_kharkiv_hospital",
    "client_005_odesa_hotel",
)
SOURCE_MODELS: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0")
FIRST_ANCHOR = datetime(2026, 4, 13, 23)
GENERATED_AT = datetime(2026, 5, 17, 12)


def test_bridge_packet_exports_negative_evidence_when_gate_blocks(tmp_path) -> None:
    frame = _bridge_frame(
        strict_regret=310.0,
        v2_plus_regret=180.0,
        residual_regret=210.0,
        offline_dt_regret=220.0,
        behavior_cloning_regret=230.0,
        fallback_regret=210.0,
    )

    packet = build_dfl_v2_plus_dfl_dt_bridge_packet(
        run_slug="compact-bridge-negative",
        strict_frame=frame,
        dagster_run_id="run-compact",
        asset_check_status="passed",
    )
    export_dir = write_dfl_v2_plus_dfl_dt_bridge_packet(
        packet,
        output_root=tmp_path,
        strict_frame=frame,
    )

    assert packet["evidence_check"]["passed"] is True
    assert packet["gate"]["passed"] is False
    assert packet["negative_evidence"] is True
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["claim_boundary"]["offline_strategy_promotion_only"] is True
    assert (export_dir / "dfl_v2_plus_dfl_dt_bridge_summary.json").exists()
    assert (export_dir / "dfl_v2_plus_dfl_dt_bridge_summary.md").exists()
    assert (export_dir / "dfl_v2_plus_dfl_dt_bridge_rows.csv").exists()
    markdown = (export_dir / "dfl_v2_plus_dfl_dt_bridge_summary.md").read_text(
        encoding="utf-8"
    )
    assert "Negative Evidence Result" in markdown
    assert "compact residual DFL / offline DT did not beat V2+" in markdown
    assert "market_execution_enabled=false" in markdown


def test_official_bridge_packet_exports_negative_evidence_with_official_scope(
    tmp_path,
) -> None:
    frame = _bridge_frame(
        strict_regret=310.0,
        v2_plus_regret=180.0,
        residual_regret=330.0,
        offline_dt_regret=340.0,
        behavior_cloning_regret=500.0,
        fallback_regret=330.0,
        strategy_kind=DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_STRICT_LP_STRATEGY_KIND,
        claim_scope=DFL_OFFICIAL_GLOBAL_PANEL_V2_PLUS_DFL_DT_BRIDGE_CLAIM_SCOPE,
        source_models=(
            "nbeatsx_official_global_panel_v1",
            "nbeatsx_official_global_panel_horizon_calibrated_v1",
        ),
    )

    packet = build_dfl_v2_plus_dfl_dt_bridge_packet(
        run_slug="official-bridge-negative",
        strict_frame=frame,
        dagster_run_id="53efba76-38cb-4624-9cd8-e15fb8c1c7a9",
        asset_check_status="passed",
    )
    export_dir = write_dfl_v2_plus_dfl_dt_bridge_packet(
        packet,
        output_root=tmp_path,
        strict_frame=frame,
    )

    assert packet["bridge_scope"] == "official_global_panel_v2_plus_teacher"
    assert packet["negative_evidence"] is True
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    markdown = (export_dir / "dfl_v2_plus_dfl_dt_bridge_summary.md").read_text(
        encoding="utf-8"
    )
    assert "official global-panel V2+-teacher residual DFL / offline DT did not beat V2+" in markdown
    assert "53efba76-38cb-4624-9cd8-e15fb8c1c7a9" in markdown


def test_bridge_packet_refuses_structurally_invalid_evidence() -> None:
    frame = _bridge_frame(
        strict_regret=310.0,
        v2_plus_regret=180.0,
        residual_regret=210.0,
        offline_dt_regret=220.0,
        behavior_cloning_regret=230.0,
        fallback_regret=210.0,
    )

    with pytest.raises(ValueError, match="evidence check failed"):
        build_dfl_v2_plus_dfl_dt_bridge_packet(
            run_slug="invalid",
            strict_frame=frame.drop("selection_role"),
        )


def test_bridge_packet_records_passing_challenger_without_market_execution() -> None:
    frame = _bridge_frame(
        strict_regret=310.0,
        v2_plus_regret=180.0,
        residual_regret=120.0,
        offline_dt_regret=220.0,
        behavior_cloning_regret=230.0,
        fallback_regret=120.0,
    )

    packet = build_dfl_v2_plus_dfl_dt_bridge_packet(
        run_slug="compact-bridge-positive",
        strict_frame=frame,
    )

    assert packet["gate"]["passed"] is True
    assert packet["negative_evidence"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["gate"]["metrics"]["best_challenger_role"] in {
        "residual_dfl_reference",
        "residual_dt_fallback_reference",
    }


def _bridge_frame(
    *,
    strict_regret: float,
    v2_plus_regret: float,
    residual_regret: float,
    offline_dt_regret: float,
    behavior_cloning_regret: float,
    fallback_regret: float,
    strategy_kind: str = "dfl_v2_plus_dfl_dt_bridge_strict_lp_benchmark",
    claim_scope: str = "dfl_v2_plus_dfl_dt_bridge_not_full_dfl",
    source_models: tuple[str, ...] = SOURCE_MODELS,
) -> pl.DataFrame:
    role_regrets = {
        "strict_reference": strict_regret,
        "schedule_value_learner_v2_plus_reference": v2_plus_regret,
        "residual_dfl_reference": residual_regret,
        "offline_dt_reference": offline_dt_regret,
        "filtered_behavior_cloning_reference": behavior_cloning_regret,
        "residual_dt_fallback_reference": fallback_regret,
    }
    rows: list[dict[str, object]] = []
    for source_model_name in source_models:
        for tenant_id in TENANTS:
            for anchor_index in range(18):
                anchor = FIRST_ANCHOR + timedelta(days=anchor_index)
                for role, regret in role_regrets.items():
                    rows.append(
                        {
                            "evaluation_id": f"{tenant_id}:{source_model_name}:{role}:{anchor_index}",
                            "tenant_id": tenant_id,
                            "source_model_name": source_model_name,
                            "forecast_model_name": role,
                            "strategy_kind": strategy_kind,
                            "market_venue": "DAM",
                            "anchor_timestamp": anchor,
                            "generated_at": GENERATED_AT,
                            "horizon_hours": 24,
                            "starting_soc_fraction": 0.5,
                            "starting_soc_source": "schedule_candidate_library_v2",
                            "decision_value_uah": 1000.0 - regret,
                            "forecast_objective_value_uah": 900.0,
                            "oracle_value_uah": 1000.0,
                            "regret_uah": regret,
                            "regret_ratio": regret / 1000.0,
                            "total_degradation_penalty_uah": 0.0,
                            "total_throughput_mwh": 1.0,
                            "committed_action": "HOLD",
                            "committed_power_mw": 0.0,
                            "rank_by_regret": 1,
                            "data_quality_tier": "thesis_grade",
                            "observed_coverage_ratio": 1.0,
                            "safety_violation_count": 0,
                            "selection_role": role,
                            "selected_strategy_source": role,
                            "claim_scope": claim_scope,
                            "not_full_dfl": True,
                            "not_market_execution": True,
                            "evaluation_payload": {
                                "source_forecast_model_name": source_model_name,
                                "market_execution_enabled": False,
                                "not_full_dfl": True,
                                "not_market_execution": True,
                                "safety_violation_count": 0,
                            },
                        }
                    )
    return pl.DataFrame(rows)
