from __future__ import annotations

from datetime import datetime

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_export import (
    build_dfl_ua_context_backfill_readiness_packet,
    write_dfl_ua_context_backfill_readiness_packet,
)
from smart_arbitrage.dfl.ua_v12_safe_teacher_export import (
    build_dfl_ua_v12_safe_teacher_backfill_packet,
    write_dfl_ua_v12_safe_teacher_backfill_packet,
)


def test_ua_context_backfill_readiness_packet_exports_blocked_gate(tmp_path) -> None:
    source_inventory = pl.DataFrame(
        [
            {
                "source_family": "oree_dam_publication",
                "source_rows": 1,
                "required_anchor_rows": 1,
                "publication_metadata_supported": False,
                "prior_timestamp_supported": True,
                "market_execution_enabled": False,
            }
        ]
    )
    gate = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "anchor_timestamp": datetime(2026, 4, 29, 23),
                "v11_candidate_generation_ready": False,
                "context_backfill_gate_decision": "data_acquisition_needed",
                "blocking_context_families": "dam_publication:missing_publication_time",
                "market_execution_enabled": False,
            }
        ]
    )
    empty_family = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "anchor_timestamp": datetime(2026, 4, 29, 23),
                "prior_available": False,
                "market_execution_enabled": False,
            }
        ]
    )

    packet = build_dfl_ua_context_backfill_readiness_packet(
        run_slug="test_ua_context_acquisition",
        source_inventory_frame=source_inventory,
        dam_publication_frame=empty_family,
        weather_load_pv_frame=empty_family,
        grid_event_frame=empty_family,
        calendar_block_frame=empty_family,
        coverage_gate_frame=gate,
        asset_check_status="passed",
    )
    export_dir = write_dfl_ua_context_backfill_readiness_packet(
        packet,
        output_root=tmp_path,
        source_inventory_frame=source_inventory,
        dam_publication_frame=empty_family,
        weather_load_pv_frame=empty_family,
        grid_event_frame=empty_family,
        calendar_block_frame=empty_family,
        coverage_gate_frame=gate,
    )

    assert packet["v11_candidate_generation_ready"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert (export_dir / "dfl_ua_context_backfill_readiness_summary.json").exists()
    assert (export_dir / "dfl_ua_context_backfill_readiness_summary.md").exists()
    assert (export_dir / "dfl_ua_context_source_inventory_rows.csv").exists()
    assert (export_dir / "dfl_ua_context_family_coverage_rows.csv").exists()
    assert (export_dir / "dfl_ua_context_v11_gate_decision_rows.csv").exists()


def test_ua_v12_safe_teacher_packet_exports_blocked_readiness(tmp_path) -> None:
    source_inventory = pl.DataFrame(
        [
            {
                "source_family": "measured_tenant_load_pv",
                "source_status": "blocked_missing_source",
                "coverage_ratio": 0.0,
                "source_rows": 0,
                "required_anchor_rows": 1,
                "required_for_v12_candidate_generation": False,
                "market_execution_enabled": False,
            }
        ]
    )
    context_panel = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "anchor_timestamp": datetime(2026, 4, 29, 23),
                "v12_existing_source_context_ready": True,
                "v12_context_expansion_decision": "context_backfill_ready",
                "market_execution_enabled": False,
            }
        ]
    )
    teacher = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "split_name": "train",
                "candidate_source": "ua_low_tail_v12_generated_candidate",
                "label_v12_material_safe_switch": True,
                "label_v12_tail_risk_loss": False,
                "market_execution_enabled": False,
            }
        ]
    )
    candidates = teacher.with_columns(
        pl.lit("pending_strict_rescore").alias("candidate_value_label_status")
    )
    strict_rescore = candidates.with_columns(
        pl.lit("strict_rescored_v12_candidate").alias("candidate_value_label_status")
    )
    readiness = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "prior_material_safe_switch_example_count": 1,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "dt_lava_ready": False,
                "readiness_decision": (
                    "blocked_insufficient_prior_safe_switch_examples"
                ),
                "target_label_space": "schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "market_execution_enabled": False,
            }
        ]
    )

    packet = build_dfl_ua_v12_safe_teacher_backfill_packet(
        run_slug="test_v12_safe_teacher",
        source_inventory_frame=source_inventory,
        expanded_context_panel_frame=context_panel,
        safe_teacher_label_panel_frame=teacher,
        low_tail_candidate_library_frame=candidates,
        low_tail_strict_rescore_frame=strict_rescore,
        readiness_decision_frame=readiness,
        asset_check_status="passed",
    )
    export_dir = write_dfl_ua_v12_safe_teacher_backfill_packet(
        packet,
        output_root=tmp_path,
        source_inventory_frame=source_inventory,
        expanded_context_panel_frame=context_panel,
        safe_teacher_label_panel_frame=teacher,
        low_tail_candidate_library_frame=candidates,
        low_tail_strict_rescore_frame=strict_rescore,
        readiness_decision_frame=readiness,
    )

    assert packet["dt_lava_ready"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert (export_dir / "dfl_ua_v12_safe_teacher_summary.json").exists()
    assert (export_dir / "dfl_ua_v12_safe_teacher_summary.md").exists()
    assert (export_dir / "dfl_ua_v12_source_inventory_rows.csv").exists()
    assert (export_dir / "dfl_ua_v12_readiness_decision_rows.csv").exists()
