from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
import subprocess
import sys

import polars as pl

from smart_arbitrage.dfl.ua_context_acquisition_export import (
    build_dfl_ua_context_backfill_readiness_packet,
    write_dfl_ua_context_backfill_readiness_packet,
)
from smart_arbitrage.dfl.ua_v12_safe_teacher_export import (
    build_dfl_ua_v12_safe_teacher_backfill_packet,
    write_dfl_ua_v12_safe_teacher_backfill_packet,
)
from smart_arbitrage.dfl.ua_context_v13_acquisition_export import (
    build_dfl_ua_context_v13_acquisition_packet,
    write_dfl_ua_context_v13_acquisition_packet,
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


def test_ua_context_v13_acquisition_packet_exports_data_acquisition_needed(
    tmp_path,
) -> None:
    source_inventory = pl.DataFrame(
        [
            {
                "source_family": "measured_or_source_backed_tenant_load_pv",
                "source_status": "blocked_missing_source",
                "coverage_ratio": 0.0,
                "required_for_v13_candidate_generation": True,
                "market_execution_enabled": False,
            },
            {
                "source_family": "v12_safe_teacher_label_support",
                "source_status": "blocked_insufficient_safe_teacher_labels",
                "coverage_ratio": 0.35,
                "required_for_v13_candidate_generation": True,
                "market_execution_enabled": False,
            },
        ]
    )
    readiness = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "v13_candidate_generation_ready": False,
                "readiness_decision": "data_acquisition_needed",
                "blocking_context_families": (
                    "measured_or_source_backed_tenant_load_pv:"
                    "blocked_missing_source"
                ),
                "prior_material_safe_switch_example_count": 7,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "dt_lava_ready": False,
                "target_label_space": "v13_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "market_execution_enabled": False,
            }
        ]
    )
    receipt_source_audit = {
        "claim_scope": "oree_dam_publication_receipt_source_audit",
        "all_probes_insufficient_for_v13_receipts": True,
        "candidate_receipt_months": [],
        "candidate_receipt_source_found": False,
        "insufficient_months": ["01.2026", "02.2026", "03.2026", "04.2026"],
        "market_execution_enabled": False,
        "months_probed": ["01.2026", "02.2026", "03.2026", "04.2026"],
        "not_full_dfl": True,
        "not_market_execution": True,
        "probe_count": 4,
        "receipt_csv_generated": False,
    }
    acquisition_input_preflight = {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "config_path": "configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml",
        "dam_publication_receipts": {
            "configured": False,
            "path": None,
            "required_columns": ["timestamp", "source_publication_timestamp"],
            "status": "missing_config_path",
        },
        "data_acquisition_needed": True,
        "dt_lava_ready": False,
        "full_v13_gate_evaluated": False,
        "market_execution_enabled": False,
        "missing_required_inputs": [
            "oree_dam_publication_receipts_csv_path",
            "ua_context_safe_switch_examples_csv_path",
        ],
        "permits_model_training": False,
        "safe_switch_examples": {
            "configured": False,
            "path": None,
            "required_columns": [
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "split_name",
                "source_evidence_timestamp",
                "label_v13_material_safe_switch",
                "label_v13_tail_risk_loss",
            ],
            "status": "missing_config_path",
        },
        "v13_candidate_generation_ready": False,
    }

    packet = build_dfl_ua_context_v13_acquisition_packet(
        run_slug="test_v13_acquisition",
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_inventory,
        receipt_source_audit=receipt_source_audit,
        acquisition_input_preflight=acquisition_input_preflight,
        asset_check_status="passed",
    )
    export_dir = write_dfl_ua_context_v13_acquisition_packet(
        packet,
        output_root=tmp_path,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_inventory,
        receipt_source_audit=receipt_source_audit,
        acquisition_input_preflight=acquisition_input_preflight,
    )

    assert packet["v13_candidate_generation_ready"] is False
    assert packet["claim_boundary"]["market_execution_enabled"] is False
    assert packet["readiness_summary"]["readiness_decisions"] == [
        "data_acquisition_needed"
    ]
    assert packet["safe_switch_deficit_summary"] == {
        "blocked_tenant_source_count": 1,
        "max_missing_examples": 13,
        "total_missing_examples": 13,
        "tenant_source_deficits": [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "prior_material_safe_switch_example_count": 7,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "missing_prior_material_safe_switch_examples": 13,
                "dt_lava_ready": False,
                "readiness_decision": "data_acquisition_needed",
            }
        ],
    }
    assert packet["receipt_source_audit_summary"] == {
        "all_probes_insufficient_for_v13_receipts": True,
        "candidate_receipt_months": [],
        "candidate_receipt_source_found": False,
        "claim_scope": "oree_dam_publication_receipt_source_audit",
        "insufficient_months": ["01.2026", "02.2026", "03.2026", "04.2026"],
        "market_execution_enabled": False,
        "months_probed": ["01.2026", "02.2026", "03.2026", "04.2026"],
        "not_market_execution": True,
        "probe_count": 4,
        "receipt_csv_generated": False,
    }
    assert packet["acquisition_input_preflight_summary"] == {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "dam_publication_receipts_status": "missing_config_path",
        "data_acquisition_needed": True,
        "dt_lava_ready": False,
        "full_v13_gate_evaluated": False,
        "market_execution_enabled": False,
        "missing_required_inputs": [
            "oree_dam_publication_receipts_csv_path",
            "ua_context_safe_switch_examples_csv_path",
        ],
        "permits_model_training": False,
        "safe_switch_examples_status": "missing_config_path",
        "v13_candidate_generation_ready": False,
    }
    assert packet["attached_artifacts"]["receipt_source_audit_json"] == (
        "dfl_ua_context_v13_receipt_source_audit.json"
    )
    assert packet["attached_artifacts"]["acquisition_input_preflight_json"] == (
        "dfl_ua_context_v13_acquisition_input_preflight.json"
    )
    assert packet["attached_artifacts"]["source_acquisition_backlog_csv"] == (
        "dfl_ua_context_v13_source_acquisition_backlog.csv"
    )
    assert packet["safe_switch_acquisition_target_summary"] == {
        "target_tenant_source_count": 1,
        "total_new_prior_material_safe_switch_examples_required": 13,
        "max_new_prior_material_safe_switch_examples_required": 13,
        "target_rows": [
            {
                "acquisition_priority_rank": 1,
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "current_prior_material_safe_switch_examples": 7,
                "required_prior_material_safe_switch_examples": 20,
                "target_new_prior_material_safe_switch_examples": 13,
                "target_total_prior_material_safe_switch_examples": 20,
                "required_evidence_kind": (
                    "train_prior_non_tail_risk_material_safe_switch_rows"
                ),
                "blocking_context_families": (
                    "measured_or_source_backed_tenant_load_pv:"
                    "blocked_missing_source"
                ),
                "primary_blocking_source_family": (
                    "measured_or_source_backed_tenant_load_pv"
                ),
                "recommended_next_step": (
                    "acquire_ukrainian_context_and_backfill_safe_labels"
                ),
                "target_label_space": "v13_precondition_context_coverage",
                "source_readiness_required_before_dt_lava": True,
                "target_is_precondition_only": True,
                "market_execution_enabled": False,
            }
        ],
    }
    assert packet["source_acquisition_backlog_summary"] == {
        "backlog_item_count": 3,
        "source_family_blocker_count": 2,
        "safe_switch_target_count": 1,
        "market_execution_enabled": False,
        "permits_model_training": False,
        "top_priority_blocker": "measured_or_source_backed_tenant_load_pv",
    }
    assert (export_dir / "dfl_ua_context_v13_acquisition_summary.json").exists()
    assert (export_dir / "dfl_ua_context_v13_acquisition_summary.md").exists()
    assert "Safe-Switch Support Deficit" in (
        export_dir / "dfl_ua_context_v13_acquisition_summary.md"
    ).read_text(encoding="utf-8")
    assert "Safe-Switch Acquisition Targets" in (
        export_dir / "dfl_ua_context_v13_acquisition_summary.md"
    ).read_text(encoding="utf-8")
    assert "Acquisition Input Preflight" in (
        export_dir / "dfl_ua_context_v13_acquisition_summary.md"
    ).read_text(encoding="utf-8")
    assert (
        export_dir / "dfl_ua_context_v13_source_acquisition_evidence_rows.csv"
    ).exists()
    assert (export_dir / "dfl_ua_context_v13_source_inventory_rows.csv").exists()
    assert (export_dir / "dfl_ua_context_v13_readiness_rows.csv").exists()
    assert (
        export_dir / "dfl_ua_context_v13_safe_switch_acquisition_targets.csv"
    ).exists()
    backlog_path = export_dir / "dfl_ua_context_v13_source_acquisition_backlog.csv"
    assert backlog_path.exists()
    backlog = pl.read_csv(backlog_path)
    assert backlog["market_execution_enabled"].to_list() == [False, False, False]
    assert backlog["permits_model_training"].to_list() == [False, False, False]
    assert backlog["backlog_item_type"].to_list() == [
        "source_family_blocker",
        "safe_switch_target",
        "source_family_blocker",
    ]
    assert backlog["blocking_source_family"].to_list() == [
        "measured_or_source_backed_tenant_load_pv",
        "measured_or_source_backed_tenant_load_pv",
        "v12_safe_teacher_label_support",
    ]
    assert (export_dir / "dfl_ua_context_v13_receipt_source_audit.json").exists()
    exported_audit = json.loads(
        (export_dir / "dfl_ua_context_v13_receipt_source_audit.json").read_text(
            encoding="utf-8"
        )
    )
    assert exported_audit["candidate_receipt_source_found"] is False
    assert exported_audit["market_execution_enabled"] is False
    assert (
        export_dir / "dfl_ua_context_v13_acquisition_input_preflight.json"
    ).exists()
    exported_preflight = json.loads(
        (
            export_dir / "dfl_ua_context_v13_acquisition_input_preflight.json"
        ).read_text(encoding="utf-8")
    )
    assert exported_preflight["missing_required_inputs"] == [
        "oree_dam_publication_receipts_csv_path",
        "ua_context_safe_switch_examples_csv_path",
    ]
    assert exported_preflight["market_execution_enabled"] is False


def test_ua_context_v13_acquisition_packet_cli_accepts_csv_inputs(
    tmp_path: Path,
) -> None:
    source_inventory = pl.DataFrame(
        [
            {
                "source_family": "v12_safe_teacher_label_support",
                "source_status": "blocked_insufficient_safe_teacher_labels",
                "coverage_ratio": 0.35,
                "required_for_v13_candidate_generation": True,
                "market_execution_enabled": False,
            }
        ]
    )
    readiness = pl.DataFrame(
        [
            {
                "tenant_id": "tenant_a",
                "source_model_name": "model_a",
                "v13_candidate_generation_ready": False,
                "readiness_decision": "data_acquisition_needed",
                "blocking_context_families": (
                    "v12_safe_teacher_label_support:"
                    "blocked_insufficient_safe_teacher_labels"
                ),
                "prior_material_safe_switch_example_count": 7,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "dt_lava_ready": False,
                "target_label_space": "v13_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "market_execution_enabled": False,
            }
        ]
    )
    source_inventory_path = tmp_path / "source_inventory.csv"
    readiness_path = tmp_path / "readiness.csv"
    receipt_source_audit_path = tmp_path / "receipt_source_audit.json"
    acquisition_input_preflight_path = tmp_path / "acquisition_input_preflight.json"
    source_inventory.write_csv(source_inventory_path)
    readiness.write_csv(readiness_path)
    receipt_source_audit_path.write_text(
        json.dumps(
            {
                "claim_scope": "oree_dam_publication_receipt_source_audit",
                "all_probes_insufficient_for_v13_receipts": True,
                "candidate_receipt_months": [],
                "candidate_receipt_source_found": False,
                "insufficient_months": ["01.2026", "02.2026"],
                "market_execution_enabled": False,
                "months_probed": ["01.2026", "02.2026"],
                "not_full_dfl": True,
                "not_market_execution": True,
                "probe_count": 2,
                "receipt_csv_generated": False,
            }
        ),
        encoding="utf-8",
    )
    acquisition_input_preflight_path.write_text(
        json.dumps(
            {
                "claim_boundary": "v13_source_readiness_only_not_market_execution",
                "config_path": "configs/real_data_dfl_ua_context_v13_acquisition_week3.yaml",
                "dam_publication_receipts": {
                    "configured": False,
                    "path": None,
                    "required_columns": [
                        "timestamp",
                        "source_publication_timestamp",
                    ],
                    "status": "missing_config_path",
                },
                "data_acquisition_needed": True,
                "dt_lava_ready": False,
                "full_v13_gate_evaluated": False,
                "market_execution_enabled": False,
                "missing_required_inputs": [
                    "oree_dam_publication_receipts_csv_path"
                ],
                "permits_model_training": False,
                "safe_switch_examples": {
                    "configured": True,
                    "path": str(tmp_path / "safe_switch_examples.csv"),
                    "required_columns": [
                        "tenant_id",
                        "source_model_name",
                        "anchor_timestamp",
                        "split_name",
                        "source_evidence_timestamp",
                        "label_v13_material_safe_switch",
                        "label_v13_tail_risk_loss",
                    ],
                    "safe_switch_example_rows": 2,
                    "status": "validated",
                    "tenant_source_count": 1,
                },
                "v13_candidate_generation_ready": False,
            }
        ),
        encoding="utf-8",
    )

    result = subprocess.run(
        [
            sys.executable,
            "scripts/materialize_ua_context_v13_acquisition_packet.py",
            "--source-inventory-csv",
            str(source_inventory_path),
            "--readiness-csv",
            str(readiness_path),
            "--receipt-source-audit-json",
            str(receipt_source_audit_path),
            "--acquisition-input-preflight-json",
            str(acquisition_input_preflight_path),
            "--output-root",
            str(tmp_path),
            "--run-slug",
            "test_v13_csv_export",
        ],
        check=True,
        capture_output=True,
        text=True,
    )

    summary = json.loads(result.stdout)

    assert summary["safe_switch_deficit_summary"]["total_missing_examples"] == 13
    assert (
        summary["safe_switch_acquisition_target_summary"][
            "total_new_prior_material_safe_switch_examples_required"
        ]
        == 13
    )
    assert summary["source_acquisition_backlog_summary"] == {
        "backlog_item_count": 2,
        "source_family_blocker_count": 1,
        "safe_switch_target_count": 1,
        "market_execution_enabled": False,
        "permits_model_training": False,
        "top_priority_blocker": "v12_safe_teacher_label_support",
    }
    assert summary["market_execution_enabled"] is False
    assert (
        summary["receipt_source_audit_summary"]["candidate_receipt_source_found"]
        is False
    )
    assert summary["receipt_source_audit_summary"]["probe_count"] == 2
    assert summary["acquisition_input_preflight_summary"] == {
        "claim_boundary": "v13_source_readiness_only_not_market_execution",
        "dam_publication_receipts_status": "missing_config_path",
        "data_acquisition_needed": True,
        "dt_lava_ready": False,
        "full_v13_gate_evaluated": False,
        "market_execution_enabled": False,
        "missing_required_inputs": ["oree_dam_publication_receipts_csv_path"],
        "permits_model_training": False,
        "safe_switch_examples_status": "validated",
        "v13_candidate_generation_ready": False,
    }
    assert (
        summary["safe_switch_acquisition_target_summary"]["target_rows"][0][
            "primary_blocking_source_family"
        ]
        == "v12_safe_teacher_label_support"
    )
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_acquisition_summary.json"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_safe_switch_acquisition_targets.csv"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_source_acquisition_backlog.csv"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_receipt_source_audit.json"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_acquisition_input_preflight.json"
    ).exists()
