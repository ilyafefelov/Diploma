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
    receipt_source_lead_audit = {
        "claim_scope": "v13_dam_receipt_source_lead_audit_not_market_execution",
        "lead_count": 3,
        "candidate_receipt_source_found": False,
        "candidate_receipt_lead_count": 0,
        "candidate_receipt_lead_ids": [],
        "dataset_level_metadata_only_count": 1,
        "auth_blocked_count": 1,
        "probe_negative_count": 1,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "lead_rows": [
            {
                "lead_id": "energy_map_dam_indexes_dataset",
                "source_url": "https://energy-map.info/en/datasets/example",
                "source_title": "Energy Map DAM indexes",
                "lead_status": "insufficient_dataset_level_metadata_only",
                "blocking_reasons": ["dataset_level_metadata_only"],
            },
            {
                "lead_id": "energy_map_dam_download_path",
                "source_url": "https://energy-map.info/en/datasets/example",
                "source_title": "Energy Map DAM download path",
                "lead_status": "blocked_auth_required",
                "blocking_reasons": ["download_auth_required"],
            },
            {
                "lead_id": "oree_data_view_monthly_probe",
                "source_url": "https://www.oree.com.ua/index.php/pricectr/data_view",
                "source_title": "OREE data_view monthly probe",
                "lead_status": "blocked_negative_probe",
                "blocking_reasons": [
                    "source_probe_not_sufficient_for_v13_receipts"
                ],
            },
        ],
    }
    safe_switch_candidate_audits = [
        {
            "claim_scope": "v13_safe_switch_candidate_source_audit_not_market_execution",
            "source_rows": 23499,
            "material_label_column": "label_v12_material_safe_switch",
            "tail_risk_label_column": "label_v12_tail_risk_loss",
            "source_evidence_timestamp_column": "generated_at",
            "uses_canonical_v13_labels": False,
            "accepted_candidate_rows": 0,
            "weak_safe_switch_win_rows": 348,
            "duplicate_accepted_rows": 0,
            "accepted_tenant_source_count": 0,
            "normalized_safe_switch_csv_ready": False,
            "blocking_reasons": [
                "noncanonical_material_label_column:label_v12_material_safe_switch",
                "noncanonical_tail_risk_label_column:label_v12_tail_risk_loss",
                "noncanonical_source_evidence_timestamp_column:generated_at",
            ],
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        },
        {
            "claim_scope": "v13_safe_switch_candidate_source_audit_not_market_execution",
            "source_rows": 23499,
            "material_label_column": "label_v11_material_safe_switch",
            "tail_risk_label_column": "label_v11_tail_risk_loss",
            "source_evidence_timestamp_column": "generated_at",
            "uses_canonical_v13_labels": False,
            "accepted_candidate_rows": 19,
            "weak_safe_switch_win_rows": 348,
            "duplicate_accepted_rows": 8,
            "accepted_tenant_source_count": 5,
            "normalized_safe_switch_csv_ready": False,
            "blocking_reasons": [
                "noncanonical_material_label_column:label_v11_material_safe_switch",
                "noncanonical_tail_risk_label_column:label_v11_tail_risk_loss",
                "noncanonical_source_evidence_timestamp_column:generated_at",
                "duplicate_accepted_candidate_rows",
            ],
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        },
    ]
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
    scmo_ws_security_preflight = {
        "claim_scope": "scmo_ws_security_credential_preflight",
        "credential_material_ready": False,
        "credential_material_present": False,
        "credential_file_pair_valid": False,
        "credential_material_validation_status": "missing_env_or_file",
        "signed_download_request_ready": False,
        "ws_security_signature_supported": False,
        "ws_security_signature_status": "xml_signature_not_implemented",
        "missing_env_vars": [
            "SCMO_USERNAME",
            "SCMO_PASSWORD",
            "SCMO_CLIENT_CERT_PEM",
            "SCMO_CLIENT_KEY_PEM",
        ],
        "missing_files": [],
        "username_present": False,
        "password_present": False,
        "client_cert_path_present": False,
        "client_key_path_present": False,
        "client_key_password_present": False,
        "secret_values_written": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }

    packet = build_dfl_ua_context_v13_acquisition_packet(
        run_slug="test_v13_acquisition",
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_inventory,
        receipt_source_audit=receipt_source_audit,
        receipt_source_lead_audit=receipt_source_lead_audit,
        safe_switch_candidate_audits=safe_switch_candidate_audits,
        acquisition_input_preflight=acquisition_input_preflight,
        scmo_ws_security_preflight=scmo_ws_security_preflight,
        asset_check_status="passed",
    )
    export_dir = write_dfl_ua_context_v13_acquisition_packet(
        packet,
        output_root=tmp_path,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        acquisition_source_evidence_frame=source_inventory,
        receipt_source_audit=receipt_source_audit,
        receipt_source_lead_audit=receipt_source_lead_audit,
        safe_switch_candidate_audits=safe_switch_candidate_audits,
        acquisition_input_preflight=acquisition_input_preflight,
        scmo_ws_security_preflight=scmo_ws_security_preflight,
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
    assert packet["receipt_source_lead_audit_summary"] == {
        "auth_blocked_count": 1,
        "candidate_receipt_lead_count": 0,
        "candidate_receipt_lead_ids": [],
        "candidate_receipt_source_found": False,
        "claim_scope": "v13_dam_receipt_source_lead_audit_not_market_execution",
        "dataset_level_metadata_only_count": 1,
        "lead_count": 3,
        "market_execution_enabled": False,
        "permits_model_training": False,
        "probe_negative_count": 1,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
    }
    assert packet["safe_switch_candidate_audit_summary"] == {
        "accepted_tenant_source_count": 5,
        "audit_count": 2,
        "audit_rows": [
            {
                "accepted_candidate_rows": 0,
                "blocking_reasons": [
                    "noncanonical_material_label_column:label_v12_material_safe_switch",
                    "noncanonical_tail_risk_label_column:label_v12_tail_risk_loss",
                    "noncanonical_source_evidence_timestamp_column:generated_at",
                ],
                "duplicate_accepted_rows": 0,
                "material_label_column": "label_v12_material_safe_switch",
                "normalized_safe_switch_csv_ready": False,
                "source_rows": 23499,
                "uses_canonical_v13_labels": False,
                "weak_safe_switch_win_rows": 348,
            },
            {
                "accepted_candidate_rows": 19,
                "blocking_reasons": [
                    "noncanonical_material_label_column:label_v11_material_safe_switch",
                    "noncanonical_tail_risk_label_column:label_v11_tail_risk_loss",
                    "noncanonical_source_evidence_timestamp_column:generated_at",
                    "duplicate_accepted_candidate_rows",
                ],
                "duplicate_accepted_rows": 8,
                "material_label_column": "label_v11_material_safe_switch",
                "normalized_safe_switch_csv_ready": False,
                "source_rows": 23499,
                "uses_canonical_v13_labels": False,
                "weak_safe_switch_win_rows": 348,
            },
        ],
        "dt_lava_ready": False,
        "market_execution_enabled": False,
        "normalized_safe_switch_csv_ready_count": 0,
        "permits_model_training": False,
        "total_accepted_candidate_rows": 19,
        "total_source_rows": 46998,
        "total_weak_safe_switch_win_rows": 696,
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
    assert packet["scmo_ws_security_preflight_summary"] == {
        "claim_scope": "scmo_ws_security_credential_preflight",
        "client_cert_path_present": False,
        "client_key_path_present": False,
        "client_key_password_present": False,
        "client_p12_path_present": False,
        "client_p12_password_present": False,
        "credential_file_pair_valid": False,
        "credential_material_format": "missing",
        "credential_material_ready": False,
        "credential_material_present": False,
        "credential_material_validation_status": "missing_env_or_file",
        "dt_lava_ready": False,
        "market_execution_enabled": False,
        "missing_env_vars": [
            "SCMO_USERNAME",
            "SCMO_PASSWORD",
            "SCMO_CLIENT_CERT_PEM",
            "SCMO_CLIENT_KEY_PEM",
        ],
        "missing_files": [],
        "mtls_client_cert_ready": False,
        "password_present": False,
        "pem_cert_key_pair_present": False,
        "pkcs12_bundle_present": False,
        "permits_model_training": False,
        "receipt_csv_generated": False,
        "secret_values_written": False,
        "signed_download_request_ready": False,
        "username_present": False,
        "validated_receipt_csv_ready": False,
        "ws_security_signature_status": "xml_signature_not_implemented",
        "ws_security_signature_supported": False,
    }
    assert packet["attached_artifacts"]["receipt_source_audit_json"] == (
        "dfl_ua_context_v13_receipt_source_audit.json"
    )
    assert packet["attached_artifacts"]["receipt_source_lead_audit_json"] == (
        "dfl_ua_context_v13_receipt_source_lead_audit.json"
    )
    assert packet["attached_artifacts"]["safe_switch_candidate_audits_json"] == (
        "dfl_ua_context_v13_safe_switch_candidate_audits.json"
    )
    assert packet["attached_artifacts"]["acquisition_input_preflight_json"] == (
        "dfl_ua_context_v13_acquisition_input_preflight.json"
    )
    assert packet["attached_artifacts"]["scmo_ws_security_preflight_json"] == (
        "dfl_ua_context_v13_scmo_ws_security_preflight.json"
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
        "backlog_item_count": 6,
        "receipt_source_lead_count": 3,
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
    assert "SCMO WS-Security Preflight" in (
        export_dir / "dfl_ua_context_v13_acquisition_summary.md"
    ).read_text(encoding="utf-8")
    assert "Receipt Source Lead Audit" in (
        export_dir / "dfl_ua_context_v13_acquisition_summary.md"
    ).read_text(encoding="utf-8")
    assert "Safe-Switch Candidate Audits" in (
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
    assert backlog["market_execution_enabled"].to_list() == [
        False,
        False,
        False,
        False,
        False,
        False,
    ]
    assert backlog["permits_model_training"].to_list() == [
        False,
        False,
        False,
        False,
        False,
        False,
    ]
    assert backlog["backlog_item_type"].to_list().count("receipt_source_lead") == 3
    lead_backlog = backlog.filter(pl.col("backlog_item_type") == "receipt_source_lead")
    assert lead_backlog["blocking_source_family"].to_list() == [
        "explicit_dam_publication_receipts",
        "explicit_dam_publication_receipts",
        "explicit_dam_publication_receipts",
    ]
    assert lead_backlog["source_url"].to_list() == [
        "https://energy-map.info/en/datasets/example",
        "https://energy-map.info/en/datasets/example",
        "https://www.oree.com.ua/index.php/pricectr/data_view",
    ]
    assert lead_backlog["source_status"].to_list() == [
        "blocked_auth_required",
        "insufficient_dataset_level_metadata_only",
        "blocked_negative_probe",
    ]
    assert lead_backlog["recommended_next_step"].to_list() == [
        "obtain_authorized_export_then_validate_receipts",
        "locate_row_level_publication_timestamps_or_reject_lead",
        "do_not_convert_negative_probe_to_receipts",
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
        export_dir / "dfl_ua_context_v13_receipt_source_lead_audit.json"
    ).exists()
    exported_lead_audit = json.loads(
        (
            export_dir / "dfl_ua_context_v13_receipt_source_lead_audit.json"
        ).read_text(encoding="utf-8")
    )
    assert exported_lead_audit["candidate_receipt_source_found"] is False
    assert exported_lead_audit["validated_receipt_csv_ready"] is False
    assert exported_lead_audit["market_execution_enabled"] is False
    safe_switch_candidate_audit_path = (
        export_dir / "dfl_ua_context_v13_safe_switch_candidate_audits.json"
    )
    assert safe_switch_candidate_audit_path.exists()
    exported_safe_switch_candidate_audits = json.loads(
        safe_switch_candidate_audit_path.read_text(encoding="utf-8")
    )
    assert len(exported_safe_switch_candidate_audits) == 2
    assert exported_safe_switch_candidate_audits[0]["market_execution_enabled"] is False
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
    scmo_preflight_path = (
        export_dir / "dfl_ua_context_v13_scmo_ws_security_preflight.json"
    )
    assert scmo_preflight_path.exists()
    exported_scmo_preflight = json.loads(
        scmo_preflight_path.read_text(encoding="utf-8")
    )
    assert exported_scmo_preflight["credential_material_ready"] is False
    assert exported_scmo_preflight["validated_receipt_csv_ready"] is False
    assert exported_scmo_preflight["market_execution_enabled"] is False


def test_ua_context_v13_packet_attaches_policy_deadline_evidence_without_unlock(
    tmp_path: Path,
) -> None:
    source_inventory = pl.DataFrame(
        [
            {
                "source_family": "explicit_dam_publication_receipts",
                "source_status": "partial_context_rule_deadline_without_row_receipts",
                "coverage_ratio": 0.0,
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
                    "explicit_dam_publication_receipts:"
                    "partial_context_rule_deadline_without_row_receipts"
                ),
                "prior_material_safe_switch_example_count": 20,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "dt_lava_ready": False,
                "target_label_space": "v13_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "market_execution_enabled": False,
            }
        ]
    )
    policy_publication_evidence = {
        "claim_scope": "oree_policy_publication_deadline_evidence_not_v13_receipt",
        "policy_evidence_row_count": 2,
        "observed_market_count": 2,
        "markets_observed": ["DAM", "IDM"],
        "all_policy_deadlines_have_observed_public_artifact": True,
        "can_satisfy_v13_explicit_receipts": False,
        "source_publication_timestamp_available": False,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }

    packet = build_dfl_ua_context_v13_acquisition_packet(
        run_slug="test_v13_policy_deadline_evidence",
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        policy_publication_evidence=policy_publication_evidence,
        asset_check_status="blocked_v13_explicit_dam_publication_receipts",
    )
    export_dir = write_dfl_ua_context_v13_acquisition_packet(
        packet,
        output_root=tmp_path,
        source_inventory_frame=source_inventory,
        readiness_frame=readiness,
        policy_publication_evidence=policy_publication_evidence,
    )

    assert packet["v13_candidate_generation_ready"] is False
    assert packet["policy_publication_evidence_summary"] == {
        "all_policy_deadlines_have_observed_public_artifact": True,
        "can_satisfy_v13_explicit_receipts": False,
        "claim_scope": "oree_policy_publication_deadline_evidence_not_v13_receipt",
        "dt_lava_ready": False,
        "market_execution_enabled": False,
        "markets_observed": ["DAM", "IDM"],
        "observed_market_count": 2,
        "permits_model_training": False,
        "policy_evidence_row_count": 2,
        "receipt_csv_generated": False,
        "source_publication_timestamp_available": False,
        "validated_receipt_csv_ready": False,
    }
    assert packet["attached_artifacts"]["policy_publication_evidence_json"] == (
        "dfl_ua_context_v13_policy_publication_evidence.json"
    )
    policy_evidence_path = (
        export_dir / "dfl_ua_context_v13_policy_publication_evidence.json"
    )
    assert policy_evidence_path.exists()
    assert json.loads(policy_evidence_path.read_text(encoding="utf-8"))[
        "can_satisfy_v13_explicit_receipts"
    ] is False
    markdown = (
        export_dir / "dfl_ua_context_v13_acquisition_summary.md"
    ).read_text(encoding="utf-8")
    assert "Policy Publication Deadline Evidence" in markdown
    assert "does not satisfy explicit DAM publication receipts" in markdown


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
    receipt_source_lead_audit_path = tmp_path / "receipt_source_lead_audit.json"
    safe_switch_candidate_audit_path = tmp_path / "safe_switch_candidate_audit.json"
    acquisition_input_preflight_path = tmp_path / "acquisition_input_preflight.json"
    scmo_ws_security_preflight_path = tmp_path / "scmo_ws_security_preflight.json"
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
    receipt_source_lead_audit_path.write_text(
        json.dumps(
            {
                "claim_scope": "v13_dam_receipt_source_lead_audit_not_market_execution",
                "lead_count": 3,
                "candidate_receipt_source_found": False,
                "candidate_receipt_lead_count": 0,
                "candidate_receipt_lead_ids": [],
                "dataset_level_metadata_only_count": 1,
                "auth_blocked_count": 1,
                "probe_negative_count": 1,
                "receipt_csv_generated": False,
                "validated_receipt_csv_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
                "lead_rows": [
                    {
                        "lead_id": "energy_map_dam_indexes_dataset",
                        "source_url": "https://energy-map.info/en/datasets/example",
                        "source_title": "Energy Map DAM indexes",
                        "lead_status": "insufficient_dataset_level_metadata_only",
                        "blocking_reasons": ["dataset_level_metadata_only"],
                    },
                    {
                        "lead_id": "energy_map_dam_download_path",
                        "source_url": "https://energy-map.info/en/datasets/example",
                        "source_title": "Energy Map DAM download path",
                        "lead_status": "blocked_auth_required",
                        "blocking_reasons": ["download_auth_required"],
                    },
                    {
                        "lead_id": "oree_data_view_monthly_probe",
                        "source_url": "https://www.oree.com.ua/index.php/pricectr/data_view",
                        "source_title": "OREE data_view monthly probe",
                        "lead_status": "blocked_negative_probe",
                        "blocking_reasons": [
                            "source_probe_not_sufficient_for_v13_receipts"
                        ],
                    },
                ],
            }
        ),
        encoding="utf-8",
    )
    safe_switch_candidate_audit_path.write_text(
        json.dumps(
            {
                "claim_scope": "v13_safe_switch_candidate_source_audit_not_market_execution",
                "source_rows": 23499,
                "material_label_column": "label_v12_material_safe_switch",
                "tail_risk_label_column": "label_v12_tail_risk_loss",
                "source_evidence_timestamp_column": "generated_at",
                "uses_canonical_v13_labels": False,
                "accepted_candidate_rows": 0,
                "weak_safe_switch_win_rows": 348,
                "duplicate_accepted_rows": 0,
                "accepted_tenant_source_count": 0,
                "normalized_safe_switch_csv_ready": False,
                "blocking_reasons": [
                    "noncanonical_material_label_column:label_v12_material_safe_switch",
                    "noncanonical_tail_risk_label_column:label_v12_tail_risk_loss",
                    "noncanonical_source_evidence_timestamp_column:generated_at",
                ],
                "dt_lava_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
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
    scmo_ws_security_preflight_path.write_text(
        json.dumps(
            {
                "claim_scope": "scmo_ws_security_credential_preflight",
                "credential_material_ready": False,
                "credential_material_present": False,
                "credential_file_pair_valid": False,
                "credential_material_validation_status": "missing_env_or_file",
                "signed_download_request_ready": False,
                "ws_security_signature_supported": False,
                "ws_security_signature_status": "xml_signature_not_implemented",
                "missing_env_vars": [
                    "SCMO_USERNAME",
                    "SCMO_PASSWORD",
                    "SCMO_CLIENT_CERT_PEM",
                    "SCMO_CLIENT_KEY_PEM",
                ],
                "missing_files": [],
                "username_present": False,
                "password_present": False,
                "client_cert_path_present": False,
                "client_key_path_present": False,
                "client_key_password_present": False,
                "secret_values_written": False,
                "receipt_csv_generated": False,
                "validated_receipt_csv_ready": False,
                "dt_lava_ready": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
                "not_market_execution": True,
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
            "--receipt-source-lead-audit-json",
            str(receipt_source_lead_audit_path),
            "--safe-switch-candidate-audit-json",
            str(safe_switch_candidate_audit_path),
            "--acquisition-input-preflight-json",
            str(acquisition_input_preflight_path),
            "--scmo-ws-security-preflight-json",
            str(scmo_ws_security_preflight_path),
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
        "backlog_item_count": 5,
        "receipt_source_lead_count": 3,
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
    assert summary["receipt_source_lead_audit_summary"] == {
        "auth_blocked_count": 1,
        "candidate_receipt_lead_count": 0,
        "candidate_receipt_lead_ids": [],
        "candidate_receipt_source_found": False,
        "claim_scope": "v13_dam_receipt_source_lead_audit_not_market_execution",
        "dataset_level_metadata_only_count": 1,
        "lead_count": 3,
        "market_execution_enabled": False,
        "permits_model_training": False,
        "probe_negative_count": 1,
        "receipt_csv_generated": False,
        "validated_receipt_csv_ready": False,
    }
    assert summary["safe_switch_candidate_audit_summary"] == {
        "accepted_tenant_source_count": 0,
        "audit_count": 1,
        "audit_rows": [
            {
                "accepted_candidate_rows": 0,
                "blocking_reasons": [
                    "noncanonical_material_label_column:label_v12_material_safe_switch",
                    "noncanonical_tail_risk_label_column:label_v12_tail_risk_loss",
                    "noncanonical_source_evidence_timestamp_column:generated_at",
                ],
                "duplicate_accepted_rows": 0,
                "material_label_column": "label_v12_material_safe_switch",
                "normalized_safe_switch_csv_ready": False,
                "source_rows": 23499,
                "uses_canonical_v13_labels": False,
                "weak_safe_switch_win_rows": 348,
            }
        ],
        "dt_lava_ready": False,
        "market_execution_enabled": False,
        "normalized_safe_switch_csv_ready_count": 0,
        "permits_model_training": False,
        "total_accepted_candidate_rows": 0,
        "total_source_rows": 23499,
        "total_weak_safe_switch_win_rows": 348,
    }
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
    assert summary["scmo_ws_security_preflight_summary"] == {
        "claim_scope": "scmo_ws_security_credential_preflight",
        "client_cert_path_present": False,
        "client_key_path_present": False,
        "client_key_password_present": False,
        "client_p12_path_present": False,
        "client_p12_password_present": False,
        "credential_file_pair_valid": False,
        "credential_material_format": "missing",
        "credential_material_ready": False,
        "credential_material_present": False,
        "credential_material_validation_status": "missing_env_or_file",
        "dt_lava_ready": False,
        "market_execution_enabled": False,
        "missing_env_vars": [
            "SCMO_USERNAME",
            "SCMO_PASSWORD",
            "SCMO_CLIENT_CERT_PEM",
            "SCMO_CLIENT_KEY_PEM",
        ],
        "missing_files": [],
        "mtls_client_cert_ready": False,
        "password_present": False,
        "pem_cert_key_pair_present": False,
        "pkcs12_bundle_present": False,
        "permits_model_training": False,
        "receipt_csv_generated": False,
        "secret_values_written": False,
        "signed_download_request_ready": False,
        "username_present": False,
        "validated_receipt_csv_ready": False,
        "ws_security_signature_status": "xml_signature_not_implemented",
        "ws_security_signature_supported": False,
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
        / "dfl_ua_context_v13_receipt_source_lead_audit.json"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_safe_switch_candidate_audits.json"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_acquisition_input_preflight.json"
    ).exists()
    assert (
        tmp_path
        / "test_v13_csv_export"
        / "dfl_ua_context_v13_scmo_ws_security_preflight.json"
    ).exists()
