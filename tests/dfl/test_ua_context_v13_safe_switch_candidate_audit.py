from __future__ import annotations

import json

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_safe_switch_audit import (
    audit_dfl_ua_context_safe_switch_candidate_source_v13_frame,
)
from smart_arbitrage.dfl.ua_context_v13_safe_switch_review_backlog import (
    build_dfl_ua_context_safe_switch_review_backlog_v13_frame,
    summarize_dfl_ua_context_safe_switch_review_backlog_v13_frame,
)


def test_v13_safe_switch_candidate_audit_accepts_only_material_non_tail_risk_rows() -> None:
    frame = pl.DataFrame(
        [
            _row(
                anchor_timestamp="2026-01-01T23:00:00",
                material=True,
                tail_risk=False,
            ),
            _row(
                anchor_timestamp="2026-01-02T23:00:00",
                material=False,
                tail_risk=False,
                weak_safe_win=True,
            ),
            _row(
                anchor_timestamp="2026-01-03T23:00:00",
                material=True,
                tail_risk=True,
            ),
            _row(
                anchor_timestamp="2026-01-04T23:00:00",
                split_name="final_holdout",
                material=True,
                tail_risk=False,
            ),
        ]
    )

    audit = audit_dfl_ua_context_safe_switch_candidate_source_v13_frame(frame)

    assert audit["claim_scope"] == "v13_safe_switch_candidate_source_audit_not_market_execution"
    assert audit["accepted_candidate_rows"] == 1
    assert audit["rejected_candidate_rows"] == 3
    assert audit["weak_safe_switch_win_rows"] == 1
    assert audit["tail_risk_rejected_rows"] == 1
    assert audit["non_train_selection_rejected_rows"] == 1
    assert audit["normalized_safe_switch_csv_ready"] is True
    assert audit["permits_model_training"] is False
    assert audit["market_execution_enabled"] is False
    assert audit["tenant_source_counts"] == [
        {
            "accepted_candidate_rows": 1,
            "missing_to_floor": 19,
            "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
            "tenant_id": "client_001_kyiv_mall",
        }
    ]


def test_v13_safe_switch_candidate_audit_rejects_market_execution_claim() -> None:
    frame = pl.DataFrame(
        [
            _row(
                anchor_timestamp="2026-01-01T23:00:00",
                material=True,
                tail_risk=False,
                market_execution_enabled=True,
            )
        ]
    )

    audit = audit_dfl_ua_context_safe_switch_candidate_source_v13_frame(frame)

    assert audit["accepted_candidate_rows"] == 0
    assert audit["normalized_safe_switch_csv_ready"] is False
    assert audit["market_execution_enabled"] is False
    assert audit["blocking_reasons"] == ["source_contains_market_execution_rows"]


def test_v13_safe_switch_candidate_audit_reports_missing_label_columns() -> None:
    frame = pl.DataFrame(
        [
            {
                "tenant_id": "client_001_kyiv_mall",
                "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "anchor_timestamp": "2026-01-01T23:00:00",
                "split_name": "train_selection",
                "source_evidence_timestamp": "2026-05-24T12:00:00",
                "market_execution_enabled": False,
                "label_safe_switch_win": True,
            }
        ]
    )

    audit = audit_dfl_ua_context_safe_switch_candidate_source_v13_frame(frame)

    assert audit["accepted_candidate_rows"] == 0
    assert audit["weak_safe_switch_win_rows"] == 1
    assert audit["normalized_safe_switch_csv_ready"] is False
    assert audit["blocking_reasons"] == [
        "missing_material_label_column:label_v13_material_safe_switch",
        "missing_tail_risk_label_column:label_v13_tail_risk_loss",
    ]


def test_v13_safe_switch_candidate_audit_counts_noncanonical_labels_but_not_ready() -> None:
    frame = pl.DataFrame(
        [
            {
                "tenant_id": "client_001_kyiv_mall",
                "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
                "anchor_timestamp": "2026-01-01T23:00:00",
                "split_name": "train_selection",
                "generated_at": "2026-05-24T12:00:00",
                "label_v11_material_safe_switch": True,
                "label_v11_tail_risk_loss": False,
                "market_execution_enabled": False,
            }
        ]
    )

    audit = audit_dfl_ua_context_safe_switch_candidate_source_v13_frame(
        frame,
        material_label_column="label_v11_material_safe_switch",
        tail_risk_label_column="label_v11_tail_risk_loss",
        source_evidence_timestamp_column="generated_at",
    )

    assert audit["accepted_candidate_rows"] == 1
    assert audit["uses_canonical_v13_labels"] is False
    assert audit["normalized_safe_switch_csv_ready"] is False
    assert audit["blocking_reasons"] == [
        "noncanonical_material_label_column:label_v11_material_safe_switch",
        "noncanonical_tail_risk_label_column:label_v11_tail_risk_loss",
        "noncanonical_source_evidence_timestamp_column:generated_at",
    ]


def test_v13_safe_switch_candidate_audit_cli_writes_summary(tmp_path) -> None:
    from scripts.audit_ua_context_safe_switch_candidates_v13 import main

    input_path = tmp_path / "candidate_rows.csv"
    output_path = tmp_path / "audit.json"
    pl.DataFrame(
        [
            _row(
                anchor_timestamp="2026-01-01T23:00:00",
                material=True,
                tail_risk=False,
            )
        ]
    ).write_csv(input_path)

    exit_code = main(["--input", str(input_path), "--output", str(output_path)])

    assert exit_code == 0
    audit = json.loads(output_path.read_text(encoding="utf-8"))
    assert audit["accepted_candidate_rows"] == 1
    assert audit["normalized_safe_switch_csv_ready"] is True
    assert audit["market_execution_enabled"] is False


def test_v13_safe_switch_review_backlog_prioritizes_client_004_without_promotion() -> None:
    targets = pl.DataFrame(
        [
            _target(
                acquisition_priority_rank=1,
                tenant_id="client_004_kharkiv_hospital",
                current_prior_material_safe_switch_examples=2,
                target_new_prior_material_safe_switch_examples=18,
            ),
            _target(
                acquisition_priority_rank=2,
                tenant_id="client_001_kyiv_mall",
                current_prior_material_safe_switch_examples=4,
                target_new_prior_material_safe_switch_examples=16,
            ),
        ]
    )
    candidates = pl.DataFrame(
        [
            _row(
                tenant_id="client_001_kyiv_mall",
                anchor_timestamp="2026-01-01T23:00:00",
                material=True,
                tail_risk=False,
                weak_safe_win=True,
            ),
            _row(
                tenant_id="client_004_kharkiv_hospital",
                anchor_timestamp="2026-01-02T23:00:00",
                material=True,
                tail_risk=False,
                weak_safe_win=True,
            ),
            _row(
                tenant_id="client_004_kharkiv_hospital",
                anchor_timestamp="2026-01-03T23:00:00",
                material=False,
                tail_risk=False,
                weak_safe_win=True,
            ),
        ]
    )

    backlog = build_dfl_ua_context_safe_switch_review_backlog_v13_frame(
        candidates,
        targets,
        material_label_column="label_v11_material_safe_switch",
        tail_risk_label_column="label_v11_tail_risk_loss",
        source_evidence_timestamp_column="source_evidence_timestamp",
        max_review_rows_per_target=2,
    )

    assert backlog["tenant_id"].to_list()[:2] == [
        "client_004_kharkiv_hospital",
        "client_004_kharkiv_hospital",
    ]
    assert backlog["candidate_can_satisfy_v13_without_validation"].to_list() == [
        False,
        False,
        False,
    ]
    assert backlog["review_status"].to_list() == [
        "requires_canonical_v13_relabel",
        "requires_material_safe_switch_review",
        "requires_canonical_v13_relabel",
    ]
    assert backlog["permits_model_training"].to_list() == [False, False, False]
    assert backlog["market_execution_enabled"].to_list() == [False, False, False]

    summary = summarize_dfl_ua_context_safe_switch_review_backlog_v13_frame(backlog)
    assert summary["review_rows"] == 3
    assert summary["phase0_priority_tenant_id"] == "client_004_kharkiv_hospital"
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_v13_safe_switch_review_backlog_rejects_market_execution_rows() -> None:
    targets = pl.DataFrame(
        [
            _target(
                acquisition_priority_rank=1,
                tenant_id="client_004_kharkiv_hospital",
                current_prior_material_safe_switch_examples=2,
                target_new_prior_material_safe_switch_examples=18,
            )
        ]
    )
    candidates = pl.DataFrame(
        [
            _row(
                tenant_id="client_004_kharkiv_hospital",
                anchor_timestamp="2026-01-02T23:00:00",
                material=True,
                tail_risk=False,
                market_execution_enabled=True,
            )
        ]
    )

    try:
        build_dfl_ua_context_safe_switch_review_backlog_v13_frame(
            candidates,
            targets,
        )
    except ValueError as error:
        assert "market execution" in str(error)
    else:
        raise AssertionError("expected market execution rows to be rejected")


def test_v13_safe_switch_review_backlog_cli_writes_non_promotional_outputs(tmp_path) -> None:
    from scripts.export_ua_context_v13_safe_switch_review_backlog import main

    targets_path = tmp_path / "targets.csv"
    candidates_path = tmp_path / "candidates.csv"
    output_path = tmp_path / "review_backlog.csv"
    summary_path = tmp_path / "review_backlog_summary.json"
    pl.DataFrame(
        [
            _target(
                acquisition_priority_rank=1,
                tenant_id="client_004_kharkiv_hospital",
                current_prior_material_safe_switch_examples=2,
                target_new_prior_material_safe_switch_examples=18,
            )
        ]
    ).write_csv(targets_path)
    pl.DataFrame(
        [
            _row(
                tenant_id="client_004_kharkiv_hospital",
                anchor_timestamp="2026-01-02T23:00:00",
                material=True,
                tail_risk=False,
                weak_safe_win=True,
            )
        ]
    ).write_csv(candidates_path)

    exit_code = main(
        [
            "--candidate-rows-csv",
            str(candidates_path),
            "--acquisition-targets-csv",
            str(targets_path),
            "--output-csv",
            str(output_path),
            "--summary-json",
            str(summary_path),
        ]
    )

    assert exit_code == 0
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    assert summary["review_rows"] == 1
    assert summary["phase0_priority_tenant_id"] == "client_004_kharkiv_hospital"
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def _row(
    *,
    tenant_id: str = "client_001_kyiv_mall",
    anchor_timestamp: str,
    material: bool,
    tail_risk: bool,
    split_name: str = "train_selection",
    weak_safe_win: bool = False,
    market_execution_enabled: bool = False,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "anchor_timestamp": anchor_timestamp,
        "split_name": split_name,
        "source_evidence_timestamp": "2026-05-24T12:00:00",
        "label_v13_material_safe_switch": material,
        "label_v13_tail_risk_loss": tail_risk,
        "label_v11_material_safe_switch": material,
        "label_v11_tail_risk_loss": tail_risk,
        "label_safe_switch_win": weak_safe_win,
        "candidate_family": "strict_raw_blend_v2",
        "candidate_model_name": "candidate_v1",
        "label_regret_delta_vs_v2_plus_uah": -10.0 if material else 0.0,
        "market_execution_enabled": market_execution_enabled,
        "raw_hourly_action_imitation": False,
    }


def _target(
    *,
    acquisition_priority_rank: int,
    tenant_id: str,
    current_prior_material_safe_switch_examples: int,
    target_new_prior_material_safe_switch_examples: int,
) -> dict[str, object]:
    return {
        "acquisition_priority_rank": acquisition_priority_rank,
        "tenant_id": tenant_id,
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "current_prior_material_safe_switch_examples": (
            current_prior_material_safe_switch_examples
        ),
        "required_prior_material_safe_switch_examples": 20,
        "target_new_prior_material_safe_switch_examples": (
            target_new_prior_material_safe_switch_examples
        ),
        "target_total_prior_material_safe_switch_examples": 20,
        "blocking_context_families": (
            "explicit_dam_publication_receipts:partial_context_rule_deadline_without_row_receipts"
        ),
        "primary_blocking_source_family": "explicit_dam_publication_receipts",
        "market_execution_enabled": False,
    }
