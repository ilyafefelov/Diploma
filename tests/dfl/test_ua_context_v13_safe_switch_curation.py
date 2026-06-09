from __future__ import annotations

import json

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    normalize_dfl_ua_context_safe_switch_examples_v13_frame,
)
from smart_arbitrage.dfl.ua_context_v13_safe_switch_curation import (
    build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame,
    extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame,
    summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame,
)


def test_v13_safe_switch_curation_worksheet_stays_blocked_until_reviewed() -> None:
    worksheet = build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        pl.DataFrame([_backlog_row()])
    )

    assert worksheet["curator_review_status"].to_list() == [
        "pending_source_review"
    ]
    assert worksheet["candidate_source_evidence_timestamp"].to_list() == [
        "2026-05-24T12:00:00"
    ]
    assert worksheet["source_evidence_timestamp"].to_list() == [None]
    assert worksheet["ready_for_v13_safe_switch_validator"].to_list() == [False]
    assert worksheet["candidate_can_satisfy_v13_without_validation"].to_list() == [
        False
    ]
    assert worksheet["permits_model_training"].to_list() == [False]
    assert worksheet["market_execution_enabled"].to_list() == [False]

    summary = summarize_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        worksheet
    )
    assert summary["worksheet_rows"] == 1
    assert summary["ready_for_v13_safe_switch_validator_rows"] == 0
    assert summary["curated_safe_switch_examples_rows"] == 0
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_v13_safe_switch_curation_extracts_only_source_backed_approved_rows() -> None:
    worksheet = build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        pl.DataFrame([_backlog_row()])
    ).with_columns(
        pl.lit("approved_source_backed_v13_safe_switch").alias(
            "curator_review_status"
        ),
        pl.lit("reviewer-1").alias("curator_id"),
        pl.lit("2026-05-24T15:00:00").alias("curator_reviewed_at"),
        pl.lit("2026-05-24T12:30:00").alias("source_evidence_timestamp"),
        pl.lit("https://evidence.example/safe-switch/client-004/2026-01-02").alias(
            "source_url"
        ),
        pl.lit("Source-backed safe-switch review note").alias("source_title"),
        pl.lit("safe-switch-review-001").alias("source_evidence_id"),
        pl.lit(True).alias("label_v13_material_safe_switch"),
        pl.lit(False).alias("label_v13_tail_risk_loss"),
    )

    examples = extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame(
        worksheet
    )

    assert examples.height == 1
    assert examples["tenant_id"].to_list() == ["client_004_kharkiv_hospital"]
    assert examples["split_name"].to_list() == ["train_selection"]
    assert examples["source_url"].to_list() == [
        "https://evidence.example/safe-switch/client-004/2026-01-02"
    ]
    assert examples["receipt_id"].to_list() == ["safe-switch-review-001"]
    assert examples["market_execution_enabled"].to_list() == [False]
    validated = normalize_dfl_ua_context_safe_switch_examples_v13_frame(examples)
    assert validated.height == 1


def test_v13_safe_switch_curation_rejects_approved_rows_without_source_evidence() -> None:
    worksheet = build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame(
        pl.DataFrame([_backlog_row()])
    ).with_columns(
        pl.lit("approved_source_backed_v13_safe_switch").alias(
            "curator_review_status"
        ),
        pl.lit(True).alias("label_v13_material_safe_switch"),
        pl.lit(False).alias("label_v13_tail_risk_loss"),
    )

    try:
        extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame(
            worksheet
        )
    except ValueError as error:
        assert "source evidence" in str(error)
    else:
        raise AssertionError("expected missing source evidence to be rejected")


def test_v13_safe_switch_curation_cli_writes_worksheet_and_extracts_examples(
    tmp_path,
) -> None:
    from scripts.export_ua_context_v13_safe_switch_curation_worksheet import (
        main as worksheet_main,
    )
    from scripts.extract_ua_context_v13_safe_switch_examples_from_curation import (
        main as extract_main,
    )

    backlog_path = tmp_path / "review_backlog.csv"
    worksheet_path = tmp_path / "curation_worksheet.csv"
    worksheet_summary_path = tmp_path / "curation_worksheet_summary.json"
    examples_path = tmp_path / "safe_switch_examples_v13.csv"
    examples_summary_path = tmp_path / "safe_switch_examples_v13_summary.json"
    pl.DataFrame([_backlog_row()]).write_csv(backlog_path)

    worksheet_exit_code = worksheet_main(
        [
            "--review-backlog-csv",
            str(backlog_path),
            "--output-csv",
            str(worksheet_path),
            "--summary-json",
            str(worksheet_summary_path),
        ]
    )

    assert worksheet_exit_code == 0
    worksheet_summary = json.loads(
        worksheet_summary_path.read_text(encoding="utf-8")
    )
    assert worksheet_summary["ready_for_v13_safe_switch_validator_rows"] == 0
    worksheet = pl.read_csv(worksheet_path)
    reviewed = worksheet.with_columns(
        pl.lit("approved_source_backed_v13_safe_switch").alias(
            "curator_review_status"
        ),
        pl.lit("reviewer-1").alias("curator_id"),
        pl.lit("2026-05-24T15:00:00").alias("curator_reviewed_at"),
        pl.lit("2026-05-24T12:30:00").alias("source_evidence_timestamp"),
        pl.lit("https://evidence.example/safe-switch/client-004/2026-01-02").alias(
            "source_url"
        ),
        pl.lit("Source-backed safe-switch review note").alias("source_title"),
        pl.lit("safe-switch-review-001").alias("source_evidence_id"),
        pl.lit(True).alias("label_v13_material_safe_switch"),
        pl.lit(False).alias("label_v13_tail_risk_loss"),
    )
    reviewed.write_csv(worksheet_path)

    extract_exit_code = extract_main(
        [
            "--curation-worksheet-csv",
            str(worksheet_path),
            "--output-csv",
            str(examples_path),
            "--summary-json",
            str(examples_summary_path),
        ]
    )

    assert extract_exit_code == 0
    examples_summary = json.loads(examples_summary_path.read_text(encoding="utf-8"))
    assert examples_summary["curated_safe_switch_examples_rows"] == 1
    assert examples_summary["permits_model_training"] is False
    assert examples_summary["market_execution_enabled"] is False
    examples = pl.read_csv(examples_path)
    assert examples["tenant_id"].to_list() == ["client_004_kharkiv_hospital"]


def _backlog_row() -> dict[str, object]:
    return {
        "acquisition_priority_rank": 1,
        "tenant_id": "client_004_kharkiv_hospital",
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "current_prior_material_safe_switch_examples": 2,
        "required_prior_material_safe_switch_examples": 20,
        "target_new_prior_material_safe_switch_examples": 18,
        "review_rank_for_target": 1,
        "anchor_timestamp": "2026-01-02T23:00:00",
        "split_name": "train_selection",
        "source_evidence_timestamp": "2026-05-24T12:00:00",
        "candidate_family": "strict_raw_blend_v2",
        "candidate_model_name": "candidate_v1",
        "material_label_column": "label_v11_material_safe_switch",
        "tail_risk_label_column": "label_v11_tail_risk_loss",
        "source_evidence_timestamp_column": "generated_at",
        "uses_canonical_v13_labels": False,
        "material_candidate": True,
        "weak_safe_switch_win": True,
        "tail_risk_candidate": False,
        "label_regret_delta_vs_v2_plus_uah": -12.5,
        "review_status": "requires_canonical_v13_relabel",
        "required_review_action": "curate_source_backed_canonical_v13_safe_switch_row",
        "candidate_can_satisfy_v13_without_validation": False,
        "can_feed_safe_switch_validator_after_review": False,
        "primary_blocking_source_family": "explicit_dam_publication_receipts",
        "target_label_space": "v13_precondition_context_coverage",
        "claim_scope": "v13_safe_switch_review_backlog_not_training_data",
        "dt_lava_ready": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
    }
