import json
import subprocess
import sys
from pathlib import Path

import pytest

from smart_arbitrage.forecasting.official_evidence_attempts import (
    OfficialEvidenceAttemptConfig,
    build_official_evidence_attempt_manifest,
    official_evidence_attempt_slug,
    summarize_official_evidence_attempt_resume,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def test_attempt_manifest_lists_resumable_batches_and_claim_boundary() -> None:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_global_panel_backfill",
            generated_at_iso="2026-05-11T20:30:00+00:00",
            total_anchors=10,
            batch_size=4,
            start_anchor_index=2,
            end_anchor_index=9,
            anchor_batch_order="chronological",
            enabled_official_models_csv="nbeatsx_official_global_panel_v1",
            nbeatsx_max_steps=25,
            tft_max_epochs=0,
            downstream_gate_enabled=True,
            asset_selection="asset_a,asset_b",
            downstream_selection="gate_a",
            run_root=".tmp_runtime/official_global_panel_batches",
        )
    )

    assert manifest["attempt_kind"] == "official_global_panel_backfill"
    assert manifest["run_slug"] == "official-global-panel-backfill-2026-05-11T203000-0000"
    assert manifest["resume_generated_at_iso"] == "2026-05-11T20:30:00+00:00"
    assert manifest["claim_boundary"] == {
        "offline_strategy_promotion_language": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "claim_scope": "offline_strategy_promotion_evidence_attempt",
    }
    assert manifest["batch_plan"] == [
        {
            "batch_id": "anchor-2",
            "anchor_batch_start_index": 2,
            "anchor_batch_size": 4,
            "anchor_batch_end_index_exclusive": 6,
        },
        {
            "batch_id": "anchor-6",
            "anchor_batch_start_index": 6,
            "anchor_batch_size": 3,
            "anchor_batch_end_index_exclusive": 9,
        },
    ]
    assert manifest["resume_policy"]["next_anchor_index_rule"] == (
        "resume from the first missing or failed batch start index; "
        "keep resume_generated_at_iso unchanged"
    )
    assert manifest["local_run_dir"] == (
        ".tmp_runtime/official_global_panel_batches/"
        "official-global-panel-backfill-2026-05-11T203000-0000"
    )


def test_attempt_manifest_rejects_invalid_batch_window() -> None:
    with pytest.raises(ValueError, match="end_anchor_index must be greater"):
        build_official_evidence_attempt_manifest(
            OfficialEvidenceAttemptConfig(
                attempt_kind="official_schedule_value",
                generated_at_iso="2026-05-11T09:53:24+00:00",
                total_anchors=18,
                batch_size=4,
                start_anchor_index=8,
                end_anchor_index=8,
                asset_selection="asset",
            )
        )


def test_attempt_slug_is_stable_for_timezone_timestamp() -> None:
    assert (
        official_evidence_attempt_slug(
            "official_schedule_value",
            "2026-05-11T09:53:24+00:00",
        )
        == "official-schedule-value-2026-05-11T095324-0000"
    )


def test_attempt_resume_summary_uses_manifest_batch_plan_and_persisted_counts() -> None:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_global_panel_backfill",
            generated_at_iso="2026-05-11T20:30:00+00:00",
            total_anchors=12,
            batch_size=4,
            start_anchor_index=0,
            enabled_official_models_csv=(
                "nbeatsx_official_global_panel_v1,"
                "nbeatsx_official_global_panel_horizon_calibrated_v1"
            ),
            asset_selection="asset_a,asset_b",
        )
    )

    summary = summarize_official_evidence_attempt_resume(
        manifest,
        persisted_anchor_counts_by_source={
            "nbeatsx_official_global_panel_v1": 8,
            "nbeatsx_official_global_panel_horizon_calibrated_v1": 6,
        },
    )

    assert summary["status"] == "resume_required"
    assert summary["effective_persisted_anchor_count"] == 6
    assert summary["next_anchor_index"] == 4
    assert summary["completed_batch_start_indices"] == [0]
    assert summary["resume_generated_at_iso"] == "2026-05-11T20:30:00+00:00"
    assert summary["claim_boundary"]["market_execution_enabled"] is False


def test_attempt_resume_summary_reports_complete_attempt() -> None:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_schedule_value",
            generated_at_iso="2026-05-11T09:53:24+00:00",
            total_anchors=9,
            batch_size=4,
            asset_selection="asset",
        )
    )

    summary = summarize_official_evidence_attempt_resume(
        manifest,
        persisted_anchor_count=9,
    )

    assert summary["status"] == "complete"
    assert summary["next_anchor_index"] is None
    assert summary["completed_batch_start_indices"] == [0, 4, 8]


def test_attempt_resume_summary_handles_partial_resume_window() -> None:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_global_panel_backfill",
            generated_at_iso="2026-05-11T20:30:00+00:00",
            total_anchors=12,
            batch_size=4,
            start_anchor_index=4,
            asset_selection="asset",
        )
    )

    summary = summarize_official_evidence_attempt_resume(
        manifest,
        persisted_anchor_count=12,
    )

    assert summary["status"] == "complete"
    assert summary["effective_persisted_anchor_count"] == 12
    assert summary["planned_anchor_count"] == 12
    assert summary["next_anchor_index"] is None
    assert summary["completed_batch_start_indices"] == [4, 8]


def test_attempt_resume_summary_rejects_counts_beyond_manifest_window() -> None:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_schedule_value",
            generated_at_iso="2026-05-11T09:53:24+00:00",
            total_anchors=9,
            batch_size=4,
            asset_selection="asset",
        )
    )

    with pytest.raises(ValueError, match="cannot exceed planned anchor count"):
        summarize_official_evidence_attempt_resume(
            manifest,
            persisted_anchor_count=10,
        )


def test_attempt_resume_cli_reads_manifest_and_source_counts(tmp_path: Path) -> None:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_schedule_value",
            generated_at_iso="2026-05-11T09:53:24+00:00",
            total_anchors=12,
            batch_size=4,
            enabled_official_models_csv="nbeatsx_official_v0,tft_official_v0",
            asset_selection="asset",
        )
    )
    manifest_path = tmp_path / "attempt_manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "scripts/summarize_official_evidence_attempt_resume.py",
            "--manifest",
            str(manifest_path),
            "--persisted-anchor-counts-csv",
            "nbeatsx_official_v0=8,tft_official_v0=4",
        ],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    summary = json.loads(result.stdout)
    assert summary["status"] == "resume_required"
    assert summary["effective_persisted_anchor_count"] == 4
    assert summary["next_anchor_index"] == 4
