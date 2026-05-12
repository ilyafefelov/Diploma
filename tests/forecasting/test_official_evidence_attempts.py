import pytest

from smart_arbitrage.forecasting.official_evidence_attempts import (
    OfficialEvidenceAttemptConfig,
    build_official_evidence_attempt_manifest,
    official_evidence_attempt_slug,
)


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
