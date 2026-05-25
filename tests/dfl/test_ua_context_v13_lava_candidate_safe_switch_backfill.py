from __future__ import annotations

from datetime import datetime

import polars as pl
import pytest

from smart_arbitrage.dfl.ua_context_v13_lava_candidate_safe_switch_backfill import (
    build_v13_safe_switch_examples_from_lava_candidates_frame,
    summarize_v13_safe_switch_lava_candidate_backfill,
)


def test_lava_candidate_backfill_emits_source_backed_v13_rows() -> None:
    examples = build_v13_safe_switch_examples_from_lava_candidates_frame(
        _candidate_frame(),
        _oree_observations_frame(),
        _targets_frame(),
        _existing_examples_frame(),
    )

    assert examples.height == 2
    assert examples["tenant_id"].to_list() == [
        "client_004_kharkiv_hospital",
        "client_001_kyiv_mall",
    ]
    assert examples["label_v13_material_safe_switch"].to_list() == [True, True]
    assert examples["label_v13_tail_risk_loss"].to_list() == [False, False]
    assert examples["market_execution_enabled"].to_list() == [False, False]
    assert examples["source_evidence_timestamp"].to_list() == [
        datetime(2026, 5, 24, 20, 30, 29),
        datetime(2026, 5, 24, 20, 31, 29),
    ]
    assert examples["source_url"].to_list() == [
        "https://www.oree.com.ua/index.php/PXS/downloadxlsx/01.01.2026/DAM/2",
        "https://www.oree.com.ua/index.php/PXS/downloadxlsx/02.01.2026/DAM/2",
    ]
    assert examples["receipt_id"].to_list() == [
        "oree-lava-safe-switch:2026-01-01T23:00:00.000000:hash-client-004",
        "oree-lava-safe-switch:2026-01-02T23:00:00.000000:hash-client-001",
    ]


def test_lava_candidate_backfill_summary_reports_deficits_and_missing_observations() -> None:
    examples = build_v13_safe_switch_examples_from_lava_candidates_frame(
        _candidate_frame(),
        _oree_observations_frame().filter(pl.col("delivery_date") != "2026-01-02"),
        _targets_frame(),
        _existing_examples_frame(),
    )
    summary = summarize_v13_safe_switch_lava_candidate_backfill(
        examples,
        _candidate_frame(),
        _oree_observations_frame().filter(pl.col("delivery_date") != "2026-01-02"),
        _targets_frame(),
        _existing_examples_frame(),
    )

    assert examples.height == 1
    assert summary["claim_scope"] == (
        "v13_safe_switch_lava_candidate_backfill_not_model_training"
    )
    assert summary["selected_backfill_rows"] == 1
    assert summary["source_observed_candidate_rows"] == 1
    assert summary["missing_observation_candidate_rows"] == 1
    assert summary["remaining_missing_after_backfill"] == 1
    assert summary["missing_observation_delivery_dates"] == ["2026-01-02"]
    assert summary["next_observation_delivery_dates"] == ["2026-01-02"]
    assert summary["dt_lava_ready"] is False
    assert summary["permits_model_training"] is False
    assert summary["market_execution_enabled"] is False


def test_lava_candidate_backfill_rejects_market_execution_claims() -> None:
    with pytest.raises(ValueError, match="candidate frame contains market execution"):
        build_v13_safe_switch_examples_from_lava_candidates_frame(
            _candidate_frame(market_execution_enabled=True),
            _oree_observations_frame(),
            _targets_frame(),
            _existing_examples_frame(),
        )


def test_lava_candidate_backfill_cli_writes_rows_and_summary(tmp_path) -> None:
    from scripts.backfill_v13_safe_switch_from_lava_candidates import main

    import pickle

    candidate_pickle = tmp_path / "candidate_frame.pkl"
    observations_csv = tmp_path / "oree_observations.csv"
    targets_csv = tmp_path / "targets.csv"
    existing_csv = tmp_path / "existing.csv"
    output_csv = tmp_path / "safe_switch_backfill.csv"
    summary_json = tmp_path / "safe_switch_backfill_summary.json"

    with candidate_pickle.open("wb") as file:
        pickle.dump(_candidate_frame(), file)
    _oree_observations_frame().write_csv(observations_csv)
    _targets_frame().write_csv(targets_csv)
    _existing_examples_frame().write_csv(existing_csv)

    exit_code = main(
        [
            "--candidate-frame-pickle",
            str(candidate_pickle),
            "--oree-observations-csv",
            str(observations_csv),
            "--acquisition-targets-csv",
            str(targets_csv),
            "--existing-safe-switch-csv",
            str(existing_csv),
            "--output-csv",
            str(output_csv),
            "--summary-json",
            str(summary_json),
        ]
    )

    assert exit_code == 0
    output = pl.read_csv(output_csv)
    summary = summary_json.read_text(encoding="utf-8")
    assert output.height == 2
    assert "v13_safe_switch_lava_candidate_backfill_not_model_training" in summary
    assert "market_execution_enabled" in output.columns


def _candidate_frame(*, market_execution_enabled: bool = False) -> pl.DataFrame:
    return pl.DataFrame(
        [
            _candidate_row(
                tenant_id="client_004_kharkiv_hospital",
                anchor_timestamp="2026-01-01T23:00:00",
                regret_delta=-120.0,
                market_execution_enabled=market_execution_enabled,
            ),
            _candidate_row(
                tenant_id="client_004_kharkiv_hospital",
                anchor_timestamp="2026-01-01T23:00:00",
                regret_delta=-80.0,
            ),
            _candidate_row(
                tenant_id="client_001_kyiv_mall",
                anchor_timestamp="2026-01-02T23:00:00",
                regret_delta=-55.0,
            ),
            _candidate_row(
                tenant_id="client_001_kyiv_mall",
                anchor_timestamp="2026-01-03T23:00:00",
                regret_delta=-10.0,
            ),
            _candidate_row(
                tenant_id="client_001_kyiv_mall",
                anchor_timestamp="2026-01-04T23:00:00",
                regret_delta=-70.0,
                split_name="final_holdout",
            ),
            _candidate_row(
                tenant_id="client_001_kyiv_mall",
                anchor_timestamp="2026-01-05T23:00:00",
                regret_delta=-70.0,
                safety_violation_count=1,
            ),
            _candidate_row(
                tenant_id="client_002_lviv_office",
                anchor_timestamp="2026-01-06T23:00:00",
                regret_delta=-90.0,
            ),
        ]
    )


def _candidate_row(
    *,
    tenant_id: str,
    anchor_timestamp: str,
    regret_delta: float,
    split_name: str = "train_selection",
    safety_violation_count: int = 0,
    market_execution_enabled: bool = False,
) -> dict[str, object]:
    return {
        "tenant_id": tenant_id,
        "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        "candidate_family": "strict_control",
        "candidate_model_name": "strict_similar_day",
        "anchor_timestamp": anchor_timestamp,
        "split_name": split_name,
        "eligible_for_final_selection": True,
        "label_regret_delta_vs_v2_plus_uah": regret_delta,
        "safety_violation_count": safety_violation_count,
        "market_execution_enabled": market_execution_enabled,
    }


def _oree_observations_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            _observation_row("2026-01-01", "hash-client-004"),
            _observation_row("2026-01-02", "hash-client-001", second=31),
        ]
    )


def _observation_row(
    delivery_date: str,
    download_sha256: str,
    *,
    second: int = 30,
) -> dict[str, object]:
    day_label = datetime.fromisoformat(delivery_date).strftime("%d.%m.%Y")
    return {
        "delivery_date": delivery_date,
        "source_observed_at_utc": f"2026-05-24T20:{second}:29+00:00",
        "download_url": (
            "https://www.oree.com.ua/index.php/PXS/downloadxlsx/"
            f"{day_label}/DAM/2"
        ),
        "download_sha256": download_sha256,
        "source_title": "OREE PXS DAM downloadxlsx endpoint",
        "market_execution_enabled": False,
    }


def _targets_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "acquisition_priority_rank": 1,
                "tenant_id": "client_004_kharkiv_hospital",
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "current_prior_material_safe_switch_examples": 18,
                "required_prior_material_safe_switch_examples": 20,
            },
            {
                "acquisition_priority_rank": 2,
                "tenant_id": "client_001_kyiv_mall",
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "current_prior_material_safe_switch_examples": 19,
                "required_prior_material_safe_switch_examples": 20,
            },
            {
                "acquisition_priority_rank": 3,
                "tenant_id": "client_002_lviv_office",
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "current_prior_material_safe_switch_examples": 20,
                "required_prior_material_safe_switch_examples": 20,
            },
        ]
    )


def _existing_examples_frame() -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": "client_004_kharkiv_hospital",
                "source_model_name": (
                    "nbeatsx_official_global_panel_horizon_calibrated_v1"
                ),
                "anchor_timestamp": "2025-12-31T23:00:00",
                "split_name": "train_selection",
                "source_evidence_timestamp": "2026-05-24T20:29:29+00:00",
                "label_v13_material_safe_switch": True,
                "label_v13_tail_risk_loss": False,
                "market_execution_enabled": False,
            }
        ]
    )
