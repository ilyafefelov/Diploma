from __future__ import annotations

import json

from scripts.materialize_hf_safe_switch_scorer_robustness_packet import (
    main as materialize_hf_safe_switch_scorer_robustness_packet,
)
from smart_arbitrage.dfl.hf_safe_switch_scorer_robustness import (
    ROBUSTNESS_CLAIM_SCOPE,
    summarize_hf_safe_switch_scorer_robustness,
    write_hf_safe_switch_scorer_robustness_packet,
)
from tests.dfl.test_hf_safe_switch_scorer import _csv_ready, _teacher_rows


def test_hf_safe_switch_robustness_summary_passes_with_stable_seed_evidence(
    tmp_path,
) -> None:
    packet = summarize_hf_safe_switch_scorer_robustness(
        seed_packets=[
            _seed_packet(seed=7, threshold=50.0, mean_regret=158.0, loss_count=1),
            _seed_packet(seed=42, threshold=50.0, mean_regret=159.0, loss_count=2),
            _seed_packet(seed=2026, threshold=50.0, mean_regret=166.0, loss_count=3),
            _seed_packet(seed=20260525, threshold=50.0, mean_regret=167.0, loss_count=2),
            _seed_packet(seed=20260601, threshold=50.0, mean_regret=169.0, loss_count=3),
        ],
        run_slug="robust-pass",
        canonical_aggregate={
            "baseline_mean_regret": 174.77,
            "mean_test_regret": 168.1566,
            "pass_level": "secondary",
        },
        bootstrap_iterations=32,
        bootstrap_seed=1,
    )
    paths = write_hf_safe_switch_scorer_robustness_packet(
        output_dir=tmp_path,
        packet=packet,
    )

    summary = packet["summary"]
    assert summary["claim_scope"] == ROBUSTNESS_CLAIM_SCOPE
    assert summary["robustness_gate_passed"] is True
    assert summary["robustness_gate_reason"] == "passed"
    assert summary["selected_operating_threshold_uah"] == 50.0
    assert summary["selected_threshold_metrics"]["seed_count"] == 5
    assert summary["selected_threshold_metrics"]["seeds_beating_canonical_count"] == 4
    assert summary["selected_threshold_metrics"]["median_switch_loss_count"] == 2.0
    assert summary["publication_receipt_verified"] is False
    assert summary["source_publication_timestamp_available"] is False
    assert summary["dt_promotion_gate_passed"] is False
    assert summary["promotion_gate_passed"] is False
    assert summary["market_execution_enabled"] is False
    assert paths["robustness_summary_json"].exists()
    assert paths["robustness_threshold_metrics_csv"].exists()
    assert paths["seed_metrics_csv"].exists()
    assert paths["failure_slices_csv"].exists()


def test_hf_safe_switch_robustness_rejects_mean_win_with_loss_switch_risk() -> None:
    packet = summarize_hf_safe_switch_scorer_robustness(
        seed_packets=[
            _seed_packet(
                seed=seed,
                threshold=50.0,
                mean_regret=158.0,
                loss_count=7,
                positive_switch_delta=80.0,
            )
            for seed in (7, 42, 2026, 20260525, 20260601)
        ],
        run_slug="robust-loss-risk",
        canonical_aggregate={
            "baseline_mean_regret": 174.77,
            "mean_test_regret": 168.1566,
            "pass_level": "secondary",
        },
        bootstrap_iterations=16,
        bootstrap_seed=1,
    )

    summary = packet["summary"]
    assert summary["robustness_gate_passed"] is False
    assert summary["robustness_gate_reason"] == "mean_win_but_loss_switch_risk"
    assert summary["selected_threshold_metrics"]["median_switch_loss_count"] == 7.0
    assert summary["selected_threshold_metrics"]["max_positive_switch_delta_uah"] == 80.0
    assert summary["canonical_comparison"]["median_hf_minus_canonical_uah"] < 0.0
    assert summary["market_execution_enabled"] is False


def test_hf_safe_switch_robustness_cli_writes_research_packet(tmp_path) -> None:
    teacher_rows_csv = tmp_path / "teacher_rows.csv"
    aggregate_json = tmp_path / "aggregate.json"
    output_dir = tmp_path / "robustness_packet"
    _csv_ready(_teacher_rows()).write_csv(teacher_rows_csv)
    aggregate_json.write_text(
        json.dumps(
            {
                "baseline_mean_regret": 174.77,
                "mean_test_regret": 168.1566,
                "pass_level": "secondary",
            }
        ),
        encoding="utf-8",
    )

    exit_code = materialize_hf_safe_switch_scorer_robustness_packet(
        [
            "--teacher-rows-csv",
            str(teacher_rows_csv),
            "--canonical-aggregate-json",
            str(aggregate_json),
            "--output-dir",
            str(output_dir),
            "--run-slug",
            "robust-cli-test",
            "--seeds",
            "7,42",
            "--thresholds-uah",
            "0,50",
            "--max-epochs",
            "1",
            "--hidden-dim",
            "8",
            "--num-layers",
            "1",
            "--num-heads",
            "1",
            "--bootstrap-iterations",
            "8",
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (output_dir / "robustness_summary.json").read_text(encoding="utf-8")
    )
    assert summary["claim_scope"] == ROBUSTNESS_CLAIM_SCOPE
    assert summary["seed_count"] == 2
    assert summary["robustness_gate_passed"] is False
    assert summary["market_execution_enabled"] is False
    assert (output_dir / "robustness_threshold_metrics.csv").exists()
    assert (output_dir / "seed_metrics.csv").exists()
    assert (output_dir / "failure_slices.csv").exists()


def _seed_packet(
    *,
    seed: int,
    threshold: float,
    mean_regret: float,
    loss_count: int,
    positive_switch_delta: float = 40.0,
) -> dict[str, object]:
    selected_rows = [
        {
            "seed": seed,
            "threshold_uah": threshold,
            "selected_schedule_family": "strict_reference",
            "selected_minus_v2_plus_regret_uah": -20.0,
            "abstained_to_v2_plus": False,
            "market_execution_enabled": False,
        },
        {
            "seed": seed,
            "threshold_uah": threshold,
            "selected_schedule_family": "raw_reference",
            "selected_minus_v2_plus_regret_uah": positive_switch_delta,
            "abstained_to_v2_plus": False,
            "market_execution_enabled": False,
        },
    ]
    return {
        "summary": {
            "run_slug": f"seed-{seed}",
            "seed": seed,
            "best_threshold_uah": threshold,
            "market_execution_enabled": False,
            "promotion_gate_passed": False,
            "dt_promotion_gate_passed": False,
        },
        "threshold_results": [
            {
                "threshold_uah": threshold,
                "metrics": {
                    "selected_mean_regret_uah": mean_regret,
                    "selected_median_regret_uah": mean_regret,
                    "selected_mean_value_uah": 1000.0 - mean_regret,
                    "v2_plus_mean_regret_uah": 174.77,
                    "selected_minus_v2_plus_mean_regret_uah": mean_regret - 174.77,
                    "non_v2_plus_switch_count": 10,
                    "abstention_count": 80,
                    "switch_win_count": 3,
                    "switch_loss_count": loss_count,
                    "switch_tie_count": 1,
                    "switch_mean_regret_delta_uah": -5.0,
                    "market_execution_enabled": False,
                },
                "selected_rows": selected_rows,
            }
        ],
    }
