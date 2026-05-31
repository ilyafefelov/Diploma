from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.aggregate_canonical_experiment_metrics import (
    main as aggregate_canonical_experiment_metrics,
)
from smart_arbitrage.dfl.canonical_experiment_metrics import (
    BASELINE_V2_PLUS_MEAN_REGRET_UAH,
    classify_experiment_pass_level,
    canonical_metrics_from_dt_v2_plus_promotion_summary,
    validate_canonical_experiment_metrics_payload,
)


def test_canonical_metrics_payload_validates_required_fields() -> None:
    payload = _metrics_payload(test_regret_mean=168.16)

    normalized = validate_canonical_experiment_metrics_payload(payload)

    assert normalized["model"] == "dt_v2_plus_safe_switch"
    assert normalized["seed"] == 42
    assert normalized["test_regret_mean"] == pytest.approx(168.16)

    del payload["cmd"]
    with pytest.raises(ValueError, match="missing required fields"):
        validate_canonical_experiment_metrics_payload(payload)


def test_pass_level_marks_current_safe_switch_result_as_secondary() -> None:
    result = classify_experiment_pass_level(
        mean_test_regret=168.15664125116336,
        t_pvalue_vs_v2plus=1.0,
    )

    assert result == "secondary"


def test_pass_level_requires_primary_threshold_and_significant_pvalue() -> None:
    assert (
        classify_experiment_pass_level(
            mean_test_regret=165.95,
            t_pvalue_vs_v2plus=0.031,
        )
        == "primary"
    )
    assert (
        classify_experiment_pass_level(
            mean_test_regret=165.95,
            t_pvalue_vs_v2plus=0.07,
        )
        == "secondary"
    )
    assert (
        classify_experiment_pass_level(
            mean_test_regret=180.0,
            t_pvalue_vs_v2plus=0.001,
        )
        == "fail"
    )


def test_current_dt_v2_plus_promotion_summary_converts_to_canonical_metrics() -> None:
    summary = {
        "claim_scope": "dt_v2_plus_residual_challenger_promotion_evidence_not_market_execution",
        "run_slug": "week3_dt_v2_plus_promotion_evidence_current",
        "evaluation": {
            "selected_mean_regret_uah": 168.15664125116336,
            "selected_median_regret_uah": 61.708534960034285,
            "v2_plus_mean_regret_uah": BASELINE_V2_PLUS_MEAN_REGRET_UAH,
        },
        "gate": {
            "selected_mean_regret_uah": 168.15664125116336,
            "promotion_gate_passed": False,
            "market_execution_enabled": False,
            "source_model_name": "nbeatsx_official_global_panel_horizon_calibrated_v1",
        },
    }

    payload = canonical_metrics_from_dt_v2_plus_promotion_summary(
        summary,
        seed=42,
        git_commit="abc1234",
        cmd="materialized-existing-dt-v2-plus-promotion-summary",
    )

    assert payload["run_id"] == "week3_dt_v2_plus_promotion_evidence_current"
    assert payload["model"] == "dt_v2_plus_safe_switch_selector"
    assert payload["val_regret_mean"] == pytest.approx(168.15664125116336)
    assert payload["test_regret_mean"] == pytest.approx(168.15664125116336)


def test_aggregate_cli_writes_primary_summary_for_three_seed_model(tmp_path: Path) -> None:
    model_dir = tmp_path / "runs" / "my_model_v3"
    for seed, regret in [(42, 165.8), (2026, 166.0), (7, 165.6)]:
        seed_dir = model_dir / f"seed_{seed}"
        seed_dir.mkdir(parents=True)
        (seed_dir / "metrics.json").write_text(
            json.dumps(_metrics_payload(seed=seed, test_regret_mean=regret)),
            encoding="utf-8",
        )
    baseline_path = tmp_path / "baseline_seed_means.json"
    baseline_path.write_text(json.dumps([174.5, 174.77, 175.1]), encoding="utf-8")

    exit_code = aggregate_canonical_experiment_metrics(
        [
            "--model-dir",
            str(model_dir),
            "--baseline-seed-means-json",
            str(baseline_path),
        ]
    )

    assert exit_code == 0
    aggregate = json.loads((model_dir / "aggregate.json").read_text(encoding="utf-8"))
    assert aggregate["model"] == "my_model_v3"
    assert aggregate["n_seeds"] == 3
    assert aggregate["mean_test_regret"] == pytest.approx(165.8)
    assert aggregate["pass_level"] == "primary"
    assert aggregate["baseline_mean_regret"] == pytest.approx(BASELINE_V2_PLUS_MEAN_REGRET_UAH)


def _metrics_payload(
    *,
    seed: int = 42,
    test_regret_mean: float,
) -> dict[str, object]:
    return {
        "run_id": f"2026-05-30T14-22-11Z_seed_{seed}",
        "model": "dt_v2_plus_safe_switch",
        "seed": seed,
        "epoch": 37,
        "val_regret_mean": test_regret_mean + 1.0,
        "val_regret_std": 14.77,
        "test_regret_mean": test_regret_mean,
        "test_regret_std": 13.42,
        "early_stop_epoch": 37,
        "wall_time_s": 12873.5,
        "gpu_hours": 3.6,
        "max_mem_gb": 21.4,
        "git_commit": "a1b2c3d",
        "cmd": "python train.py --seed 42 --model dt_v2_plus_safe_switch",
    }
