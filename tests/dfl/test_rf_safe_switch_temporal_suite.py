from __future__ import annotations

import json
from pathlib import Path

from scripts.aggregate_rf_safe_switch_temporal_suite import main


def test_temporal_suite_cli_aggregates_independent_protocol_rows(
    tmp_path: Path,
) -> None:
    calibrated = tmp_path / "calibrated.json"
    raw = tmp_path / "raw.json"
    calibrated.write_text(
        json.dumps(
            _summary(
                source="calibrated",
                threshold=20.0,
                selector_regret=174.77,
                v2_plus_regret=174.77,
                switch_count=0,
            )
        ),
        encoding="utf-8",
    )
    raw.write_text(
        json.dumps(
            _summary(
                source="raw",
                threshold=0.0,
                selector_regret=190.0,
                v2_plus_regret=193.36,
                switch_count=5,
            )
        ),
        encoding="utf-8",
    )
    output_json = tmp_path / "suite.json"
    output_csv = tmp_path / "suite.csv"

    exit_code = main(
        [
            "--input-summary",
            str(calibrated),
            "--input-summary",
            str(raw),
            "--output-json",
            str(output_json),
            "--output-csv",
            str(output_csv),
        ]
    )

    assert exit_code == 0
    suite = json.loads(output_json.read_text(encoding="utf-8"))
    assert suite["run_count"] == 2
    assert suite["source_model_names"] == ["calibrated", "raw"]
    assert suite["evaluation_window_indices"] == [1]
    assert suite["thresholds_uah"] == [0.0, 20.0]
    assert suite["all_independent_holdouts"] is True
    assert suite["maximum_content_overlap_ratio"] == 0.0
    assert suite["promotion_gate_passed"] is False
    assert suite["market_execution_enabled"] is False
    rows = suite["rows"]
    assert rows[0]["source_model_name"] == "calibrated"
    assert rows[0]["selector_minus_v2_plus_mean_regret_uah"] == 0.0
    assert rows[0]["seed_delta_mean_uah"] == 0.0
    assert rows[0]["seed_delta_min_uah"] == 0.0
    assert rows[0]["seed_delta_max_uah"] == 0.0
    assert rows[0]["seed_switch_count_min"] == 0
    assert rows[0]["seed_switch_count_max"] == 0
    assert rows[1]["source_model_name"] == "raw"
    assert rows[1]["selector_minus_v2_plus_mean_regret_uah"] == -3.36
    assert output_csv.exists()


def _summary(
    *,
    source: str,
    threshold: float,
    selector_regret: float,
    v2_plus_regret: float,
    switch_count: int,
) -> dict[str, object]:
    delta = round(selector_regret - v2_plus_regret, 2)
    return {
        "run_slug": f"{source}-{threshold}",
        "claim_scope": (
            "rf_safe_switch_temporal_replay_retrospective_not_market_execution"
        ),
        "model": "random_forest_v2_plus_safe_switch",
        "estimator_class": "random_forest",
        "source_model_name": source,
        "training_window_indices": [4, 3, 2],
        "evaluation_window_index": 1,
        "selector_config": {
            "min_predicted_improvement_uah": threshold,
            "tail_risk_loss_threshold_uah": 150.0,
            "max_family_tail_risk_probability": 0.5,
        },
        "evaluation_independence": {
            "content_overlap_candidate_row_count": 0,
            "content_overlap_ratio": 0.0,
            "evaluation_candidate_row_count": 360,
            "independent_holdout": True,
            "train_candidate_row_count": 1080,
        },
        "seed_sensitivity": {
            "seeds": [42],
            "rows": [
                {
                    "seed": 42,
                    "selector_minus_v2_plus_mean_regret_uah": delta,
                    "non_v2_plus_switch_count": switch_count,
                }
            ],
        },
        "evaluation": {
            "profile_date_row_count": 90,
            "distinct_market_date_count": 18,
            "selector_mean_regret_uah": selector_regret,
            "v2_plus_mean_regret_uah": v2_plus_regret,
            "selector_minus_v2_plus_mean_regret_uah": delta,
            "non_v2_plus_switch_count": switch_count,
            "abstention_count": 90 - switch_count,
            "distinct_switch_date_count": min(switch_count, 1),
            "observed_tail_loss_count": 0,
        },
        "date_cluster_summary": {
            "date_win_count": int(delta < 0),
            "date_tie_count": int(delta == 0),
            "date_loss_count": int(delta > 0),
            "moving_block_bootstrap": {
                "ci_low_uah": delta,
                "ci_high_uah": delta,
            },
        },
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
