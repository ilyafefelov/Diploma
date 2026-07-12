from __future__ import annotations

import json
from pathlib import Path
import pickle

from scripts.materialize_rf_safe_switch_temporal_replay import (
    main as materialize_rf_safe_switch_temporal_replay,
)
from smart_arbitrage.dfl.rf_safe_switch_temporal_replay import (
    RF_SAFE_SWITCH_TEMPORAL_REPLAY_CLAIM_SCOPE,
    build_rf_safe_switch_temporal_replay_packet,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame,
)
from tests.dfl.test_schedule_value_learner_v2_plus_robustness import (
    SOURCE_MODELS,
    TENANTS,
    _candidate_library_104,
)


def test_rf_safe_switch_temporal_replay_uses_distinct_rolling_windows() -> None:
    rolling_strict_rows = (
        build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame(
            _candidate_library_104(
                v2_plus_window_regrets=[70.0, 70.0, 70.0, 85.0]
            ),
            tenant_ids=TENANTS,
            forecast_model_names=SOURCE_MODELS,
            validation_window_count=4,
            validation_anchor_count=18,
            min_prior_anchors_before_window=30,
        )
    )

    packet = build_rf_safe_switch_temporal_replay_packet(
        rolling_strict_rows,
        run_slug="rf-safe-switch-temporal-test",
        source_model_name=SOURCE_MODELS[0],
        training_window_indices=(4, 3, 2),
        evaluation_window_index=1,
        seeds=(42,),
        bootstrap_iterations=100,
    )

    summary = packet["summary"]
    assert summary["claim_scope"] == RF_SAFE_SWITCH_TEMPORAL_REPLAY_CLAIM_SCOPE
    assert summary["model"] == "random_forest_v2_plus_safe_switch"
    assert summary["estimator_class"] == "random_forest"
    assert summary["training_window_indices"] == [4, 3, 2]
    assert summary["evaluation_window_index"] == 1
    assert summary["selector_config"] == {
        "min_predicted_improvement_uah": 20.0,
        "tail_risk_loss_threshold_uah": 150.0,
        "max_family_tail_risk_probability": 0.5,
    }
    assert summary["evaluation_independence"]["independent_holdout"] is True
    assert summary["evaluation_independence"]["content_overlap_candidate_row_count"] == 0
    assert summary["evaluation"]["distinct_market_date_count"] == 18
    assert summary["evaluation"]["profile_date_row_count"] == 90
    assert summary["seed_sensitivity"]["seed_count"] == 1
    assert summary["market_execution_enabled"] is False
    assert summary["promotion_gate_passed"] is False
    assert packet["teacher_rows"].height == (3 * 18 * 5 * 4) + (18 * 5 * 4)


def test_rf_safe_switch_temporal_replay_cli_writes_evidence_packet(
    tmp_path: Path,
) -> None:
    candidate_library_pickle = tmp_path / "candidate_library.pkl"
    output_dir = tmp_path / "temporal_replay"
    with candidate_library_pickle.open("wb") as file:
        pickle.dump(
            _candidate_library_104(
                v2_plus_window_regrets=[70.0, 70.0, 70.0, 85.0]
            ),
            file,
        )

    exit_code = materialize_rf_safe_switch_temporal_replay(
        [
            "--candidate-library-pickle",
            str(candidate_library_pickle),
            "--output-dir",
            str(output_dir),
            "--source-model-name",
            SOURCE_MODELS[0],
            "--tenant-ids",
            ",".join(TENANTS),
            "--seeds",
            "42",
            "--bootstrap-iterations",
            "100",
        ]
    )

    assert exit_code == 0
    summary = json.loads(
        (output_dir / "rf_safe_switch_temporal_replay_summary.json").read_text(
            encoding="utf-8"
        )
    )
    assert summary["evaluation_independence"]["independent_holdout"] is True
    assert (output_dir / "rf_safe_switch_temporal_teacher_rows.csv").exists()
    assert (output_dir / "rf_safe_switch_temporal_selected_rows.csv").exists()
