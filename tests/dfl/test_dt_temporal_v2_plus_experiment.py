from __future__ import annotations

import polars as pl

from smart_arbitrage.dfl.dt_temporal_v2_plus_experiment import (
    DT_TEMPORAL_V2_PLUS_EXPERIMENT_CLAIM_SCOPE,
    run_dt_temporal_v2_plus_experiment,
)
from smart_arbitrage.dfl.dt_research_shadow import (
    OBJECTIVE_KIND_CROSS_ENTROPY,
    OBJECTIVE_KIND_DECISION_AWARE,
)
from smart_arbitrage.dfl.schedule_value_learner_v2_plus_robustness import (
    build_dfl_schedule_value_learner_v2_plus_rolling_strict_rows_frame,
)
from tests.dfl.test_schedule_value_learner_v2_plus_robustness import (
    SOURCE_MODELS,
    TENANTS,
    _candidate_library_104,
)


def test_dt_temporal_experiment_compares_bc_and_decision_aware_objectives(
    tmp_path,
) -> None:
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
        .with_columns(
            pl.when(pl.col("evaluation_window_index") == 1)
            .then(pl.col("regret_uah") + 0.123)
            .otherwise(pl.col("regret_uah"))
            .alias("regret_uah"),
            pl.when(pl.col("evaluation_window_index") == 1)
            .then(pl.col("decision_value_uah") - 0.123)
            .otherwise(pl.col("decision_value_uah"))
            .alias("decision_value_uah"),
        )
    )

    result = run_dt_temporal_v2_plus_experiment(
        rolling_strict_rows,
        output_dir=tmp_path,
        run_slug="dt-temporal-test",
        source_model_names=(SOURCE_MODELS[0],),
        evaluation_window_indices=(1,),
        objective_kinds=(
            OBJECTIVE_KIND_CROSS_ENTROPY,
            OBJECTIVE_KIND_DECISION_AWARE,
        ),
        seeds=(42,),
        context_length=4,
        max_epochs=1,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        learning_rate=0.001,
        model_backbone="local",
    )

    summary = result["summary"]
    rows = result["rows"]
    assert summary["claim_scope"] == DT_TEMPORAL_V2_PLUS_EXPERIMENT_CLAIM_SCOPE
    assert summary["protocol_run_count"] == 2
    assert summary["all_protocols_independent"] is True
    assert summary["promotable_v13_permitted_training_rows"] == 0
    assert summary["market_execution_enabled"] is False
    assert {row["objective_kind"] for row in rows} == {
        OBJECTIVE_KIND_CROSS_ENTROPY,
        OBJECTIVE_KIND_DECISION_AWARE,
    }
    assert all(row["content_overlap_candidate_row_count"] == 0 for row in rows)
    assert all(row["returns_to_go_nonzero_count"] > 0 for row in rows)
    assert all(row["action_target_count"] > 0 for row in rows)
    assert all(row["evaluation_sequence_count"] == 90 for row in rows)
    assert (tmp_path / "dt_temporal_v2_plus_experiment_summary.json").exists()
    assert (tmp_path / "dt_temporal_v2_plus_experiment_rows.csv").exists()
