from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pytest

from smart_arbitrage.dfl.full_history_hf_candidate_ranker import (
    build_full_history_ranker_candidate_frame,
    fixed_universe_v2_plus_reference_for_future_test,
    run_full_history_hf_candidate_ranker,
    split_full_history_candidate_frame,
)


def test_full_history_ranker_uses_prior_train_validation_and_future_test_blocks() -> None:
    frame = _candidate_rows(anchor_count=8)

    split = split_full_history_candidate_frame(
        frame,
        test_start=datetime(2026, 1, 8, 23),
        validation_anchor_count=2,
        minimum_train_anchor_count=5,
    )

    assert split["train_anchor_count"] == 5
    assert split["validation_anchor_count"] == 2
    assert split["test_anchor_count"] == 1
    assert split["train_end"] == "2026-01-05T23:00:00"
    assert split["validation_start"] == "2026-01-06T23:00:00"
    assert split["test_start"] == "2026-01-08T23:00:00"
    assert split["train_rows"].select(pl.col("anchor_timestamp").max()).item() < split[
        "validation_rows"
    ].select(pl.col("anchor_timestamp").min()).item()
    assert split["validation_rows"].select(pl.col("anchor_timestamp").max()).item() < split[
        "test_rows"
    ].select(pl.col("anchor_timestamp").min()).item()


def test_full_history_ranker_refuses_the_18_day_packet_by_default() -> None:
    with pytest.raises(ValueError, match="at least 293 prior anchors"):
        split_full_history_candidate_frame(
            _candidate_rows(anchor_count=19),
            test_start=datetime(2026, 1, 20, 23),
        )


def test_full_history_ranker_adds_a_prior_only_v2_plus_fallback_per_anchor() -> None:
    library = _candidate_rows(anchor_count=2).drop(
        [
            "dt_schedule_family_target",
            "regret_delta_vs_v2_plus_uah",
            "schedule_value_uah",
        ]
    )
    panel = build_full_history_ranker_candidate_frame(library)

    assert panel.group_by(
        ["tenant_id", "source_model_name", "anchor_timestamp"]
    ).len().get_column("len").to_list() == [3, 3]
    fallback = panel.filter(
        pl.col("dt_schedule_family_target") == "schedule_value_learner_v2_plus"
    )
    assert fallback.height == 2
    assert fallback.get_column("regret_delta_vs_v2_plus_uah").to_list() == [0.0, 0.0]
    assert fallback.get_column("market_execution_enabled").unique().to_list() == [False]


def test_fixed_universe_v2_plus_reference_uses_history_to_freeze_future_comparator() -> None:
    panel = build_full_history_ranker_candidate_frame(_candidate_rows(anchor_count=8))

    reference = fixed_universe_v2_plus_reference_for_future_test(
        panel,
        test_start=datetime(2026, 1, 8, 23),
    )

    assert reference["calibrated"]["reference_kind"] == "fixed_universe_frozen_v2_plus"
    assert reference["calibrated"]["future_row_count"] == 1
    assert reference["calibrated"]["future_mean_regret_uah"] == 95.0


def test_full_history_ranker_freezes_validation_threshold_before_future_test(
    tmp_path: Path,
) -> None:
    result = run_full_history_hf_candidate_ranker(
        build_full_history_ranker_candidate_frame(_candidate_rows(anchor_count=8)),
        test_start=datetime(2026, 1, 8, 23),
        validation_anchor_count=2,
        minimum_train_anchor_count=5,
        max_epochs=1,
        hidden_dim=16,
        num_layers=1,
        num_heads=2,
        output_dir=tmp_path,
    )

    source_result = result["sources"]["calibrated"]
    assert source_result["threshold_selected_on"] == "validation"
    assert source_result["test_anchor_count"] == 1
    assert source_result["market_execution_enabled"] is False
    assert (tmp_path / "calibrated" / "test_summary.json").exists()


def _candidate_rows(*, anchor_count: int) -> pl.DataFrame:
    start = datetime(2026, 1, 1, 23)
    rows: list[dict[str, object]] = []
    for index in range(anchor_count):
        anchor = start + timedelta(days=index)
        for family, delta in (("strict_control", 0.0), ("robust", -5.0)):
            rows.append(
                {
                    "tenant_id": "client_001",
                    "source_model_name": "calibrated",
                    "anchor_timestamp": anchor,
                    "dt_schedule_family_target": family,
                    "candidate_family": family,
                    "candidate_model_name": family,
                    "regret_delta_vs_v2_plus_uah": delta,
                    "regret_uah": 100.0 + delta,
                    "schedule_value_uah": 1_000.0 - delta,
                    "decision_value_uah": 1_000.0 - delta,
                    "forecast_objective_value_uah": 900.0,
                    "prior_family_mean_regret_uah": 100.0 + delta,
                    "forecast_price_uah_mwh_vector": [100.0, 200.0],
                    "dispatch_mw_vector": [0.1, -0.1],
                    "soc_fraction_vector": [0.5, 0.5],
                    "selector_feature_forecast_spread_uah_mwh": 100.0,
                    "forecast_spread_uah_mwh": 100.0,
                    "selector_feature_terminal_soc_delta_fraction": 0.0,
                    "selector_feature_total_throughput_delta_mwh": 0.2,
                    "total_throughput_mwh": 0.2,
                    "selector_feature_total_degradation_penalty_uah": 1.0,
                    "total_degradation_penalty_uah": 1.0,
                    "selector_feature_soc_min_slack_fraction": 0.2,
                    "soc_min_slack_fraction": 0.2,
                    "selector_feature_candidate_index": 0.0,
                    "selector_feature_candidate_count": 2.0,
                    "selector_feature_anchor_hour": 23.0,
                }
            )
    return pl.DataFrame(rows)
