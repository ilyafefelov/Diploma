"""Retrospective time-separated random-forest safe-switch replay."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
import json
from pathlib import Path
from statistics import mean, pstdev
from typing import Any, Final

import numpy as np
import polars as pl

from smart_arbitrage.dfl.dt_research_shadow import (
    build_dt_research_shadow_teacher_rows_from_temporal_v2_plus_strict_rows,
)
from smart_arbitrage.dfl.regret_aware_v2_plus_selector import (
    FEATURE_SET_EXPANDED,
    MODEL_KIND_RANDOM_FOREST,
    V2_PLUS_FAMILY_ALIASES,
    build_regret_aware_v2_plus_selector_packet,
)

RF_SAFE_SWITCH_TEMPORAL_REPLAY_CLAIM_SCOPE: Final[str] = (
    "rf_safe_switch_temporal_replay_retrospective_not_market_execution"
)
DEFAULT_TAIL_RISK_LOSS_THRESHOLD_UAH: Final[float] = 150.0


def build_rf_safe_switch_temporal_replay_packet(
    rolling_strict_rows_frame: pl.DataFrame,
    *,
    run_slug: str,
    source_model_name: str,
    training_window_indices: tuple[int, ...] = (4, 3, 2),
    evaluation_window_index: int = 1,
    seeds: tuple[int, ...] = (42, 2026, 7),
    min_predicted_improvement_uah: float = 20.0,
    tail_risk_loss_threshold_uah: float = DEFAULT_TAIL_RISK_LOSS_THRESHOLD_UAH,
    max_family_tail_risk_probability: float = 0.5,
    bootstrap_iterations: int = 20_000,
    bootstrap_block_length: int = 3,
    bootstrap_seed: int = 20260712,
) -> dict[str, Any]:
    """Fit on earlier rolling windows and score one later retrospective window."""

    _validate_config(
        run_slug=run_slug,
        source_model_name=source_model_name,
        training_window_indices=training_window_indices,
        evaluation_window_index=evaluation_window_index,
        seeds=seeds,
        bootstrap_iterations=bootstrap_iterations,
        bootstrap_block_length=bootstrap_block_length,
    )
    source_rows = rolling_strict_rows_frame.filter(
        pl.col("source_model_name") == source_model_name
    )
    training_strict_rows = source_rows.filter(
        pl.col("evaluation_window_index").is_in(training_window_indices)
    )
    evaluation_strict_rows = source_rows.filter(
        pl.col("evaluation_window_index") == evaluation_window_index
    )
    if training_strict_rows.is_empty():
        raise ValueError("temporal replay has no training strict rows.")
    if evaluation_strict_rows.is_empty():
        raise ValueError("temporal replay has no evaluation strict rows.")
    latest_training_anchor = training_strict_rows["anchor_timestamp"].max()
    earliest_evaluation_anchor = evaluation_strict_rows["anchor_timestamp"].min()
    if not isinstance(latest_training_anchor, datetime) or not isinstance(
        earliest_evaluation_anchor, datetime
    ):
        raise ValueError("temporal replay requires datetime anchor timestamps.")
    if latest_training_anchor >= earliest_evaluation_anchor:
        raise ValueError(
            "temporal replay training anchors must predate evaluation anchors."
        )

    teacher_rows = (
        build_dt_research_shadow_teacher_rows_from_temporal_v2_plus_strict_rows(
            training_strict_rows_frame=training_strict_rows,
            evaluation_strict_rows_frame=evaluation_strict_rows,
        )
    )
    seed_summaries: list[dict[str, Any]] = []
    primary_selected_rows: pl.DataFrame | None = None
    primary_selector_summary: dict[str, Any] | None = None
    for seed in seeds:
        result = build_regret_aware_v2_plus_selector_packet(
            teacher_rows,
            run_slug=f"{run_slug}_seed_{seed}",
            min_predicted_improvement_uah=min_predicted_improvement_uah,
            tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
            max_family_tail_risk_probability=max_family_tail_risk_probability,
            model_kind=MODEL_KIND_RANDOM_FOREST,
            feature_set=FEATURE_SET_EXPANDED,
            random_seed=seed,
        )
        selector_summary = dict(result["summary"])
        independence = dict(selector_summary["evaluation_independence"])
        if not bool(independence["independent_holdout"]):
            raise ValueError("temporal replay requires zero train/evaluation content overlap.")
        selected_rows = result["selected_rows"]
        if not isinstance(selected_rows, pl.DataFrame):
            raise TypeError("selector selected_rows must be a Polars DataFrame.")
        evaluation = dict(selector_summary["evaluation"])
        seed_summaries.append(
            {
                "seed": seed,
                "selector_mean_regret_uah": float(
                    evaluation["selector_mean_regret_uah"]
                ),
                "v2_plus_mean_regret_uah": float(
                    evaluation["v2_plus_mean_regret_uah"]
                ),
                "selector_minus_v2_plus_mean_regret_uah": float(
                    evaluation["selector_minus_v2_plus_mean_regret_uah"]
                ),
                "non_v2_plus_switch_count": int(
                    evaluation["non_v2_plus_switch_count"]
                ),
                "abstention_count": int(evaluation["abstention_count"]),
            }
        )
        if primary_selected_rows is None:
            primary_selected_rows = selected_rows
            primary_selector_summary = selector_summary
    if primary_selected_rows is None or primary_selector_summary is None:
        raise ValueError("temporal replay did not produce a primary seed result.")

    date_cluster = _date_cluster_summary(
        primary_selected_rows,
        iterations=bootstrap_iterations,
        block_length=bootstrap_block_length,
        seed=bootstrap_seed,
    )
    primary_evaluation = dict(primary_selector_summary["evaluation"])
    switch_rows = primary_selected_rows.filter(
        ~pl.col("selected_schedule_family").is_in(sorted(V2_PLUS_FAMILY_ALIASES))
    )
    switch_dates = sorted(
        {_market_date(value) for value in switch_rows["anchor_timestamp"].to_list()}
    )
    seed_regrets = [row["selector_mean_regret_uah"] for row in seed_summaries]
    summary = {
        "run_slug": run_slug,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "claim_scope": RF_SAFE_SWITCH_TEMPORAL_REPLAY_CLAIM_SCOPE,
        "model": "random_forest_v2_plus_safe_switch",
        "estimator_class": "random_forest",
        "source_model_name": source_model_name,
        "training_window_indices": list(training_window_indices),
        "evaluation_window_index": evaluation_window_index,
        "selector_config": {
            "min_predicted_improvement_uah": min_predicted_improvement_uah,
            "tail_risk_loss_threshold_uah": tail_risk_loss_threshold_uah,
            "max_family_tail_risk_probability": max_family_tail_risk_probability,
        },
        "training_strict_row_count": training_strict_rows.height,
        "evaluation_strict_row_count": evaluation_strict_rows.height,
        "evaluation_independence": dict(
            primary_selector_summary["evaluation_independence"]
        ),
        "primary_seed": seeds[0],
        "evaluation": {
            "profile_date_row_count": primary_selected_rows.height,
            "distinct_market_date_count": len(
                {
                    _market_date(value)
                    for value in primary_selected_rows["anchor_timestamp"].to_list()
                }
            ),
            "selector_mean_regret_uah": float(
                primary_evaluation["selector_mean_regret_uah"]
            ),
            "v2_plus_mean_regret_uah": float(
                primary_evaluation["v2_plus_mean_regret_uah"]
            ),
            "selector_minus_v2_plus_mean_regret_uah": float(
                primary_evaluation["selector_minus_v2_plus_mean_regret_uah"]
            ),
            "non_v2_plus_switch_count": int(
                primary_evaluation["non_v2_plus_switch_count"]
            ),
            "abstention_count": int(primary_evaluation["abstention_count"]),
            "distinct_switch_dates": switch_dates,
            "distinct_switch_date_count": len(switch_dates),
            "observed_tail_loss_count": switch_rows.filter(
                pl.col("selected_minus_v2_plus_regret_uah")
                >= tail_risk_loss_threshold_uah
            ).height,
        },
        "date_cluster_summary": date_cluster,
        "seed_sensitivity": {
            "seed_count": len(seed_summaries),
            "seeds": list(seeds),
            "selector_mean_regret_mean_uah": mean(seed_regrets),
            "selector_mean_regret_std_uah": (
                pstdev(seed_regrets) if len(seed_regrets) > 1 else 0.0
            ),
            "rows": seed_summaries,
            "interpretation": "model-stability sensitivity, not independent replication",
        },
        "interpretation": (
            "retrospective time-separated replay; not untouched prospective confirmation"
        ),
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    return {
        "summary": summary,
        "teacher_rows": teacher_rows,
        "selected_rows": primary_selected_rows,
        "rolling_strict_rows": source_rows,
    }


def write_rf_safe_switch_temporal_replay_packet(
    *,
    output_dir: Path,
    packet: dict[str, Any],
) -> dict[str, Path]:
    """Write replay evidence using explicit lineage-oriented filenames."""

    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "summary_json": output_dir / "rf_safe_switch_temporal_replay_summary.json",
        "teacher_rows_csv": output_dir / "rf_safe_switch_temporal_teacher_rows.csv",
        "selected_rows_csv": output_dir / "rf_safe_switch_temporal_selected_rows.csv",
        "rolling_strict_rows_csv": output_dir / "rf_safe_switch_rolling_strict_rows.csv",
    }
    paths["summary_json"].write_text(
        json.dumps(packet["summary"], indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    for key, frame_key in (
        ("teacher_rows_csv", "teacher_rows"),
        ("selected_rows_csv", "selected_rows"),
        ("rolling_strict_rows_csv", "rolling_strict_rows"),
    ):
        frame = packet[frame_key]
        if not isinstance(frame, pl.DataFrame):
            raise TypeError(f"{frame_key} must be a Polars DataFrame.")
        _csv_ready(frame).write_csv(paths[key])
    return paths


def _date_cluster_summary(
    selected_rows: pl.DataFrame,
    *,
    iterations: int,
    block_length: int,
    seed: int,
) -> dict[str, Any]:
    date_differences: dict[str, list[float]] = {}
    for row in selected_rows.iter_rows(named=True):
        date_differences.setdefault(_market_date(row["anchor_timestamp"]), []).append(
            float(row["selected_minus_v2_plus_regret_uah"])
        )
    ordered_dates = sorted(date_differences)
    cluster_means = [mean(date_differences[date]) for date in ordered_dates]
    bootstrap_means = _circular_block_bootstrap_means(
        cluster_means,
        iterations=iterations,
        block_length=block_length,
        seed=seed,
    )
    return {
        "unit": "market_date_mean_across_configured_profiles",
        "distinct_market_date_count": len(ordered_dates),
        "mean_selector_minus_v2_plus_regret_uah": mean(cluster_means),
        "date_win_count": sum(value < 0.0 for value in cluster_means),
        "date_tie_count": sum(value == 0.0 for value in cluster_means),
        "date_loss_count": sum(value > 0.0 for value in cluster_means),
        "moving_block_bootstrap": {
            "iterations": iterations,
            "block_length_dates": block_length,
            "seed": seed,
            "ci_low_uah": float(np.quantile(bootstrap_means, 0.025)),
            "ci_high_uah": float(np.quantile(bootstrap_means, 0.975)),
        },
    }


def _circular_block_bootstrap_means(
    values: Sequence[float],
    *,
    iterations: int,
    block_length: int,
    seed: int,
) -> np.ndarray:
    if not values:
        raise ValueError("date-cluster bootstrap requires at least one value.")
    rng = np.random.default_rng(seed)
    observed = np.asarray(values, dtype=float)
    result = np.empty(iterations, dtype=float)
    block_count = int(np.ceil(len(observed) / block_length))
    offsets = np.arange(block_length)
    for index in range(iterations):
        starts = rng.integers(0, len(observed), size=block_count)
        sample_indices = ((starts[:, None] + offsets) % len(observed)).reshape(-1)
        result[index] = float(observed[sample_indices[: len(observed)]].mean())
    return result


def _market_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    return str(value)[:10]


def _csv_ready(frame: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column in frame.columns:
        if frame.schema[column].is_nested():
            expressions.append(
                pl.col(column)
                .map_elements(
                    _json_text,
                    return_dtype=pl.String,
                )
                .alias(column)
            )
    return frame.with_columns(expressions) if expressions else frame


def _json_text(value: object) -> str:
    if isinstance(value, pl.Series):
        value = value.to_list()
    return json.dumps(value, default=str, sort_keys=True)


def _validate_config(
    *,
    run_slug: str,
    source_model_name: str,
    training_window_indices: tuple[int, ...],
    evaluation_window_index: int,
    seeds: tuple[int, ...],
    bootstrap_iterations: int,
    bootstrap_block_length: int,
) -> None:
    if not run_slug.strip():
        raise ValueError("run_slug must not be empty.")
    if not source_model_name.strip():
        raise ValueError("source_model_name must not be empty.")
    if not training_window_indices:
        raise ValueError("training_window_indices must not be empty.")
    if evaluation_window_index in training_window_indices:
        raise ValueError("evaluation window must not be used for training.")
    if len(training_window_indices) != len(set(training_window_indices)):
        raise ValueError("training_window_indices must be unique.")
    if not seeds or len(seeds) != len(set(seeds)):
        raise ValueError("seeds must be non-empty and unique.")
    if bootstrap_iterations <= 0:
        raise ValueError("bootstrap_iterations must be positive.")
    if bootstrap_block_length <= 0:
        raise ValueError("bootstrap_block_length must be positive.")


__all__ = [
    "RF_SAFE_SWITCH_TEMPORAL_REPLAY_CLAIM_SCOPE",
    "build_rf_safe_switch_temporal_replay_packet",
    "write_rf_safe_switch_temporal_replay_packet",
]
