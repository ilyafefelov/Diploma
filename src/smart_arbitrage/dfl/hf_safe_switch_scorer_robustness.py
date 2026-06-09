"""Robustness aggregation for HF safe-switch scorer research packets."""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Final

import numpy as np
import polars as pl

from smart_arbitrage.dfl.hf_safe_switch_scorer import (
    build_hf_safe_switch_scorer_packet,
)

ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "hf_safe_switch_scorer_robustness_shadow_not_promotable_not_market_execution"
)
ROBUSTNESS_SUMMARY_JSON_NAME: Final[str] = "robustness_summary.json"
ROBUSTNESS_THRESHOLD_METRICS_CSV_NAME: Final[str] = (
    "robustness_threshold_metrics.csv"
)
SEED_METRICS_CSV_NAME: Final[str] = "seed_metrics.csv"
FAILURE_SLICES_CSV_NAME: Final[str] = "failure_slices.csv"
BEST_SELECTED_ROWS_CSV_NAME: Final[str] = "best_selected_rows.csv"
DEFAULT_ROBUSTNESS_SEEDS: Final[tuple[int, ...]] = (
    7,
    42,
    2026,
    20260525,
    20260601,
)
DEFAULT_ROBUSTNESS_THRESHOLDS_UAH: Final[tuple[float, ...]] = (
    0.0,
    5.0,
    10.0,
    20.0,
    30.0,
    40.0,
    50.0,
    75.0,
    100.0,
)
MIN_REQUIRED_SEED_COUNT: Final[int] = 5
MIN_CANONICAL_WIN_COUNT: Final[int] = 4
MAX_MEDIAN_SWITCH_LOSS_COUNT: Final[float] = 3.0
MAX_POSITIVE_SWITCH_DELTA_UAH: Final[float] = 50.0


def build_hf_safe_switch_scorer_robustness_packet(
    teacher_rows_frame: pl.DataFrame,
    *,
    run_slug: str,
    seeds: Sequence[int] = DEFAULT_ROBUSTNESS_SEEDS,
    thresholds_uah: Sequence[float] = DEFAULT_ROBUSTNESS_THRESHOLDS_UAH,
    max_epochs: int = 200,
    hidden_dim: int = 64,
    num_layers: int = 2,
    num_heads: int = 2,
    learning_rate: float = 0.003,
    weight_decay: float = 0.01,
    regret_scale_uah: float = 100.0,
    safe_switch_extra_weight: float = 30.0,
    pairwise_margin_scaled: float = 0.2,
    max_predicted_tail_risk_probability: float = 0.5,
    max_family_tail_risk_probability: float = 1.0,
    tail_risk_loss_threshold_uah: float = 150.0,
    canonical_aggregate: Mapping[str, Any] | None = None,
    output_dir: Path | None = None,
    save_checkpoints: bool = False,
    bootstrap_iterations: int = 1000,
    bootstrap_seed: int = 20260601,
) -> dict[str, Any]:
    """Train multiple HF scorer seeds and aggregate robustness evidence."""

    if not seeds:
        raise ValueError("seeds must not be empty.")
    seed_packets: list[dict[str, Any]] = []
    for seed in seeds:
        seed_output_dir = (
            output_dir / f"seed_{seed}" if output_dir is not None and save_checkpoints else None
        )
        seed_packet = build_hf_safe_switch_scorer_packet(
            teacher_rows_frame=teacher_rows_frame,
            run_slug=f"{run_slug}_seed_{seed}",
            thresholds_uah=thresholds_uah,
            max_epochs=max_epochs,
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            num_heads=num_heads,
            learning_rate=learning_rate,
            weight_decay=weight_decay,
            regret_scale_uah=regret_scale_uah,
            safe_switch_extra_weight=safe_switch_extra_weight,
            pairwise_margin_scaled=pairwise_margin_scaled,
            max_predicted_tail_risk_probability=(
                max_predicted_tail_risk_probability
            ),
            max_family_tail_risk_probability=max_family_tail_risk_probability,
            tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
            seed=int(seed),
            output_dir=seed_output_dir,
            canonical_aggregate=canonical_aggregate,
            save_checkpoint=save_checkpoints,
        )
        seed_packet["summary"]["seed"] = int(seed)
        seed_packet.pop("model", None)
        seed_packets.append(seed_packet)
    return summarize_hf_safe_switch_scorer_robustness(
        seed_packets=seed_packets,
        run_slug=run_slug,
        canonical_aggregate=canonical_aggregate,
        bootstrap_iterations=bootstrap_iterations,
        bootstrap_seed=bootstrap_seed,
    )


def summarize_hf_safe_switch_scorer_robustness(
    *,
    seed_packets: Sequence[Mapping[str, Any]],
    run_slug: str,
    canonical_aggregate: Mapping[str, Any] | None = None,
    bootstrap_iterations: int = 1000,
    bootstrap_seed: int = 20260601,
) -> dict[str, Any]:
    """Aggregate already-built seed packets into robustness evidence."""

    if not seed_packets:
        raise ValueError("seed_packets must not be empty.")
    canonical = dict(canonical_aggregate or {})
    baseline_regret = float(canonical.get("baseline_mean_regret", 174.77))
    canonical_regret = float(canonical.get("mean_test_regret", 168.1566))
    seed_metrics, selected_rows = _seed_metric_rows(
        seed_packets=seed_packets,
        baseline_regret=baseline_regret,
        canonical_regret=canonical_regret,
    )
    threshold_metrics = _threshold_metric_rows(
        seed_metrics=seed_metrics,
        selected_rows=selected_rows,
        baseline_regret=baseline_regret,
        canonical_regret=canonical_regret,
    )
    if not threshold_metrics:
        raise ValueError("no threshold metrics found in seed packets.")
    selected_threshold = _select_threshold(threshold_metrics)
    selected_seed_metrics = [
        row
        for row in seed_metrics
        if float(row["threshold_uah"]) == float(selected_threshold["threshold_uah"])
    ]
    selected_rows_for_threshold = [
        row
        for row in selected_rows
        if float(row["threshold_uah"]) == float(selected_threshold["threshold_uah"])
    ]
    failure_slices = _failure_slice_rows(selected_rows_for_threshold)
    gate_passed, gate_reason = _robustness_gate(
        selected_threshold,
        baseline_regret=baseline_regret,
        canonical_regret=canonical_regret,
    )
    selected_means = [
        float(row["selected_mean_regret_uah"]) for row in selected_seed_metrics
    ]
    summary = {
        "claim_scope": ROBUSTNESS_CLAIM_SCOPE,
        "run_slug": run_slug,
        "generated_at": datetime.now(UTC).isoformat(),
        "seed_count": len(seed_packets),
        "seeds": [int(_seed_from_packet(packet, fallback=index)) for index, packet in enumerate(seed_packets)],
        "thresholds_uah": sorted(
            {float(row["threshold_uah"]) for row in seed_metrics}
        ),
        "selected_operating_threshold_uah": float(
            selected_threshold["threshold_uah"]
        ),
        "selected_threshold_selection_reason": (
            "guard_passed_best_mean"
            if bool(selected_threshold["robustness_gate_passed"])
            else "diagnostic_best_mean_no_guard_pass"
        ),
        "selected_threshold_metrics": selected_threshold,
        "robustness_gate_passed": bool(gate_passed),
        "robustness_gate_reason": gate_reason,
        "robustness_gate_config": {
            "min_required_seed_count": MIN_REQUIRED_SEED_COUNT,
            "min_canonical_win_count": MIN_CANONICAL_WIN_COUNT,
            "max_median_switch_loss_count": MAX_MEDIAN_SWITCH_LOSS_COUNT,
            "max_positive_switch_delta_uah": MAX_POSITIVE_SWITCH_DELTA_UAH,
            "requires_every_seed_to_beat_v2_plus_baseline": True,
        },
        "canonical_comparison": {
            "v2_plus_baseline_mean_regret_uah": baseline_regret,
            "canonical_safe_switch_mean_regret_uah": canonical_regret,
            "canonical_pass_level": str(canonical.get("pass_level", "")),
            "median_hf_mean_regret_uah": float(
                selected_threshold["selected_mean_regret_median"]
            ),
            "mean_hf_mean_regret_uah": float(
                selected_threshold["selected_mean_regret_mean"]
            ),
            "median_hf_minus_canonical_uah": float(
                selected_threshold["selected_mean_regret_median"]
            )
            - canonical_regret,
            "mean_hf_minus_canonical_uah": float(
                selected_threshold["selected_mean_regret_mean"]
            )
            - canonical_regret,
            "mean_hf_minus_v2_plus_uah": float(
                selected_threshold["selected_mean_regret_mean"]
            )
            - baseline_regret,
            "market_execution_enabled": False,
        },
        "bootstrap_ci": _bootstrap_ci(
            selected_means,
            baseline_regret=baseline_regret,
            canonical_regret=canonical_regret,
            iterations=bootstrap_iterations,
            seed=bootstrap_seed,
        ),
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "dt_promotion_gate_passed": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    return {
        "summary": summary,
        "threshold_metrics": threshold_metrics,
        "seed_metrics": seed_metrics,
        "failure_slices": failure_slices,
        "best_selected_rows": selected_rows_for_threshold,
    }


def write_hf_safe_switch_scorer_robustness_packet(
    *,
    output_dir: Path,
    packet: Mapping[str, Any],
) -> dict[str, Path]:
    """Write robustness summary and tabular evidence artifacts."""

    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / ROBUSTNESS_SUMMARY_JSON_NAME
    threshold_metrics_path = output_dir / ROBUSTNESS_THRESHOLD_METRICS_CSV_NAME
    seed_metrics_path = output_dir / SEED_METRICS_CSV_NAME
    failure_slices_path = output_dir / FAILURE_SLICES_CSV_NAME
    best_selected_rows_path = output_dir / BEST_SELECTED_ROWS_CSV_NAME

    summary_path.write_text(
        json.dumps(_jsonable(packet["summary"]), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pl.DataFrame(packet["threshold_metrics"], infer_schema_length=None).write_csv(
        threshold_metrics_path
    )
    pl.DataFrame(packet["seed_metrics"], infer_schema_length=None).write_csv(
        seed_metrics_path
    )
    _frame_or_empty(
        packet["failure_slices"],
        columns=(
            "selected_schedule_family",
            "loss_switch_count",
            "mean_positive_switch_delta_uah",
            "max_positive_switch_delta_uah",
            "market_execution_enabled",
        ),
    ).write_csv(failure_slices_path)
    _frame_or_empty(
        packet["best_selected_rows"],
        columns=(
            "seed",
            "threshold_uah",
            "selected_schedule_family",
            "selected_minus_v2_plus_regret_uah",
            "abstained_to_v2_plus",
            "market_execution_enabled",
        ),
    ).write_csv(best_selected_rows_path)
    return {
        "robustness_summary_json": summary_path,
        "robustness_threshold_metrics_csv": threshold_metrics_path,
        "seed_metrics_csv": seed_metrics_path,
        "failure_slices_csv": failure_slices_path,
        "best_selected_rows_csv": best_selected_rows_path,
    }


def _seed_metric_rows(
    *,
    seed_packets: Sequence[Mapping[str, Any]],
    baseline_regret: float,
    canonical_regret: float,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    seed_metrics: list[dict[str, Any]] = []
    selected_rows: list[dict[str, Any]] = []
    for index, packet in enumerate(seed_packets):
        seed = _seed_from_packet(packet, fallback=index)
        for result in packet["threshold_results"]:
            threshold = float(result["threshold_uah"])
            metrics = dict(result["metrics"])
            selected_mean = float(metrics["selected_mean_regret_uah"])
            seed_metrics.append(
                {
                    "seed": int(seed),
                    "threshold_uah": threshold,
                    **metrics,
                    "beats_v2_plus_baseline": selected_mean < baseline_regret,
                    "beats_canonical_safe_switch": selected_mean < canonical_regret,
                    "market_execution_enabled": False,
                }
            )
            for row in result.get("selected_rows", []):
                selected_rows.append(
                    {
                        "seed": int(seed),
                        "threshold_uah": threshold,
                        **dict(row),
                        "market_execution_enabled": False,
                    }
                )
    return seed_metrics, selected_rows


def _threshold_metric_rows(
    *,
    seed_metrics: Sequence[Mapping[str, Any]],
    selected_rows: Sequence[Mapping[str, Any]],
    baseline_regret: float,
    canonical_regret: float,
) -> list[dict[str, Any]]:
    grouped: dict[float, list[Mapping[str, Any]]] = defaultdict(list)
    for row in seed_metrics:
        grouped[float(row["threshold_uah"])].append(row)
    rows: list[dict[str, Any]] = []
    for threshold, group in sorted(grouped.items()):
        means = [float(row["selected_mean_regret_uah"]) for row in group]
        switch_losses = [float(row["switch_loss_count"]) for row in group]
        max_positive_delta = _max_positive_switch_delta(
            row for row in selected_rows if float(row["threshold_uah"]) == threshold
        )
        row = {
            "threshold_uah": float(threshold),
            "seed_count": len(group),
            "selected_mean_regret_mean": _mean(means),
            "selected_mean_regret_median": _median(means),
            "selected_mean_regret_std": _std(means),
            "selected_mean_regret_min": min(means),
            "selected_mean_regret_max": max(means),
            "mean_minus_v2_plus_baseline_uah": _mean(means) - baseline_regret,
            "median_minus_canonical_safe_switch_uah": (
                _median(means) - canonical_regret
            ),
            "seeds_beating_v2_plus_baseline_count": int(
                sum(float(value) < baseline_regret for value in means)
            ),
            "seeds_beating_canonical_count": int(
                sum(float(value) < canonical_regret for value in means)
            ),
            "all_seeds_beat_v2_plus_baseline": all(
                float(value) < baseline_regret for value in means
            ),
            "median_switch_loss_count": _median(switch_losses),
            "max_switch_loss_count": max(switch_losses),
            "max_positive_switch_delta_uah": max_positive_delta,
            "market_execution_enabled": False,
        }
        passed, reason = _robustness_gate(
            row,
            baseline_regret=baseline_regret,
            canonical_regret=canonical_regret,
        )
        row["robustness_gate_passed"] = passed
        row["robustness_gate_reason"] = reason
        rows.append(row)
    return rows


def _select_threshold(
    threshold_metrics: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    passing = [
        dict(row) for row in threshold_metrics if bool(row["robustness_gate_passed"])
    ]
    candidates = passing if passing else [dict(row) for row in threshold_metrics]
    return min(
        candidates,
        key=lambda row: (
            float(row["selected_mean_regret_mean"]),
            float(row["median_switch_loss_count"]),
            float(row["threshold_uah"]),
        ),
    )


def _robustness_gate(
    metrics: Mapping[str, Any],
    *,
    baseline_regret: float,
    canonical_regret: float,
) -> tuple[bool, str]:
    seed_count = int(metrics["seed_count"])
    median_regret = float(metrics["selected_mean_regret_median"])
    canonical_wins = int(metrics["seeds_beating_canonical_count"])
    all_beat_v2 = bool(metrics["all_seeds_beat_v2_plus_baseline"])
    median_loss = float(metrics["median_switch_loss_count"])
    max_positive_delta = float(metrics["max_positive_switch_delta_uah"])

    regret_wins = median_regret < canonical_regret and all_beat_v2
    loss_safe = (
        median_loss <= MAX_MEDIAN_SWITCH_LOSS_COUNT
        and max_positive_delta <= MAX_POSITIVE_SWITCH_DELTA_UAH
    )
    if seed_count < MIN_REQUIRED_SEED_COUNT:
        return False, "insufficient_seed_count"
    if median_regret >= canonical_regret:
        return False, "does_not_beat_canonical_safe_switch_median"
    if not all_beat_v2:
        return False, "does_not_beat_v2_plus_baseline_consistently"
    if canonical_wins < MIN_CANONICAL_WIN_COUNT:
        return False, "does_not_beat_canonical_safe_switch_consistently"
    if regret_wins and not loss_safe:
        return False, "mean_win_but_loss_switch_risk"
    if not loss_safe:
        return False, "loss_switch_risk"
    return True, "passed"


def _failure_slice_rows(
    selected_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[str, list[float]] = defaultdict(list)
    for row in selected_rows:
        if bool(row.get("abstained_to_v2_plus", False)):
            continue
        delta = float(row.get("selected_minus_v2_plus_regret_uah", 0.0))
        if delta > 0.0:
            grouped[str(row.get("selected_schedule_family", ""))].append(delta)
    return [
        {
            "selected_schedule_family": family,
            "loss_switch_count": len(values),
            "mean_positive_switch_delta_uah": _mean(values),
            "max_positive_switch_delta_uah": max(values),
            "market_execution_enabled": False,
        }
        for family, values in sorted(grouped.items())
    ]


def _bootstrap_ci(
    values: Sequence[float],
    *,
    baseline_regret: float,
    canonical_regret: float,
    iterations: int,
    seed: int,
) -> dict[str, float | int]:
    if not values:
        return {
            "iterations": int(iterations),
            "mean_regret_low": float("nan"),
            "mean_regret_high": float("nan"),
            "mean_minus_v2_plus_low": float("nan"),
            "mean_minus_v2_plus_high": float("nan"),
            "mean_minus_canonical_low": float("nan"),
            "mean_minus_canonical_high": float("nan"),
        }
    if iterations <= 0:
        mean_value = _mean(values)
        return {
            "iterations": 0,
            "mean_regret_low": mean_value,
            "mean_regret_high": mean_value,
            "mean_minus_v2_plus_low": mean_value - baseline_regret,
            "mean_minus_v2_plus_high": mean_value - baseline_regret,
            "mean_minus_canonical_low": mean_value - canonical_regret,
            "mean_minus_canonical_high": mean_value - canonical_regret,
        }
    rng = np.random.default_rng(seed)
    data = np.asarray(values, dtype=np.float64)
    draws = rng.choice(data, size=(iterations, len(data)), replace=True)
    means = draws.mean(axis=1)
    low, high = np.quantile(means, [0.025, 0.975])
    return {
        "iterations": int(iterations),
        "mean_regret_low": float(low),
        "mean_regret_high": float(high),
        "mean_minus_v2_plus_low": float(low - baseline_regret),
        "mean_minus_v2_plus_high": float(high - baseline_regret),
        "mean_minus_canonical_low": float(low - canonical_regret),
        "mean_minus_canonical_high": float(high - canonical_regret),
    }


def _max_positive_switch_delta(rows: Iterable[Mapping[str, Any]]) -> float:
    values = [
        float(row.get("selected_minus_v2_plus_regret_uah", 0.0))
        for row in rows
        if not bool(row.get("abstained_to_v2_plus", False))
        and float(row.get("selected_minus_v2_plus_regret_uah", 0.0)) > 0.0
    ]
    return max(values) if values else 0.0


def _seed_from_packet(packet: Mapping[str, Any], *, fallback: int) -> int:
    summary = packet.get("summary", {})
    if isinstance(summary, Mapping) and "seed" in summary:
        return int(summary["seed"])
    return int(fallback)


def _mean(values: Sequence[float]) -> float:
    return float(np.mean(values)) if values else 0.0


def _median(values: Sequence[float]) -> float:
    return float(np.median(values)) if values else 0.0


def _std(values: Sequence[float]) -> float:
    return float(np.std(values, ddof=0)) if values else 0.0


def _frame_or_empty(
    rows: object,
    *,
    columns: Sequence[str],
) -> pl.DataFrame:
    if isinstance(rows, Sequence) and rows:
        return pl.DataFrame(rows, infer_schema_length=None)
    return pl.DataFrame({column: [] for column in columns})


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, np.ndarray):
        return value.tolist()
    if isinstance(value, Path):
        return str(value)
    return value


__all__ = [
    "DEFAULT_ROBUSTNESS_SEEDS",
    "DEFAULT_ROBUSTNESS_THRESHOLDS_UAH",
    "ROBUSTNESS_CLAIM_SCOPE",
    "build_hf_safe_switch_scorer_robustness_packet",
    "summarize_hf_safe_switch_scorer_robustness",
    "write_hf_safe_switch_scorer_robustness_packet",
]
