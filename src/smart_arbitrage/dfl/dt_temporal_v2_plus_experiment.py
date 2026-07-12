"""Time-separated Decision Transformer comparison against frozen V2+."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import UTC, datetime
import json
from pathlib import Path
from statistics import mean
from typing import Any, Final

import numpy as np
import polars as pl

from smart_arbitrage.dfl.dt_research_shadow import (
    OBJECTIVE_KIND_CROSS_ENTROPY,
    OBJECTIVE_KIND_DECISION_AWARE,
    audit_dt_research_shadow_temporal_independence,
    build_dt_research_shadow_sequence_packet,
    build_dt_research_shadow_teacher_rows_from_temporal_v2_plus_strict_rows,
    run_dt_research_shadow_smoke,
    write_dt_research_shadow_sequence_packet,
)

DT_TEMPORAL_V2_PLUS_EXPERIMENT_CLAIM_SCOPE: Final[str] = (
    "dt_temporal_v2_plus_research_shadow_not_full_dfl_not_market_execution"
)
DEFAULT_PROTOCOLS: Final[dict[int, tuple[int, ...]]] = {
    1: (4, 3, 2),
    2: (4, 3),
    3: (4,),
}
SUPPORTED_OBJECTIVES: Final[tuple[str, ...]] = (
    OBJECTIVE_KIND_CROSS_ENTROPY,
    OBJECTIVE_KIND_DECISION_AWARE,
)
SUMMARY_JSON_NAME: Final[str] = "dt_temporal_v2_plus_experiment_summary.json"
ROWS_CSV_NAME: Final[str] = "dt_temporal_v2_plus_experiment_rows.csv"


def run_dt_temporal_v2_plus_experiment(
    rolling_strict_rows_frame: pl.DataFrame,
    *,
    output_dir: Path,
    run_slug: str,
    source_model_names: tuple[str, ...],
    evaluation_window_indices: tuple[int, ...] = (1, 2, 3),
    objective_kinds: tuple[str, ...] = SUPPORTED_OBJECTIVES,
    seeds: tuple[int, ...] = (42, 2026, 7),
    context_length: int = 4,
    max_epochs: int = 20,
    hidden_dim: int = 64,
    num_layers: int = 2,
    num_heads: int = 2,
    learning_rate: float = 0.001,
    min_predicted_improvement_uah: float = 20.0,
    max_family_tail_risk_probability: float = 0.5,
    tail_risk_loss_threshold_uah: float = 150.0,
    model_backbone: str = "hf",
) -> dict[str, Any]:
    """Train BC and decision-aware DT shadows on genuinely earlier windows."""

    _validate_config(
        run_slug=run_slug,
        source_model_names=source_model_names,
        evaluation_window_indices=evaluation_window_indices,
        objective_kinds=objective_kinds,
        seeds=seeds,
        max_epochs=max_epochs,
        learning_rate=learning_rate,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    result_rows: list[dict[str, Any]] = []
    promotable_rows: list[int] = []
    for source_model_name in source_model_names:
        source_rows = rolling_strict_rows_frame.filter(
            pl.col("source_model_name") == source_model_name
        )
        if source_rows.is_empty():
            raise ValueError(f"No rolling strict rows for source: {source_model_name}")
        for evaluation_window_index in evaluation_window_indices:
            training_window_indices = DEFAULT_PROTOCOLS[evaluation_window_index]
            training_rows = source_rows.filter(
                pl.col("evaluation_window_index").is_in(training_window_indices)
            )
            evaluation_rows = source_rows.filter(
                pl.col("evaluation_window_index") == evaluation_window_index
            )
            teacher_rows = (
                build_dt_research_shadow_teacher_rows_from_temporal_v2_plus_strict_rows(
                    training_strict_rows_frame=training_rows,
                    evaluation_strict_rows_frame=evaluation_rows,
                )
            )
            independence = audit_dt_research_shadow_temporal_independence(teacher_rows)
            if not bool(independence["independent_holdout"]):
                raise ValueError("DT temporal experiment requires zero content overlap.")
            protocol_slug = (
                f"{_path_token(source_model_name)}_eval_{evaluation_window_index}"
            )
            protocol_dir = output_dir / "protocols" / protocol_slug
            sequence_packet = build_dt_research_shadow_sequence_packet(
                teacher_rows_frame=teacher_rows,
                run_slug=f"{run_slug}_{protocol_slug}",
                context_length=context_length,
            )
            sequence_paths = write_dt_research_shadow_sequence_packet(
                output_dir=protocol_dir,
                packet=sequence_packet,
                teacher_rows_frame=teacher_rows,
            )
            tensor_counts = _tensor_contract_counts(sequence_paths["sequence_npz"])
            for objective_kind in objective_kinds:
                for seed in seeds:
                    run_dir = protocol_dir / objective_kind / f"seed_{seed}"
                    paths = run_dt_research_shadow_smoke(
                        sequence_npz_path=sequence_paths["sequence_npz"],
                        output_dir=run_dir,
                        max_epochs=max_epochs,
                        hidden_dim=hidden_dim,
                        num_layers=num_layers,
                        num_heads=num_heads,
                        learning_rate=learning_rate,
                        seed=seed,
                        model_backbone=model_backbone,
                        objective_kind=objective_kind,
                        min_predicted_improvement_uah=(
                            min_predicted_improvement_uah
                        ),
                        max_family_tail_risk_probability=(
                            max_family_tail_risk_probability
                        ),
                    )
                    smoke = _read_json(paths["summary_json"])
                    preview = _read_json(paths["selected_preview_json"])
                    metrics = dict(smoke["evaluation_metrics"])
                    preview_rows = list(preview["preview_rows"])
                    delta = float(
                        metrics["dt_selected_mean_regret_uah"]
                        - metrics["v2_plus_mean_regret_uah"]
                    )
                    promotable = int(
                        smoke["promotable_v13_permitted_training_rows"]
                    )
                    promotable_rows.append(promotable)
                    result_rows.append(
                        {
                            "source_model_name": source_model_name,
                            "evaluation_window_index": evaluation_window_index,
                            "training_window_indices": ",".join(
                                str(value) for value in training_window_indices
                            ),
                            "objective_kind": objective_kind,
                            "seed": seed,
                            "model_backbone": str(smoke["model_backbone"]),
                            "max_epochs": max_epochs,
                            "learning_rate": learning_rate,
                            "train_sequence_count": int(
                                smoke["train_sequence_count"]
                            ),
                            "evaluation_sequence_count": int(
                                smoke["evaluation_sequence_count"]
                            ),
                            **tensor_counts,
                            "content_overlap_candidate_row_count": int(
                                independence["content_overlap_candidate_row_count"]
                            ),
                            "independent_holdout": bool(
                                independence["independent_holdout"]
                            ),
                            "dt_selected_mean_regret_uah": float(
                                metrics["dt_selected_mean_regret_uah"]
                            ),
                            "dt_selected_median_regret_uah": float(
                                metrics["dt_selected_median_regret_uah"]
                            ),
                            "v2_plus_mean_regret_uah": float(
                                metrics["v2_plus_mean_regret_uah"]
                            ),
                            "v2_plus_median_regret_uah": float(
                                metrics["v2_plus_median_regret_uah"]
                            ),
                            "strict_mean_regret_uah": float(
                                metrics["strict_mean_regret_uah"]
                            ),
                            "dt_minus_v2_plus_mean_regret_uah": delta,
                            "non_v2_plus_switch_count": int(
                                metrics["non_v2_plus_switch_count"]
                            ),
                            "abstention_count": int(metrics["abstention_count"]),
                            "switch_win_count": int(metrics["switch_win_count"]),
                            "switch_loss_count": int(metrics["switch_loss_count"]),
                            "observed_tail_loss_count": sum(
                                float(row["regret_vs_v2_plus_uah"])
                                >= tail_risk_loss_threshold_uah
                                for row in preview_rows
                                if not bool(row["abstained_to_v2_plus"])
                            ),
                            "eval_cross_entropy_loss": float(
                                metrics["eval_cross_entropy_loss"]
                            ),
                            "eval_decision_aware_ranking_loss": float(
                                metrics["eval_decision_aware_ranking_loss"]
                            ),
                            "promotable_v13_permitted_training_rows": promotable,
                            "promotion_gate_passed": False,
                            "market_execution_enabled": False,
                            "run_directory": str(run_dir),
                        }
                    )
    summary = _summary(
        run_slug=run_slug,
        rows=result_rows,
        promotable_rows=promotable_rows,
        source_model_names=source_model_names,
        evaluation_window_indices=evaluation_window_indices,
        objective_kinds=objective_kinds,
        seeds=seeds,
        context_length=context_length,
        max_epochs=max_epochs,
        hidden_dim=hidden_dim,
        num_layers=num_layers,
        num_heads=num_heads,
        learning_rate=learning_rate,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
    )
    (output_dir / SUMMARY_JSON_NAME).write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    pl.DataFrame(result_rows).write_csv(output_dir / ROWS_CSV_NAME)
    return {"summary": summary, "rows": result_rows}


def _tensor_contract_counts(path: Path) -> dict[str, int]:
    npz = np.load(path, allow_pickle=True)
    actions = np.asarray(npz["actions"])
    returns_to_go = np.asarray(npz["returns_to_go"])
    return {
        "action_target_count": int(np.count_nonzero(actions >= 0)),
        "nonzero_action_target_count": int(np.count_nonzero(actions > 0)),
        "returns_to_go_nonzero_count": int(np.count_nonzero(returns_to_go)),
        "regret_reward_target_nonzero_count": int(np.count_nonzero(returns_to_go)),
    }


def _summary(
    *,
    run_slug: str,
    rows: Sequence[dict[str, Any]],
    promotable_rows: Sequence[int],
    source_model_names: tuple[str, ...],
    evaluation_window_indices: tuple[int, ...],
    objective_kinds: tuple[str, ...],
    seeds: tuple[int, ...],
    context_length: int,
    max_epochs: int,
    hidden_dim: int,
    num_layers: int,
    num_heads: int,
    learning_rate: float,
    min_predicted_improvement_uah: float,
    max_family_tail_risk_probability: float,
) -> dict[str, Any]:
    deltas = [float(row["dt_minus_v2_plus_mean_regret_uah"]) for row in rows]
    objective_summary: dict[str, dict[str, float | int]] = {}
    for objective in objective_kinds:
        objective_rows = [row for row in rows if row["objective_kind"] == objective]
        objective_deltas = [
            float(row["dt_minus_v2_plus_mean_regret_uah"])
            for row in objective_rows
        ]
        objective_summary[objective] = {
            "run_count": len(objective_rows),
            "mean_dt_minus_v2_plus_regret_uah": mean(objective_deltas),
            "beneficial_run_count": sum(value < 0.0 for value in objective_deltas),
            "tie_run_count": sum(value == 0.0 for value in objective_deltas),
            "harmful_run_count": sum(value > 0.0 for value in objective_deltas),
        }
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "claim_scope": DT_TEMPORAL_V2_PLUS_EXPERIMENT_CLAIM_SCOPE,
        "protocol_run_count": len(rows),
        "source_model_names": list(source_model_names),
        "evaluation_window_indices": list(evaluation_window_indices),
        "objective_kinds": list(objective_kinds),
        "seeds": list(seeds),
        "model_config": {
            "context_length": context_length,
            "max_epochs": max_epochs,
            "hidden_dim": hidden_dim,
            "num_layers": num_layers,
            "num_heads": num_heads,
            "learning_rate": learning_rate,
            "min_predicted_improvement_uah": min_predicted_improvement_uah,
            "max_family_tail_risk_probability": (
                max_family_tail_risk_probability
            ),
        },
        "all_protocols_independent": all(
            bool(row["independent_holdout"]) for row in rows
        ),
        "all_runs_have_nonzero_return_conditioning": all(
            int(row["returns_to_go_nonzero_count"]) > 0 for row in rows
        ),
        "all_runs_have_action_targets": all(
            int(row["action_target_count"]) > 0 for row in rows
        ),
        "beneficial_run_count": sum(value < 0.0 for value in deltas),
        "tie_run_count": sum(value == 0.0 for value in deltas),
        "harmful_run_count": sum(value > 0.0 for value in deltas),
        "objective_summary": objective_summary,
        "promotable_v13_permitted_training_rows": max(promotable_rows, default=0),
        "full_differentiable_dfl": False,
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
        "interpretation": (
            "time-separated return-conditioned DT research shadow; decision-aware "
            "objective is DFL-style candidate ranking, not a differentiable "
            "forecast-storage-market-clearing stack"
        ),
    }


def _validate_config(
    *,
    run_slug: str,
    source_model_names: tuple[str, ...],
    evaluation_window_indices: tuple[int, ...],
    objective_kinds: tuple[str, ...],
    seeds: tuple[int, ...],
    max_epochs: int,
    learning_rate: float,
) -> None:
    if not run_slug.strip():
        raise ValueError("run_slug must not be empty.")
    if not source_model_names:
        raise ValueError("source_model_names must not be empty.")
    if not evaluation_window_indices or any(
        value not in DEFAULT_PROTOCOLS for value in evaluation_window_indices
    ):
        raise ValueError("evaluation_window_indices must use protocols 1, 2, or 3.")
    if not objective_kinds or any(
        value not in SUPPORTED_OBJECTIVES for value in objective_kinds
    ):
        raise ValueError("objective_kinds contain an unsupported objective.")
    if not seeds or len(seeds) != len(set(seeds)):
        raise ValueError("seeds must be non-empty and unique.")
    if max_epochs <= 0:
        raise ValueError("max_epochs must be positive.")
    if learning_rate <= 0.0:
        raise ValueError("learning_rate must be positive.")


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TypeError(f"{path} must contain a JSON object.")
    return value


def _path_token(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value)


__all__ = [
    "DT_TEMPORAL_V2_PLUS_EXPERIMENT_CLAIM_SCOPE",
    "run_dt_temporal_v2_plus_experiment",
]
