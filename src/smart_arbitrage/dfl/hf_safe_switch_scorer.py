"""HF-backed candidate regret/tail-risk scorer for frozen safe-switch rows.

This is a research-shadow scorer, not a V13 training or market-execution path.
It uses a Hugging Face DecisionTransformerModel as a sequence encoder over
candidate rows, then scores regret delta versus V2+ and tail risk per candidate.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Final

import numpy as np
import polars as pl
import torch
from torch import nn
from transformers import DecisionTransformerConfig, DecisionTransformerModel

from smart_arbitrage.dfl import regret_aware_v2_plus_selector as regret_selector

CLAIM_SCOPE: Final[str] = (
    "hf_safe_switch_candidate_scorer_shadow_not_promotable_not_market_execution"
)
SUMMARY_JSON_NAME: Final[str] = "hf_safe_switch_scorer_summary.json"
THRESHOLD_METRICS_CSV_NAME: Final[str] = "hf_safe_switch_scorer_threshold_metrics.csv"
SELECTED_ROWS_CSV_PREFIX: Final[str] = "hf_safe_switch_scorer_selected_rows"
CHECKPOINT_DIR_NAME: Final[str] = "hf_safe_switch_scorer_model_checkpoint"
CHECKPOINT_METADATA_JSON_NAME: Final[str] = "checkpoint_metadata.json"
V2_PLUS_FAMILY: Final[str] = "schedule_value_learner_v2_plus"


@dataclass(frozen=True, slots=True)
class HfSafeSwitchInferenceBundle:
    model: DecisionTransformerModel
    metadata: Mapping[str, Any]
    feature_names: tuple[str, ...]
    candidate_families: tuple[str, ...]
    feature_means: np.ndarray
    feature_scales: np.ndarray
    threshold_uah: float
    max_predicted_tail_risk_probability: float
    max_family_tail_risk_probability: float
    family_tail_risk: Mapping[str, float]
    regret_scale_uah: float


def build_hf_safe_switch_scorer_packet(
    teacher_rows_frame: pl.DataFrame,
    *,
    run_slug: str,
    thresholds_uah: Sequence[float] = (0.0, 5.0, 10.0, 20.0, 50.0),
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
    max_family_tail_risk_probability: float = 0.5,
    tail_risk_loss_threshold_uah: float = 150.0,
    seed: int = 20260525,
    output_dir: Path | None = None,
    canonical_aggregate: Mapping[str, Any] | None = None,
    save_checkpoint: bool = False,
) -> dict[str, Any]:
    """Train/evaluate a research-only HF candidate scorer with V2+ fallback."""

    if not run_slug:
        raise ValueError("run_slug must be non-empty.")
    if max_epochs <= 0:
        raise ValueError("max_epochs must be positive.")
    if regret_scale_uah <= 0.0:
        raise ValueError("regret_scale_uah must be positive.")
    if not thresholds_uah:
        raise ValueError("thresholds_uah must not be empty.")
    _refuse_market_execution(teacher_rows_frame)

    frame = teacher_rows_frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
    ).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "dt_candidate_index_target",
        ]
    )
    families = tuple(sorted(str(v) for v in frame["dt_schedule_family_target"].unique()))
    feature_names = regret_selector._feature_names(  # noqa: SLF001
        families,
        feature_set=regret_selector.FEATURE_SET_EXPANDED,
        model_kind=regret_selector.MODEL_KIND_RANDOM_FOREST,
    )
    train = _dataset_from_rows(
        frame=frame,
        split_name="train_selection",
        feature_names=feature_names,
        family_names=families,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
    )
    evaluation = _dataset_from_rows(
        frame=frame,
        split_name="final_holdout",
        feature_names=feature_names,
        family_names=families,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
    )
    means, scales = _feature_normalization(train["features"])
    train_features = _normalize_features(train["features"], means=means, scales=scales)
    eval_features = _normalize_features(evaluation["features"], means=means, scales=scales)

    torch.manual_seed(seed)
    np.random.seed(seed)
    model = DecisionTransformerModel(
        DecisionTransformerConfig(
            state_dim=int(train_features.shape[-1]),
            act_dim=2,
            hidden_size=hidden_dim,
            max_ep_len=int(train_features.shape[1]),
            n_layer=num_layers,
            n_head=num_heads,
            action_tanh=False,
        )
    )
    training_summary = _train_model(
        model=model,
        train_features=train_features,
        train_delta=train["regret_delta"],
        train_tail_risk=train["tail_risk"],
        regret_scale_uah=regret_scale_uah,
        max_epochs=max_epochs,
        learning_rate=learning_rate,
        weight_decay=weight_decay,
        safe_switch_extra_weight=safe_switch_extra_weight,
        pairwise_margin_scaled=pairwise_margin_scaled,
    )
    train_predictions = _predict(
        model=model,
        features=train_features,
        regret_scale_uah=regret_scale_uah,
    )
    eval_predictions = _predict(
        model=model,
        features=eval_features,
        regret_scale_uah=regret_scale_uah,
    )
    family_tail_risk = _family_tail_risk(train)
    threshold_results = [
        _evaluate_threshold(
            evaluation=evaluation,
            predictions=eval_predictions,
            threshold_uah=float(threshold),
            max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
            max_family_tail_risk_probability=max_family_tail_risk_probability,
            family_tail_risk=family_tail_risk,
        )
        for threshold in thresholds_uah
    ]
    best_result = min(
        threshold_results,
        key=lambda result: (
            float(result["metrics"]["selected_mean_regret_uah"]),
            float(result["metrics"]["switch_loss_count"]),
        ),
    )
    canonical = dict(canonical_aggregate or {})
    summary: dict[str, Any] = {
        "claim_scope": CLAIM_SCOPE,
        "run_slug": run_slug,
        "generated_at": datetime.now(UTC).isoformat(),
        "model_backbone": "huggingface_decision_transformer_model",
        "method": (
            "DecisionTransformerModel encoder with candidate-level regret-delta "
            "regression, tail-risk BCE, pairwise safe-switch ranking, and V2+ fallback."
        ),
        "feature_set": regret_selector.FEATURE_SET_EXPANDED,
        "feature_count": len(feature_names),
        "feature_names": feature_names,
        "candidate_families": list(families),
        "dataset_summary": {
            "teacher_row_count": frame.height,
            "train_anchor_count": len(train["keys"]),
            "evaluation_anchor_count": len(evaluation["keys"]),
            "candidate_count": int(train["features"].shape[1]),
            "research_shadow_training_rows": int(
                frame.filter(pl.col("split_name") == "train_selection").height
            ),
            "promotable_v13_permitted_training_rows": int(
                frame.filter(pl.col("permits_model_training").eq(True)).height
                if "permits_model_training" in frame.columns
                else 0
            ),
        },
        "training": training_summary,
        "selection_config": {
            "thresholds_uah": [float(v) for v in thresholds_uah],
            "max_predicted_tail_risk_probability": float(
                max_predicted_tail_risk_probability
            ),
            "max_family_tail_risk_probability": float(
                max_family_tail_risk_probability
            ),
            "tail_risk_loss_threshold_uah": float(tail_risk_loss_threshold_uah),
            "regret_scale_uah": float(regret_scale_uah),
            "safe_switch_extra_weight": float(safe_switch_extra_weight),
            "pairwise_margin_scaled": float(pairwise_margin_scaled),
        },
        "family_tail_risk": family_tail_risk,
        "threshold_results": [
            {k: v for k, v in result.items() if k != "selected_rows"}
            for result in threshold_results
        ],
        "best_threshold_uah": float(best_result["threshold_uah"]),
        "best_metrics": dict(best_result["metrics"]),
        "canonical_comparison": _canonical_comparison(
            best_metrics=best_result["metrics"],
            canonical=canonical,
        ),
        "train_prediction_diagnostics": _prediction_diagnostics(
            train,
            train_predictions,
        ),
        "eval_prediction_diagnostics": _prediction_diagnostics(
            evaluation,
            eval_predictions,
        ),
        "checkpoint": {
            "saved": False,
            "path": "",
            "format": "huggingface_save_pretrained",
            "load_smoke_passed": False,
            "market_execution_enabled": False,
        },
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "dt_promotion_gate_passed": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }
    if output_dir is not None and save_checkpoint:
        summary["checkpoint"] = _write_checkpoint(
            output_dir=output_dir,
            model=model,
            state_dim=int(train_features.shape[-1]),
            candidate_count=int(train_features.shape[1]),
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            num_heads=num_heads,
            feature_names=feature_names,
            candidate_families=families,
            feature_means=means,
            feature_scales=scales,
            selected_threshold_uah=float(best_result["threshold_uah"]),
            regret_scale_uah=regret_scale_uah,
            max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
            max_family_tail_risk_probability=max_family_tail_risk_probability,
            family_tail_risk=family_tail_risk,
        )
    return {
        "summary": summary,
        "threshold_results": threshold_results,
        "best_selected_rows": best_result["selected_rows"],
        "model": model,
    }


def write_hf_safe_switch_scorer_packet(
    *,
    output_dir: Path,
    packet: Mapping[str, Any],
) -> dict[str, Path]:
    """Persist summary and selected-row threshold artifacts."""

    output_dir.mkdir(parents=True, exist_ok=True)
    summary = dict(packet["summary"])
    threshold_results = list(packet["threshold_results"])
    summary_path = output_dir / SUMMARY_JSON_NAME
    threshold_csv_path = output_dir / THRESHOLD_METRICS_CSV_NAME
    summary_path.write_text(
        json.dumps(_jsonable(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    threshold_rows = [
        {
            "threshold_uah": result["threshold_uah"],
            **dict(result["metrics"]),
            "market_execution_enabled": False,
        }
        for result in threshold_results
    ]
    pl.DataFrame(threshold_rows).write_csv(threshold_csv_path)
    selected_paths: dict[str, Path] = {}
    for result in threshold_results:
        threshold_label = _threshold_label(float(result["threshold_uah"]))
        selected_path = output_dir / f"{SELECTED_ROWS_CSV_PREFIX}_{threshold_label}.csv"
        pl.DataFrame(result["selected_rows"], infer_schema_length=None).write_csv(
            selected_path
        )
        selected_paths[threshold_label] = selected_path
    return {
        "summary_json": summary_path,
        "threshold_metrics_csv": threshold_csv_path,
        **{f"selected_rows_{key}": path for key, path in selected_paths.items()},
    }


def _dataset_from_rows(
    *,
    frame: pl.DataFrame,
    split_name: str,
    feature_names: Sequence[str],
    family_names: Sequence[str],
    tail_risk_loss_threshold_uah: float,
) -> dict[str, Any]:
    split = frame.filter(pl.col("split_name") == split_name)
    if split.is_empty():
        raise ValueError(f"HF safe-switch scorer requires {split_name} rows.")
    groups: dict[tuple[str, str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in split.iter_rows(named=True):
        groups[
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                str(row["anchor_timestamp"]),
            )
        ].append(row)
    keys: list[tuple[str, str, str]] = []
    features: list[list[list[float]]] = []
    regret_delta: list[list[float]] = []
    tail_risk: list[list[float]] = []
    regret: list[list[float]] = []
    value: list[list[float]] = []
    safety: list[list[int]] = []
    families: list[list[str]] = []
    candidate_ids: list[list[str]] = []
    candidate_indices: list[list[int]] = []
    for key, rows in sorted(groups.items()):
        ordered = sorted(rows, key=lambda row: int(row["dt_candidate_index_target"]))
        if len(ordered) != len(family_names):
            raise ValueError(
                "HF safe-switch scorer requires a complete candidate family block "
                f"for {key}; expected {len(family_names)}, got {len(ordered)}."
            )
        keys.append(key)
        features.append(
            [
                regret_selector._feature_values(  # noqa: SLF001
                    row,
                    feature_names=feature_names,
                    family_names=family_names,
                )
                for row in ordered
            ]
        )
        regret_delta.append(
            [float(row["regret_delta_vs_v2_plus_uah"]) for row in ordered]
        )
        tail_risk.append(
            [
                float(
                    bool(row.get("label_tail_risk_loss", False))
                    or float(row["regret_delta_vs_v2_plus_uah"])
                    >= tail_risk_loss_threshold_uah
                )
                for row in ordered
            ]
        )
        regret.append([float(row["regret_uah"]) for row in ordered])
        value.append([float(row["schedule_value_uah"]) for row in ordered])
        safety.append([int(row.get("safety_violation_count") or 0) for row in ordered])
        families.append([str(row["dt_schedule_family_target"]) for row in ordered])
        candidate_ids.append([str(row["dt_candidate_id_target"]) for row in ordered])
        candidate_indices.append(
            [int(row["dt_candidate_index_target"]) for row in ordered]
        )
    return {
        "keys": keys,
        "features": np.asarray(features, dtype=np.float32),
        "regret_delta": np.asarray(regret_delta, dtype=np.float32),
        "tail_risk": np.asarray(tail_risk, dtype=np.float32),
        "regret": np.asarray(regret, dtype=np.float32),
        "value": np.asarray(value, dtype=np.float32),
        "safety": np.asarray(safety, dtype=np.int64),
        "families": np.asarray(families, dtype=object),
        "candidate_ids": np.asarray(candidate_ids, dtype=object),
        "candidate_indices": np.asarray(candidate_indices, dtype=np.int64),
    }


def _train_model(
    *,
    model: DecisionTransformerModel,
    train_features: np.ndarray,
    train_delta: np.ndarray,
    train_tail_risk: np.ndarray,
    regret_scale_uah: float,
    max_epochs: int,
    learning_rate: float,
    weight_decay: float,
    safe_switch_extra_weight: float,
    pairwise_margin_scaled: float,
) -> dict[str, float | int]:
    optimizer = torch.optim.AdamW(
        model.parameters(),
        lr=learning_rate,
        weight_decay=weight_decay,
    )
    x = torch.tensor(train_features, dtype=torch.float32)
    target_delta = torch.tensor(train_delta / regret_scale_uah, dtype=torch.float32)
    target_tail = torch.tensor(train_tail_risk, dtype=torch.float32)
    sample_weights = 1.0 + torch.abs(torch.tensor(train_delta, dtype=torch.float32)) / 50.0
    sample_weights = sample_weights + (
        torch.tensor(train_delta, dtype=torch.float32) < -1.0
    ).to(torch.float32) * safe_switch_extra_weight
    inputs = _hf_inputs(x)
    first_loss = 0.0
    last_loss = 0.0
    first_mse = 0.0
    last_mse = 0.0
    first_rank = 0.0
    last_rank = 0.0
    model.train()
    for epoch in range(max_epochs):
        optimizer.zero_grad()
        output = model(states=x, **inputs, return_dict=True).action_preds
        predicted_delta = output[..., 0]
        predicted_tail_logit = output[..., 1]
        mse = ((predicted_delta - target_delta) ** 2 * sample_weights).mean()
        bce = nn.functional.binary_cross_entropy_with_logits(
            predicted_tail_logit,
            target_tail,
            pos_weight=torch.tensor(5.0, dtype=torch.float32),
        )
        ranking = _pairwise_safe_switch_ranking_loss(
            predicted_delta=predicted_delta,
            train_delta=torch.tensor(train_delta, dtype=torch.float32),
            margin_scaled=pairwise_margin_scaled,
        )
        loss = mse + 0.1 * bce + ranking
        if epoch == 0:
            first_loss = float(loss.detach().item())
            first_mse = float(mse.detach().item())
            first_rank = float(ranking.detach().item())
        last_loss = float(loss.detach().item())
        last_mse = float(mse.detach().item())
        last_rank = float(ranking.detach().item())
        loss.backward()
        optimizer.step()
    return {
        "max_epochs": int(max_epochs),
        "train_loss_first": first_loss,
        "train_loss_last": last_loss,
        "train_weighted_mse_first": first_mse,
        "train_weighted_mse_last": last_mse,
        "train_pairwise_ranking_loss_first": first_rank,
        "train_pairwise_ranking_loss_last": last_rank,
    }


def _predict(
    *,
    model: DecisionTransformerModel,
    features: np.ndarray,
    regret_scale_uah: float,
) -> dict[str, np.ndarray]:
    x = torch.tensor(features, dtype=torch.float32)
    model.eval()
    with torch.no_grad():
        output = model(states=x, **_hf_inputs(x), return_dict=True).action_preds
        predicted_delta = output[..., 0].detach().cpu().numpy() * regret_scale_uah
        predicted_tail_probability = (
            torch.sigmoid(output[..., 1]).detach().cpu().numpy()
        )
    return {
        "predicted_regret_delta_uah": predicted_delta,
        "predicted_tail_risk_probability": predicted_tail_probability,
    }


def _evaluate_threshold(
    *,
    evaluation: Mapping[str, Any],
    predictions: Mapping[str, np.ndarray],
    threshold_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float,
    family_tail_risk: Mapping[str, float],
) -> dict[str, Any]:
    predicted_delta = predictions["predicted_regret_delta_uah"]
    predicted_tail = predictions["predicted_tail_risk_probability"]
    selected_rows: list[dict[str, Any]] = []
    for row_index, key in enumerate(evaluation["keys"]):
        families = evaluation["families"][row_index]
        v2_positions = np.flatnonzero(families == V2_PLUS_FAMILY)
        v2_position = int(v2_positions[0]) if len(v2_positions) else 0
        selected_position = v2_position
        candidate_positions: list[int] = []
        for candidate_position in range(len(families)):
            if candidate_position == v2_position:
                continue
            family = str(families[candidate_position])
            family_tail = float(family_tail_risk.get(family, 0.0))
            if (
                predicted_delta[row_index, candidate_position] < -threshold_uah
                and predicted_tail[row_index, candidate_position]
                <= max_predicted_tail_risk_probability
                and family_tail <= max_family_tail_risk_probability
                and int(evaluation["safety"][row_index, candidate_position]) == 0
            ):
                candidate_positions.append(candidate_position)
        if candidate_positions:
            selected_position = min(
                candidate_positions,
                key=lambda position: float(predicted_delta[row_index, position]),
            )
        selected_rows.append(
            _selected_row(
                key=key,
                evaluation=evaluation,
                predicted_delta=predicted_delta,
                predicted_tail=predicted_tail,
                row_index=row_index,
                selected_position=selected_position,
                v2_position=v2_position,
                threshold_uah=threshold_uah,
            )
        )
    metrics = _selection_metrics(selected_rows)
    return {
        "threshold_uah": float(threshold_uah),
        "metrics": metrics,
        "selected_rows": selected_rows,
    }


def _selected_row(
    *,
    key: tuple[str, str, str],
    evaluation: Mapping[str, Any],
    predicted_delta: np.ndarray,
    predicted_tail: np.ndarray,
    row_index: int,
    selected_position: int,
    v2_position: int,
    threshold_uah: float,
) -> dict[str, Any]:
    selected_regret = float(evaluation["regret"][row_index, selected_position])
    v2_regret = float(evaluation["regret"][row_index, v2_position])
    selected_value = float(evaluation["value"][row_index, selected_position])
    v2_value = float(evaluation["value"][row_index, v2_position])
    selected_family = str(evaluation["families"][row_index, selected_position])
    return {
        "tenant_id": key[0],
        "source_model_name": key[1],
        "anchor_timestamp": key[2],
        "threshold_uah": float(threshold_uah),
        "selected_candidate_id": str(
            evaluation["candidate_ids"][row_index, selected_position]
        ),
        "selected_candidate_index": int(
            evaluation["candidate_indices"][row_index, selected_position]
        ),
        "selected_schedule_family": selected_family,
        "selected_regret_uah": selected_regret,
        "selected_value_uah": selected_value,
        "v2_plus_candidate_id": str(evaluation["candidate_ids"][row_index, v2_position]),
        "v2_plus_regret_uah": v2_regret,
        "v2_plus_value_uah": v2_value,
        "selected_minus_v2_plus_regret_uah": selected_regret - v2_regret,
        "selected_minus_v2_plus_value_uah": selected_value - v2_value,
        "predicted_regret_delta_vs_v2_plus_uah": float(
            predicted_delta[row_index, selected_position]
        ),
        "predicted_tail_risk_probability": float(
            predicted_tail[row_index, selected_position]
        ),
        "abstained_to_v2_plus": selected_position == v2_position,
        "research_shadow_not_promotable": True,
        "dt_lava_ready": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }


def _selection_metrics(selected_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    regrets = [float(row["selected_regret_uah"]) for row in selected_rows]
    values = [float(row["selected_value_uah"]) for row in selected_rows]
    v2_regrets = [float(row["v2_plus_regret_uah"]) for row in selected_rows]
    deltas = [
        float(row["selected_minus_v2_plus_regret_uah"]) for row in selected_rows
    ]
    switch_deltas = [
        delta
        for row, delta in zip(selected_rows, deltas, strict=True)
        if not bool(row["abstained_to_v2_plus"])
    ]
    return {
        "selected_mean_regret_uah": _mean(regrets),
        "selected_median_regret_uah": _median(regrets),
        "selected_mean_value_uah": _mean(values),
        "v2_plus_mean_regret_uah": _mean(v2_regrets),
        "selected_minus_v2_plus_mean_regret_uah": _mean(deltas),
        "non_v2_plus_switch_count": int(len(switch_deltas)),
        "abstention_count": int(len(selected_rows) - len(switch_deltas)),
        "switch_win_count": int(sum(delta < 0.0 for delta in switch_deltas)),
        "switch_loss_count": int(sum(delta > 0.0 for delta in switch_deltas)),
        "switch_tie_count": int(sum(delta == 0.0 for delta in switch_deltas)),
        "switch_mean_regret_delta_uah": _mean(switch_deltas),
        "market_execution_enabled": False,
    }


def _feature_normalization(features: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    flat = features.reshape(-1, features.shape[-1])
    means = flat.mean(axis=0)
    scales = flat.std(axis=0)
    scales = np.where(scales == 0.0, 1.0, scales)
    return means, scales


def _normalize_features(
    features: np.ndarray,
    *,
    means: np.ndarray,
    scales: np.ndarray,
) -> np.ndarray:
    return ((features - means) / scales).astype(np.float32)


def _hf_inputs(features: torch.Tensor) -> dict[str, torch.Tensor]:
    batch_size, context_length = features.shape[:2]
    return {
        "actions": torch.zeros((batch_size, context_length, 2), dtype=features.dtype),
        "rewards": torch.zeros((batch_size, context_length, 1), dtype=features.dtype),
        "returns_to_go": torch.zeros(
            (batch_size, context_length, 1),
            dtype=features.dtype,
        ),
        "timesteps": torch.arange(context_length, dtype=torch.long).view(1, -1).expand(
            batch_size,
            -1,
        ),
        "attention_mask": torch.ones((batch_size, context_length), dtype=torch.long),
    }


def _pairwise_safe_switch_ranking_loss(
    *,
    predicted_delta: torch.Tensor,
    train_delta: torch.Tensor,
    margin_scaled: float,
) -> torch.Tensor:
    losses: list[torch.Tensor] = []
    for row_index in range(predicted_delta.shape[0]):
        v2_position = 1
        winning_positions = torch.nonzero(
            train_delta[row_index] < -1.0,
            as_tuple=False,
        ).reshape(-1)
        for position in winning_positions:
            losses.append(
                torch.relu(
                    predicted_delta[row_index, position]
                    - predicted_delta[row_index, v2_position]
                    + margin_scaled
                )
            )
    if not losses:
        return torch.zeros((), dtype=predicted_delta.dtype)
    return torch.stack(losses).mean()


def _family_tail_risk(dataset: Mapping[str, Any]) -> dict[str, float]:
    families = dataset["families"]
    tail = dataset["tail_risk"]
    counts: dict[str, int] = {}
    positives: dict[str, int] = {}
    for row_index in range(families.shape[0]):
        for candidate_index in range(families.shape[1]):
            family = str(families[row_index, candidate_index])
            counts[family] = counts.get(family, 0) + 1
            if bool(tail[row_index, candidate_index]):
                positives[family] = positives.get(family, 0) + 1
    return {
        family: float(positives.get(family, 0) / count)
        for family, count in counts.items()
        if count > 0
    }


def _prediction_diagnostics(
    dataset: Mapping[str, Any],
    predictions: Mapping[str, np.ndarray],
) -> dict[str, float]:
    predicted = predictions["predicted_regret_delta_uah"].reshape(-1)
    target = dataset["regret_delta"].reshape(-1)
    return {
        "mean_predicted_regret_delta_uah": _mean(predicted.tolist()),
        "mean_actual_regret_delta_uah": _mean(target.tolist()),
        "rmse_regret_delta_uah": float(np.sqrt(np.mean((predicted - target) ** 2))),
    }


def _canonical_comparison(
    *,
    best_metrics: Mapping[str, Any],
    canonical: Mapping[str, Any],
) -> dict[str, Any]:
    baseline = float(canonical.get("baseline_mean_regret", float("nan")))
    canonical_safe_switch = float(canonical.get("mean_test_regret", float("nan")))
    selected = float(best_metrics["selected_mean_regret_uah"])
    return {
        "v2_plus_baseline_mean_regret_uah": baseline,
        "canonical_safe_switch_mean_regret_uah": canonical_safe_switch,
        "hf_safe_switch_selected_mean_regret_uah": selected,
        "hf_minus_v2_plus_baseline_mean_regret_uah": selected - baseline,
        "hf_minus_canonical_safe_switch_mean_regret_uah": (
            selected - canonical_safe_switch
        ),
        "hf_beats_v2_plus_baseline": selected < baseline,
        "hf_beats_canonical_safe_switch": selected < canonical_safe_switch,
        "canonical_pass_level": str(canonical.get("pass_level", "")),
        "market_execution_enabled": False,
    }


def _write_checkpoint(
    *,
    output_dir: Path,
    model: DecisionTransformerModel,
    state_dim: int,
    candidate_count: int,
    hidden_dim: int,
    num_layers: int,
    num_heads: int,
    feature_names: Sequence[str],
    candidate_families: Sequence[str],
    feature_means: np.ndarray,
    feature_scales: np.ndarray,
    selected_threshold_uah: float,
    regret_scale_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float,
    family_tail_risk: Mapping[str, float],
) -> dict[str, Any]:
    checkpoint_dir = output_dir / CHECKPOINT_DIR_NAME
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    model.save_pretrained(checkpoint_dir)
    loaded = DecisionTransformerModel.from_pretrained(checkpoint_dir)
    load_smoke_passed = int(loaded.config.state_dim) == state_dim
    metadata = {
        "saved": True,
        "path": str(checkpoint_dir),
        "format": "huggingface_save_pretrained",
        "state_dim": int(state_dim),
        "candidate_count": int(candidate_count),
        "head_dim": 2,
        "hidden_dim": int(hidden_dim),
        "num_layers": int(num_layers),
        "num_heads": int(num_heads),
        "load_smoke_passed": bool(load_smoke_passed),
        "feature_names": list(feature_names),
        "candidate_families": list(candidate_families),
        "feature_means": [float(value) for value in feature_means.tolist()],
        "feature_scales": [float(value) for value in feature_scales.tolist()],
        "selected_threshold_uah": float(selected_threshold_uah),
        "regret_scale_uah": float(regret_scale_uah),
        "max_predicted_tail_risk_probability": float(
            max_predicted_tail_risk_probability
        ),
        "max_family_tail_risk_probability": float(max_family_tail_risk_probability),
        "family_tail_risk": {
            str(family): float(probability)
            for family, probability in family_tail_risk.items()
        },
        "claim_scope": CLAIM_SCOPE,
        "dt_promotion_gate_passed": False,
        "promotion_gate_passed": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "not_market_execution": True,
    }
    (checkpoint_dir / CHECKPOINT_METADATA_JSON_NAME).write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return metadata


def load_hf_safe_switch_inference_bundle(
    checkpoint_dir: Path,
) -> HfSafeSwitchInferenceBundle:
    """Load a non-executable HF scorer checkpoint with inference metadata."""

    metadata_path = checkpoint_dir / CHECKPOINT_METADATA_JSON_NAME
    if not metadata_path.exists():
        raise FileNotFoundError(f"HF safe-switch checkpoint metadata not found: {metadata_path}")
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        raise ValueError("HF safe-switch checkpoint metadata must be a JSON object.")
    _refuse_executable_checkpoint_metadata(metadata)
    feature_names = _string_tuple(metadata.get("feature_names"), field_name="feature_names")
    candidate_families = _string_tuple(
        metadata.get("candidate_families"),
        field_name="candidate_families",
    )
    feature_means = _float_array(metadata.get("feature_means"), field_name="feature_means")
    feature_scales = _float_array(metadata.get("feature_scales"), field_name="feature_scales")
    if feature_means.shape != feature_scales.shape:
        raise ValueError("feature_means and feature_scales must have matching shapes.")
    if feature_means.shape[0] != len(feature_names):
        raise ValueError("feature normalization length must match feature_names.")
    model = _load_materialized_decision_transformer_model(checkpoint_dir)
    return HfSafeSwitchInferenceBundle(
        model=model,
        metadata=metadata,
        feature_names=feature_names,
        candidate_families=candidate_families,
        feature_means=feature_means,
        feature_scales=feature_scales,
        threshold_uah=float(metadata.get("selected_threshold_uah", 0.0)),
        max_predicted_tail_risk_probability=float(
            metadata.get("max_predicted_tail_risk_probability", 0.5)
        ),
        max_family_tail_risk_probability=float(
            metadata.get("max_family_tail_risk_probability", 0.5)
        ),
        family_tail_risk={
            str(family): float(probability)
            for family, probability in dict(metadata.get("family_tail_risk", {})).items()
        },
        regret_scale_uah=float(metadata.get("regret_scale_uah", 100.0)),
    )


def _load_materialized_decision_transformer_model(
    checkpoint_dir: Path,
) -> DecisionTransformerModel:
    config = DecisionTransformerConfig.from_pretrained(checkpoint_dir)
    with torch.device("cpu"):
        model = DecisionTransformerModel(config)
    state_dict_path = checkpoint_dir / "model.safetensors"
    if state_dict_path.exists():
        from safetensors.torch import load_file as load_safetensors_file

        state_dict = load_safetensors_file(str(state_dict_path), device="cpu")
    else:
        state_dict_path = checkpoint_dir / "pytorch_model.bin"
        if not state_dict_path.exists():
            raise FileNotFoundError(
                f"HF safe-switch checkpoint weights not found in {checkpoint_dir}."
            )
        state_dict = torch.load(
            state_dict_path,
            map_location="cpu",
            weights_only=True,
        )
    missing_keys, unexpected_keys = model.load_state_dict(state_dict, strict=False)
    if missing_keys or unexpected_keys:
        raise RuntimeError(
            "HF safe-switch checkpoint state dict mismatch: "
            f"missing={missing_keys}, unexpected={unexpected_keys}."
        )
    if _has_meta_tensors(model):
        raise RuntimeError(
            "HF safe-switch checkpoint still contains meta tensors after explicit "
            "CPU state-dict materialization."
        )
    model.to(torch.device("cpu"))  # type: ignore[arg-type]
    model.eval()
    return model


def _has_meta_tensors(model: DecisionTransformerModel) -> bool:
    return any(parameter.is_meta for parameter in model.parameters()) or any(
        buffer.is_meta for buffer in model.buffers()
    )


def score_hf_safe_switch_candidate_rows(
    *,
    bundle: HfSafeSwitchInferenceBundle,
    candidate_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Score one live candidate block and select with the safe-switch guard."""

    if not candidate_rows:
        raise ValueError("candidate_rows must not be empty.")
    ordered_rows = sorted(
        (dict(row) for row in candidate_rows),
        key=lambda row: int(row["dt_candidate_index_target"]),
    )
    if len(ordered_rows) != len(bundle.candidate_families):
        raise ValueError(
            "HF safe-switch live scoring requires one complete candidate family block."
        )
    features = np.asarray(
        [
            regret_selector._feature_values(  # noqa: SLF001
                row,
                feature_names=bundle.feature_names,
                family_names=bundle.candidate_families,
            )
            for row in ordered_rows
        ],
        dtype=np.float32,
    ).reshape(1, len(ordered_rows), len(bundle.feature_names))
    normalized = _normalize_features(
        features,
        means=bundle.feature_means,
        scales=bundle.feature_scales,
    )
    predictions = _predict(
        model=bundle.model,
        features=normalized,
        regret_scale_uah=bundle.regret_scale_uah,
    )
    scored_candidates: list[dict[str, Any]] = []
    for index, row in enumerate(ordered_rows):
        family = str(row["dt_schedule_family_target"])
        scored_candidates.append(
            {
                **row,
                "predicted_regret_delta_vs_v2_plus_uah": float(
                    predictions["predicted_regret_delta_uah"][0, index]
                ),
                "predicted_tail_risk_probability": float(
                    predictions["predicted_tail_risk_probability"][0, index]
                ),
                "family_tail_risk_probability": float(
                    bundle.family_tail_risk.get(family, 0.0)
                ),
                "market_execution_enabled": False,
                "promotion_gate_passed": False,
                "dt_lava_ready": False,
                "not_market_execution": True,
            }
        )
    selection = select_hf_safe_switch_candidate(
        scored_candidates,
        threshold_uah=bundle.threshold_uah,
        max_predicted_tail_risk_probability=(
            bundle.max_predicted_tail_risk_probability
        ),
        max_family_tail_risk_probability=bundle.max_family_tail_risk_probability,
    )
    selected = dict(selection["selected_candidate"])
    selected_value = selected.get("schedule_value_uah")
    if selected_value is None:
        selected_value = selected.get("decision_value_uah", 0.0)
    diagnostics = summarize_hf_safe_switch_guard(
        scored_candidates,
        selected_candidate=selected,
        threshold_uah=bundle.threshold_uah,
        max_predicted_tail_risk_probability=(
            bundle.max_predicted_tail_risk_probability
        ),
        max_family_tail_risk_probability=bundle.max_family_tail_risk_probability,
    )
    return {
        "selected_candidate": selected,
        "selected_candidate_id": str(selected["dt_candidate_id_target"]),
        "selected_schedule_family": str(selected["dt_schedule_family_target"]),
        "selected_candidate_index": int(selected["dt_candidate_index_target"]),
        "selected_schedule_value_uah": float(selected_value),
        "predicted_regret_delta_vs_v2_plus_uah": float(
            diagnostics["reported_selected_predicted_regret_delta_vs_v2_plus_uah"]
        ),
        "raw_selected_predicted_regret_delta_vs_v2_plus_uah": float(
            selected["predicted_regret_delta_vs_v2_plus_uah"]
        ),
        "predicted_tail_risk_probability": float(
            selected["predicted_tail_risk_probability"]
        ),
        "abstained_to_v2_plus": bool(selection["abstained_to_v2_plus"]),
        "selection_reason": str(selection["selection_reason"]),
        "live_actual_regret_available": False,
        "scored_candidates": scored_candidates,
        "selection_diagnostics": diagnostics,
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
    }


def summarize_hf_safe_switch_guard(
    scored_candidates: Sequence[Mapping[str, Any]],
    *,
    selected_candidate: Mapping[str, Any],
    threshold_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float = 1.0,
    fallback_family: str = V2_PLUS_FAMILY,
) -> dict[str, Any]:
    """Summarize why live HF candidates passed or failed the safe-switch guard."""

    if not scored_candidates:
        raise ValueError("scored_candidates must not be empty.")
    threshold = float(threshold_uah)
    selected = dict(selected_candidate)
    selected_family = str(selected["dt_schedule_family_target"])
    selected_value = _candidate_schedule_value(selected)
    reported_delta = (
        0.0
        if selected_family == fallback_family
        else float(selected["predicted_regret_delta_vs_v2_plus_uah"])
    )
    nonfallback: list[dict[str, Any]] = [
        dict(candidate)
        for candidate in scored_candidates
        if str(candidate["dt_schedule_family_target"]) != fallback_family
    ]
    threshold_failed = 0
    predicted_tail_failed = 0
    family_tail_failed = 0
    safety_failed = 0
    eligible: list[dict[str, Any]] = []
    safe_nonfallback: list[dict[str, Any]] = []
    for candidate in nonfallback:
        predicted_delta = float(candidate["predicted_regret_delta_vs_v2_plus_uah"])
        predicted_tail = float(candidate["predicted_tail_risk_probability"])
        family_tail = float(candidate.get("family_tail_risk_probability", 0.0))
        safety_violations = int(candidate.get("safety_violation_count", 0) or 0)
        threshold_guard_failed = predicted_delta >= -threshold
        predicted_tail_guard_failed = (
            predicted_tail > max_predicted_tail_risk_probability
        )
        family_tail_guard_failed = family_tail > max_family_tail_risk_probability
        safety_guard_failed = safety_violations > 0
        threshold_failed += int(threshold_guard_failed)
        predicted_tail_failed += int(predicted_tail_guard_failed)
        family_tail_failed += int(family_tail_guard_failed)
        safety_failed += int(safety_guard_failed)
        if (
            not predicted_tail_guard_failed
            and not family_tail_guard_failed
            and not safety_guard_failed
        ):
            safe_nonfallback.append(candidate)
        if (
            not threshold_guard_failed
            and not predicted_tail_guard_failed
            and not family_tail_guard_failed
            and not safety_guard_failed
        ):
            eligible.append(candidate)
    best_nonfallback = _min_by_predicted_delta(nonfallback)
    best_safe_nonfallback = _min_by_predicted_delta(safe_nonfallback)
    best_value_candidate = (
        max(nonfallback, key=_candidate_schedule_value) if nonfallback else None
    )
    best_value = (
        _candidate_schedule_value(best_value_candidate)
        if best_value_candidate is not None
        else selected_value
    )
    return {
        "reported_selected_predicted_regret_delta_vs_v2_plus_uah": reported_delta,
        "raw_selected_predicted_regret_delta_vs_v2_plus_uah": float(
            selected["predicted_regret_delta_vs_v2_plus_uah"]
        ),
        "best_nonfallback_schedule_family": _candidate_family(best_nonfallback),
        "best_nonfallback_predicted_regret_delta_vs_v2_plus_uah": _candidate_float(
            best_nonfallback,
            "predicted_regret_delta_vs_v2_plus_uah",
        ),
        "best_nonfallback_predicted_tail_risk_probability": _candidate_float(
            best_nonfallback,
            "predicted_tail_risk_probability",
        ),
        "best_nonfallback_family_tail_risk_probability": _candidate_float(
            best_nonfallback,
            "family_tail_risk_probability",
        ),
        "best_nonfallback_threshold_margin_to_switch_uah": (
            _threshold_margin(best_nonfallback, threshold)
        ),
        "best_safe_nonfallback_schedule_family": _candidate_family(
            best_safe_nonfallback
        ),
        "best_safe_nonfallback_predicted_regret_delta_vs_v2_plus_uah": (
            _candidate_float(
                best_safe_nonfallback,
                "predicted_regret_delta_vs_v2_plus_uah",
            )
        ),
        "best_safe_nonfallback_predicted_tail_risk_probability": _candidate_float(
            best_safe_nonfallback,
            "predicted_tail_risk_probability",
        ),
        "best_safe_nonfallback_threshold_margin_to_switch_uah": _threshold_margin(
            best_safe_nonfallback,
            threshold,
        ),
        "best_value_schedule_family": _candidate_family(best_value_candidate),
        "best_template_schedule_value_uah": float(best_value),
        "selected_vs_best_template_value_gap_uah": float(
            max(0.0, best_value - selected_value)
        ),
        "eligible_nonfallback_candidate_count": float(len(eligible)),
        "threshold_guard_failed_count": float(threshold_failed),
        "predicted_tail_guard_failed_count": float(predicted_tail_failed),
        "family_tail_guard_failed_count": float(family_tail_failed),
        "safety_guard_failed_count": float(safety_failed),
    }


def select_hf_safe_switch_candidate(
    scored_candidates: Sequence[Mapping[str, Any]],
    *,
    threshold_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float = 1.0,
    fallback_family: str = V2_PLUS_FAMILY,
) -> dict[str, Any]:
    """Select a guarded non-fallback candidate or abstain to V2+ fallback."""

    if not scored_candidates:
        raise ValueError("scored_candidates must not be empty.")
    fallback = next(
        (
            dict(candidate)
            for candidate in scored_candidates
            if str(candidate["dt_schedule_family_target"]) == fallback_family
        ),
        dict(scored_candidates[0]),
    )
    eligible: list[dict[str, Any]] = []
    for candidate in scored_candidates:
        row = dict(candidate)
        family = str(row["dt_schedule_family_target"])
        if family == fallback_family:
            continue
        predicted_delta = float(row["predicted_regret_delta_vs_v2_plus_uah"])
        predicted_tail = float(row["predicted_tail_risk_probability"])
        family_tail = float(row.get("family_tail_risk_probability", 0.0))
        safety_violations = int(row.get("safety_violation_count", 0) or 0)
        if (
            predicted_delta < -float(threshold_uah)
            and predicted_tail <= max_predicted_tail_risk_probability
            and family_tail <= max_family_tail_risk_probability
            and safety_violations == 0
        ):
            eligible.append(row)
    if not eligible:
        return {
            "selected_candidate": fallback,
            "selected_schedule_family": str(fallback["dt_schedule_family_target"]),
            "abstained_to_v2_plus": True,
            "selection_reason": "guard_abstained_to_safe_fallback",
        }
    selected = min(
        eligible,
        key=lambda row: float(row["predicted_regret_delta_vs_v2_plus_uah"]),
    )
    return {
        "selected_candidate": selected,
        "selected_schedule_family": str(selected["dt_schedule_family_target"]),
        "abstained_to_v2_plus": False,
        "selection_reason": "predicted_guard_passed",
    }


def _candidate_schedule_value(candidate: Mapping[str, Any] | None) -> float:
    if candidate is None:
        return 0.0
    value = candidate.get("schedule_value_uah")
    if value is None:
        value = candidate.get("decision_value_uah", 0.0)
    return float(value)


def _min_by_predicted_delta(candidates: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    if not candidates:
        return None
    return dict(
        min(
            candidates,
            key=lambda row: float(row["predicted_regret_delta_vs_v2_plus_uah"]),
        )
    )


def _candidate_family(candidate: Mapping[str, Any] | None) -> str:
    if candidate is None:
        return ""
    return str(candidate.get("dt_schedule_family_target", ""))


def _candidate_float(candidate: Mapping[str, Any] | None, key: str) -> float:
    if candidate is None:
        return 0.0
    return float(candidate.get(key, 0.0))


def _threshold_margin(candidate: Mapping[str, Any] | None, threshold_uah: float) -> float:
    if candidate is None:
        return 0.0
    predicted_delta = float(candidate["predicted_regret_delta_vs_v2_plus_uah"])
    return float(max(0.0, float(threshold_uah) + predicted_delta))


def _refuse_executable_checkpoint_metadata(metadata: Mapping[str, Any]) -> None:
    for flag_name in (
        "market_execution_enabled",
        "promotion_gate_passed",
        "dt_promotion_gate_passed",
        "permits_model_training",
    ):
        if metadata.get(flag_name) is True:
            raise ValueError(f"HF safe-switch inference bundle refuses true {flag_name}.")
    if metadata.get("not_market_execution") is False:
        raise ValueError("HF safe-switch inference bundle requires not_market_execution=true.")


def _string_tuple(value: Any, *, field_name: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list.")
    return tuple(str(item) for item in value)


def _float_array(value: Any, *, field_name: str) -> np.ndarray:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty list.")
    return np.asarray([float(item) for item in value], dtype=np.float32)


def _refuse_market_execution(frame: pl.DataFrame) -> None:
    for column in (
        "market_execution_enabled",
        "promotion_gate_passed",
        "market_execution_gate_passed",
        "raw_hourly_action_imitation",
    ):
        if column in frame.columns and bool(frame.select(pl.col(column).any()).item()):
            raise ValueError(f"HF safe-switch scorer refuses true {column} rows.")


def _mean(values: Sequence[float]) -> float:
    return float(np.mean(values)) if values else 0.0


def _median(values: Sequence[float]) -> float:
    return float(np.median(values)) if values else 0.0


def _threshold_label(value: float) -> str:
    return f"threshold_{value:g}".replace(".", "p")


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
    "CHECKPOINT_DIR_NAME",
    "CLAIM_SCOPE",
    "SUMMARY_JSON_NAME",
    "THRESHOLD_METRICS_CSV_NAME",
    "build_hf_safe_switch_scorer_packet",
    "load_hf_safe_switch_inference_bundle",
    "score_hf_safe_switch_candidate_rows",
    "select_hf_safe_switch_candidate",
    "summarize_hf_safe_switch_guard",
    "write_hf_safe_switch_scorer_packet",
]
