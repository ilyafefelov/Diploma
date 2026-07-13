"""Temporal protocol for the full-history HF value-aligned candidate ranker."""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Final, Sequence

import polars as pl

from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import regret_aware_v2_plus_selector as regret_selector
from smart_arbitrage.dfl.schedule_value_learner_v2_plus import (
    V2_PLUS_CANDIDATE_FAMILIES,
)


_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "dt_schedule_family_target",
        "regret_delta_vs_v2_plus_uah",
        "regret_uah",
        "schedule_value_uah",
    }
)
_PANEL_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "candidate_family",
        "candidate_model_name",
        "decision_value_uah",
        "prior_family_mean_regret_uah",
        "forecast_spread_uah_mwh",
        "forecast_objective_value_uah",
        "total_degradation_penalty_uah",
        "total_throughput_mwh",
        "soc_min_slack_fraction",
    }
)
_LIBRARY_IDENTITY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "regret_uah",
    }
)
_V2_PLUS_FALLBACK_FAMILY: Final[str] = "schedule_value_learner_v2_plus"


def split_full_history_candidate_frame(
    candidate_rows: pl.DataFrame,
    *,
    test_start: datetime,
    validation_anchor_count: int = 28,
    minimum_train_anchor_count: int = 293,
) -> dict[str, object]:
    """Split candidate rows into prior train/validation and future test blocks.

    The ranker may learn from realized regret labels in train and validation, but
    its test rows are strictly later. Candidate rows remain grouped by tenant,
    source, and decision anchor so all alternatives for one decision stay in
    one split.
    """

    missing = _REQUIRED_COLUMNS.difference(candidate_rows.columns)
    if missing:
        raise ValueError(f"candidate_rows is missing columns: {sorted(missing)}")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    if minimum_train_anchor_count <= 0:
        raise ValueError("minimum_train_anchor_count must be positive.")

    anchors = sorted(
        value
        for value in candidate_rows.get_column("anchor_timestamp").unique().to_list()
        if isinstance(value, datetime) and value < test_start
    )
    if len(anchors) <= validation_anchor_count:
        raise ValueError(
            "Full-history ranker requires at least "
            f"{minimum_train_anchor_count} prior anchors plus a separate "
            "validation block."
        )
    validation_anchors = anchors[-validation_anchor_count:]
    validation_start = validation_anchors[0]
    train_rows = candidate_rows.filter(pl.col("anchor_timestamp") < validation_start)
    validation_rows = candidate_rows.filter(
        (pl.col("anchor_timestamp") >= validation_start)
        & (pl.col("anchor_timestamp") < test_start)
    )
    test_rows = candidate_rows.filter(pl.col("anchor_timestamp") >= test_start)
    if test_rows.is_empty():
        raise ValueError("Future test block is empty.")
    _require_full_history_per_tenant_source(
        train_rows,
        minimum_train_anchor_count=minimum_train_anchor_count,
    )
    return {
        "train_rows": train_rows,
        "validation_rows": validation_rows,
        "test_rows": test_rows,
        "train_anchor_count": train_rows.get_column("anchor_timestamp").n_unique(),
        "validation_anchor_count": validation_rows.get_column("anchor_timestamp").n_unique(),
        "test_anchor_count": test_rows.get_column("anchor_timestamp").n_unique(),
        "train_end": train_rows.get_column("anchor_timestamp").max().isoformat(),
        "validation_start": validation_start.isoformat(),
        "test_start": test_start.isoformat(),
        "claim_scope": "full_history_prior_only_hf_candidate_ranker_not_market_execution",
        "market_execution_enabled": False,
    }


def build_full_history_ranker_candidate_frame(
    candidate_library: pl.DataFrame,
) -> pl.DataFrame:
    """Attach a prior-only V2+ fallback and ranker labels to every candidate.

    Realized regret remains a training/evaluation label. The constructed model
    inputs are delegated to the existing scorer's prior-context feature guard.
    """

    required = _LIBRARY_IDENTITY_COLUMNS | _PANEL_REQUIRED_COLUMNS
    missing = required.difference(candidate_library.columns)
    if missing:
        raise ValueError(
            f"candidate_library is missing columns: {sorted(missing)}"
        )
    complete_library = _complete_candidate_library(candidate_library)
    rows: list[dict[str, Any]] = []
    for _, group in complete_library.group_by(
        ["tenant_id", "source_model_name"],
        maintain_order=True,
    ):
        rows.extend(_ranker_rows_for_source(list(group.iter_rows(named=True))))
    frame = pl.DataFrame(rows)
    return _assign_candidate_indices(frame)


def run_full_history_hf_candidate_ranker(
    candidate_rows: pl.DataFrame,
    *,
    test_start: datetime,
    output_dir: Path,
    validation_anchor_count: int = 28,
    minimum_train_anchor_count: int = 293,
    thresholds_uah: Sequence[float] = (0.0, 5.0, 10.0, 20.0, 50.0),
    max_epochs: int = 80,
    hidden_dim: int = 64,
    num_layers: int = 2,
    num_heads: int = 2,
    seed: int = 20260713,
) -> dict[str, object]:
    """Train on history, select threshold on validation, then score future dates."""

    split = split_full_history_candidate_frame(
        candidate_rows,
        test_start=test_start,
        validation_anchor_count=validation_anchor_count,
        minimum_train_anchor_count=minimum_train_anchor_count,
    )
    fixed_universe_v2_plus_reference = fixed_universe_v2_plus_reference_for_future_test(
        candidate_rows,
        test_start=test_start,
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    results: dict[str, object] = {}
    sources = sorted(
        str(value)
        for value in candidate_rows.get_column("source_model_name").unique().to_list()
    )
    for source_model_name in sources:
        from smart_arbitrage.dfl import hf_safe_switch_scorer as hf_scorer

        source_dir = output_dir / _safe_path_part(source_model_name)
        source_dir.mkdir(parents=True, exist_ok=True)
        train_rows = _source_rows(split["train_rows"], source_model_name)
        validation_rows = _source_rows(split["validation_rows"], source_model_name)
        test_rows = _source_rows(split["test_rows"], source_model_name)
        validation_packet = hf_scorer.build_hf_safe_switch_scorer_packet(
            pl.concat(
                [
                    train_rows.with_columns(pl.lit("train_selection").alias("split_name")),
                    validation_rows.with_columns(pl.lit("final_holdout").alias("split_name")),
                ],
                how="vertical_relaxed",
            ),
            run_slug=f"full_history_{_safe_path_part(source_model_name)}",
            thresholds_uah=thresholds_uah,
            max_epochs=max_epochs,
            hidden_dim=hidden_dim,
            num_layers=num_layers,
            num_heads=num_heads,
            seed=seed,
            output_dir=source_dir,
            save_checkpoint=True,
        )
        frozen_threshold = float(validation_packet["summary"]["best_threshold_uah"])
        test_result = _score_frozen_model_on_test(
            scorer=hf_scorer,
            model=validation_packet["model"],
            train_rows=train_rows,
            test_rows=test_rows,
            threshold_uah=frozen_threshold,
        )
        source_summary = {
            "source_model_name": source_model_name,
            "train_anchor_count": train_rows.get_column("anchor_timestamp").n_unique(),
            "validation_anchor_count": validation_rows.get_column("anchor_timestamp").n_unique(),
            "test_anchor_count": test_rows.get_column("anchor_timestamp").n_unique(),
            "threshold_selected_on": "validation",
            "frozen_threshold_uah": frozen_threshold,
            "validation_metrics": validation_packet["summary"]["best_metrics"],
            "test_metrics": test_result["metrics"],
            "fixed_universe_v2_plus_test_reference": fixed_universe_v2_plus_reference[
                source_model_name
            ],
            "ranker_training_reference": (
                "rolling_prior_only_v2_plus_fallback_per_anchor"
            ),
            "market_execution_enabled": False,
            "claim_scope": "full_history_hf_candidate_ranker_future_test_not_market_execution",
        }
        (source_dir / "test_summary.json").write_text(
            json.dumps(source_summary, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        pl.DataFrame(test_result["selected_rows"]).write_csv(
            source_dir / "test_selected_rows.csv"
        )
        results[source_model_name] = source_summary
    return {
        "split": {key: value for key, value in split.items() if not key.endswith("_rows")},
        "sources": results,
        "fixed_universe_v2_plus_test_reference": fixed_universe_v2_plus_reference,
        "market_execution_enabled": False,
    }


def _score_frozen_model_on_test(
    *,
    scorer: Any,
    model: Any,
    train_rows: pl.DataFrame,
    test_rows: pl.DataFrame,
    threshold_uah: float,
) -> dict[str, object]:
    family_names = tuple(
        sorted(str(value) for value in train_rows["dt_schedule_family_target"].unique())
    )
    feature_names = regret_selector._feature_names(  # noqa: SLF001
        family_names,
        feature_set=regret_selector.FEATURE_SET_EXPANDED,
        model_kind=regret_selector.MODEL_KIND_RANDOM_FOREST,
    )
    train_dataset = scorer._dataset_from_rows(  # noqa: SLF001
        frame=train_rows.with_columns(pl.lit("train_selection").alias("split_name")),
        split_name="train_selection",
        feature_names=feature_names,
        family_names=family_names,
        tail_risk_loss_threshold_uah=150.0,
    )
    test_dataset = scorer._dataset_from_rows(  # noqa: SLF001
        frame=test_rows.with_columns(pl.lit("final_holdout").alias("split_name")),
        split_name="final_holdout",
        feature_names=feature_names,
        family_names=family_names,
        tail_risk_loss_threshold_uah=150.0,
    )
    means, scales = scorer._feature_normalization(train_dataset["features"])  # noqa: SLF001
    predictions = scorer._predict(  # noqa: SLF001
        model=model,
        features=scorer._normalize_features(  # noqa: SLF001
            test_dataset["features"], means=means, scales=scales
        ),
        regret_scale_uah=100.0,
    )
    result = scorer._evaluate_threshold(  # noqa: SLF001
        evaluation=test_dataset,
        predictions=predictions,
        threshold_uah=threshold_uah,
        max_predicted_tail_risk_probability=0.5,
        max_family_tail_risk_probability=0.5,
        family_tail_risk=scorer._family_tail_risk(train_dataset),  # noqa: SLF001
    )
    return {"metrics": result["metrics"], "selected_rows": result["selected_rows"]}


def _source_rows(frame: object, source_model_name: str) -> pl.DataFrame:
    if not isinstance(frame, pl.DataFrame):
        raise TypeError("Temporal split did not contain a Polars DataFrame.")
    return frame.filter(pl.col("source_model_name") == source_model_name)


def _safe_path_part(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value)


def _complete_candidate_library(candidate_library: pl.DataFrame) -> pl.DataFrame:
    group_keys = ["tenant_id", "source_model_name"]
    candidate_keys = [*group_keys, "candidate_family", "candidate_model_name"]
    expected = candidate_library.group_by(group_keys).agg(
        pl.col("anchor_timestamp").n_unique().alias("expected_anchor_count")
    )
    availability = candidate_library.group_by(candidate_keys).agg(
        pl.col("anchor_timestamp").n_unique().alias("available_anchor_count")
    )
    complete = (
        availability.join(expected, on=group_keys)
        .filter(pl.col("available_anchor_count") == pl.col("expected_anchor_count"))
        .select(candidate_keys)
    )
    result = candidate_library.join(complete, on=candidate_keys)
    if result.is_empty():
        raise ValueError("No candidate model is available for every anchor.")
    return result


def fixed_universe_v2_plus_reference_for_future_test(
    candidate_rows: pl.DataFrame,
    *,
    test_start: datetime,
) -> dict[str, dict[str, float | int | str]]:
    """Return the frozen V2+ comparator inside the ranker's fixed universe.

    This deliberately differs from the ranker's rolling target reference. The
    V2+ fallback decision is frozen once from all rows strictly before the
    future test block. Its candidate universe is the ranker's complete-token
    subset, so it is a compatible internal baseline—not the released V1.2
    V2+ headline result, which used the complete candidate library.
    """

    required = _LIBRARY_IDENTITY_COLUMNS | _PANEL_REQUIRED_COLUMNS | {
        "dt_schedule_family_target"
    }
    missing = required.difference(candidate_rows.columns)
    if missing:
        raise ValueError(f"candidate_rows is missing columns: {sorted(missing)}")
    original_rows = candidate_rows.filter(
        pl.col("dt_schedule_family_target") != _V2_PLUS_FALLBACK_FAMILY
    )
    summaries: dict[str, dict[str, float | int | str]] = {}
    for source_model_name, source_frame in original_rows.group_by(
        "source_model_name", maintain_order=True
    ):
        selected_future_rows: list[dict[str, Any]] = []
        for _, tenant_frame in source_frame.group_by("tenant_id", maintain_order=True):
            rows = list(tenant_frame.iter_rows(named=True))
            history = [row for row in rows if row["anchor_timestamp"] < test_start]
            future = [row for row in rows if row["anchor_timestamp"] >= test_start]
            if not history or not future:
                raise ValueError(
                    "Fixed-universe V2+ reference requires history and future rows "
                    f"for source={source_model_name!s}."
                )
            selected_v2_history = _select_v2_rows_by_anchor(history)
            selected_plus_history = _select_plus_or_v2_rows_by_anchor(
                history, selected_v2_history
            )
            fallback_to_v2 = _fallback_to_v2(
                v2_history=selected_v2_history,
                plus_history=selected_plus_history,
            )
            selected_v2_future = _select_v2_rows_by_anchor(future)
            selected_future_rows.extend(
                selected_v2_future
                if fallback_to_v2
                else _select_plus_or_v2_rows_by_anchor(future, selected_v2_future)
            )
        source_name = str(source_model_name[0] if isinstance(source_model_name, tuple) else source_model_name)
        regrets = [float(row["regret_uah"]) for row in selected_future_rows]
        summaries[source_name] = {
            "reference_kind": "fixed_universe_frozen_v2_plus",
            "future_row_count": len(regrets),
            "future_mean_regret_uah": sum(regrets) / len(regrets),
        }
    return summaries


def _ranker_rows_for_source(source_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in source_rows:
        anchor = row["anchor_timestamp"]
        if not isinstance(anchor, datetime):
            raise TypeError("candidate_library anchor_timestamp must be datetime.")
        by_anchor.setdefault(anchor, []).append(row)
    v2_history: list[dict[str, Any]] = []
    plus_history: list[dict[str, Any]] = []
    output: list[dict[str, Any]] = []
    for anchor in sorted(by_anchor):
        candidates = by_anchor[anchor]
        v2_candidate = _v2_candidate(candidates)
        plus_candidate = _best_plus_or_v2_candidate(candidates, v2_candidate)
        fallback = (
            v2_candidate
            if _fallback_to_v2(v2_history=v2_history, plus_history=plus_history)
            else plus_candidate
        )
        fallback_regret = float(fallback["regret_uah"])
        for row in candidates:
            output.append(
                _ranker_row(
                    row,
                    regret_delta=float(row["regret_uah"]) - fallback_regret,
                    family=_candidate_token(row),
                )
            )
        output.append(
            _ranker_row(
                fallback,
                regret_delta=0.0,
                family=_V2_PLUS_FALLBACK_FAMILY,
                is_fallback=True,
            )
        )
        v2_history.append(v2_candidate)
        plus_history.append(plus_candidate)
    return output


def _select_v2_rows_by_anchor(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        by_anchor.setdefault(row["anchor_timestamp"], []).append(row)
    return [_v2_candidate(by_anchor[anchor]) for anchor in sorted(by_anchor)]


def _select_plus_or_v2_rows_by_anchor(
    rows: list[dict[str, Any]],
    v2_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    v2_by_anchor = {row["anchor_timestamp"]: row for row in v2_rows}
    by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        by_anchor.setdefault(row["anchor_timestamp"], []).append(row)
    return [
        _best_plus_or_v2_candidate(by_anchor[anchor], v2_by_anchor[anchor])
        for anchor in sorted(by_anchor)
    ]


def _v2_candidate(candidates: list[dict[str, Any]]) -> dict[str, Any]:
    base_candidates = [
        row
        for row in candidates
        if str(row["candidate_family"]) not in V2_PLUS_CANDIDATE_FAMILIES
    ]
    if not base_candidates:
        raise ValueError("Candidate block has no frozen-V2 candidate.")
    profile = v2._profile_by_name("prior_regret_value")  # noqa: SLF001
    return min(
        base_candidates,
        key=lambda row: (
            v2._score_row(row, profile=profile),  # noqa: SLF001
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ),
    )
def _best_plus_or_v2_candidate(
    candidates: list[dict[str, Any]],
    v2_candidate: dict[str, Any],
) -> dict[str, Any]:
    plus_candidates = [
        row
        for row in candidates
        if str(row["candidate_family"]) in V2_PLUS_CANDIDATE_FAMILIES
    ]
    return min(
        [v2_candidate, *plus_candidates],
        key=lambda row: (
            float(row["prior_family_mean_regret_uah"]),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ),
    )


def _fallback_to_v2(
    *,
    v2_history: list[dict[str, Any]],
    plus_history: list[dict[str, Any]],
) -> bool:
    if not v2_history:
        return True
    v2_mean = sum(float(row["regret_uah"]) for row in v2_history) / len(v2_history)
    plus_mean = sum(float(row["regret_uah"]) for row in plus_history) / len(
        plus_history
    )
    if v2_mean <= 0.0:
        return True
    return ((v2_mean - plus_mean) / v2_mean) < 0.01


def _ranker_row(
    row: dict[str, Any],
    *,
    regret_delta: float,
    family: str,
    is_fallback: bool = False,
) -> dict[str, Any]:
    result = dict(row)
    result.update(
        {
            "dt_schedule_family_target": family,
            "schedule_value_uah": float(row["decision_value_uah"]),
            "regret_delta_vs_v2_plus_uah": regret_delta,
            "market_execution_enabled": False,
            "not_full_dfl": True,
            "not_market_execution": True,
            "hf_ranker_v2_plus_fallback": is_fallback,
        }
    )
    return result


def _candidate_token(row: dict[str, Any]) -> str:
    return f"{row['candidate_family']}::{row['candidate_model_name']}"


def _assign_candidate_indices(frame: pl.DataFrame) -> pl.DataFrame:
    group_keys = ["tenant_id", "source_model_name", "anchor_timestamp"]
    return (
        frame.sort(
            [
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "dt_schedule_family_target",
                "candidate_model_name",
            ]
        )
        .with_columns(
            pl.int_range(0, pl.len()).over(group_keys)
            .alias("dt_candidate_index_target")
        )
        .with_columns(pl.len().over(group_keys).alias("teacher_anchor_candidate_count"))
        .with_columns(
            pl.concat_str(
                [
                    pl.col("tenant_id"),
                    pl.col("source_model_name"),
                    pl.col("anchor_timestamp").cast(pl.String),
                    pl.col("dt_schedule_family_target"),
                    pl.col("candidate_model_name"),
                ],
                separator="|",
            ).alias("dt_candidate_id_target")
        )
    )


def _require_full_history_per_tenant_source(
    train_rows: pl.DataFrame,
    *,
    minimum_train_anchor_count: int,
) -> None:
    counts = train_rows.group_by(["tenant_id", "source_model_name"]).agg(
        pl.col("anchor_timestamp").n_unique().alias("anchor_count")
    )
    insufficient = counts.filter(pl.col("anchor_count") < minimum_train_anchor_count)
    if not insufficient.is_empty():
        details = insufficient.sort(["tenant_id", "source_model_name"]).to_dicts()
        raise ValueError(
            "Full-history ranker requires at least "
            f"{minimum_train_anchor_count} prior anchors per tenant/source: {details}"
        )
