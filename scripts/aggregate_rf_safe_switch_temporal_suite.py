"""Aggregate materialized RF safe-switch temporal replay summaries."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import csv
import json
from pathlib import Path
from statistics import mean, pstdev
import sys
from typing import Any, Final

REPLAY_CLAIM_SCOPE: Final[str] = (
    "rf_safe_switch_temporal_replay_retrospective_not_market_execution"
)
SUITE_CLAIM_SCOPE: Final[str] = (
    "rf_safe_switch_temporal_suite_retrospective_not_market_execution"
)
CSV_FIELDS: Final[tuple[str, ...]] = (
    "run_slug",
    "source_model_name",
    "training_window_indices",
    "evaluation_window_index",
    "min_predicted_improvement_uah",
    "seeds",
    "seed_delta_mean_uah",
    "seed_delta_std_uah",
    "seed_delta_min_uah",
    "seed_delta_max_uah",
    "seed_switch_count_min",
    "seed_switch_count_max",
    "train_candidate_row_count",
    "evaluation_candidate_row_count",
    "content_overlap_candidate_row_count",
    "content_overlap_ratio",
    "independent_holdout",
    "profile_date_row_count",
    "distinct_market_date_count",
    "selector_mean_regret_uah",
    "v2_plus_mean_regret_uah",
    "selector_minus_v2_plus_mean_regret_uah",
    "non_v2_plus_switch_count",
    "abstention_count",
    "distinct_switch_date_count",
    "observed_tail_loss_count",
    "date_win_count",
    "date_tie_count",
    "date_loss_count",
    "bootstrap_ci_low_uah",
    "bootstrap_ci_high_uah",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Aggregate time-separated RF replay summaries without treating "
            "protocol sensitivity rows as independent replications."
        )
    )
    parser.add_argument(
        "--input-summary",
        type=Path,
        action="append",
        required=True,
    )
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    args = parser.parse_args(argv)

    suite = aggregate_temporal_replay_summaries(args.input_summary)
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    args.output_json.write_text(
        json.dumps(suite, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with args.output_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(suite["rows"])
    json.dump(
        {
            "output_json": str(args.output_json),
            "output_csv": str(args.output_csv),
            "run_count": suite["run_count"],
        },
        sys.stdout,
        indent=2,
    )
    sys.stdout.write("\n")
    return 0


def aggregate_temporal_replay_summaries(
    paths: Sequence[Path],
) -> dict[str, Any]:
    """Validate and flatten materialized replay summaries."""

    if not paths:
        raise ValueError("temporal suite requires at least one input summary.")
    rows: list[dict[str, Any]] = []
    protocol_keys: set[tuple[object, ...]] = set()
    for path in paths:
        payload = _mapping(json.loads(path.read_text(encoding="utf-8")), path.name)
        if payload.get("claim_scope") != REPLAY_CLAIM_SCOPE:
            raise ValueError(f"{path} has an unsupported claim scope.")
        if payload.get("model") != "random_forest_v2_plus_safe_switch":
            raise ValueError(f"{path} is not the RF safe-switch replay.")
        if payload.get("estimator_class") != "random_forest":
            raise ValueError(f"{path} has an unexpected estimator class.")
        if _bool(payload, "promotion_gate_passed"):
            raise ValueError(f"{path} must not pass promotion.")
        if _bool(payload, "market_execution_enabled"):
            raise ValueError(f"{path} must not enable market execution.")

        config = _mapping(payload.get("selector_config"), "selector_config")
        independence = _mapping(
            payload.get("evaluation_independence"), "evaluation_independence"
        )
        evaluation = _mapping(payload.get("evaluation"), "evaluation")
        clusters = _mapping(payload.get("date_cluster_summary"), "date_cluster_summary")
        bootstrap = _mapping(
            clusters.get("moving_block_bootstrap"), "moving_block_bootstrap"
        )
        seed_sensitivity = _mapping(
            payload.get("seed_sensitivity"), "seed_sensitivity"
        )
        training_windows = _int_list(
            payload.get("training_window_indices"), "training_window_indices"
        )
        seeds = _int_list(seed_sensitivity.get("seeds"), "seeds")
        seed_rows = _mapping_list(seed_sensitivity.get("rows"), "seed_sensitivity.rows")
        if len(seed_rows) != len(seeds):
            raise ValueError("seed_sensitivity rows must match the declared seeds.")
        seed_deltas = [
            _number(row, "selector_minus_v2_plus_mean_regret_uah")
            for row in seed_rows
        ]
        seed_switch_counts = [
            _integer(row, "non_v2_plus_switch_count") for row in seed_rows
        ]
        source_model_name = _text(payload, "source_model_name")
        evaluation_window_index = _integer(
            payload, "evaluation_window_index"
        )
        threshold = _number(config, "min_predicted_improvement_uah")
        protocol_key = (
            source_model_name,
            tuple(training_windows),
            evaluation_window_index,
            threshold,
            tuple(seeds),
        )
        if protocol_key in protocol_keys:
            raise ValueError(f"duplicate temporal replay protocol: {protocol_key}")
        protocol_keys.add(protocol_key)
        rows.append(
            {
                "run_slug": _text(payload, "run_slug"),
                "source_model_name": source_model_name,
                "training_window_indices": ",".join(map(str, training_windows)),
                "evaluation_window_index": evaluation_window_index,
                "min_predicted_improvement_uah": threshold,
                "seeds": ",".join(map(str, seeds)),
                "seed_delta_mean_uah": mean(seed_deltas),
                "seed_delta_std_uah": (
                    pstdev(seed_deltas) if len(seed_deltas) > 1 else 0.0
                ),
                "seed_delta_min_uah": min(seed_deltas),
                "seed_delta_max_uah": max(seed_deltas),
                "seed_switch_count_min": min(seed_switch_counts),
                "seed_switch_count_max": max(seed_switch_counts),
                "train_candidate_row_count": _integer(
                    independence, "train_candidate_row_count"
                ),
                "evaluation_candidate_row_count": _integer(
                    independence, "evaluation_candidate_row_count"
                ),
                "content_overlap_candidate_row_count": _integer(
                    independence, "content_overlap_candidate_row_count"
                ),
                "content_overlap_ratio": _number(
                    independence, "content_overlap_ratio"
                ),
                "independent_holdout": _bool(independence, "independent_holdout"),
                "profile_date_row_count": _integer(
                    evaluation, "profile_date_row_count"
                ),
                "distinct_market_date_count": _integer(
                    evaluation, "distinct_market_date_count"
                ),
                "selector_mean_regret_uah": _number(
                    evaluation, "selector_mean_regret_uah"
                ),
                "v2_plus_mean_regret_uah": _number(
                    evaluation, "v2_plus_mean_regret_uah"
                ),
                "selector_minus_v2_plus_mean_regret_uah": _number(
                    evaluation, "selector_minus_v2_plus_mean_regret_uah"
                ),
                "non_v2_plus_switch_count": _integer(
                    evaluation, "non_v2_plus_switch_count"
                ),
                "abstention_count": _integer(evaluation, "abstention_count"),
                "distinct_switch_date_count": _integer(
                    evaluation, "distinct_switch_date_count"
                ),
                "observed_tail_loss_count": _integer(
                    evaluation, "observed_tail_loss_count"
                ),
                "date_win_count": _integer(clusters, "date_win_count"),
                "date_tie_count": _integer(clusters, "date_tie_count"),
                "date_loss_count": _integer(clusters, "date_loss_count"),
                "bootstrap_ci_low_uah": _number(bootstrap, "ci_low_uah"),
                "bootstrap_ci_high_uah": _number(bootstrap, "ci_high_uah"),
            }
        )
    rows.sort(
        key=lambda row: (
            str(row["source_model_name"]),
            int(row["evaluation_window_index"]),
            float(row["min_predicted_improvement_uah"]),
        )
    )
    return {
        "claim_scope": SUITE_CLAIM_SCOPE,
        "run_count": len(rows),
        "source_model_names": sorted({str(row["source_model_name"]) for row in rows}),
        "evaluation_window_indices": sorted(
            {int(row["evaluation_window_index"]) for row in rows}
        ),
        "thresholds_uah": sorted(
            {float(row["min_predicted_improvement_uah"]) for row in rows}
        ),
        "all_independent_holdouts": all(
            bool(row["independent_holdout"]) for row in rows
        ),
        "maximum_content_overlap_ratio": max(
            float(row["content_overlap_ratio"]) for row in rows
        ),
        "rows": rows,
        "interpretation": (
            "protocol and model-stability sensitivity over retrospective, "
            "time-separated replays; rows are not independent replications"
        ),
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _mapping(value: object, field_name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"{field_name} must be a JSON object.")
    return value


def _text(payload: Mapping[str, Any], field_name: str) -> str:
    value = payload.get(field_name)
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{field_name} must be non-empty text.")
    return value


def _number(payload: Mapping[str, Any], field_name: str) -> float:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field_name} must be numeric.")
    return float(value)


def _integer(payload: Mapping[str, Any], field_name: str) -> int:
    value = payload.get(field_name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValueError(f"{field_name} must be an integer.")
    return value


def _bool(payload: Mapping[str, Any], field_name: str) -> bool:
    value = payload.get(field_name)
    if not isinstance(value, bool):
        raise ValueError(f"{field_name} must be boolean.")
    return value


def _int_list(value: object, field_name: str) -> list[int]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty integer list.")
    if any(isinstance(item, bool) or not isinstance(item, int) for item in value):
        raise ValueError(f"{field_name} must be a non-empty integer list.")
    return value


def _mapping_list(value: object, field_name: str) -> list[Mapping[str, Any]]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{field_name} must be a non-empty object list.")
    if any(not isinstance(item, Mapping) for item in value):
        raise ValueError(f"{field_name} must be a non-empty object list.")
    return value


if __name__ == "__main__":
    raise SystemExit(main())
