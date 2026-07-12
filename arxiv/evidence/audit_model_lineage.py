from __future__ import annotations

import argparse
import csv
import json
from datetime import datetime
from pathlib import Path


MIRROR_COLUMNS = (
    "candidate_family",
    "candidate_model_name",
    "forecast_price_uah_mwh_vector",
    "actual_price_uah_mwh_vector",
    "dispatch_mw_vector",
    "soc_fraction_vector",
    "decision_value_uah",
    "forecast_objective_value_uah",
    "oracle_value_uah",
    "regret_uah",
    "regret_ratio",
    "total_degradation_penalty_uah",
    "total_throughput_mwh",
    "forecast_spread_uah_mwh",
    "actual_spread_uah_mwh",
    "forecast_top_k_actual_overlap",
    "forecast_bottom_k_actual_overlap",
    "soc_min_slack_fraction",
    "safety_violation_count",
    "label_regret_delta_vs_v2_plus_uah",
    "label_beats_v2_plus",
    "label_safe_switch_win",
    "label_tail_risk_loss",
    "teacher_candidate_index",
    "teacher_anchor_candidate_count",
    "teacher_schedule_candidate_class",
    "teacher_target_family",
    "teacher_return_to_go_delta_uah",
    "teacher_tail_risk_penalty_uah",
    "teacher_tail_risk_probability_target",
    "teacher_loss_weight",
    "sequence_position",
    "dt_return_to_go_uah",
    "dt_tail_risk_target",
    "dt_candidate_index_target",
    "dt_candidate_family_target",
    "dt_schedule_family_target",
    "return_to_go_regret_target_uah",
    "regret_delta_vs_v2_plus_uah",
    "schedule_value_uah",
    "best_available_candidate_family",
    "best_available_candidate_model_name",
    "best_available_candidate_regret_uah",
    "best_available_regret_gap_vs_v2_plus_uah",
)


def _load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _shift_year(timestamp: str, year: int) -> str:
    return datetime.fromisoformat(timestamp).replace(year=year).isoformat(timespec="microseconds")


def audit(lineage_dir: Path) -> dict[str, object]:
    with (lineage_dir / "rf_safe_switch_teacher_rows.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        teacher_rows = list(csv.DictReader(handle))

    training_rows = [row for row in teacher_rows if row["split_name"] == "train_selection"]
    final_rows = [row for row in teacher_rows if row["split_name"] == "final_holdout"]
    training_by_key = {
        (
            row["tenant_id"],
            row["source_model_name"],
            row["anchor_timestamp"],
            row["candidate_family"],
            row["sequence_position"],
        ): row
        for row in training_rows
    }

    mismatched_columns: set[str] = set()
    paired_count = 0
    for final_row in final_rows:
        train_timestamp = _shift_year(final_row["anchor_timestamp"], 2025)
        key = (
            final_row["tenant_id"],
            final_row["source_model_name"],
            train_timestamp,
            final_row["candidate_family"],
            final_row["sequence_position"],
        )
        train_row = training_by_key.get(key)
        if train_row is None:
            raise ValueError(f"Missing timestamp-shifted training pair: {key}")
        paired_count += 1
        for column in MIRROR_COLUMNS:
            if train_row[column] != final_row[column]:
                mismatched_columns.add(column)

    with (lineage_dir / "rf_safe_switch_selected_rows.csv").open(
        newline="", encoding="utf-8"
    ) as handle:
        selected_rows = list(csv.DictReader(handle))
    switch_rows = [row for row in selected_rows if row["abstained_to_v2_plus"].lower() == "false"]
    switch_dates = sorted(
        {datetime.fromisoformat(row["anchor_timestamp"]).date().isoformat() for row in switch_rows}
    )

    rf_summary = _load_json(lineage_dir / "rf_safe_switch_summary.json")
    temporal_summary = _load_json(
        lineage_dir / "rf_safe_switch_temporal_replay_summary.json"
    )
    temporal_suite = _load_json(
        lineage_dir / "rf_safe_switch_temporal_suite_summary.json"
    )
    hf_robustness = _load_json(lineage_dir / "hf_dt_backbone_robustness_summary.json")
    hf_read_model = _load_json(lineage_dir / "hf_32_day_read_model_audit.json")

    evaluation = rf_summary["evaluation"]
    temporal_independence = temporal_summary["evaluation_independence"]
    temporal_evaluation = temporal_summary["evaluation"]
    temporal_suite_rows = temporal_suite["rows"]
    temporal_suite_deltas = [
        float(row["selector_minus_v2_plus_mean_regret_uah"])
        for row in temporal_suite_rows
    ]
    canonical_comparison = hf_robustness["canonical_comparison"]
    return {
        "canonical_artifact_identifier": "dt_v2_plus",
        "canonical_estimator_class": rf_summary["model_kind"],
        "canonical_estimator_loss": rf_summary["loss_function"],
        "exact_mirror": {
            "training_candidate_row_count": len(training_rows),
            "evaluation_candidate_row_count": len(final_rows),
            "paired_candidate_row_count": paired_count,
            "timestamp_shift_years": -1,
            "compared_model_input_and_target_columns": list(MIRROR_COLUMNS),
            "mismatched_columns": sorted(mismatched_columns),
            "all_model_inputs_and_targets_equal": not mismatched_columns,
        },
        "rf_selection": {
            "mean_regret_uah": evaluation["selector_mean_regret_uah"],
            "switch_count": evaluation["non_v2_plus_switch_count"],
            "abstention_count": evaluation["abstention_count"],
            "distinct_switch_dates": switch_dates,
            "distinct_switch_date_count": len(switch_dates),
            "independent_holdout": False,
            "interpretation": "exact-mirror in-packet pipeline diagnostic",
        },
        "temporal_replay": {
            "training_candidate_row_count": temporal_independence[
                "train_candidate_row_count"
            ],
            "evaluation_candidate_row_count": temporal_independence[
                "evaluation_candidate_row_count"
            ],
            "content_overlap_candidate_row_count": temporal_independence[
                "content_overlap_candidate_row_count"
            ],
            "independent_holdout": temporal_independence["independent_holdout"],
            "profile_date_row_count": temporal_evaluation["profile_date_row_count"],
            "distinct_market_date_count": temporal_evaluation[
                "distinct_market_date_count"
            ],
            "switch_count": temporal_evaluation["non_v2_plus_switch_count"],
            "abstention_count": temporal_evaluation["abstention_count"],
            "selector_minus_v2_plus_mean_regret_uah": temporal_evaluation[
                "selector_minus_v2_plus_mean_regret_uah"
            ],
            "promotion_gate_passed": temporal_summary["promotion_gate_passed"],
            "market_execution_enabled": temporal_summary[
                "market_execution_enabled"
            ],
            "interpretation": temporal_summary["interpretation"],
        },
        "temporal_suite": {
            "run_count": temporal_suite["run_count"],
            "source_model_count": len(temporal_suite["source_model_names"]),
            "evaluation_window_indices": temporal_suite[
                "evaluation_window_indices"
            ],
            "thresholds_uah": temporal_suite["thresholds_uah"],
            "all_independent_holdouts": temporal_suite[
                "all_independent_holdouts"
            ],
            "maximum_content_overlap_ratio": temporal_suite[
                "maximum_content_overlap_ratio"
            ],
            "beneficial_protocol_count": sum(
                delta < 0.0 for delta in temporal_suite_deltas
            ),
            "tie_protocol_count": sum(delta == 0.0 for delta in temporal_suite_deltas),
            "harmful_protocol_count": sum(
                delta > 0.0 for delta in temporal_suite_deltas
            ),
            "maximum_primary_seed_harm_uah": max(temporal_suite_deltas),
            "promotion_gate_passed": temporal_suite["promotion_gate_passed"],
            "market_execution_enabled": temporal_suite[
                "market_execution_enabled"
            ],
            "interpretation": temporal_suite["interpretation"],
        },
        "hf_diagnostics": {
            "architecture": "Hugging Face DecisionTransformerModel backbone candidate scorer",
            "frozen_mean_regret_uah": canonical_comparison["mean_hf_mean_regret_uah"],
            "frozen_packet_independent_holdout": False,
            "read_model_audit_day_count": int(hf_read_model["source_backed_day_count"]),
            "read_model_nonfallback_day_count": int(
                hf_read_model["selected_nonfallback_day_count"]
            ),
            "read_model_mean_selected_value_uah": hf_read_model["mean_selected_value_uah"],
            "read_model_audit_has_realized_regret": False,
        },
        "claim_boundary": (
            "The 168.1566 UAH artifact is random forest, not Decision Transformer; "
            "RF and HF frozen values are non-independent diagnostics, not OOS estimates. "
            "The post-defense temporal replay removes candidate-content overlap but "
            "the wider temporal suite finds no beneficial protocol and three harmful "
            "protocols, so it does not support promotion."
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Audit RF/DT/HF evidence lineage.")
    parser.add_argument("--lineage-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result = audit(args.lineage_dir)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
