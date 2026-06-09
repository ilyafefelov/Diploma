"""Materialize a formal value-aligned HF live shadow promotion proof packet."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_AUDIT_DIR = (
    PROJECT_ROOT
    / "data"
    / "research_runs"
    / "hf_live_safe_switch_value_aligned_audit_2026_05_01_2026_06_01"
)
DEFAULT_ROBUSTNESS_SUMMARY_JSON = (
    PROJECT_ROOT
    / "data"
    / "research_runs"
    / "week5_hf_safe_switch_scorer_robustness_2026_06_01"
    / "robustness_summary.json"
)
DEFAULT_CANONICAL_AGGREGATE_JSON = PROJECT_ROOT / "runs" / "dt_v2_plus" / "aggregate.json"
DEFAULT_OUTPUT_DIR = (
    PROJECT_ROOT
    / "data"
    / "research_runs"
    / "hf_live_safe_switch_value_aligned_shadow_promotion_proof_2026_05_01_2026_06_01"
)
DEFAULT_RUN_SLUG = "hf_live_safe_switch_value_aligned_shadow_promotion_proof_2026_05_01_2026_06_01"

DEFAULT_CANDIDATE_GRID_ID = "candidate_library_value_aligned"
DEFAULT_BASELINE_GRID_ID = "default"
DEFAULT_THRESHOLD_UAH = 100.0
DEFAULT_MIN_SOURCE_BACKED_DAYS = 20.0
DEFAULT_MIN_SWITCH_RATE = 0.60
DEFAULT_MAX_VALUE_GAP_RATIO = 0.50
DEFAULT_MIN_SELECTED_VALUE_IMPROVEMENT_UAH = 0.0
DEFAULT_MAX_TAIL_FAILURE_DELTA_COUNT = 0.0
DEFAULT_MAX_SAFETY_FAILURE_COUNT = 0.0
FALLBACK_FAMILY = "schedule_value_learner_v2_plus"


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Build a formal multi-day proof packet for the value-aligned HF live "
            "safe-switch shadow source. This gate is scoped to shadow candidate "
            "library promotion only; it does not enable market execution."
        )
    )
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_AUDIT_DIR)
    parser.add_argument(
        "--robustness-summary-json",
        type=Path,
        default=DEFAULT_ROBUSTNESS_SUMMARY_JSON,
    )
    parser.add_argument(
        "--canonical-aggregate-json",
        type=Path,
        default=DEFAULT_CANONICAL_AGGREGATE_JSON,
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    parser.add_argument("--min-source-backed-days", type=float, default=DEFAULT_MIN_SOURCE_BACKED_DAYS)
    parser.add_argument("--min-switch-rate", type=float, default=DEFAULT_MIN_SWITCH_RATE)
    parser.add_argument("--max-value-gap-ratio", type=float, default=DEFAULT_MAX_VALUE_GAP_RATIO)
    parser.add_argument(
        "--min-selected-value-improvement-uah",
        type=float,
        default=DEFAULT_MIN_SELECTED_VALUE_IMPROVEMENT_UAH,
    )
    parser.add_argument(
        "--max-tail-failure-delta-count",
        type=float,
        default=DEFAULT_MAX_TAIL_FAILURE_DELTA_COUNT,
    )
    parser.add_argument(
        "--max-safety-failure-count",
        type=float,
        default=DEFAULT_MAX_SAFETY_FAILURE_COUNT,
    )
    args = parser.parse_args(argv)

    proof = build_value_aligned_shadow_promotion_proof(
        audit_dir=args.audit_dir,
        robustness_summary_json=args.robustness_summary_json,
        canonical_aggregate_json=args.canonical_aggregate_json,
        run_slug=args.run_slug,
        min_source_backed_days=args.min_source_backed_days,
        min_switch_rate=args.min_switch_rate,
        max_value_gap_ratio=args.max_value_gap_ratio,
        min_selected_value_improvement_uah=args.min_selected_value_improvement_uah,
        max_tail_failure_delta_count=args.max_tail_failure_delta_count,
        max_safety_failure_count=args.max_safety_failure_count,
    )
    paths = write_value_aligned_shadow_promotion_proof(
        output_dir=args.output_dir,
        proof=proof,
    )
    result = {
        "promotion_gate_json": str(paths["promotion_gate_json"]),
        "promotion_gate_md": str(paths["promotion_gate_md"]),
        "selected_nonfallback_days_csv": str(paths["selected_nonfallback_days_csv"]),
        "shadow_promotion_gate_passed": proof["shadow_promotion_gate_passed"],
        "blocking_reasons": proof["blocking_reasons"],
        "market_execution_enabled": False,
        "production_market_promotion_gate_passed": False,
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if proof["shadow_promotion_gate_passed"] else 2


def build_value_aligned_shadow_promotion_proof(
    *,
    audit_dir: Path,
    robustness_summary_json: Path,
    canonical_aggregate_json: Path,
    run_slug: str,
    candidate_grid_id: str = DEFAULT_CANDIDATE_GRID_ID,
    baseline_grid_id: str = DEFAULT_BASELINE_GRID_ID,
    threshold_uah: float = DEFAULT_THRESHOLD_UAH,
    min_source_backed_days: float = DEFAULT_MIN_SOURCE_BACKED_DAYS,
    min_switch_rate: float = DEFAULT_MIN_SWITCH_RATE,
    max_value_gap_ratio: float = DEFAULT_MAX_VALUE_GAP_RATIO,
    min_selected_value_improvement_uah: float = DEFAULT_MIN_SELECTED_VALUE_IMPROVEMENT_UAH,
    max_tail_failure_delta_count: float = DEFAULT_MAX_TAIL_FAILURE_DELTA_COUNT,
    max_safety_failure_count: float = DEFAULT_MAX_SAFETY_FAILURE_COUNT,
) -> dict[str, Any]:
    """Build a source-backed shadow-promotion proof from existing artifacts."""

    audit_summary = _load_json(audit_dir / "summary.json")
    robustness_summary = _load_json(robustness_summary_json)
    canonical_aggregate = _load_json(canonical_aggregate_json)
    threshold_rows = _read_csv(audit_dir / "threshold_summary.csv")
    candidate_rows = _read_csv(audit_dir / "candidate_scores.csv")

    baseline_threshold = _find_threshold_row(
        threshold_rows,
        template_grid_id=baseline_grid_id,
        threshold_uah=threshold_uah,
    )
    candidate_threshold = _find_threshold_row(
        threshold_rows,
        template_grid_id=candidate_grid_id,
        threshold_uah=threshold_uah,
    )
    selected_nonfallback_days = _selected_nonfallback_days(
        candidate_rows,
        candidate_grid_id=candidate_grid_id,
        threshold_uah=threshold_uah,
    )

    source_backed_days = _float(candidate_threshold, "source_backed_day_count")
    switch_rate = _float(candidate_threshold, "switch_rate")
    selected_value_improvement = _summary_metric(
        audit_summary,
        f"{candidate_grid_id}_selected_value_improvement_uah",
    )
    value_gap_ratio = _summary_metric(
        audit_summary,
        f"{candidate_grid_id}_value_gap_ratio_vs_default",
    )
    tail_failure_delta = _summary_metric(
        audit_summary,
        f"{candidate_grid_id}_tail_failure_delta_count",
    )
    safety_failure_count = _summary_metric(
        audit_summary,
        f"{candidate_grid_id}_safety_failure_count",
    )
    robustness_metrics = _mapping(
        robustness_summary.get("selected_threshold_metrics"),
    )
    canonical_comparison = _mapping(robustness_summary.get("canonical_comparison"))
    hf_mean_regret = _first_float(
        robustness_metrics.get("selected_mean_regret_mean"),
        canonical_comparison.get("mean_hf_mean_regret_uah"),
    )
    v2_plus_mean_regret = _first_float(
        canonical_comparison.get("v2_plus_baseline_mean_regret_uah"),
        canonical_aggregate.get("baseline_mean_regret"),
    )
    hf_minus_v2 = _first_float(
        robustness_metrics.get("mean_minus_v2_plus_baseline_uah"),
        canonical_comparison.get("mean_hf_minus_v2_plus_uah"),
        hf_mean_regret - v2_plus_mean_regret,
    )

    gate_results = {
        "source_backed_multi_day_window": _gate_result(
            source_backed_days >= min_source_backed_days,
            observed=source_backed_days,
            required=f">= {min_source_backed_days:g}",
        ),
        "multi_day_nonfallback_switch_rate": _gate_result(
            switch_rate >= min_switch_rate,
            observed=switch_rate,
            required=f">= {min_switch_rate:g}",
        ),
        "selected_value_improvement": _gate_result(
            selected_value_improvement > min_selected_value_improvement_uah,
            observed=selected_value_improvement,
            required=f"> {min_selected_value_improvement_uah:g} UAH",
        ),
        "value_gap_ratio": _gate_result(
            value_gap_ratio <= max_value_gap_ratio,
            observed=value_gap_ratio,
            required=f"<= {max_value_gap_ratio:g}",
        ),
        "tail_risk_control": _gate_result(
            tail_failure_delta <= max_tail_failure_delta_count,
            observed=tail_failure_delta,
            required=f"<= {max_tail_failure_delta_count:g}",
        ),
        "zero_safety_failures": _gate_result(
            safety_failure_count <= max_safety_failure_count,
            observed=safety_failure_count,
            required=f"<= {max_safety_failure_count:g}",
        ),
        "hf_robustness_gate": _gate_result(
            bool(robustness_summary.get("robustness_gate_passed")),
            observed=robustness_summary.get("robustness_gate_passed"),
            required="true",
        ),
        "frozen_regret_vs_v2_plus": _gate_result(
            hf_minus_v2 < 0.0 and hf_mean_regret < v2_plus_mean_regret,
            observed=hf_minus_v2,
            required="< 0 UAH",
        ),
    }
    blocking_reasons = [
        gate_name for gate_name, gate in gate_results.items() if not gate["passed"]
    ]
    passed = not blocking_reasons
    return {
        "run_slug": run_slug,
        "claim_scope": "value_aligned_hf_live_shadow_candidate_library_promotion_proof",
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "promotion_gate_scope": "value_aligned_shadow_candidate_library",
        "shadow_promotion_gate_passed": passed,
        "production_market_promotion_gate_passed": False,
        "market_execution_enabled": False,
        "market_order_payload_emitted": False,
        "proposed_bid_emitted": False,
        "dt_lava_ready": False,
        "candidate_template_grid_id": candidate_grid_id,
        "baseline_template_grid_id": baseline_grid_id,
        "selected_operating_threshold_uah": threshold_uah,
        "source_price_scope": audit_summary.get(
            "source_price_scope",
            "official_oree_observed_rows_only",
        ),
        "source_backed_day_count": source_backed_days,
        "baseline_switch_rate": _float(baseline_threshold, "switch_rate"),
        "value_aligned_switch_rate": switch_rate,
        "selected_nonfallback_day_count": float(len(selected_nonfallback_days)),
        "selected_nonfallback_days": selected_nonfallback_days,
        "mean_selected_value_uah": _float(
            candidate_threshold,
            "mean_selected_schedule_value_uah",
        ),
        "mean_best_template_value_uah": _float(
            candidate_threshold,
            "mean_best_template_schedule_value_uah",
        ),
        "mean_selected_vs_best_template_value_gap_uah": _float(
            candidate_threshold,
            "mean_selected_vs_best_template_value_gap_uah",
        ),
        "selected_value_improvement_vs_default_uah": selected_value_improvement,
        "value_gap_ratio_vs_default": value_gap_ratio,
        "tail_failure_delta_vs_default_count": tail_failure_delta,
        "safety_failure_count": safety_failure_count,
        "hf_frozen_mean_regret_uah": hf_mean_regret,
        "v2_plus_baseline_mean_regret_uah": v2_plus_mean_regret,
        "hf_minus_v2_plus_mean_regret_uah": hf_minus_v2,
        "robustness_gate_passed": bool(robustness_summary.get("robustness_gate_passed")),
        "canonical_safe_switch_mean_regret_uah": _first_float(
            canonical_comparison.get("canonical_safe_switch_mean_regret_uah"),
            canonical_aggregate.get("mean_test_regret"),
        ),
        "gate_results": gate_results,
        "blocking_reasons": blocking_reasons,
        "artifact_inputs": {
            "audit_summary_json": str(audit_dir / "summary.json"),
            "threshold_summary_csv": str(audit_dir / "threshold_summary.csv"),
            "candidate_scores_csv": str(audit_dir / "candidate_scores.csv"),
            "robustness_summary_json": str(robustness_summary_json),
            "canonical_aggregate_json": str(canonical_aggregate_json),
        },
    }


def write_value_aligned_shadow_promotion_proof(
    *,
    output_dir: Path,
    proof: Mapping[str, Any],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "promotion_gate_json": output_dir / "promotion_gate.json",
        "promotion_gate_md": output_dir / "promotion_gate.md",
        "selected_nonfallback_days_csv": output_dir / "selected_nonfallback_days.csv",
    }
    paths["promotion_gate_json"].write_text(
        json.dumps(dict(proof), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    paths["promotion_gate_md"].write_text(_promotion_markdown(proof), encoding="utf-8")
    _write_csv(paths["selected_nonfallback_days_csv"], proof["selected_nonfallback_days"])
    return paths


def _selected_nonfallback_days(
    rows: Sequence[Mapping[str, str]],
    *,
    candidate_grid_id: str,
    threshold_uah: float,
) -> list[dict[str, Any]]:
    grouped: dict[str, list[Mapping[str, str]]] = {}
    for row in rows:
        if row.get("template_grid_id") != candidate_grid_id:
            continue
        grouped.setdefault(str(row["target_delivery_date"]), []).append(row)
    selected: list[dict[str, Any]] = []
    for target_date, candidates in sorted(grouped.items()):
        eligible = [
            row
            for row in candidates
            if row.get("candidate_family") != FALLBACK_FAMILY
            and _as_float(row.get("predicted_regret_delta_vs_v2_plus_uah"))
            < -threshold_uah
            and _as_bool(row.get("predicted_tail_guard_passed"))
            and _as_bool(row.get("family_tail_guard_passed"))
            and _as_bool(row.get("safety_guard_passed"))
        ]
        if not eligible:
            continue
        selected_row = min(
            eligible,
            key=lambda row: _as_float(row.get("predicted_regret_delta_vs_v2_plus_uah")),
        )
        selected.append(
            {
                "target_delivery_date": target_date,
                "candidate_family": selected_row["candidate_family"],
                "candidate_id": selected_row.get("candidate_id", ""),
                "predicted_regret_delta_vs_v2_plus_uah": _as_float(
                    selected_row.get("predicted_regret_delta_vs_v2_plus_uah")
                ),
                "predicted_tail_risk_probability": _as_float(
                    selected_row.get("predicted_tail_risk_probability")
                ),
                "family_tail_risk_probability": _as_float(
                    selected_row.get("family_tail_risk_probability")
                ),
                "schedule_value_uah": _as_float(selected_row.get("schedule_value_uah")),
                "safety_violation_count": _as_float(
                    selected_row.get("safety_violation_count")
                ),
            }
        )
    return selected


def _find_threshold_row(
    rows: Sequence[Mapping[str, str]],
    *,
    template_grid_id: str,
    threshold_uah: float,
) -> Mapping[str, str]:
    for row in rows:
        if row.get("template_grid_id") == template_grid_id and _as_float(
            row.get("threshold_uah")
        ) == threshold_uah:
            return row
    raise ValueError(f"Missing {template_grid_id} threshold {threshold_uah:g} summary.")


def _summary_metric(summary: Mapping[str, Any], key: str) -> float:
    if key not in summary:
        raise ValueError(f"Missing audit summary metric: {key}")
    return _as_float(summary[key])


def _float(row: Mapping[str, Any], key: str) -> float:
    if key not in row:
        raise ValueError(f"Missing required field: {key}")
    return _as_float(row[key])


def _first_float(*values: object) -> float:
    for value in values:
        if value is not None:
            return _as_float(value)
    raise ValueError("No numeric value available.")


def _as_float(value: object) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        return float(value)
    raise ValueError(f"Expected numeric value, got {value!r}.")


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes"}:
            return True
        if normalized in {"false", "0", "no"}:
            return False
    return bool(value)


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _gate_result(passed: bool, *, observed: object, required: str) -> dict[str, Any]:
    return {
        "passed": passed,
        "observed": observed,
        "required": required,
    }


def _load_json(path: Path) -> dict[str, Any]:
    data = json.loads(path.read_text(encoding="utf-8-sig"))
    if not isinstance(data, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return data


def _read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def _write_csv(path: Path, rows: object) -> None:
    if not isinstance(rows, Sequence):
        raise ValueError("selected_nonfallback_days must be a sequence.")
    normalized = [dict(row) for row in rows if isinstance(row, Mapping)]
    if not normalized:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(normalized[0]))
        writer.writeheader()
        writer.writerows(normalized)


def _promotion_markdown(proof: Mapping[str, Any]) -> str:
    gate_results = _mapping(proof.get("gate_results"))
    gate_lines = [
        f"- `{name}`: {gate['passed']} (observed={gate['observed']}, required={gate['required']})"
        for name, gate in gate_results.items()
        if isinstance(gate, Mapping)
    ]
    return "\n".join(
        [
            "# HF Live Safe-Switch Value-Aligned Shadow Promotion Proof",
            "",
            f"- Run slug: `{proof['run_slug']}`",
            f"- Promotion scope: `{proof['promotion_gate_scope']}`",
            f"- shadow_promotion_gate_passed: `{proof['shadow_promotion_gate_passed']}`",
            f"- Candidate grid: `{proof['candidate_template_grid_id']}`",
            f"- Source-backed days: {proof['source_backed_day_count']}",
            f"- Non-fallback switch rate: {proof['value_aligned_switch_rate']}",
            f"- Selected value improvement vs default: {proof['selected_value_improvement_vs_default_uah']} UAH",
            f"- Value gap ratio vs default: {proof['value_gap_ratio_vs_default']}",
            f"- Tail-failure delta vs default: {proof['tail_failure_delta_vs_default_count']}",
            f"- Safety failures: {proof['safety_failure_count']}",
            f"- HF frozen mean regret: {proof['hf_frozen_mean_regret_uah']} UAH",
            f"- V2+ baseline mean regret: {proof['v2_plus_baseline_mean_regret_uah']} UAH",
            f"- HF minus V2+ mean regret: {proof['hf_minus_v2_plus_mean_regret_uah']} UAH",
            "",
            "## Gate Results",
            *gate_lines,
            "",
            "Market execution remains disabled: no ProposedBid and no market order payload.",
        ]
    )


if __name__ == "__main__":
    raise SystemExit(main())
