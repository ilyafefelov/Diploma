"""Offline DT/V2+ challenger promotion-evidence gate.

The gate evaluates a residual DT selector after the fact. V2+ remains the
champion/default/fallback, and strict/oracle regret is used only as frozen
scoring evidence, not as a selector input.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
import json
import math
from pathlib import Path
from typing import Any, Final, cast

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
)

DT_V2_PLUS_PROMOTION_EVIDENCE_CLAIM_SCOPE: Final[str] = (
    "dt_v2_plus_residual_challenger_promotion_evidence_not_market_execution"
)
ARTIFACT_PREFIX: Final[str] = "dt_v2_plus_promotion_evidence"
GATE_ROWS_CSV_NAME: Final[str] = f"{ARTIFACT_PREFIX}_gate_rows.csv"
SELECTED_ROWS_CSV_NAME: Final[str] = f"{ARTIFACT_PREFIX}_selected_rows.csv"
SAFE_SWITCH_ROWS_CSV_NAME: Final[str] = f"{ARTIFACT_PREFIX}_safe_switch_opportunities.csv"
SUMMARY_JSON_NAME: Final[str] = f"{ARTIFACT_PREFIX}_summary.json"
SUMMARY_MD_NAME: Final[str] = f"{ARTIFACT_PREFIX}_summary.md"
V2_PLUS_FAMILY_ALIASES: Final[frozenset[str]] = frozenset(
    {
        "schedule_value_learner_v2_plus",
        "schedule_value_learner_v2_plus_reference",
        "frozen_v2_plus_fallback",
    }
)
STRICT_REFERENCE_FAMILIES: Final[frozenset[str]] = frozenset(
    {"strict_reference", "strict_similar_day"}
)
REQUIRED_SELECTED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "selected_candidate_id",
        "selected_schedule_family",
        "selected_regret_uah",
        "selected_value_uah",
        "v2_plus_regret_uah",
        "v2_plus_value_uah",
        "selected_minus_v2_plus_regret_uah",
        "predicted_improvement_vs_v2_plus_uah",
        "abstained_to_v2_plus",
        "tail_risk_guard_passed",
        "research_shadow_not_promotable",
        "promotion_gate_passed",
        "market_execution_enabled",
        "not_market_execution",
    }
)
REQUIRED_TEACHER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "dt_candidate_id_target",
        "dt_schedule_family_target",
        "regret_uah",
        "schedule_value_uah",
        "oracle_value_uah",
        "regret_delta_vs_v2_plus_uah",
        "safety_violation_count",
        "not_market_execution",
        "market_execution_enabled",
        "promotion_gate_passed",
        "raw_hourly_action_imitation",
    }
)


def build_dt_v2_plus_promotion_evidence_packet(
    selected_rows_frame: pl.DataFrame,
    teacher_rows_frame: pl.DataFrame,
    *,
    run_slug: str,
    min_final_holdout_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    max_non_v2_plus_switch_rate: float = 0.25,
    tail_risk_loss_threshold_uah: float = 150.0,
    max_tail_risk_loss_count: int = 0,
) -> dict[str, Any]:
    """Evaluate whether a residual DT selector earns offline promotion evidence."""

    _validate_config(
        run_slug=run_slug,
        min_final_holdout_anchor_count=min_final_holdout_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        max_non_v2_plus_switch_rate=max_non_v2_plus_switch_rate,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
        max_tail_risk_loss_count=max_tail_risk_loss_count,
    )
    selected_rows = _normalize_selected_rows(selected_rows_frame)
    teacher_rows = _normalize_teacher_rows(teacher_rows_frame)
    final_teacher_rows = teacher_rows.filter(pl.col("split_name") == "final_holdout")
    if final_teacher_rows.is_empty():
        raise ValueError("DT/V2+ promotion evidence requires final_holdout teacher rows.")

    final_teacher_dicts = list(final_teacher_rows.iter_rows(named=True))
    selected_dicts = list(selected_rows.iter_rows(named=True))
    coverage = _coverage_summary(selected_dicts, final_teacher_dicts)
    safe_switch_rows = _safe_switch_opportunity_rows(final_teacher_dicts)
    scoring = _scoring_summary(
        selected_dicts,
        final_teacher_dicts,
        safe_switch_rows=safe_switch_rows,
        coverage=coverage,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
    )
    blocker = _promotion_blocker(
        coverage=coverage,
        scoring=scoring,
        min_final_holdout_anchor_count=min_final_holdout_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        max_non_v2_plus_switch_rate=max_non_v2_plus_switch_rate,
        max_tail_risk_loss_count=max_tail_risk_loss_count,
    )
    promotion_evidence_passed = blocker == "none"
    gate_row = {
        "run_slug": run_slug,
        "source_model_name": scoring["source_model_name"],
        "final_holdout_anchor_count": coverage["selected_anchor_count"],
        "teacher_final_holdout_anchor_count": coverage["teacher_anchor_count"],
        "matching_final_holdout_coverage": coverage["matching_final_holdout_coverage"],
        "selected_mean_regret_uah": scoring["selected_mean_regret_uah"],
        "v2_plus_mean_regret_uah": scoring["v2_plus_mean_regret_uah"],
        "selected_median_regret_uah": scoring["selected_median_regret_uah"],
        "v2_plus_median_regret_uah": scoring["v2_plus_median_regret_uah"],
        "selector_minus_v2_plus_mean_regret_uah": scoring[
            "selector_minus_v2_plus_mean_regret_uah"
        ],
        "mean_regret_improvement_ratio_vs_v2_plus": scoring[
            "mean_regret_improvement_ratio_vs_v2_plus"
        ],
        "median_not_worse_vs_v2_plus": scoring["median_not_worse_vs_v2_plus"],
        "non_v2_plus_switch_count": scoring["non_v2_plus_switch_count"],
        "non_v2_plus_switch_rate": scoring["non_v2_plus_switch_rate"],
        "safe_switch_win_count": scoring["safe_switch_win_count"],
        "safe_switch_loss_count": scoring["safe_switch_loss_count"],
        "safe_switch_tie_count": scoring["safe_switch_tie_count"],
        "tail_risk_loss_count": scoring["tail_risk_loss_count"],
        "max_switch_loss_uah": scoring["max_switch_loss_uah"],
        "observed_safe_switch_opportunity_count": len(safe_switch_rows),
        "recovered_safe_switch_opportunity_count": scoring[
            "recovered_safe_switch_opportunity_count"
        ],
        "strict_reference_mean_regret_uah": scoring["strict_reference_mean_regret_uah"],
        "oracle_scored_final_holdout_row_count": scoring[
            "oracle_scored_final_holdout_row_count"
        ],
        "promotion_evidence_passed": promotion_evidence_passed,
        "promotion_blocker": blocker,
        "fallback_strategy": "schedule_value_learner_v2_plus",
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
        "dt_lava_ready": False,
        "permits_model_training": False,
        "claim_scope": DT_V2_PLUS_PROMOTION_EVIDENCE_CLAIM_SCOPE,
        "not_market_execution": True,
    }
    gate_rows = pl.DataFrame([gate_row], infer_schema_length=None)
    safe_switch_frame = (
        pl.DataFrame(safe_switch_rows, infer_schema_length=None)
        if safe_switch_rows
        else _empty_safe_switch_frame()
    )
    summary = _summary(
        run_slug=run_slug,
        gate_row=gate_row,
        scoring=scoring,
        coverage=coverage,
        safe_switch_rows=safe_switch_rows,
        min_final_holdout_anchor_count=min_final_holdout_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        max_non_v2_plus_switch_rate=max_non_v2_plus_switch_rate,
        tail_risk_loss_threshold_uah=tail_risk_loss_threshold_uah,
        max_tail_risk_loss_count=max_tail_risk_loss_count,
    )
    return {
        "gate_rows": gate_rows,
        "selected_rows": selected_rows,
        "safe_switch_opportunity_rows": safe_switch_frame,
        "summary": summary,
    }


def write_dt_v2_plus_promotion_evidence_packet(
    *,
    output_dir: Path,
    packet: Mapping[str, Any],
) -> dict[str, Path]:
    """Write gate rows and summary artifacts."""

    output_dir.mkdir(parents=True, exist_ok=True)
    gate_rows = _frame_value(packet["gate_rows"], key="gate_rows")
    selected_rows = _frame_value(packet["selected_rows"], key="selected_rows")
    safe_switch_rows = _frame_value(
        packet["safe_switch_opportunity_rows"],
        key="safe_switch_opportunity_rows",
    )
    summary = _mapping(packet["summary"])
    gate_rows_csv = output_dir / GATE_ROWS_CSV_NAME
    selected_rows_csv = output_dir / SELECTED_ROWS_CSV_NAME
    safe_switch_rows_csv = output_dir / SAFE_SWITCH_ROWS_CSV_NAME
    summary_json = output_dir / SUMMARY_JSON_NAME
    summary_md = output_dir / SUMMARY_MD_NAME
    gate_rows.write_csv(gate_rows_csv)
    selected_rows.write_csv(selected_rows_csv)
    safe_switch_rows.write_csv(safe_switch_rows_csv)
    summary_json.write_text(
        json.dumps(_jsonable(summary), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_md.write_text(_summary_markdown(summary), encoding="utf-8")
    return {
        "gate_rows_csv": gate_rows_csv,
        "selected_rows_csv": selected_rows_csv,
        "safe_switch_opportunities_csv": safe_switch_rows_csv,
        "summary_json": summary_json,
        "summary_markdown": summary_md,
    }


def _validate_config(
    *,
    run_slug: str,
    min_final_holdout_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    max_non_v2_plus_switch_rate: float,
    tail_risk_loss_threshold_uah: float,
    max_tail_risk_loss_count: int,
) -> None:
    if not run_slug:
        raise ValueError("run_slug must be non-empty.")
    if min_final_holdout_anchor_count <= 0:
        raise ValueError("min_final_holdout_anchor_count must be positive.")
    if min_mean_regret_improvement_ratio_vs_v2_plus < 0.0:
        raise ValueError(
            "min_mean_regret_improvement_ratio_vs_v2_plus must not be negative."
        )
    if not 0.0 <= max_non_v2_plus_switch_rate <= 1.0:
        raise ValueError("max_non_v2_plus_switch_rate must be in [0, 1].")
    if tail_risk_loss_threshold_uah <= 0.0:
        raise ValueError("tail_risk_loss_threshold_uah must be positive.")
    if max_tail_risk_loss_count < 0:
        raise ValueError("max_tail_risk_loss_count must not be negative.")


def _normalize_selected_rows(frame: pl.DataFrame) -> pl.DataFrame:
    _require_columns(frame, REQUIRED_SELECTED_COLUMNS, frame_name="selected_rows_frame")
    if frame.is_empty():
        raise ValueError("selected_rows_frame must not be empty.")
    if _frame_has_true(frame, "market_execution_enabled"):
        raise ValueError("selected_rows_frame requires market_execution_enabled=false.")
    if _frame_has_true(frame, "promotion_gate_passed"):
        raise ValueError("selected_rows_frame requires promotion_gate_passed=false.")
    if _frame_has_true(frame, "dt_lava_ready"):
        raise ValueError("selected_rows_frame requires dt_lava_ready=false.")
    if _frame_has_true(frame, "permits_model_training"):
        raise ValueError("selected_rows_frame requires permits_model_training=false.")
    if not _frame_all_true(frame, "not_market_execution"):
        raise ValueError("selected_rows_frame requires not_market_execution=true.")
    if not _frame_all_true(frame, "research_shadow_not_promotable"):
        raise ValueError(
            "selected_rows_frame requires research_shadow_not_promotable=true."
        )
    expressions = [
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(False).alias("promotion_gate_passed"),
        pl.lit(False).alias("dt_lava_ready"),
        pl.lit(False).alias("permits_model_training"),
        pl.lit(True).alias("not_market_execution"),
    ]
    return frame.with_columns(expressions).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp"]
    )


def _normalize_teacher_rows(frame: pl.DataFrame) -> pl.DataFrame:
    _require_columns(frame, REQUIRED_TEACHER_COLUMNS, frame_name="teacher_rows_frame")
    if frame.is_empty():
        raise ValueError("teacher_rows_frame must not be empty.")
    if _frame_has_true(frame, "market_execution_enabled"):
        raise ValueError("teacher_rows_frame requires market_execution_enabled=false.")
    if _frame_has_true(frame, "promotion_gate_passed"):
        raise ValueError("teacher_rows_frame requires promotion_gate_passed=false.")
    if _frame_has_true(frame, "market_execution_gate_passed"):
        raise ValueError("teacher_rows_frame requires market_execution_gate_passed=false.")
    if _frame_has_true(frame, "permits_model_training"):
        raise ValueError("teacher_rows_frame requires permits_model_training=false.")
    if _frame_has_true(frame, "raw_hourly_action_imitation"):
        raise ValueError("teacher_rows_frame refuses raw hourly action rows.")
    if not _frame_all_true(frame, "not_market_execution"):
        raise ValueError("teacher_rows_frame requires not_market_execution=true.")
    return frame.with_columns(
        pl.col("anchor_timestamp").cast(pl.Datetime, strict=False),
        pl.lit(False).alias("market_execution_enabled"),
        pl.lit(False).alias("promotion_gate_passed"),
        pl.lit(False).alias("dt_lava_ready"),
        pl.lit(False).alias("permits_model_training"),
        pl.lit(True).alias("not_market_execution"),
    ).sort(["tenant_id", "source_model_name", "anchor_timestamp"])


def _coverage_summary(
    selected_rows: Sequence[Mapping[str, Any]],
    final_teacher_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    selected_anchor_keys = [_anchor_key(row) for row in selected_rows]
    teacher_anchor_keys = sorted({_anchor_key(row) for row in final_teacher_rows})
    duplicate_selected_count = len(selected_anchor_keys) - len(set(selected_anchor_keys))
    selected_anchor_set = set(selected_anchor_keys)
    teacher_anchor_set = set(teacher_anchor_keys)
    v2_anchor_set = {
        _anchor_key(row)
        for row in final_teacher_rows
        if _is_v2_plus_family(str(row["dt_schedule_family_target"]))
    }
    selected_missing_teacher = selected_anchor_set.difference(teacher_anchor_set)
    teacher_missing_selected = teacher_anchor_set.difference(selected_anchor_set)
    missing_v2 = selected_anchor_set.difference(v2_anchor_set)
    return {
        "selected_anchor_count": len(selected_anchor_set),
        "teacher_anchor_count": len(teacher_anchor_set),
        "duplicate_selected_anchor_count": duplicate_selected_count,
        "matching_final_holdout_coverage": (
            duplicate_selected_count == 0
            and selected_anchor_set == teacher_anchor_set
            and not missing_v2
        ),
        "selected_missing_teacher_count": len(selected_missing_teacher),
        "teacher_missing_selected_count": len(teacher_missing_selected),
        "missing_v2_plus_reference_count": len(missing_v2),
    }


def _safe_switch_opportunity_rows(
    final_teacher_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for _, anchor_rows in _group_anchor_rows(final_teacher_rows):
        safe_candidates = [
            row
            for row in anchor_rows
            if not _is_v2_plus_family(str(row["dt_schedule_family_target"]))
            and int(_float(row.get("safety_violation_count"))) == 0
            and _float(row["regret_delta_vs_v2_plus_uah"]) < 0.0
        ]
        if not safe_candidates:
            continue
        best = min(
            safe_candidates,
            key=lambda row: _float(row["regret_delta_vs_v2_plus_uah"]),
        )
        rows.append(
            {
                "tenant_id": str(best["tenant_id"]),
                "source_model_name": str(best["source_model_name"]),
                "anchor_timestamp": _datetime_value(
                    best["anchor_timestamp"],
                    field_name="anchor_timestamp",
                ),
                "best_safe_switch_candidate_id": str(best["dt_candidate_id_target"]),
                "best_safe_switch_family": str(best["dt_schedule_family_target"]),
                "best_safe_switch_regret_delta_vs_v2_plus_uah": _float(
                    best["regret_delta_vs_v2_plus_uah"]
                ),
                "best_safe_switch_regret_uah": _float(best["regret_uah"]),
                "best_safe_switch_value_uah": _float(best["schedule_value_uah"]),
            }
        )
    return rows


def _scoring_summary(
    selected_rows: Sequence[Mapping[str, Any]],
    final_teacher_rows: Sequence[Mapping[str, Any]],
    *,
    safe_switch_rows: Sequence[Mapping[str, Any]],
    coverage: Mapping[str, Any],
    tail_risk_loss_threshold_uah: float,
) -> dict[str, Any]:
    source_model_names = sorted({str(row["source_model_name"]) for row in selected_rows})
    selected_regrets = [_float(row["selected_regret_uah"]) for row in selected_rows]
    v2_regrets = [_float(row["v2_plus_regret_uah"]) for row in selected_rows]
    selected_median = _median(selected_regrets)
    v2_median = _median(v2_regrets)
    selected_mean = _mean(selected_regrets)
    v2_mean = _mean(v2_regrets)
    switch_rows = [
        row
        for row in selected_rows
        if not _is_v2_plus_family(str(row["selected_schedule_family"]))
    ]
    switch_deltas = [_float(row["selected_minus_v2_plus_regret_uah"]) for row in switch_rows]
    safe_opportunity_keys = {_anchor_key(row) for row in safe_switch_rows}
    recovered_safe_switch_count = sum(
        1
        for row in switch_rows
        if _anchor_key(row) in safe_opportunity_keys
        and _float(row["selected_minus_v2_plus_regret_uah"]) < 0.0
    )
    strict_reference_regrets = [
        _float(row["regret_uah"])
        for row in final_teacher_rows
        if str(row["dt_schedule_family_target"]).casefold() in STRICT_REFERENCE_FAMILIES
    ]
    oracle_scored_count = sum(
        1 for row in final_teacher_rows if _has_finite_float(row.get("oracle_value_uah"))
    )
    switch_rate = (
        len(switch_rows) / int(coverage["selected_anchor_count"])
        if int(coverage["selected_anchor_count"]) > 0
        else 0.0
    )
    return {
        "source_model_name": ",".join(source_model_names),
        "selected_mean_regret_uah": selected_mean,
        "v2_plus_mean_regret_uah": v2_mean,
        "selected_median_regret_uah": selected_median,
        "v2_plus_median_regret_uah": v2_median,
        "selector_minus_v2_plus_mean_regret_uah": selected_mean - v2_mean,
        "mean_regret_improvement_ratio_vs_v2_plus": _improvement_ratio(
            v2_mean,
            selected_mean,
        ),
        "median_not_worse_vs_v2_plus": selected_median <= v2_median,
        "non_v2_plus_switch_count": len(switch_rows),
        "non_v2_plus_switch_rate": switch_rate,
        "safe_switch_win_count": sum(1 for delta in switch_deltas if delta < 0.0),
        "safe_switch_loss_count": sum(1 for delta in switch_deltas if delta > 0.0),
        "safe_switch_tie_count": sum(1 for delta in switch_deltas if delta == 0.0),
        "tail_risk_loss_count": sum(
            1 for delta in switch_deltas if delta >= tail_risk_loss_threshold_uah
        ),
        "max_switch_loss_uah": max([0.0, *switch_deltas]),
        "recovered_safe_switch_opportunity_count": recovered_safe_switch_count,
        "strict_reference_mean_regret_uah": _mean(strict_reference_regrets),
        "oracle_scored_final_holdout_row_count": oracle_scored_count,
    }


def _promotion_blocker(
    *,
    coverage: Mapping[str, Any],
    scoring: Mapping[str, Any],
    min_final_holdout_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    max_non_v2_plus_switch_rate: float,
    max_tail_risk_loss_count: int,
) -> str:
    if not bool(coverage["matching_final_holdout_coverage"]):
        return "selected_final_holdout_coverage_mismatch"
    if int(coverage["selected_anchor_count"]) < min_final_holdout_anchor_count:
        return "final_holdout_undercoverage"
    if int(scoring["non_v2_plus_switch_count"]) == 0:
        return "no_non_v2_plus_switches"
    if float(scoring["non_v2_plus_switch_rate"]) > max_non_v2_plus_switch_rate:
        return "switch_rate_not_rare"
    if int(scoring["tail_risk_loss_count"]) > max_tail_risk_loss_count:
        return "tail_risk_loss_detected"
    if not bool(scoring["median_not_worse_vs_v2_plus"]):
        return "median_regret_degraded_vs_v2_plus"
    if (
        float(scoring["mean_regret_improvement_ratio_vs_v2_plus"])
        < min_mean_regret_improvement_ratio_vs_v2_plus
    ):
        return "mean_regret_not_improved_vs_v2_plus"
    if int(scoring["safe_switch_win_count"]) == 0:
        return "no_winning_safe_switches"
    return "none"


def _summary(
    *,
    run_slug: str,
    gate_row: Mapping[str, Any],
    scoring: Mapping[str, Any],
    coverage: Mapping[str, Any],
    safe_switch_rows: Sequence[Mapping[str, Any]],
    min_final_holdout_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    max_non_v2_plus_switch_rate: float,
    tail_risk_loss_threshold_uah: float,
    max_tail_risk_loss_count: int,
) -> dict[str, Any]:
    return {
        "run_slug": run_slug,
        "claim_scope": DT_V2_PLUS_PROMOTION_EVIDENCE_CLAIM_SCOPE,
        "generated_at": datetime.now(UTC).replace(microsecond=0).isoformat(),
        "method": (
            "Offline residual-DT challenger gate. V2+ remains champion/default/"
            "fallback; strict/oracle regret is used only as frozen final-holdout "
            "scoring evidence."
        ),
        "gate": dict(gate_row),
        "evaluation": dict(scoring),
        "coverage": dict(coverage),
        "safe_switch_opportunities": {
            "observed_safe_switch_opportunity_count": len(safe_switch_rows),
            "recovered_safe_switch_opportunity_count": gate_row[
                "recovered_safe_switch_opportunity_count"
            ],
        },
        "gate_config": {
            "min_final_holdout_anchor_count": min_final_holdout_anchor_count,
            "min_mean_regret_improvement_ratio_vs_v2_plus": (
                min_mean_regret_improvement_ratio_vs_v2_plus
            ),
            "max_non_v2_plus_switch_rate": max_non_v2_plus_switch_rate,
            "tail_risk_loss_threshold_uah": tail_risk_loss_threshold_uah,
            "max_tail_risk_loss_count": max_tail_risk_loss_count,
        },
        "boundary": {
            "offline_promotion_evidence_only": True,
            "promotion_evidence_passed": bool(gate_row["promotion_evidence_passed"]),
            "promotion_gate_passed": False,
            "v2_plus_remains_default": True,
            "fallback_strategy": "schedule_value_learner_v2_plus",
            "strict_oracle_reference_is_runtime_input": False,
            "frozen_strict_oracle_used_for_after_the_fact_scoring": True,
            "no_dashboard_api_default_switch": True,
            "dt_lava_ready": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
            "not_market_execution": True,
            "no_proposed_bid": True,
        },
        "attached_artifacts": {
            "gate_rows_csv": GATE_ROWS_CSV_NAME,
            "selected_rows_csv": SELECTED_ROWS_CSV_NAME,
            "safe_switch_opportunities_csv": SAFE_SWITCH_ROWS_CSV_NAME,
            "summary_json": SUMMARY_JSON_NAME,
            "summary_markdown": SUMMARY_MD_NAME,
        },
    }


def _summary_markdown(summary: Mapping[str, Any]) -> str:
    gate = _mapping(summary["gate"])
    boundary = _mapping(summary["boundary"])
    lines = [
        "# DT V2+ Promotion Evidence",
        "",
        f"Run slug: `{summary['run_slug']}`",
        "",
        str(summary["method"]),
        "",
        "## Gate",
        "",
        "| Metric | Value |",
        "|---|---:|",
        f"| Promotion evidence passed | `{gate['promotion_evidence_passed']}` |",
        f"| Promotion blocker | `{gate['promotion_blocker']}` |",
        f"| Final holdout anchors | `{gate['final_holdout_anchor_count']}` |",
        f"| Non-V2+ switches | `{gate['non_v2_plus_switch_count']}` |",
        f"| Mean regret improvement vs V2+ | `{float(gate['mean_regret_improvement_ratio_vs_v2_plus']):.2%}` |",
        f"| Selector minus V2+ mean regret | `{float(gate['selector_minus_v2_plus_mean_regret_uah']):.2f}` UAH |",
        f"| Tail-risk losses | `{gate['tail_risk_loss_count']}` |",
        f"| Observed safe-switch opportunities | `{gate['observed_safe_switch_opportunity_count']}` |",
        f"| Recovered safe-switch opportunities | `{gate['recovered_safe_switch_opportunity_count']}` |",
        "",
        "## Boundary",
        "",
        f"- V2+ remains default: `{boundary['v2_plus_remains_default']}`.",
        "- Strict/oracle is a frozen after-the-fact scorer, not a runtime selector input.",
        "- `market_execution_enabled=false`; `promotion_gate_passed=false`; no `ProposedBid`.",
    ]
    return "\n".join(lines) + "\n"


def _empty_safe_switch_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "tenant_id": pl.String,
            "source_model_name": pl.String,
            "anchor_timestamp": pl.Datetime,
            "best_safe_switch_candidate_id": pl.String,
            "best_safe_switch_family": pl.String,
            "best_safe_switch_regret_delta_vs_v2_plus_uah": pl.Float64,
            "best_safe_switch_regret_uah": pl.Float64,
            "best_safe_switch_value_uah": pl.Float64,
        }
    )


def _group_anchor_rows(
    rows: Sequence[Mapping[str, Any]],
) -> list[tuple[tuple[str, str, datetime], list[Mapping[str, Any]]]]:
    grouped: dict[tuple[str, str, datetime], list[Mapping[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_anchor_key(row), []).append(row)
    return sorted(grouped.items(), key=lambda item: item[0])


def _anchor_key(row: Mapping[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
    )


def _is_v2_plus_family(family: str) -> bool:
    normalized = family.casefold()
    return normalized in V2_PLUS_FAMILY_ALIASES or "v2_plus" in normalized


def _improvement_ratio(control_mean: float, candidate_mean: float) -> float:
    if abs(control_mean) <= 1e-9:
        return 0.0
    return (control_mean - candidate_mean) / abs(control_mean)


def _require_columns(
    frame: pl.DataFrame,
    required: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = sorted(required.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} missing columns: {missing}")


def _frame_has_true(frame: pl.DataFrame, column: str) -> bool:
    return column in frame.columns and bool(frame.select(pl.col(column).any()).item())


def _frame_all_true(frame: pl.DataFrame, column: str) -> bool:
    return column in frame.columns and bool(frame.select(pl.col(column).all()).item())


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.replace(tzinfo=None) if parsed.tzinfo else parsed
    raise TypeError(f"{field_name} must be a datetime or ISO datetime string.")


def _float(value: object, *, fallback: float = 0.0) -> float:
    if value is None:
        return fallback
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError):
        return fallback
    if math.isnan(result) or math.isinf(result):
        return fallback
    return result


def _has_finite_float(value: object) -> bool:
    try:
        result = float(cast(Any, value))
    except (TypeError, ValueError):
        return False
    return not (math.isnan(result) or math.isinf(result))


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _median(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[midpoint])
    return float((ordered[midpoint - 1] + ordered[midpoint]) / 2.0)


def _mapping(value: object) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("expected mapping value")
    return value


def _frame_value(value: object, *, key: str) -> pl.DataFrame:
    if not isinstance(value, pl.DataFrame):
        raise TypeError(f"packet['{key}'] must be a Polars DataFrame.")
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


__all__ = [
    "DT_V2_PLUS_PROMOTION_EVIDENCE_CLAIM_SCOPE",
    "GATE_ROWS_CSV_NAME",
    "SAFE_SWITCH_ROWS_CSV_NAME",
    "SELECTED_ROWS_CSV_NAME",
    "SUMMARY_JSON_NAME",
    "SUMMARY_MD_NAME",
    "build_dt_v2_plus_promotion_evidence_packet",
    "write_dt_v2_plus_promotion_evidence_packet",
]
