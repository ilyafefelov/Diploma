"""Offline promotion gate for the Schedule/Value Learner V2 challenger."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from shutil import copyfile
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.schedule_value_learner import (
    REQUIRED_STRICT_COLUMNS,
    schedule_value_learner_v2_model_name,
)
from smart_arbitrage.dfl.schedule_value_learner_robustness import (
    REQUIRED_ROBUSTNESS_COLUMNS,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_value_production_gate_offline_strategy_not_market_execution"
)
DFL_SCHEDULE_VALUE_PRODUCTION_GATE_ACADEMIC_SCOPE: Final[str] = (
    "Offline/read-model default-fallback gate for the Schedule/Value Learner V2. "
    "It can promote a robust research challenger only inside strategy evidence; "
    "market execution remains disabled."
)
STRICT_DEFAULT_FALLBACK: Final[str] = "strict_similar_day_default_fallback"
ATTEMPT_MANIFEST_ARTIFACT_NAME: Final[str] = "attempt_manifest.json"
MONITOR_SNAPSHOT_ARTIFACT_NAME: Final[str] = "resume-summary.json"
LEARNER_TRACE_SUMMARY_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_trace_summary.json"
)
LEARNER_TRACE_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_schedule_value_learner_v2_trace_summary.md"
)

REQUIRED_SCHEDULE_VALUE_PRODUCTION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "tenant_count",
        "latest_validation_tenant_anchor_count",
        "latest_strict_mean_regret_uah",
        "latest_selected_mean_regret_uah",
        "latest_strict_median_regret_uah",
        "latest_selected_median_regret_uah",
        "latest_mean_regret_improvement_ratio_vs_strict",
        "latest_median_not_worse",
        "latest_source_signal",
        "rolling_window_count",
        "rolling_strict_pass_window_count",
        "rolling_development_pass_window_count",
        "robust_research_challenger",
        "allowed_challenger",
        "fallback_strategy",
        "promotion_blocker",
        "production_promote",
        "market_execution_enabled",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_SCHEDULE_VALUE_LEARNER_TRACE_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "selected_weight_profile_name",
        "selected_train_family_counts",
        "selected_final_family_counts",
        "train_anchor_count",
        "final_holdout_anchor_count",
        "final_holdout_tenant_anchor_count",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_schedule_value_production_gate_frame(
    schedule_value_strict_frame: pl.DataFrame,
    schedule_value_robustness_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    min_rolling_window_count: int = 4,
    min_rolling_strict_pass_windows: int = 3,
) -> pl.DataFrame:
    """Build one offline promotion/fallback decision row per source model."""

    _validate_config(
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        min_rolling_window_count=min_rolling_window_count,
        min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
    )
    _require_columns(
        schedule_value_strict_frame,
        REQUIRED_STRICT_COLUMNS,
        frame_name="dfl_schedule_value_learner_v2_strict_lp_benchmark_frame",
    )
    _require_columns(
        schedule_value_robustness_frame,
        REQUIRED_ROBUSTNESS_COLUMNS,
        frame_name="dfl_schedule_value_learner_v2_robustness_frame",
    )
    strict_rows = list(schedule_value_strict_frame.iter_rows(named=True))
    robustness_rows = list(schedule_value_robustness_frame.iter_rows(named=True))
    rows = [
        _promotion_row(
            source_model_name,
            strict_rows=strict_rows,
            robustness_rows=robustness_rows,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
            min_rolling_window_count=min_rolling_window_count,
            min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
        )
        for source_model_name in source_model_names
    ]
    return pl.DataFrame(rows).sort("source_model_name")


def validate_dfl_schedule_value_production_gate_evidence(
    promotion_gate_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_rolling_window_count: int = 4,
) -> EvidenceCheckOutcome:
    """Validate promotion-gate rows while keeping market execution disabled."""

    failures = _missing_column_failures(
        promotion_gate_frame,
        REQUIRED_SCHEDULE_VALUE_PRODUCTION_COLUMNS,
    )
    if failures:
        return EvidenceCheckOutcome(
            False, "; ".join(failures), {"row_count": promotion_gate_frame.height}
        )
    rows = list(promotion_gate_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False, "schedule/value production gate has no rows", {"row_count": 0}
        )

    expected_sources = source_model_names or tuple(
        sorted({str(row["source_model_name"]) for row in rows})
    )
    rows_by_source = {str(row["source_model_name"]): row for row in rows}
    for source_model_name in expected_sources:
        row = rows_by_source.get(source_model_name)
        if row is None:
            failures.append(f"{source_model_name} production gate row is missing")
            continue
        failures.extend(
            _row_validation_failures(
                row,
                min_tenant_count=min_tenant_count,
                min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
                min_rolling_window_count=min_rolling_window_count,
            )
        )

    promoted_source_model_names = sorted(
        str(row["source_model_name"]) for row in rows if bool(row["production_promote"])
    )
    metadata = {
        "row_count": len(rows),
        "source_model_names": list(expected_sources),
        "promoted_source_model_names": promoted_source_model_names,
        "production_promote_count": len(promoted_source_model_names),
        "model_summaries": [_source_summary(row) for row in rows],
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Schedule/value production gate evidence is valid and market execution remains disabled."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_schedule_value_production_gate(
    promotion_gate_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> PromotionGateResult:
    """Return a concise gate result for Dagster metadata."""

    evidence = validate_dfl_schedule_value_production_gate_evidence(
        promotion_gate_frame,
        source_model_names=source_model_names,
    )
    promoted = list(evidence.metadata.get("promoted_source_model_names", []))
    if not evidence.passed:
        return PromotionGateResult(
            False, "blocked", evidence.description, evidence.metadata
        )
    if promoted:
        return PromotionGateResult(
            True,
            "offline_strategy_promoted_market_execution_disabled",
            "Schedule/value learner passed offline promotion gate; market execution remains disabled.",
            evidence.metadata,
        )
    return PromotionGateResult(
        False,
        "valid_evidence_no_promotion",
        "Schedule/value production gate evidence is valid but no source is promoted.",
        evidence.metadata,
    )


def build_dfl_schedule_value_production_gate_registry(
    *,
    run_slug: str,
    gate_frame: pl.DataFrame,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
) -> dict[str, Any]:
    """Build a concise supervisor-facing registry from the promotion gate frame."""

    evidence = validate_dfl_schedule_value_production_gate_evidence(gate_frame)
    rows = [
        _registry_row(row)
        for row in gate_frame.sort("source_model_name").iter_rows(named=True)
    ]
    promoted = [
        row["source_model_name"] for row in rows if bool(row["production_promote"])
    ]
    market_execution_enabled = any(
        bool(row["market_execution_enabled"]) for row in rows
    )
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "claim_boundary": {
            "claim_scope": DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "offline_read_model_strategy_evidence_only": True,
            "market_execution_enabled": False,
            "strict_fallback": STRICT_DEFAULT_FALLBACK,
        },
        "summary": {
            "evidence_passed": evidence.passed,
            "evidence_description": evidence.description,
            "row_count": gate_frame.height,
            "production_promote_count": len(promoted),
            "promoted_source_model_names": promoted,
            "market_execution_enabled": market_execution_enabled,
            "fallback_strategy": STRICT_DEFAULT_FALLBACK,
        },
        "source_model_rows": rows,
    }


def build_dfl_schedule_value_learner_v2_trace_summary(
    learner_frame: pl.DataFrame,
    *,
    early_train_candidate_schedules_per_anchor: int = 9,
    max_candidate_schedules_per_anchor: int = 10,
) -> dict[str, Any]:
    """Build a compact trace of V2 source/tenant profile selections."""

    if learner_frame.is_empty():
        raise ValueError("dfl_schedule_value_learner_v2_frame must not be empty.")
    _require_columns(
        learner_frame,
        REQUIRED_SCHEDULE_VALUE_LEARNER_TRACE_COLUMNS,
        frame_name="dfl_schedule_value_learner_v2_frame",
    )
    if early_train_candidate_schedules_per_anchor < 1:
        raise ValueError("early_train_candidate_schedules_per_anchor must be positive.")
    if max_candidate_schedules_per_anchor < early_train_candidate_schedules_per_anchor:
        raise ValueError(
            "max_candidate_schedules_per_anchor must be greater than or equal to "
            "early_train_candidate_schedules_per_anchor."
        )

    rows = sorted(
        learner_frame.iter_rows(named=True),
        key=lambda row: (str(row["source_model_name"]), str(row["tenant_id"])),
    )
    _validate_learner_trace_flags(rows)
    tenant_source_rows = [
        _learner_trace_tenant_source_row(
            row,
            early_train_candidate_schedules_per_anchor=early_train_candidate_schedules_per_anchor,
            max_candidate_schedules_per_anchor=max_candidate_schedules_per_anchor,
        )
        for row in rows
    ]
    source_model_rows = _learner_trace_source_rows(
        tenant_source_rows,
        max_candidate_schedules_per_anchor=max_candidate_schedules_per_anchor,
    )
    return {
        "claim_boundary": {
            "offline_read_model_strategy_evidence_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "strict_fallback": STRICT_DEFAULT_FALLBACK,
        },
        "summary": {
            "row_count": len(tenant_source_rows),
            "tenant_count": len({row["tenant_id"] for row in tenant_source_rows}),
            "source_model_count": len(source_model_rows),
            "early_train_candidate_schedules_per_anchor": (
                early_train_candidate_schedules_per_anchor
            ),
            "max_candidate_schedules_per_anchor": max_candidate_schedules_per_anchor,
            "final_holdout_candidate_count_per_source_model": {
                row["source_model_name"]: row["final_holdout_candidate_count"]
                for row in source_model_rows
            },
        },
        "source_model_rows": source_model_rows,
        "tenant_source_rows": tenant_source_rows,
    }


def write_dfl_schedule_value_production_gate_registry(
    registry: dict[str, Any],
    *,
    output_root: Path,
    run_slug: str,
    attempt_manifest_path: Path | None = None,
    monitor_snapshot_path: Path | None = None,
    learner_trace_frame: pl.DataFrame | None = None,
) -> Path:
    """Write local JSON and Markdown registry artifacts."""

    export_dir = output_root / run_slug
    export_dir.mkdir(parents=True, exist_ok=True)
    registry_to_write = dict(registry)
    attached_artifacts: dict[str, str] = {}
    if attempt_manifest_path is not None:
        attached_artifacts["attempt_manifest"] = _copy_registry_artifact(
            attempt_manifest_path,
            export_dir=export_dir,
            artifact_name=ATTEMPT_MANIFEST_ARTIFACT_NAME,
        )
    if monitor_snapshot_path is not None:
        attached_artifacts["monitor_snapshot"] = _copy_registry_artifact(
            monitor_snapshot_path,
            export_dir=export_dir,
            artifact_name=MONITOR_SNAPSHOT_ARTIFACT_NAME,
        )
    if learner_trace_frame is not None:
        learner_trace_summary = build_dfl_schedule_value_learner_v2_trace_summary(
            learner_trace_frame
        )
        (export_dir / LEARNER_TRACE_SUMMARY_ARTIFACT_NAME).write_text(
            json.dumps(_jsonable(learner_trace_summary), indent=2, sort_keys=True),
            encoding="utf-8",
        )
        (export_dir / LEARNER_TRACE_MARKDOWN_ARTIFACT_NAME).write_text(
            _learner_trace_summary_markdown(learner_trace_summary),
            encoding="utf-8",
        )
        attached_artifacts["learner_trace_summary"] = (
            LEARNER_TRACE_SUMMARY_ARTIFACT_NAME
        )
        attached_artifacts["learner_trace_markdown"] = (
            LEARNER_TRACE_MARKDOWN_ARTIFACT_NAME
        )
    if attached_artifacts:
        registry_to_write["attached_artifacts"] = attached_artifacts
    (export_dir / "dfl_schedule_value_production_gate_registry.json").write_text(
        json.dumps(_jsonable(registry_to_write), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / "dfl_schedule_value_production_gate_registry.md").write_text(
        _production_gate_registry_markdown(registry_to_write),
        encoding="utf-8",
    )
    return export_dir


def _copy_registry_artifact(
    source_path: Path, *, export_dir: Path, artifact_name: str
) -> str:
    if not source_path.exists():
        raise FileNotFoundError(f"Registry attachment does not exist: {source_path}")
    if not source_path.is_file():
        raise ValueError(f"Registry attachment must be a file: {source_path}")
    destination = export_dir / artifact_name
    if source_path.resolve() != destination.resolve():
        copyfile(source_path, destination)
    return artifact_name


def _validate_learner_trace_flags(rows: list[dict[str, Any]]) -> None:
    if not all(bool(row["not_full_dfl"]) for row in rows):
        raise ValueError(
            "dfl_schedule_value_learner_v2_frame requires not_full_dfl=true."
        )
    if not all(bool(row["not_market_execution"]) for row in rows):
        raise ValueError(
            "dfl_schedule_value_learner_v2_frame requires not_market_execution=true."
        )


def _learner_trace_tenant_source_row(
    row: dict[str, Any],
    *,
    early_train_candidate_schedules_per_anchor: int,
    max_candidate_schedules_per_anchor: int,
) -> dict[str, Any]:
    train_anchor_count = _positive_int(
        row["train_anchor_count"], field_name="train_anchor_count"
    )
    final_holdout_anchor_count = _positive_int(
        row["final_holdout_anchor_count"],
        field_name="final_holdout_anchor_count",
    )
    final_holdout_tenant_anchor_count = _positive_int(
        row["final_holdout_tenant_anchor_count"],
        field_name="final_holdout_tenant_anchor_count",
    )
    return {
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "selected_weight_profile_name": str(row["selected_weight_profile_name"]),
        "train_anchor_count": train_anchor_count,
        "final_holdout_anchor_count": final_holdout_anchor_count,
        "final_holdout_tenant_anchor_count": final_holdout_tenant_anchor_count,
        "early_train_candidate_schedules_per_anchor": (
            early_train_candidate_schedules_per_anchor
        ),
        "max_candidate_schedules_per_anchor": max_candidate_schedules_per_anchor,
        "final_holdout_candidate_count": (
            final_holdout_anchor_count * max_candidate_schedules_per_anchor
        ),
        "selected_train_family_counts": _candidate_family_counts(
            row["selected_train_family_counts"],
            field_name="selected_train_family_counts",
        ),
        "selected_final_family_counts": _candidate_family_counts(
            row["selected_final_family_counts"],
            field_name="selected_final_family_counts",
        ),
    }


def _learner_trace_source_rows(
    tenant_source_rows: list[dict[str, Any]],
    *,
    max_candidate_schedules_per_anchor: int,
) -> list[dict[str, Any]]:
    source_model_names = sorted(
        {str(row["source_model_name"]) for row in tenant_source_rows}
    )
    source_rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        source_rows_for_model = [
            row
            for row in tenant_source_rows
            if str(row["source_model_name"]) == source_model_name
        ]
        final_holdout_tenant_anchor_count = max(
            int(row["final_holdout_tenant_anchor_count"])
            for row in source_rows_for_model
        )
        source_rows.append(
            {
                "source_model_name": source_model_name,
                "tenant_count": len(
                    {row["tenant_id"] for row in source_rows_for_model}
                ),
                "selected_weight_profile_names": sorted(
                    {
                        str(row["selected_weight_profile_name"])
                        for row in source_rows_for_model
                    }
                ),
                "final_holdout_tenant_anchor_count": final_holdout_tenant_anchor_count,
                "final_holdout_candidate_count": (
                    final_holdout_tenant_anchor_count
                    * max_candidate_schedules_per_anchor
                ),
                "selected_final_family_counts": _merge_candidate_family_counts(
                    [
                        row["selected_final_family_counts"]
                        for row in source_rows_for_model
                        if isinstance(row["selected_final_family_counts"], dict)
                    ]
                ),
            }
        )
    return source_rows


def _candidate_family_counts(value: Any, *, field_name: str) -> dict[str, int]:
    if isinstance(value, str):
        loaded = json.loads(value)
        if not isinstance(loaded, dict):
            raise ValueError(f"{field_name} must be a mapping.")
        value = loaded
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping.")
    return {
        str(key): 0 if count is None else _nonnegative_int(count, field_name=field_name)
        for key, count in sorted(value.items(), key=lambda item: str(item[0]))
    }


def _merge_candidate_family_counts(
    count_mappings: list[dict[str, int]],
) -> dict[str, int]:
    merged: dict[str, int] = {}
    for count_mapping in count_mappings:
        for family_name, count in count_mapping.items():
            merged[family_name] = merged.get(family_name, 0) + count
    return dict(sorted(merged.items()))


def _promotion_row(
    source_model_name: str,
    *,
    strict_rows: list[dict[str, Any]],
    robustness_rows: list[dict[str, Any]],
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_rolling_window_count: int,
    min_rolling_strict_pass_windows: int,
) -> dict[str, Any]:
    source_strict_rows = [
        row for row in strict_rows if str(row["source_model_name"]) == source_model_name
    ]
    source_robustness_rows = [
        row
        for row in robustness_rows
        if str(row["source_model_name"]) == source_model_name
    ]
    strict_reference_rows = [
        row for row in source_strict_rows if _selection_role(row) == "strict_reference"
    ]
    learner_model_name = schedule_value_learner_v2_model_name(source_model_name)
    selected_rows = [
        row
        for row in source_strict_rows
        if str(row["forecast_model_name"]) == learner_model_name
    ]
    strict_anchor_set = _tenant_anchor_set(strict_reference_rows)
    selected_anchor_set = _tenant_anchor_set(selected_rows)
    matching_anchor_coverage = (
        bool(strict_anchor_set) and strict_anchor_set == selected_anchor_set
    )
    latest_validation_tenant_anchor_count = (
        len(strict_anchor_set) if matching_anchor_coverage else 0
    )
    tenant_count = len({tenant_id for tenant_id, _ in strict_anchor_set})
    latest_strict_mean = _mean_regret(strict_reference_rows)
    latest_selected_mean = _mean_regret(selected_rows)
    latest_strict_median = _median_regret(strict_reference_rows)
    latest_selected_median = _median_regret(selected_rows)
    improvement_ratio = _improvement_ratio(latest_strict_mean, latest_selected_mean)
    latest_median_not_worse = latest_selected_median <= latest_strict_median
    latest_mean_passed = improvement_ratio >= min_mean_regret_improvement_ratio
    strict_evidence_valid = _strict_evidence_valid(source_strict_rows)

    rolling_window_count = len(
        {int(row["window_index"]) for row in source_robustness_rows}
    )
    rolling_strict_pass_window_count = sum(
        1
        for row in source_robustness_rows
        if bool(row["source_specific_strict_passed"])
    )
    rolling_development_pass_window_count = sum(
        1 for row in source_robustness_rows if bool(row["development_passed"])
    )
    latest_window_passed = any(
        int(row["window_index"]) == 1 and bool(row["source_specific_strict_passed"])
        for row in source_robustness_rows
    )
    robust_research_challenger = (
        any(bool(row["robust_research_challenger"]) for row in source_robustness_rows)
        and rolling_window_count >= min_rolling_window_count
        and rolling_strict_pass_window_count >= min_rolling_strict_pass_windows
        and latest_window_passed
        and _robustness_claims_valid(source_robustness_rows)
    )
    latest_source_signal = (
        strict_evidence_valid
        and matching_anchor_coverage
        and tenant_count >= min_tenant_count
        and latest_validation_tenant_anchor_count >= min_validation_tenant_anchor_count
        and latest_mean_passed
        and latest_median_not_worse
    )
    promotion_blocker = _promotion_blocker(
        strict_evidence_valid=strict_evidence_valid,
        matching_anchor_coverage=matching_anchor_coverage,
        tenant_count=tenant_count,
        min_tenant_count=min_tenant_count,
        latest_validation_tenant_anchor_count=latest_validation_tenant_anchor_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        latest_mean_passed=latest_mean_passed,
        latest_median_not_worse=latest_median_not_worse,
        rolling_window_count=rolling_window_count,
        min_rolling_window_count=min_rolling_window_count,
        rolling_strict_pass_window_count=rolling_strict_pass_window_count,
        min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
        latest_window_passed=latest_window_passed,
        robust_research_challenger=robust_research_challenger,
    )
    production_promote = promotion_blocker == "none"
    return {
        "source_model_name": source_model_name,
        "tenant_count": tenant_count,
        "latest_validation_tenant_anchor_count": latest_validation_tenant_anchor_count,
        "latest_strict_mean_regret_uah": latest_strict_mean,
        "latest_selected_mean_regret_uah": latest_selected_mean,
        "latest_strict_median_regret_uah": latest_strict_median,
        "latest_selected_median_regret_uah": latest_selected_median,
        "latest_mean_regret_improvement_ratio_vs_strict": improvement_ratio,
        "latest_median_not_worse": latest_median_not_worse,
        "latest_source_signal": latest_source_signal,
        "rolling_window_count": rolling_window_count,
        "rolling_strict_pass_window_count": rolling_strict_pass_window_count,
        "rolling_development_pass_window_count": rolling_development_pass_window_count,
        "robust_research_challenger": robust_research_challenger,
        "allowed_challenger": learner_model_name if production_promote else "",
        "fallback_strategy": STRICT_DEFAULT_FALLBACK,
        "promotion_blocker": promotion_blocker,
        "production_promote": production_promote,
        "market_execution_enabled": False,
        "claim_scope": DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE,
        "academic_scope": DFL_SCHEDULE_VALUE_PRODUCTION_GATE_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _promotion_blocker(
    *,
    strict_evidence_valid: bool,
    matching_anchor_coverage: bool,
    tenant_count: int,
    min_tenant_count: int,
    latest_validation_tenant_anchor_count: int,
    min_validation_tenant_anchor_count: int,
    latest_mean_passed: bool,
    latest_median_not_worse: bool,
    rolling_window_count: int,
    min_rolling_window_count: int,
    rolling_strict_pass_window_count: int,
    min_rolling_strict_pass_windows: int,
    latest_window_passed: bool,
    robust_research_challenger: bool,
) -> str:
    if not strict_evidence_valid:
        return "evidence_invalid"
    if not matching_anchor_coverage:
        return "anchor_coverage_mismatch"
    if tenant_count < min_tenant_count:
        return "tenant_undercoverage"
    if latest_validation_tenant_anchor_count < min_validation_tenant_anchor_count:
        return "validation_undercoverage"
    if not latest_median_not_worse:
        return "median_degraded"
    if not latest_mean_passed:
        return "mean_improvement_below_threshold"
    if rolling_window_count < min_rolling_window_count:
        return "rolling_undercoverage"
    if rolling_strict_pass_window_count < min_rolling_strict_pass_windows:
        return "rolling_not_robust"
    if not latest_window_passed:
        return "latest_window_not_robust"
    if not robust_research_challenger:
        return "robust_challenger_missing"
    return "none"


def _row_validation_failures(
    row: dict[str, Any],
    *,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_rolling_window_count: int,
) -> list[str]:
    failures: list[str] = []
    source_model_name = str(row["source_model_name"])
    if int(row["tenant_count"]) < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count is below {min_tenant_count}")
    if (
        int(row["latest_validation_tenant_anchor_count"])
        < min_validation_tenant_anchor_count
    ):
        failures.append(
            f"{source_model_name} latest validation tenant-anchor count is below "
            f"{min_validation_tenant_anchor_count}"
        )
    if int(row["rolling_window_count"]) < min_rolling_window_count:
        failures.append(
            f"{source_model_name} rolling window count is below {min_rolling_window_count}"
        )
    if str(row["claim_scope"]) != DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE:
        failures.append(f"{source_model_name} claim_scope is invalid")
    if not bool(row["not_full_dfl"]):
        failures.append(f"{source_model_name} requires not_full_dfl=true")
    if not bool(row["not_market_execution"]):
        failures.append(f"{source_model_name} requires not_market_execution=true")
    if bool(row["market_execution_enabled"]):
        failures.append(f"{source_model_name} requires market_execution_enabled=false")
    if bool(row["production_promote"]):
        if str(row["promotion_blocker"]) != "none":
            failures.append(
                f"{source_model_name} promoted row must have promotion_blocker=none"
            )
        if not str(row["allowed_challenger"]):
            failures.append(
                f"{source_model_name} promoted row must record allowed_challenger"
            )
    return failures


def _strict_evidence_valid(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    for row in rows:
        payload = _payload(row)
        if (
            str(row.get("data_quality_tier", payload.get("data_quality_tier", "")))
            != "thesis_grade"
        ):
            return False
        if (
            float(
                row.get(
                    "observed_coverage_ratio",
                    payload.get("observed_coverage_ratio", 0.0),
                )
            )
            < 1.0
        ):
            return False
        if (
            int(
                row.get(
                    "safety_violation_count", payload.get("safety_violation_count", 1)
                )
            )
            != 0
        ):
            return False
        if not bool(row.get("not_full_dfl", payload.get("not_full_dfl", False))):
            return False
        if not bool(
            row.get("not_market_execution", payload.get("not_market_execution", False))
        ):
            return False
    return True


def _robustness_claims_valid(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    return all(
        bool(row["not_full_dfl"]) and bool(row["not_market_execution"]) for row in rows
    )


def _source_summary(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_model_name": str(row["source_model_name"]),
        "latest_mean_regret_improvement_ratio_vs_strict": float(
            row["latest_mean_regret_improvement_ratio_vs_strict"]
        ),
        "rolling_strict_pass_window_count": int(
            row["rolling_strict_pass_window_count"]
        ),
        "robust_research_challenger": bool(row["robust_research_challenger"]),
        "production_promote": bool(row["production_promote"]),
        "promotion_blocker": str(row["promotion_blocker"]),
    }


def _registry_row(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "source_model_name": str(row["source_model_name"]),
        "latest_validation_tenant_anchor_count": int(
            row["latest_validation_tenant_anchor_count"]
        ),
        "latest_strict_mean_regret_uah": float(row["latest_strict_mean_regret_uah"]),
        "latest_selected_mean_regret_uah": float(
            row["latest_selected_mean_regret_uah"]
        ),
        "latest_mean_regret_improvement_ratio_vs_strict": float(
            row["latest_mean_regret_improvement_ratio_vs_strict"]
        ),
        "latest_strict_median_regret_uah": float(
            row["latest_strict_median_regret_uah"]
        ),
        "latest_selected_median_regret_uah": float(
            row["latest_selected_median_regret_uah"]
        ),
        "rolling_window_count": int(row["rolling_window_count"]),
        "rolling_strict_pass_window_count": int(
            row["rolling_strict_pass_window_count"]
        ),
        "robust_research_challenger": bool(row["robust_research_challenger"]),
        "production_promote": bool(row["production_promote"]),
        "promotion_blocker": str(row["promotion_blocker"]),
        "fallback_strategy": str(row["fallback_strategy"]),
        "allowed_challenger": str(row["allowed_challenger"]),
        "market_execution_enabled": bool(row["market_execution_enabled"]),
    }


def _production_gate_registry_markdown(registry: dict[str, Any]) -> str:
    lines = [
        "# DFL Schedule/Value Production Gate Registry",
        "",
        f"Run slug: `{registry['run_slug']}`",
        f"Dagster run id: `{registry.get('dagster_run_id') or 'not recorded'}`",
        "",
        "## Claim Boundary",
        "",
        "- Offline/read-model strategy evidence only.",
        "- `not_full_dfl=true`",
        "- `not_market_execution=true`",
        "- `market_execution_enabled=false`; market execution remains disabled.",
        f"- Fallback strategy: `{STRICT_DEFAULT_FALLBACK}`",
        "",
        "## Summary",
        "",
        f"- Evidence passed: `{registry['summary']['evidence_passed']}`",
        f"- Production promote count: `{registry['summary']['production_promote_count']}`",
        f"- Promoted source models: `{', '.join(registry['summary']['promoted_source_model_names']) or 'none'}`",
        "",
        "| Source model | Latest anchors | Strict mean | Selected mean | Improvement | Strict median | Selected median | Rolling strict passes | Production promote | Allowed challenger |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for row in registry["source_model_rows"]:
        lines.append(
            "| {source} | {anchors} | {strict_mean:.3f} | {selected_mean:.3f} | "
            "{improvement:.2%} | {strict_median:.3f} | {selected_median:.3f} | "
            "{rolling_passes} / {rolling_windows} | {promote} | `{challenger}` |".format(
                source=_display_source_model(str(row["source_model_name"])),
                anchors=row["latest_validation_tenant_anchor_count"],
                strict_mean=row["latest_strict_mean_regret_uah"],
                selected_mean=row["latest_selected_mean_regret_uah"],
                improvement=row["latest_mean_regret_improvement_ratio_vs_strict"],
                strict_median=row["latest_strict_median_regret_uah"],
                selected_median=row["latest_selected_median_regret_uah"],
                rolling_passes=row["rolling_strict_pass_window_count"],
                rolling_windows=row["rolling_window_count"],
                promote=str(row["production_promote"]).lower(),
                challenger=row["allowed_challenger"] or "none",
            )
        )
    attached_artifacts = registry.get("attached_artifacts", {})
    if isinstance(attached_artifacts, dict) and attached_artifacts:
        lines.extend(["", "## Attached Evidence", ""])
        attempt_manifest = attached_artifacts.get("attempt_manifest")
        if attempt_manifest:
            lines.append(f"- Attempt manifest: `{attempt_manifest}`")
        monitor_snapshot = attached_artifacts.get("monitor_snapshot")
        if monitor_snapshot:
            lines.append(f"- Monitor snapshot: `{monitor_snapshot}`")
        learner_trace_summary = attached_artifacts.get("learner_trace_summary")
        if learner_trace_summary:
            lines.append(f"- Learner V2 trace summary: `{learner_trace_summary}`")
        learner_trace_markdown = attached_artifacts.get("learner_trace_markdown")
        if learner_trace_markdown:
            lines.append(
                f"- Learner V2 trace summary markdown: `{learner_trace_markdown}`"
            )
    lines.extend(
        [
            "",
            "## Decision",
            "",
            "The promoted rows are valid only inside the offline evidence stack. "
            "`strict_similar_day` remains the fallback for undercovered, "
            "out-of-distribution, failed-source, and live-execution contexts.",
            "",
        ]
    )
    return "\n".join(lines)


def _display_source_model(source_model_name: str) -> str:
    if source_model_name == "nbeatsx_silver_v0":
        return "NBEATSx (`nbeatsx_silver_v0`)"
    if source_model_name == "tft_silver_v0":
        return "TFT (`tft_silver_v0`)"
    if source_model_name.startswith("nbeatsx_official"):
        return f"Official NBEATSx (`{source_model_name}`)"
    if source_model_name.startswith("tft_official"):
        return f"Official TFT (`{source_model_name}`)"
    if source_model_name.startswith("nbeatsx"):
        return f"NBEATSx (`{source_model_name}`)"
    if source_model_name.startswith("tft"):
        return f"TFT (`{source_model_name}`)"
    return f"`{source_model_name}`"


def _learner_trace_summary_markdown(summary: dict[str, Any]) -> str:
    lines = [
        "# Schedule/Value Learner V2 Trace Summary",
        "",
        "This attachment records the source/tenant weight-profile selections used by the "
        "current Offline Strategy Promotion evidence packet. It is trace evidence only; "
        "market execution remains disabled.",
        "",
        "## Candidate Library Cardinality",
        "",
        f"- Early train anchors: `{summary['summary']['early_train_candidate_schedules_per_anchor']}` candidate schedules per anchor.",
        f"- After residual candidate availability: `{summary['summary']['max_candidate_schedules_per_anchor']}` candidate schedules per anchor.",
        "- Latest final holdout: `900` candidate schedules per source model when five tenants "
        "and 18 final anchors are present.",
        "",
        "## Source Summary",
        "",
        "| Source model | Tenants | Weight profiles | Final holdout schedules | Final selected family counts |",
        "|---|---:|---|---:|---|",
    ]
    for row in summary["source_model_rows"]:
        lines.append(
            "| {source} | {tenants} | {profiles} | {candidate_count} | {family_counts} |".format(
                source=str(row["source_model_name"]),
                tenants=int(row["tenant_count"]),
                profiles=", ".join(row["selected_weight_profile_names"]),
                candidate_count=int(row["final_holdout_candidate_count"]),
                family_counts=_inline_counts(row["selected_final_family_counts"]),
            )
        )
    lines.extend(
        [
            "",
            "## Tenant / Source Selections",
            "",
            "| Tenant | Source model | Selected weight profile | Final holdout schedules | Final selected family counts |",
            "|---|---|---|---:|---|",
        ]
    )
    for row in summary["tenant_source_rows"]:
        lines.append(
            "| {tenant} | {source} | {profile} | {candidate_count} | {family_counts} |".format(
                tenant=str(row["tenant_id"]),
                source=str(row["source_model_name"]),
                profile=str(row["selected_weight_profile_name"]),
                candidate_count=int(row["final_holdout_candidate_count"]),
                family_counts=_inline_counts(row["selected_final_family_counts"]),
            )
        )
    return "\n".join(lines)


def _inline_counts(counts: dict[str, int]) -> str:
    return ", ".join(f"{family}: {count}" for family, count in counts.items()) or "none"


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [_jsonable(item) for item in value]
    return value


def _positive_int(value: Any, *, field_name: str) -> int:
    parsed = _nonnegative_int(value, field_name=field_name)
    if parsed < 1:
        raise ValueError(f"{field_name} must be positive.")
    return parsed


def _nonnegative_int(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field_name} must be an integer.")
    parsed = int(value)
    if parsed < 0:
        raise ValueError(f"{field_name} must be nonnegative.")
    return parsed


def _validate_config(
    *,
    source_model_names: tuple[str, ...],
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_rolling_window_count: int,
    min_rolling_strict_pass_windows: int,
) -> None:
    if not source_model_names:
        raise ValueError("source_model_names must contain at least one source model.")
    if min_tenant_count < 1:
        raise ValueError("min_tenant_count must be positive.")
    if min_validation_tenant_anchor_count < 1:
        raise ValueError("min_validation_tenant_anchor_count must be positive.")
    if min_mean_regret_improvement_ratio < 0:
        raise ValueError("min_mean_regret_improvement_ratio must be non-negative.")
    if min_rolling_window_count < 1:
        raise ValueError("min_rolling_window_count must be positive.")
    if min_rolling_strict_pass_windows < 1:
        raise ValueError("min_rolling_strict_pass_windows must be positive.")
    if min_rolling_strict_pass_windows > min_rolling_window_count:
        raise ValueError(
            "min_rolling_strict_pass_windows cannot exceed min_rolling_window_count."
        )


def _require_columns(
    frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str
) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(
            f"{frame_name} is missing required columns: {', '.join(missing)}"
        )


def _missing_column_failures(
    frame: pl.DataFrame, required_columns: frozenset[str]
) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _selection_role(row: dict[str, Any]) -> str:
    if row.get("selection_role"):
        return str(row["selection_role"])
    payload = _payload(row)
    return str(payload.get("selection_role", payload.get("selector_row_role", "")))


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    value = row.get("evaluation_payload", {})
    return value if isinstance(value, dict) else {}


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, datetime]]:
    return {
        (
            str(row["tenant_id"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        for row in rows
    }


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return mean(regrets) if regrets else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return median(regrets) if regrets else 0.0


def _improvement_ratio(control_value: float, candidate_value: float) -> float:
    return (
        (control_value - candidate_value) / abs(control_value)
        if abs(control_value) > 1e-9
        else 0.0
    )


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(f"{field_name} must be an ISO datetime.") from exc
    raise ValueError(f"{field_name} must be a datetime.")


__all__ = [
    "DFL_SCHEDULE_VALUE_PRODUCTION_GATE_CLAIM_SCOPE",
    "build_dfl_schedule_value_learner_v2_trace_summary",
    "build_dfl_schedule_value_production_gate_frame",
    "build_dfl_schedule_value_production_gate_registry",
    "evaluate_dfl_schedule_value_production_gate",
    "validate_dfl_schedule_value_production_gate_evidence",
    "write_dfl_schedule_value_production_gate_registry",
]
