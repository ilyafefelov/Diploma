"""Source-specific research challenger gate for strict-control DFL evidence."""

from __future__ import annotations

from datetime import datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.residual_schedule_value import (
    DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
)
from smart_arbitrage.dfl.strict_failure_feature_selector import (
    DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_PREFIX,
)
from smart_arbitrage.dfl.strict_failure_features import (
    DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE,
    REQUIRED_FEATURE_AUDIT_COLUMNS,
)
from smart_arbitrage.dfl.strict_failure_robustness import (
    DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE,
    DEFAULT_MIN_ROBUST_PASSING_WINDOWS,
    DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
    REQUIRED_ROBUSTNESS_COLUMNS,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER_CLAIM_SCOPE: Final[str] = (
    "dfl_source_specific_research_challenger_not_full_dfl"
)
DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER_ACADEMIC_SCOPE: Final[str] = (
    "Source-specific research challenger gate that combines latest residual/DT fallback "
    "strict evidence, feature-aware selector evidence, rolling robustness, and prior "
    "feature-audit context. It is not full DFL, not Decision Transformer control, and "
    "not market execution."
)

REQUIRED_STRICT_EVALUATION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "anchor_timestamp",
        "generated_at",
        "regret_uah",
        "evaluation_payload",
    }
)
REQUIRED_SOURCE_SPECIFIC_CHALLENGER_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "tenant_count",
        "latest_validation_tenant_anchor_count",
        "latest_strict_mean_regret_uah",
        "latest_fallback_mean_regret_uah",
        "latest_strict_median_regret_uah",
        "latest_fallback_median_regret_uah",
        "latest_mean_regret_improvement_ratio_vs_strict",
        "latest_median_not_worse",
        "latest_source_signal",
        "rolling_window_count",
        "rolling_development_pass_window_count",
        "rolling_strict_pass_window_count",
        "rolling_development_pass",
        "robust_research_challenger",
        "production_promote",
        "dominant_failure_cluster",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "evidence_failure_messages",
    }
)


def build_dfl_source_specific_research_challenger_frame(
    residual_dt_fallback_strict_frame: pl.DataFrame,
    feature_aware_selector_strict_frame: pl.DataFrame,
    strict_failure_selector_robustness_frame: pl.DataFrame,
    strict_failure_feature_audit_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    min_rolling_strict_pass_windows: int = DEFAULT_MIN_ROBUST_PASSING_WINDOWS,
    min_rolling_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
) -> pl.DataFrame:
    """Combine source-specific DFL evidence into one gate row per source model."""

    _validate_config(
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
        min_rolling_window_count=min_rolling_window_count,
    )
    _require_columns(
        residual_dt_fallback_strict_frame,
        REQUIRED_STRICT_EVALUATION_COLUMNS,
        frame_name="dfl_residual_dt_fallback_strict_lp_benchmark_frame",
    )
    _require_columns(
        feature_aware_selector_strict_frame,
        REQUIRED_STRICT_EVALUATION_COLUMNS,
        frame_name="dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame",
    )
    _require_columns(
        strict_failure_selector_robustness_frame,
        REQUIRED_ROBUSTNESS_COLUMNS,
        frame_name="dfl_strict_failure_selector_robustness_frame",
    )
    _require_columns(
        strict_failure_feature_audit_frame,
        REQUIRED_FEATURE_AUDIT_COLUMNS,
        frame_name="dfl_strict_failure_feature_audit_frame",
    )

    fallback_rows = list(residual_dt_fallback_strict_frame.iter_rows(named=True))
    feature_rows = list(feature_aware_selector_strict_frame.iter_rows(named=True))
    robustness_rows = list(strict_failure_selector_robustness_frame.iter_rows(named=True))
    audit_rows = list(strict_failure_feature_audit_frame.iter_rows(named=True))

    rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        latest_summary = _latest_fallback_summary(
            fallback_rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        feature_summary = _feature_aware_summary(
            feature_rows,
            source_model_name=source_model_name,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        rolling_summary = _rolling_summary(
            robustness_rows,
            source_model_name=source_model_name,
            min_rolling_window_count=min_rolling_window_count,
        )
        audit_summary = _audit_summary(audit_rows, source_model_name=source_model_name)
        failures = [
            *latest_summary.pop("failure_messages"),
            *feature_summary.pop("failure_messages"),
            *rolling_summary.pop("failure_messages"),
            *audit_summary.pop("failure_messages"),
        ]
        latest_source_signal = bool(latest_summary["latest_source_signal"])
        rolling_strict_pass_count = int(rolling_summary["rolling_strict_pass_window_count"])
        robust_research_challenger = (
            latest_source_signal and rolling_strict_pass_count >= min_rolling_strict_pass_windows
        )
        rows.append(
            {
                "source_model_name": source_model_name,
                **latest_summary,
                **feature_summary,
                **rolling_summary,
                **audit_summary,
                "latest_source_signal": latest_source_signal,
                "robust_research_challenger": robust_research_challenger,
                "production_promote": False,
                "gate_label": _gate_label(
                    latest_source_signal=latest_source_signal,
                    rolling_development_pass=bool(rolling_summary["rolling_development_pass"]),
                    robust_research_challenger=robust_research_challenger,
                ),
                "claim_scope": DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER_CLAIM_SCOPE,
                "academic_scope": DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER_ACADEMIC_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "evidence_failure_messages": failures,
                "evidence_failure_count": len(failures),
            }
        )
    return pl.DataFrame(rows).sort("source_model_name")


def validate_dfl_source_specific_research_challenger_evidence(
    challenger_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_rolling_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
) -> EvidenceCheckOutcome:
    """Validate source-specific gate evidence without requiring promotion."""

    failures = _missing_column_failures(
        challenger_frame,
        REQUIRED_SOURCE_SPECIFIC_CHALLENGER_COLUMNS,
    )
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": challenger_frame.height})
    rows = list(challenger_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "source-specific challenger evidence has no rows", {"row_count": 0})

    expected_source_names = source_model_names or tuple(sorted({str(row["source_model_name"]) for row in rows}))
    rows_by_source = {str(row["source_model_name"]): row for row in rows}
    summaries: list[dict[str, Any]] = []
    for source_model_name in expected_source_names:
        row = rows_by_source.get(source_model_name)
        if row is None:
            failures.append(f"{source_model_name} source-specific challenger row is missing")
            continue
        row_failures = _row_validation_failures(
            row,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_rolling_window_count=min_rolling_window_count,
        )
        failures.extend(row_failures)
        summaries.append(_model_summary(row, row_failures))

    robust_sources = [
        str(summary["source_model_name"])
        for summary in summaries
        if bool(summary["robust_research_challenger"])
    ]
    latest_signal_sources = [
        str(summary["source_model_name"])
        for summary in summaries
        if bool(summary["latest_source_signal"])
    ]
    metadata = {
        "row_count": challenger_frame.height,
        "source_model_count": len(expected_source_names),
        "source_model_names": list(expected_source_names),
        "latest_signal_source_model_names": latest_signal_sources,
        "robust_source_model_names": robust_sources,
        "production_promote": False,
        "model_summaries": summaries,
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Source-specific challenger evidence has valid source, coverage, provenance, and rolling-window context."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_source_specific_research_challenger_gate(
    challenger_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> PromotionGateResult:
    """Return the research-challenger status while keeping production promotion blocked."""

    evidence = validate_dfl_source_specific_research_challenger_evidence(
        challenger_frame,
        source_model_names=source_model_names,
    )
    if not evidence.passed:
        return PromotionGateResult(False, "blocked", evidence.description, evidence.metadata)
    robust_sources = list(evidence.metadata["robust_source_model_names"])
    latest_signal_sources = list(evidence.metadata["latest_signal_source_model_names"])
    if robust_sources:
        return PromotionGateResult(
            False,
            "robust_research_challenger_production_blocked",
            "At least one source is a robust research challenger, but production promotion remains blocked.",
            evidence.metadata,
        )
    if latest_signal_sources:
        return PromotionGateResult(
            False,
            "latest_signal_not_robust",
            "At least one source beats strict on the latest holdout, but rolling robustness is not sufficient.",
            evidence.metadata,
        )
    return PromotionGateResult(
        False,
        "blocked",
        f"No source-specific challenger clears the latest {CONTROL_MODEL_NAME} signal.",
        evidence.metadata,
    )


def _validate_config(
    *,
    source_model_names: tuple[str, ...],
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_rolling_strict_pass_windows: int,
    min_rolling_window_count: int,
) -> None:
    if not source_model_names:
        raise ValueError("source_model_names must contain at least one source model.")
    if min_tenant_count < 1:
        raise ValueError("min_tenant_count must be at least 1.")
    if min_validation_tenant_anchor_count < 1:
        raise ValueError("min_validation_tenant_anchor_count must be at least 1.")
    if min_mean_regret_improvement_ratio < 0.0:
        raise ValueError("min_mean_regret_improvement_ratio must be non-negative.")
    if min_rolling_strict_pass_windows < 1:
        raise ValueError("min_rolling_strict_pass_windows must be at least 1.")
    if min_rolling_window_count < 1:
        raise ValueError("min_rolling_window_count must be at least 1.")


def _latest_fallback_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> dict[str, Any]:
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [row for row in source_rows if _row_role(row) == "strict_reference"]
    fallback_rows = [row for row in source_rows if _row_role(row) == "fallback_strategy"]
    strict_anchors = _tenant_anchor_set(strict_rows)
    fallback_anchors = _tenant_anchor_set(fallback_rows)
    tenant_count = len({tenant_id for tenant_id, _ in fallback_anchors})
    validation_count = len(fallback_anchors)
    failures: list[str] = []
    if not source_rows:
        failures.append(f"{source_model_name} residual/DT fallback rows are missing")
    if strict_anchors != fallback_anchors:
        failures.append(f"{source_model_name} strict/fallback rows must cover matching tenant-anchor sets")
    if tenant_count < min_tenant_count:
        failures.append(
            f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}"
        )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(_provenance_failures([*strict_rows, *fallback_rows], frame_label="fallback"))
    strict_mean = _mean_regret(strict_rows)
    fallback_mean = _mean_regret(fallback_rows)
    strict_median = _median_regret(strict_rows)
    fallback_median = _median_regret(fallback_rows)
    improvement = _improvement_ratio(strict_mean, fallback_mean)
    latest_signal = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement >= min_mean_regret_improvement_ratio
        and fallback_median <= strict_median
        and not failures
    )
    return {
        "tenant_count": tenant_count,
        "latest_validation_tenant_anchor_count": validation_count,
        "latest_strict_mean_regret_uah": strict_mean,
        "latest_fallback_mean_regret_uah": fallback_mean,
        "latest_strict_median_regret_uah": strict_median,
        "latest_fallback_median_regret_uah": fallback_median,
        "latest_mean_regret_improvement_ratio_vs_strict": improvement,
        "latest_median_not_worse": fallback_median <= strict_median if fallback_rows else False,
        "latest_signal_claim_scope": DFL_RESIDUAL_DT_FALLBACK_STRICT_CLAIM_SCOPE,
        "latest_source_signal": latest_signal,
        "failure_messages": failures,
    }


def _feature_aware_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> dict[str, Any]:
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [row for row in source_rows if _is_strict_reference(row)]
    raw_rows = [row for row in source_rows if _is_raw_reference(row, source_model_name=source_model_name)]
    selector_rows = [row for row in source_rows if _is_feature_selector(row, source_model_name=source_model_name)]
    failures: list[str] = []
    if not source_rows:
        failures.append(f"{source_model_name} feature-aware selector rows are missing")
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selector_anchors = _tenant_anchor_set(selector_rows)
    if strict_anchors and selector_anchors and (strict_anchors != selector_anchors):
        failures.append(f"{source_model_name} feature-aware strict/selector anchors must match")
    if selector_anchors and raw_anchors and selector_anchors != raw_anchors:
        failures.append(f"{source_model_name} feature-aware raw/selector anchors must match")
    failures.extend(_provenance_failures([*strict_rows, *raw_rows, *selector_rows], frame_label="feature-aware"))
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selector_mean = _mean_regret(selector_rows)
    strict_median = _median_regret(strict_rows)
    selector_median = _median_regret(selector_rows)
    improvement_vs_strict = _improvement_ratio(strict_mean, selector_mean)
    improvement_vs_raw = _improvement_ratio(raw_mean, selector_mean)
    validation_count = len(selector_anchors)
    return {
        "feature_aware_validation_tenant_anchor_count": validation_count,
        "feature_aware_selector_mean_regret_uah": selector_mean,
        "feature_aware_raw_mean_regret_uah": raw_mean,
        "feature_aware_mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "feature_aware_mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "feature_aware_latest_signal": (
            validation_count >= min_validation_tenant_anchor_count
            and improvement_vs_strict >= min_mean_regret_improvement_ratio
            and selector_median <= strict_median
            and not failures
        ),
        "feature_aware_development_pass": validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
        and not failures,
        "failure_messages": failures,
    }


def _rolling_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_rolling_window_count: int,
) -> dict[str, Any]:
    source_rows = [row for row in rows if str(row["source_model_name"]) == source_model_name]
    failures: list[str] = []
    if not source_rows:
        failures.append(f"{source_model_name} rolling robustness rows are missing")
    window_indices = sorted({int(row["window_index"]) for row in source_rows})
    if len(window_indices) < min_rolling_window_count:
        failures.append(
            f"{source_model_name} rolling window count must be at least "
            f"{min_rolling_window_count}; observed {len(window_indices)}"
        )
    expected_prefix = list(range(1, min_rolling_window_count + 1))
    if window_indices[:min_rolling_window_count] != expected_prefix:
        failures.append(f"{source_model_name} rolling windows must include latest-first 1..{min_rolling_window_count}")
    failures.extend(_robustness_claim_failures(source_rows))
    development_count = sum(1 for row in source_rows if bool(row["development_passed"]))
    strict_count = sum(1 for row in source_rows if bool(row["source_specific_strict_passed"]))
    latest_window_strict_pass = any(
        bool(row["source_specific_strict_passed"])
        for row in source_rows
        if int(row["window_index"]) == 1
    )
    return {
        "rolling_window_count": len(window_indices),
        "rolling_development_pass_window_count": development_count,
        "rolling_strict_pass_window_count": strict_count,
        "rolling_latest_window_strict_pass": latest_window_strict_pass,
        "rolling_development_pass": development_count >= min_rolling_window_count and not failures,
        "rolling_failure_count": len(failures),
        "failure_messages": failures,
    }


def _audit_summary(rows: list[dict[str, Any]], *, source_model_name: str) -> dict[str, Any]:
    source_rows = [row for row in rows if str(row["source_model_name"]) == source_model_name]
    failures: list[str] = []
    if not source_rows:
        failures.append(f"{source_model_name} feature-audit rows are missing")
    failures.extend(_feature_audit_claim_failures(source_rows))
    cluster_counts = _value_counts(str(row["failure_cluster"]) for row in source_rows)
    dominant_cluster = next(iter(cluster_counts), "unknown")
    return {
        "feature_audit_row_count": len(source_rows),
        "dominant_failure_cluster": dominant_cluster,
        "failure_cluster_counts": cluster_counts,
        "failure_messages": failures,
    }


def _row_validation_failures(
    row: dict[str, Any],
    *,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_rolling_window_count: int,
) -> list[str]:
    failures = list(_string_list(row["evidence_failure_messages"]))
    source_model_name = str(row["source_model_name"])
    if int(row["tenant_count"]) < min_tenant_count:
        failures.append(
            f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {row['tenant_count']}"
        )
    if int(row["latest_validation_tenant_anchor_count"]) < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {row['latest_validation_tenant_anchor_count']}"
        )
    if int(row["rolling_window_count"]) < min_rolling_window_count:
        failures.append(
            f"{source_model_name} rolling window count must be at least "
            f"{min_rolling_window_count}; observed {row['rolling_window_count']}"
        )
    if str(row["claim_scope"]) != DFL_SOURCE_SPECIFIC_RESEARCH_CHALLENGER_CLAIM_SCOPE:
        failures.append("source-specific challenger claim_scope must remain research-only")
    if not bool(row["not_full_dfl"]):
        failures.append("source-specific challenger rows must keep not_full_dfl=true")
    if not bool(row["not_market_execution"]):
        failures.append("source-specific challenger rows must keep not_market_execution=true")
    if bool(row["production_promote"]):
        failures.append("source-specific challenger production_promote must remain false")
    return failures


def _model_summary(row: dict[str, Any], row_failures: list[str]) -> dict[str, Any]:
    return {
        "source_model_name": str(row["source_model_name"]),
        "tenant_count": int(row["tenant_count"]),
        "latest_validation_tenant_anchor_count": int(row["latest_validation_tenant_anchor_count"]),
        "latest_source_signal": bool(row["latest_source_signal"]),
        "rolling_development_pass_window_count": int(row["rolling_development_pass_window_count"]),
        "rolling_strict_pass_window_count": int(row["rolling_strict_pass_window_count"]),
        "robust_research_challenger": bool(row["robust_research_challenger"]),
        "production_promote": bool(row["production_promote"]),
        "gate_label": str(row["gate_label"]),
        "dominant_failure_cluster": str(row["dominant_failure_cluster"]),
        "failure_count": len(row_failures),
    }


def _gate_label(
    *,
    latest_source_signal: bool,
    rolling_development_pass: bool,
    robust_research_challenger: bool,
) -> str:
    if robust_research_challenger:
        return "robust_research_challenger"
    if latest_source_signal:
        return "latest_signal_not_robust"
    if rolling_development_pass:
        return "rolling_development_only"
    return "blocked"


def _is_strict_reference(row: dict[str, Any]) -> bool:
    return str(row["forecast_model_name"]) == CONTROL_MODEL_NAME or _row_role(row) == "strict_reference"


def _is_raw_reference(row: dict[str, Any], *, source_model_name: str) -> bool:
    return str(row["forecast_model_name"]) == source_model_name or _row_role(row) == "raw_reference"


def _is_feature_selector(row: dict[str, Any], *, source_model_name: str) -> bool:
    selector_model_name = f"{DFL_FEATURE_AWARE_STRICT_FAILURE_SELECTOR_PREFIX}{source_model_name}"
    return str(row["forecast_model_name"]) == selector_model_name or _row_role(row) == "selector"


def _provenance_failures(rows: list[dict[str, Any]], *, frame_label: str) -> list[str]:
    failures: list[str] = []
    payloads = [_payload(row) for row in rows]
    if not rows:
        return failures
    if any(str(payload.get("data_quality_tier", "demo_grade")) != "thesis_grade" for payload in payloads):
        failures.append(f"{frame_label} evidence requires thesis_grade rows")
    if any(float(payload.get("observed_coverage_ratio", 0.0)) < 1.0 for payload in payloads):
        failures.append(f"{frame_label} evidence requires observed coverage ratio of 1.0")
    safety_violation_count = sum(int(payload.get("safety_violation_count", 0)) for payload in payloads)
    if safety_violation_count:
        failures.append(
            f"{frame_label} evidence requires zero safety violations; observed {safety_violation_count}"
        )
    if any(not bool(payload.get("not_full_dfl", True)) for payload in payloads):
        failures.append(f"{frame_label} evidence must keep not_full_dfl=true")
    if any(not bool(payload.get("not_market_execution", True)) for payload in payloads):
        failures.append(f"{frame_label} evidence must keep not_market_execution=true")
    return failures


def _robustness_claim_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    if any(str(row["claim_scope"]) != DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE for row in rows):
        failures.append("rolling robustness rows must keep expected claim_scope")
    if any(not bool(row["not_full_dfl"]) for row in rows):
        failures.append("rolling robustness rows must keep not_full_dfl=true")
    if any(not bool(row["not_market_execution"]) for row in rows):
        failures.append("rolling robustness rows must keep not_market_execution=true")
    if any(bool(row["production_promote"]) for row in rows):
        failures.append("rolling robustness rows must keep production_promote=false")
    return failures


def _feature_audit_claim_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    if any(str(row["claim_scope"]) != DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE for row in rows):
        failures.append("feature-audit rows must keep expected claim_scope")
    if any(not bool(row["not_full_dfl"]) for row in rows):
        failures.append("feature-audit rows must keep not_full_dfl=true")
    if any(not bool(row["not_market_execution"]) for row in rows):
        failures.append("feature-audit rows must keep not_market_execution=true")
    return failures


def _value_counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        key = str(value)
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items(), key=lambda item: (-item[1], item[0])))


def _source_model_name(row: dict[str, Any]) -> str:
    if row.get("source_model_name"):
        return str(row["source_model_name"])
    payload = _payload(row)
    return str(payload.get("source_forecast_model_name", ""))


def _row_role(row: dict[str, Any]) -> str:
    if row.get("selection_role"):
        return str(row["selection_role"])
    payload = _payload(row)
    if payload.get("selector_row_role"):
        return str(payload["selector_row_role"])
    return str(payload.get("selection_role", ""))


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
    return (control_value - candidate_value) / abs(control_value) if abs(control_value) > 1e-9 else 0.0


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return payload if isinstance(payload, dict) else {}


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        return datetime.fromisoformat(value)
    raise ValueError(f"{field_name} must be a datetime or ISO datetime string")


def _string_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    return [str(value)]


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"source-specific challenger evidence is missing required columns: {missing}"] if missing else []
