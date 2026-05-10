"""Offline production-promotion gate for source-specific DFL evidence."""

from __future__ import annotations

from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.data_expansion import CONTROL_MODEL_NAME
from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.source_specific_challenger import (
    REQUIRED_SOURCE_SPECIFIC_CHALLENGER_COLUMNS,
)
from smart_arbitrage.dfl.strict_failure_features import (
    REQUIRED_FEATURE_AUDIT_COLUMNS,
)
from smart_arbitrage.dfl.strict_failure_robustness import (
    DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE,
    DEFAULT_MIN_ROBUST_PASSING_WINDOWS,
    DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
    REQUIRED_ROBUSTNESS_COLUMNS,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_PRODUCTION_PROMOTION_GATE_CLAIM_SCOPE: Final[str] = (
    "dfl_production_promotion_gate_offline_strategy_not_market_execution"
)
DFL_PRODUCTION_PROMOTION_GATE_ACADEMIC_SCOPE: Final[str] = (
    "Offline/read-model production-promotion gate for source-specific DFL/TFT "
    "challengers. It can promote a strategy only inside research evidence and "
    "keeps market execution disabled."
)
STRICT_STABLE_REGIME: Final[str] = "strict_stable_region"

REQUIRED_COVERAGE_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "eligible_anchor_count",
        "data_quality_tier",
        "price_observed_coverage_ratio",
        "weather_observed_coverage_ratio",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_PRODUCTION_PROMOTION_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "regime_label",
        "tenant_count",
        "latest_validation_tenant_anchor_count",
        "latest_source_signal",
        "latest_mean_regret_improvement_ratio_vs_strict",
        "latest_median_not_worse",
        "rolling_window_count",
        "rolling_strict_pass_window_count",
        "rolling_development_pass_window_count",
        "regime_window_count",
        "regime_validation_anchor_count",
        "coverage_min_eligible_anchor_count",
        "coverage_expansion_available",
        "fallback_strategy",
        "promotion_blocker",
        "production_promote",
        "market_execution_enabled",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_REGIME_GATED_V2_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "anchor_timestamp",
        "regret_uah",
        "evaluation_payload",
    }
)


def build_dfl_production_promotion_gate_frame(
    source_specific_challenger_frame: pl.DataFrame,
    strict_failure_selector_robustness_frame: pl.DataFrame,
    strict_failure_feature_audit_frame: pl.DataFrame,
    dfl_data_coverage_audit_frame: pl.DataFrame,
    regime_gated_tft_selector_v2_strict_frame: pl.DataFrame | None = None,
    *,
    source_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    min_rolling_strict_pass_windows: int = DEFAULT_MIN_ROBUST_PASSING_WINDOWS,
    min_rolling_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
    backfill_target_anchor_count_per_tenant: int = 180,
) -> pl.DataFrame:
    """Build one offline promotion row per source model and audited regime."""

    _validate_config(
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
        min_rolling_window_count=min_rolling_window_count,
        backfill_target_anchor_count_per_tenant=backfill_target_anchor_count_per_tenant,
    )
    _require_columns(
        source_specific_challenger_frame,
        REQUIRED_SOURCE_SPECIFIC_CHALLENGER_COLUMNS,
        frame_name="dfl_source_specific_research_challenger_frame",
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
    _require_columns(
        dfl_data_coverage_audit_frame,
        REQUIRED_COVERAGE_AUDIT_COLUMNS,
        frame_name="dfl_data_coverage_audit_frame",
    )

    source_rows = {
        str(row["source_model_name"]): row
        for row in source_specific_challenger_frame.iter_rows(named=True)
    }
    robustness_rows = list(strict_failure_selector_robustness_frame.iter_rows(named=True))
    audit_rows = list(strict_failure_feature_audit_frame.iter_rows(named=True))
    coverage_summary = _coverage_summary(
        list(dfl_data_coverage_audit_frame.iter_rows(named=True)),
        min_tenant_count=min_tenant_count,
        backfill_target_anchor_count_per_tenant=backfill_target_anchor_count_per_tenant,
    )
    v2_regime_summaries = _v2_regime_summaries(
        regime_gated_tft_selector_v2_strict_frame,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
    )

    rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        source_row = source_rows.get(source_model_name)
        if source_row is None:
            rows.append(
                _missing_source_row(
                    source_model_name=source_model_name,
                    coverage_summary=coverage_summary,
                    min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
                    min_rolling_window_count=min_rolling_window_count,
                    backfill_target_anchor_count_per_tenant=backfill_target_anchor_count_per_tenant,
                )
            )
            continue
        source_robustness_rows = [
            row for row in robustness_rows if str(row["source_model_name"]) == source_model_name
        ]
        source_audit_rows = [
            row for row in audit_rows if str(row["source_model_name"]) == source_model_name
        ]
        regimes = _regime_summaries(source_audit_rows)
        if not regimes:
            regimes = [_empty_regime_summary()]
        for regime_summary in regimes:
            row = _promotion_row(
                source_row,
                source_robustness_rows=source_robustness_rows,
                regime_summary=regime_summary,
                v2_regime_summary=v2_regime_summaries.get(
                    (source_model_name, str(regime_summary["regime_label"]))
                ),
                coverage_summary=coverage_summary,
                min_tenant_count=min_tenant_count,
                min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
                min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
                min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
                min_rolling_window_count=min_rolling_window_count,
                backfill_target_anchor_count_per_tenant=backfill_target_anchor_count_per_tenant,
            )
            rows.append(row)

    return pl.DataFrame(rows).sort(["source_model_name", "regime_label"])


def validate_dfl_production_promotion_gate_evidence(
    promotion_gate_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_rolling_window_count: int = DEFAULT_ROLLING_VALIDATION_WINDOW_COUNT,
) -> EvidenceCheckOutcome:
    """Validate offline promotion-gate evidence and market-execution boundary."""

    failures = _missing_column_failures(
        promotion_gate_frame,
        REQUIRED_PRODUCTION_PROMOTION_COLUMNS,
    )
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": promotion_gate_frame.height})
    rows = list(promotion_gate_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "production promotion gate has no rows", {"row_count": 0})

    expected_sources = source_model_names or tuple(sorted({str(row["source_model_name"]) for row in rows}))
    rows_by_source: dict[str, list[dict[str, Any]]] = {
        str(row["source_model_name"]): [] for row in rows
    }
    for row in rows:
        rows_by_source[str(row["source_model_name"])].append(row)

    summaries: list[dict[str, Any]] = []
    for source_model_name in expected_sources:
        source_rows = rows_by_source.get(source_model_name, [])
        if not source_rows:
            failures.append(f"{source_model_name} production promotion row is missing")
            continue
        source_failures: list[str] = []
        for row in source_rows:
            source_failures.extend(
                _row_validation_failures(
                    row,
                    min_tenant_count=min_tenant_count,
                    min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
                    min_rolling_window_count=min_rolling_window_count,
                )
            )
        failures.extend(source_failures)
        summaries.append(_source_summary(source_model_name, source_rows, source_failures))

    promoted_sources = [
        str(summary["source_model_name"])
        for summary in summaries
        if bool(summary["production_promote"])
    ]
    metadata = {
        "row_count": len(rows),
        "source_model_count": len(expected_sources),
        "source_model_names": list(expected_sources),
        "promoted_source_model_names": promoted_sources,
        "production_promote_count": len(promoted_sources),
        "model_summaries": summaries,
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Production-promotion gate evidence is valid and market execution remains disabled."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_production_promotion_gate(
    promotion_gate_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> PromotionGateResult:
    """Evaluate whether any source/regime is promoted for offline strategy evidence."""

    evidence = validate_dfl_production_promotion_gate_evidence(
        promotion_gate_frame,
        source_model_names=source_model_names,
    )
    if not evidence.passed:
        return PromotionGateResult(False, "blocked", evidence.description, evidence.metadata)
    promoted_sources = list(evidence.metadata["promoted_source_model_names"])
    if promoted_sources:
        return PromotionGateResult(
            True,
            "offline_strategy_promoted",
            "At least one source/regime passed the offline production-promotion gate; "
            "market execution remains disabled.",
            evidence.metadata,
        )
    return PromotionGateResult(
        False,
        "strict_default_remains",
        f"No source/regime clears the strict {CONTROL_MODEL_NAME} production-promotion gate.",
        evidence.metadata,
    )


def _v2_regime_summaries(
    strict_frame: pl.DataFrame | None,
    *,
    min_mean_regret_improvement_ratio: float,
) -> dict[tuple[str, str], dict[str, Any]]:
    if strict_frame is None or strict_frame.height == 0:
        return {}
    _require_columns(
        strict_frame,
        REQUIRED_REGIME_GATED_V2_STRICT_COLUMNS,
        frame_name="regime_gated_tft_selector_v2_strict_frame",
    )
    rows = list(strict_frame.iter_rows(named=True))
    source_regimes: list[tuple[str, str]] = sorted(
        {
            (_source_model_name(row), str(_payload_value(row, "regime_label", default="unknown")))
            for row in rows
        }
    )
    summaries: dict[tuple[str, str], dict[str, Any]] = {}
    for source_model_name, regime_label in source_regimes:
        scoped_rows = [
            row
            for row in rows
            if _source_model_name(row) == source_model_name
            and _payload_value(row, "regime_label", default="unknown") == regime_label
        ]
        window_summaries = _v2_window_summaries(
            scoped_rows,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        if not window_summaries:
            continue
        latest = min(window_summaries, key=lambda row: int(row["window_index"]))
        summaries[(source_model_name, regime_label)] = {
            "tenant_count": latest["tenant_count"],
            "latest_validation_tenant_anchor_count": latest["validation_tenant_anchor_count"],
            "latest_source_signal": latest["strict_pass"],
            "latest_mean_regret_improvement_ratio_vs_strict": latest[
                "mean_regret_improvement_ratio_vs_strict"
            ],
            "latest_median_not_worse": latest["median_not_worse"],
            "latest_strict_mean_regret_uah": latest["strict_mean_regret_uah"],
            "latest_fallback_mean_regret_uah": latest["selector_mean_regret_uah"],
            "latest_strict_median_regret_uah": latest["strict_median_regret_uah"],
            "latest_fallback_median_regret_uah": latest["selector_median_regret_uah"],
            "rolling_window_count": len(window_summaries),
            "rolling_strict_pass_window_count": sum(
                1 for row in window_summaries if bool(row["strict_pass"])
            ),
            "rolling_development_pass_window_count": sum(
                1 for row in window_summaries if bool(row["development_pass"])
            ),
            "robust_research_challenger": bool(latest["strict_pass"])
            and sum(1 for row in window_summaries if bool(row["strict_pass"])) >= 3,
            "regime_window_count": len(window_summaries),
            "regime_validation_anchor_count": sum(
                int(row["validation_tenant_anchor_count"]) for row in window_summaries
            ),
            "regime_mean_improvement_ratio_vs_strict": _mean_float(
                row["mean_regret_improvement_ratio_vs_strict"] for row in window_summaries
            ),
            "regime_mean_improvement_ratio_vs_raw": _mean_float(
                row["mean_regret_improvement_ratio_vs_raw"] for row in window_summaries
            ),
        }
    return summaries


def _v2_window_summaries(
    rows: list[dict[str, Any]],
    *,
    min_mean_regret_improvement_ratio: float,
) -> list[dict[str, Any]]:
    window_indices = sorted(
        {int(str(_payload_value(row, "window_index", default=0))) for row in rows}
    )
    summaries: list[dict[str, Any]] = []
    for window_index in window_indices:
        window_rows = [
            row
            for row in rows
            if int(str(_payload_value(row, "window_index", default=0))) == window_index
        ]
        strict_rows = [row for row in window_rows if _payload_value(row, "selector_row_role") == "strict_reference"]
        raw_rows = [row for row in window_rows if _payload_value(row, "selector_row_role") == "raw_reference"]
        selector_rows = [row for row in window_rows if _payload_value(row, "selector_row_role") == "selector"]
        if not strict_rows or not selector_rows:
            continue
        strict_mean = _mean_regret(strict_rows)
        raw_mean = _mean_regret(raw_rows)
        selector_mean = _mean_regret(selector_rows)
        strict_median = _median_regret(strict_rows)
        selector_median = _median_regret(selector_rows)
        improvement_vs_strict = _improvement_ratio(strict_mean, selector_mean)
        improvement_vs_raw = _improvement_ratio(raw_mean, selector_mean)
        summaries.append(
            {
                "window_index": window_index,
                "tenant_count": len({str(row["tenant_id"]) for row in selector_rows}),
                "validation_tenant_anchor_count": len(_tenant_anchor_set(selector_rows)),
                "strict_mean_regret_uah": strict_mean,
                "selector_mean_regret_uah": selector_mean,
                "strict_median_regret_uah": strict_median,
                "selector_median_regret_uah": selector_median,
                "median_not_worse": selector_median <= strict_median,
                "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
                "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
                "strict_pass": improvement_vs_strict >= min_mean_regret_improvement_ratio
                and selector_median <= strict_median,
                "development_pass": improvement_vs_raw > 0.0,
            }
        )
    return summaries


def _promotion_row(
    source_row: dict[str, Any],
    *,
    source_robustness_rows: list[dict[str, Any]],
    regime_summary: dict[str, Any],
    v2_regime_summary: dict[str, Any] | None,
    coverage_summary: dict[str, Any],
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_rolling_strict_pass_windows: int,
    min_rolling_window_count: int,
    backfill_target_anchor_count_per_tenant: int,
) -> dict[str, Any]:
    source_model_name = str(source_row["source_model_name"])
    evidence_failures = [
        *_string_list(source_row["evidence_failure_messages"]),
        *coverage_summary["failure_messages"],
        *_robustness_claim_failures(source_robustness_rows),
    ]
    source_latest_signal = bool(source_row["latest_source_signal"])
    latest_improvement = float(source_row["latest_mean_regret_improvement_ratio_vs_strict"])
    median_not_worse = bool(source_row["latest_median_not_worse"])
    validation_count = int(source_row["latest_validation_tenant_anchor_count"])
    tenant_count = int(source_row["tenant_count"])
    latest_strict_mean = float(source_row["latest_strict_mean_regret_uah"])
    latest_fallback_mean = float(source_row["latest_fallback_mean_regret_uah"])
    latest_strict_median = float(source_row["latest_strict_median_regret_uah"])
    latest_fallback_median = float(source_row["latest_fallback_median_regret_uah"])
    rolling_window_count = int(source_row["rolling_window_count"])
    rolling_strict_count = int(source_row["rolling_strict_pass_window_count"])
    rolling_development_count = int(source_row["rolling_development_pass_window_count"])
    robust_research_challenger = bool(source_row["robust_research_challenger"])
    regime_window_count = int(regime_summary["regime_window_count"])
    regime_validation_count = int(regime_summary["regime_validation_anchor_count"])
    regime_improvement_vs_strict = float(regime_summary["regime_mean_improvement_ratio_vs_strict"])
    regime_improvement_vs_raw = float(regime_summary["regime_mean_improvement_ratio_vs_raw"])
    if v2_regime_summary is not None:
        source_latest_signal = bool(v2_regime_summary["latest_source_signal"])
        latest_improvement = float(v2_regime_summary["latest_mean_regret_improvement_ratio_vs_strict"])
        median_not_worse = bool(v2_regime_summary["latest_median_not_worse"])
        validation_count = int(v2_regime_summary["latest_validation_tenant_anchor_count"])
        tenant_count = int(v2_regime_summary["tenant_count"])
        latest_strict_mean = float(v2_regime_summary["latest_strict_mean_regret_uah"])
        latest_fallback_mean = float(v2_regime_summary["latest_fallback_mean_regret_uah"])
        latest_strict_median = float(v2_regime_summary["latest_strict_median_regret_uah"])
        latest_fallback_median = float(v2_regime_summary["latest_fallback_median_regret_uah"])
        rolling_window_count = int(v2_regime_summary["rolling_window_count"])
        rolling_strict_count = int(v2_regime_summary["rolling_strict_pass_window_count"])
        rolling_development_count = int(v2_regime_summary["rolling_development_pass_window_count"])
        robust_research_challenger = bool(v2_regime_summary["robust_research_challenger"])
        regime_window_count = int(v2_regime_summary["regime_window_count"])
        regime_validation_count = int(v2_regime_summary["regime_validation_anchor_count"])
        regime_improvement_vs_strict = float(
            v2_regime_summary["regime_mean_improvement_ratio_vs_strict"]
        )
        regime_improvement_vs_raw = float(v2_regime_summary["regime_mean_improvement_ratio_vs_raw"])
    blocker = _promotion_blocker(
        source_latest_signal=source_latest_signal,
        latest_improvement=latest_improvement,
        median_not_worse=median_not_worse,
        validation_count=validation_count,
        rolling_window_count=rolling_window_count,
        rolling_strict_count=rolling_strict_count,
        regime_label=str(regime_summary["regime_label"]),
        evidence_failures=evidence_failures,
        tenant_count=tenant_count,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        min_rolling_strict_pass_windows=min_rolling_strict_pass_windows,
        min_rolling_window_count=min_rolling_window_count,
    )
    production_promote = blocker == "none"
    return {
        "source_model_name": source_model_name,
        "regime_label": str(regime_summary["regime_label"]),
        "tenant_count": tenant_count,
        "latest_validation_tenant_anchor_count": validation_count,
        "latest_strict_mean_regret_uah": latest_strict_mean,
        "latest_fallback_mean_regret_uah": latest_fallback_mean,
        "latest_strict_median_regret_uah": latest_strict_median,
        "latest_fallback_median_regret_uah": latest_fallback_median,
        "latest_mean_regret_improvement_ratio_vs_strict": latest_improvement,
        "latest_median_not_worse": median_not_worse,
        "latest_source_signal": source_latest_signal,
        "rolling_window_count": rolling_window_count,
        "rolling_strict_pass_window_count": rolling_strict_count,
        "rolling_development_pass_window_count": rolling_development_count,
        "robust_research_challenger": robust_research_challenger,
        "regime_window_count": regime_window_count,
        "regime_validation_anchor_count": regime_validation_count,
        "regime_mean_improvement_ratio_vs_strict": regime_improvement_vs_strict,
        "regime_mean_improvement_ratio_vs_raw": regime_improvement_vs_raw,
        "regime_gated_tft_v2_evidence_used": v2_regime_summary is not None,
        "coverage_min_eligible_anchor_count": int(coverage_summary["min_eligible_anchor_count"]),
        "coverage_max_eligible_anchor_count": int(coverage_summary["max_eligible_anchor_count"]),
        "coverage_target_anchor_count_per_tenant": int(
            coverage_summary["target_anchor_count_per_tenant"]
        ),
        "backfill_target_anchor_count_per_tenant": backfill_target_anchor_count_per_tenant,
        "coverage_expansion_available": bool(coverage_summary["coverage_expansion_available"]),
        "coverage_ceiling_documented": not bool(coverage_summary["coverage_expansion_available"]),
        "fallback_strategy": (
            f"promoted_{source_model_name}_regime_gate"
            if production_promote
            else CONTROL_MODEL_NAME
        ),
        "promotion_blocker": blocker,
        "production_promote": production_promote,
        "market_execution_enabled": False,
        "claim_scope": DFL_PRODUCTION_PROMOTION_GATE_CLAIM_SCOPE,
        "academic_scope": DFL_PRODUCTION_PROMOTION_GATE_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evidence_failure_messages": evidence_failures,
        "evidence_failure_count": len(evidence_failures),
    }


def _promotion_blocker(
    *,
    source_latest_signal: bool,
    latest_improvement: float,
    median_not_worse: bool,
    validation_count: int,
    rolling_window_count: int,
    rolling_strict_count: int,
    regime_label: str,
    evidence_failures: list[str],
    tenant_count: int,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_rolling_strict_pass_windows: int,
    min_rolling_window_count: int,
) -> str:
    if evidence_failures:
        return "evidence_invalid"
    if tenant_count < min_tenant_count:
        return "coverage_insufficient"
    if validation_count < min_validation_tenant_anchor_count:
        return "coverage_insufficient"
    if not source_latest_signal:
        return "latest_signal_missing"
    if latest_improvement < min_mean_regret_improvement_ratio:
        return "mean_regret_not_improved"
    if not median_not_worse:
        return "median_degraded"
    if rolling_window_count < min_rolling_window_count:
        return "rolling_undercoverage"
    if rolling_strict_count < min_rolling_strict_pass_windows:
        return "rolling_not_robust"
    if regime_label in {STRICT_STABLE_REGIME, "unknown"}:
        return STRICT_STABLE_REGIME
    return "none"


def _coverage_summary(
    coverage_rows: list[dict[str, Any]],
    *,
    min_tenant_count: int,
    backfill_target_anchor_count_per_tenant: int,
) -> dict[str, Any]:
    failures: list[str] = []
    tenant_count = len({str(row["tenant_id"]) for row in coverage_rows})
    if tenant_count < min_tenant_count:
        failures.append(f"coverage tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if not coverage_rows:
        return {
            "tenant_count": 0,
            "min_eligible_anchor_count": 0,
            "max_eligible_anchor_count": 0,
            "target_anchor_count_per_tenant": 0,
            "coverage_expansion_available": False,
            "failure_messages": ["coverage audit rows are missing"],
        }
    if any(str(row["data_quality_tier"]) != "thesis_grade" for row in coverage_rows):
        failures.append("coverage audit requires thesis_grade rows")
    if any(float(row["price_observed_coverage_ratio"]) < 1.0 for row in coverage_rows):
        failures.append("coverage audit requires observed price coverage ratio of 1.0")
    if any(float(row["weather_observed_coverage_ratio"]) < 1.0 for row in coverage_rows):
        failures.append("coverage audit requires observed weather coverage ratio of 1.0")
    if any(not bool(row["not_full_dfl"]) for row in coverage_rows):
        failures.append("coverage audit rows must keep not_full_dfl=true")
    if any(not bool(row["not_market_execution"]) for row in coverage_rows):
        failures.append("coverage audit rows must keep not_market_execution=true")
    eligible_counts = [int(row["eligible_anchor_count"]) for row in coverage_rows]
    target_counts = [int(row.get("target_anchor_count_per_tenant", 0)) for row in coverage_rows]
    min_eligible = min(eligible_counts)
    return {
        "tenant_count": tenant_count,
        "min_eligible_anchor_count": min_eligible,
        "max_eligible_anchor_count": max(eligible_counts),
        "target_anchor_count_per_tenant": max(target_counts) if target_counts else 0,
        "coverage_expansion_available": min_eligible >= backfill_target_anchor_count_per_tenant,
        "failure_messages": failures,
    }


def _regime_summaries(audit_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    regimes: list[dict[str, Any]] = []
    for regime_label in sorted({str(row["failure_cluster"]) for row in audit_rows}):
        rows = [row for row in audit_rows if str(row["failure_cluster"]) == regime_label]
        regimes.append(
            {
                "regime_label": regime_label,
                "regime_window_count": len({int(row["window_index"]) for row in rows}),
                "regime_validation_anchor_count": sum(
                    int(row["validation_anchor_count"]) for row in rows
                ),
                "regime_mean_improvement_ratio_vs_strict": _mean_float(
                    row["mean_regret_improvement_ratio_vs_strict"] for row in rows
                ),
                "regime_mean_improvement_ratio_vs_raw": _mean_float(
                    row["mean_regret_improvement_ratio_vs_raw"] for row in rows
                ),
            }
        )
    return regimes


def _empty_regime_summary() -> dict[str, Any]:
    return {
        "regime_label": "unknown",
        "regime_window_count": 0,
        "regime_validation_anchor_count": 0,
        "regime_mean_improvement_ratio_vs_strict": 0.0,
        "regime_mean_improvement_ratio_vs_raw": 0.0,
    }


def _missing_source_row(
    *,
    source_model_name: str,
    coverage_summary: dict[str, Any],
    min_validation_tenant_anchor_count: int,
    min_rolling_window_count: int,
    backfill_target_anchor_count_per_tenant: int,
) -> dict[str, Any]:
    return {
        "source_model_name": source_model_name,
        "regime_label": "unknown",
        "tenant_count": 0,
        "latest_validation_tenant_anchor_count": 0,
        "latest_strict_mean_regret_uah": 0.0,
        "latest_fallback_mean_regret_uah": 0.0,
        "latest_strict_median_regret_uah": 0.0,
        "latest_fallback_median_regret_uah": 0.0,
        "latest_mean_regret_improvement_ratio_vs_strict": 0.0,
        "latest_median_not_worse": False,
        "latest_source_signal": False,
        "rolling_window_count": min_rolling_window_count,
        "rolling_strict_pass_window_count": 0,
        "rolling_development_pass_window_count": 0,
        "robust_research_challenger": False,
        "regime_window_count": 0,
        "regime_validation_anchor_count": 0,
        "regime_mean_improvement_ratio_vs_strict": 0.0,
        "regime_mean_improvement_ratio_vs_raw": 0.0,
        "coverage_min_eligible_anchor_count": int(coverage_summary["min_eligible_anchor_count"]),
        "coverage_max_eligible_anchor_count": int(coverage_summary["max_eligible_anchor_count"]),
        "coverage_target_anchor_count_per_tenant": int(
            coverage_summary["target_anchor_count_per_tenant"]
        ),
        "backfill_target_anchor_count_per_tenant": backfill_target_anchor_count_per_tenant,
        "coverage_expansion_available": bool(coverage_summary["coverage_expansion_available"]),
        "coverage_ceiling_documented": not bool(coverage_summary["coverage_expansion_available"]),
        "fallback_strategy": CONTROL_MODEL_NAME,
        "promotion_blocker": "evidence_invalid",
        "production_promote": False,
        "market_execution_enabled": False,
        "claim_scope": DFL_PRODUCTION_PROMOTION_GATE_CLAIM_SCOPE,
        "academic_scope": DFL_PRODUCTION_PROMOTION_GATE_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evidence_failure_messages": [
            f"{source_model_name} source-specific challenger row is missing",
            *coverage_summary["failure_messages"],
        ],
        "evidence_failure_count": 1 + len(coverage_summary["failure_messages"]),
        "min_validation_tenant_anchor_count": min_validation_tenant_anchor_count,
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
    if str(row["claim_scope"]) != DFL_PRODUCTION_PROMOTION_GATE_CLAIM_SCOPE:
        failures.append("production promotion gate claim_scope is invalid")
    if not bool(row["not_full_dfl"]):
        failures.append("production promotion rows must keep not_full_dfl=true")
    if not bool(row["not_market_execution"]):
        failures.append("production promotion rows must keep not_market_execution=true")
    if bool(row["market_execution_enabled"]):
        failures.append("production promotion rows must keep market_execution_enabled=false")
    if bool(row["production_promote"]):
        if str(row["promotion_blocker"]) != "none":
            failures.append("promoted rows must have promotion_blocker=none")
        if str(row["fallback_strategy"]) == CONTROL_MODEL_NAME:
            failures.append("promoted rows must not use strict_similar_day as fallback_strategy")
    return failures


def _source_summary(
    source_model_name: str,
    source_rows: list[dict[str, Any]],
    source_failures: list[str],
) -> dict[str, Any]:
    promoted_rows = [row for row in source_rows if bool(row["production_promote"])]
    blocker_counts: dict[str, int] = {}
    for row in source_rows:
        blocker = str(row["promotion_blocker"])
        blocker_counts[blocker] = blocker_counts.get(blocker, 0) + 1
    return {
        "source_model_name": source_model_name,
        "row_count": len(source_rows),
        "production_promote": bool(promoted_rows),
        "promoted_regime_labels": [str(row["regime_label"]) for row in promoted_rows],
        "promotion_blockers": blocker_counts,
        "failure_count": len(source_failures),
    }


def _robustness_claim_failures(rows: list[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["rolling robustness rows are missing"]
    failures: list[str] = []
    if any(str(row["claim_scope"]) != DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE for row in rows):
        failures.append("rolling robustness rows must keep expected claim_scope")
    if any(not bool(row["not_full_dfl"]) for row in rows):
        failures.append("rolling robustness rows must keep not_full_dfl=true")
    if any(not bool(row["not_market_execution"]) for row in rows):
        failures.append("rolling robustness rows must keep not_market_execution=true")
    return failures


def _validate_config(
    *,
    source_model_names: tuple[str, ...],
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
    min_rolling_strict_pass_windows: int,
    min_rolling_window_count: int,
    backfill_target_anchor_count_per_tenant: int,
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
    if backfill_target_anchor_count_per_tenant < 1:
        raise ValueError("backfill_target_anchor_count_per_tenant must be at least 1.")


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(required_columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {', '.join(missing)}")


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    return [f"missing required column: {column}" for column in sorted(required_columns.difference(frame.columns))]


def _mean_float(values: Any) -> float:
    items = [float(value) for value in values]
    return mean(items) if items else 0.0


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return mean(regrets) if regrets else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return median(regrets) if regrets else 0.0


def _improvement_ratio(baseline: float, candidate: float) -> float:
    return (baseline - candidate) / abs(baseline) if abs(baseline) > 1e-9 else 0.0


def _tenant_anchor_set(rows: list[dict[str, Any]]) -> set[tuple[str, object]]:
    return {(str(row["tenant_id"]), row["anchor_timestamp"]) for row in rows}


def _source_model_name(row: dict[str, Any]) -> str:
    if "source_model_name" in row and row["source_model_name"]:
        return str(row["source_model_name"])
    return str(_payload_value(row, "source_forecast_model_name", default=""))


def _payload_value(row: dict[str, Any], key: str, *, default: object = None) -> object:
    payload = row.get("evaluation_payload", {})
    if isinstance(payload, dict):
        return payload.get(key, default)
    return default


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    if isinstance(value, tuple):
        return [str(item) for item in value]
    if value is None:
        return []
    return [str(value)]
