"""Regime-gated prior-only TFT selector v2."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.dfl.strict_failure_features import (
    REQUIRED_FEATURE_AUDIT_COLUMNS,
    REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS,
)
from smart_arbitrage.dfl.strict_failure_selector import (
    CANDIDATE_FAMILY_RAW,
    CANDIDATE_FAMILY_STRICT,
    REQUIRED_EVALUATION_COLUMNS,
    REQUIRED_LIBRARY_COLUMNS,
    _committed_action,
    _family_sort_index,
    _first_or_default,
    _improvement_ratio,
    _mean_regret,
    _median_regret,
    _missing_column_failures,
    _payload,
    _provenance_failures,
    _require_columns,
    _source_model_name,
    _tenant_anchor_set,
    _datetime_value,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_REGIME_GATED_TFT_SELECTOR_V2_CLAIM_SCOPE: Final[str] = (
    "dfl_regime_gated_tft_selector_v2_not_full_dfl"
)
DFL_REGIME_GATED_TFT_SELECTOR_V2_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_regime_gated_tft_selector_v2_strict_lp_gate_not_full_dfl"
)
DFL_REGIME_GATED_TFT_SELECTOR_V2_STRATEGY_KIND: Final[str] = (
    "dfl_regime_gated_tft_selector_v2_strict_lp_benchmark"
)
DFL_REGIME_GATED_TFT_SELECTOR_V2_PREFIX: Final[str] = "dfl_regime_gated_tft_selector_v2_"
DFL_REGIME_GATED_TFT_SELECTOR_V2_ACADEMIC_SCOPE: Final[str] = (
    "Prior-only regime gate for allowing a TFT challenger only in audited "
    "strict-control failure regimes. This is not full DFL, not Decision "
    "Transformer control, and not market execution."
)
DEFAULT_TFT_SOURCE_MODEL_NAME: Final[str] = "tft_silver_v0"
STRICT_STABLE_REGIME: Final[str] = "strict_stable_region"
STRICT_FAILURE_REGIMES: Final[frozenset[str]] = frozenset(
    {
        "strict_failure_captured",
        "high_spread_volatility",
        "rank_instability",
        "load_weather_stress",
        "tenant_specific_outlier",
    }
)
REQUIRED_REGIME_SELECTOR_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_model_name",
        "selector_model_name",
        "window_index",
        "regime_label",
        "selected_rule_name",
        "selected_fallback_strategy",
        "allow_challenger",
        "promotion_blocker",
        "training_window_count",
        "validation_tenant_count",
        "validation_anchor_count",
        "validation_tenant_anchor_count",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REGIME_GATED_TFT_SELECTOR_V2_STRICT_SCHEMA: Final[dict[str, Any]] = {
    "evaluation_id": pl.Utf8,
    "tenant_id": pl.Utf8,
    "source_model_name": pl.Utf8,
    "forecast_model_name": pl.Utf8,
    "strategy_kind": pl.Utf8,
    "market_venue": pl.Utf8,
    "anchor_timestamp": pl.Datetime,
    "generated_at": pl.Datetime,
    "horizon_hours": pl.Int64,
    "starting_soc_fraction": pl.Float64,
    "starting_soc_source": pl.Utf8,
    "decision_value_uah": pl.Float64,
    "forecast_objective_value_uah": pl.Float64,
    "oracle_value_uah": pl.Float64,
    "regret_uah": pl.Float64,
    "regret_ratio": pl.Float64,
    "total_degradation_penalty_uah": pl.Float64,
    "total_throughput_mwh": pl.Float64,
    "committed_action": pl.Utf8,
    "committed_power_mw": pl.Float64,
    "rank_by_regret": pl.Int64,
    "window_index": pl.Int64,
    "regime_label": pl.Utf8,
    "claim_scope": pl.Utf8,
    "not_full_dfl": pl.Boolean,
    "not_market_execution": pl.Boolean,
    "evaluation_payload": pl.Object,
}


def regime_gated_tft_selector_v2_model_name(source_model_name: str) -> str:
    """Return the stable selector model name for a source model."""

    return f"{DFL_REGIME_GATED_TFT_SELECTOR_V2_PREFIX}{source_model_name}"


def build_dfl_regime_gated_tft_selector_v2_frame(
    prior_feature_panel_frame: pl.DataFrame,
    strict_failure_feature_audit_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    tft_source_model_name: str = DEFAULT_TFT_SOURCE_MODEL_NAME,
    min_training_window_count: int = 3,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> pl.DataFrame:
    """Select regime-level TFT switch rules from earlier windows only."""

    _validate_selector_config(
        tenant_ids=tenant_ids,
        source_model_names=source_model_names,
        min_training_window_count=min_training_window_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
    )
    _validate_feature_panel(prior_feature_panel_frame)
    _validate_feature_audit(strict_failure_feature_audit_frame)
    panel_rows = list(prior_feature_panel_frame.iter_rows(named=True))
    audit_rows = list(strict_failure_feature_audit_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for source_model_name in source_model_names:
        source_windows = sorted(
            {
                int(row["window_index"])
                for row in panel_rows
                if str(row["source_model_name"]) == source_model_name
            }
        )
        if not source_windows:
            raise ValueError(f"missing prior feature panel rows for {source_model_name}")
        for window_index in source_windows:
            regimes = _window_regime_labels(
                audit_rows,
                source_model_name=source_model_name,
                window_index=window_index,
            )
            if not regimes:
                regimes = (STRICT_STABLE_REGIME,)
            for regime_label in regimes:
                validation_rows = _window_panel_rows(
                    panel_rows,
                    source_model_name=source_model_name,
                    window_index=window_index,
                    regime_label=regime_label,
                    audit_rows=audit_rows,
                )
                train_rows = _prior_audit_rows(
                    audit_rows,
                    source_model_name=source_model_name,
                    window_index=window_index,
                    regime_label=regime_label,
                )
                rows.append(
                    _selector_row(
                        source_model_name=source_model_name,
                        tft_source_model_name=tft_source_model_name,
                        window_index=window_index,
                        regime_label=regime_label,
                        validation_rows=validation_rows,
                        train_rows=train_rows,
                        tenant_count=len(tenant_ids),
                        min_training_window_count=min_training_window_count,
                        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
                    )
                )
    return pl.DataFrame(rows).sort(["source_model_name", "window_index", "regime_label"])


def build_dfl_regime_gated_tft_selector_v2_strict_lp_benchmark_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    regime_gated_selector_frame: pl.DataFrame,
    prior_feature_panel_frame: pl.DataFrame,
    strict_failure_feature_audit_frame: pl.DataFrame | None = None,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict LP/oracle rows for the regime-gated TFT selector."""

    _validate_library_frame(schedule_candidate_library_frame)
    _validate_regime_selector_frame(regime_gated_selector_frame)
    _validate_feature_panel(prior_feature_panel_frame)
    audit_rows = (
        list(strict_failure_feature_audit_frame.iter_rows(named=True))
        if strict_failure_feature_audit_frame is not None and strict_failure_feature_audit_frame.height
        else []
    )
    if strict_failure_feature_audit_frame is not None and strict_failure_feature_audit_frame.height:
        _validate_feature_audit(strict_failure_feature_audit_frame)

    resolved_generated_at = generated_at or _latest_generated_at(schedule_candidate_library_frame)
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    panel_rows = list(prior_feature_panel_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for selector_row in regime_gated_selector_frame.iter_rows(named=True):
        source_model_name = str(selector_row["source_model_name"])
        window_index = int(selector_row["window_index"])
        regime_label = str(selector_row["regime_label"])
        feature_rows = _window_panel_rows(
            panel_rows,
            source_model_name=source_model_name,
            window_index=window_index,
            regime_label=regime_label,
            audit_rows=audit_rows,
        )
        if not feature_rows:
            continue
        for feature_row in sorted(
            feature_rows,
            key=lambda row: _comparison_datetime(
                row["anchor_timestamp"],
                field_name="anchor_timestamp",
            ),
        ):
            source_rows = _library_rows(
                library_rows,
                tenant_id=str(feature_row["tenant_id"]),
                source_model_name=source_model_name,
            )
            anchor_timestamp = _comparison_datetime(
                feature_row["anchor_timestamp"],
                field_name="anchor_timestamp",
            )
            if not _has_anchor_rows(source_rows, anchor_timestamp=anchor_timestamp):
                continue
            decision = _decision_for_feature_row(source_rows, feature_row)
            selected_row = (
                decision["best_prior_non_strict_row"]
                if bool(selector_row["allow_challenger"])
                else decision["strict_row"]
            )
            for role, candidate_row in (
                ("strict_reference", decision["strict_row"]),
                ("raw_reference", decision["raw_row"]),
                ("best_prior_non_strict_reference", decision["best_prior_non_strict_row"]),
                ("selector", selected_row),
            ):
                rows.append(
                    _strict_benchmark_row(
                        candidate_row,
                        source_model_name=source_model_name,
                        selector_row=selector_row,
                        feature_row=feature_row,
                        role=role,
                        generated_at=resolved_generated_at,
                    )
                )
    if not rows:
        return pl.DataFrame(schema=REGIME_GATED_TFT_SELECTOR_V2_STRICT_SCHEMA)
    return pl.DataFrame(rows).sort(
        ["source_model_name", "window_index", "regime_label", "tenant_id", "anchor_timestamp", "forecast_model_name"]
    )


def validate_dfl_regime_gated_tft_selector_v2_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    control_model_name: str = CONTROL_MODEL_NAME,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio: float = DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
) -> EvidenceCheckOutcome:
    """Validate regime-gated TFT selector strict LP evidence."""

    failures = _missing_column_failures(strict_frame, REQUIRED_EVALUATION_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": strict_frame.height})
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "regime-gated TFT selector evidence has no rows", {"row_count": 0})
    expected_sources = source_model_names or tuple(sorted({_source_model_name(row) for row in rows}))
    summaries: list[dict[str, Any]] = []
    for source_model_name in expected_sources:
        summary, source_failures = _selector_gate_summary(
            rows,
            source_model_name=source_model_name,
            control_model_name=control_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
        )
        summaries.append(summary)
        failures.extend(source_failures)
    metadata = {
        "row_count": len(rows),
        "source_model_count": len(expected_sources),
        "source_model_names": list(expected_sources),
        "model_summaries": summaries,
        "production_gate_passed": any(bool(summary["production_gate_passed"]) for summary in summaries),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Regime-gated TFT selector v2 evidence is valid."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def evaluate_dfl_regime_gated_tft_selector_v2_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
) -> PromotionGateResult:
    """Evaluate whether the regime-gated selector clears the strict evidence gate."""

    evidence = validate_dfl_regime_gated_tft_selector_v2_evidence(
        strict_frame,
        source_model_names=source_model_names,
    )
    if not evidence.passed:
        return PromotionGateResult(False, "blocked", evidence.description, evidence.metadata)
    if bool(evidence.metadata["production_gate_passed"]):
        return PromotionGateResult(
            True,
            "regime_gated_research_challenger",
            "At least one regime-gated TFT source clears the offline strict evidence gate.",
            evidence.metadata,
        )
    return PromotionGateResult(
        False,
        "strict_default_remains",
        f"No regime-gated TFT source clears the strict {CONTROL_MODEL_NAME} gate.",
        evidence.metadata,
    )


def _selector_row(
    *,
    source_model_name: str,
    tft_source_model_name: str,
    window_index: int,
    regime_label: str,
    validation_rows: list[dict[str, Any]],
    train_rows: list[dict[str, Any]],
    tenant_count: int,
    min_training_window_count: int,
    min_mean_regret_improvement_ratio: float,
) -> dict[str, Any]:
    training_window_count = len({int(row["window_index"]) for row in train_rows})
    training_improvement = _mean_field(train_rows, "mean_regret_improvement_ratio_vs_strict")
    training_median_not_worse = _training_median_not_worse(train_rows)
    blocker = _selection_blocker(
        source_model_name=source_model_name,
        tft_source_model_name=tft_source_model_name,
        regime_label=regime_label,
        training_window_count=training_window_count,
        training_improvement=training_improvement,
        training_median_not_worse=training_median_not_worse,
        min_training_window_count=min_training_window_count,
        min_mean_regret_improvement_ratio=min_mean_regret_improvement_ratio,
    )
    allow_challenger = blocker == "none"
    strict_mean = _mean_field(validation_rows, "analysis_only_strict_regret_uah")
    selected_mean = _mean_field(validation_rows, "analysis_only_best_non_strict_regret_uah")
    return {
        "source_model_name": source_model_name,
        "selector_model_name": regime_gated_tft_selector_v2_model_name(source_model_name),
        "window_index": window_index,
        "regime_label": regime_label,
        "selected_rule_name": (
            "allow_tft_in_prior_strict_failure_regime"
            if allow_challenger
            else f"default_strict_due_to_{blocker}"
        ),
        "selected_fallback_strategy": "tft_challenger" if allow_challenger else CONTROL_MODEL_NAME,
        "allow_challenger": allow_challenger,
        "promotion_blocker": blocker,
        "training_window_count": training_window_count,
        "training_regime_row_count": len(train_rows),
        "training_mean_regret_improvement_ratio_vs_strict": training_improvement,
        "training_median_not_worse": training_median_not_worse,
        "validation_tenant_count": len({str(row["tenant_id"]) for row in validation_rows}),
        "validation_anchor_count": len({_comparison_datetime(row["anchor_timestamp"], field_name="anchor_timestamp") for row in validation_rows}),
        "validation_tenant_anchor_count": len(validation_rows),
        "validation_strict_mean_regret_uah": strict_mean,
        "validation_challenger_mean_regret_uah": selected_mean,
        "validation_mean_regret_improvement_ratio_vs_strict": _improvement_ratio(
            strict_mean,
            selected_mean if allow_challenger else strict_mean,
        ),
        "tenant_count": tenant_count,
        "claim_scope": DFL_REGIME_GATED_TFT_SELECTOR_V2_CLAIM_SCOPE,
        "academic_scope": DFL_REGIME_GATED_TFT_SELECTOR_V2_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _selection_blocker(
    *,
    source_model_name: str,
    tft_source_model_name: str,
    regime_label: str,
    training_window_count: int,
    training_improvement: float,
    training_median_not_worse: bool,
    min_training_window_count: int,
    min_mean_regret_improvement_ratio: float,
) -> str:
    if source_model_name != tft_source_model_name:
        return "source_not_tft"
    if regime_label == STRICT_STABLE_REGIME:
        return "strict_stable_region"
    if regime_label not in STRICT_FAILURE_REGIMES:
        return "regime_not_allowed"
    if training_window_count < min_training_window_count:
        return "prior_regime_undercoverage"
    if training_improvement < min_mean_regret_improvement_ratio:
        return "prior_regime_not_predictive"
    if not training_median_not_worse:
        return "prior_regime_median_degraded"
    return "none"


def _window_regime_labels(
    audit_rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    window_index: int,
) -> tuple[str, ...]:
    labels = sorted(
        {
            str(row["failure_cluster"])
            for row in audit_rows
            if str(row["source_model_name"]) == source_model_name
            and int(row["window_index"]) == window_index
        }
    )
    return tuple(labels)


def _window_panel_rows(
    panel_rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    window_index: int,
    regime_label: str,
    audit_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    matches: list[dict[str, Any]] = []
    audit_regimes = _audit_regimes_by_tenant_window(audit_rows)
    for row in panel_rows:
        if str(row["source_model_name"]) != source_model_name:
            continue
        if int(row["window_index"]) != window_index:
            continue
        row_regime = _feature_row_regime(row, audit_regimes=audit_regimes)
        if row_regime == regime_label:
            matches.append(row)
    return matches


def _feature_row_regime(
    row: dict[str, Any],
    *,
    audit_regimes: dict[tuple[str, str, int], str],
) -> str:
    if "analysis_only_failure_cluster" in row and row["analysis_only_failure_cluster"]:
        return str(row["analysis_only_failure_cluster"])
    key = (str(row["tenant_id"]), str(row["source_model_name"]), int(row["window_index"]))
    return audit_regimes.get(key, STRICT_STABLE_REGIME)


def _audit_regimes_by_tenant_window(
    audit_rows: list[dict[str, Any]],
) -> dict[tuple[str, str, int], str]:
    return {
        (str(row["tenant_id"]), str(row["source_model_name"]), int(row["window_index"])): str(
            row["failure_cluster"]
        )
        for row in audit_rows
    }


def _prior_audit_rows(
    audit_rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    window_index: int,
    regime_label: str,
) -> list[dict[str, Any]]:
    return [
        row
        for row in audit_rows
        if str(row["source_model_name"]) == source_model_name
        and int(row["window_index"]) > window_index
        and str(row["failure_cluster"]) == regime_label
    ]


def _decision_for_feature_row(
    source_rows: list[dict[str, Any]],
    feature_row: dict[str, Any],
) -> dict[str, Any]:
    anchor_timestamp = _comparison_datetime(feature_row["anchor_timestamp"], field_name="anchor_timestamp")
    prior_cutoff = _comparison_datetime(feature_row["prior_cutoff_timestamp"], field_name="prior_cutoff_timestamp")
    anchor_rows = [
        row
        for row in source_rows
        if _comparison_datetime(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
    ]
    strict_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
    raw_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_RAW)
    non_strict_candidates: list[dict[str, Any]] = []
    for row in anchor_rows:
        if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
            continue
        prior_mean = _prior_mean_regret(source_rows, row, prior_cutoff=prior_cutoff)
        if prior_mean is None:
            continue
        candidate = dict(row)
        candidate["selector_prior_mean_regret_uah"] = prior_mean
        non_strict_candidates.append(candidate)
    if not non_strict_candidates:
        fallback_family = str(feature_row.get("analysis_only_selected_candidate_family", ""))
        fallback_matches = [
            row for row in anchor_rows if str(row["candidate_family"]) == fallback_family
        ]
        if not fallback_matches:
            raise ValueError("regime-gated selector is missing non-strict prior candidates")
        best_non_strict = dict(fallback_matches[0])
        best_non_strict["selector_prior_mean_regret_uah"] = float(
            feature_row.get("selector_feature_prior_best_non_strict_mean_regret_uah", 0.0)
        )
        return {
            "strict_row": strict_row,
            "raw_row": raw_row,
            "best_prior_non_strict_row": best_non_strict,
        }
    best_non_strict = min(
        non_strict_candidates,
        key=lambda row: (
            float(row["selector_prior_mean_regret_uah"]),
            _family_sort_index(str(row["candidate_family"])),
            str(row["candidate_model_name"]),
        ),
    )
    return {
        "strict_row": strict_row,
        "raw_row": raw_row,
        "best_prior_non_strict_row": best_non_strict,
    }


def _strict_benchmark_row(
    row: dict[str, Any],
    *,
    source_model_name: str,
    selector_row: dict[str, Any],
    feature_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    selector_model_name = regime_gated_tft_selector_v2_model_name(source_model_name)
    forecast_model_name = selector_model_name if role == "selector" else str(row["candidate_model_name"])
    candidate_family = str(row["candidate_family"])
    anchor_timestamp = _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    payload.update(
        {
            "strict_gate_kind": "dfl_regime_gated_tft_selector_v2_strict_lp",
            "source_forecast_model_name": source_model_name,
            "selector_model_name": selector_model_name,
            "window_index": int(selector_row["window_index"]),
            "regime_label": str(selector_row["regime_label"]),
            "selected_rule_name": str(selector_row["selected_rule_name"]),
            "selected_fallback_strategy": str(selector_row["selected_fallback_strategy"]),
            "allow_challenger": bool(selector_row["allow_challenger"]),
            "promotion_blocker": str(selector_row["promotion_blocker"]),
            "feature_prior_price_regime": str(
                feature_row.get("selector_feature_prior_price_regime", "unknown")
            ),
            "feature_prior_volatility_regime": str(
                feature_row.get("selector_feature_prior_spread_volatility_regime", "unknown")
            ),
            "selector_row_candidate_family": candidate_family,
            "selector_row_candidate_model_name": str(row["candidate_model_name"]),
            "selector_row_role": role,
            "claim_scope": DFL_REGIME_GATED_TFT_SELECTOR_V2_STRICT_CLAIM_SCOPE,
            "academic_scope": DFL_REGIME_GATED_TFT_SELECTOR_V2_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": int(row["safety_violation_count"]),
            "not_full_dfl": True,
            "not_market_execution": True,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:regime-gated-tft-selector-v2:{source_model_name}:"
            f"{selector_row['window_index']}:{selector_row['regime_label']}:{role}:"
            f"{candidate_family}:{anchor_timestamp.strftime('%Y%m%dT%H%M')}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": DFL_REGIME_GATED_TFT_SELECTOR_V2_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], default=0.5),
        "starting_soc_source": "schedule_candidate_library_v2",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": _committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], default=0.0)),
        "rank_by_regret": 1,
        "window_index": int(selector_row["window_index"]),
        "regime_label": str(selector_row["regime_label"]),
        "claim_scope": DFL_REGIME_GATED_TFT_SELECTOR_V2_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "evaluation_payload": payload,
    }


def _selector_gate_summary(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    control_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio: float,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    selected_model_name = regime_gated_tft_selector_v2_model_name(source_model_name)
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [row for row in source_rows if row["forecast_model_name"] == control_model_name]
    raw_rows = [row for row in source_rows if row["forecast_model_name"] == source_model_name]
    selected_rows = [row for row in source_rows if row["forecast_model_name"] == selected_model_name]
    strict_anchors = _tenant_anchor_set(strict_rows)
    raw_anchors = _tenant_anchor_set(raw_rows)
    selected_anchors = _tenant_anchor_set(selected_rows)
    if strict_anchors != raw_anchors or strict_anchors != selected_anchors:
        failures.append(f"{source_model_name} strict/raw/selector rows must cover matching tenant-anchor sets")
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(_provenance_failures([*strict_rows, *raw_rows, *selected_rows]))
    failures.extend(_top_level_claim_failures([*strict_rows, *raw_rows, *selected_rows]))
    strict_mean = _mean_regret(strict_rows)
    raw_mean = _mean_regret(raw_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_raw = _improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    development_passed = validation_count >= min_validation_tenant_anchor_count and improvement_vs_raw > 0.0
    production_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio
        and selected_median <= strict_median
        and not failures
    )
    if selected_rows and strict_rows and improvement_vs_strict < min_mean_regret_improvement_ratio:
        failures.append(
            f"{source_model_name} mean regret improvement vs {control_model_name} must be at least "
            f"{min_mean_regret_improvement_ratio:.1%}; observed {improvement_vs_strict:.1%}"
        )
    if selected_rows and strict_rows and selected_median > strict_median:
        failures.append(
            f"{source_model_name} median regret must not be worse than {control_model_name}; "
            f"observed selector={selected_median:.2f}, strict={strict_median:.2f}"
        )
    return {
        "source_model_name": source_model_name,
        "selector_model_name": selected_model_name,
        "tenant_count": tenant_count,
        "validation_tenant_anchor_count": validation_count,
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "development_gate_passed": development_passed,
        "production_gate_passed": production_passed,
        "failures": failures,
    }, failures


def _library_rows(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
) -> list[dict[str, Any]]:
    matches = [
        row
        for row in rows
        if str(row["tenant_id"]) == tenant_id and str(row["source_model_name"]) == source_model_name
    ]
    if not matches:
        raise ValueError(f"missing schedule candidate rows for {tenant_id}/{source_model_name}")
    return matches


def _has_anchor_rows(rows: list[dict[str, Any]], *, anchor_timestamp: datetime) -> bool:
    return any(
        _comparison_datetime(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
        for row in rows
    )


def _single_family_row(rows: list[dict[str, Any]], candidate_family: str) -> dict[str, Any]:
    matches = [row for row in rows if str(row["candidate_family"]) == candidate_family]
    if not matches:
        raise ValueError(f"missing {candidate_family} row")
    return matches[0]


def _prior_mean_regret(
    rows: list[dict[str, Any]],
    row: dict[str, Any],
    *,
    prior_cutoff: datetime,
) -> float | None:
    prior_rows = [
        candidate
        for candidate in rows
        if str(candidate["split_name"]) == "train_selection"
        and str(candidate["candidate_family"]) == str(row["candidate_family"])
        and str(candidate["candidate_model_name"]) == str(row["candidate_model_name"])
        and _comparison_datetime(candidate["anchor_timestamp"], field_name="anchor_timestamp") < prior_cutoff
    ]
    return _mean_regret(prior_rows) if prior_rows else None


def _comparison_datetime(value: object, *, field_name: str) -> datetime:
    timestamp = _datetime_value(value, field_name=field_name)
    if timestamp.tzinfo is None:
        return timestamp
    return timestamp.astimezone(UTC).replace(tzinfo=None)


def _validate_selector_config(
    *,
    tenant_ids: tuple[str, ...],
    source_model_names: tuple[str, ...],
    min_training_window_count: int,
    min_mean_regret_improvement_ratio: float,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not source_model_names:
        raise ValueError("source_model_names must contain at least one model.")
    if min_training_window_count <= 0:
        raise ValueError("min_training_window_count must be positive.")
    if min_mean_regret_improvement_ratio < 0.0:
        raise ValueError("min_mean_regret_improvement_ratio must be non-negative.")


def _validate_feature_panel(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS, frame_name="prior_feature_panel_frame")
    for row in frame.iter_rows(named=True):
        if row.get("not_full_dfl") is not True or row.get("not_market_execution") is not True:
            raise ValueError("prior feature panel rows must keep DFL and market-execution claim flags")


def _validate_feature_audit(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_FEATURE_AUDIT_COLUMNS, frame_name="strict_failure_feature_audit_frame")
    for row in frame.iter_rows(named=True):
        if row.get("not_full_dfl") is not True or row.get("not_market_execution") is not True:
            raise ValueError("feature audit rows must keep DFL and market-execution claim flags")


def _validate_library_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="schedule_candidate_library_frame")
    for row in frame.iter_rows(named=True):
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("schedule candidate library rows must be thesis_grade")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("schedule candidate library rows must have observed coverage")
        if int(row["safety_violation_count"]) != 0:
            raise ValueError("schedule candidate library rows must have zero safety violations")
        if row.get("not_full_dfl") is not True or row.get("not_market_execution") is not True:
            raise ValueError("schedule candidate library rows must keep claim flags")


def _validate_regime_selector_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_REGIME_SELECTOR_COLUMNS, frame_name="regime_gated_selector_frame")
    for row in frame.iter_rows(named=True):
        if row.get("not_full_dfl") is not True or row.get("not_market_execution") is not True:
            raise ValueError("regime-gated selector rows must keep claim flags")


def _mean_field(rows: list[dict[str, Any]], column_name: str) -> float:
    values = [float(row[column_name]) for row in rows if row.get(column_name) is not None]
    return mean(values) if values else 0.0


def _training_median_not_worse(rows: list[dict[str, Any]]) -> bool:
    if not rows:
        return False
    selected_regrets = [float(row["selected_mean_regret_uah"]) for row in rows]
    strict_regrets = [float(row["strict_mean_regret_uah"]) for row in rows]
    return median(selected_regrets) <= median(strict_regrets)


def _top_level_claim_failures(rows: list[dict[str, Any]]) -> list[str]:
    failures: list[str] = []
    for row in rows:
        if "not_full_dfl" in row and row["not_full_dfl"] is False:
            failures.append("regime-gated selector evidence must remain not_full_dfl")
            break
        if "not_market_execution" in row and row["not_market_execution"] is False:
            failures.append("regime-gated selector evidence must remain not_market_execution")
            break
    return failures


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    if "generated_at" not in frame.columns or frame.is_empty():
        return datetime.now(UTC)
    values = [
        _datetime_value(value, field_name="generated_at")
        for value in frame["generated_at"].to_list()
    ]
    return max(values) if values else datetime.now(UTC)
