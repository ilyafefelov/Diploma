"""Plateau autopsy and candidate-value DFL v4 challenger evidence."""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import candidate_value_dfl_v3 as v3
from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

CANDIDATE_VALUE_DFL_V4_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_dfl_v4_not_full_dfl"
)
CANDIDATE_VALUE_DFL_V4_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_dfl_v4_strict_lp_gate_not_full_dfl"
)
CANDIDATE_VALUE_DFL_V4_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_candidate_value_dfl_v4_strict_lp_benchmark"
)
CANDIDATE_VALUE_DFL_V4_PREFIX: Final[str] = "dfl_candidate_value_dfl_v4_"
CANDIDATE_VALUE_DFL_V4_ACADEMIC_SCOPE: Final[str] = (
    "Candidate-level value scorer over V4 feasible LP-scored schedules. "
    "It is selected on train/prior anchors and falls back to frozen V2+ unless "
    "prior evidence predicts improvement. This is not full DFL and not market execution."
)
CANDIDATE_VALUE_DFL_V4_PLATEAU_AUTOPSY_CLAIM_SCOPE: Final[str] = (
    "dfl_v2_v3_plateau_autopsy_not_full_dfl"
)
CANDIDATE_VALUE_DFL_V4_DATA_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_plateau_data_quality_audit_not_full_dfl"
)
DFL_SCHEDULE_CANDIDATE_LIBRARY_V4_CLAIM_SCOPE: Final[str] = (
    "dfl_schedule_candidate_library_v4_not_full_dfl"
)
DFL_CANDIDATE_VALUE_LABEL_PANEL_V4_CLAIM_SCOPE: Final[str] = (
    "dfl_candidate_value_label_panel_v4_not_full_dfl"
)

CANDIDATE_FAMILY_QUANTILE_RISK_V4: Final[str] = "calibrated_quantile_risk_v4"
CANDIDATE_FAMILY_BLOCK_PEAK_V4: Final[str] = "block_structured_peak_schedule_v4"
CANDIDATE_FAMILY_SOC_RESERVE_V4: Final[str] = "soc_terminal_reserve_v4"
CANDIDATE_FAMILY_SPREAD_VOLATILITY_V4: Final[str] = "spread_volatility_robust_v4"
CANDIDATE_FAMILY_TENANT_DEGRADATION_V4: Final[str] = (
    "tenant_degradation_throughput_sweep_v4"
)
CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V4: Final[str] = (
    "oracle_neighborhood_diagnostic_v4"
)
V4_GENERATED_CANDIDATE_FAMILIES: Final[frozenset[str]] = frozenset(
    {
        CANDIDATE_FAMILY_QUANTILE_RISK_V4,
        CANDIDATE_FAMILY_BLOCK_PEAK_V4,
        CANDIDATE_FAMILY_SOC_RESERVE_V4,
        CANDIDATE_FAMILY_SPREAD_VOLATILITY_V4,
        CANDIDATE_FAMILY_TENANT_DEGRADATION_V4,
        CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V4,
    }
)

LEARNED_SCORER_TYPE_V4: Final[str] = "learned_linear_candidate_value_v4"
LEARNED_SCORER_PROFILE_NAME_V4: Final[str] = "learned_candidate_value_ridge_v4"
LEARNED_SCORER_FEATURE_COLUMNS_V4: Final[tuple[str, ...]] = (
    *v3.LEARNED_SCORER_FEATURE_COLUMNS,
    "selector_feature_forecast_volatility_uah_mwh",
    "selector_feature_terminal_soc_fraction",
    "selector_feature_dispatch_reversal_count",
    "selector_feature_peak_hour_index",
    "selector_feature_trough_hour_index",
    "selector_feature_anchor_hour",
)
REQUIRED_PLATEAU_AUTOPSY_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "plateau_cause",
        "best_candidate_family",
        "selected_family",
        "v2_plus_regret_uah",
        "best_candidate_regret_uah",
        "selected_regret_uah",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)
REQUIRED_DATA_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "audit_area",
        "audit_status",
        "row_count",
        "gap_count",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)
REQUIRED_LABEL_PANEL_COLUMNS_V4: Final[frozenset[str]] = frozenset(
    {
        *v3.REQUIRED_LABEL_PANEL_COLUMNS,
        "selector_feature_forecast_volatility_uah_mwh",
        "selector_feature_terminal_soc_fraction",
        "selector_feature_dispatch_reversal_count",
        "selector_feature_peak_hour_index",
        "selector_feature_trough_hour_index",
        "selector_feature_anchor_hour",
    }
)


def candidate_value_dfl_v4_model_name(source_model_name: str) -> str:
    """Return the stable candidate-value DFL v4 model name."""

    return f"{CANDIDATE_VALUE_DFL_V4_PREFIX}{source_model_name}"


def build_dfl_v2_v3_plateau_autopsy_frame(
    schedule_candidate_library_v3_frame: pl.DataFrame,
    candidate_value_label_panel_v3_frame: pl.DataFrame,
    candidate_value_dfl_v3_frame: pl.DataFrame,
    candidate_value_dfl_v3_strict_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Classify why V3 matched or failed to improve over frozen V2+."""

    v2._validate_library_frame(schedule_candidate_library_v3_frame)
    v3._validate_label_panel_frame(candidate_value_label_panel_v3_frame)
    v3._validate_candidate_value_model_frame(candidate_value_dfl_v3_frame)
    v2._require_columns(
        candidate_value_dfl_v3_strict_frame,
        v3.REQUIRED_STRICT_COLUMNS,
        frame_name="candidate_value_dfl_v3_strict_frame",
    )
    library_by_key = {
        _candidate_identity_key(row): row
        for row in schedule_candidate_library_v3_frame.iter_rows(named=True)
    }
    model_by_tenant_source = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in candidate_value_dfl_v3_frame.iter_rows(named=True)
    }
    strict_rows = list(candidate_value_dfl_v3_strict_frame.iter_rows(named=True))
    v2_plus_by_anchor = _strict_role_rows_by_anchor(
        strict_rows,
        role="schedule_value_learner_v2_plus_reference",
    )
    selected_by_anchor = _strict_role_rows_by_anchor(
        strict_rows,
        role="candidate_value_dfl_v3",
    )
    label_rows = [
        row
        for row in candidate_value_label_panel_v3_frame.iter_rows(named=True)
        if str(row["split_name"]) == "final_holdout"
        and "oracle_neighborhood_diagnostic" not in str(row["candidate_family"])
    ]
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in label_rows:
        grouped.setdefault(_tenant_source_anchor_key(row), []).append(row)

    rows: list[dict[str, Any]] = []
    for key, anchor_label_rows in sorted(grouped.items()):
        tenant_id, source_model_name, anchor_timestamp = key
        v2_plus_row = v2_plus_by_anchor[key]
        selected_row = selected_by_anchor[key]
        model_row = model_by_tenant_source[(tenant_id, source_model_name)]
        best_label_row = min(
            anchor_label_rows,
            key=lambda row: (
                float(row["label_regret_uah"]),
                v2._family_sort_index(str(row["candidate_family"])),
                str(row["candidate_model_name"]),
            ),
        )
        best_library_row = library_by_key[_candidate_identity_key(best_label_row)]
        selected_family = str(
            dict(v2._payload(selected_row)).get("selector_row_candidate_family", "")
        )
        v2_plus_regret = float(v2_plus_row["regret_uah"])
        selected_regret = float(selected_row["regret_uah"])
        best_regret = float(best_label_row["label_regret_uah"])
        fallback_to_v2_plus = bool(model_row["fallback_to_v2_plus"])
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor_timestamp,
                "anchor_window": "final_holdout",
                "plateau_cause": _plateau_cause(
                    v2_plus_regret=v2_plus_regret,
                    selected_regret=selected_regret,
                    best_regret=best_regret,
                    fallback_to_v2_plus=fallback_to_v2_plus,
                ),
                "best_candidate_family": str(best_label_row["candidate_family"]),
                "best_candidate_model_name": str(best_label_row["candidate_model_name"]),
                "selected_family": selected_family,
                "fallback_to_v2_plus": fallback_to_v2_plus,
                "v2_plus_regret_uah": v2_plus_regret,
                "best_candidate_regret_uah": best_regret,
                "selected_regret_uah": selected_regret,
                "best_minus_v2_plus_regret_uah": best_regret - v2_plus_regret,
                "selected_minus_best_regret_uah": selected_regret - best_regret,
                "price_regime": _price_regime(best_library_row),
                "spread_volatility_bucket": _spread_volatility_bucket(best_library_row),
                "soc_binding_bucket": _soc_binding_bucket(best_library_row),
                "throughput_bucket": _throughput_bucket(best_library_row),
                "terminal_soc_bucket": _terminal_soc_bucket(best_library_row),
                "claim_scope": CANDIDATE_VALUE_DFL_V4_PLATEAU_AUTOPSY_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id", "anchor_timestamp"])


def validate_dfl_v2_v3_plateau_autopsy_evidence(
    plateau_autopsy_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate plateau-autopsy evidence and claim boundaries."""

    missing_columns = sorted(
        REQUIRED_PLATEAU_AUTOPSY_COLUMNS.difference(plateau_autopsy_frame.columns)
    )
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"V2+/V3 plateau autopsy is missing columns: {missing_columns}",
            {"row_count": plateau_autopsy_frame.height},
        )
    failures = _claim_boundary_failures(
        plateau_autopsy_frame,
        expected_claim_scope=CANDIDATE_VALUE_DFL_V4_PLATEAU_AUTOPSY_CLAIM_SCOPE,
    )
    return EvidenceCheckOutcome(
        not failures,
        "V2+/V3 plateau autopsy preserves research claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": plateau_autopsy_frame.height,
            "plateau_cause_counts": _value_counts(
                plateau_autopsy_frame, column="plateau_cause"
            ),
            "market_execution_enabled": False,
        },
    )


def build_dfl_plateau_data_quality_audit_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    benchmark_context_frame: pl.DataFrame,
    plateau_autopsy_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Audit data/context gaps before adding another DFL/DT model."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    v2._require_columns(
        plateau_autopsy_frame,
        REQUIRED_PLATEAU_AUTOPSY_COLUMNS,
        frame_name="plateau_autopsy_frame",
    )
    library_rows = list(schedule_candidate_library_frame.iter_rows(named=True))
    rows = [
        _data_audit_row(
            audit_area="ukrainian_dam_history",
            audit_status="ready"
            if _observed_thesis_grade_ratio(library_rows) >= 0.99
            else "gap_detected",
            row_count=len(library_rows),
            gap_count=sum(
                1 for row in library_rows if str(row.get("data_quality_tier")) != "thesis_grade"
            ),
            details={
                "observed_thesis_grade_ratio": _observed_thesis_grade_ratio(library_rows),
            },
        ),
        _data_audit_row(
            audit_area="weather_load_context",
            audit_status="ready"
            if _has_prefix_column(benchmark_context_frame, "weather_")
            and _has_substring_column(benchmark_context_frame, "load")
            else "gap_detected",
            row_count=benchmark_context_frame.height,
            gap_count=0
            if _has_prefix_column(benchmark_context_frame, "weather_")
            and _has_substring_column(benchmark_context_frame, "load")
            else 1,
            details={
                "weather_columns": _columns_with_prefix(benchmark_context_frame, "weather_"),
                "load_columns": _columns_with_substring(benchmark_context_frame, "load"),
            },
        ),
        _data_audit_row(
            audit_area="calendar_event_context",
            audit_status="ready"
            if any(
                _has_substring_column(benchmark_context_frame, token)
                for token in ("holiday", "calendar", "event", "outage", "grid")
            )
            else "gap_detected",
            row_count=benchmark_context_frame.height,
            gap_count=0
            if any(
                _has_substring_column(benchmark_context_frame, token)
                for token in ("holiday", "calendar", "event", "outage", "grid")
            )
            else 1,
            details={
                "context_columns": [
                    column
                    for column in benchmark_context_frame.columns
                    if any(
                        token in column.lower()
                        for token in ("holiday", "calendar", "event", "outage", "grid")
                    )
                ],
            },
        ),
        _data_audit_row(
            audit_area="publication_time_availability",
            audit_status="ready"
            if any(
                _has_substring_column(benchmark_context_frame, token)
                for token in ("publication", "source_timestamp", "known_future")
            )
            else "gap_detected",
            row_count=benchmark_context_frame.height,
            gap_count=0
            if any(
                _has_substring_column(benchmark_context_frame, token)
                for token in ("publication", "source_timestamp", "known_future")
            )
            else 1,
            details={
                "publication_columns": [
                    column
                    for column in benchmark_context_frame.columns
                    if any(
                        token in column.lower()
                        for token in ("publication", "source_timestamp", "known_future")
                    )
                ],
            },
        ),
        _data_audit_row(
            audit_area="regret_cluster_alignment",
            audit_status="ready" if plateau_autopsy_frame.height else "gap_detected",
            row_count=plateau_autopsy_frame.height,
            gap_count=0 if plateau_autopsy_frame.height else 1,
            details={
                "plateau_cause_counts": _value_counts(
                    plateau_autopsy_frame,
                    column="plateau_cause",
                ),
            },
        ),
    ]
    return pl.DataFrame(rows).sort("audit_area")


def validate_dfl_plateau_data_quality_audit_evidence(
    data_quality_audit_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate plateau data-quality audit rows."""

    missing_columns = sorted(
        REQUIRED_DATA_AUDIT_COLUMNS.difference(data_quality_audit_frame.columns)
    )
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"plateau data-quality audit is missing columns: {missing_columns}",
            {"row_count": data_quality_audit_frame.height},
        )
    failures = _claim_boundary_failures(
        data_quality_audit_frame,
        expected_claim_scope=CANDIDATE_VALUE_DFL_V4_DATA_AUDIT_CLAIM_SCOPE,
    )
    return EvidenceCheckOutcome(
        not failures,
        "Plateau data-quality audit preserves research claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": data_quality_audit_frame.height,
            "audit_status_counts": _value_counts(
                data_quality_audit_frame,
                column="audit_status",
            ),
            "market_execution_enabled": False,
        },
    )


def build_dfl_schedule_candidate_library_v4_frame(
    schedule_candidate_library_v3_frame: pl.DataFrame,
    *,
    quantile_risk_spread_scales: tuple[float, ...] = (1.25, 1.5),
    block_peak_delta_uah_mwh: float = 225.0,
    terminal_reserve_shift_uah_mwh: float = 250.0,
    spread_volatility_scale: float = 0.65,
    tenant_sweep_spread_scales: tuple[float, ...] = (0.75, 1.35),
    include_train_oracle_neighborhood: bool = True,
    max_train_generation_anchor_count_per_tenant: int | None = 20,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Expand V3 with stronger V4 candidate schedules."""

    v2._validate_library_frame(schedule_candidate_library_v3_frame)
    if any(scale <= 0.0 for scale in quantile_risk_spread_scales):
        raise ValueError("quantile_risk_spread_scales must contain positive values.")
    if any(scale <= 0.0 for scale in tenant_sweep_spread_scales):
        raise ValueError("tenant_sweep_spread_scales must contain positive values.")
    if spread_volatility_scale <= 0.0:
        raise ValueError("spread_volatility_scale must be positive.")
    resolved_generated_at = generated_at or v2._latest_generated_at(
        schedule_candidate_library_v3_frame
    )
    rows = [
        _with_v4_library_claim(row, version="v4_source")
        for row in schedule_candidate_library_v3_frame.iter_rows(named=True)
    ]
    grouped = v2_plus._rows_by_tenant_source_anchor(schedule_candidate_library_v3_frame)
    generation_keys = _generation_anchor_keys(
        grouped,
        max_train_generation_anchor_count_per_tenant=(
            max_train_generation_anchor_count_per_tenant
        ),
    )
    for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2])):
        if key not in generation_keys:
            continue
        tenant_id, source_model_name, _anchor_timestamp = key
        anchor_rows = grouped[key]
        strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
        raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
        raw_forecast = v2._float_list(
            raw_row["forecast_price_uah_mwh_vector"],
            field_name="raw forecast",
        )
        for scale in quantile_risk_spread_scales:
            rows.append(
                _generated_v4_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_QUANTILE_RISK_V4,
                    candidate_model_name=(
                        f"dfl_candidate_library_v4_quantile_risk_"
                        f"{source_model_name}_{scale:.2f}"
                    ),
                    forecast_prices=_bounded_prices(
                        v2_plus._rank_extrema_perturbation(
                            v2_plus._scale_spread(raw_forecast, scale=scale),
                            delta=125.0,
                        )
                    ),
                    generated_at=resolved_generated_at,
                    metadata={"quantile_risk_spread_scale": scale},
                )
            )
        rows.append(
            _generated_v4_candidate(
                strict_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_BLOCK_PEAK_V4,
                candidate_model_name=f"dfl_candidate_library_v4_block_peak_{source_model_name}",
                forecast_prices=_bounded_prices(
                    _block_peak_adjusted(
                        raw_forecast,
                        anchor_timestamp=v2._datetime_value(
                            strict_row["anchor_timestamp"],
                            field_name="anchor_timestamp",
                        ),
                        delta=block_peak_delta_uah_mwh,
                    )
                ),
                generated_at=resolved_generated_at,
                metadata={"block_peak_delta_uah_mwh": block_peak_delta_uah_mwh},
            )
        )
        rows.append(
            _generated_v4_candidate(
                strict_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_SOC_RESERVE_V4,
                candidate_model_name=f"dfl_candidate_library_v4_soc_reserve_{source_model_name}",
                forecast_prices=_bounded_prices(
                    v2_plus._terminal_target_adjustment(
                        raw_forecast,
                        shift_uah_mwh=terminal_reserve_shift_uah_mwh,
                    )
                ),
                generated_at=resolved_generated_at,
                metadata={"terminal_reserve_shift_uah_mwh": terminal_reserve_shift_uah_mwh},
            )
        )
        rows.append(
            _generated_v4_candidate(
                strict_row,
                source_model_name=source_model_name,
                candidate_family=CANDIDATE_FAMILY_SPREAD_VOLATILITY_V4,
                candidate_model_name=(
                    f"dfl_candidate_library_v4_spread_volatility_{source_model_name}"
                ),
                forecast_prices=_bounded_prices(
                    v2_plus._scale_spread(
                        v2_plus._rank_extrema_perturbation(raw_forecast, delta=175.0),
                        scale=spread_volatility_scale,
                    )
                ),
                generated_at=resolved_generated_at,
                metadata={"spread_volatility_scale": spread_volatility_scale},
            )
        )
        for scale in tenant_sweep_spread_scales:
            rows.append(
                _generated_v4_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_TENANT_DEGRADATION_V4,
                    candidate_model_name=(
                        f"dfl_candidate_library_v4_tenant_sweep_{source_model_name}_"
                        f"{_tenant_slug(tenant_id)}_{scale:.2f}"
                    ),
                    forecast_prices=_bounded_prices(
                        v2_plus._scale_spread(raw_forecast, scale=scale)
                    ),
                    generated_at=resolved_generated_at,
                    metadata={
                        "tenant_sweep_spread_scale": scale,
                        "tenant_id": tenant_id,
                    },
                )
            )
        if include_train_oracle_neighborhood and str(strict_row["split_name"]) != "final_holdout":
            rows.append(
                _generated_v4_candidate(
                    strict_row,
                    source_model_name=source_model_name,
                    candidate_family=CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V4,
                    candidate_model_name=(
                        f"dfl_candidate_library_v4_oracle_train_only_{source_model_name}"
                    ),
                    forecast_prices=v2._float_list(
                        strict_row["actual_price_uah_mwh_vector"],
                        field_name="actual oracle-neighborhood prices",
                    ),
                    generated_at=resolved_generated_at,
                    metadata={
                        "analysis_only_oracle_neighborhood": True,
                        "train_only": True,
                    },
                )
            )
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_candidate_value_label_panel_v4_frame(
    schedule_candidate_library_v4_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build V4 candidate features and realized value labels."""

    v2._validate_library_frame(schedule_candidate_library_v4_frame)
    rows: list[dict[str, Any]] = []
    grouped = v2_plus._rows_by_tenant_source_anchor(schedule_candidate_library_v4_frame)
    for key in sorted(grouped, key=lambda item: (item[0], item[1], item[2])):
        anchor_rows = grouped[key]
        best_regret = min(float(row["regret_uah"]) for row in anchor_rows)
        strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
        strict_regret = float(strict_row["regret_uah"])
        for row in sorted(
            anchor_rows,
            key=lambda item: (
                str(item["candidate_family"]),
                str(item["candidate_model_name"]),
            ),
        ):
            regret = float(row["regret_uah"])
            rows.append(
                {
                    "tenant_id": str(row["tenant_id"]),
                    "source_model_name": str(row["source_model_name"]),
                    "candidate_family": str(row["candidate_family"]),
                    "candidate_model_name": str(row["candidate_model_name"]),
                    "anchor_timestamp": v2._datetime_value(
                        row["anchor_timestamp"],
                        field_name="anchor_timestamp",
                    ),
                    "split_name": str(row["split_name"]),
                    "horizon_hours": int(row["horizon_hours"]),
                    **_selector_feature_values_v4(row),
                    "selector_feature_candidate_library_version": str(
                        row.get("candidate_library_version", "unknown")
                    ),
                    "label_regret_uah": regret,
                    "label_decision_value_uah": float(row["decision_value_uah"]),
                    "label_oracle_value_uah": float(row["oracle_value_uah"]),
                    "label_is_anchor_best_candidate": abs(regret - best_regret) <= 1e-9,
                    "label_regret_margin_to_anchor_best_uah": regret - best_regret,
                    "label_value_margin_vs_strict_uah": strict_regret - regret,
                    "label_value_tier": v3._value_tier(
                        regret=regret,
                        best_regret=best_regret,
                    ),
                    "claim_scope": DFL_CANDIDATE_VALUE_LABEL_PANEL_V4_CLAIM_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                    "market_execution_enabled": False,
                    "evaluation_payload": {
                        "claim_scope": DFL_CANDIDATE_VALUE_LABEL_PANEL_V4_CLAIM_SCOPE,
                        "source_candidate_claim_scope": str(row.get("claim_scope", "")),
                        "selector_features_prior_only": True,
                        "labels_are_realized_scoring_outcomes": True,
                        "not_full_dfl": True,
                        "not_market_execution": True,
                        "market_execution_enabled": False,
                    },
                }
            )
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def validate_dfl_candidate_value_label_panel_v4_evidence(
    label_panel_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate V4 label-panel structure and claim boundaries."""

    missing_columns = sorted(
        REQUIRED_LABEL_PANEL_COLUMNS_V4.difference(label_panel_frame.columns)
    )
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"candidate-value DFL v4 label panel is missing columns: {missing_columns}",
            {"row_count": label_panel_frame.height},
        )
    failures = _claim_boundary_failures(
        label_panel_frame,
        expected_claim_scope=DFL_CANDIDATE_VALUE_LABEL_PANEL_V4_CLAIM_SCOPE,
    )
    return EvidenceCheckOutcome(
        not failures,
        "Candidate-value DFL v4 label panel keeps prior features separate from realized labels."
        if not failures
        else "; ".join(failures),
        {
            "row_count": label_panel_frame.height,
            "selector_feature_columns": [
                column
                for column in label_panel_frame.columns
                if column.startswith("selector_feature_")
            ],
            "market_execution_enabled": False,
        },
    )


def build_dfl_candidate_value_dfl_v4_frame(
    schedule_candidate_library_v4_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    candidate_value_label_panel_v4_frame: pl.DataFrame | None = None,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01,
    ridge_l2: float = 1.0,
) -> pl.DataFrame:
    """Train/select a V4 candidate-level value scorer with V2+ fallback."""

    v3._validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            min_prior_mean_improvement_ratio_vs_v2_plus
        ),
    )
    v2._validate_library_frame(schedule_candidate_library_v4_frame)
    v3._validate_v2_plus_model_frame(learner_v2_plus_frame)
    label_panel_frame = (
        candidate_value_label_panel_v4_frame
        if candidate_value_label_panel_v4_frame is not None
        else build_dfl_candidate_value_label_panel_v4_frame(
            schedule_candidate_library_v4_frame
        )
    )
    _validate_label_panel_frame_v4(label_panel_frame)
    label_rows_by_key = _label_rows_by_key(label_panel_frame)
    v2_plus_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_plus_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = v2._library_rows(
                schedule_candidate_library_v4_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [
                row for row in source_rows if str(row["split_name"]) == "train_selection"
            ]
            final_rows = [
                row for row in source_rows if str(row["split_name"]) == "final_holdout"
            ]
            final_anchor_count = len(v2._anchor_set(final_rows))
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} final-holdout tenant-anchor count must be "
                    f"{final_validation_anchor_count_per_tenant}; observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} candidate-value DFL v4 needs train rows"
                )
            v2_plus_row = v2_plus_rows.get((tenant_id, source_model_name))
            if v2_plus_row is None:
                raise ValueError(f"missing V2+ learner row for {tenant_id}/{source_model_name}")
            eligible_families = v3._eligible_candidate_families(
                train_rows,
                final_rows,
                required_final_anchor_count=final_anchor_count,
            )
            train_label_rows = _label_rows_for_library_rows(
                train_rows,
                label_rows_by_key=label_rows_by_key,
                candidate_families=eligible_families,
            )
            final_label_rows = _label_rows_for_library_rows(
                final_rows,
                label_rows_by_key=label_rows_by_key,
                candidate_families=eligible_families,
            )
            learned_scorer = _fit_learned_candidate_value_scorer_v4(
                train_label_rows,
                candidate_families=eligible_families,
                ridge_l2=ridge_l2,
            )
            selected_train_rows = _select_rows_by_learned_scorer_v4(
                train_rows,
                label_rows_by_key=label_rows_by_key,
                scorer=learned_scorer,
                candidate_families=eligible_families,
            )
            selected_final_rows = _select_rows_by_learned_scorer_v4(
                final_rows,
                label_rows_by_key=label_rows_by_key,
                scorer=learned_scorer,
                candidate_families=eligible_families,
            )
            v2_plus_train_mean = float(v2_plus_row["selected_train_mean_regret_uah"])
            v2_plus_final_mean = float(v2_plus_row["selected_final_mean_regret_uah"])
            selected_train_mean = v2._mean_regret(selected_train_rows)
            fallback_to_v2_plus = (
                v2._improvement_ratio(v2_plus_train_mean, selected_train_mean)
                < min_prior_mean_improvement_ratio_vs_v2_plus
            )
            effective_train_rows = [] if fallback_to_v2_plus else selected_train_rows
            effective_final_rows = [] if fallback_to_v2_plus else selected_final_rows
            strict_final_rows = v2._selected_family_rows(
                final_rows,
                v2.CANDIDATE_FAMILY_STRICT,
            )
            raw_final_rows = v2._selected_family_rows(final_rows, v2.CANDIDATE_FAMILY_RAW)
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": candidate_value_dfl_v4_model_name(
                        source_model_name
                    ),
                    "selected_value_profile_name": str(learned_scorer["name"]),
                    "selected_scorer_type": LEARNED_SCORER_TYPE_V4,
                    "selected_objective_name": (
                        "candidate_value_v4_regime_feature_ridge_pairwise_ranking"
                    ),
                    "selected_feature_names": list(LEARNED_SCORER_FEATURE_COLUMNS_V4),
                    "selected_feature_weights": dict(learned_scorer["weights"]),
                    "selected_feature_means": dict(learned_scorer["feature_means"]),
                    "selected_feature_scales": dict(learned_scorer["feature_scales"]),
                    "eligible_candidate_families": sorted(eligible_families),
                    "teacher_family_scores": v3._teacher_family_scores(
                        train_rows,
                        candidate_families=eligible_families,
                    ),
                    "fallback_to_v2_plus": fallback_to_v2_plus,
                    "train_anchor_count": len(v2._anchor_set(train_rows)),
                    "final_holdout_anchor_count": final_anchor_count,
                    "final_holdout_tenant_anchor_count": final_anchor_count
                    * len(tenant_ids),
                    "strict_final_mean_regret_uah": v2._mean_regret(strict_final_rows),
                    "raw_final_mean_regret_uah": v2._mean_regret(raw_final_rows),
                    "v2_plus_train_mean_regret_uah": v2_plus_train_mean,
                    "v2_plus_final_mean_regret_uah": v2_plus_final_mean,
                    "candidate_train_mean_regret_uah": selected_train_mean,
                    "candidate_train_pairwise_loss_uah": _pairwise_regret_weighted_loss_v4(
                        train_label_rows,
                        scorer=learned_scorer,
                        candidate_families=eligible_families,
                    ),
                    "candidate_train_label_row_count": len(train_label_rows),
                    "candidate_final_label_row_count": len(final_label_rows),
                    "candidate_final_mean_regret_uah": v2._mean_regret(
                        selected_final_rows
                    ),
                    "selected_train_mean_regret_uah": (
                        v2_plus_train_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_train_rows)
                    ),
                    "selected_final_mean_regret_uah": (
                        v2_plus_final_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_final_rows)
                    ),
                    "selected_train_median_regret_uah": (
                        float(v2_plus_row["selected_train_median_regret_uah"])
                        if fallback_to_v2_plus
                        else v2._median_regret(effective_train_rows)
                    ),
                    "selected_final_median_regret_uah": (
                        float(v2_plus_row["selected_final_median_regret_uah"])
                        if fallback_to_v2_plus
                        else v2._median_regret(effective_final_rows)
                    ),
                    "candidate_train_family_counts": v2._family_counts(
                        selected_train_rows
                    ),
                    "candidate_final_family_counts": v2._family_counts(
                        selected_final_rows
                    ),
                    "selected_train_family_counts": (
                        dict(v2_plus_row["selected_train_family_counts"])
                        if fallback_to_v2_plus
                        else v2._family_counts(effective_train_rows)
                    ),
                    "selected_final_family_counts": (
                        dict(v2_plus_row["selected_final_family_counts"])
                        if fallback_to_v2_plus
                        else v2._family_counts(effective_final_rows)
                    ),
                    "train_mean_regret_improvement_ratio_vs_v2_plus": v2._improvement_ratio(
                        v2_plus_train_mean,
                        v2_plus_train_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_train_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_v2_plus": v2._improvement_ratio(
                        v2_plus_final_mean,
                        v2_plus_final_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_final_rows),
                    ),
                    "final_mean_regret_improvement_ratio_vs_strict": v2._improvement_ratio(
                        v2._mean_regret(strict_final_rows),
                        v2_plus_final_mean
                        if fallback_to_v2_plus
                        else v2._mean_regret(effective_final_rows),
                    ),
                    "claim_scope": CANDIDATE_VALUE_DFL_V4_CLAIM_SCOPE,
                    "academic_scope": CANDIDATE_VALUE_DFL_V4_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_candidate_value_dfl_v4_strict_lp_benchmark_frame(
    schedule_candidate_library_v4_frame: pl.DataFrame,
    candidate_value_dfl_v4_frame: pl.DataFrame,
    v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/V2+/candidate-value rows for the V4 gate."""

    v2._validate_library_frame(schedule_candidate_library_v4_frame)
    _validate_candidate_value_model_frame_v4(candidate_value_dfl_v4_frame)
    resolved_generated_at = generated_at or v2._latest_generated_at(v2_plus_strict_frame)
    library_rows = list(schedule_candidate_library_v4_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for learner_row in candidate_value_dfl_v4_frame.iter_rows(named=True):
        tenant_id = str(learner_row["tenant_id"])
        source_model_name = str(learner_row["source_model_name"])
        final_rows = [
            row
            for row in library_rows
            if str(row["tenant_id"]) == tenant_id
            and str(row["source_model_name"]) == source_model_name
            and str(row["split_name"]) == "final_holdout"
        ]
        final_anchors = sorted(v2._anchor_set(final_rows))
        v2_plus_by_anchor = v3._v2_plus_reference_rows(
            v2_plus_strict_frame,
            tenant_id=tenant_id,
            source_model_name=source_model_name,
        )
        candidate_rows = _select_rows_by_model_row_v4(
            final_rows,
            learner_row=learner_row,
            candidate_families=frozenset(
                str(family) for family in learner_row["eligible_candidate_families"]
            ),
        )
        candidate_by_anchor = {
            v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"): row
            for row in candidate_rows
        }
        for anchor_timestamp in final_anchors:
            anchor_rows = [
                row
                for row in final_rows
                if v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                == anchor_timestamp
            ]
            strict_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
            raw_row = v2._single_family_row(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
            v2_plus_row = v2_plus_by_anchor[anchor_timestamp]
            selected_row = (
                v2_plus_row
                if bool(learner_row["fallback_to_v2_plus"])
                else candidate_by_anchor[anchor_timestamp]
            )
            rows.extend(
                [
                    _strict_benchmark_row_v4(
                        strict_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="strict_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row_v4(
                        raw_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="raw_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row_v4(
                        v2_plus_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_plus_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row_v4(
                        selected_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="candidate_value_dfl_v4",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_candidate_value_dfl_v4_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural V4 evidence without requiring headline replacement."""

    missing_columns = sorted(v3.REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"candidate-value DFL v4 evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(
            False,
            "candidate-value DFL v4 evidence has no rows",
            {"row_count": 0},
        )
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    failures: list[str] = []
    summaries: list[dict[str, Any]] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary_v4(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio_vs_v2_plus=0.0,
            min_mean_regret_improvement_ratio_vs_strict=(
                DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
            ),
            include_promotion_failures=False,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    return EvidenceCheckOutcome(
        not failures,
        "Candidate-value DFL v4 evidence has valid coverage and claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": strict_frame.height,
            "source_model_count": len(source_names),
            "source_model_names": list(source_names),
            "model_summaries": summaries,
        },
    )


def evaluate_dfl_candidate_value_dfl_v4_gate(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = 0.0,
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Evaluate V4 against strict and frozen V2+."""

    missing_columns = sorted(v3.REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns))
    if missing_columns:
        return PromotionGateResult(
            False,
            "blocked",
            f"candidate-value DFL v4 strict frame is missing required columns: {missing_columns}",
            {},
        )
    rows = list(strict_frame.iter_rows(named=True))
    if not rows:
        return PromotionGateResult(
            False,
            "blocked",
            "candidate-value DFL v4 strict frame has no rows",
            {},
        )
    source_names = source_model_names or tuple(
        sorted({_source_model_name(row) for row in rows})
    )
    summaries: list[dict[str, Any]] = []
    failures: list[str] = []
    for source_model_name in source_names:
        summary, summary_failures = _gate_summary_v4(
            rows,
            source_model_name=source_model_name,
            min_tenant_count=min_tenant_count,
            min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
            min_mean_regret_improvement_ratio_vs_v2_plus=(
                min_mean_regret_improvement_ratio_vs_v2_plus
            ),
            min_mean_regret_improvement_ratio_vs_strict=(
                min_mean_regret_improvement_ratio_vs_strict
            ),
            include_promotion_failures=True,
        )
        summaries.append(summary)
        failures.extend(summary_failures)
    replacement_passing = [
        summary for summary in summaries if summary["replacement_gate_passed"]
    ]
    development_passing = [
        summary for summary in summaries if summary["development_gate_passed"]
    ]
    best = max(
        summaries,
        key=lambda summary: float(summary["mean_regret_improvement_ratio_vs_v2_plus"]),
    )
    metrics = {
        "best_source_model_name": best["source_model_name"],
        "tenant_count": best["tenant_count"],
        "validation_tenant_anchor_count": best["validation_tenant_anchor_count"],
        "strict_mean_regret_uah": best["strict_mean_regret_uah"],
        "raw_mean_regret_uah": best["raw_mean_regret_uah"],
        "v2_plus_mean_regret_uah": best["v2_plus_mean_regret_uah"],
        "selected_mean_regret_uah": best["selected_mean_regret_uah"],
        "strict_median_regret_uah": best["strict_median_regret_uah"],
        "v2_plus_median_regret_uah": best["v2_plus_median_regret_uah"],
        "selected_median_regret_uah": best["selected_median_regret_uah"],
        "mean_regret_improvement_ratio_vs_strict": best[
            "mean_regret_improvement_ratio_vs_strict"
        ],
        "mean_regret_improvement_ratio_vs_raw": best[
            "mean_regret_improvement_ratio_vs_raw"
        ],
        "mean_regret_improvement_ratio_vs_v2_plus": best[
            "mean_regret_improvement_ratio_vs_v2_plus"
        ],
        "development_gate_passed": bool(development_passing),
        "offline_strategy_replacement_passed": bool(replacement_passing),
        "market_execution_enabled": False,
        "passing_source_model_names": [
            str(summary["source_model_name"]) for summary in replacement_passing
        ],
        "model_summaries": summaries,
    }
    if replacement_passing and not failures:
        return PromotionGateResult(
            True,
            "replace_v2_plus",
            "candidate-value DFL v4 passes strict LP/oracle and frozen V2+ gate",
            metrics,
        )
    if development_passing:
        return PromotionGateResult(
            False,
            "diagnostic_pass_replacement_blocked",
            "candidate-value DFL v4 improves over raw neural schedules but remains "
            "blocked versus V2+ evidence: " + "; ".join(failures),
            metrics,
        )
    return PromotionGateResult(
        False,
        "blocked",
        "; ".join(failures)
        if failures
        else "candidate-value DFL v4 has no development improvement",
        metrics,
    )


def _plateau_cause(
    *,
    v2_plus_regret: float,
    selected_regret: float,
    best_regret: float,
    fallback_to_v2_plus: bool,
) -> str:
    if best_regret >= v2_plus_regret - 1e-9:
        return "candidate_not_better"
    if fallback_to_v2_plus and selected_regret > best_regret + 1e-9:
        return "fallback_too_conservative"
    if selected_regret > best_regret + 1e-9:
        return "candidate_available_but_not_selected"
    return "candidate_selected_but_not_replacement"


def _data_audit_row(
    *,
    audit_area: str,
    audit_status: str,
    row_count: int,
    gap_count: int,
    details: dict[str, Any],
) -> dict[str, Any]:
    return {
        "audit_area": audit_area,
        "audit_status": audit_status,
        "row_count": row_count,
        "gap_count": gap_count,
        "details": details,
        "claim_scope": CANDIDATE_VALUE_DFL_V4_DATA_AUDIT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _observed_thesis_grade_ratio(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    passing = sum(
        1
        for row in rows
        if str(row.get("data_quality_tier")) == "thesis_grade"
        and float(row.get("observed_coverage_ratio", 0.0)) >= 0.99
    )
    return passing / len(rows)


def _has_prefix_column(frame: pl.DataFrame, prefix: str) -> bool:
    return any(column.startswith(prefix) for column in frame.columns)


def _has_substring_column(frame: pl.DataFrame, token: str) -> bool:
    lowered = token.lower()
    return any(lowered in column.lower() for column in frame.columns)


def _columns_with_prefix(frame: pl.DataFrame, prefix: str) -> list[str]:
    return sorted(column for column in frame.columns if column.startswith(prefix))


def _columns_with_substring(frame: pl.DataFrame, token: str) -> list[str]:
    lowered = token.lower()
    return sorted(column for column in frame.columns if lowered in column.lower())


def _with_v4_library_claim(row: dict[str, Any], *, version: str) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(v2._payload(row))
    payload.update(
        {
            "claim_scope": DFL_SCHEDULE_CANDIDATE_LIBRARY_V4_CLAIM_SCOPE,
            "candidate_library_version": version,
            "no_leakage_prior_only": True,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied["claim_scope"] = DFL_SCHEDULE_CANDIDATE_LIBRARY_V4_CLAIM_SCOPE
    copied["candidate_library_version"] = version
    copied["evaluation_payload"] = payload
    copied["not_full_dfl"] = True
    copied["not_market_execution"] = True
    return copied


def _generated_v4_candidate(
    reference_row: dict[str, Any],
    *,
    source_model_name: str,
    candidate_family: str,
    candidate_model_name: str,
    forecast_prices: list[float],
    generated_at: datetime,
    metadata: dict[str, Any],
) -> dict[str, Any]:
    row = v2_plus._evaluated_candidate_row(
        reference_row,
        source_model_name=source_model_name,
        candidate_family=candidate_family,
        candidate_model_name=candidate_model_name,
        forecast_prices=forecast_prices,
        generated_at=generated_at,
        metadata={
            **metadata,
            "candidate_library_version": "v4_generated",
            "no_leakage_prior_only": True,
        },
    )
    return _with_v4_library_claim(row, version="v4_generated")


def _generation_anchor_keys(
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]],
    *,
    max_train_generation_anchor_count_per_tenant: int | None,
) -> set[tuple[str, str, datetime]]:
    generation_keys: set[tuple[str, str, datetime]] = set()
    train_keys_by_tenant_source: dict[tuple[str, str], list[tuple[str, str, datetime]]] = {}
    for key, rows in grouped.items():
        if not rows:
            continue
        split_name = str(rows[0]["split_name"])
        if split_name == "final_holdout":
            generation_keys.add(key)
        elif split_name == "train_selection":
            tenant_id, source_model_name, _anchor = key
            train_keys_by_tenant_source.setdefault(
                (tenant_id, source_model_name),
                [],
            ).append(key)
    for keys in train_keys_by_tenant_source.values():
        sorted_keys = sorted(keys, key=lambda item: item[2])
        if max_train_generation_anchor_count_per_tenant is None:
            selected_keys = sorted_keys
        elif max_train_generation_anchor_count_per_tenant <= 0:
            selected_keys = []
        else:
            selected_keys = sorted_keys[-max_train_generation_anchor_count_per_tenant:]
        generation_keys.update(selected_keys)
    return generation_keys


def _block_peak_adjusted(
    values: list[float],
    *,
    anchor_timestamp: datetime,
    delta: float,
) -> list[float]:
    adjusted = values.copy()
    for index, value in enumerate(values):
        delivery_hour = (anchor_timestamp.hour + index + 1) % 24
        if 7 <= delivery_hour <= 10 or 18 <= delivery_hour <= 21:
            adjusted[index] = value + delta
        elif 0 <= delivery_hour <= 4:
            adjusted[index] = value - 0.5 * delta
    return adjusted


def _bounded_prices(
    prices: list[float],
    *,
    floor_uah_mwh: float = 0.0,
    cap_uah_mwh: float = 16_000.0,
) -> list[float]:
    return [min(cap_uah_mwh, max(floor_uah_mwh, float(price))) for price in prices]


def _tenant_slug(tenant_id: str) -> str:
    return tenant_id.replace("client_", "").replace("_", "-")[:24]


def _validate_label_panel_frame_v4(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame,
        REQUIRED_LABEL_PANEL_COLUMNS_V4,
        frame_name="candidate_value_label_panel_v4_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_CANDIDATE_VALUE_LABEL_PANEL_V4_CLAIM_SCOPE:
            raise ValueError("candidate-value DFL v4 label panel has unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("candidate-value DFL v4 label rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "candidate-value DFL v4 label rows must keep not_market_execution=true"
            )


def _validate_candidate_value_model_frame_v4(frame: pl.DataFrame) -> None:
    v2._require_columns(
        frame,
        v3.REQUIRED_MODEL_COLUMNS,
        frame_name="candidate_value_dfl_v4_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != CANDIDATE_VALUE_DFL_V4_CLAIM_SCOPE:
            raise ValueError("candidate-value DFL v4 frame has unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("candidate-value DFL v4 rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "candidate-value DFL v4 rows must keep not_market_execution=true"
            )


def _label_rows_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, datetime, str, str], dict[str, Any]]:
    return {_candidate_identity_key(row): row for row in frame.iter_rows(named=True)}


def _candidate_identity_key(row: dict[str, Any]) -> tuple[str, str, datetime, str, str]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        str(row["candidate_family"]),
        str(row["candidate_model_name"]),
    )


def _tenant_source_anchor_key(row: dict[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
    )


def _strict_role_rows_by_anchor(
    rows: list[dict[str, Any]],
    *,
    role: str,
) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    result: dict[tuple[str, str, datetime], dict[str, Any]] = {}
    for row in rows:
        if _selection_role(row) != role:
            continue
        result[_tenant_source_anchor_key(row)] = row
    return result


def _label_rows_for_library_rows(
    rows: list[dict[str, Any]],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    label_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row["candidate_family"]) not in candidate_families:
            continue
        label_row = label_rows_by_key.get(_candidate_identity_key(row))
        if label_row is not None:
            label_rows.append(label_row)
    return label_rows


def _fit_learned_candidate_value_scorer_v4(
    train_label_rows: list[dict[str, Any]],
    *,
    candidate_families: frozenset[str],
    ridge_l2: float,
) -> dict[str, Any]:
    eligible_rows = [
        row
        for row in train_label_rows
        if str(row["candidate_family"]) in candidate_families
        and str(row["split_name"]) == "train_selection"
    ]
    if not eligible_rows:
        raise ValueError("candidate-value DFL v4 learned scorer needs train label rows")
    feature_means: dict[str, float] = {}
    feature_scales: dict[str, float] = {}
    for column in LEARNED_SCORER_FEATURE_COLUMNS_V4:
        values = [float(row[column]) for row in eligible_rows]
        feature_means[column] = mean(values)
        span = max(values) - min(values)
        feature_scales[column] = span if span > 1e-9 else 1.0
    family_columns = tuple(f"family::{family}" for family in sorted(candidate_families))
    feature_matrix = [
        _learned_feature_vector_v4(
            row,
            feature_means=feature_means,
            feature_scales=feature_scales,
            family_columns=family_columns,
        )
        for row in eligible_rows
    ]
    targets = [float(row["label_regret_uah"]) for row in eligible_rows]
    coefficients = v3._fit_ridge_coefficients(
        feature_matrix,
        targets,
        ridge_l2=ridge_l2,
    )
    feature_names = [*LEARNED_SCORER_FEATURE_COLUMNS_V4, *family_columns]
    weights = {"intercept": coefficients[0]}
    weights.update(
        {
            feature_name: coefficients[index + 1]
            for index, feature_name in enumerate(feature_names)
        }
    )
    return {
        "name": LEARNED_SCORER_PROFILE_NAME_V4,
        "scorer_type": LEARNED_SCORER_TYPE_V4,
        "weights": weights,
        "feature_means": feature_means,
        "feature_scales": feature_scales,
        "family_columns": family_columns,
    }


def _selector_feature_values_v4(row: dict[str, Any]) -> dict[str, float]:
    if "selector_feature_forecast_volatility_uah_mwh" in row:
        return {
            column: float(row[column])
            for column in LEARNED_SCORER_FEATURE_COLUMNS_V4
        }
    forecast_prices = v2._float_list(
        row["forecast_price_uah_mwh_vector"],
        field_name="forecast prices",
    )
    dispatch = v2._float_list(row["dispatch_mw_vector"], field_name="dispatch")
    soc = v2._float_list(row["soc_fraction_vector"], field_name="soc")
    base = v3._selector_feature_values(row)
    anchor = v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
    return {
        **base,
        "selector_feature_forecast_volatility_uah_mwh": _stddev(forecast_prices),
        "selector_feature_terminal_soc_fraction": soc[-1] if soc else 0.5,
        "selector_feature_dispatch_reversal_count": float(_dispatch_reversal_count(dispatch)),
        "selector_feature_peak_hour_index": float(_arg_extreme(forecast_prices, largest=True)),
        "selector_feature_trough_hour_index": float(_arg_extreme(forecast_prices, largest=False)),
        "selector_feature_anchor_hour": float(anchor.hour),
    }


def _learned_feature_vector_v4(
    row: dict[str, Any],
    *,
    feature_means: dict[str, float],
    feature_scales: dict[str, float],
    family_columns: tuple[str, ...],
) -> list[float]:
    features = _selector_feature_values_v4(row)
    numeric = [
        (features[column] - feature_means[column]) / feature_scales[column]
        for column in LEARNED_SCORER_FEATURE_COLUMNS_V4
    ]
    family = str(row["candidate_family"])
    one_hot = [1.0 if column == f"family::{family}" else 0.0 for column in family_columns]
    return [*numeric, *one_hot]


def _select_rows_by_learned_scorer_v4(
    rows: list[dict[str, Any]],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
    scorer: dict[str, Any],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    selected_rows: list[dict[str, Any]] = []
    for _, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        candidates = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        if not candidates:
            continue
        selected_rows.append(
            min(
                candidates,
                key=lambda row: (
                    _predict_learned_candidate_regret_v4(
                        _label_row_or_candidate_row(
                            row,
                            label_rows_by_key=label_rows_by_key,
                        ),
                        scorer=scorer,
                    ),
                    v2._family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _label_row_or_candidate_row(
    row: dict[str, Any],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
) -> dict[str, Any]:
    label_row = label_rows_by_key.get(_candidate_identity_key(row))
    if label_row is None:
        return row
    return label_row


def _select_rows_by_model_row_v4(
    rows: list[dict[str, Any]],
    *,
    learner_row: dict[str, Any],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    scorer = _learned_scorer_from_model_row_v4(learner_row)
    selected_rows: list[dict[str, Any]] = []
    for _, anchor_rows in sorted(v2._rows_by_anchor(rows).items()):
        candidates = [
            row
            for row in anchor_rows
            if str(row["candidate_family"]) in candidate_families
        ]
        if not candidates:
            continue
        selected_rows.append(
            min(
                candidates,
                key=lambda row: (
                    _predict_learned_candidate_regret_v4(row, scorer=scorer),
                    v2._family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _learned_scorer_from_model_row_v4(row: dict[str, Any]) -> dict[str, Any]:
    family_columns = tuple(
        sorted(
            key
            for key in dict(row["selected_feature_weights"])
            if key.startswith("family::")
        )
    )
    return {
        "name": str(row["selected_value_profile_name"]),
        "scorer_type": LEARNED_SCORER_TYPE_V4,
        "weights": dict(row["selected_feature_weights"]),
        "feature_means": dict(row.get("selected_feature_means", {})),
        "feature_scales": dict(row.get("selected_feature_scales", {})),
        "family_columns": family_columns,
    }


def _predict_learned_candidate_regret_v4(
    row: dict[str, Any],
    *,
    scorer: dict[str, Any],
) -> float:
    weights = dict(scorer["weights"])
    feature_means = dict(scorer["feature_means"])
    feature_scales = dict(scorer["feature_scales"])
    family_columns = tuple(str(column) for column in scorer["family_columns"])
    features = _learned_feature_vector_v4(
        row,
        feature_means=feature_means,
        feature_scales=feature_scales,
        family_columns=family_columns,
    )
    feature_names = [*LEARNED_SCORER_FEATURE_COLUMNS_V4, *family_columns]
    score = float(weights.get("intercept", 0.0))
    for feature_name, feature_value in zip(feature_names, features, strict=True):
        score += float(weights.get(feature_name, 0.0)) * feature_value
    return score


def _pairwise_regret_weighted_loss_v4(
    train_label_rows: list[dict[str, Any]],
    *,
    scorer: dict[str, Any],
    candidate_families: frozenset[str],
) -> float:
    losses: list[float] = []
    rows_by_anchor: dict[datetime, list[dict[str, Any]]] = {}
    for row in train_label_rows:
        if str(row["candidate_family"]) not in candidate_families:
            continue
        anchor = v2._datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        rows_by_anchor.setdefault(anchor, []).append(row)
    for anchor_rows in rows_by_anchor.values():
        for left_index, left in enumerate(anchor_rows):
            for right in anchor_rows[left_index + 1 :]:
                left_regret = float(left["label_regret_uah"])
                right_regret = float(right["label_regret_uah"])
                if abs(left_regret - right_regret) <= 1e-9:
                    continue
                better, worse = (left, right) if left_regret < right_regret else (right, left)
                if _predict_learned_candidate_regret_v4(
                    better,
                    scorer=scorer,
                ) > _predict_learned_candidate_regret_v4(worse, scorer=scorer):
                    losses.append(abs(left_regret - right_regret))
                else:
                    losses.append(0.0)
    return mean(losses) if losses else 0.0


def _strict_benchmark_row_v4(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(v2._payload(row))
    forecast_model_name = _forecast_model_name_for_role_v4(
        row,
        source_model_name=source_model_name,
        role=role,
    )
    anchor_timestamp = v2._datetime_value(
        row["anchor_timestamp"],
        field_name="anchor_timestamp",
    )
    candidate_family = str(
        row.get("candidate_family", payload.get("selector_row_candidate_family", ""))
    )
    candidate_model_name = str(
        row.get(
            "candidate_model_name",
            payload.get("selector_row_candidate_model_name", forecast_model_name),
        )
    )
    payload.update(
        {
            "strict_gate_kind": "candidate_value_dfl_v4_strict_lp",
            "source_forecast_model_name": source_model_name,
            "learner_model_name": candidate_value_dfl_v4_model_name(source_model_name),
            "selected_value_profile_name": str(learner_row["selected_value_profile_name"]),
            "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
            "fallback_to_v2_plus": bool(learner_row["fallback_to_v2_plus"]),
            "selector_row_candidate_family": candidate_family,
            "selector_row_candidate_model_name": candidate_model_name,
            "selection_role": role,
            "claim_scope": CANDIDATE_VALUE_DFL_V4_STRICT_CLAIM_SCOPE,
            "academic_scope": CANDIDATE_VALUE_DFL_V4_ACADEMIC_SCOPE,
            "data_quality_tier": "thesis_grade",
            "observed_coverage_ratio": 1.0,
            "safety_violation_count": v3._safety_violation_count(row),
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:candidate-value-dfl-v4:{source_model_name}:"
            f"{role}:{candidate_family}:{candidate_model_name}:"
            f"{anchor_timestamp:%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": source_model_name,
        "forecast_model_name": forecast_model_name,
        "strategy_kind": CANDIDATE_VALUE_DFL_V4_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": anchor_timestamp,
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": v3._starting_soc_fraction(row),
        "starting_soc_source": "schedule_candidate_library_v4",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v3._committed_action(row),
        "committed_power_mw": v3._committed_power_mw(row),
        "rank_by_regret": 1,
        "selection_role": role,
        "claim_scope": CANDIDATE_VALUE_DFL_V4_STRICT_CLAIM_SCOPE,
        "academic_scope": CANDIDATE_VALUE_DFL_V4_ACADEMIC_SCOPE,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": v3._safety_violation_count(row),
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": payload,
    }


def _forecast_model_name_for_role_v4(
    row: dict[str, Any],
    *,
    source_model_name: str,
    role: str,
) -> str:
    if role == "strict_reference":
        return CONTROL_MODEL_NAME
    if role == "raw_reference":
        return source_model_name
    if role == "schedule_value_learner_v2_plus_reference":
        return str(row["forecast_model_name"]) if "forecast_model_name" in row else (
            v2_plus.schedule_value_learner_v2_plus_model_name(source_model_name)
        )
    if role == "candidate_value_dfl_v4":
        return candidate_value_dfl_v4_model_name(source_model_name)
    return str(row.get("forecast_model_name", source_model_name))


def _gate_summary_v4(
    rows: list[dict[str, Any]],
    *,
    source_model_name: str,
    min_tenant_count: int,
    min_validation_tenant_anchor_count: int,
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
    include_promotion_failures: bool,
) -> tuple[dict[str, Any], list[str]]:
    failures: list[str] = []
    source_rows = [row for row in rows if _source_model_name(row) == source_model_name]
    strict_rows = [
        row for row in source_rows if _selection_role(row) == "strict_reference"
    ]
    raw_rows = [row for row in source_rows if _selection_role(row) == "raw_reference"]
    v2_plus_rows = [
        row
        for row in source_rows
        if _selection_role(row) == "schedule_value_learner_v2_plus_reference"
    ]
    selected_rows = [
        row for row in source_rows if _selection_role(row) == "candidate_value_dfl_v4"
    ]
    strict_anchors = v2._tenant_anchor_set(strict_rows)
    raw_anchors = v2._tenant_anchor_set(raw_rows)
    v2_plus_anchors = v2._tenant_anchor_set(v2_plus_rows)
    selected_anchors = v2._tenant_anchor_set(selected_rows)
    if (
        strict_anchors != raw_anchors
        or strict_anchors != v2_plus_anchors
        or strict_anchors != selected_anchors
    ):
        failures.append(
            f"{source_model_name} strict/raw/V2+/V4 rows must cover matching tenant-anchor sets"
        )
    tenant_count = len({tenant_id for tenant_id, _ in selected_anchors})
    validation_count = len(selected_anchors)
    if tenant_count < min_tenant_count:
        failures.append(
            f"{source_model_name} tenant_count must be at least {min_tenant_count}; observed {tenant_count}"
        )
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            f"{source_model_name} validation tenant-anchor count must be at least "
            f"{min_validation_tenant_anchor_count}; observed {validation_count}"
        )
    failures.extend(
        v2._provenance_failures(
            [*strict_rows, *raw_rows, *v2_plus_rows, *selected_rows]
        )
    )
    strict_mean = v2._mean_regret(strict_rows)
    raw_mean = v2._mean_regret(raw_rows)
    v2_plus_mean = v2._mean_regret(v2_plus_rows)
    selected_mean = v2._mean_regret(selected_rows)
    strict_median = v2._median_regret(strict_rows)
    v2_plus_median = v2._median_regret(v2_plus_rows)
    selected_median = v2._median_regret(selected_rows)
    improvement_vs_raw = v2._improvement_ratio(raw_mean, selected_mean)
    improvement_vs_strict = v2._improvement_ratio(strict_mean, selected_mean)
    improvement_vs_v2_plus = v2._improvement_ratio(v2_plus_mean, selected_mean)
    development_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_raw > 0.0
    )
    replacement_passed = (
        validation_count >= min_validation_tenant_anchor_count
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and improvement_vs_v2_plus > min_mean_regret_improvement_ratio_vs_v2_plus
        and selected_median <= v2_plus_median
        and not failures
    )
    if include_promotion_failures:
        if (
            selected_rows
            and v2_plus_rows
            and improvement_vs_v2_plus
            <= min_mean_regret_improvement_ratio_vs_v2_plus
        ):
            failures.append(
                f"{source_model_name} candidate-value DFL v4 must improve over frozen V2+ "
                f"by more than {min_mean_regret_improvement_ratio_vs_v2_plus:.1%}; "
                f"observed {improvement_vs_v2_plus:.1%}"
            )
        if (
            selected_rows
            and strict_rows
            and improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict
        ):
            failures.append(
                f"{source_model_name} mean regret improvement vs {CONTROL_MODEL_NAME} "
                f"must be at least {min_mean_regret_improvement_ratio_vs_strict:.1%}; "
                f"observed {improvement_vs_strict:.1%}"
            )
        if selected_rows and v2_plus_rows and selected_median > v2_plus_median:
            failures.append(
                f"{source_model_name} median regret must not be worse than frozen V2+; "
                f"observed learner={selected_median:.2f}, v2_plus={v2_plus_median:.2f}"
            )
    return (
        {
            "source_model_name": source_model_name,
            "tenant_count": tenant_count,
            "validation_tenant_anchor_count": validation_count,
            "strict_mean_regret_uah": strict_mean,
            "raw_mean_regret_uah": raw_mean,
            "v2_plus_mean_regret_uah": v2_plus_mean,
            "selected_mean_regret_uah": selected_mean,
            "strict_median_regret_uah": strict_median,
            "v2_plus_median_regret_uah": v2_plus_median,
            "selected_median_regret_uah": selected_median,
            "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
            "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
            "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2_plus,
            "development_gate_passed": development_passed,
            "replacement_gate_passed": replacement_passed,
            "market_execution_enabled": False,
        },
        failures,
    )


def _source_model_name(row: dict[str, Any]) -> str:
    return str(
        row.get(
            "source_model_name",
            dict(v2._payload(row)).get("source_forecast_model_name", ""),
        )
    )


def _selection_role(row: dict[str, Any]) -> str:
    return str(row.get("selection_role", dict(v2._payload(row)).get("selection_role", "")))


def _stddev(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    return (sum((value - avg) ** 2 for value in values) / len(values)) ** 0.5


def _dispatch_reversal_count(values: list[float]) -> int:
    signs = [1 if value > 0 else -1 if value < 0 else 0 for value in values]
    non_zero = [sign for sign in signs if sign != 0]
    return sum(
        1
        for left, right in zip(non_zero, non_zero[1:], strict=False)
        if left != right
    )


def _arg_extreme(values: list[float], *, largest: bool) -> int:
    if not values:
        return 0
    return max(range(len(values)), key=values.__getitem__) if largest else min(
        range(len(values)),
        key=values.__getitem__,
    )


def _price_regime(row: dict[str, Any]) -> str:
    spread = float(row.get("forecast_spread_uah_mwh", 0.0))
    if spread >= 5_000.0:
        return "high_spread"
    if spread >= 2_500.0:
        return "medium_spread"
    return "low_spread"


def _spread_volatility_bucket(row: dict[str, Any]) -> str:
    forecast = v2._float_list(
        row["forecast_price_uah_mwh_vector"],
        field_name="forecast prices",
    )
    volatility = _stddev(forecast)
    if volatility >= 2_500.0:
        return "high_volatility"
    if volatility >= 1_000.0:
        return "medium_volatility"
    return "low_volatility"


def _soc_binding_bucket(row: dict[str, Any]) -> str:
    slack = float(row.get("soc_min_slack_fraction", 1.0))
    if slack <= 0.05:
        return "soc_binding"
    if slack <= 0.15:
        return "soc_near_binding"
    return "soc_slack_available"


def _throughput_bucket(row: dict[str, Any]) -> str:
    throughput = float(row.get("total_throughput_mwh", 0.0))
    if throughput >= 4.0:
        return "high_throughput"
    if throughput >= 1.0:
        return "medium_throughput"
    return "low_throughput"


def _terminal_soc_bucket(row: dict[str, Any]) -> str:
    soc = v2._float_list(row["soc_fraction_vector"], field_name="soc")
    terminal = soc[-1] if soc else 0.5
    if terminal <= 0.15:
        return "low_terminal_soc"
    if terminal >= 0.85:
        return "high_terminal_soc"
    return "mid_terminal_soc"


def _value_counts(frame: pl.DataFrame, *, column: str) -> dict[str, int]:
    if frame.is_empty() or column not in frame.columns:
        return {}
    counts: dict[str, int] = {}
    for value in frame[column].to_list():
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _claim_boundary_failures(frame: pl.DataFrame, *, expected_claim_scope: str) -> list[str]:
    failures: list[str] = []
    if frame.is_empty():
        failures.append("frame must not be empty")
        return failures
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != expected_claim_scope:
            failures.append("unexpected claim_scope")
            break
        if not bool(row["not_full_dfl"]):
            failures.append("not_full_dfl must be true")
            break
        if not bool(row["not_market_execution"]):
            failures.append("not_market_execution must be true")
            break
        if bool(row.get("market_execution_enabled", False)):
            failures.append("market_execution_enabled must be false")
            break
    return failures


__all__ = [
    "CANDIDATE_FAMILY_BLOCK_PEAK_V4",
    "CANDIDATE_FAMILY_ORACLE_NEIGHBORHOOD_DIAGNOSTIC_V4",
    "CANDIDATE_FAMILY_QUANTILE_RISK_V4",
    "CANDIDATE_VALUE_DFL_V4_STRICT_LP_STRATEGY_KIND",
    "build_dfl_candidate_value_dfl_v4_frame",
    "build_dfl_candidate_value_dfl_v4_strict_lp_benchmark_frame",
    "build_dfl_candidate_value_label_panel_v4_frame",
    "build_dfl_plateau_data_quality_audit_frame",
    "build_dfl_schedule_candidate_library_v4_frame",
    "build_dfl_v2_v3_plateau_autopsy_frame",
    "candidate_value_dfl_v4_model_name",
    "evaluate_dfl_candidate_value_dfl_v4_gate",
    "validate_dfl_candidate_value_dfl_v4_evidence",
    "validate_dfl_candidate_value_label_panel_v4_evidence",
    "validate_dfl_plateau_data_quality_audit_evidence",
    "validate_dfl_v2_v3_plateau_autopsy_evidence",
]
