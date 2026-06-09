"""Prior-window feature audit for strict-failure selector evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean, median, pstdev
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl.strict_failure_selector import (
    CANDIDATE_FAMILY_RAW,
    CANDIDATE_FAMILY_STRICT,
    REFERENCE_FAMILY_ORDER,
    REQUIRED_LIBRARY_COLUMNS,
)
from smart_arbitrage.dfl.strict_failure_robustness import (
    DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE,
    REQUIRED_ROBUSTNESS_COLUMNS,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

DFL_STRICT_FAILURE_PRIOR_FEATURE_PANEL_CLAIM_SCOPE: Final[str] = (
    "dfl_strict_failure_prior_feature_panel_not_full_dfl"
)
DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_strict_failure_feature_audit_not_full_dfl"
)
DFL_STRICT_FAILURE_FEATURE_AUDIT_ACADEMIC_SCOPE: Final[str] = (
    "Prior-window feature audit for strict-failure selector behavior. "
    "This is explanatory research evidence only, not full DFL, not Decision Transformer "
    "control, and not market execution."
)

REQUIRED_CONTEXT_BENCHMARK_COLUMNS: Final[frozenset[str]] = frozenset(
    {"tenant_id", "timestamp", "price_uah_mwh"}
)
REQUIRED_CONTEXT_LOAD_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "timestamp",
        "net_load_mw",
        "btm_battery_power_mw",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "window_index",
        "anchor_timestamp",
        "prior_cutoff_timestamp",
        "selector_feature_prior_anchor_count",
        "selector_feature_prior_strict_mean_regret_uah",
        "selector_feature_prior_raw_mean_regret_uah",
        "selector_feature_prior_best_non_strict_mean_regret_uah",
        "selector_feature_prior_price_spread_std_uah_mwh",
        "selector_feature_prior_net_load_mean_mw",
        "analysis_only_strict_regret_uah",
        "analysis_only_raw_regret_uah",
        "analysis_only_selected_regret_uah",
        "analysis_only_selected_candidate_family",
        "analysis_only_selector_beats_strict",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)
REQUIRED_FEATURE_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "window_index",
        "validation_anchor_count",
        "strict_mean_regret_uah",
        "raw_mean_regret_uah",
        "selected_mean_regret_uah",
        "mean_regret_improvement_ratio_vs_strict",
        "mean_regret_improvement_ratio_vs_raw",
        "failure_cluster",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)


def build_dfl_strict_failure_prior_feature_panel_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    strict_failure_selector_robustness_frame: pl.DataFrame,
    benchmark_feature_frame: pl.DataFrame,
    tenant_historical_net_load_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...] = ("tft_silver_v0", "nbeatsx_silver_v0"),
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    min_prior_anchor_count: int = 3,
) -> pl.DataFrame:
    """Build prior-window selector features while keeping validation outcomes as labels."""

    _validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        validation_window_count=validation_window_count,
        validation_anchor_count=validation_anchor_count,
        min_prior_anchors_before_window=min_prior_anchors_before_window,
        min_prior_anchor_count=min_prior_anchor_count,
    )
    _validate_library_frame(schedule_candidate_library_frame)
    _validate_robustness_frame(strict_failure_selector_robustness_frame)
    _validate_benchmark_context_frame(benchmark_feature_frame)
    _validate_load_context_frame(tenant_historical_net_load_frame)

    benchmark_rows = list(benchmark_feature_frame.iter_rows(named=True))
    load_rows = list(tenant_historical_net_load_frame.iter_rows(named=True))
    rows: list[dict[str, Any]] = []
    for source_model_name in forecast_model_names:
        source_robustness_rows = _robustness_rows(
            strict_failure_selector_robustness_frame,
            source_model_name=source_model_name,
            expected_window_count=validation_window_count,
        )
        for robustness_row in source_robustness_rows:
            window_index = int(robustness_row["window_index"])
            validation_start = _datetime_value(
                robustness_row["validation_start_anchor_timestamp"],
                field_name="validation_start_anchor_timestamp",
            )
            validation_end = _datetime_value(
                robustness_row["validation_end_anchor_timestamp"],
                field_name="validation_end_anchor_timestamp",
            )
            minimum_prior = int(robustness_row["minimum_prior_anchor_count_before_window"])
            if minimum_prior < min_prior_anchors_before_window:
                raise ValueError("feature panel window has under-coverage before validation")
            selected_thresholds = _mapping_value(
                robustness_row["selected_thresholds_by_tenant"],
                field_name="selected_thresholds_by_tenant",
            )
            for tenant_id in tenant_ids:
                tenant_rows = _library_rows(
                    schedule_candidate_library_frame,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                )
                anchors = sorted(_anchor_set(tenant_rows))
                validation_anchors = [
                    anchor
                    for anchor in anchors
                    if validation_start <= anchor <= validation_end
                ]
                if len(validation_anchors) != validation_anchor_count:
                    raise ValueError(
                        "feature panel coverage mismatch for "
                        f"{tenant_id}/{source_model_name}/window_{window_index}"
                    )
                prior_rows = [
                    row
                    for row in tenant_rows
                    if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                    < validation_start
                ]
                if len(_anchor_set(prior_rows)) < min_prior_anchor_count:
                    raise ValueError(
                        f"missing prior anchors for {tenant_id}/{source_model_name}/window_{window_index}"
                    )
                selector_context = _selector_context(
                    prior_rows,
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    prior_cutoff=validation_start,
                    benchmark_rows=benchmark_rows,
                    load_rows=load_rows,
                )
                threshold = float(selected_thresholds.get(tenant_id, 0.0))
                for anchor_timestamp in validation_anchors:
                    decision = _decision_for_anchor(
                        tenant_rows,
                        anchor_timestamp=anchor_timestamp,
                        prior_cutoff=validation_start,
                        min_prior_anchor_count=min_prior_anchor_count,
                    )
                    selected_row = _selected_row(decision, switch_threshold_uah=threshold)
                    rows.append(
                        _feature_row(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            window_index=window_index,
                            anchor_timestamp=anchor_timestamp,
                            validation_start=validation_start,
                            validation_end=validation_end,
                            selected_threshold_uah=threshold,
                            selector_context=selector_context,
                            robustness_row=robustness_row,
                            decision=decision,
                            selected_row=selected_row,
                            benchmark_rows=benchmark_rows,
                        )
                    )

    if not rows:
        return pl.DataFrame(
            schema={column_name: pl.Null for column_name in REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS}
        )
    return pl.DataFrame(rows).sort(
        ["source_model_name", "window_index", "tenant_id", "anchor_timestamp"]
    )


def build_dfl_strict_failure_feature_audit_frame(
    prior_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Summarize strict-failure selector failure modes by source, window, and tenant."""

    _require_columns(
        prior_feature_panel_frame,
        REQUIRED_PRIOR_FEATURE_PANEL_COLUMNS,
        frame_name="dfl_strict_failure_prior_feature_panel_frame",
    )
    rows: list[dict[str, Any]] = []
    panel_rows = list(prior_feature_panel_frame.iter_rows(named=True))
    for source_model_name in sorted({str(row["source_model_name"]) for row in panel_rows}):
        for window_index in sorted(
            {int(row["window_index"]) for row in panel_rows if str(row["source_model_name"]) == source_model_name}
        ):
            for tenant_id in sorted(
                {
                    str(row["tenant_id"])
                    for row in panel_rows
                    if str(row["source_model_name"]) == source_model_name
                    and int(row["window_index"]) == window_index
                }
            ):
                group_rows = [
                    row
                    for row in panel_rows
                    if str(row["source_model_name"]) == source_model_name
                    and int(row["window_index"]) == window_index
                    and str(row["tenant_id"]) == tenant_id
                ]
                rows.append(_audit_row(tenant_id, source_model_name, window_index, group_rows))
    if not rows:
        return pl.DataFrame(schema={column_name: pl.Null for column_name in REQUIRED_FEATURE_AUDIT_COLUMNS})
    return pl.DataFrame(rows).sort(["source_model_name", "window_index", "tenant_id"])


def validate_dfl_strict_failure_feature_audit_evidence(
    feature_audit_frame: pl.DataFrame,
    *,
    min_tenant_count: int = 5,
    min_source_model_count: int = 2,
    min_window_count: int = 4,
    expected_validation_anchor_count: int = 18,
) -> EvidenceCheckOutcome:
    """Validate structural evidence for strict-failure feature audits."""

    failures = _missing_column_failures(feature_audit_frame, REQUIRED_FEATURE_AUDIT_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": feature_audit_frame.height})
    rows = list(feature_audit_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "strict-failure feature audit has no rows", {"row_count": 0})

    tenant_ids = sorted({str(row["tenant_id"]) for row in rows})
    source_model_names = sorted({str(row["source_model_name"]) for row in rows})
    window_indices = sorted({int(row["window_index"]) for row in rows})
    if len(tenant_ids) < min_tenant_count:
        failures.append(f"tenant_count must be at least {min_tenant_count}; observed {len(tenant_ids)}")
    if len(source_model_names) < min_source_model_count:
        failures.append(
            f"source_model_count must be at least {min_source_model_count}; observed {len(source_model_names)}"
        )
    if len(window_indices) < min_window_count:
        failures.append(f"window_count must be at least {min_window_count}; observed {len(window_indices)}")
    bad_anchor_rows = [
        row
        for row in rows
        if int(row["validation_anchor_count"]) != expected_validation_anchor_count
    ]
    if bad_anchor_rows:
        failures.append(
            "feature audit rows must keep exactly "
            f"{expected_validation_anchor_count} validation anchors"
        )
    claim_flag_failure_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    if claim_flag_failure_rows:
        failures.append("feature audit claim flags must remain research-only/not market execution")

    metadata = {
        "row_count": feature_audit_frame.height,
        "tenant_count": len(tenant_ids),
        "tenant_ids": tenant_ids,
        "source_model_count": len(source_model_names),
        "source_model_names": source_model_names,
        "window_count": len(window_indices),
        "window_indices": window_indices,
        "failure_clusters": sorted({str(row["failure_cluster"]) for row in rows}),
        "claim_flag_failure_rows": len(claim_flag_failure_rows),
        "bad_validation_anchor_rows": len(bad_anchor_rows),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Strict-failure feature audit evidence has valid rolling-window coverage."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _feature_row(
    *,
    tenant_id: str,
    source_model_name: str,
    window_index: int,
    anchor_timestamp: datetime,
    validation_start: datetime,
    validation_end: datetime,
    selected_threshold_uah: float,
    selector_context: dict[str, Any],
    robustness_row: dict[str, Any],
    decision: dict[str, Any],
    selected_row: dict[str, Any],
    benchmark_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    strict_row = decision["strict_row"]
    raw_row = decision["raw_row"]
    validation_context = _single_benchmark_context(
        benchmark_rows,
        tenant_id=tenant_id,
        anchor_timestamp=anchor_timestamp,
    )
    selected_regret = float(selected_row["regret_uah"])
    strict_regret = float(strict_row["regret_uah"])
    raw_regret = float(raw_row["regret_uah"])
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "selector_model_name": robustness_row["selector_model_name"],
        "window_index": window_index,
        "anchor_timestamp": anchor_timestamp,
        "prior_cutoff_timestamp": validation_start,
        "validation_start_anchor_timestamp": validation_start,
        "validation_end_anchor_timestamp": validation_end,
        "selector_feature_selected_threshold_uah": selected_threshold_uah,
        "selector_feature_anchor_hour": anchor_timestamp.hour,
        "selector_feature_anchor_weekday": anchor_timestamp.weekday(),
        "selector_feature_anchor_month": anchor_timestamp.month,
        "selector_feature_anchor_is_weekend": anchor_timestamp.weekday() >= 5,
        **selector_context,
        "analysis_only_strict_regret_uah": strict_regret,
        "analysis_only_raw_regret_uah": raw_regret,
        "analysis_only_best_non_strict_regret_uah": float(
            decision["best_prior_non_strict_row"]["regret_uah"]
        ),
        "analysis_only_selected_regret_uah": selected_regret,
        "analysis_only_selected_candidate_family": selected_row["candidate_family"],
        "analysis_only_selected_candidate_model_name": selected_row["candidate_model_name"],
        "analysis_only_selector_beats_strict": selected_regret < strict_regret,
        "analysis_only_selector_beats_raw": selected_regret < raw_regret,
        "analysis_only_window_gate_label": robustness_row.get("gate_label", "unknown"),
        "analysis_only_validation_price_uah_mwh": validation_context.get("price_uah_mwh"),
        "analysis_only_validation_weather_temperature": validation_context.get("weather_temperature"),
        "claim_scope": DFL_STRICT_FAILURE_PRIOR_FEATURE_PANEL_CLAIM_SCOPE,
        "academic_scope": DFL_STRICT_FAILURE_FEATURE_AUDIT_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _selector_context(
    prior_rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    prior_cutoff: datetime,
    benchmark_rows: list[dict[str, Any]],
    load_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    strict_prior = _family_rows(prior_rows, CANDIDATE_FAMILY_STRICT)
    raw_prior = _family_rows(prior_rows, CANDIDATE_FAMILY_RAW)
    non_strict_prior = [row for row in prior_rows if str(row["candidate_family"]) != CANDIDATE_FAMILY_STRICT]
    best_non_strict_mean = min(
        (
            _mean_regret(rows)
            for rows in _candidate_groups(non_strict_prior).values()
            if rows
        ),
        default=0.0,
    )
    price_stats = _price_stats(benchmark_rows, tenant_id=tenant_id, prior_cutoff=prior_cutoff)
    load_stats = _load_stats(load_rows, tenant_id=tenant_id, prior_cutoff=prior_cutoff)
    top_rank_overlap = _mean_optional(raw_prior, "forecast_top_k_actual_overlap")
    bottom_rank_overlap = _mean_optional(raw_prior, "forecast_bottom_k_actual_overlap")
    strict_mean = _mean_regret(strict_prior)
    raw_mean = _mean_regret(raw_prior)
    return {
        "selector_feature_prior_anchor_count": len(_anchor_set(prior_rows)),
        "selector_feature_prior_strict_mean_regret_uah": strict_mean,
        "selector_feature_prior_raw_mean_regret_uah": raw_mean,
        "selector_feature_prior_best_non_strict_mean_regret_uah": best_non_strict_mean,
        "selector_feature_prior_strict_minus_best_non_strict_uah": strict_mean - best_non_strict_mean,
        "selector_feature_prior_raw_minus_best_non_strict_uah": raw_mean - best_non_strict_mean,
        "selector_feature_prior_top_rank_overlap_mean": top_rank_overlap,
        "selector_feature_prior_bottom_rank_overlap_mean": bottom_rank_overlap,
        "selector_feature_prior_peak_index_error_mean": _mean_optional(raw_prior, "peak_index_abs_error"),
        "selector_feature_prior_trough_index_error_mean": _mean_optional(raw_prior, "trough_index_abs_error"),
        "selector_feature_prior_price_mean_uah_mwh": price_stats["price_mean"],
        "selector_feature_prior_price_spread_mean_uah_mwh": price_stats["price_spread_mean"],
        "selector_feature_prior_price_spread_std_uah_mwh": price_stats["price_spread_std"],
        "selector_feature_prior_price_regime": _spread_regime(price_stats["price_spread_mean"]),
        "selector_feature_prior_spread_volatility_regime": _volatility_regime(price_stats["price_spread_std"]),
        "selector_feature_prior_weather_temperature_mean": price_stats["temperature_mean"],
        "selector_feature_prior_weather_effective_solar_mean": price_stats["effective_solar_mean"],
        "selector_feature_prior_net_load_mean_mw": load_stats["net_load_mean"],
        "selector_feature_prior_net_load_std_mw": load_stats["net_load_std"],
        "selector_feature_prior_btm_battery_power_mean_mw": load_stats["btm_battery_mean"],
        "selector_feature_prior_load_regime": _load_regime(load_stats["net_load_mean"]),
        "analysis_only_source_model_name": source_model_name,
    }


def _audit_row(
    tenant_id: str,
    source_model_name: str,
    window_index: int,
    rows: list[dict[str, Any]],
) -> dict[str, Any]:
    strict_regrets = [float(row["analysis_only_strict_regret_uah"]) for row in rows]
    raw_regrets = [float(row["analysis_only_raw_regret_uah"]) for row in rows]
    selected_regrets = [float(row["analysis_only_selected_regret_uah"]) for row in rows]
    strict_mean = mean(strict_regrets)
    raw_mean = mean(raw_regrets)
    selected_mean = mean(selected_regrets)
    strict_median = median(strict_regrets)
    selected_median = median(selected_regrets)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    improvement_vs_raw = _improvement_ratio(raw_mean, selected_mean)
    selected_family_counts = _value_counts(str(row["analysis_only_selected_candidate_family"]) for row in rows)
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "window_index": window_index,
        "validation_anchor_count": len(rows),
        "strict_mean_regret_uah": strict_mean,
        "raw_mean_regret_uah": raw_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "mean_regret_improvement_ratio_vs_raw": improvement_vs_raw,
        "selector_beats_strict_anchor_count": sum(
            1 for row in rows if bool(row["analysis_only_selector_beats_strict"])
        ),
        "selector_beats_raw_anchor_count": sum(
            1 for row in rows if bool(row["analysis_only_selector_beats_raw"])
        ),
        "selected_family_counts": selected_family_counts,
        "prior_price_spread_std_uah_mwh": _mean_optional(
            rows,
            "selector_feature_prior_price_spread_std_uah_mwh",
        ),
        "prior_top_rank_overlap_mean": _mean_optional(
            rows,
            "selector_feature_prior_top_rank_overlap_mean",
        ),
        "prior_net_load_std_mw": _mean_optional(rows, "selector_feature_prior_net_load_std_mw"),
        "failure_cluster": _failure_cluster(
            improvement_vs_strict=improvement_vs_strict,
            selected_median=selected_median,
            strict_median=strict_median,
            price_spread_std=_mean_optional(
                rows,
                "selector_feature_prior_price_spread_std_uah_mwh",
            ),
            top_rank_overlap=_mean_optional(
                rows,
                "selector_feature_prior_top_rank_overlap_mean",
            ),
            net_load_std=_mean_optional(rows, "selector_feature_prior_net_load_std_mw"),
        ),
        "claim_scope": DFL_STRICT_FAILURE_FEATURE_AUDIT_CLAIM_SCOPE,
        "academic_scope": DFL_STRICT_FAILURE_FEATURE_AUDIT_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _decision_for_anchor(
    rows: list[dict[str, Any]],
    *,
    anchor_timestamp: datetime,
    prior_cutoff: datetime,
    min_prior_anchor_count: int,
) -> dict[str, Any]:
    anchor_rows = [
        row
        for row in rows
        if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp") == anchor_timestamp
    ]
    strict_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_STRICT)
    raw_row = _single_family_row(anchor_rows, CANDIDATE_FAMILY_RAW)
    strict_prior_mean = _prior_mean_regret(
        rows,
        strict_row,
        prior_cutoff=prior_cutoff,
        min_prior_anchor_count=min_prior_anchor_count,
    )
    if strict_prior_mean is None:
        raise ValueError("strict-failure feature panel is missing strict prior history")
    non_strict_candidates: list[dict[str, Any]] = []
    for row in anchor_rows:
        if str(row["candidate_family"]) == CANDIDATE_FAMILY_STRICT:
            continue
        prior_mean = _prior_mean_regret(
            rows,
            row,
            prior_cutoff=prior_cutoff,
            min_prior_anchor_count=min_prior_anchor_count,
        )
        if prior_mean is None:
            continue
        candidate = dict(row)
        candidate["selector_prior_mean_regret_uah"] = prior_mean
        non_strict_candidates.append(candidate)
    if not non_strict_candidates:
        raise ValueError("strict-failure feature panel is missing non-strict prior history")
    best_non_strict = min(
        non_strict_candidates,
        key=lambda row: (
            float(row["selector_prior_mean_regret_uah"]),
            _family_sort_index(str(row["candidate_family"])),
            str(row["candidate_model_name"]),
        ),
    )
    return {
        "anchor_timestamp": anchor_timestamp,
        "strict_row": dict(strict_row),
        "raw_row": dict(raw_row),
        "best_prior_non_strict_row": best_non_strict,
        "strict_prior_mean_regret_uah": strict_prior_mean,
        "best_non_strict_prior_mean_regret_uah": float(
            best_non_strict["selector_prior_mean_regret_uah"]
        ),
        "prior_advantage_uah": strict_prior_mean
        - float(best_non_strict["selector_prior_mean_regret_uah"]),
    }


def _selected_row(decision: dict[str, Any], *, switch_threshold_uah: float) -> dict[str, Any]:
    if float(decision["prior_advantage_uah"]) >= switch_threshold_uah:
        return dict(decision["best_prior_non_strict_row"])
    return dict(decision["strict_row"])


def _validate_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
    min_prior_anchor_count: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must contain at least one tenant.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must contain at least one model.")
    if validation_window_count < 1:
        raise ValueError("validation_window_count must be at least 1.")
    if validation_anchor_count < 1:
        raise ValueError("validation_anchor_count must be at least 1.")
    if min_prior_anchors_before_window < 1:
        raise ValueError("min_prior_anchors_before_window must be at least 1.")
    if min_prior_anchor_count < 1:
        raise ValueError("min_prior_anchor_count must be at least 1.")


def _validate_library_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_LIBRARY_COLUMNS, frame_name="dfl_schedule_candidate_library_v2_frame")
    for row in frame.iter_rows(named=True):
        horizon_hours = int(row["horizon_hours"])
        for column in (
            "forecast_price_uah_mwh_vector",
            "actual_price_uah_mwh_vector",
            "dispatch_mw_vector",
            "soc_fraction_vector",
        ):
            if len(_float_list(row[column], field_name=column)) != horizon_hours:
                raise ValueError(f"vector length must match horizon_hours for {column}")
        if str(row["data_quality_tier"]) != "thesis_grade":
            raise ValueError("strict-failure feature panel requires thesis_grade rows")
        if float(row["observed_coverage_ratio"]) < 1.0:
            raise ValueError("strict-failure feature panel requires observed coverage ratio of 1.0")
        if int(row["safety_violation_count"]):
            raise ValueError("strict-failure feature panel requires zero safety violations")
        if not bool(row["not_full_dfl"]):
            raise ValueError("strict-failure feature panel requires not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("strict-failure feature panel requires not_market_execution=true")
    split_by_anchor: dict[tuple[str, str, datetime], set[str]] = {}
    for row in frame.iter_rows(named=True):
        key = (
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp"),
        )
        split_by_anchor.setdefault(key, set()).add(str(row["split_name"]))
    if any(len(splits) > 1 for splits in split_by_anchor.values()):
        raise ValueError("train/final overlap is not allowed in strict-failure feature evidence")


def _validate_robustness_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_ROBUSTNESS_COLUMNS.union(
            {
                "validation_start_anchor_timestamp",
                "validation_end_anchor_timestamp",
                "minimum_prior_anchor_count_before_window",
                "selected_thresholds_by_tenant",
                "selector_model_name",
            }
        ),
        frame_name="dfl_strict_failure_selector_robustness_frame",
    )
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != DFL_STRICT_FAILURE_SELECTOR_ROBUSTNESS_CLAIM_SCOPE:
            raise ValueError("strict-failure feature panel requires robustness claim_scope rows")
        if not bool(row["not_full_dfl"]):
            raise ValueError("strict-failure feature panel requires robustness not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError("strict-failure feature panel requires robustness not_market_execution=true")


def _validate_benchmark_context_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_CONTEXT_BENCHMARK_COLUMNS,
        frame_name="real_data_benchmark_silver_feature_frame",
    )
    if frame.height == 0:
        raise ValueError("real_data_benchmark_silver_feature_frame must not be empty")


def _validate_load_context_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_CONTEXT_LOAD_COLUMNS,
        frame_name="tenant_historical_net_load_silver",
    )
    if frame.height == 0:
        raise ValueError("tenant_historical_net_load_silver must not be empty")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != "tenant_historical_net_load_configured_proxy":
            raise ValueError("tenant historical net-load rows must be configured proxy evidence")
        if not bool(row["not_full_dfl"]):
            raise ValueError("tenant historical net-load rows must remain not_full_dfl")
        if not bool(row["not_market_execution"]):
            raise ValueError("tenant historical net-load rows must remain not_market_execution")


def _robustness_rows(
    frame: pl.DataFrame,
    *,
    source_model_name: str,
    expected_window_count: int,
) -> list[dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if str(row["source_model_name"]) == source_model_name
    ]
    window_indices = sorted({int(row["window_index"]) for row in rows})
    if window_indices[:expected_window_count] != list(range(1, expected_window_count + 1)):
        raise ValueError(f"missing robustness windows for {source_model_name}")
    return sorted(rows, key=lambda row: int(row["window_index"]))[:expected_window_count]


def _library_rows(frame: pl.DataFrame, *, tenant_id: str, source_model_name: str) -> list[dict[str, Any]]:
    rows = [
        row
        for row in frame.iter_rows(named=True)
        if str(row["tenant_id"]) == tenant_id and str(row["source_model_name"]) == source_model_name
    ]
    if not rows:
        raise ValueError(f"coverage missing schedule candidate rows for {tenant_id}/{source_model_name}")
    return rows


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
    min_prior_anchor_count: int,
) -> float | None:
    prior_rows = [
        candidate
        for candidate in rows
        if str(candidate["candidate_family"]) == str(row["candidate_family"])
        and str(candidate["candidate_model_name"]) == str(row["candidate_model_name"])
        and _datetime_value(candidate["anchor_timestamp"], field_name="anchor_timestamp")
        < prior_cutoff
    ]
    if len(_anchor_set(prior_rows)) < min_prior_anchor_count:
        return None
    return _mean_regret(prior_rows)


def _price_stats(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    prior_cutoff: datetime,
) -> dict[str, float]:
    prior_rows = [
        row
        for row in rows
        if str(row["tenant_id"]) == tenant_id
        and _datetime_value(row["timestamp"], field_name="timestamp") < prior_cutoff
    ]
    if not prior_rows:
        raise ValueError(f"missing benchmark context before {prior_cutoff.isoformat()} for {tenant_id}")
    prices = [_float_value(row.get("price_uah_mwh"), default=0.0) for row in prior_rows]
    spreads = _rolling_differences(prices)
    return {
        "price_mean": mean(prices),
        "price_spread_mean": mean(spreads) if spreads else 0.0,
        "price_spread_std": pstdev(spreads) if len(spreads) > 1 else 0.0,
        "temperature_mean": _mean_field(prior_rows, "weather_temperature"),
        "effective_solar_mean": _mean_field(prior_rows, "weather_effective_solar"),
    }


def _load_stats(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    prior_cutoff: datetime,
) -> dict[str, float]:
    prior_rows = [
        row
        for row in rows
        if str(row["tenant_id"]) == tenant_id
        and _datetime_value(row["timestamp"], field_name="timestamp") < prior_cutoff
    ]
    if not prior_rows:
        raise ValueError(f"missing tenant load context before {prior_cutoff.isoformat()} for {tenant_id}")
    net_load = [_float_value(row.get("net_load_mw"), default=0.0) for row in prior_rows]
    btm_power = [_float_value(row.get("btm_battery_power_mw"), default=0.0) for row in prior_rows]
    return {
        "net_load_mean": mean(net_load),
        "net_load_std": pstdev(net_load) if len(net_load) > 1 else 0.0,
        "btm_battery_mean": mean(btm_power),
    }


def _single_benchmark_context(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
) -> dict[str, Any]:
    matches = [
        row
        for row in rows
        if str(row["tenant_id"]) == tenant_id
        and _datetime_value(row["timestamp"], field_name="timestamp") == anchor_timestamp
    ]
    return matches[0] if matches else {}


def _failure_cluster(
    *,
    improvement_vs_strict: float,
    selected_median: float,
    strict_median: float,
    price_spread_std: float,
    top_rank_overlap: float,
    net_load_std: float,
) -> str:
    if improvement_vs_strict >= 0.05 and selected_median <= strict_median:
        return "strict_failure_captured"
    if abs(improvement_vs_strict) <= 0.01:
        return "strict_stable_region"
    if price_spread_std >= 750.0:
        return "high_spread_volatility"
    if top_rank_overlap < 0.5:
        return "rank_instability"
    if net_load_std >= 0.08:
        return "load_weather_stress"
    return "tenant_specific_outlier"


def _spread_regime(spread: float) -> str:
    if spread >= 3_000.0:
        return "high_spread"
    if spread >= 1_000.0:
        return "medium_spread"
    return "low_spread"


def _volatility_regime(spread_std: float) -> str:
    if spread_std >= 750.0:
        return "volatile"
    if spread_std >= 250.0:
        return "mixed"
    return "stable"


def _load_regime(net_load_mw: float) -> str:
    if net_load_mw >= 0.4:
        return "high_load"
    if net_load_mw >= 0.15:
        return "medium_load"
    return "low_load"


def _candidate_groups(rows: list[dict[str, Any]]) -> dict[tuple[str, str], list[dict[str, Any]]]:
    groups: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        key = (str(row["candidate_family"]), str(row["candidate_model_name"]))
        groups.setdefault(key, []).append(row)
    return groups


def _family_rows(rows: list[dict[str, Any]], candidate_family: str) -> list[dict[str, Any]]:
    return [row for row in rows if str(row["candidate_family"]) == candidate_family]


def _anchor_set(rows: Any) -> set[datetime]:
    return {
        _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
        for row in rows
    }


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    regrets = [float(row["regret_uah"]) for row in rows]
    return mean(regrets) if regrets else 0.0


def _mean_optional(rows: list[dict[str, Any]], column_name: str) -> float:
    values = [
        _float_value(row.get(column_name), default=0.0)
        for row in rows
        if row.get(column_name) is not None
    ]
    return mean(values) if values else 0.0


def _mean_field(rows: list[dict[str, Any]], column_name: str) -> float:
    values = [
        _float_value(row.get(column_name), default=0.0)
        for row in rows
        if row.get(column_name) is not None
    ]
    return mean(values) if values else 0.0


def _rolling_differences(values: list[float]) -> list[float]:
    if len(values) < 2:
        return []
    return [abs(values[index] - values[index - 1]) for index in range(1, len(values))]


def _improvement_ratio(baseline: float, candidate: float) -> float:
    return (baseline - candidate) / abs(baseline) if abs(baseline) > 1e-9 else 0.0


def _family_sort_index(candidate_family: str) -> int:
    if candidate_family in REFERENCE_FAMILY_ORDER:
        return REFERENCE_FAMILY_ORDER.index(candidate_family)
    return len(REFERENCE_FAMILY_ORDER)


def _value_counts(values: Any) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


def _float_list(value: object, *, field_name: str) -> list[float]:
    if not isinstance(value, list):
        raise ValueError(f"{field_name} must be a list")
    return [float(item) for item in value]


def _mapping_value(value: object, *, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be a mapping")
    return value


def _float_value(value: object, *, default: float) -> float:
    if value is None:
        return default
    if isinstance(value, bool):
        raise ValueError("Boolean values are not valid numeric values.")
    if isinstance(value, int | float | str):
        return float(value)
    raise ValueError("Value must be numeric.")


def _datetime_value(value: object, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value.astimezone(UTC)
    if isinstance(value, str):
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)
    raise ValueError(f"{field_name} must be a datetime")


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    failures = _missing_column_failures(frame, required_columns)
    if failures:
        raise ValueError(f"{frame_name} " + "; ".join(failures))
