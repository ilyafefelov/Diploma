"""Ukrainian-context safe-switch layer before DT/LAVA.

The module enriches the corrected V2+ oracle-gap label panel with prior-only
Ukrainian context: calendar/publication metadata, weather/load proxies, and
Ukrenergo grid-event features. It then trains bounded candidate-index scorers.
Every scorer keeps corrected V2+ as fallback and remains Offline Strategy
Promotion evidence only, not live dispatch or deployed DT control.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from datetime import datetime, timedelta
from statistics import mean, median
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import oracle_gap_safe_switch as oracle_gap
from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl.lava_schedule_neighbor_bridge import (
    STRICT_REFERENCE_ROLE,
    V2_PLUS_REFERENCE_ROLE,
)
from smart_arbitrage.dfl.promotion_gate import (
    CONTROL_MODEL_NAME,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)

UA_CONTEXT_CALENDAR_PUBLICATION_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_calendar_publication_context_not_full_dfl"
)
UA_CONTEXT_WEATHER_LOAD_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_weather_load_context_not_full_dfl"
)
UA_CONTEXT_GRID_EVENT_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_grid_event_context_not_full_dfl"
)
UA_CONTEXT_ORACLE_GAP_FEATURE_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_oracle_gap_feature_panel_not_full_dfl"
)
UA_CONTEXT_SAFE_SWITCH_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_safe_switch_separability_audit_not_full_dfl"
)
UA_CONTEXT_SAFE_SWITCH_SCORER_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_safe_switch_scorer_not_full_dfl"
)
UA_CONTEXT_SAFE_SWITCH_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_safe_switch_strict_lp_gate_not_full_dfl"
)
UA_CONTEXT_SAFE_SWITCH_ROBUSTNESS_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_safe_switch_rolling_robustness_not_full_dfl"
)
UA_CONTEXT_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_ua_context_safe_switch_strict_lp_benchmark"
)
UA_CONTEXT_SAFE_SWITCH_SKLEARN_MODEL_NAME: Final[str] = (
    "dfl_ua_context_safe_switch_sklearn_v1"
)
UA_CONTEXT_SAFE_SWITCH_TORCH_MODEL_NAME: Final[str] = (
    "dfl_ua_context_safe_switch_torch_mlp_v1"
)
UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN: Final[str] = (
    "ua_context_safe_switch_sklearn"
)
UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH: Final[str] = (
    "ua_context_safe_switch_torch"
)

UA_CONTEXT_SELECTOR_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_anchor_hour",
    "selector_feature_anchor_day_of_week",
    "selector_feature_anchor_is_weekend",
    "selector_feature_anchor_month",
    "selector_feature_is_ua_public_holiday",
    "selector_feature_morning_block",
    "selector_feature_evening_block",
    "selector_feature_publication_time_ready",
    "selector_feature_hours_since_publication",
    "selector_feature_weather_load_context_ready",
    "selector_feature_weather_temperature_c",
    "selector_feature_weather_wind_speed_ms",
    "selector_feature_weather_effective_solar",
    "selector_feature_weather_precipitation",
    "selector_feature_net_load_mw",
    "selector_feature_pv_estimate_mw",
    "selector_feature_load_weather_source_observed",
    "selector_feature_grid_event_context_ready",
    "selector_feature_grid_event_count_24h",
    "selector_feature_tenant_region_affected",
    "selector_feature_national_grid_risk_score",
    "selector_feature_days_since_grid_event",
    "selector_feature_outage_flag",
    "selector_feature_saving_request_flag",
    "selector_feature_solar_shift_hint",
)
UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    *oracle_gap.ORACLE_GAP_SELECTOR_FEATURE_COLUMNS,
    *UA_CONTEXT_SELECTOR_FEATURE_COLUMNS,
)

_UA_FIXED_HOLIDAYS: Final[set[tuple[int, int]]] = {
    (1, 1),
    (1, 7),
    (3, 8),
    (5, 1),
    (5, 8),
    (6, 28),
    (8, 24),
    (10, 14),
    (12, 25),
}
_CONTEXT_JOIN_COLUMNS: Final[tuple[str, ...]] = (
    "tenant_id",
    "source_model_name",
    "anchor_timestamp",
)
_REQUIRED_UA_CONTEXT_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        "candidate_source",
        "eligible_for_final_selection",
        "label_regret_delta_vs_v2_plus_uah",
        "label_safe_switch_win",
        "label_tail_risk_loss",
        *UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS,
        "target_label_space",
        "raw_hourly_action_imitation",
        "market_execution_enabled",
    }
)


def build_dfl_ua_calendar_publication_context_frame(
    oracle_gap_feature_panel_frame: pl.DataFrame,
    benchmark_context_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build calendar and prior-known DAM publication context by anchor."""

    _validate_oracle_gap_panel(oracle_gap_feature_panel_frame)
    rows: list[dict[str, Any]] = []
    for anchor_row in _unique_anchor_rows(oracle_gap_feature_panel_frame):
        tenant_id = str(anchor_row["tenant_id"])
        source_model_name = str(anchor_row["source_model_name"])
        anchor = _datetime_value(anchor_row["anchor_timestamp"])
        context_row = _latest_row_before_anchor(
            benchmark_context_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        publication_at = _publication_timestamp(
            context_row,
            anchor_timestamp=anchor,
        )
        ready = publication_at is not None and publication_at <= anchor
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor,
                "feature_available_timestamp": publication_at,
                "available_before_anchor": bool(ready),
                "calendar_publication_context_blocker": (
                    "context_ready" if ready else "missing_publication_time"
                ),
                "selector_feature_anchor_hour": float(anchor.hour),
                "selector_feature_anchor_day_of_week": float(anchor.weekday()),
                "selector_feature_anchor_is_weekend": float(anchor.weekday() >= 5),
                "selector_feature_anchor_month": float(anchor.month),
                "selector_feature_is_ua_public_holiday": float(
                    (anchor.month, anchor.day) in _UA_FIXED_HOLIDAYS
                ),
                "selector_feature_morning_block": float(6 <= anchor.hour <= 10),
                "selector_feature_evening_block": float(17 <= anchor.hour <= 22),
                "selector_feature_publication_time_ready": float(ready),
                "selector_feature_hours_since_publication": _hours_between(
                    publication_at,
                    anchor,
                    default=999.0,
                ),
                "context_source": "oree_dam_publication_backtest_metadata",
                "claim_scope": UA_CONTEXT_CALENDAR_PUBLICATION_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None).sort(list(_CONTEXT_JOIN_COLUMNS))


def build_dfl_ua_weather_load_context_frame(
    oracle_gap_feature_panel_frame: pl.DataFrame,
    benchmark_context_frame: pl.DataFrame,
    tenant_historical_net_load_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build prior-only Open-Meteo/weather plus tenant load proxy context."""

    _validate_oracle_gap_panel(oracle_gap_feature_panel_frame)
    rows: list[dict[str, Any]] = []
    for anchor_row in _unique_anchor_rows(oracle_gap_feature_panel_frame):
        tenant_id = str(anchor_row["tenant_id"])
        source_model_name = str(anchor_row["source_model_name"])
        anchor = _datetime_value(anchor_row["anchor_timestamp"])
        weather_row = _latest_row_before_anchor(
            benchmark_context_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        load_row = _latest_row_before_anchor(
            tenant_historical_net_load_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        weather_ready = weather_row is not None
        load_ready = load_row is not None
        available_at = max(
            [
                value
                for value in (
                    _row_timestamp(weather_row),
                    _row_timestamp(load_row),
                )
                if value is not None
            ],
            default=None,
        )
        ready = bool(weather_ready and load_ready and available_at is not None)
        available_before_anchor = (
            available_at <= anchor if ready and available_at is not None else False
        )
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor,
                "feature_available_timestamp": available_at,
                "available_before_anchor": available_before_anchor,
                "weather_load_context_blocker": (
                    "context_ready" if ready else "missing_weather_load_context"
                ),
                "selector_feature_weather_load_context_ready": float(ready),
                "selector_feature_weather_temperature_c": _row_float(
                    weather_row,
                    ("weather_temperature", "temperature"),
                ),
                "selector_feature_weather_wind_speed_ms": _row_float(
                    weather_row,
                    ("weather_wind_speed", "wind_speed"),
                ),
                "selector_feature_weather_effective_solar": _row_float(
                    weather_row,
                    ("weather_effective_solar", "effective_solar"),
                ),
                "selector_feature_weather_precipitation": _row_float(
                    weather_row,
                    ("weather_precipitation", "precipitation"),
                ),
                "selector_feature_net_load_mw": _row_float(
                    load_row,
                    ("net_load_mw", "load_mw", "consumption_mw"),
                ),
                "selector_feature_pv_estimate_mw": _row_float(
                    load_row,
                    ("pv_estimate_mw", "solar_generation_mw"),
                ),
                "selector_feature_load_weather_source_observed": float(
                    _row_text(weather_row, ("weather_source_kind", "source_kind"))
                    in {"observed", "observed_open_meteo", "historical_open_meteo"}
                ),
                "context_source": "open_meteo_tenant_load_proxy",
                "claim_scope": UA_CONTEXT_WEATHER_LOAD_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None).sort(list(_CONTEXT_JOIN_COLUMNS))


def build_dfl_ua_grid_event_context_frame(
    oracle_gap_feature_panel_frame: pl.DataFrame,
    grid_event_signal_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build prior-only Ukrenergo/grid-event context features by anchor."""

    _validate_oracle_gap_panel(oracle_gap_feature_panel_frame)
    rows: list[dict[str, Any]] = []
    for anchor_row in _unique_anchor_rows(oracle_gap_feature_panel_frame):
        tenant_id = str(anchor_row["tenant_id"])
        source_model_name = str(anchor_row["source_model_name"])
        anchor = _datetime_value(anchor_row["anchor_timestamp"])
        grid_row = _latest_row_before_anchor(
            grid_event_signal_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor,
        )
        available_at = _row_timestamp(grid_row)
        ready = grid_row is not None and available_at is not None and available_at <= anchor
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor,
                "feature_available_timestamp": available_at,
                "available_before_anchor": bool(ready),
                "grid_event_context_blocker": (
                    "context_ready" if ready else "missing_grid_event_context"
                ),
                "selector_feature_grid_event_context_ready": float(ready),
                "selector_feature_grid_event_count_24h": _row_float(
                    grid_row,
                    ("grid_event_count_24h",),
                ),
                "selector_feature_tenant_region_affected": _row_float(
                    grid_row,
                    ("tenant_region_affected",),
                ),
                "selector_feature_national_grid_risk_score": _row_float(
                    grid_row,
                    ("national_grid_risk_score",),
                ),
                "selector_feature_days_since_grid_event": _row_float(
                    grid_row,
                    ("days_since_grid_event",),
                    default=999.0,
                ),
                "selector_feature_outage_flag": _row_float(grid_row, ("outage_flag",)),
                "selector_feature_saving_request_flag": _row_float(
                    grid_row,
                    ("saving_request_flag",),
                ),
                "selector_feature_solar_shift_hint": _row_float(
                    grid_row,
                    ("solar_shift_hint",),
                ),
                "context_source": "ukrenergo_grid_event_signal",
                "claim_scope": UA_CONTEXT_GRID_EVENT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None).sort(list(_CONTEXT_JOIN_COLUMNS))


def build_dfl_ua_context_oracle_gap_feature_panel_frame(
    oracle_gap_safe_switch_feature_panel_frame: pl.DataFrame,
    ua_calendar_publication_context_frame: pl.DataFrame,
    ua_weather_load_context_frame: pl.DataFrame,
    ua_grid_event_context_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Merge UA context lanes onto the oracle-gap candidate feature panel."""

    _validate_oracle_gap_panel(oracle_gap_safe_switch_feature_panel_frame)
    calendar_by_key = _context_by_key(ua_calendar_publication_context_frame)
    weather_load_by_key = _context_by_key(ua_weather_load_context_frame)
    grid_by_key = _context_by_key(ua_grid_event_context_frame)
    rows: list[dict[str, Any]] = []
    for row in oracle_gap_safe_switch_feature_panel_frame.iter_rows(named=True):
        key = _context_key(row)
        context_values = {
            **_selector_context(calendar_by_key.get(key)),
            **_selector_context(weather_load_by_key.get(key)),
            **_selector_context(grid_by_key.get(key)),
        }
        missing = sorted(
            column
            for column in UA_CONTEXT_SELECTOR_FEATURE_COLUMNS
            if column not in context_values
        )
        if missing:
            raise ValueError(f"UA context feature merge lost columns: {missing}")
        copied = dict(row)
        copied.update(context_values)
        blockers = [
            value
            for value in (
                calendar_by_key.get(key, {}).get("calendar_publication_context_blocker"),
                weather_load_by_key.get(key, {}).get("weather_load_context_blocker"),
                grid_by_key.get(key, {}).get("grid_event_context_blocker"),
            )
            if value is not None and str(value) != "context_ready"
        ]
        copied.update(
            {
                "feature_panel_version": "ua_context_oracle_gap_safe_switch_v1",
                "selected_feature_names": list(UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS),
                "diagnostic_context_blockers": sorted(str(value) for value in blockers),
                "diagnostic_external_market_features_used": False,
                "training_source_scope": "ukrainian_only_oree_open_meteo_tenant_grid",
                "claim_scope": UA_CONTEXT_ORACLE_GAP_FEATURE_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        rows.append(copied)
    panel = pl.DataFrame(rows, infer_schema_length=None).sort(
        [
            "source_model_name",
            "tenant_id",
            "anchor_timestamp",
            "candidate_source",
            "candidate_family",
            "candidate_model_name",
        ]
    )
    _validate_ua_context_panel(panel)
    return panel


def build_dfl_ua_context_safe_switch_separability_audit_frame(
    ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Summarize whether missed candidate wins are separable before scoring."""

    _validate_ua_context_panel(ua_context_oracle_gap_feature_panel_frame)
    output_rows: list[dict[str, Any]] = []
    rows = list(ua_context_oracle_gap_feature_panel_frame.iter_rows(named=True))
    scopes = sorted(
        {
            (str(row["source_model_name"]), str(row["tenant_id"]))
            for row in rows
        }
    )
    scopes.append(("__all_sources__", "__all_tenants__"))
    for source_model_name, tenant_id in scopes:
        scope_rows = [
            row
            for row in rows
            if (
                source_model_name == "__all_sources__"
                or str(row["source_model_name"]) == source_model_name
            )
            and (
                tenant_id == "__all_tenants__"
                or str(row["tenant_id"]) == tenant_id
            )
        ]
        train_candidates = [
            row
            for row in scope_rows
            if str(row["split_name"]) != "final_holdout"
            and bool(row["eligible_for_final_selection"])
            and str(row["candidate_source"]) != "v2_plus_default"
        ]
        final_candidates = [
            row
            for row in scope_rows
            if str(row["split_name"]) == "final_holdout"
            and bool(row["eligible_for_final_selection"])
            and str(row["candidate_source"]) != "v2_plus_default"
        ]
        safe_train = [row for row in train_candidates if bool(row["label_safe_switch_win"])]
        tail_train = [row for row in train_candidates if bool(row["label_tail_risk_loss"])]
        missed_final = [
            row
            for row in final_candidates
            if bool(row["label_safe_switch_win"])
            and str(row["candidate_source"]) != "v2_plus_default"
        ]
        separating_features = _separating_features(safe_train, tail_train)
        output_rows.append(
            {
                "source_model_name": source_model_name,
                "tenant_id": tenant_id,
                "train_candidate_count": len(train_candidates),
                "train_safe_switch_win_count": len(safe_train),
                "train_tail_risk_loss_count": len(tail_train),
                "final_candidate_count": len(final_candidates),
                "missed_safe_switch_opportunity_count": len(missed_final),
                "context_ready_row_ratio": _context_ready_ratio(scope_rows),
                "separating_selector_feature_count": len(separating_features),
                "top_separating_selector_features": separating_features[:8],
                "pre_anchor_distinguishable": bool(
                    safe_train and len(separating_features) > 0
                ),
                "audit_interpretation": _audit_interpretation(
                    train_candidates=train_candidates,
                    safe_train=safe_train,
                    tail_train=tail_train,
                    separating_features=separating_features,
                ),
                "target_label_space": "schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "claim_scope": UA_CONTEXT_SAFE_SWITCH_AUDIT_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def build_dfl_ua_context_safe_switch_scorer_frame(
    ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    scorer_kinds: tuple[str, ...] = ("sklearn", "torch"),
    min_prior_safe_win_count: int = 1,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    allowed_candidate_sources: tuple[str, ...] = (
        "oracle_gap_candidate",
        "poland_shadow_candidate",
        "tft_shadow_candidate",
    ),
    ridge_l2: float = 10.0,
    torch_hidden_size: int = 8,
    torch_max_epochs: int = 20,
    use_cuda_if_available: bool = True,
) -> pl.DataFrame:
    """Train bounded UA-context safe-switch scorers with V2+ fallback."""

    _validate_ua_context_panel(ua_context_oracle_gap_feature_panel_frame)
    _validate_scorer_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        scorer_kinds=scorer_kinds,
        min_prior_safe_win_count=min_prior_safe_win_count,
        min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
        min_predicted_improvement_uah=min_predicted_improvement_uah,
        max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        allowed_candidate_sources=allowed_candidate_sources,
        ridge_l2=ridge_l2,
        torch_hidden_size=torch_hidden_size,
        torch_max_epochs=torch_max_epochs,
    )
    rows = list(ua_context_oracle_gap_feature_panel_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for scorer_kind in scorer_kinds:
        for tenant_id in tenant_ids:
            for source_model_name in forecast_model_names:
                source_rows = [
                    row
                    for row in rows
                    if str(row["tenant_id"]) == tenant_id
                    and str(row["source_model_name"]) == source_model_name
                ]
                output_rows.append(
                    _fit_safe_switch_for_scope(
                        source_rows,
                        tenant_id=tenant_id,
                        source_model_name=source_model_name,
                        scorer_kind=scorer_kind,
                        min_prior_safe_win_count=min_prior_safe_win_count,
                        min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                        min_predicted_improvement_uah=min_predicted_improvement_uah,
                        max_predicted_tail_risk_probability=(
                            max_predicted_tail_risk_probability
                        ),
                        allowed_candidate_sources=set(allowed_candidate_sources),
                        ridge_l2=ridge_l2,
                        torch_hidden_size=torch_hidden_size,
                        torch_max_epochs=torch_max_epochs,
                        use_cuda_if_available=use_cuda_if_available,
                    )
                )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "scorer_kind"]
    )


def build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame(
    ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    ua_context_safe_switch_scorer_frame: pl.DataFrame,
    schedule_value_v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict, corrected V2+, sklearn, and Torch safe-switch rows."""

    _validate_ua_context_panel(ua_context_oracle_gap_feature_panel_frame)
    _validate_scorer_frame(ua_context_safe_switch_scorer_frame)
    _require_columns(
        schedule_value_v2_plus_strict_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "evaluation_payload",
            }
        ),
        frame_name="schedule_value_v2_plus_strict_frame",
    )
    resolved_generated_at = generated_at or _latest_generated_at(
        ua_context_oracle_gap_feature_panel_frame
    )
    candidate_rows = list(ua_context_oracle_gap_feature_panel_frame.iter_rows(named=True))
    source_filter = {str(row["source_model_name"]) for row in candidate_rows}
    candidate_by_key = {_candidate_key(row): row for row in candidate_rows}
    v2_reference_by_anchor: dict[str, dict[str, Any]] = {}
    output_rows: list[dict[str, Any]] = []
    for row in schedule_value_v2_plus_strict_frame.iter_rows(named=True):
        if str(row["source_model_name"]) not in source_filter:
            continue
        if str(row["selection_role"]) not in {
            "strict_reference",
            "schedule_value_learner_v2_plus",
        }:
            continue
        role = (
            STRICT_REFERENCE_ROLE
            if str(row["selection_role"]) == "strict_reference"
            else V2_PLUS_REFERENCE_ROLE
        )
        if role == V2_PLUS_REFERENCE_ROLE:
            v2_reference_by_anchor[_anchor_key(row)] = row
        output_rows.append(
            _reference_row(row, selection_role=role, generated_at=resolved_generated_at)
        )
    for scorer_row in ua_context_safe_switch_scorer_frame.iter_rows(named=True):
        for key in scorer_row["selected_final_candidate_keys"]:
            candidate = candidate_by_key[str(key)]
            output_rows.append(
                _candidate_benchmark_row(
                    candidate,
                    scorer_row=scorer_row,
                    generated_at=resolved_generated_at,
                )
            )
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            fallback = v2_reference_by_anchor.get(str(anchor_key))
            if fallback is None:
                raise ValueError(f"Missing corrected V2+ fallback row for {anchor_key}.")
            output_rows.append(
                _fallback_benchmark_row(
                    fallback,
                    scorer_row=scorer_row,
                    generated_at=resolved_generated_at,
                )
            )
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "selection_role"]
    )


def build_dfl_ua_context_safe_switch_rolling_robustness_frame(
    ua_context_oracle_gap_feature_panel_frame: pl.DataFrame,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    validation_window_count: int = 4,
    validation_anchor_count: int = 18,
    min_prior_anchors_before_window: int = 30,
    scorer_kinds: tuple[str, ...] = ("sklearn", "torch"),
    min_prior_safe_win_count: int = 1,
    min_prior_mean_improvement_uah: float = 1.0,
    min_predicted_improvement_uah: float = 1.0,
    max_predicted_tail_risk_probability: float = 0.25,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    allowed_candidate_sources: tuple[str, ...] = (
        "oracle_gap_candidate",
        "poland_shadow_candidate",
        "tft_shadow_candidate",
    ),
    ridge_l2: float = 10.0,
    torch_hidden_size: int = 8,
    torch_max_epochs: int = 20,
    use_cuda_if_available: bool = True,
) -> pl.DataFrame:
    """Replay UA-context safe-switch selection over rolling prior-only windows."""

    _validate_ua_context_panel(ua_context_oracle_gap_feature_panel_frame)
    if validation_window_count <= 0:
        raise ValueError("validation_window_count must be positive.")
    if validation_anchor_count <= 0:
        raise ValueError("validation_anchor_count must be positive.")
    if min_prior_anchors_before_window < 0:
        raise ValueError("min_prior_anchors_before_window must not be negative.")
    rows = list(ua_context_oracle_gap_feature_panel_frame.iter_rows(named=True))
    output_rows: list[dict[str, Any]] = []
    for scorer_kind in scorer_kinds:
        for source_model_name in forecast_model_names:
            windows = _rolling_windows(
                rows,
                tenant_ids=tenant_ids,
                source_model_name=source_model_name,
                validation_window_count=validation_window_count,
                validation_anchor_count=validation_anchor_count,
                min_prior_anchors_before_window=min_prior_anchors_before_window,
            )
            source_window_rows: list[dict[str, Any]] = []
            for window_index, validation_anchors, prior_anchors in windows:
                window_panel = _with_window_split(
                    rows,
                    tenant_ids=tenant_ids,
                    source_model_name=source_model_name,
                    validation_anchors=validation_anchors,
                    prior_anchors=prior_anchors,
                )
                scorer = build_dfl_ua_context_safe_switch_scorer_frame(
                    window_panel,
                    tenant_ids=tenant_ids,
                    forecast_model_names=(source_model_name,),
                    scorer_kinds=(scorer_kind,),
                    min_prior_safe_win_count=min_prior_safe_win_count,
                    min_prior_mean_improvement_uah=min_prior_mean_improvement_uah,
                    min_predicted_improvement_uah=min_predicted_improvement_uah,
                    max_predicted_tail_risk_probability=(
                        max_predicted_tail_risk_probability
                    ),
                    allowed_candidate_sources=allowed_candidate_sources,
                    ridge_l2=ridge_l2,
                    torch_hidden_size=torch_hidden_size,
                    torch_max_epochs=torch_max_epochs,
                    use_cuda_if_available=use_cuda_if_available,
                )
                source_window_rows.append(
                    _rolling_summary_row(
                        window_panel,
                        scorer,
                        scorer_kind=scorer_kind,
                        source_model_name=source_model_name,
                        window_index=window_index,
                        validation_anchors=validation_anchors,
                        prior_anchors=prior_anchors,
                        min_mean_regret_improvement_ratio_vs_v2_plus=(
                            min_mean_regret_improvement_ratio_vs_v2_plus
                        ),
                        min_mean_regret_improvement_ratio_vs_strict=(
                            min_mean_regret_improvement_ratio_vs_strict
                        ),
                    )
                )
            pass_count = sum(
                1 for row in source_window_rows if bool(row["rolling_window_passed"])
            )
            diagnostic_count = sum(
                1 for row in source_window_rows if bool(row["diagnostic_window_passed"])
            )
            for row in source_window_rows:
                row["passing_window_count_for_source"] = pass_count
                row["diagnostic_window_count_for_source"] = diagnostic_count
                row["robust_safe_switch_challenger"] = pass_count >= validation_window_count
                row["diagnostic_signal_learnable"] = diagnostic_count >= min(
                    validation_window_count,
                    3,
                )
                row["production_promote"] = False
            output_rows.extend(source_window_rows)
    return pl.DataFrame(output_rows, infer_schema_length=None).sort(
        ["scorer_kind", "source_model_name", "window_index"]
    )


def evaluate_dfl_ua_context_safe_switch_gate(
    strict_frame: pl.DataFrame,
    *,
    selection_role: str = UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN,
    min_validation_tenant_anchor_count: int = 90,
    min_mean_regret_improvement_ratio_vs_v2_plus: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
    min_mean_regret_improvement_ratio_vs_strict: float = (
        DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO
    ),
) -> PromotionGateResult:
    """Require a UA-context safe-switch scorer to beat corrected V2+."""

    _require_columns(
        strict_frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "selection_role",
                "anchor_timestamp",
                "regret_uah",
                "not_market_execution",
                "market_execution_enabled",
            }
        ),
        frame_name="UA-context safe-switch strict frame",
    )
    if strict_frame.select(pl.col("market_execution_enabled").any()).item():
        return PromotionGateResult(
            False,
            "blocked",
            "UA-context safe-switch refuses market execution claims",
            {"market_execution_enabled": False},
        )
    summaries = _role_summaries(strict_frame)
    selected = summaries.get(selection_role)
    v2_reference = summaries.get(V2_PLUS_REFERENCE_ROLE)
    strict_reference = summaries.get(STRICT_REFERENCE_ROLE)
    validation_count = _tenant_anchor_count(
        strict_frame.filter(pl.col("selection_role") == selection_role)
    )
    failures: list[str] = []
    if selected is None:
        failures.append(f"missing {selection_role} rows")
    if v2_reference is None:
        failures.append("missing corrected V2+ reference rows")
    if strict_reference is None:
        failures.append("missing strict reference rows")
    if validation_count < min_validation_tenant_anchor_count:
        failures.append(
            "UA-context safe-switch validation tenant-anchor count below required "
            f"{min_validation_tenant_anchor_count}"
        )
    if failures or selected is None or v2_reference is None or strict_reference is None:
        return PromotionGateResult(
            False,
            "blocked",
            "; ".join(failures),
            {
                "role_summaries": summaries,
                "validation_tenant_anchor_count": validation_count,
                "market_execution_enabled": False,
            },
        )
    improvement_vs_v2 = _improvement_ratio(
        float(v2_reference["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    improvement_vs_strict = _improvement_ratio(
        float(strict_reference["mean_regret_uah"]),
        float(selected["mean_regret_uah"]),
    )
    median_degraded = float(selected["median_regret_uah"]) > float(
        v2_reference["median_regret_uah"]
    )
    if improvement_vs_v2 < min_mean_regret_improvement_ratio_vs_v2_plus:
        failures.append("mean_not_improved_vs_corrected_v2_plus")
    if improvement_vs_strict < min_mean_regret_improvement_ratio_vs_strict:
        failures.append(f"mean_not_improved_vs_{CONTROL_MODEL_NAME}")
    if median_degraded:
        failures.append("median_degraded_vs_corrected_v2_plus")
    metrics = {
        "selected_mean_regret_uah": selected["mean_regret_uah"],
        "v2_plus_mean_regret_uah": v2_reference["mean_regret_uah"],
        "strict_mean_regret_uah": strict_reference["mean_regret_uah"],
        "selected_median_regret_uah": selected["median_regret_uah"],
        "v2_plus_median_regret_uah": v2_reference["median_regret_uah"],
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "diagnostic_signal_passed": improvement_vs_v2 > 0.0 and not median_degraded,
        "validation_tenant_anchor_count": validation_count,
        "role_summaries": summaries,
        "market_execution_enabled": False,
        "offline_strategy_challenger_passed": not failures,
        "production_promote": False,
    }
    if failures:
        return PromotionGateResult(False, "blocked", "; ".join(failures), metrics)
    return PromotionGateResult(
        True,
        "offline_strategy_challenger",
        "UA-context safe-switch scorer beats corrected V2+ under strict LP/oracle evidence",
        metrics,
    )


def _fit_safe_switch_for_scope(
    rows: list[dict[str, Any]],
    *,
    tenant_id: str,
    source_model_name: str,
    scorer_kind: str,
    min_prior_safe_win_count: int,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    allowed_candidate_sources: set[str],
    ridge_l2: float,
    torch_hidden_size: int,
    torch_max_epochs: int,
    use_cuda_if_available: bool,
) -> dict[str, Any]:
    train_rows = [
        row
        for row in rows
        if str(row["split_name"]) != "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    final_rows = [
        row
        for row in rows
        if str(row["split_name"]) == "final_holdout"
        and bool(row["eligible_for_final_selection"])
    ]
    if not train_rows:
        raise ValueError(f"{tenant_id}/{source_model_name} safe switch needs train rows.")
    if not final_rows:
        raise ValueError(f"{tenant_id}/{source_model_name} safe switch needs final rows.")
    train_challengers = [
        row for row in train_rows if str(row["candidate_source"]) in allowed_candidate_sources
    ]
    if not train_challengers:
        raise ValueError(
            f"{tenant_id}/{source_model_name} safe switch needs challenger train rows."
        )
    predictor = _fit_predictor(
        train_challengers,
        scorer_kind=scorer_kind,
        ridge_l2=ridge_l2,
        torch_hidden_size=torch_hidden_size,
        torch_max_epochs=torch_max_epochs,
        use_cuda_if_available=use_cuda_if_available,
    )
    profile_stats = _profile_stats(train_challengers, ridge_l2=ridge_l2)
    selected_final: list[dict[str, Any]] = []
    fallback_anchor_keys: list[str] = []
    predicted_deltas: dict[str, float] = {}
    predicted_tail_risk: dict[str, float] = {}
    for anchor, anchor_rows in sorted(_rows_by_anchor(final_rows).items()):
        candidates: list[tuple[dict[str, Any], float, float]] = []
        for row in anchor_rows:
            profile = _profile_key(row)
            if (
                str(row["candidate_source"]) not in allowed_candidate_sources
                or profile not in profile_stats
            ):
                continue
            stats = profile_stats[profile]
            if int(stats["safe_win_count"]) < min_prior_safe_win_count:
                continue
            if float(stats["mean_prior_delta_uah"]) > -min_prior_mean_improvement_uah:
                continue
            model_delta, model_tail = predictor(row)
            profile_delta = float(stats["predicted_regret_delta_vs_v2_plus_uah"])
            profile_tail = float(stats["predicted_tail_risk_probability"])
            predicted_delta = min(model_delta, profile_delta)
            tail_probability = max(profile_tail, min(model_tail, profile_tail + 0.05))
            predicted_deltas[_candidate_key(row)] = predicted_delta
            predicted_tail_risk[_candidate_key(row)] = tail_probability
            if (
                predicted_delta <= -min_predicted_improvement_uah
                and tail_probability <= max_predicted_tail_risk_probability
            ):
                candidates.append((row, predicted_delta, tail_probability))
        if not candidates:
            fallback_anchor_keys.append(_anchor_key_from_parts(tenant_id, source_model_name, anchor))
            continue
        selected_final.append(
            min(
                candidates,
                key=lambda item: (
                    item[2],
                    item[1],
                    float(item[0]["selector_feature_schedule_distance_from_v2_plus"]),
                    str(item[0]["candidate_family"]),
                    str(item[0]["candidate_model_name"]),
                ),
            )[0]
        )
    selected_counts = _source_counts(selected_final)
    selected_family_counts = _family_counts(selected_final)
    if fallback_anchor_keys:
        selected_counts["frozen_v2_plus_fallback"] = len(fallback_anchor_keys)
        selected_family_counts["frozen_v2_plus_fallback"] = len(fallback_anchor_keys)
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "learner_model_name": _model_name_for_scorer(scorer_kind),
        "scorer_kind": scorer_kind,
        "target_label_space": "schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "selected_scorer_type": predictor.scorer_type,
        "selected_feature_names": list(UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS),
        "allowed_candidate_sources": sorted(allowed_candidate_sources),
        "risk_profile_prior_stats": profile_stats,
        "fallback_to_v2_plus": not selected_final,
        "uses_v2_plus_anchor_fallback": bool(fallback_anchor_keys),
        "selector_gate_blocker": (
            "ua_context_safe_switch_candidate_selected"
            if selected_final
            else "no_prior_safe_ua_context_profile"
        ),
        "min_predicted_improvement_uah": min_predicted_improvement_uah,
        "max_predicted_tail_risk_probability": max_predicted_tail_risk_probability,
        "train_anchor_count": _anchor_count(train_rows),
        "final_holdout_anchor_count": _anchor_count(final_rows),
        "fallback_final_anchor_keys": fallback_anchor_keys,
        "selected_final_candidate_keys": [_candidate_key(row) for row in selected_final],
        "selected_final_profile_keys": [_profile_key(row) for row in selected_final],
        "selected_final_family_counts": selected_family_counts,
        "selected_final_candidate_source_counts": selected_counts,
        "predicted_final_candidate_deltas": predicted_deltas,
        "predicted_final_tail_risk_probabilities": predicted_tail_risk,
        "model_training_metadata": predictor.metadata,
        "claim_scope": UA_CONTEXT_SAFE_SWITCH_SCORER_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


class _Predictor:
    def __init__(
        self,
        scorer_type: str,
        metadata: dict[str, Any],
        predict: Callable[[dict[str, Any]], tuple[float, float]],
    ) -> None:
        self.scorer_type = scorer_type
        self.metadata = metadata
        self._predict = predict

    def __call__(self, row: dict[str, Any]) -> tuple[float, float]:
        return self._predict(row)


def _fit_predictor(
    rows: list[dict[str, Any]],
    *,
    scorer_kind: str,
    ridge_l2: float,
    torch_hidden_size: int,
    torch_max_epochs: int,
    use_cuda_if_available: bool,
) -> _Predictor:
    if scorer_kind == "sklearn":
        return _fit_sklearn_predictor(rows, ridge_l2=ridge_l2)
    if scorer_kind == "torch":
        return _fit_torch_predictor(
            rows,
            ridge_l2=ridge_l2,
            torch_hidden_size=torch_hidden_size,
            torch_max_epochs=torch_max_epochs,
            use_cuda_if_available=use_cuda_if_available,
        )
    raise ValueError(f"unsupported scorer_kind: {scorer_kind}")


def _fit_sklearn_predictor(
    rows: list[dict[str, Any]],
    *,
    ridge_l2: float,
) -> _Predictor:
    try:
        from sklearn.linear_model import LogisticRegression, Ridge
        from sklearn.preprocessing import StandardScaler
    except Exception:
        return _constant_predictor(rows, scorer_type="sklearn_unavailable_profile_fallback")

    x = [_feature_vector(row) for row in rows]
    y_delta = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in rows]
    y_tail = [int(bool(row["label_tail_risk_loss"])) for row in rows]
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(x)
    ridge = Ridge(alpha=max(ridge_l2, 1e-6), random_state=17)
    ridge.fit(x_scaled, y_delta)
    if len(set(y_tail)) > 1:
        logistic = LogisticRegression(random_state=17, max_iter=200)
        logistic.fit(x_scaled, y_tail)
        constant_tail: float | None = None
    else:
        logistic = None
        constant_tail = float(y_tail[0]) if y_tail else 0.0

    def predict(row: dict[str, Any]) -> tuple[float, float]:
        vector = scaler.transform([_feature_vector(row)])
        delta = float(ridge.predict(vector)[0])
        if logistic is None:
            tail = float(constant_tail or 0.0)
        else:
            tail = float(logistic.predict_proba(vector)[0][1])
        return delta, tail

    return _Predictor(
        "sklearn_ridge_logistic_ua_context_v1",
        {
            "library": "sklearn",
            "train_row_count": len(rows),
            "feature_count": len(UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS),
            "tail_classifier_constant": constant_tail,
        },
        predict,
    )


def _fit_torch_predictor(
    rows: list[dict[str, Any]],
    *,
    ridge_l2: float,
    torch_hidden_size: int,
    torch_max_epochs: int,
    use_cuda_if_available: bool,
) -> _Predictor:
    try:
        import torch
    except Exception:
        return _constant_predictor(rows, scorer_type="torch_unavailable_profile_fallback")

    torch.manual_seed(17)
    device = torch.device(
        "cuda" if use_cuda_if_available and torch.cuda.is_available() else "cpu"
    )
    x_values = [_feature_vector(row) for row in rows]
    y_delta_values = [
        [float(row["label_regret_delta_vs_v2_plus_uah"])] for row in rows
    ]
    y_tail_values = [[float(bool(row["label_tail_risk_loss"]))] for row in rows]
    feature_count = len(UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS)
    model = torch.nn.Sequential(
        torch.nn.Linear(feature_count, torch_hidden_size),
        torch.nn.ReLU(),
        torch.nn.Linear(torch_hidden_size, 2),
    ).to(device)
    x_tensor = torch.tensor(x_values, dtype=torch.float32, device=device)
    mean_tensor = x_tensor.mean(dim=0, keepdim=True)
    std_tensor = x_tensor.std(dim=0, keepdim=True)
    std_tensor = torch.where(std_tensor < 1e-6, torch.ones_like(std_tensor), std_tensor)
    x_norm = (x_tensor - mean_tensor) / std_tensor
    delta_tensor = torch.tensor(y_delta_values, dtype=torch.float32, device=device)
    tail_tensor = torch.tensor(y_tail_values, dtype=torch.float32, device=device)
    optimizer = torch.optim.Adam(model.parameters(), lr=0.03, weight_decay=ridge_l2 * 1e-4)
    for _ in range(torch_max_epochs):
        optimizer.zero_grad()
        output = model(x_norm)
        delta_loss = torch.nn.functional.mse_loss(output[:, :1], delta_tensor)
        tail_loss = torch.nn.functional.binary_cross_entropy_with_logits(
            output[:, 1:2],
            tail_tensor,
        )
        loss = delta_loss + 25.0 * tail_loss
        loss.backward()
        optimizer.step()

    def predict(row: dict[str, Any]) -> tuple[float, float]:
        with torch.no_grad():
            vector = torch.tensor(
                [_feature_vector(row)],
                dtype=torch.float32,
                device=device,
            )
            vector = (vector - mean_tensor) / std_tensor
            output = model(vector)
            delta = float(output[0, 0].detach().cpu().item())
            tail = float(torch.sigmoid(output[0, 1]).detach().cpu().item())
        return delta, tail

    profile_fallback = _constant_predictor(rows, scorer_type="profile_fallback")

    def conservative_predict(row: dict[str, Any]) -> tuple[float, float]:
        torch_delta, torch_tail = predict(row)
        profile_delta, profile_tail = profile_fallback(row)
        return min(torch_delta, profile_delta), max(torch_tail, profile_tail)

    return _Predictor(
        "torch_mlp_ua_context_v1",
        {
            "library": "torch",
            "device": str(device),
            "train_row_count": len(rows),
            "feature_count": feature_count,
            "hidden_size": torch_hidden_size,
            "max_epochs": torch_max_epochs,
        },
        conservative_predict,
    )


def _constant_predictor(rows: list[dict[str, Any]], *, scorer_type: str) -> _Predictor:
    deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in rows]
    tail_losses = [float(bool(row["label_tail_risk_loss"])) for row in rows]
    predicted_delta = mean(deltas) if deltas else 0.0
    predicted_tail = (sum(tail_losses) + 1.0) / (len(tail_losses) + 2.0)

    def predict(row: dict[str, Any]) -> tuple[float, float]:
        del row
        return predicted_delta, predicted_tail

    return _Predictor(
        scorer_type,
        {
            "library": "profile_stats",
            "train_row_count": len(rows),
            "constant_delta_uah": predicted_delta,
            "constant_tail_risk_probability": predicted_tail,
        },
        predict,
    )


def _reference_row(
    row: dict[str, Any],
    *,
    selection_role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    copied = dict(row)
    payload = dict(_payload(row))
    payload.update(
        {
            "ua_context_safe_switch_role": selection_role,
            "claim_scope": UA_CONTEXT_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    copied.update(
        {
            "selection_role": selection_role,
            "strategy_kind": UA_CONTEXT_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
            "generated_at": generated_at,
            "claim_scope": UA_CONTEXT_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "evaluation_payload": payload,
        }
    )
    return copied


def _candidate_benchmark_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    payload = dict(_payload(row))
    key = _candidate_key(row)
    payload.update(
        {
            "ua_context_safe_switch_selected": True,
            "selected_candidate_key": key,
            "scorer_kind": str(scorer_row["scorer_kind"]),
            "predicted_regret_delta_vs_v2_plus_uah": dict(
                scorer_row["predicted_final_candidate_deltas"]
            ).get(key),
            "predicted_tail_risk_probability": dict(
                scorer_row["predicted_final_tail_risk_probabilities"]
            ).get(key),
            "claim_scope": UA_CONTEXT_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
            "not_full_dfl": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
        }
    )
    return {
        "evaluation_id": (
            f"{row['tenant_id']}:ua-context-safe-switch:{scorer_row['scorer_kind']}:"
            f"{row['source_model_name']}:{row['candidate_family']}:"
            f"{_datetime_value(row['anchor_timestamp']):%Y%m%dT%H%M}"
        ),
        "tenant_id": str(row["tenant_id"]),
        "source_model_name": str(row["source_model_name"]),
        "forecast_model_name": str(scorer_row["learner_model_name"]),
        "strategy_kind": UA_CONTEXT_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND,
        "market_venue": "DAM",
        "anchor_timestamp": _datetime_value(row["anchor_timestamp"]),
        "generated_at": generated_at,
        "horizon_hours": int(row["horizon_hours"]),
        "starting_soc_fraction": _first_or_default(row["soc_fraction_vector"], 0.5),
        "starting_soc_source": "ua_context_safe_switch_feature_panel",
        "decision_value_uah": float(row["decision_value_uah"]),
        "forecast_objective_value_uah": float(row["forecast_objective_value_uah"]),
        "oracle_value_uah": float(row["oracle_value_uah"]),
        "regret_uah": float(row["regret_uah"]),
        "regret_ratio": float(row["regret_ratio"]),
        "total_degradation_penalty_uah": float(row["total_degradation_penalty_uah"]),
        "total_throughput_mwh": float(row["total_throughput_mwh"]),
        "committed_action": v2._committed_action(row),
        "committed_power_mw": abs(_first_or_default(row["dispatch_mw_vector"], 0.0)),
        "rank_by_regret": 1,
        "data_quality_tier": "thesis_grade",
        "observed_coverage_ratio": 1.0,
        "safety_violation_count": int(row["safety_violation_count"]),
        "selection_role": _selection_role_for_scorer(str(scorer_row["scorer_kind"])),
        "selected_candidate_family": str(row["candidate_family"]),
        "selected_candidate_model_name": str(row["candidate_model_name"]),
        "fallback_to_v2_plus": False,
        "claim_scope": UA_CONTEXT_SAFE_SWITCH_STRICT_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
        "evaluation_payload": payload,
    }


def _fallback_benchmark_row(
    row: dict[str, Any],
    *,
    scorer_row: dict[str, Any],
    generated_at: datetime,
) -> dict[str, Any]:
    copied = _reference_row(
        row,
        selection_role=_selection_role_for_scorer(str(scorer_row["scorer_kind"])),
        generated_at=generated_at,
    )
    payload = dict(copied["evaluation_payload"])
    payload.update(
        {
            "ua_context_safe_switch_selected": False,
            "fallback_to_corrected_v2_plus": True,
            "scorer_kind": str(scorer_row["scorer_kind"]),
            "selector_gate_blocker": str(scorer_row["selector_gate_blocker"]),
        }
    )
    copied.update(
        {
            "forecast_model_name": str(scorer_row["learner_model_name"]),
            "fallback_to_v2_plus": True,
            "evaluation_payload": payload,
        }
    )
    return copied


def _rolling_summary_row(
    panel: pl.DataFrame,
    scorer: pl.DataFrame,
    *,
    scorer_kind: str,
    source_model_name: str,
    window_index: int,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
    min_mean_regret_improvement_ratio_vs_v2_plus: float,
    min_mean_regret_improvement_ratio_vs_strict: float,
) -> dict[str, Any]:
    rows = list(panel.iter_rows(named=True))
    scorer_rows = list(scorer.iter_rows(named=True))
    strict_rows = [
        row
        for row in rows
        if str(row["candidate_family"]) == v2.CANDIDATE_FAMILY_STRICT
        and str(row["split_name"]) == "final_holdout"
    ]
    v2_rows = [
        row
        for row in rows
        if str(row["candidate_source"]) == "v2_plus_default"
        and str(row["split_name"]) == "final_holdout"
    ]
    selected_rows = _selected_validation_rows(rows, scorer_rows)
    strict_mean = _mean_regret(strict_rows)
    v2_mean = _mean_regret(v2_rows)
    selected_mean = _mean_regret(selected_rows)
    strict_median = _median_regret(strict_rows)
    v2_median = _median_regret(v2_rows)
    selected_median = _median_regret(selected_rows)
    improvement_vs_v2 = _improvement_ratio(v2_mean, selected_mean)
    improvement_vs_strict = _improvement_ratio(strict_mean, selected_mean)
    median_not_worse = selected_median <= v2_median
    window_passed = (
        improvement_vs_v2 >= min_mean_regret_improvement_ratio_vs_v2_plus
        and improvement_vs_strict >= min_mean_regret_improvement_ratio_vs_strict
        and median_not_worse
    )
    diagnostic_passed = improvement_vs_v2 > 0.0 and median_not_worse
    return {
        "source_model_name": source_model_name,
        "scorer_kind": scorer_kind,
        "selection_role": _selection_role_for_scorer(scorer_kind),
        "window_index": window_index,
        "tenant_count": len({str(row["tenant_id"]) for row in selected_rows}),
        "validation_anchor_count_per_tenant": len(validation_anchors),
        "validation_tenant_anchor_count": _anchor_count(selected_rows),
        "minimum_prior_anchor_count_before_window": len(prior_anchors),
        "strict_mean_regret_uah": strict_mean,
        "v2_plus_mean_regret_uah": v2_mean,
        "selected_mean_regret_uah": selected_mean,
        "strict_median_regret_uah": strict_median,
        "v2_plus_median_regret_uah": v2_median,
        "selected_median_regret_uah": selected_median,
        "mean_regret_improvement_ratio_vs_v2_plus": improvement_vs_v2,
        "mean_regret_improvement_ratio_vs_strict": improvement_vs_strict,
        "median_not_worse_vs_v2_plus": median_not_worse,
        "rolling_window_passed": window_passed,
        "diagnostic_window_passed": diagnostic_passed,
        "fallback_row_count": sum(
            len(row["fallback_final_anchor_keys"]) for row in scorer_rows
        ),
        "selected_candidate_source_counts": _source_counts(selected_rows),
        "validation_window_anchor_start": min(validation_anchors).isoformat(),
        "validation_window_anchor_end": max(validation_anchors).isoformat(),
        "target_label_space": "schedule_candidate_index",
        "raw_hourly_action_imitation": False,
        "claim_scope": UA_CONTEXT_SAFE_SWITCH_ROBUSTNESS_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _selected_validation_rows(
    rows: list[dict[str, Any]],
    scorer_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_key = {_candidate_key(row): row for row in rows}
    fallback_by_anchor = {
        _anchor_key(row): row
        for row in rows
        if str(row["candidate_source"]) == "v2_plus_default"
        and str(row["split_name"]) == "final_holdout"
    }
    selected: list[dict[str, Any]] = []
    for scorer_row in scorer_rows:
        for key in scorer_row["selected_final_candidate_keys"]:
            selected.append(by_key[str(key)])
        for anchor_key in scorer_row["fallback_final_anchor_keys"]:
            selected.append(fallback_by_anchor[str(anchor_key)])
    return selected


def _with_window_split(
    rows: list[dict[str, Any]],
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_anchors: tuple[datetime, ...],
    prior_anchors: tuple[datetime, ...],
) -> pl.DataFrame:
    tenant_set = set(tenant_ids)
    validation_set = set(validation_anchors)
    prior_set = set(prior_anchors)
    output_rows: list[dict[str, Any]] = []
    for row in rows:
        if str(row["tenant_id"]) not in tenant_set:
            continue
        if str(row["source_model_name"]) != source_model_name:
            continue
        anchor = _datetime_value(row["anchor_timestamp"])
        if anchor in validation_set:
            split_name = "final_holdout"
        elif anchor in prior_set:
            split_name = "train_selection"
        else:
            continue
        copied = dict(row)
        copied["split_name"] = split_name
        copied["is_train_or_prior_anchor"] = split_name != "final_holdout"
        output_rows.append(copied)
    return pl.DataFrame(output_rows, infer_schema_length=None)


def _rolling_windows(
    rows: list[dict[str, Any]],
    *,
    tenant_ids: tuple[str, ...],
    source_model_name: str,
    validation_window_count: int,
    validation_anchor_count: int,
    min_prior_anchors_before_window: int,
) -> list[tuple[int, tuple[datetime, ...], tuple[datetime, ...]]]:
    anchors = sorted(
        {
            _datetime_value(row["anchor_timestamp"])
            for row in rows
            if str(row["source_model_name"]) == source_model_name
            and str(row["tenant_id"]) in set(tenant_ids)
        },
        reverse=True,
    )
    windows: list[tuple[int, tuple[datetime, ...], tuple[datetime, ...]]] = []
    for index in range(validation_window_count):
        start = index * validation_anchor_count
        end = start + validation_anchor_count
        validation = tuple(sorted(anchors[start:end]))
        if len(validation) != validation_anchor_count:
            raise ValueError(
                f"{source_model_name} lacks validation anchors for window {index + 1}; "
                f"expected {validation_anchor_count}, observed {len(validation)}"
            )
        prior = tuple(anchor for anchor in sorted(anchors) if anchor < min(validation))
        if len(prior) < min_prior_anchors_before_window:
            raise ValueError(
                f"{source_model_name} window {index + 1} needs at least "
                f"{min_prior_anchors_before_window} prior anchors; observed {len(prior)}"
            )
        windows.append((index + 1, validation, prior))
    return windows


def _profile_stats(
    rows: list[dict[str, Any]],
    *,
    ridge_l2: float,
) -> dict[str, dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_profile_key(row), []).append(row)
    stats: dict[str, dict[str, Any]] = {}
    for profile, profile_rows in grouped.items():
        deltas = [float(row["label_regret_delta_vs_v2_plus_uah"]) for row in profile_rows]
        tail_losses = [bool(row["label_tail_risk_loss"]) for row in profile_rows]
        safe_wins = [bool(row["label_safe_switch_win"]) for row in profile_rows]
        count = len(profile_rows)
        stats[profile] = {
            "candidate_source": str(profile_rows[0]["candidate_source"]),
            "candidate_family": str(profile_rows[0]["candidate_family"]),
            "candidate_model_name": str(profile_rows[0]["candidate_model_name"]),
            "train_row_count": count,
            "safe_win_count": sum(1 for value in safe_wins if value),
            "tail_loss_count": sum(1 for value in tail_losses if value),
            "safe_precision": (
                sum(1 for value in safe_wins if value) / count if count else 0.0
            ),
            "mean_prior_delta_uah": mean(deltas),
            "predicted_regret_delta_vs_v2_plus_uah": sum(deltas)
            / (count + ridge_l2),
            "predicted_tail_risk_probability": (
                sum(1 for value in tail_losses if value) + 1.0
            )
            / (count + 2.0),
        }
    return stats


def _separating_features(
    safe_rows: list[dict[str, Any]],
    tail_rows: list[dict[str, Any]],
) -> list[str]:
    if not safe_rows:
        return []
    if not tail_rows:
        return [
            column
            for column in UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS
            if _std([float(row[column]) for row in safe_rows]) > 0.0
        ][:8] or list(UA_CONTEXT_SELECTOR_FEATURE_COLUMNS[:3])
    scores: list[tuple[float, str]] = []
    for column in UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS:
        safe_mean = mean(float(row[column]) for row in safe_rows)
        tail_mean = mean(float(row[column]) for row in tail_rows)
        delta = abs(safe_mean - tail_mean)
        if delta > 1e-9:
            scores.append((delta, column))
    return [column for _, column in sorted(scores, reverse=True)]


def _audit_interpretation(
    *,
    train_candidates: list[dict[str, Any]],
    safe_train: list[dict[str, Any]],
    tail_train: list[dict[str, Any]],
    separating_features: list[str],
) -> str:
    if not train_candidates:
        return "no_train_candidates_for_safe_switch"
    if not safe_train:
        return "no_prior_safe_wins"
    if tail_train and not separating_features:
        return "safe_wins_not_separable_from_tail_risk"
    if separating_features:
        return "prior_context_has_separable_signal"
    return "prior_safe_wins_exist_but_context_variance_is_weak"


def _context_ready_ratio(rows: list[dict[str, Any]]) -> float:
    if not rows:
        return 0.0
    ready = [
        float(row.get("selector_feature_weather_load_context_ready", 0.0)) > 0.0
        and float(row.get("selector_feature_publication_time_ready", 0.0)) > 0.0
        and float(row.get("selector_feature_grid_event_context_ready", 0.0)) > 0.0
        for row in rows
    ]
    return sum(1 for value in ready if value) / len(ready)


def _feature_vector(row: dict[str, Any]) -> list[float]:
    return [float(row[column]) for column in UA_CONTEXT_SAFE_SWITCH_FEATURE_COLUMNS]


def _unique_anchor_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    return frame.unique(list(_CONTEXT_JOIN_COLUMNS), maintain_order=True).select(
        [*_CONTEXT_JOIN_COLUMNS, "split_name"]
    ).to_dicts()


def _latest_row_before_anchor(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
) -> dict[str, Any] | None:
    if frame.is_empty() or "timestamp" not in frame.columns:
        return None
    filtered = frame
    if "tenant_id" in frame.columns:
        filtered = filtered.filter(pl.col("tenant_id") == tenant_id)
    if filtered.is_empty():
        return None
    rows = [
        row
        for row in filtered.iter_rows(named=True)
        if (row_timestamp := _row_timestamp(row)) is not None
        and row_timestamp < anchor_timestamp
    ]
    if not rows:
        return None
    return max(rows, key=lambda row: _row_timestamp(row) or datetime.min)


def _publication_timestamp(
    row: dict[str, Any] | None,
    *,
    anchor_timestamp: datetime,
) -> datetime | None:
    if row is None:
        return None
    for column in (
        "publication_timestamp",
        "source_publication_timestamp",
        "published_at",
        "available_at",
    ):
        value = row.get(column)
        if value is not None:
            return _datetime_value(value)
    source_kind = str(row.get("source_kind", row.get("price_source_kind", ""))).lower()
    if "observed" in source_kind or "oree" in source_kind:
        return anchor_timestamp - timedelta(hours=24)
    return None


def _context_by_key(frame: pl.DataFrame) -> dict[tuple[str, str, datetime], dict[str, Any]]:
    _require_columns(
        frame,
        frozenset(_CONTEXT_JOIN_COLUMNS),
        frame_name="UA context frame",
    )
    return {_context_key(row): row for row in frame.iter_rows(named=True)}


def _selector_context(row: dict[str, Any] | None) -> dict[str, float]:
    if row is None:
        return {}
    return {
        column: float(row[column])
        for column in row
        if str(column).startswith("selector_feature_")
    }


def _context_key(row: dict[str, Any]) -> tuple[str, str, datetime]:
    return (
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _row_timestamp(row: dict[str, Any] | None) -> datetime | None:
    if row is None:
        return None
    value = row.get("timestamp")
    return _datetime_value(value) if value is not None else None


def _row_float(
    row: dict[str, Any] | None,
    columns: Sequence[str],
    *,
    default: float = 0.0,
) -> float:
    if row is None:
        return default
    for column in columns:
        value = row.get(column)
        if value is not None:
            return float(value)
    return default


def _row_text(row: dict[str, Any] | None, columns: Sequence[str]) -> str:
    if row is None:
        return ""
    for column in columns:
        value = row.get(column)
        if value is not None:
            return str(value)
    return ""


def _hours_between(
    start: datetime | None,
    end: datetime,
    *,
    default: float,
) -> float:
    if start is None:
        return default
    return max((end - start).total_seconds() / 3600.0, 0.0)


def _selection_role_for_scorer(scorer_kind: str) -> str:
    if scorer_kind == "sklearn":
        return UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN
    if scorer_kind == "torch":
        return UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH
    raise ValueError(f"unsupported scorer_kind: {scorer_kind}")


def _model_name_for_scorer(scorer_kind: str) -> str:
    if scorer_kind == "sklearn":
        return UA_CONTEXT_SAFE_SWITCH_SKLEARN_MODEL_NAME
    if scorer_kind == "torch":
        return UA_CONTEXT_SAFE_SWITCH_TORCH_MODEL_NAME
    raise ValueError(f"unsupported scorer_kind: {scorer_kind}")


def _validate_scorer_config(
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    scorer_kinds: tuple[str, ...],
    min_prior_safe_win_count: int,
    min_prior_mean_improvement_uah: float,
    min_predicted_improvement_uah: float,
    max_predicted_tail_risk_probability: float,
    allowed_candidate_sources: tuple[str, ...],
    ridge_l2: float,
    torch_hidden_size: int,
    torch_max_epochs: int,
) -> None:
    if not tenant_ids:
        raise ValueError("tenant_ids must not be empty.")
    if not forecast_model_names:
        raise ValueError("forecast_model_names must not be empty.")
    if not scorer_kinds:
        raise ValueError("scorer_kinds must not be empty.")
    unsupported = sorted(set(scorer_kinds).difference({"sklearn", "torch"}))
    if unsupported:
        raise ValueError(f"unsupported scorer kinds: {unsupported}")
    if min_prior_safe_win_count < 1:
        raise ValueError("min_prior_safe_win_count must be at least 1.")
    if min_prior_mean_improvement_uah < 0.0:
        raise ValueError("min_prior_mean_improvement_uah must not be negative.")
    if min_predicted_improvement_uah < 0.0:
        raise ValueError("min_predicted_improvement_uah must not be negative.")
    if not 0.0 <= max_predicted_tail_risk_probability <= 1.0:
        raise ValueError(
            "max_predicted_tail_risk_probability must be between 0 and 1."
        )
    if not allowed_candidate_sources:
        raise ValueError("allowed_candidate_sources must not be empty.")
    if ridge_l2 < 0.0:
        raise ValueError("ridge_l2 must not be negative.")
    if torch_hidden_size < 1:
        raise ValueError("torch_hidden_size must be positive.")
    if torch_max_epochs < 1:
        raise ValueError("torch_max_epochs must be positive.")


def _validate_oracle_gap_panel(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "anchor_timestamp",
                "split_name",
                "candidate_source",
                "eligible_for_final_selection",
                "market_execution_enabled",
                *oracle_gap.ORACLE_GAP_SELECTOR_FEATURE_COLUMNS,
            }
        ),
        frame_name="oracle-gap feature panel",
    )
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("UA-context safe-switch refuses market execution.")


def _validate_ua_context_panel(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _REQUIRED_UA_CONTEXT_PANEL_COLUMNS,
        frame_name="UA-context oracle-gap feature panel",
    )
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("UA-context feature panel refuses market execution.")
    if frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("UA-context safe switch does not imitate raw hourly actions.")


def _validate_scorer_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        frozenset(
            {
                "tenant_id",
                "source_model_name",
                "scorer_kind",
                "selected_final_candidate_keys",
                "fallback_final_anchor_keys",
                "predicted_final_candidate_deltas",
                "predicted_final_tail_risk_probabilities",
                "raw_hourly_action_imitation",
                "market_execution_enabled",
            }
        ),
        frame_name="UA-context safe-switch scorer frame",
    )
    if frame.select(pl.col("market_execution_enabled").any()).item():
        raise ValueError("UA-context scorer frame refuses market execution.")
    if frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("UA-context scorer does not imitate raw hourly actions.")


def _require_columns(frame: pl.DataFrame, columns: frozenset[str], *, frame_name: str) -> None:
    missing = sorted(columns.difference(frame.columns))
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {missing}")


def _datetime_value(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.replace(tzinfo=None)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00")).replace(tzinfo=None)


def _rows_by_anchor(rows: list[dict[str, Any]]) -> dict[datetime, list[dict[str, Any]]]:
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for row in rows:
        grouped.setdefault(_datetime_value(row["anchor_timestamp"]), []).append(row)
    return grouped


def _candidate_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row["tenant_id"]),
            str(row["source_model_name"]),
            _datetime_value(row["anchor_timestamp"]).isoformat(),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ]
    )


def _anchor_key(row: dict[str, Any]) -> str:
    return _anchor_key_from_parts(
        str(row["tenant_id"]),
        str(row["source_model_name"]),
        _datetime_value(row["anchor_timestamp"]),
    )


def _anchor_key_from_parts(tenant_id: str, source_model_name: str, anchor: datetime) -> str:
    return "|".join([tenant_id, source_model_name, anchor.isoformat()])


def _profile_key(row: dict[str, Any]) -> str:
    return "|".join(
        [
            str(row["candidate_source"]),
            str(row["candidate_family"]),
            str(row["candidate_model_name"]),
        ]
    )


def _role_summaries(frame: pl.DataFrame) -> dict[str, dict[str, Any]]:
    summaries: dict[str, dict[str, Any]] = {}
    for role in sorted(str(value) for value in frame["selection_role"].unique()):
        role_rows = frame.filter(pl.col("selection_role") == role).to_dicts()
        summaries[role] = {
            "row_count": len(role_rows),
            "tenant_anchor_count": len(
                {
                    (
                        str(row["tenant_id"]),
                        str(row["source_model_name"]),
                        _datetime_value(row["anchor_timestamp"]),
                    )
                    for row in role_rows
                }
            ),
            "mean_regret_uah": _mean_regret(role_rows),
            "median_regret_uah": _median_regret(role_rows),
        }
    return summaries


def _tenant_anchor_count(frame: pl.DataFrame) -> int:
    return len(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"]),
            )
            for row in frame.iter_rows(named=True)
        }
    )


def _anchor_count(rows: list[dict[str, Any]]) -> int:
    return len(
        {
            (
                str(row["tenant_id"]),
                str(row["source_model_name"]),
                _datetime_value(row["anchor_timestamp"]),
            )
            for row in rows
        }
    )


def _source_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row.get("candidate_source", "unknown_source"))
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _family_counts(rows: list[dict[str, Any]]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for row in rows:
        key = str(row["candidate_family"])
        counts[key] = counts.get(key, 0) + 1
    return dict(sorted(counts.items()))


def _mean_regret(rows: list[dict[str, Any]]) -> float:
    return mean(float(row["regret_uah"]) for row in rows) if rows else 0.0


def _median_regret(rows: list[dict[str, Any]]) -> float:
    return median(float(row["regret_uah"]) for row in rows) if rows else 0.0


def _improvement_ratio(baseline: float, challenger: float) -> float:
    return (baseline - challenger) / abs(baseline) if abs(baseline) > 1e-9 else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    avg = mean(values)
    return (sum((value - avg) ** 2 for value in values) / (len(values) - 1)) ** 0.5


def _first_or_default(values: Any, default: float) -> float:
    parsed = _float_list(values)
    return parsed[0] if parsed else default


def _float_list(value: Any) -> list[float]:
    if value is None:
        return []
    if isinstance(value, list | tuple):
        return [float(item) for item in value]
    if isinstance(value, pl.Series):
        return [float(item) for item in value.to_list()]
    return [float(value)]


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload", {})
    return dict(payload) if isinstance(payload, dict) else {}


def _latest_generated_at(frame: pl.DataFrame) -> datetime:
    return max(_datetime_value(row["generated_at"]) for row in frame.iter_rows(named=True))


__all__ = [
    "UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_SKLEARN",
    "UA_CONTEXT_SAFE_SWITCH_SELECTION_ROLE_TORCH",
    "UA_CONTEXT_SAFE_SWITCH_STRICT_LP_STRATEGY_KIND",
    "build_dfl_ua_calendar_publication_context_frame",
    "build_dfl_ua_context_oracle_gap_feature_panel_frame",
    "build_dfl_ua_context_safe_switch_rolling_robustness_frame",
    "build_dfl_ua_context_safe_switch_scorer_frame",
    "build_dfl_ua_context_safe_switch_separability_audit_frame",
    "build_dfl_ua_context_safe_switch_strict_lp_benchmark_frame",
    "build_dfl_ua_grid_event_context_frame",
    "build_dfl_ua_weather_load_context_frame",
    "evaluate_dfl_ua_context_safe_switch_gate",
]
