"""Point-in-time context repair and context-enriched candidate-value DFL v5."""

from __future__ import annotations

from datetime import datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.dfl import candidate_value_dfl_v3 as v3
from smart_arbitrage.dfl import candidate_value_dfl_v4 as v4
from smart_arbitrage.dfl import schedule_value_learner as v2
from smart_arbitrage.dfl import schedule_value_learner_v2_plus as v2_plus
from smart_arbitrage.dfl.promotion_gate import (
    DEFAULT_MIN_ANCHOR_COUNT,
    DEFAULT_MIN_MEAN_REGRET_IMPROVEMENT_RATIO,
    PromotionGateResult,
)
from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome

POINT_IN_TIME_CONTEXT_REPAIR_CLAIM_SCOPE: Final[str] = (
    "dfl_point_in_time_context_repair_not_full_dfl"
)
POINT_IN_TIME_CONTEXT_FEATURE_PANEL_CLAIM_SCOPE: Final[str] = (
    "dfl_point_in_time_context_feature_panel_not_full_dfl"
)
CONTEXT_ENRICHED_CANDIDATE_LIBRARY_V5_CLAIM_SCOPE: Final[str] = (
    "dfl_context_enriched_schedule_candidate_library_v5_not_full_dfl"
)
CONTEXT_ENRICHED_LABEL_PANEL_V5_CLAIM_SCOPE: Final[str] = (
    "dfl_context_enriched_candidate_value_label_panel_v5_not_full_dfl"
)
CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_CLAIM_SCOPE: Final[str] = (
    "dfl_context_enriched_candidate_value_dfl_v5_not_full_dfl"
)
CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_CLAIM_SCOPE: Final[str] = (
    "dfl_context_enriched_candidate_value_dfl_v5_strict_lp_gate_not_full_dfl"
)
CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND: Final[str] = (
    "dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark"
)
CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_PREFIX: Final[str] = (
    "dfl_context_enriched_candidate_value_dfl_v5_"
)
CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_ACADEMIC_SCOPE: Final[str] = (
    "Context-enriched candidate-level value scorer over V4 feasible LP-scored "
    "schedules. It uses only point-in-time Ukrainian context features and falls "
    "back to frozen V2+ unless prior/train evidence predicts improvement. This "
    "is not full DFL and not market execution."
)
LEARNED_SCORER_TYPE_V5: Final[str] = (
    "context_enriched_learned_linear_candidate_value_v5"
)
LEARNED_SCORER_PROFILE_NAME_V5: Final[str] = "context_enriched_candidate_value_ridge_v5"
V5_CONTEXT_SELECTOR_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "selector_feature_weather_load_context_ready",
    "selector_feature_calendar_event_context_ready",
    "selector_feature_publication_time_ready",
    "selector_feature_context_blocker_count",
    "selector_feature_anchor_hour",
    "selector_feature_anchor_day_of_week",
    "selector_feature_anchor_is_weekend",
    "selector_feature_weather_temperature_c",
    "selector_feature_weather_wind_speed_ms",
    "selector_feature_net_load_mw",
)
LEARNED_SCORER_FEATURE_COLUMNS_V5: Final[tuple[str, ...]] = (
    *v4.LEARNED_SCORER_FEATURE_COLUMNS_V4,
    *V5_CONTEXT_SELECTOR_FEATURE_COLUMNS,
)
CONTEXT_BLOCKERS: Final[frozenset[str]] = frozenset(
    {
        "missing_weather_load_context",
        "missing_calendar_event_context",
        "missing_publication_time",
        "context_available_not_used",
        "context_ready",
    }
)
EXTERNAL_MARKET_TOKENS: Final[tuple[str, ...]] = (
    "entsoe",
    "poland",
    "opsp",
    "opsd",
    "ember",
    "nord_pool",
    "pricefm",
    "thief",
)
REQUIRED_CONTEXT_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "feature_family",
        "blocker",
        "feature_available_timestamp",
        "available_before_anchor",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)
REQUIRED_CONTEXT_PANEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "split_name",
        *V5_CONTEXT_SELECTOR_FEATURE_COLUMNS,
        "label_best_candidate_regret_uah",
        "label_v2_plus_regret_uah",
        "label_strict_regret_uah",
        "diagnostic_context_blockers",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
        "market_execution_enabled",
    }
)


def context_enriched_candidate_value_dfl_v5_model_name(source_model_name: str) -> str:
    """Return the stable V5 model name for a source model."""

    return f"{CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_PREFIX}{source_model_name}"


def build_dfl_point_in_time_context_repair_audit_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    benchmark_context_frame: pl.DataFrame,
    plateau_autopsy_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Build exact point-in-time context blocker rows by tenant/source/anchor."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    rows: list[dict[str, Any]] = []
    autopsy_by_key = (
        {
            _tenant_source_anchor_key(row): row
            for row in plateau_autopsy_frame.iter_rows(named=True)
        }
        if plateau_autopsy_frame is not None and not plateau_autopsy_frame.is_empty()
        else {}
    )
    external_columns = _external_columns(benchmark_context_frame)
    for anchor_row in _unique_anchor_rows(schedule_candidate_library_frame):
        tenant_id = str(anchor_row["tenant_id"])
        source_model_name = str(anchor_row["source_model_name"])
        anchor_timestamp = v2._datetime_value(
            anchor_row["anchor_timestamp"],
            field_name="anchor_timestamp",
        )
        context_rows = _context_rows_before_anchor(
            benchmark_context_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor_timestamp,
        )
        for family in (
            "weather_load_context",
            "calendar_event_context",
            "publication_time_availability",
        ):
            blocker, available_at, details = _context_family_status(
                family,
                context_rows=context_rows,
                context_columns=benchmark_context_frame.columns,
                anchor_timestamp=anchor_timestamp,
            )
            rows.append(
                _context_audit_row(
                    tenant_id=tenant_id,
                    source_model_name=source_model_name,
                    anchor_timestamp=anchor_timestamp,
                    split_name=str(anchor_row["split_name"]),
                    feature_family=family,
                    blocker=blocker,
                    feature_available_timestamp=available_at,
                    details={
                        **details,
                        "external_market_columns_ignored": external_columns,
                    },
                )
            )
        autopsy_row = autopsy_by_key.get(
            (tenant_id, source_model_name, anchor_timestamp)
        )
        rows.append(
            _context_audit_row(
                tenant_id=tenant_id,
                source_model_name=source_model_name,
                anchor_timestamp=anchor_timestamp,
                split_name=str(anchor_row["split_name"]),
                feature_family="final_regret_cluster_alignment",
                blocker=(
                    "context_available_not_used"
                    if autopsy_row is not None
                    and str(autopsy_row.get("plateau_cause", "candidate_not_better"))
                    != "candidate_not_better"
                    else "context_ready"
                ),
                feature_available_timestamp=None,
                details={
                    "plateau_cause": None
                    if autopsy_row is None
                    else autopsy_row.get("plateau_cause"),
                    "analysis_only_not_selector_feature": True,
                },
            )
        )
    return pl.DataFrame(rows).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp", "feature_family"]
    )


def validate_dfl_point_in_time_context_repair_audit_evidence(
    context_repair_audit_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate point-in-time context-repair audit structure and boundaries."""

    missing_columns = sorted(
        REQUIRED_CONTEXT_AUDIT_COLUMNS.difference(context_repair_audit_frame.columns)
    )
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"point-in-time context repair audit is missing columns: {missing_columns}",
            {"row_count": context_repair_audit_frame.height},
        )
    failures = _claim_boundary_failures(
        context_repair_audit_frame,
        expected_claim_scope=POINT_IN_TIME_CONTEXT_REPAIR_CLAIM_SCOPE,
    )
    blockers = {
        str(blocker) for blocker in context_repair_audit_frame["blocker"].to_list()
    }
    unknown_blockers = sorted(blockers.difference(CONTEXT_BLOCKERS))
    if unknown_blockers:
        failures.append(f"unexpected context blockers: {unknown_blockers}")
    return EvidenceCheckOutcome(
        not failures,
        "Point-in-time context repair audit preserves claim boundaries."
        if not failures
        else "; ".join(failures),
        {
            "row_count": context_repair_audit_frame.height,
            "blocker_counts": _value_counts(
                context_repair_audit_frame, column="blocker"
            ),
            "market_execution_enabled": False,
        },
    )


def build_dfl_point_in_time_context_feature_panel_frame(
    schedule_candidate_library_frame: pl.DataFrame,
    context_repair_audit_frame: pl.DataFrame,
    benchmark_context_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build prior-only Ukrainian context features for V5 candidate selection."""

    v2._validate_library_frame(schedule_candidate_library_frame)
    _require_columns(
        context_repair_audit_frame,
        REQUIRED_CONTEXT_AUDIT_COLUMNS,
        frame_name="context_repair_audit_frame",
    )
    audit_by_key = _audit_rows_by_anchor(context_repair_audit_frame)
    rows_by_anchor = v2_plus._rows_by_tenant_source_anchor(
        schedule_candidate_library_frame
    )
    rows: list[dict[str, Any]] = []
    for key in sorted(rows_by_anchor, key=lambda item: (item[0], item[1], item[2])):
        tenant_id, source_model_name, anchor_timestamp = key
        anchor_rows = rows_by_anchor[key]
        context_rows = _context_rows_before_anchor(
            benchmark_context_frame,
            tenant_id=tenant_id,
            anchor_timestamp=anchor_timestamp,
        )
        audit_rows = audit_by_key.get(key, [])
        features = _context_selector_features(
            anchor_timestamp=anchor_timestamp,
            context_rows=context_rows,
            audit_rows=audit_rows,
        )
        labels = _anchor_labels(anchor_rows)
        rows.append(
            {
                "tenant_id": tenant_id,
                "source_model_name": source_model_name,
                "anchor_timestamp": anchor_timestamp,
                "split_name": str(anchor_rows[0]["split_name"]),
                **features,
                **labels,
                "diagnostic_context_blockers": sorted(
                    {str(row["blocker"]) for row in audit_rows}
                ),
                "diagnostic_context_feature_families": sorted(
                    {str(row["feature_family"]) for row in audit_rows}
                ),
                "diagnostic_external_market_features_used": False,
                "training_source_scope": "ukrainian_only_oree_open_meteo_tenant_context",
                "claim_scope": POINT_IN_TIME_CONTEXT_FEATURE_PANEL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
                "evaluation_payload": {
                    "selector_features_prior_only": True,
                    "labels_are_realized_scoring_outcomes": True,
                    "external_market_features_used": False,
                    "market_execution_enabled": False,
                },
            }
        )
    return pl.DataFrame(rows).sort(
        ["source_model_name", "tenant_id", "anchor_timestamp"]
    )


def validate_dfl_point_in_time_context_feature_panel_evidence(
    context_feature_panel_frame: pl.DataFrame,
) -> EvidenceCheckOutcome:
    """Validate V5 context feature-panel no-leakage and source boundaries."""

    missing_columns = sorted(
        REQUIRED_CONTEXT_PANEL_COLUMNS.difference(context_feature_panel_frame.columns)
    )
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"point-in-time context feature panel is missing columns: {missing_columns}",
            {"row_count": context_feature_panel_frame.height},
        )
    failures = _claim_boundary_failures(
        context_feature_panel_frame,
        expected_claim_scope=POINT_IN_TIME_CONTEXT_FEATURE_PANEL_CLAIM_SCOPE,
    )
    selector_columns = [
        column
        for column in context_feature_panel_frame.columns
        if column.startswith("selector_feature_")
    ]
    label_columns = [
        column
        for column in context_feature_panel_frame.columns
        if column.startswith("label_")
    ]
    external_selector_columns = _external_names(selector_columns)
    if external_selector_columns:
        failures.append(
            f"external market selector features are not allowed: {external_selector_columns}"
        )
    if not selector_columns:
        failures.append("context feature panel must expose selector_feature_* columns")
    if not label_columns:
        failures.append("context feature panel must expose label_* columns")
    if set(
        context_feature_panel_frame[
            "diagnostic_external_market_features_used"
        ].to_list()
    ) != {False}:
        failures.append("external market features must not enter V5 training")
    return EvidenceCheckOutcome(
        not failures,
        "Point-in-time context feature panel keeps prior features separate from labels."
        if not failures
        else "; ".join(failures),
        {
            "row_count": context_feature_panel_frame.height,
            "selector_feature_columns": selector_columns,
            "label_columns": label_columns,
            "market_execution_enabled": False,
        },
    )


def build_dfl_context_enriched_schedule_candidate_library_v5_frame(
    schedule_candidate_library_v4_frame: pl.DataFrame,
    point_in_time_context_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Attach V5 point-in-time selector features to the V4 candidate library."""

    v2._validate_library_frame(schedule_candidate_library_v4_frame)
    _require_columns(
        point_in_time_context_feature_panel_frame,
        REQUIRED_CONTEXT_PANEL_COLUMNS,
        frame_name="point_in_time_context_feature_panel_frame",
    )
    context_by_key = {
        _tenant_source_anchor_key(row): row
        for row in point_in_time_context_feature_panel_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for row in schedule_candidate_library_v4_frame.iter_rows(named=True):
        key = _tenant_source_anchor_key(row)
        context_row = context_by_key.get(key)
        if context_row is None:
            raise ValueError(f"missing V5 context feature row for {key}")
        copied = dict(row)
        payload = dict(v2._payload(row))
        for column in V5_CONTEXT_SELECTOR_FEATURE_COLUMNS:
            copied[column] = float(context_row[column])
        payload.update(
            {
                "claim_scope": CONTEXT_ENRICHED_CANDIDATE_LIBRARY_V5_CLAIM_SCOPE,
                "candidate_library_version": "v5_context_enriched",
                "context_feature_panel_claim_scope": context_row["claim_scope"],
                "external_market_features_used": False,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        copied["candidate_library_version"] = "v5_context_enriched"
        copied["claim_scope"] = CONTEXT_ENRICHED_CANDIDATE_LIBRARY_V5_CLAIM_SCOPE
        copied["not_full_dfl"] = True
        copied["not_market_execution"] = True
        copied["evaluation_payload"] = payload
        rows.append(copied)
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_context_enriched_candidate_value_label_panel_v5_frame(
    context_enriched_schedule_candidate_library_v5_frame: pl.DataFrame,
    point_in_time_context_feature_panel_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Build V5 prior/context selector features plus realized value labels."""

    base_labels = v4.build_dfl_candidate_value_label_panel_v4_frame(
        context_enriched_schedule_candidate_library_v5_frame
    )
    context_by_key = {
        _tenant_source_anchor_key(row): row
        for row in point_in_time_context_feature_panel_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for row in base_labels.iter_rows(named=True):
        key = _tenant_source_anchor_key(row)
        context_row = context_by_key.get(key)
        if context_row is None:
            raise ValueError(f"missing V5 context features for label row {key}")
        copied = dict(row)
        payload = dict(v2._payload(row))
        for column in V5_CONTEXT_SELECTOR_FEATURE_COLUMNS:
            copied[column] = float(context_row[column])
        copied["claim_scope"] = CONTEXT_ENRICHED_LABEL_PANEL_V5_CLAIM_SCOPE
        copied["not_full_dfl"] = True
        copied["not_market_execution"] = True
        copied["market_execution_enabled"] = False
        payload.update(
            {
                "claim_scope": CONTEXT_ENRICHED_LABEL_PANEL_V5_CLAIM_SCOPE,
                "context_features_prior_only": True,
                "external_market_features_used": False,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
        copied["evaluation_payload"] = payload
        rows.append(copied)
    return pl.DataFrame(rows).sort(
        [
            "tenant_id",
            "source_model_name",
            "anchor_timestamp",
            "candidate_family",
            "candidate_model_name",
        ]
    )


def build_dfl_context_enriched_candidate_value_dfl_v5_frame(
    context_enriched_schedule_candidate_library_v5_frame: pl.DataFrame,
    learner_v2_plus_frame: pl.DataFrame,
    context_enriched_candidate_value_label_panel_v5_frame: pl.DataFrame | None = None,
    *,
    tenant_ids: tuple[str, ...],
    forecast_model_names: tuple[str, ...],
    final_validation_anchor_count_per_tenant: int = 18,
    min_prior_mean_improvement_ratio_vs_v2_plus: float = 0.01,
    ridge_l2: float = 1.0,
) -> pl.DataFrame:
    """Train/select a context-enriched candidate-level value scorer with V2+ fallback."""

    v3._validate_config(
        tenant_ids=tenant_ids,
        forecast_model_names=forecast_model_names,
        final_validation_anchor_count_per_tenant=final_validation_anchor_count_per_tenant,
        min_prior_mean_improvement_ratio_vs_v2_plus=(
            min_prior_mean_improvement_ratio_vs_v2_plus
        ),
    )
    v2._validate_library_frame(context_enriched_schedule_candidate_library_v5_frame)
    v3._validate_v2_plus_model_frame(learner_v2_plus_frame)
    label_panel = (
        context_enriched_candidate_value_label_panel_v5_frame
        if context_enriched_candidate_value_label_panel_v5_frame is not None
        else build_dfl_context_enriched_candidate_value_label_panel_v5_frame(
            context_enriched_schedule_candidate_library_v5_frame,
            _context_panel_from_library(
                context_enriched_schedule_candidate_library_v5_frame
            ),
        )
    )
    _validate_label_panel_frame_v5(label_panel)
    label_rows_by_key = _label_rows_by_key(label_panel)
    v2_plus_rows = {
        (str(row["tenant_id"]), str(row["source_model_name"])): row
        for row in learner_v2_plus_frame.iter_rows(named=True)
    }
    rows: list[dict[str, Any]] = []
    for tenant_id in tenant_ids:
        for source_model_name in forecast_model_names:
            source_rows = v2._library_rows(
                context_enriched_schedule_candidate_library_v5_frame,
                tenant_id=tenant_id,
                source_model_name=source_model_name,
            )
            train_rows = [
                row
                for row in source_rows
                if str(row["split_name"]) == "train_selection"
            ]
            final_rows = [
                row for row in source_rows if str(row["split_name"]) == "final_holdout"
            ]
            final_anchor_count = len(v2._anchor_set(final_rows))
            if final_anchor_count != final_validation_anchor_count_per_tenant:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} final-holdout tenant-anchor "
                    f"count must be {final_validation_anchor_count_per_tenant}; "
                    f"observed {final_anchor_count}"
                )
            if not train_rows:
                raise ValueError(
                    f"{tenant_id}/{source_model_name} candidate-value DFL v5 needs train rows"
                )
            v2_plus_row = v2_plus_rows.get((tenant_id, source_model_name))
            if v2_plus_row is None:
                raise ValueError(
                    f"missing V2+ learner row for {tenant_id}/{source_model_name}"
                )
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
            learned_scorer = _fit_learned_candidate_value_scorer_v5(
                train_label_rows,
                candidate_families=eligible_families,
                ridge_l2=ridge_l2,
            )
            selected_train_rows = _select_rows_by_learned_scorer_v5(
                train_rows,
                label_rows_by_key=label_rows_by_key,
                scorer=learned_scorer,
                candidate_families=eligible_families,
            )
            selected_final_rows = _select_rows_by_learned_scorer_v5(
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
            raw_final_rows = v2._selected_family_rows(
                final_rows, v2.CANDIDATE_FAMILY_RAW
            )
            rows.append(
                {
                    "tenant_id": tenant_id,
                    "source_model_name": source_model_name,
                    "learner_model_name": context_enriched_candidate_value_dfl_v5_model_name(
                        source_model_name
                    ),
                    "selected_value_profile_name": str(learned_scorer["name"]),
                    "selected_scorer_type": LEARNED_SCORER_TYPE_V5,
                    "selected_objective_name": (
                        "candidate_value_v5_context_feature_ridge_pairwise_ranking"
                    ),
                    "selected_feature_names": list(LEARNED_SCORER_FEATURE_COLUMNS_V5),
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
                    "candidate_train_pairwise_loss_uah": _pairwise_regret_weighted_loss_v5(
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
                    "claim_scope": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_CLAIM_SCOPE,
                    "academic_scope": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_ACADEMIC_SCOPE,
                    "not_full_dfl": True,
                    "not_market_execution": True,
                }
            )
    return pl.DataFrame(rows).sort(["source_model_name", "tenant_id"])


def build_dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame(
    context_enriched_schedule_candidate_library_v5_frame: pl.DataFrame,
    context_enriched_candidate_value_dfl_v5_frame: pl.DataFrame,
    v2_plus_strict_frame: pl.DataFrame,
    *,
    generated_at: datetime | None = None,
) -> pl.DataFrame:
    """Emit strict/raw/V2+/context-enriched V5 rows for the strict gate."""

    v2._validate_library_frame(context_enriched_schedule_candidate_library_v5_frame)
    _validate_candidate_value_model_frame_v5(
        context_enriched_candidate_value_dfl_v5_frame
    )
    resolved_generated_at = generated_at or v2._latest_generated_at(
        v2_plus_strict_frame
    )
    library_rows = list(
        context_enriched_schedule_candidate_library_v5_frame.iter_rows(named=True)
    )
    rows: list[dict[str, Any]] = []
    for learner_row in context_enriched_candidate_value_dfl_v5_frame.iter_rows(
        named=True
    ):
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
        candidate_rows = _select_rows_by_model_row_v5(
            final_rows,
            learner_row=learner_row,
            candidate_families=frozenset(
                str(family) for family in learner_row["eligible_candidate_families"]
            ),
        )
        candidate_by_anchor = {
            v2._datetime_value(
                row["anchor_timestamp"], field_name="anchor_timestamp"
            ): row
            for row in candidate_rows
        }
        for anchor_timestamp in final_anchors:
            anchor_rows = [
                row
                for row in final_rows
                if v2._datetime_value(
                    row["anchor_timestamp"], field_name="anchor_timestamp"
                )
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
                    _strict_benchmark_row_v5(
                        strict_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="strict_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row_v5(
                        raw_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="raw_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row_v5(
                        v2_plus_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="schedule_value_learner_v2_plus_reference",
                        generated_at=resolved_generated_at,
                    ),
                    _strict_benchmark_row_v5(
                        selected_row,
                        source_model_name=source_model_name,
                        learner_row=learner_row,
                        role="context_enriched_candidate_value_dfl_v5",
                        generated_at=resolved_generated_at,
                    ),
                ]
            )
    return pl.DataFrame(rows).sort(
        ["tenant_id", "source_model_name", "anchor_timestamp", "selection_role"]
    )


def validate_dfl_context_enriched_candidate_value_dfl_v5_evidence(
    strict_frame: pl.DataFrame,
    *,
    source_model_names: tuple[str, ...] | None = None,
    min_tenant_count: int = 5,
    min_validation_tenant_anchor_count: int = DEFAULT_MIN_ANCHOR_COUNT,
) -> EvidenceCheckOutcome:
    """Validate structural V5 evidence without requiring headline replacement."""

    missing_columns = sorted(
        v3.REQUIRED_STRICT_COLUMNS.difference(strict_frame.columns)
    )
    if missing_columns:
        return EvidenceCheckOutcome(
            False,
            f"context-enriched DFL v5 evidence is missing required columns: {missing_columns}",
            {"row_count": strict_frame.height},
        )
    failures = _claim_boundary_failures(
        strict_frame,
        expected_claim_scope=CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_CLAIM_SCOPE,
    )
    roles = (
        set(strict_frame["selection_role"].to_list()) if strict_frame.height else set()
    )
    if "context_enriched_candidate_value_dfl_v5" not in roles:
        failures.append("V5 strict evidence must include the V5 selection role")
    adapted = _strict_frame_for_v4_gate(strict_frame)
    base = v4.validate_dfl_candidate_value_dfl_v4_evidence(
        adapted,
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
    )
    if not base.passed:
        failures.append(base.description)
    return EvidenceCheckOutcome(
        not failures,
        "Context-enriched candidate-value DFL v5 evidence has valid coverage and boundaries."
        if not failures
        else "; ".join(failures),
        {
            **base.metadata,
            "row_count": strict_frame.height,
            "strategy_kind": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND,
            "market_execution_enabled": False,
        },
    )


def evaluate_dfl_context_enriched_candidate_value_dfl_v5_gate(
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
    """Evaluate V5 against strict and frozen V2+ using unchanged gate semantics."""

    base_gate = v4.evaluate_dfl_candidate_value_dfl_v4_gate(
        _strict_frame_for_v4_gate(strict_frame),
        source_model_names=source_model_names,
        min_tenant_count=min_tenant_count,
        min_validation_tenant_anchor_count=min_validation_tenant_anchor_count,
        min_mean_regret_improvement_ratio_vs_v2_plus=(
            min_mean_regret_improvement_ratio_vs_v2_plus
        ),
        min_mean_regret_improvement_ratio_vs_strict=(
            min_mean_regret_improvement_ratio_vs_strict
        ),
    )
    metrics = {
        **base_gate.metrics,
        "strategy_kind": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND,
        "market_execution_enabled": False,
    }
    if base_gate.passed:
        return PromotionGateResult(
            True,
            "replace_v2_plus",
            "context-enriched candidate-value DFL v5 passes strict LP/oracle and frozen V2+ gate",
            metrics,
        )
    return PromotionGateResult(
        False,
        base_gate.decision,
        base_gate.description.replace(
            "candidate-value DFL v4", "context-enriched DFL v5"
        ),
        metrics,
    )


def _context_audit_row(
    *,
    tenant_id: str,
    source_model_name: str,
    anchor_timestamp: datetime,
    split_name: str,
    feature_family: str,
    blocker: str,
    feature_available_timestamp: datetime | None,
    details: dict[str, Any],
) -> dict[str, Any]:
    available_before_anchor = (
        feature_available_timestamp is not None
        and feature_available_timestamp < anchor_timestamp
    )
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "anchor_timestamp": anchor_timestamp,
        "split_name": split_name,
        "feature_family": feature_family,
        "blocker": blocker,
        "feature_available_timestamp": feature_available_timestamp,
        "available_before_anchor": available_before_anchor,
        "details": details,
        "claim_scope": POINT_IN_TIME_CONTEXT_REPAIR_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _context_family_status(
    family: str,
    *,
    context_rows: list[dict[str, Any]],
    context_columns: list[str],
    anchor_timestamp: datetime,
) -> tuple[str, datetime | None, dict[str, Any]]:
    if family == "weather_load_context":
        weather_columns = _columns_with_prefix(context_columns, "weather_")
        load_columns = _columns_with_substring(context_columns, "load")
        ready = bool(context_rows and weather_columns and load_columns)
        return (
            "context_ready" if ready else "missing_weather_load_context",
            _latest_context_timestamp(context_rows) if ready else None,
            {"weather_columns": weather_columns, "load_columns": load_columns},
        )
    if family == "calendar_event_context":
        columns = [
            column
            for column in context_columns
            if any(
                token in column.lower()
                for token in ("holiday", "calendar", "event", "outage", "grid")
            )
        ]
        ready = bool(context_rows and columns)
        return (
            "context_ready" if ready else "missing_calendar_event_context",
            _latest_context_timestamp(context_rows) if ready else None,
            {"context_columns": columns},
        )
    columns = [
        column
        for column in context_columns
        if any(
            token in column.lower()
            for token in ("publication", "source_timestamp", "known_future")
        )
    ]
    available_times = [
        value
        for row in context_rows
        for column in columns
        if (value := _optional_datetime(row.get(column))) is not None
        and value < anchor_timestamp
    ]
    ready = bool(available_times)
    return (
        "context_ready" if ready else "missing_publication_time",
        max(available_times) if available_times else None,
        {"publication_columns": columns},
    )


def _context_selector_features(
    *,
    anchor_timestamp: datetime,
    context_rows: list[dict[str, Any]],
    audit_rows: list[dict[str, Any]],
) -> dict[str, float]:
    blockers = {str(row["blocker"]) for row in audit_rows}
    latest = context_rows[-1] if context_rows else {}
    return {
        "selector_feature_weather_load_context_ready": float(
            "missing_weather_load_context" not in blockers
        ),
        "selector_feature_calendar_event_context_ready": float(
            "missing_calendar_event_context" not in blockers
        ),
        "selector_feature_publication_time_ready": float(
            "missing_publication_time" not in blockers
        ),
        "selector_feature_context_blocker_count": float(
            sum(1 for blocker in blockers if blocker.startswith("missing_"))
        ),
        "selector_feature_anchor_hour": float(anchor_timestamp.hour),
        "selector_feature_anchor_day_of_week": float(anchor_timestamp.weekday()),
        "selector_feature_anchor_is_weekend": float(anchor_timestamp.weekday() >= 5),
        "selector_feature_weather_temperature_c": _first_numeric_value(
            latest,
            prefixes=("weather_temperature", "weather_temp", "temperature"),
        ),
        "selector_feature_weather_wind_speed_ms": _first_numeric_value(
            latest,
            prefixes=("weather_wind_speed", "wind_speed", "weather_wind"),
        ),
        "selector_feature_net_load_mw": _first_numeric_value(
            latest,
            contains=("net_load", "configured_load", "load_mw", "load"),
        ),
    }


def _anchor_labels(anchor_rows: list[dict[str, Any]]) -> dict[str, float]:
    best_regret = min(float(row["regret_uah"]) for row in anchor_rows)
    strict_regret = _family_regret(anchor_rows, v2.CANDIDATE_FAMILY_STRICT)
    raw_regret = _family_regret(anchor_rows, v2.CANDIDATE_FAMILY_RAW)
    v2_plus_rows = [
        row
        for row in anchor_rows
        if "v2_plus" in str(row["candidate_family"])
        or "v2_plus" in str(row["candidate_model_name"])
    ]
    v2_plus_regret = (
        min(float(row["regret_uah"]) for row in v2_plus_rows)
        if v2_plus_rows
        else best_regret
    )
    return {
        "label_best_candidate_regret_uah": best_regret,
        "label_v2_plus_regret_uah": v2_plus_regret,
        "label_strict_regret_uah": strict_regret,
        "label_raw_regret_uah": raw_regret,
        "label_v2_plus_margin_to_best_uah": v2_plus_regret - best_regret,
    }


def _family_regret(anchor_rows: list[dict[str, Any]], family: str) -> float:
    try:
        row = v2._single_family_row(anchor_rows, family)
    except ValueError:
        return min(float(row["regret_uah"]) for row in anchor_rows)
    return float(row["regret_uah"])


def _fit_learned_candidate_value_scorer_v5(
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
        raise ValueError(
            "context-enriched DFL v5 learned scorer needs train label rows"
        )
    feature_means: dict[str, float] = {}
    feature_scales: dict[str, float] = {}
    for column in LEARNED_SCORER_FEATURE_COLUMNS_V5:
        values = [float(row[column]) for row in eligible_rows]
        feature_means[column] = mean(values)
        span = max(values) - min(values)
        feature_scales[column] = span if span > 1e-9 else 1.0
    family_columns = tuple(f"family::{family}" for family in sorted(candidate_families))
    feature_matrix = [
        _learned_feature_vector_v5(
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
    feature_names = [*LEARNED_SCORER_FEATURE_COLUMNS_V5, *family_columns]
    weights = {"intercept": coefficients[0]}
    weights.update(
        {
            feature_name: coefficients[index + 1]
            for index, feature_name in enumerate(feature_names)
        }
    )
    return {
        "name": LEARNED_SCORER_PROFILE_NAME_V5,
        "scorer_type": LEARNED_SCORER_TYPE_V5,
        "weights": weights,
        "feature_means": feature_means,
        "feature_scales": feature_scales,
        "family_columns": family_columns,
    }


def _selector_feature_values_v5(row: dict[str, Any]) -> dict[str, float]:
    base = v4._selector_feature_values_v4(row)
    return {
        **base,
        **{
            column: float(row.get(column, 0.0))
            for column in V5_CONTEXT_SELECTOR_FEATURE_COLUMNS
        },
    }


def _learned_feature_vector_v5(
    row: dict[str, Any],
    *,
    feature_means: dict[str, float],
    feature_scales: dict[str, float],
    family_columns: tuple[str, ...],
) -> list[float]:
    features = _selector_feature_values_v5(row)
    numeric = [
        (features[column] - feature_means[column]) / feature_scales[column]
        for column in LEARNED_SCORER_FEATURE_COLUMNS_V5
    ]
    family = str(row["candidate_family"])
    one_hot = [
        1.0 if column == f"family::{family}" else 0.0 for column in family_columns
    ]
    return [*numeric, *one_hot]


def _select_rows_by_learned_scorer_v5(
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
                    _predict_learned_candidate_regret_v5(
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


def _select_rows_by_model_row_v5(
    rows: list[dict[str, Any]],
    *,
    learner_row: dict[str, Any],
    candidate_families: frozenset[str],
) -> list[dict[str, Any]]:
    scorer = _learned_scorer_from_model_row_v5(learner_row)
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
                    _predict_learned_candidate_regret_v5(row, scorer=scorer),
                    v2._family_sort_index(str(row["candidate_family"])),
                    str(row["candidate_model_name"]),
                ),
            )
        )
    return selected_rows


def _learned_scorer_from_model_row_v5(row: dict[str, Any]) -> dict[str, Any]:
    family_columns = tuple(
        sorted(
            key
            for key in dict(row["selected_feature_weights"])
            if key.startswith("family::")
        )
    )
    return {
        "name": str(row["selected_value_profile_name"]),
        "scorer_type": LEARNED_SCORER_TYPE_V5,
        "weights": dict(row["selected_feature_weights"]),
        "feature_means": dict(row.get("selected_feature_means", {})),
        "feature_scales": dict(row.get("selected_feature_scales", {})),
        "family_columns": family_columns,
    }


def _predict_learned_candidate_regret_v5(
    row: dict[str, Any],
    *,
    scorer: dict[str, Any],
) -> float:
    weights = dict(scorer["weights"])
    family_columns = tuple(str(column) for column in scorer["family_columns"])
    features = _learned_feature_vector_v5(
        row,
        feature_means=dict(scorer["feature_means"]),
        feature_scales=dict(scorer["feature_scales"]),
        family_columns=family_columns,
    )
    feature_names = [*LEARNED_SCORER_FEATURE_COLUMNS_V5, *family_columns]
    score = float(weights.get("intercept", 0.0))
    for feature_name, feature_value in zip(feature_names, features, strict=True):
        score += float(weights.get(feature_name, 0.0)) * feature_value
    return score


def _pairwise_regret_weighted_loss_v5(
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
        anchor = v2._datetime_value(
            row["anchor_timestamp"], field_name="anchor_timestamp"
        )
        rows_by_anchor.setdefault(anchor, []).append(row)
    for anchor_rows in rows_by_anchor.values():
        for left_index, left in enumerate(anchor_rows):
            for right in anchor_rows[left_index + 1 :]:
                left_regret = float(left["label_regret_uah"])
                right_regret = float(right["label_regret_uah"])
                if abs(left_regret - right_regret) <= 1e-9:
                    continue
                better, worse = (
                    (left, right) if left_regret < right_regret else (right, left)
                )
                if _predict_learned_candidate_regret_v5(
                    better,
                    scorer=scorer,
                ) > _predict_learned_candidate_regret_v5(worse, scorer=scorer):
                    losses.append(abs(left_regret - right_regret))
                else:
                    losses.append(0.0)
    return mean(losses) if losses else 0.0


def _strict_benchmark_row_v5(
    row: dict[str, Any],
    *,
    source_model_name: str,
    learner_row: dict[str, Any],
    role: str,
    generated_at: datetime,
) -> dict[str, Any]:
    v4_role = (
        "candidate_value_dfl_v4"
        if role == "context_enriched_candidate_value_dfl_v5"
        else role
    )
    v4_row = dict(
        v4._strict_benchmark_row_v4(
            row,
            source_model_name=source_model_name,
            learner_row=_v5_model_row_as_v4(learner_row),
            role=v4_role,
            generated_at=generated_at,
        )
    )
    payload = dict(v4_row["evaluation_payload"])
    payload.update(
        {
            "strict_gate_kind": "context_enriched_candidate_value_dfl_v5_strict_lp",
            "learner_model_name": context_enriched_candidate_value_dfl_v5_model_name(
                source_model_name
            ),
            "selected_feature_weights": dict(learner_row["selected_feature_weights"]),
            "selection_role": role,
            "claim_scope": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_CLAIM_SCOPE,
            "academic_scope": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_ACADEMIC_SCOPE,
            "external_market_features_used": False,
        }
    )
    v4_row.update(
        {
            "evaluation_id": str(v4_row["evaluation_id"]).replace(
                "candidate-value-dfl-v4",
                "context-enriched-candidate-value-dfl-v5",
            ),
            "strategy_kind": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND,
            "selection_role": role,
            "claim_scope": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_CLAIM_SCOPE,
            "academic_scope": CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_ACADEMIC_SCOPE,
            "evaluation_payload": payload,
        }
    )
    if role == "context_enriched_candidate_value_dfl_v5":
        v4_row["forecast_model_name"] = (
            context_enriched_candidate_value_dfl_v5_model_name(source_model_name)
        )
    return v4_row


def _v5_model_row_as_v4(row: dict[str, Any]) -> dict[str, Any]:
    copied = dict(row)
    copied["learner_model_name"] = v4.candidate_value_dfl_v4_model_name(
        str(row["source_model_name"])
    )
    copied["selected_scorer_type"] = v4.LEARNED_SCORER_TYPE_V4
    copied["claim_scope"] = v4.CANDIDATE_VALUE_DFL_V4_CLAIM_SCOPE
    copied["academic_scope"] = v4.CANDIDATE_VALUE_DFL_V4_ACADEMIC_SCOPE
    return copied


def _strict_frame_for_v4_gate(strict_frame: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for row in strict_frame.iter_rows(named=True):
        copied = dict(row)
        role = str(copied["selection_role"])
        if role == "context_enriched_candidate_value_dfl_v5":
            copied["selection_role"] = "candidate_value_dfl_v4"
        copied["strategy_kind"] = v4.CANDIDATE_VALUE_DFL_V4_STRICT_LP_STRATEGY_KIND
        copied["claim_scope"] = v4.CANDIDATE_VALUE_DFL_V4_STRICT_CLAIM_SCOPE
        payload = dict(v2._payload(row))
        payload["claim_scope"] = v4.CANDIDATE_VALUE_DFL_V4_STRICT_CLAIM_SCOPE
        payload["selection_role"] = copied["selection_role"]
        copied["evaluation_payload"] = payload
        rows.append(copied)
    return pl.DataFrame(rows)


def _context_panel_from_library(library: pl.DataFrame) -> pl.DataFrame:
    rows: list[dict[str, Any]] = []
    for anchor_row in _unique_anchor_rows(library):
        rows.append(
            {
                "tenant_id": anchor_row["tenant_id"],
                "source_model_name": anchor_row["source_model_name"],
                "anchor_timestamp": anchor_row["anchor_timestamp"],
                "split_name": anchor_row["split_name"],
                **{
                    column: float(anchor_row.get(column, 0.0))
                    for column in V5_CONTEXT_SELECTOR_FEATURE_COLUMNS
                },
                "label_best_candidate_regret_uah": 0.0,
                "label_v2_plus_regret_uah": 0.0,
                "label_strict_regret_uah": 0.0,
                "diagnostic_context_blockers": [],
                "claim_scope": POINT_IN_TIME_CONTEXT_FEATURE_PANEL_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows)


def _label_rows_by_key(
    frame: pl.DataFrame,
) -> dict[tuple[str, str, datetime, str, str], dict[str, Any]]:
    return {_candidate_identity_key(row): row for row in frame.iter_rows(named=True)}


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


def _label_row_or_candidate_row(
    row: dict[str, Any],
    *,
    label_rows_by_key: dict[tuple[str, str, datetime, str, str], dict[str, Any]],
) -> dict[str, Any]:
    return label_rows_by_key.get(_candidate_identity_key(row), row)


def _candidate_identity_key(
    row: dict[str, Any],
) -> tuple[str, str, datetime, str, str]:
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


def _unique_anchor_rows(frame: pl.DataFrame) -> list[dict[str, Any]]:
    seen: set[tuple[str, str, datetime]] = set()
    rows: list[dict[str, Any]] = []
    for row in frame.sort(
        ["source_model_name", "tenant_id", "anchor_timestamp"]
    ).iter_rows(named=True):
        key = _tenant_source_anchor_key(row)
        if key in seen:
            continue
        seen.add(key)
        rows.append(row)
    return rows


def _audit_rows_by_anchor(
    context_repair_audit_frame: pl.DataFrame,
) -> dict[tuple[str, str, datetime], list[dict[str, Any]]]:
    grouped: dict[tuple[str, str, datetime], list[dict[str, Any]]] = {}
    for row in context_repair_audit_frame.iter_rows(named=True):
        grouped.setdefault(_tenant_source_anchor_key(row), []).append(row)
    return grouped


def _context_rows_before_anchor(
    frame: pl.DataFrame,
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
) -> list[dict[str, Any]]:
    if frame.is_empty() or "timestamp" not in frame.columns:
        return []
    rows: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        if "tenant_id" in row and str(row["tenant_id"]) != tenant_id:
            continue
        timestamp = _optional_datetime(row.get("timestamp"))
        if timestamp is None or timestamp >= anchor_timestamp:
            continue
        rows.append(row)
    return sorted(
        rows,
        key=lambda item: _optional_datetime(item.get("timestamp")) or datetime.min,
    )


def _latest_context_timestamp(rows: list[dict[str, Any]]) -> datetime | None:
    timestamps = [
        timestamp
        for row in rows
        if (timestamp := _optional_datetime(row.get("timestamp"))) is not None
    ]
    return max(timestamps) if timestamps else None


def _optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def _first_numeric_value(
    row: dict[str, Any],
    *,
    prefixes: tuple[str, ...] = (),
    contains: tuple[str, ...] = (),
) -> float:
    for key, value in row.items():
        lowered = key.lower()
        if prefixes and not any(lowered.startswith(prefix) for prefix in prefixes):
            continue
        if contains and not any(token in lowered for token in contains):
            continue
        if isinstance(value, bool):
            return float(value)
        if isinstance(value, int | float):
            return float(value)
    return 0.0


def _columns_with_prefix(columns: list[str], prefix: str) -> list[str]:
    return sorted(column for column in columns if column.startswith(prefix))


def _columns_with_substring(columns: list[str], token: str) -> list[str]:
    lowered = token.lower()
    return sorted(column for column in columns if lowered in column.lower())


def _external_columns(frame: pl.DataFrame) -> list[str]:
    return _external_names(frame.columns)


def _external_names(names: list[str]) -> list[str]:
    return sorted(
        name
        for name in names
        if any(token in name.lower() for token in EXTERNAL_MARKET_TOKENS)
    )


def _require_columns(
    frame: pl.DataFrame,
    required_columns: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing_columns = sorted(required_columns.difference(frame.columns))
    if missing_columns:
        raise ValueError(f"{frame_name} is missing columns: {missing_columns}")


def _validate_label_panel_frame_v5(frame: pl.DataFrame) -> None:
    required = frozenset(
        {
            *v4.REQUIRED_LABEL_PANEL_COLUMNS_V4,
            *V5_CONTEXT_SELECTOR_FEATURE_COLUMNS,
        }
    )
    _require_columns(frame, required, frame_name="candidate_value_label_panel_v5_frame")
    for row in frame.iter_rows(named=True):
        if str(row["claim_scope"]) != CONTEXT_ENRICHED_LABEL_PANEL_V5_CLAIM_SCOPE:
            raise ValueError(
                "candidate-value DFL v5 label panel has unexpected claim_scope"
            )
        if not bool(row["not_full_dfl"]):
            raise ValueError(
                "candidate-value DFL v5 label rows must keep not_full_dfl=true"
            )
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "candidate-value DFL v5 label rows must keep not_market_execution=true"
            )


def _validate_candidate_value_model_frame_v5(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        v3.REQUIRED_MODEL_COLUMNS,
        frame_name="candidate_value_dfl_v5_frame",
    )
    for row in frame.iter_rows(named=True):
        if (
            str(row["claim_scope"])
            != CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_CLAIM_SCOPE
        ):
            raise ValueError("candidate-value DFL v5 frame has unexpected claim_scope")
        if not bool(row["not_full_dfl"]):
            raise ValueError("candidate-value DFL v5 rows must keep not_full_dfl=true")
        if not bool(row["not_market_execution"]):
            raise ValueError(
                "candidate-value DFL v5 rows must keep not_market_execution=true"
            )


def _claim_boundary_failures(
    frame: pl.DataFrame,
    *,
    expected_claim_scope: str,
) -> list[str]:
    failures: list[str] = []
    if frame.is_empty():
        return ["frame must not be empty"]
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


def _value_counts(frame: pl.DataFrame, *, column: str) -> dict[str, int]:
    if frame.is_empty() or column not in frame.columns:
        return {}
    counts: dict[str, int] = {}
    for value in frame[column].to_list():
        counts[str(value)] = counts.get(str(value), 0) + 1
    return dict(sorted(counts.items()))


__all__ = [
    "CONTEXT_ENRICHED_CANDIDATE_VALUE_DFL_V5_STRICT_LP_STRATEGY_KIND",
    "V5_CONTEXT_SELECTOR_FEATURE_COLUMNS",
    "build_dfl_context_enriched_candidate_value_dfl_v5_frame",
    "build_dfl_context_enriched_candidate_value_dfl_v5_strict_lp_benchmark_frame",
    "build_dfl_context_enriched_candidate_value_label_panel_v5_frame",
    "build_dfl_context_enriched_schedule_candidate_library_v5_frame",
    "build_dfl_point_in_time_context_feature_panel_frame",
    "build_dfl_point_in_time_context_repair_audit_frame",
    "context_enriched_candidate_value_dfl_v5_model_name",
    "evaluate_dfl_context_enriched_candidate_value_dfl_v5_gate",
    "validate_dfl_context_enriched_candidate_value_dfl_v5_evidence",
    "validate_dfl_point_in_time_context_feature_panel_evidence",
    "validate_dfl_point_in_time_context_repair_audit_evidence",
]
