"""Semantic grid-event audit for strict-control failure evidence."""

from __future__ import annotations

from datetime import UTC, datetime
from statistics import mean
from typing import Any, Final

import polars as pl

from smart_arbitrage.evidence.quality_checks import EvidenceCheckOutcome
from smart_arbitrage.forecasting.afe import (
    REQUIRED_AFE_FEATURE_CATALOG_COLUMNS,
    validate_forecast_afe_feature_catalog_evidence,
)
from smart_arbitrage.forecasting.grid_event_signals import GRID_EVENT_FEATURE_COLUMNS

DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_CLAIM_SCOPE: Final[str] = (
    "dfl_semantic_event_strict_failure_audit_not_full_dfl"
)
DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_ACADEMIC_SCOPE: Final[str] = (
    "Semantic AFE audit over official Ukrenergo grid-event features and strict-failure "
    "selector outcomes. This is explanatory research evidence only, not full DFL, "
    "not Decision Transformer control, and not market execution."
)

REQUIRED_SEMANTIC_STRICT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "forecast_model_name",
        "anchor_timestamp",
        "regret_uah",
        "evaluation_payload",
    }
)
REQUIRED_GRID_EVENT_SIGNAL_COLUMNS: Final[frozenset[str]] = frozenset(
    {"tenant_id", "timestamp", *GRID_EVENT_FEATURE_COLUMNS}
)
REQUIRED_SEMANTIC_AUDIT_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "window_index",
        "validation_anchor_count",
        "event_anchor_count",
        "strict_failure_count",
        "strict_failure_with_event_count",
        "semantic_source_name",
        "semantic_source_kind",
        "event_signal_coverage_rate",
        "selector_gain_with_events_uah",
        "selector_gain_without_events_uah",
        "min_event_source_freshness_hours",
        "claim_scope",
        "not_full_dfl",
        "not_market_execution",
    }
)

_EVENT_ACTIVITY_COLUMNS: Final[tuple[str, ...]] = (
    "grid_event_count_24h",
    "tenant_region_affected",
    "national_grid_risk_score",
    "outage_flag",
    "saving_request_flag",
    "solar_shift_hint",
)


def build_dfl_semantic_event_strict_failure_audit_frame(
    strict_selector_frame: pl.DataFrame,
    grid_event_signal_frame: pl.DataFrame,
    afe_feature_catalog_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Summarize whether official grid-event features explain strict-control failures."""

    _validate_afe_catalog(afe_feature_catalog_frame)
    _validate_strict_selector_frame(strict_selector_frame)
    _validate_grid_event_signal_frame(grid_event_signal_frame)

    strict_rows = list(strict_selector_frame.iter_rows(named=True))
    event_rows = _event_rows_by_key(grid_event_signal_frame)
    pairs_by_group: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for source_model_name in sorted({_source_model_name(row) for row in strict_rows}):
        source_rows = [row for row in strict_rows if _source_model_name(row) == source_model_name]
        tenants = sorted({str(row["tenant_id"]) for row in source_rows})
        for tenant_id in tenants:
            tenant_rows = [row for row in source_rows if str(row["tenant_id"]) == tenant_id]
            window_indices = sorted({_window_index(row) for row in tenant_rows})
            for window_index in window_indices:
                window_rows = [row for row in tenant_rows if _window_index(row) == window_index]
                anchor_values = sorted(
                    {
                        _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                        for row in window_rows
                    }
                )
                for anchor_timestamp in anchor_values:
                    anchor_rows = [
                        row
                        for row in window_rows
                        if _datetime_value(row["anchor_timestamp"], field_name="anchor_timestamp")
                        == anchor_timestamp
                    ]
                    strict_row = _single_role_row(
                        anchor_rows,
                        role="strict_reference",
                        forecast_model_name="strict_similar_day",
                    )
                    selector_row = _single_role_row(anchor_rows, role="selector")
                    event_row = _event_row(
                        event_rows,
                        tenant_id=tenant_id,
                        anchor_timestamp=anchor_timestamp,
                    )
                    pairs_by_group.setdefault((tenant_id, source_model_name, window_index), []).append(
                        _anchor_pair(
                            tenant_id=tenant_id,
                            source_model_name=source_model_name,
                            window_index=window_index,
                            anchor_timestamp=anchor_timestamp,
                            strict_row=strict_row,
                            selector_row=selector_row,
                            event_row=event_row,
                        )
                    )

    rows = [
        _audit_row(tenant_id, source_model_name, window_index, pairs)
        for (tenant_id, source_model_name, window_index), pairs in sorted(pairs_by_group.items())
    ]
    if not rows:
        return pl.DataFrame(
            schema={column_name: pl.Null for column_name in REQUIRED_SEMANTIC_AUDIT_COLUMNS}
        )
    return pl.DataFrame(rows).sort(["source_model_name", "window_index", "tenant_id"])


def validate_dfl_semantic_event_strict_failure_audit_evidence(
    audit_frame: pl.DataFrame,
    *,
    min_tenant_count: int = 5,
    min_source_model_count: int = 2,
    min_validation_anchor_count: int = 18,
) -> EvidenceCheckOutcome:
    """Validate semantic grid-event strict-failure audit evidence."""

    failures = _missing_column_failures(audit_frame, REQUIRED_SEMANTIC_AUDIT_COLUMNS)
    if failures:
        return EvidenceCheckOutcome(False, "; ".join(failures), {"row_count": audit_frame.height})
    rows = list(audit_frame.iter_rows(named=True))
    if not rows:
        return EvidenceCheckOutcome(False, "semantic event failure audit has no rows", {"row_count": 0})

    tenant_ids = sorted({str(row["tenant_id"]) for row in rows})
    source_model_names = sorted({str(row["source_model_name"]) for row in rows})
    bad_anchor_rows = [
        row for row in rows if int(row["validation_anchor_count"]) < min_validation_anchor_count
    ]
    bad_source_rows = [
        row
        for row in rows
        if str(row["semantic_source_name"]) != "UKRENERGO_TELEGRAM"
        or str(row["semantic_source_kind"]) != "official_telegram"
    ]
    claim_flag_failure_rows = [
        row
        for row in rows
        if str(row["claim_scope"]) != DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_CLAIM_SCOPE
        or not bool(row["not_full_dfl"])
        or not bool(row["not_market_execution"])
    ]
    future_leakage_rows = [
        row for row in rows if float(row["min_event_source_freshness_hours"]) < 0.0
    ]
    if len(tenant_ids) < min_tenant_count:
        failures.append(f"tenant_count must be at least {min_tenant_count}; observed {len(tenant_ids)}")
    if len(source_model_names) < min_source_model_count:
        failures.append(
            f"source_model_count must be at least {min_source_model_count}; observed {len(source_model_names)}"
        )
    if bad_anchor_rows:
        failures.append(
            f"validation_anchor_count must be at least {min_validation_anchor_count} for each audit row"
        )
    if bad_source_rows:
        failures.append("semantic source must remain official Ukrenergo Telegram")
    if claim_flag_failure_rows:
        failures.append("semantic event audit claim flags must remain research-only/not market execution")
    if future_leakage_rows:
        failures.append("semantic event audit found future event freshness")

    metadata = {
        "row_count": audit_frame.height,
        "tenant_count": len(tenant_ids),
        "tenant_ids": tenant_ids,
        "source_model_count": len(source_model_names),
        "source_model_names": source_model_names,
        "bad_validation_anchor_rows": len(bad_anchor_rows),
        "bad_source_rows": len(bad_source_rows),
        "claim_flag_failure_rows": len(claim_flag_failure_rows),
        "future_leakage_rows": len(future_leakage_rows),
        "total_event_anchor_count": sum(int(row["event_anchor_count"]) for row in rows),
        "total_strict_failure_with_event_count": sum(
            int(row["strict_failure_with_event_count"]) for row in rows
        ),
    }
    return EvidenceCheckOutcome(
        passed=not failures,
        description=(
            "Semantic grid-event strict-failure audit evidence has valid source and claim boundaries."
            if not failures
            else "; ".join(failures)
        ),
        metadata=metadata,
    )


def _validate_afe_catalog(frame: pl.DataFrame) -> None:
    outcome = validate_forecast_afe_feature_catalog_evidence(frame)
    if not outcome.passed:
        raise ValueError(f"AFE feature catalog is invalid: {outcome.description}")
    _require_columns(frame, REQUIRED_AFE_FEATURE_CATALOG_COLUMNS, frame_name="forecast_afe_feature_catalog_frame")
    semantic_rows = [
        row for row in frame.iter_rows(named=True) if str(row["feature_group"]) == "semantic_grid_event"
    ]
    if not semantic_rows:
        raise ValueError("AFE feature catalog is missing semantic grid-event rows")
    for row in semantic_rows:
        if str(row["source_name"]) != "UKRENERGO_TELEGRAM":
            raise ValueError("semantic event audit requires official Telegram source rows")
        if str(row["semantic_source_kind"]) != "official_telegram":
            raise ValueError("semantic event audit requires official Telegram semantic rows")
        if not bool(row["training_use_allowed"]):
            raise ValueError("implemented Ukrenergo semantic rows must be training-use allowed")


def _validate_strict_selector_frame(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        REQUIRED_SEMANTIC_STRICT_COLUMNS,
        frame_name="dfl_feature_aware_strict_failure_selector_strict_lp_benchmark_frame",
    )
    if frame.height == 0:
        raise ValueError("strict selector frame must not be empty")
    for row in frame.iter_rows(named=True):
        payload = _payload(row)
        if payload.get("data_quality_tier") != "thesis_grade":
            raise ValueError("semantic event audit requires thesis_grade strict selector rows")
        if float(payload.get("observed_coverage_ratio", 0.0)) < 1.0:
            raise ValueError("semantic event audit requires observed strict selector coverage")
        if int(payload.get("safety_violation_count", 0)):
            raise ValueError("semantic event audit requires zero safety violations")
        if not bool(payload.get("not_full_dfl", False)):
            raise ValueError("semantic event audit requires not_full_dfl=true")
        if not bool(payload.get("not_market_execution", False)):
            raise ValueError("semantic event audit requires not_market_execution=true")


def _validate_grid_event_signal_frame(frame: pl.DataFrame) -> None:
    _require_columns(frame, REQUIRED_GRID_EVENT_SIGNAL_COLUMNS, frame_name="grid_event_signal_silver")
    for row in frame.iter_rows(named=True):
        freshness = _float_value(row.get("event_source_freshness_hours"), default=999.0)
        if freshness < 0.0:
            raise ValueError("grid-event semantic features must not contain future event freshness")


def _audit_row(
    tenant_id: str,
    source_model_name: str,
    window_index: int,
    pairs: list[dict[str, Any]],
) -> dict[str, Any]:
    event_pairs = [pair for pair in pairs if bool(pair["event_active"])]
    non_event_pairs = [pair for pair in pairs if not bool(pair["event_active"])]
    strict_failures = [pair for pair in pairs if float(pair["selector_gain_uah"]) > 0.0]
    strict_failures_with_event = [
        pair for pair in strict_failures if bool(pair["event_active"])
    ]
    event_coverage = len(event_pairs) / len(pairs) if pairs else 0.0
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "window_index": window_index,
        "validation_anchor_count": len(pairs),
        "validation_start_anchor_timestamp": min(pair["anchor_timestamp"] for pair in pairs),
        "validation_end_anchor_timestamp": max(pair["anchor_timestamp"] for pair in pairs),
        "event_anchor_count": len(event_pairs),
        "non_event_anchor_count": len(non_event_pairs),
        "strict_failure_count": len(strict_failures),
        "strict_failure_with_event_count": len(strict_failures_with_event),
        "semantic_source_name": "UKRENERGO_TELEGRAM",
        "semantic_source_kind": "official_telegram",
        "event_feature_names": list(GRID_EVENT_FEATURE_COLUMNS),
        "event_signal_coverage_rate": event_coverage,
        "mean_national_grid_risk_score": _mean_field(pairs, "national_grid_risk_score"),
        "max_national_grid_risk_score": max(
            (float(pair["national_grid_risk_score"]) for pair in pairs),
            default=0.0,
        ),
        "mean_strict_regret_with_events_uah": _mean_field(event_pairs, "strict_regret_uah"),
        "mean_strict_regret_without_events_uah": _mean_field(non_event_pairs, "strict_regret_uah"),
        "mean_selector_regret_with_events_uah": _mean_field(event_pairs, "selector_regret_uah"),
        "mean_selector_regret_without_events_uah": _mean_field(non_event_pairs, "selector_regret_uah"),
        "selector_gain_with_events_uah": _mean_field(event_pairs, "selector_gain_uah"),
        "selector_gain_without_events_uah": _mean_field(non_event_pairs, "selector_gain_uah"),
        "min_event_source_freshness_hours": min(
            (float(pair["event_source_freshness_hours"]) for pair in pairs),
            default=999.0,
        ),
        "explanation_label": _explanation_label(event_pairs, non_event_pairs),
        "claim_scope": DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_CLAIM_SCOPE,
        "academic_scope": DFL_SEMANTIC_EVENT_STRICT_FAILURE_AUDIT_ACADEMIC_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
    }


def _anchor_pair(
    *,
    tenant_id: str,
    source_model_name: str,
    window_index: int,
    anchor_timestamp: datetime,
    strict_row: dict[str, Any],
    selector_row: dict[str, Any],
    event_row: dict[str, Any],
) -> dict[str, Any]:
    strict_regret = float(strict_row["regret_uah"])
    selector_regret = float(selector_row["regret_uah"])
    return {
        "tenant_id": tenant_id,
        "source_model_name": source_model_name,
        "window_index": window_index,
        "anchor_timestamp": anchor_timestamp,
        "strict_regret_uah": strict_regret,
        "selector_regret_uah": selector_regret,
        "selector_gain_uah": strict_regret - selector_regret,
        "event_active": _event_active(event_row),
        **{
            column_name: _float_value(event_row.get(column_name), default=0.0)
            for column_name in GRID_EVENT_FEATURE_COLUMNS
        },
    }


def _explanation_label(
    event_pairs: list[dict[str, Any]],
    non_event_pairs: list[dict[str, Any]],
) -> str:
    if not event_pairs:
        return "no_semantic_event_coverage"
    event_gain = _mean_field(event_pairs, "selector_gain_uah")
    non_event_gain = _mean_field(non_event_pairs, "selector_gain_uah")
    if event_gain > non_event_gain:
        return "semantic_events_explain_some_strict_failures"
    return "semantic_events_not_primary_driver"


def _event_active(event_row: dict[str, Any]) -> bool:
    return any(_float_value(event_row.get(column_name), default=0.0) > 0.0 for column_name in _EVENT_ACTIVITY_COLUMNS)


def _event_rows_by_key(frame: pl.DataFrame) -> dict[tuple[str, datetime], dict[str, Any]]:
    return {
        (
            str(row["tenant_id"]),
            _datetime_value(row["timestamp"], field_name="timestamp"),
        ): row
        for row in frame.iter_rows(named=True)
    }


def _event_row(
    rows_by_key: dict[tuple[str, datetime], dict[str, Any]],
    *,
    tenant_id: str,
    anchor_timestamp: datetime,
) -> dict[str, Any]:
    key = (tenant_id, anchor_timestamp)
    if key not in rows_by_key:
        raise ValueError(
            f"grid_event_signal_silver is missing {tenant_id}/{anchor_timestamp.isoformat()}"
        )
    return rows_by_key[key]


def _single_role_row(
    rows: list[dict[str, Any]],
    *,
    role: str,
    forecast_model_name: str | None = None,
) -> dict[str, Any]:
    matches = [
        row
        for row in rows
        if _selector_role(row) == role
        or (forecast_model_name is not None and str(row["forecast_model_name"]) == forecast_model_name)
    ]
    if not matches:
        if forecast_model_name:
            raise ValueError(f"missing {forecast_model_name} row")
        raise ValueError(f"missing selector role {role}")
    return matches[0]


def _source_model_name(row: dict[str, Any]) -> str:
    if row.get("source_model_name"):
        return str(row["source_model_name"])
    payload = _payload(row)
    source_name = payload.get("source_forecast_model_name")
    if not source_name:
        raise ValueError("strict selector rows must include source model name")
    return str(source_name)


def _selector_role(row: dict[str, Any]) -> str:
    payload = _payload(row)
    return str(payload.get("selector_row_role", ""))


def _window_index(row: dict[str, Any]) -> int:
    payload = _payload(row)
    raw_value = payload.get("window_index", payload.get("final_window_index", 1))
    return int(raw_value)


def _payload(row: dict[str, Any]) -> dict[str, Any]:
    payload = row.get("evaluation_payload")
    if not isinstance(payload, dict):
        raise ValueError("evaluation_payload must be a dictionary")
    return payload


def _mean_field(rows: list[dict[str, Any]], column_name: str) -> float:
    values = [float(row[column_name]) for row in rows if row.get(column_name) is not None]
    return mean(values) if values else 0.0


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


def _require_columns(frame: pl.DataFrame, required_columns: frozenset[str], *, frame_name: str) -> None:
    failures = _missing_column_failures(frame, required_columns)
    if failures:
        raise ValueError(f"{frame_name} " + "; ".join(failures))


def _missing_column_failures(frame: pl.DataFrame, required_columns: frozenset[str]) -> list[str]:
    missing = sorted(required_columns.difference(frame.columns))
    return [f"missing required columns: {missing}"] if missing else []

