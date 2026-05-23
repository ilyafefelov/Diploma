"""Ukrainian context acquisition readiness before V13 candidate generation.

This module stops at acquisition/readiness evidence. It does not create
candidate schedules, train selectors, or start DT/LAVA.
"""

from __future__ import annotations

from typing import Any, Final

import polars as pl

UA_CONTEXT_SOURCE_INVENTORY_V13_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_source_inventory_v13_not_full_dfl"
)
UA_CONTEXT_ACQUISITION_READINESS_V13_CLAIM_SCOPE: Final[str] = (
    "dfl_ua_context_acquisition_readiness_v13_not_full_dfl"
)

_V12_INVENTORY_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_family",
        "source_status",
        "coverage_ratio",
        "market_execution_enabled",
    }
)
_V12_READINESS_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "prior_material_safe_switch_example_count",
        "min_prior_material_safe_switch_examples_for_dt",
        "dt_lava_ready",
        "target_label_space",
        "raw_hourly_action_imitation",
        "market_execution_enabled",
    }
)
_V13_INVENTORY_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_family",
        "source_status",
        "coverage_ratio",
        "required_for_v13_candidate_generation",
        "market_execution_enabled",
    }
)
_V13_SOURCE_EVIDENCE_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "source_family",
        "source_status",
        "coverage_ratio",
        "required_for_v13_candidate_generation",
        "market_execution_enabled",
    }
)
_CURRENT_SOURCE_FAMILIES: Final[frozenset[str]] = frozenset(
    {
        "oree_dam_history",
        "open_meteo_archive",
        "tenant_load_pv_proxy",
        "ukrenergo_grid_event_archive",
        "calendar_publication_rules",
    }
)
_TARGETED_ACQUISITION_FAMILIES: Final[tuple[tuple[str, str], ...]] = (
    (
        "measured_or_source_backed_tenant_load_pv",
        "Measured tenant load/PV telemetry or a source-backed tenant load/PV proxy",
    ),
    (
        "explicit_dam_publication_receipts",
        "Row-level OREE DAM publication receipts or source-backed publication logs",
    ),
    (
        "richer_grid_outage_archive",
        "Historical Ukrenergo/outage/no-event archive with source coverage windows",
    ),
    (
        "extended_ukrainian_dam_weather_history",
        "Longer Ukrainian DAM, weather, and tenant-context backfill for sparse labels",
    ),
)


def build_dfl_ua_context_acquisition_source_evidence_v13_frame(
    dam_publication_backfill_frame: pl.DataFrame,
    weather_load_pv_backfill_frame: pl.DataFrame,
    grid_event_backfill_frame: pl.DataFrame,
    context_backfill_coverage_gate_frame: pl.DataFrame,
) -> pl.DataFrame:
    """Summarize acquired Ukrainian source evidence before V13 readiness.

    This is still a pre-candidate evidence frame. It can mark weather/load/PV
    proxy and grid no-event coverage as source-backed context, but it keeps
    OREE market-rule publication timing separate from explicit row-level
    publication receipts.
    """

    _refuse_market_execution(
        dam_publication_backfill_frame,
        frame_name="DAM publication backfill frame",
    )
    _refuse_market_execution(
        weather_load_pv_backfill_frame,
        frame_name="weather/load/PV backfill frame",
    )
    _refuse_market_execution(
        grid_event_backfill_frame,
        frame_name="grid-event backfill frame",
    )
    _refuse_market_execution(
        context_backfill_coverage_gate_frame,
        frame_name="context backfill coverage gate frame",
    )

    dam_ready = _status_ratio(
        dam_publication_backfill_frame,
        status_column="dam_publication_backfill_status",
        ready_value="context_ready",
    )
    explicit_dam_ready = _explicit_dam_receipt_ratio(dam_publication_backfill_frame)
    weather_ready = _status_ratio(
        weather_load_pv_backfill_frame,
        status_column="weather_load_pv_backfill_status",
        ready_value="context_ready",
    )
    grid_ready = _status_ratio(
        grid_event_backfill_frame,
        status_column="grid_event_backfill_status",
        ready_value="context_ready",
    )
    coverage_ready = _status_ratio(
        context_backfill_coverage_gate_frame,
        status_column="context_backfill_gate_decision",
        ready_value="context_backfill_ready",
    )

    rows = [
        _evidence_row(
            source_family="oree_dam_history",
            source_group="current_ukrainian_source",
            coverage_ratio=dam_ready,
            source_description="OREE DAM history with source-backed publication timing",
            source_evidence_mode=_evidence_modes(
                dam_publication_backfill_frame,
                "publication_evidence_mode",
            ),
            source_rows=dam_publication_backfill_frame.height,
            required_anchor_rows=dam_publication_backfill_frame.height,
        ),
        _evidence_row(
            source_family="open_meteo_archive",
            source_group="current_ukrainian_source",
            coverage_ratio=weather_ready,
            source_description="Open-Meteo archive weather context",
            source_evidence_mode="source_backed_weather_archive",
            source_rows=weather_load_pv_backfill_frame.height,
            required_anchor_rows=weather_load_pv_backfill_frame.height,
        ),
        _evidence_row(
            source_family="tenant_load_pv_proxy",
            source_group="current_ukrainian_source",
            coverage_ratio=weather_ready,
            source_description="Configured tenant load/PV proxy joined to weather context",
            source_evidence_mode="source_backed_proxy",
            source_rows=weather_load_pv_backfill_frame.height,
            required_anchor_rows=weather_load_pv_backfill_frame.height,
        ),
        _evidence_row(
            source_family="ukrenergo_grid_event_archive",
            source_group="current_ukrainian_source",
            coverage_ratio=grid_ready,
            source_description="Ukrenergo grid-event and source-backed no-event coverage",
            source_evidence_mode="source_backed_event_or_no_event_archive",
            source_rows=grid_event_backfill_frame.height,
            required_anchor_rows=grid_event_backfill_frame.height,
        ),
        _evidence_row(
            source_family="calendar_publication_rules",
            source_group="current_ukrainian_source",
            coverage_ratio=coverage_ready,
            source_description="Calendar, DST, block, and publication-rule context",
            source_evidence_mode="deterministic_calendar_and_market_rule",
            source_rows=context_backfill_coverage_gate_frame.height,
            required_anchor_rows=context_backfill_coverage_gate_frame.height,
        ),
        _evidence_row(
            source_family="measured_or_source_backed_tenant_load_pv",
            source_group="targeted_ukrainian_acquisition",
            coverage_ratio=weather_ready,
            source_description=(
                "Measured tenant load/PV if available; otherwise source-backed "
                "tenant load/PV proxy provenance"
            ),
            source_evidence_mode="source_backed_proxy",
            source_rows=weather_load_pv_backfill_frame.height,
            required_anchor_rows=weather_load_pv_backfill_frame.height,
        ),
        _evidence_row(
            source_family="explicit_dam_publication_receipts",
            source_group="targeted_ukrainian_acquisition",
            coverage_ratio=explicit_dam_ready,
            source_description="Explicit row-level OREE DAM publication receipts",
            source_evidence_mode=_evidence_modes(
                dam_publication_backfill_frame,
                "publication_evidence_mode",
            ),
            source_rows=dam_publication_backfill_frame.height,
            required_anchor_rows=dam_publication_backfill_frame.height,
            partial_status=(
                "partial_context_rule_deadline_without_row_receipts"
                if dam_ready >= 1.0 and explicit_dam_ready < 1.0
                else None
            ),
        ),
        _evidence_row(
            source_family="richer_grid_outage_archive",
            source_group="targeted_ukrainian_acquisition",
            coverage_ratio=grid_ready,
            source_description="Historical grid outage/event/no-event archive coverage",
            source_evidence_mode="source_backed_event_or_no_event_archive",
            source_rows=grid_event_backfill_frame.height,
            required_anchor_rows=grid_event_backfill_frame.height,
        ),
        _evidence_row(
            source_family="extended_ukrainian_dam_weather_history",
            source_group="targeted_ukrainian_acquisition",
            coverage_ratio=coverage_ready,
            source_description="Extended Ukrainian DAM/weather/context coverage window",
            source_evidence_mode="widened_2025_2026_coverage",
            source_rows=context_backfill_coverage_gate_frame.height,
            required_anchor_rows=context_backfill_coverage_gate_frame.height,
        ),
    ]
    frame = pl.DataFrame(rows, infer_schema_length=None).sort("source_family")
    _validate_v13_source_evidence(frame)
    return frame


def build_dfl_ua_context_source_inventory_v13_frame(
    ua_context_source_expansion_inventory_v12_frame: pl.DataFrame,
    ua_v12_dt_lava_readiness_decision_frame: pl.DataFrame,
    ua_context_acquisition_source_evidence_v13_frame: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Summarize source and teacher-label blockers before V13 candidates."""

    _validate_v12_inventory(ua_context_source_expansion_inventory_v12_frame)
    _validate_v12_readiness(ua_v12_dt_lava_readiness_decision_frame)
    if ua_context_acquisition_source_evidence_v13_frame is not None:
        _validate_v13_source_evidence(
            ua_context_acquisition_source_evidence_v13_frame
        )
    evidence_by_family = _evidence_by_family(
        ua_context_acquisition_source_evidence_v13_frame
    )

    rows: list[dict[str, Any]] = []
    for row in ua_context_source_expansion_inventory_v12_frame.iter_rows(named=True):
        source_family = str(row["source_family"])
        evidence = evidence_by_family.get(source_family)
        coverage_ratio = _safe_float(
            evidence.get("coverage_ratio")
            if evidence is not None
            else row["coverage_ratio"]
        )
        source_status = (
            str(evidence["source_status"])
            if evidence is not None
            else _v13_status_from_v12(row)
        )
        rows.append(
            {
                "source_family": source_family,
                "source_group": str(evidence["source_group"])
                if evidence is not None and "source_group" in evidence
                else _source_group(row),
                "source_status": source_status,
                "v12_source_status": str(row["source_status"]),
                "coverage_ratio": coverage_ratio,
                "required_for_v13_candidate_generation": (
                    source_family in _CURRENT_SOURCE_FAMILIES
                    or source_family
                    in {family for family, _ in _TARGETED_ACQUISITION_FAMILIES}
                ),
                "source_description": str(
                    evidence.get("source_description")
                    if evidence is not None
                    else row.get("source_description", source_family)
                ),
                "source_evidence_mode": str(
                    evidence.get("source_evidence_mode", "v12_inventory")
                    if evidence is not None
                    else "v12_inventory"
                ),
                "source_rows": _safe_int(evidence.get("source_rows"))
                if evidence is not None
                else 0,
                "required_anchor_rows": _safe_int(
                    evidence.get("required_anchor_rows")
                )
                if evidence is not None
                else 0,
                "ready_anchor_rows": _safe_int(evidence.get("ready_anchor_rows"))
                if evidence is not None
                else 0,
                "blocker": "none"
                if source_status == "ready_prior_context"
                else f"{source_family}:{source_status}",
                "target_label_space": "v13_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "no_eu_rows_as_ukrainian_targets": True,
                "claim_scope": UA_CONTEXT_SOURCE_INVENTORY_V13_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )

    present_families = {str(row["source_family"]) for row in rows}
    for source_family, source_description in _TARGETED_ACQUISITION_FAMILIES:
        if source_family in present_families:
            continue
        evidence = evidence_by_family.get(source_family)
        if evidence is not None:
            source_status = str(evidence["source_status"])
            coverage_ratio = _safe_float(evidence["coverage_ratio"])
            source_description = str(
                evidence.get("source_description", source_description)
            )
            source_group = str(
                evidence.get("source_group", "targeted_ukrainian_acquisition")
            )
            source_evidence_mode = str(evidence.get("source_evidence_mode", "none"))
            source_rows = _safe_int(evidence.get("source_rows"))
            required_anchor_rows = _safe_int(evidence.get("required_anchor_rows"))
            ready_anchor_rows = _safe_int(evidence.get("ready_anchor_rows"))
            v12_source_status = "not_in_v12_inventory_acquired_for_v13"
        else:
            source_status = "blocked_missing_source"
            coverage_ratio = 0.0
            source_group = "targeted_ukrainian_acquisition"
            source_evidence_mode = "none"
            source_rows = 0
            required_anchor_rows = 0
            ready_anchor_rows = 0
            v12_source_status = "missing_from_v12_inventory"
        rows.append(
            {
                "source_family": source_family,
                "source_group": source_group,
                "source_status": source_status,
                "v12_source_status": v12_source_status,
                "coverage_ratio": coverage_ratio,
                "required_for_v13_candidate_generation": True,
                "source_description": source_description,
                "source_evidence_mode": source_evidence_mode,
                "source_rows": source_rows,
                "required_anchor_rows": required_anchor_rows,
                "ready_anchor_rows": ready_anchor_rows,
                "blocker": "none"
                if source_status == "ready_prior_context"
                else f"{source_family}:{source_status}",
                "target_label_space": "v13_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "no_eu_rows_as_ukrainian_targets": True,
                "claim_scope": UA_CONTEXT_SOURCE_INVENTORY_V13_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )

    safe_label_row = _safe_label_support_row(
        ua_v12_dt_lava_readiness_decision_frame
    )
    rows.append(safe_label_row)

    frame = pl.DataFrame(rows, infer_schema_length=None).sort("source_family")
    _validate_v13_inventory(frame)
    return frame


def build_dfl_ua_context_acquisition_readiness_v13_frame(
    ua_v12_dt_lava_readiness_decision_frame: pl.DataFrame,
    ua_context_source_inventory_v13_frame: pl.DataFrame,
    *,
    min_prior_material_safe_switch_examples_for_dt: int = 20,
) -> pl.DataFrame:
    """Gate whether V13 lower-tail-risk candidate generation may start."""

    _validate_v12_readiness(ua_v12_dt_lava_readiness_decision_frame)
    _validate_v13_inventory(ua_context_source_inventory_v13_frame)
    if min_prior_material_safe_switch_examples_for_dt < 0:
        raise ValueError(
            "min_prior_material_safe_switch_examples_for_dt must not be negative."
        )

    required_blockers = [
        f"{row['source_family']}:{row['source_status']}"
        for row in ua_context_source_inventory_v13_frame.iter_rows(named=True)
        if bool(row["required_for_v13_candidate_generation"])
        and str(row["source_status"]) != "ready_prior_context"
        and str(row["source_family"]) != "v12_safe_teacher_label_support"
    ]
    required_source_count = ua_context_source_inventory_v13_frame.filter(
        pl.col("required_for_v13_candidate_generation")
    ).height
    ready_source_count = ua_context_source_inventory_v13_frame.filter(
        pl.col("required_for_v13_candidate_generation")
        & (pl.col("source_status") == "ready_prior_context")
    ).height

    rows: list[dict[str, Any]] = []
    for row in ua_v12_dt_lava_readiness_decision_frame.iter_rows(named=True):
        prior_count = int(row["prior_material_safe_switch_example_count"])
        configured_min = max(
            min_prior_material_safe_switch_examples_for_dt,
            int(row["min_prior_material_safe_switch_examples_for_dt"]),
        )
        safe_label_ready = prior_count >= configured_min
        blockers = list(required_blockers)
        if not safe_label_ready:
            blockers.append("v12_safe_teacher_label_support:blocked_insufficient_safe_teacher_labels")
        ready = not blockers
        rows.append(
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "prior_material_safe_switch_example_count": prior_count,
                "min_prior_material_safe_switch_examples_for_dt": configured_min,
                "safe_teacher_label_support_ready": safe_label_ready,
                "required_source_family_count": required_source_count,
                "ready_source_family_count": ready_source_count,
                "blocked_source_family_count": len(blockers),
                "blocking_context_families": ",".join(blockers)
                if blockers
                else "none",
                "v13_candidate_generation_ready": ready,
                "readiness_decision": "v13_candidate_generation_ready"
                if ready
                else "data_acquisition_needed",
                "recommended_next_step": "build_v13_lower_tail_risk_candidates"
                if ready
                else "acquire_ukrainian_context_and_backfill_safe_labels",
                "dt_lava_ready": False,
                "dt_lava_blocked_reason": "v13_candidates_not_built_yet"
                if ready
                else "v13_precondition_not_ready",
                "target_label_space": "v13_precondition_context_coverage",
                "raw_hourly_action_imitation": False,
                "no_eu_rows_as_ukrainian_targets": True,
                "claim_scope": UA_CONTEXT_ACQUISITION_READINESS_V13_CLAIM_SCOPE,
                "not_full_dfl": True,
                "not_market_execution": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows, infer_schema_length=None).sort(
        ["source_model_name", "tenant_id"]
    )


def _safe_label_support_row(v12_readiness_frame: pl.DataFrame) -> dict[str, Any]:
    prior_counts = v12_readiness_frame["prior_material_safe_switch_example_count"]
    required_counts = v12_readiness_frame["min_prior_material_safe_switch_examples_for_dt"]
    min_prior_count = _safe_int(prior_counts.min()) if v12_readiness_frame.height else 0
    max_required_count = (
        _safe_int(required_counts.max()) if v12_readiness_frame.height else 20
    )
    coverage_ratio = min_prior_count / max_required_count if max_required_count else 1.0
    ready = v12_readiness_frame.height > 0 and min_prior_count >= max_required_count
    status = (
        "ready_prior_context"
        if ready
        else "blocked_insufficient_safe_teacher_labels"
    )
    return {
        "source_family": "v12_safe_teacher_label_support",
        "source_group": "teacher_label_precondition",
        "source_status": status,
        "v12_source_status": ",".join(
            sorted(
                str(value)
                for value in v12_readiness_frame["readiness_decision"].unique()
            )
        )
        if v12_readiness_frame.height
        else "missing_v12_readiness_rows",
        "coverage_ratio": float(min(coverage_ratio, 1.0)),
        "required_for_v13_candidate_generation": True,
        "source_description": (
            "Minimum prior/train non-tail-risk material safe-switch examples per "
            "tenant/source before V13 candidates are worth building"
        ),
        "source_evidence_mode": "v12_teacher_label_counts",
        "source_rows": v12_readiness_frame.height,
        "required_anchor_rows": v12_readiness_frame.height,
        "ready_anchor_rows": v12_readiness_frame.filter(
            pl.col("prior_material_safe_switch_example_count") >= max_required_count
        ).height
        if v12_readiness_frame.height
        else 0,
        "blocker": "none"
        if ready
        else "v12_safe_teacher_label_support:blocked_insufficient_safe_teacher_labels",
        "target_label_space": "v13_precondition_context_coverage",
        "raw_hourly_action_imitation": False,
        "no_eu_rows_as_ukrainian_targets": True,
        "claim_scope": UA_CONTEXT_SOURCE_INVENTORY_V13_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _v13_status_from_v12(row: dict[str, Any]) -> str:
    coverage_ratio = _safe_float(row["coverage_ratio"])
    source_status = str(row["source_status"])
    if coverage_ratio >= 1.0 and source_status in {
        "context_ready",
        "ready_prior_context",
    }:
        return "ready_prior_context"
    if source_status == "blocked_missing_source" or coverage_ratio <= 0.0:
        return "blocked_missing_source"
    return "partial_context"


def _evidence_row(
    *,
    source_family: str,
    source_group: str,
    coverage_ratio: float,
    source_description: str,
    source_evidence_mode: str,
    source_rows: int,
    required_anchor_rows: int,
    partial_status: str | None = None,
) -> dict[str, Any]:
    resolved_ratio = max(0.0, min(1.0, float(coverage_ratio)))
    if resolved_ratio >= 1.0:
        source_status = "ready_prior_context"
    elif partial_status is not None:
        source_status = partial_status
    elif resolved_ratio > 0.0:
        source_status = "partial_context"
    else:
        source_status = "blocked_missing_source"
    denominator = max(required_anchor_rows, source_rows)
    return {
        "source_family": source_family,
        "source_group": source_group,
        "source_status": source_status,
        "coverage_ratio": resolved_ratio,
        "source_description": source_description,
        "source_evidence_mode": source_evidence_mode,
        "source_rows": int(source_rows),
        "required_anchor_rows": int(required_anchor_rows),
        "ready_anchor_rows": int(round(resolved_ratio * denominator)),
        "required_for_v13_candidate_generation": True,
        "target_label_space": "v13_precondition_context_coverage",
        "raw_hourly_action_imitation": False,
        "no_eu_rows_as_ukrainian_targets": True,
        "claim_scope": UA_CONTEXT_SOURCE_INVENTORY_V13_CLAIM_SCOPE,
        "not_full_dfl": True,
        "not_market_execution": True,
        "market_execution_enabled": False,
    }


def _evidence_by_family(frame: pl.DataFrame | None) -> dict[str, dict[str, Any]]:
    if frame is None or frame.height == 0:
        return {}
    return {str(row["source_family"]): row for row in frame.iter_rows(named=True)}


def _status_ratio(
    frame: pl.DataFrame,
    *,
    status_column: str,
    ready_value: str,
) -> float:
    if frame.height == 0 or status_column not in frame.columns:
        return 0.0
    return frame.filter(pl.col(status_column) == ready_value).height / frame.height


def _explicit_dam_receipt_ratio(frame: pl.DataFrame) -> float:
    if frame.height == 0 or "publication_evidence_mode" not in frame.columns:
        return 0.0
    ready_rows = frame.filter(
        (pl.col("dam_publication_backfill_status") == "context_ready")
        & (pl.col("publication_evidence_mode") == "explicit_source_metadata")
    ).height
    return ready_rows / frame.height


def _evidence_modes(frame: pl.DataFrame, column_name: str) -> str:
    if frame.height == 0 or column_name not in frame.columns:
        return "missing"
    return ",".join(sorted(str(value) for value in frame[column_name].unique()))


def _source_group(row: dict[str, Any]) -> str:
    source_group = row.get("source_group")
    if source_group is not None:
        return str(source_group)
    if str(row["source_family"]) in _CURRENT_SOURCE_FAMILIES:
        return "current_ukrainian_source"
    return "targeted_ukrainian_acquisition"


def _validate_v12_inventory(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _V12_INVENTORY_REQUIRED_COLUMNS,
        frame_name="V12 source inventory frame",
    )
    _refuse_market_execution(frame, frame_name="V12 source inventory frame")


def _validate_v12_readiness(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _V12_READINESS_REQUIRED_COLUMNS,
        frame_name="V12 readiness frame",
    )
    _refuse_market_execution(frame, frame_name="V12 readiness frame")
    if frame.filter(pl.col("raw_hourly_action_imitation")).height:
        raise ValueError("V13 acquisition refuses raw hourly action imitation rows.")


def _validate_v13_inventory(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _V13_INVENTORY_REQUIRED_COLUMNS,
        frame_name="V13 source inventory frame",
    )
    _refuse_market_execution(frame, frame_name="V13 source inventory frame")


def _validate_v13_source_evidence(frame: pl.DataFrame) -> None:
    _require_columns(
        frame,
        _V13_SOURCE_EVIDENCE_REQUIRED_COLUMNS,
        frame_name="V13 source acquisition evidence frame",
    )
    _refuse_market_execution(
        frame,
        frame_name="V13 source acquisition evidence frame",
    )


def _require_columns(
    frame: pl.DataFrame,
    columns: frozenset[str],
    *,
    frame_name: str,
) -> None:
    missing = columns.difference(frame.columns)
    if missing:
        raise ValueError(f"{frame_name} is missing required columns: {sorted(missing)}")


def _refuse_market_execution(frame: pl.DataFrame, *, frame_name: str) -> None:
    if frame.filter(pl.col("market_execution_enabled")).height:
        raise ValueError(f"{frame_name} contains market execution rows.")


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (float, int, str)):
        return float(value)
    raise TypeError(f"Cannot convert {type(value).__name__} to float.")


def _safe_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Cannot convert {type(value).__name__} to int.")


__all__ = [
    "UA_CONTEXT_ACQUISITION_READINESS_V13_CLAIM_SCOPE",
    "UA_CONTEXT_SOURCE_INVENTORY_V13_CLAIM_SCOPE",
    "build_dfl_ua_context_acquisition_source_evidence_v13_frame",
    "build_dfl_ua_context_acquisition_readiness_v13_frame",
    "build_dfl_ua_context_source_inventory_v13_frame",
]
