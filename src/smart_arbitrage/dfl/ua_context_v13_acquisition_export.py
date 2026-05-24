"""Local evidence export for V13 Ukrainian context acquisition readiness."""

from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Final, Mapping

import polars as pl

from smart_arbitrage.dfl.v2_plus_dfl_dt_bridge import V2_PLUS_HEADLINE_BASELINE_METRICS

UA_CONTEXT_V13_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_acquisition_summary.json"
)
UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_acquisition_summary.md"
)
UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_source_inventory_rows.csv"
)
UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_source_acquisition_evidence_rows.csv"
)
UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_readiness_rows.csv"
)
UA_CONTEXT_V13_SAFE_SWITCH_ACQUISITION_TARGETS_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_safe_switch_acquisition_targets.csv"
)
UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_source_acquisition_backlog.csv"
)
UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_receipt_source_audit.json"
)
UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME: Final[str] = (
    "dfl_ua_context_v13_acquisition_input_preflight.json"
)


def build_dfl_ua_context_v13_acquisition_packet(
    *,
    run_slug: str,
    source_inventory_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
    acquisition_source_evidence_frame: pl.DataFrame | None = None,
    receipt_source_audit: Mapping[str, Any] | None = None,
    acquisition_input_preflight: Mapping[str, Any] | None = None,
    dagster_run_id: str | None = None,
    materialization_command: str | None = None,
    asset_check_status: str | None = None,
) -> dict[str, Any]:
    """Build a V13 source-acquisition packet from materialized frames."""

    _validate_packet_inputs(
        source_inventory_frame=source_inventory_frame,
        readiness_frame=readiness_frame,
        acquisition_source_evidence_frame=acquisition_source_evidence_frame,
        receipt_source_audit=receipt_source_audit,
        acquisition_input_preflight=acquisition_input_preflight,
    )
    readiness_summary = _readiness_summary(readiness_frame)
    safe_switch_acquisition_target_summary = (
        _safe_switch_acquisition_target_summary(readiness_frame)
    )
    source_acquisition_backlog_summary = _source_acquisition_backlog_summary(
        source_inventory_frame,
        safe_switch_acquisition_target_summary,
    )
    attached_artifacts = {
        "summary_json": UA_CONTEXT_V13_JSON_ARTIFACT_NAME,
        "summary_markdown": UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME,
        "source_evidence_csv": UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME,
        "source_inventory_csv": UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME,
        "readiness_csv": UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME,
        "safe_switch_acquisition_targets_csv": (
            UA_CONTEXT_V13_SAFE_SWITCH_ACQUISITION_TARGETS_CSV_ARTIFACT_NAME
        ),
        "source_acquisition_backlog_csv": (
            UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME
        ),
    }
    if receipt_source_audit is not None:
        attached_artifacts["receipt_source_audit_json"] = (
            UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME
        )
    if acquisition_input_preflight is not None:
        attached_artifacts["acquisition_input_preflight_json"] = (
            UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME
        )
    return {
        "run_slug": run_slug,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "dagster_run_id": dagster_run_id,
        "materialization_command": materialization_command,
        "asset_check_status": asset_check_status,
        "v13_candidate_generation_ready": readiness_summary[
            "v13_candidate_generation_ready"
        ],
        "claim_boundary": {
            "offline_strategy_promotion_only": True,
            "not_full_dfl": True,
            "not_deployed_decision_transformer_control": True,
            "not_market_execution": True,
            "market_execution_enabled": False,
            "no_european_training_rows": True,
            "no_dashboard_api_default_switch": True,
            "v13_stops_before_candidate_generation": True,
            "dt_lava_still_gated": True,
        },
        "headline_baseline": {
            **dict(V2_PLUS_HEADLINE_BASELINE_METRICS),
            "calibrated_v2_plus_median_regret_uah": 67.30,
        },
        "source_inventory_summary": _source_inventory_summary(source_inventory_frame),
        "acquisition_source_evidence_summary": _source_inventory_summary(
            acquisition_source_evidence_frame
        )
        if acquisition_source_evidence_frame is not None
        else None,
        "readiness_summary": readiness_summary,
        "safe_switch_deficit_summary": _safe_switch_deficit_summary(
            readiness_frame
        ),
        "safe_switch_acquisition_target_summary": (
            safe_switch_acquisition_target_summary
        ),
        "source_acquisition_backlog_summary": source_acquisition_backlog_summary,
        "receipt_source_audit_summary": _receipt_source_audit_summary(
            receipt_source_audit
        ),
        "acquisition_input_preflight_summary": _acquisition_input_preflight_summary(
            acquisition_input_preflight
        ),
        "attached_artifacts": attached_artifacts,
    }


def write_dfl_ua_context_v13_acquisition_packet(
    packet: dict[str, Any],
    *,
    output_root: Path,
    source_inventory_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
    acquisition_source_evidence_frame: pl.DataFrame | None = None,
    receipt_source_audit: Mapping[str, Any] | None = None,
    acquisition_input_preflight: Mapping[str, Any] | None = None,
) -> Path:
    """Write local JSON, Markdown, and CSV V13 acquisition artifacts."""

    export_dir = output_root / str(packet["run_slug"])
    export_dir.mkdir(parents=True, exist_ok=True)
    if acquisition_source_evidence_frame is not None:
        _write_csv_safe(
            acquisition_source_evidence_frame,
            export_dir / UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME,
        )
    _write_csv_safe(
        source_inventory_frame,
        export_dir / UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME,
    )
    _write_csv_safe(
        readiness_frame,
        export_dir / UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME,
    )
    _write_csv_safe(
        _safe_switch_acquisition_target_frame(packet),
        export_dir / UA_CONTEXT_V13_SAFE_SWITCH_ACQUISITION_TARGETS_CSV_ARTIFACT_NAME,
    )
    _write_csv_safe(
        _source_acquisition_backlog_frame(
            packet,
            source_inventory_frame=source_inventory_frame,
        ),
        export_dir / UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME,
    )
    if receipt_source_audit is not None:
        (export_dir / UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME).write_text(
            json.dumps(_jsonable(dict(receipt_source_audit)), indent=2, sort_keys=True),
            encoding="utf-8",
        )
    if acquisition_input_preflight is not None:
        (
            export_dir / UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME
        ).write_text(
            json.dumps(
                _jsonable(dict(acquisition_input_preflight)),
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )
    (export_dir / UA_CONTEXT_V13_JSON_ARTIFACT_NAME).write_text(
        json.dumps(_jsonable(packet), indent=2, sort_keys=True),
        encoding="utf-8",
    )
    (export_dir / UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME).write_text(
        _packet_markdown(packet),
        encoding="utf-8",
    )
    return export_dir


def _validate_packet_inputs(
    *,
    source_inventory_frame: pl.DataFrame,
    readiness_frame: pl.DataFrame,
    acquisition_source_evidence_frame: pl.DataFrame | None,
    receipt_source_audit: Mapping[str, Any] | None,
    acquisition_input_preflight: Mapping[str, Any] | None,
) -> None:
    _require_columns(
        source_inventory_frame,
        {
            "source_family",
            "source_status",
            "coverage_ratio",
            "required_for_v13_candidate_generation",
            "market_execution_enabled",
        },
        frame_name="V13 source inventory frame",
    )
    _require_columns(
        readiness_frame,
        {
            "tenant_id",
            "source_model_name",
            "v13_candidate_generation_ready",
            "readiness_decision",
            "blocking_context_families",
            "prior_material_safe_switch_example_count",
            "min_prior_material_safe_switch_examples_for_dt",
            "dt_lava_ready",
            "target_label_space",
            "raw_hourly_action_imitation",
            "market_execution_enabled",
        },
        frame_name="V13 readiness frame",
    )
    if acquisition_source_evidence_frame is not None:
        _require_columns(
            acquisition_source_evidence_frame,
            {
                "source_family",
                "source_status",
                "coverage_ratio",
                "required_for_v13_candidate_generation",
                "market_execution_enabled",
            },
            frame_name="V13 acquisition source evidence frame",
        )
    for name, frame in {
        "source inventory": source_inventory_frame,
        "readiness": readiness_frame,
        **(
            {"acquisition source evidence": acquisition_source_evidence_frame}
            if acquisition_source_evidence_frame is not None
            else {}
        ),
    }.items():
        if frame.select(pl.col("market_execution_enabled").any()).item():
            raise ValueError(f"V13 packet refuses {name} market execution rows.")
    if readiness_frame.select(pl.col("raw_hourly_action_imitation").any()).item():
        raise ValueError("V13 packet refuses raw hourly action imitation.")
    if (
        receipt_source_audit is not None
        and bool(receipt_source_audit.get("market_execution_enabled", False))
    ):
        raise ValueError("V13 packet refuses receipt audit market execution rows.")
    if acquisition_input_preflight is not None:
        _validate_acquisition_input_preflight(acquisition_input_preflight)


def _source_inventory_summary(frame: pl.DataFrame) -> dict[str, Any]:
    required = frame.filter(pl.col("required_for_v13_candidate_generation"))
    blocked = required.filter(pl.col("source_status") != "ready_prior_context")
    return {
        "source_family_count": frame.height,
        "required_source_family_count": required.height,
        "blocked_required_source_family_count": blocked.height,
        "blocked_required_sources": sorted(blocked["source_family"].to_list())
        if blocked.height
        else [],
        "source_statuses": sorted(
            str(value) for value in frame["source_status"].unique()
        ),
    }


def _readiness_summary(frame: pl.DataFrame) -> dict[str, Any]:
    ready_rows = frame.filter(pl.col("v13_candidate_generation_ready")).height
    return {
        "readiness_rows": frame.height,
        "ready_rows": ready_rows,
        "blocked_rows": frame.height - ready_rows,
        "v13_candidate_generation_ready": frame.height > 0
        and ready_rows == frame.height,
        "readiness_decisions": sorted(
            str(value) for value in frame["readiness_decision"].unique()
        ),
        "max_prior_material_safe_switch_examples": _safe_int(
            frame["prior_material_safe_switch_example_count"].max()
        )
        if frame.height
        else 0,
        "min_safe_examples_required": _safe_int(
            frame["min_prior_material_safe_switch_examples_for_dt"].max()
        )
        if frame.height
        else 20,
    }


def _safe_switch_deficit_summary(frame: pl.DataFrame) -> dict[str, Any]:
    tenant_source_deficits: list[dict[str, Any]] = []
    total_missing_examples = 0
    max_missing_examples = 0
    for row in frame.iter_rows(named=True):
        prior_examples = _safe_int(row["prior_material_safe_switch_example_count"])
        required_examples = _safe_int(
            row["min_prior_material_safe_switch_examples_for_dt"]
        )
        missing_examples = max(0, required_examples - prior_examples)
        if missing_examples == 0:
            continue
        total_missing_examples += missing_examples
        max_missing_examples = max(max_missing_examples, missing_examples)
        tenant_source_deficits.append(
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "prior_material_safe_switch_example_count": prior_examples,
                "min_prior_material_safe_switch_examples_for_dt": required_examples,
                "missing_prior_material_safe_switch_examples": missing_examples,
                "dt_lava_ready": bool(row["dt_lava_ready"]),
                "readiness_decision": str(row["readiness_decision"]),
            }
        )
    return {
        "blocked_tenant_source_count": len(tenant_source_deficits),
        "max_missing_examples": max_missing_examples,
        "total_missing_examples": total_missing_examples,
        "tenant_source_deficits": sorted(
            tenant_source_deficits,
            key=lambda item: (
                -int(item["missing_prior_material_safe_switch_examples"]),
                str(item["tenant_id"]),
                str(item["source_model_name"]),
            ),
        ),
    }


def _safe_switch_acquisition_target_summary(frame: pl.DataFrame) -> dict[str, Any]:
    target_rows: list[dict[str, Any]] = []
    for row in frame.iter_rows(named=True):
        prior_examples = _safe_int(row["prior_material_safe_switch_example_count"])
        required_examples = _safe_int(
            row["min_prior_material_safe_switch_examples_for_dt"]
        )
        missing_examples = max(0, required_examples - prior_examples)
        if missing_examples == 0:
            continue
        blocking_context_families = str(row["blocking_context_families"])
        target_rows.append(
            {
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "current_prior_material_safe_switch_examples": prior_examples,
                "required_prior_material_safe_switch_examples": required_examples,
                "target_new_prior_material_safe_switch_examples": missing_examples,
                "target_total_prior_material_safe_switch_examples": required_examples,
                "required_evidence_kind": (
                    "train_prior_non_tail_risk_material_safe_switch_rows"
                ),
                "blocking_context_families": blocking_context_families,
                "primary_blocking_source_family": _primary_blocking_source_family(
                    blocking_context_families
                ),
                "recommended_next_step": str(
                    row.get(
                        "recommended_next_step",
                        "acquire_ukrainian_context_and_backfill_safe_labels",
                    )
                ),
                "target_label_space": str(row["target_label_space"]),
                "source_readiness_required_before_dt_lava": True,
                "target_is_precondition_only": True,
                "market_execution_enabled": False,
            }
        )
    sorted_rows = sorted(
        target_rows,
        key=lambda item: (
            -int(item["target_new_prior_material_safe_switch_examples"]),
            str(item["tenant_id"]),
            str(item["source_model_name"]),
        ),
    )
    ranked_rows = [
        {"acquisition_priority_rank": index, **row}
        for index, row in enumerate(sorted_rows, start=1)
    ]
    return {
        "target_tenant_source_count": len(ranked_rows),
        "total_new_prior_material_safe_switch_examples_required": sum(
            int(row["target_new_prior_material_safe_switch_examples"])
            for row in ranked_rows
        ),
        "max_new_prior_material_safe_switch_examples_required": max(
            (
                int(row["target_new_prior_material_safe_switch_examples"])
                for row in ranked_rows
            ),
            default=0,
        ),
        "target_rows": ranked_rows,
    }


def _packet_markdown(packet: dict[str, Any]) -> str:
    readiness = packet["readiness_summary"]
    safe_switch_deficit = packet["safe_switch_deficit_summary"]
    safe_switch_targets = packet["safe_switch_acquisition_target_summary"]
    source_backlog = packet["source_acquisition_backlog_summary"]
    acquisition_input_preflight = packet.get("acquisition_input_preflight_summary")
    receipt_source_audit = packet.get("receipt_source_audit_summary")
    status = (
        "V13 Candidate Generation Ready"
        if packet["v13_candidate_generation_ready"]
        else "Data Acquisition Needed"
    )
    return "\n".join(
        [
            "# V13 Ukrainian Context Acquisition Packet",
            "",
            f"Run slug: `{packet['run_slug']}`",
            f"Dagster run: `{packet.get('dagster_run_id')}`",
            f"Asset check status: `{packet.get('asset_check_status')}`",
            "",
            "## Claim Boundary",
            "",
            "This packet is Offline Strategy Promotion evidence only. It is not "
            "candidate generation, not DT/LAVA training, not live dispatch, and "
            "not market execution. `market_execution_enabled=false`.",
            "",
            f"## {status}",
            "",
            (
                "- Frozen comparator: calibrated Ukrainian-only V2+ mean regret "
                f"`{packet['headline_baseline']['calibrated_v2_plus_mean_regret_uah']}` "
                "UAH, median `67.30` UAH, rolling `4 / 4`."
            ),
            f"- Readiness rows: `{readiness['readiness_rows']}`.",
            f"- Ready rows: `{readiness['ready_rows']}`.",
            f"- Blocked rows: `{readiness['blocked_rows']}`.",
            (
                "- Max prior material safe-switch examples: "
                f"`{readiness['max_prior_material_safe_switch_examples']}` / "
                f"`{readiness['min_safe_examples_required']}` required."
            ),
            (
                "- Readiness decisions: "
                f"`{', '.join(readiness['readiness_decisions'])}`."
            ),
            "",
            "## Safe-Switch Support Deficit",
            "",
            (
                "- Blocked tenant/source pairs: "
                f"`{safe_switch_deficit['blocked_tenant_source_count']}`."
            ),
            (
                "- Missing prior material safe-switch examples: "
                f"`{safe_switch_deficit['total_missing_examples']}` total, "
                f"`{safe_switch_deficit['max_missing_examples']}` max for one pair."
            ),
            *[
                (
                    "- "
                    f"`{item['tenant_id']}` / `{item['source_model_name']}`: "
                    f"`{item['prior_material_safe_switch_example_count']}` / "
                    f"`{item['min_prior_material_safe_switch_examples_for_dt']}` "
                    "prior examples, missing "
                    f"`{item['missing_prior_material_safe_switch_examples']}`."
                )
                for item in safe_switch_deficit["tenant_source_deficits"]
            ],
            "",
            "## Safe-Switch Acquisition Targets",
            "",
            (
                "- New prior material safe-switch examples required: "
                f"`{safe_switch_targets['total_new_prior_material_safe_switch_examples_required']}`."
            ),
            (
                "- Target tenant/source pairs: "
                f"`{safe_switch_targets['target_tenant_source_count']}`."
            ),
            "These targets are source-readiness preconditions only. They do not "
            "permit DT/LAVA training, raw hourly action imitation, or market "
            "execution while any V13 gate remains blocked.",
            *[
                (
                    "- Priority "
                    f"`{item['acquisition_priority_rank']}`: "
                    f"`{item['tenant_id']}` / `{item['source_model_name']}` needs "
                    f"`{item['target_new_prior_material_safe_switch_examples']}` "
                    "new train/prior non-tail-risk material safe-switch examples; "
                    "primary blocker "
                    f"`{item['primary_blocking_source_family']}`."
                )
                for item in safe_switch_targets["target_rows"]
            ],
            "",
            "## Source Acquisition Backlog",
            "",
            (
                "- Backlog items: "
                f"`{source_backlog['backlog_item_count']}` "
                f"(`{source_backlog['source_family_blocker_count']}` source "
                "family blockers, "
                f"`{source_backlog['safe_switch_target_count']}` safe-switch "
                "targets)."
            ),
            (
                "- Top priority blocker: "
                f"`{source_backlog['top_priority_blocker']}`."
            ),
            "The backlog CSV is an acquisition checklist only. It does not "
            "permit model training, DT/LAVA promotion, `ProposedBid` emission, "
            "or market execution.",
            "",
            *_acquisition_input_preflight_markdown(acquisition_input_preflight),
            *_receipt_source_audit_markdown(receipt_source_audit),
        ]
    )


def _safe_switch_acquisition_target_frame(packet: dict[str, Any]) -> pl.DataFrame:
    target_rows = packet["safe_switch_acquisition_target_summary"]["target_rows"]
    if not target_rows:
        return pl.DataFrame(
            schema={
                "acquisition_priority_rank": pl.Int64,
                "tenant_id": pl.String,
                "source_model_name": pl.String,
                "current_prior_material_safe_switch_examples": pl.Int64,
                "required_prior_material_safe_switch_examples": pl.Int64,
                "target_new_prior_material_safe_switch_examples": pl.Int64,
                "target_total_prior_material_safe_switch_examples": pl.Int64,
                "required_evidence_kind": pl.String,
                "blocking_context_families": pl.String,
                "primary_blocking_source_family": pl.String,
                "recommended_next_step": pl.String,
                "target_label_space": pl.String,
                "source_readiness_required_before_dt_lava": pl.Boolean,
                "target_is_precondition_only": pl.Boolean,
                "market_execution_enabled": pl.Boolean,
            }
        )
    return pl.DataFrame(target_rows)


def _source_acquisition_backlog_summary(
    source_inventory_frame: pl.DataFrame,
    safe_switch_acquisition_target_summary: dict[str, Any],
) -> dict[str, Any]:
    frame = _build_source_acquisition_backlog_frame(
        source_inventory_frame=source_inventory_frame,
        safe_switch_acquisition_target_summary=safe_switch_acquisition_target_summary,
    )
    if frame.height == 0:
        top_priority_blocker = "none"
    else:
        top_priority_blocker = str(frame["blocking_source_family"][0])
    return {
        "backlog_item_count": frame.height,
        "source_family_blocker_count": frame.filter(
            pl.col("backlog_item_type") == "source_family_blocker"
        ).height,
        "safe_switch_target_count": frame.filter(
            pl.col("backlog_item_type") == "safe_switch_target"
        ).height,
        "market_execution_enabled": False,
        "permits_model_training": False,
        "top_priority_blocker": top_priority_blocker,
    }


def _source_acquisition_backlog_frame(
    packet: dict[str, Any],
    *,
    source_inventory_frame: pl.DataFrame,
) -> pl.DataFrame:
    return _build_source_acquisition_backlog_frame(
        source_inventory_frame=source_inventory_frame,
        safe_switch_acquisition_target_summary=packet[
            "safe_switch_acquisition_target_summary"
        ],
    )


def _build_source_acquisition_backlog_frame(
    *,
    source_inventory_frame: pl.DataFrame,
    safe_switch_acquisition_target_summary: dict[str, Any],
) -> pl.DataFrame:
    target_rows = safe_switch_acquisition_target_summary["target_rows"]
    primary_blockers = {
        str(row["primary_blocking_source_family"]) for row in target_rows
    }
    top_primary_blocker = (
        str(target_rows[0]["primary_blocking_source_family"])
        if target_rows
        else "none"
    )
    rows: list[dict[str, Any]] = []
    for row in source_inventory_frame.iter_rows(named=True):
        source_family = str(row["source_family"])
        source_status = str(row["source_status"])
        if (
            not bool(row["required_for_v13_candidate_generation"])
            or source_status == "ready_prior_context"
        ):
            continue
        rows.append(
            {
                "sort_stage": 0 if source_family == top_primary_blocker else 2,
                "sort_missing_examples": 0,
                "backlog_item_id": f"source:{source_family}",
                "backlog_item_type": "source_family_blocker",
                "blocking_source_family": source_family,
                "source_status": source_status,
                "coverage_ratio": _safe_float(row.get("coverage_ratio", 0.0)),
                "tenant_id": None,
                "source_model_name": None,
                "current_prior_material_safe_switch_examples": None,
                "required_prior_material_safe_switch_examples": None,
                "target_new_prior_material_safe_switch_examples": None,
                "required_evidence_kind": _required_source_evidence_kind(
                    source_family
                ),
                "acceptance_evidence": _source_acceptance_evidence(source_family),
                "recommended_next_step": _source_recommended_next_step(
                    source_family,
                    source_status,
                ),
                "source_readiness_required_before_dt_lava": (
                    source_family in primary_blockers
                    or source_family == "v12_safe_teacher_label_support"
                ),
                "target_is_precondition_only": True,
                "permits_model_training": False,
                "market_execution_enabled": False,
            }
        )
    for row in target_rows:
        missing_examples = _safe_int(
            row["target_new_prior_material_safe_switch_examples"]
        )
        rows.append(
            {
                "sort_stage": 1,
                "sort_missing_examples": -missing_examples,
                "backlog_item_id": (
                    "safe_switch:"
                    f"{row['tenant_id']}:{row['source_model_name']}"
                ),
                "backlog_item_type": "safe_switch_target",
                "blocking_source_family": str(row["primary_blocking_source_family"]),
                "source_status": "blocked_insufficient_safe_teacher_labels",
                "coverage_ratio": None,
                "tenant_id": str(row["tenant_id"]),
                "source_model_name": str(row["source_model_name"]),
                "current_prior_material_safe_switch_examples": _safe_int(
                    row["current_prior_material_safe_switch_examples"]
                ),
                "required_prior_material_safe_switch_examples": _safe_int(
                    row["required_prior_material_safe_switch_examples"]
                ),
                "target_new_prior_material_safe_switch_examples": missing_examples,
                "required_evidence_kind": str(row["required_evidence_kind"]),
                "acceptance_evidence": (
                    "validated train/prior non-tail-risk material safe-switch "
                    "rows reach the configured tenant/source threshold"
                ),
                "recommended_next_step": str(row["recommended_next_step"]),
                "source_readiness_required_before_dt_lava": True,
                "target_is_precondition_only": True,
                "permits_model_training": False,
                "market_execution_enabled": False,
            }
        )
    if not rows:
        return _empty_source_acquisition_backlog_frame()
    return (
        pl.DataFrame(rows, infer_schema_length=None)
        .sort(
            [
                "sort_stage",
                "sort_missing_examples",
                "blocking_source_family",
                "tenant_id",
                "source_model_name",
            ],
            nulls_last=True,
        )
        .with_row_index("acquisition_priority_rank", offset=1)
        .drop(["sort_stage", "sort_missing_examples"])
    )


def _empty_source_acquisition_backlog_frame() -> pl.DataFrame:
    return pl.DataFrame(
        schema={
            "acquisition_priority_rank": pl.UInt32,
            "backlog_item_id": pl.String,
            "backlog_item_type": pl.String,
            "blocking_source_family": pl.String,
            "source_status": pl.String,
            "coverage_ratio": pl.Float64,
            "tenant_id": pl.String,
            "source_model_name": pl.String,
            "current_prior_material_safe_switch_examples": pl.Int64,
            "required_prior_material_safe_switch_examples": pl.Int64,
            "target_new_prior_material_safe_switch_examples": pl.Int64,
            "required_evidence_kind": pl.String,
            "acceptance_evidence": pl.String,
            "recommended_next_step": pl.String,
            "source_readiness_required_before_dt_lava": pl.Boolean,
            "target_is_precondition_only": pl.Boolean,
            "permits_model_training": pl.Boolean,
            "market_execution_enabled": pl.Boolean,
        }
    )


def _required_source_evidence_kind(source_family: str) -> str:
    evidence_kinds = {
        "explicit_dam_publication_receipts": (
            "source_backed_receipt_csv_with_timestamp_and_publication_timestamp"
        ),
        "measured_or_source_backed_tenant_load_pv": (
            "measured_or_source_backed_tenant_load_pv_rows"
        ),
        "richer_grid_outage_archive": (
            "source_backed_grid_event_or_no_event_archive_rows"
        ),
        "extended_ukrainian_dam_weather_history": (
            "extended_prior_ukrainian_dam_weather_context_rows"
        ),
        "v12_safe_teacher_label_support": (
            "train_prior_non_tail_risk_material_safe_switch_rows"
        ),
    }
    return evidence_kinds.get(source_family, "source_backed_prior_context_rows")


def _source_acceptance_evidence(source_family: str) -> str:
    acceptance_evidence = {
        "explicit_dam_publication_receipts": (
            "validated receipt CSV passes schema, duplicate, missing timestamp, "
            "and market_execution_enabled=false checks"
        ),
        "measured_or_source_backed_tenant_load_pv": (
            "tenant load/PV rows are source-backed or measured and prior to anchors"
        ),
        "richer_grid_outage_archive": (
            "grid event/no-event source window covers anchors and is prior available"
        ),
        "extended_ukrainian_dam_weather_history": (
            "extended Ukrainian DAM/weather rows cover the prior context window"
        ),
        "v12_safe_teacher_label_support": (
            "each tenant/source has at least 20 prior/train non-tail-risk "
            "material safe-switch examples"
        ),
    }
    return acceptance_evidence.get(
        source_family,
        "source family reaches ready_prior_context in the V13 inventory frame",
    )


def _source_recommended_next_step(source_family: str, source_status: str) -> str:
    if source_family == "explicit_dam_publication_receipts":
        return "acquire_validate_and_configure_explicit_dam_receipt_csv"
    if source_family == "v12_safe_teacher_label_support":
        return "backfill_safe_switch_examples_before_dt_lava"
    if source_status == "blocked_missing_source":
        return "acquire_source_backed_prior_context_rows"
    return "complete_source_coverage_to_ready_prior_context"


def _primary_blocking_source_family(blocking_context_families: str) -> str:
    for family_status in blocking_context_families.split(","):
        family_status = family_status.strip()
        if not family_status:
            continue
        return family_status.split(":", maxsplit=1)[0]
    return "unknown"


def _receipt_source_audit_summary(
    receipt_source_audit: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if receipt_source_audit is None:
        return None
    return {
        "all_probes_insufficient_for_v13_receipts": bool(
            receipt_source_audit.get("all_probes_insufficient_for_v13_receipts", False)
        ),
        "candidate_receipt_months": _string_list(
            receipt_source_audit.get("candidate_receipt_months", [])
        ),
        "candidate_receipt_source_found": bool(
            receipt_source_audit.get("candidate_receipt_source_found", False)
        ),
        "claim_scope": str(receipt_source_audit.get("claim_scope", "")),
        "insufficient_months": _string_list(
            receipt_source_audit.get("insufficient_months", [])
        ),
        "market_execution_enabled": False,
        "months_probed": _string_list(receipt_source_audit.get("months_probed", [])),
        "not_market_execution": bool(
            receipt_source_audit.get("not_market_execution", True)
        ),
        "probe_count": _safe_int(receipt_source_audit.get("probe_count", 0)),
        "receipt_csv_generated": bool(
            receipt_source_audit.get("receipt_csv_generated", False)
        ),
    }


def _acquisition_input_preflight_summary(
    acquisition_input_preflight: Mapping[str, Any] | None,
) -> dict[str, Any] | None:
    if acquisition_input_preflight is None:
        return None
    return {
        "claim_boundary": str(acquisition_input_preflight.get("claim_boundary", "")),
        "dam_publication_receipts_status": _section_status(
            acquisition_input_preflight.get("dam_publication_receipts")
        ),
        "data_acquisition_needed": bool(
            acquisition_input_preflight.get("data_acquisition_needed", True)
        ),
        "dt_lava_ready": False,
        "full_v13_gate_evaluated": bool(
            acquisition_input_preflight.get("full_v13_gate_evaluated", False)
        ),
        "market_execution_enabled": False,
        "missing_required_inputs": _string_list(
            acquisition_input_preflight.get("missing_required_inputs", [])
        ),
        "permits_model_training": False,
        "safe_switch_examples_status": _section_status(
            acquisition_input_preflight.get("safe_switch_examples")
        ),
        "v13_candidate_generation_ready": False,
    }


def _validate_acquisition_input_preflight(
    acquisition_input_preflight: Mapping[str, Any],
) -> None:
    unsafe_true_fields = (
        "dt_lava_ready",
        "full_v13_gate_evaluated",
        "market_execution_enabled",
        "permits_model_training",
        "v13_candidate_generation_ready",
    )
    for field_name in unsafe_true_fields:
        if bool(acquisition_input_preflight.get(field_name, False)):
            raise ValueError(
                "V13 packet refuses acquisition input preflight with "
                f"{field_name}=true."
            )


def _section_status(value: object) -> str:
    if isinstance(value, Mapping):
        return str(value.get("status", ""))
    return ""


def _acquisition_input_preflight_markdown(
    acquisition_input_preflight: dict[str, Any] | None,
) -> list[str]:
    if acquisition_input_preflight is None:
        return []
    missing_required_inputs = acquisition_input_preflight["missing_required_inputs"]
    missing_text = ", ".join(missing_required_inputs) if missing_required_inputs else ""
    return [
        "## Acquisition Input Preflight",
        "",
        (
            "- DAM receipt CSV input status: "
            f"`{acquisition_input_preflight['dam_publication_receipts_status']}`."
        ),
        (
            "- Safe-switch CSV input status: "
            f"`{acquisition_input_preflight['safe_switch_examples_status']}`."
        ),
        (
            "- Missing required configured inputs: "
            f"`{missing_text}`."
        ),
        (
            "- Full V13 gate evaluated: "
            f"`{acquisition_input_preflight['full_v13_gate_evaluated']}`."
        ),
        "- The preflight is config/input evidence only; it does not permit "
        "candidate generation, DT/LAVA training, or market execution.",
        "",
    ]


def _receipt_source_audit_markdown(
    receipt_source_audit: dict[str, Any] | None,
) -> list[str]:
    if receipt_source_audit is None:
        return []
    return [
        "## Receipt Source Audit",
        "",
        (
            "- OREE receipt source found: "
            f"`{receipt_source_audit['candidate_receipt_source_found']}`."
        ),
        (
            "- Probed months: "
            f"`{', '.join(receipt_source_audit['months_probed'])}`."
        ),
        (
            "- All probes insufficient for V13 receipts: "
            f"`{receipt_source_audit['all_probes_insufficient_for_v13_receipts']}`."
        ),
        "- The audit is negative source evidence only; it does not generate "
        "receipt rows or change `market_execution_enabled=false`.",
        "",
    ]


def _string_list(value: object) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value]
    return []


def _require_columns(
    frame: pl.DataFrame,
    required_columns: set[str],
    *,
    frame_name: str,
) -> None:
    missing_columns = required_columns.difference(frame.columns)
    if missing_columns:
        raise ValueError(
            f"{frame_name} is missing required columns: {sorted(missing_columns)}"
        )


def _write_csv_safe(frame: pl.DataFrame, path: Path) -> None:
    _csv_safe_frame(frame).write_csv(path)


def _csv_safe_frame(frame: pl.DataFrame) -> pl.DataFrame:
    expressions: list[pl.Expr] = []
    for column_name, dtype in zip(frame.columns, frame.dtypes, strict=True):
        if str(dtype).startswith(("List", "Array", "Struct")):
            expressions.append(
                pl.col(column_name)
                .map_elements(_json_string, return_dtype=pl.String)
                .alias(column_name)
            )
        else:
            expressions.append(pl.col(column_name))
    return frame.select(expressions)


def _json_string(value: object) -> str | None:
    if value is None:
        return None
    return json.dumps(_jsonable(value), sort_keys=True)


def _safe_int(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (int, float, str)):
        return int(value)
    raise TypeError(f"Cannot convert {type(value).__name__} to int.")


def _safe_float(value: object) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float, str)):
        return float(value)
    raise TypeError(f"Cannot convert {type(value).__name__} to float.")


def _jsonable(value: Any) -> Any:
    if isinstance(value, pl.Series):
        return [_jsonable(item) for item in value.to_list()]
    if isinstance(value, dict):
        return {str(key): _jsonable(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_jsonable(item) for item in value]
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return value


__all__ = [
    "UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME",
    "UA_CONTEXT_V13_JSON_ARTIFACT_NAME",
    "UA_CONTEXT_V13_MARKDOWN_ARTIFACT_NAME",
    "UA_CONTEXT_V13_READINESS_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME",
    "UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_V13_SOURCE_EVIDENCE_CSV_ARTIFACT_NAME",
    "UA_CONTEXT_V13_SOURCE_INVENTORY_CSV_ARTIFACT_NAME",
    "build_dfl_ua_context_v13_acquisition_packet",
    "write_dfl_ua_context_v13_acquisition_packet",
]
