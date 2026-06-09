"""DT/LAVA prototype readiness packet.

This module separates three decisions that are easy to conflate:

* whether a CI-fast LAVA smoke can run from a real candidate-frame artifact;
* whether V13 source readiness permits DT/LAVA model training;
* whether any market-execution gate has passed.

The current packet is intentionally a gate report, not a trainer and not an
execution contract.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
import hashlib
import json
from pathlib import Path
import pickle
from typing import Any, Final

CLAIM_SCOPE: Final[str] = "dt_lava_prototype_readiness_not_market_execution"
SUMMARY_JSON_NAME: Final[str] = "dt_lava_prototype_readiness_summary.json"
SUMMARY_MARKDOWN_NAME: Final[str] = "dt_lava_prototype_readiness_summary.md"


def build_dt_lava_prototype_readiness_summary(
    *,
    v13_acquisition_summary: Mapping[str, Any] | None,
    candidate_frame_pickle_path: str | Path | None = None,
    lava_npz_smoke_packet_validation: Mapping[str, Any] | None = None,
    materialization_blockers: Sequence[str] = (),
    offline_strategy_promotion_registry: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build a machine-checkable readiness summary for the DT/LAVA prototype path."""

    resolved_generated_at = generated_at or datetime.now(UTC)
    v13_summary = _summarize_v13(v13_acquisition_summary)
    candidate_summary = _summarize_candidate_frame(candidate_frame_pickle_path)
    lava_validation_summary = _summarize_lava_npz_smoke_validation(
        lava_npz_smoke_packet_validation
    )
    materialization_summary = _summarize_materialization_blockers(
        materialization_blockers
    )
    offline_strategy_summary = _summarize_offline_strategy_promotion(
        offline_strategy_promotion_registry
    )

    blockers: list[dict[str, Any]] = []
    blockers.extend(v13_summary["blockers"])
    blockers.extend(candidate_summary["blockers"])
    blockers.extend(lava_validation_summary["blockers"])
    blockers.extend(materialization_summary["blockers"])
    blockers.extend(offline_strategy_summary["blockers"])

    ci_smoke_ready = bool(
        candidate_summary["ci_smoke_ready"]
        and lava_validation_summary["validation_gate_passed"]
    )
    dt_lava_training_ready = bool(
        ci_smoke_ready
        and v13_summary["v13_candidate_generation_ready"]
        and v13_summary["safe_switch_examples_ready"]
        and not v13_summary["explicit_dam_publication_receipts_blocked"]
        and not materialization_summary["has_materialization_blockers"]
        and not offline_strategy_summary["market_execution_enabled"]
    )
    v13_training_permission_gate_passed = bool(
        v13_summary["v13_candidate_generation_ready"]
        and v13_summary["safe_switch_examples_ready"]
        and not v13_summary["explicit_dam_publication_receipts_blocked"]
        and not v13_summary["summary"]["market_execution_enabled"]
    )
    no_market_execution_safety_gate_passed = bool(
        not v13_summary["summary"]["market_execution_enabled"]
        and not candidate_summary["summary"]["market_execution_enabled"]
        and not offline_strategy_summary["market_execution_enabled"]
    )
    dt_lava_prototype_gate_passed = bool(
        ci_smoke_ready
        and not materialization_summary["has_materialization_blockers"]
        and no_market_execution_safety_gate_passed
    )
    gate_passport = _build_gate_passport(
        offline_strategy_summary=offline_strategy_summary["summary"],
        dt_lava_prototype_gate_passed=dt_lava_prototype_gate_passed,
        ci_smoke_ready=ci_smoke_ready,
        lava_npz_smoke_validation_summary=lava_validation_summary["summary"],
        v13_training_permission_gate_passed=v13_training_permission_gate_passed,
        dt_lava_training_ready=dt_lava_training_ready,
        no_market_execution_safety_gate_passed=no_market_execution_safety_gate_passed,
    )

    return {
        "claim_scope": CLAIM_SCOPE,
        "generated_at": resolved_generated_at.isoformat(),
        "v13": v13_summary["summary"],
        "candidate_frame": candidate_summary["summary"],
        "lava_npz_smoke_validation": lava_validation_summary["summary"],
        "materialization_inputs": materialization_summary["summary"],
        "offline_strategy_promotion": offline_strategy_summary["summary"],
        "blockers": blockers,
        "ci_smoke_ready": ci_smoke_ready,
        "dt_lava_prototype_gate_passed": dt_lava_prototype_gate_passed,
        "dt_lava_training_ready": dt_lava_training_ready,
        "permits_model_training": dt_lava_training_ready,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "no_market_execution_safety_gate_passed": (
            no_market_execution_safety_gate_passed
        ),
        "market_execution_enabled": False,
        "gate_passport": gate_passport,
        "next_gate": _next_gate(
            ci_smoke_ready=ci_smoke_ready,
            dt_lava_training_ready=dt_lava_training_ready,
            blockers=blockers,
        ),
    }


def write_dt_lava_prototype_readiness_packet(
    *,
    output_dir: str | Path,
    v13_acquisition_summary: Mapping[str, Any] | None,
    candidate_frame_pickle_path: str | Path | None = None,
    lava_npz_smoke_packet_validation: Mapping[str, Any] | None = None,
    materialization_blockers: Sequence[str] = (),
    offline_strategy_promotion_registry: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, str]:
    """Write JSON and Markdown readiness artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary = build_dt_lava_prototype_readiness_summary(
        v13_acquisition_summary=v13_acquisition_summary,
        candidate_frame_pickle_path=candidate_frame_pickle_path,
        lava_npz_smoke_packet_validation=lava_npz_smoke_packet_validation,
        materialization_blockers=materialization_blockers,
        offline_strategy_promotion_registry=offline_strategy_promotion_registry,
        generated_at=generated_at,
    )
    summary_json_path = output_path / SUMMARY_JSON_NAME
    summary_markdown_path = output_path / SUMMARY_MARKDOWN_NAME
    summary_json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(
        _render_markdown(summary),
        encoding="utf-8",
    )
    return {
        "summary_json": str(summary_json_path),
        "summary_markdown": str(summary_markdown_path),
    }


def _summarize_v13(
    v13_acquisition_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if v13_acquisition_summary is None:
        summary: dict[str, Any] = {
            "configured": False,
            "v13_candidate_generation_ready": False,
            "ready_rows": 0,
            "readiness_rows": 0,
            "blocked_rows": 0,
            "max_prior_material_safe_switch_examples": 0,
            "min_safe_examples_required": 20,
            "total_missing_safe_switch_examples": None,
            "blocked_required_sources": [],
            "market_execution_enabled": False,
        }
        return {
            "summary": summary,
            "v13_candidate_generation_ready": False,
            "safe_switch_examples_ready": False,
            "explicit_dam_publication_receipts_blocked": True,
            "blockers": [
                _blocker(
                    "v13_summary_missing",
                    "V13 acquisition summary is required before DT/LAVA promotion.",
                    "Attach the current dfl_ua_context_v13_acquisition_summary.json.",
                )
            ],
        }

    readiness = _mapping(v13_acquisition_summary.get("readiness_summary"))
    source_inventory = _mapping(
        v13_acquisition_summary.get("source_inventory_summary")
    )
    source_evidence = _mapping(
        v13_acquisition_summary.get("acquisition_source_evidence_summary")
    )
    safe_switch = _mapping(
        v13_acquisition_summary.get("safe_switch_deficit_summary")
    )
    claim_boundary = _mapping(v13_acquisition_summary.get("claim_boundary"))

    blocked_required_sources = sorted(
        {
            str(source)
            for source in (
                _sequence(source_inventory.get("blocked_required_sources"))
                + _sequence(source_evidence.get("blocked_required_sources"))
            )
        }
    )
    v13_ready = bool(
        v13_acquisition_summary.get(
            "v13_candidate_generation_ready",
            readiness.get("v13_candidate_generation_ready", False),
        )
    )
    min_safe = _int_or_default(readiness.get("min_safe_examples_required"), 20)
    max_safe = _int_or_default(
        readiness.get("max_prior_material_safe_switch_examples"),
        0,
    )
    total_missing = safe_switch.get("total_missing_examples")
    explicit_receipts_blocked = "explicit_dam_publication_receipts" in (
        blocked_required_sources
    )
    safe_switch_ready = max_safe >= min_safe and _int_or_default(total_missing, 0) == 0
    market_execution_enabled = bool(claim_boundary.get("market_execution_enabled", False))

    summary = {
        "configured": True,
        "v13_candidate_generation_ready": v13_ready,
        "ready_rows": _int_or_default(readiness.get("ready_rows"), 0),
        "readiness_rows": _int_or_default(readiness.get("readiness_rows"), 0),
        "blocked_rows": _int_or_default(readiness.get("blocked_rows"), 0),
        "max_prior_material_safe_switch_examples": max_safe,
        "min_safe_examples_required": min_safe,
        "total_missing_safe_switch_examples": (
            None if total_missing is None else _int_or_default(total_missing, 0)
        ),
        "blocked_required_sources": blocked_required_sources,
        "market_execution_enabled": market_execution_enabled,
    }
    blockers: list[dict[str, Any]] = []
    if market_execution_enabled:
        blockers.append(
            _blocker(
                "v13_market_execution_claim",
                "V13 summary must keep market_execution_enabled=false.",
                "Regenerate the V13 packet with the non-execution claim boundary.",
                severity="critical",
            )
        )
    if not v13_ready:
        blockers.append(
            _blocker(
                "v13_candidate_generation_not_ready",
                "V13 source readiness does not yet permit candidate generation.",
                "Close required source-family blockers before DT/LAVA training.",
            )
        )
    if explicit_receipts_blocked:
        blockers.append(
            _blocker(
                "explicit_dam_publication_receipts_blocked",
                "Explicit row-level DAM publication receipts are still blocked.",
                "Provide a source-backed OREE DAM receipt CSV and rerun V13.",
            )
        )
    if not safe_switch_ready:
        blockers.append(
            _blocker(
                "safe_switch_examples_short",
                f"Safe-switch evidence is below the V13 floor ({max_safe}/{min_safe}).",
                "Backfill source-backed train_selection non-tail-risk safe-switch rows.",
            )
        )

    return {
        "summary": summary,
        "v13_candidate_generation_ready": v13_ready,
        "safe_switch_examples_ready": safe_switch_ready,
        "explicit_dam_publication_receipts_blocked": explicit_receipts_blocked,
        "blockers": blockers,
    }


def _summarize_candidate_frame(path_value: str | Path | None) -> dict[str, Any]:
    if path_value is None or not str(path_value).strip():
        return {
            "summary": {
                "configured": False,
                "path": "",
                "exists": False,
                "row_count": 0,
                "train_selection_eligible_rows": 0,
                "npz_instance_count": 0,
                "npz_valid_neighbor_count": 0,
                "market_execution_enabled": False,
            },
            "ci_smoke_ready": False,
            "blockers": [
                _blocker(
                    "candidate_frame_pickle_missing",
                    "No dfl_lava_schedule_neighbor_candidate_frame pickle is configured.",
                    "Materialize or export the real LAVA schedule-neighbor candidate frame.",
                )
            ],
        }

    path = Path(path_value)
    if not path.exists():
        return {
            "summary": {
                "configured": True,
                "path": str(path),
                "exists": False,
                "row_count": 0,
                "train_selection_eligible_rows": 0,
                "npz_instance_count": 0,
                "npz_valid_neighbor_count": 0,
                "market_execution_enabled": False,
            },
            "ci_smoke_ready": False,
            "blockers": [
                _blocker(
                    "candidate_frame_pickle_missing",
                    f"Configured candidate-frame pickle does not exist: {path}",
                    "Materialize or export the real LAVA schedule-neighbor candidate frame.",
                )
            ],
        }

    try:
        frame = _load_pickle(path)
        summary = _inspect_candidate_frame(frame)
        summary.update(
            {
                "configured": True,
                "path": str(path),
                "exists": True,
                "artifact_sha256": _sha256(path),
            }
        )
        blockers: list[dict[str, Any]] = []
        if bool(summary["market_execution_enabled"]):
            blockers.append(
                _blocker(
                    "candidate_frame_execution_claim",
                    "Candidate frame contains market_execution_enabled=true.",
                    "Regenerate the candidate frame with the research-only boundary.",
                    severity="critical",
                )
            )
        if int(summary["npz_instance_count"]) < 1:
            blockers.append(
                _blocker(
                    "candidate_frame_no_npz_instances",
                    "Candidate frame has no eligible train anchors with adjacent candidates.",
                    "Materialize a candidate frame with train_selection eligible alternatives.",
                )
            )
        return {
            "summary": summary,
            "ci_smoke_ready": not blockers,
            "blockers": blockers,
        }
    except Exception as exc:
        return {
            "summary": {
                "configured": True,
                "path": str(path),
                "exists": True,
                "artifact_sha256": _sha256(path),
                "row_count": 0,
                "train_selection_eligible_rows": 0,
                "npz_instance_count": 0,
                "npz_valid_neighbor_count": 0,
                "market_execution_enabled": False,
                "error": str(exc),
            },
            "ci_smoke_ready": False,
            "blockers": [
                _blocker(
                    "candidate_frame_invalid",
                    f"Candidate-frame pickle could not be validated: {exc}",
                    "Regenerate the candidate-frame pickle from the tracked Dagster asset.",
                )
            ],
        }


def _inspect_candidate_frame(frame: Any) -> dict[str, Any]:
    from smart_arbitrage.dfl.lava_npz_smoke_contract import (  # noqa: PLC0415
        build_lava_npz_smoke_artifact_arrays_from_candidate_frame,
    )

    arrays = build_lava_npz_smoke_artifact_arrays_from_candidate_frame(
        frame,
        max_instances=8,
        max_neighbors=4,
    )
    height = int(getattr(frame, "height"))
    train_selection_eligible_rows = int(
        frame.filter(
            (frame["split_name"] == "train_selection")
            & (frame["eligible_for_final_selection"])
        ).height
    )
    market_execution_enabled = bool(frame["market_execution_enabled"].any())
    return {
        "row_count": height,
        "train_selection_eligible_rows": train_selection_eligible_rows,
        "npz_instance_count": int(arrays["feature_matrix"].shape[0]),
        "npz_valid_neighbor_count": int(arrays["adjacent_mask"].sum()),
        "market_execution_enabled": market_execution_enabled,
    }


def _summarize_materialization_blockers(
    blockers: Sequence[str],
) -> dict[str, Any]:
    normalized = [str(blocker).strip() for blocker in blockers if str(blocker).strip()]
    return {
        "summary": {
            "has_materialization_blockers": bool(normalized),
            "missing_inputs": normalized,
        },
        "has_materialization_blockers": bool(normalized),
        "blockers": [
            _blocker(
                "materialization_input_missing",
                f"Tracked materialization input is missing: {name}",
                "Materialize the upstream Dagster asset before running the LAVA candidate frame.",
            )
            for name in normalized
        ],
    }


def _summarize_lava_npz_smoke_validation(
    validation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if validation is None:
        return {
            "summary": {
                "configured": False,
                "validation_passed": False,
                "artifact_hashes_valid": None,
                "metrics_valid": None,
                "aggregate_valid": None,
                "npz_contract_valid": None,
                "baseline_comparison_valid": None,
                "baseline_comparison_ready": None,
                "promotion_gate": False,
                "permits_model_training": False,
                "market_execution_enabled": False,
                "claim_scope": "",
            },
            "validation_gate_passed": False,
            "blockers": [
                _blocker(
                    "lava_npz_smoke_validation_missing",
                    "LAVA NPZ margin-smoke packet validation is required for the prototype CI-smoke gate.",
                    "Attach lava_npz_margin_smoke_packet_validation.json before citing DT/LAVA prototype readiness.",
                )
            ],
        }

    claim_scope = str(validation.get("claim_scope", ""))
    artifact_hashes_valid = bool(validation.get("artifact_hashes_valid", False))
    metrics_valid = bool(validation.get("metrics_valid", False))
    aggregate_valid = bool(validation.get("aggregate_valid", False))
    npz_contract_valid = bool(validation.get("npz_contract_valid", False))
    baseline_comparison_valid = bool(
        validation.get("baseline_comparison_valid", False)
    )
    baseline_comparison_ready = bool(
        validation.get("baseline_comparison_ready", False)
    )
    promotion_gate = bool(validation.get("promotion_gate", False))
    permits_model_training = bool(validation.get("permits_model_training", False))
    market_execution_enabled = bool(validation.get("market_execution_enabled", False))
    validation_passed = bool(
        "not_market_execution" in claim_scope
        and artifact_hashes_valid
        and metrics_valid
        and aggregate_valid
        and npz_contract_valid
        and baseline_comparison_valid
        and baseline_comparison_ready
        and not promotion_gate
        and not permits_model_training
        and not market_execution_enabled
    )
    blockers: list[dict[str, Any]] = []
    if not validation_passed:
        blockers.append(
            _blocker(
                "lava_npz_smoke_validation_failed",
                "LAVA NPZ margin-smoke packet validation is not passing.",
                "Regenerate and validate the LAVA NPZ smoke packet before citing the prototype gate.",
            )
        )
    return {
        "summary": {
            "configured": True,
            "validation_passed": validation_passed,
            "artifact_hashes_valid": artifact_hashes_valid,
            "metrics_valid": metrics_valid,
            "aggregate_valid": aggregate_valid,
            "npz_contract_valid": npz_contract_valid,
            "baseline_comparison_valid": baseline_comparison_valid,
            "baseline_comparison_ready": baseline_comparison_ready,
            "baseline_selected_instance_count": _int_or_default(
                validation.get("baseline_selected_instance_count"),
                0,
            ),
            "strict_fallback_anchor_count": _int_or_default(
                validation.get("strict_fallback_anchor_count"),
                0,
            ),
            "v2_plus_anchor_count": _int_or_default(
                validation.get("v2_plus_anchor_count"),
                0,
            ),
            "v13_acquisition_summary_attached": bool(
                validation.get("v13_acquisition_summary_attached", False)
            ),
            "v13_gate_status": str(validation.get("v13_gate_status", "")),
            "promotion_gate": promotion_gate,
            "permits_model_training": permits_model_training,
            "market_execution_enabled": market_execution_enabled,
            "claim_scope": claim_scope,
        },
        "validation_gate_passed": validation_passed,
        "blockers": blockers,
    }


def _summarize_offline_strategy_promotion(
    registry: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if registry is None:
        return {
            "summary": {
                "configured": False,
                "evidence_passed": False,
                "promotion_gate_passed": False,
                "production_promote_count": 0,
                "promoted_source_model_names": [],
                "market_execution_enabled": False,
                "claim_scope": "",
            },
            "market_execution_enabled": False,
            "blockers": [],
        }

    summary = _mapping(registry.get("summary"))
    claim_boundary = _mapping(registry.get("claim_boundary"))
    source_model_rows = [
        _mapping(row) for row in _sequence(registry.get("source_model_rows"))
    ]
    row_market_execution = any(
        bool(row.get("market_execution_enabled", False)) for row in source_model_rows
    )
    market_execution_enabled = bool(
        summary.get("market_execution_enabled", False)
        or claim_boundary.get("market_execution_enabled", False)
        or row_market_execution
    )
    promoted_source_model_names = [
        str(name)
        for name in _sequence(summary.get("promoted_source_model_names"))
        if str(name).strip()
    ]
    production_promote_count = _int_or_default(
        summary.get("production_promote_count"),
        len(
            [
                row
                for row in source_model_rows
                if bool(row.get("production_promote", False))
            ]
        ),
    )
    evidence_passed = bool(summary.get("evidence_passed", False))
    promotion_gate_passed = bool(
        evidence_passed
        and production_promote_count > 0
        and promoted_source_model_names
        and not market_execution_enabled
    )
    blockers: list[dict[str, Any]] = []
    if market_execution_enabled:
        blockers.append(
            _blocker(
                "offline_strategy_promotion_execution_claim",
                "Upstream offline strategy promotion registry enables market execution.",
                "Regenerate the registry with market_execution_enabled=false.",
                severity="critical",
            )
        )
    return {
        "summary": {
            "configured": True,
            "evidence_passed": evidence_passed,
            "promotion_gate_passed": promotion_gate_passed,
            "production_promote_count": production_promote_count,
            "promoted_source_model_names": promoted_source_model_names,
            "market_execution_enabled": market_execution_enabled,
            "claim_scope": str(claim_boundary.get("claim_scope", "")),
            "source_model_row_count": len(source_model_rows),
        },
        "market_execution_enabled": market_execution_enabled,
        "blockers": blockers,
    }


def _build_gate_passport(
    *,
    offline_strategy_summary: Mapping[str, Any],
    dt_lava_prototype_gate_passed: bool,
    ci_smoke_ready: bool,
    lava_npz_smoke_validation_summary: Mapping[str, Any],
    v13_training_permission_gate_passed: bool,
    dt_lava_training_ready: bool,
    no_market_execution_safety_gate_passed: bool,
) -> dict[str, dict[str, Any]]:
    offline_configured = bool(offline_strategy_summary.get("configured", False))
    offline_promotion_passed = bool(
        offline_strategy_summary.get("promotion_gate_passed", False)
    )
    return {
        "upstream_offline_strategy_promotion_gate": {
            "passed": offline_promotion_passed,
            "status": _passed_status(
                passed=offline_promotion_passed,
                blocked_status="blocked" if offline_configured else "not_configured",
            ),
            "claim_scope": str(offline_strategy_summary.get("claim_scope", "")),
            "market_execution_enabled": bool(
                offline_strategy_summary.get("market_execution_enabled", False)
            ),
        },
        "lava_npz_smoke_packet_validation_gate": {
            "passed": bool(
                lava_npz_smoke_validation_summary.get("validation_passed", True)
            ),
            "status": _passed_status(
                passed=bool(
                    lava_npz_smoke_validation_summary.get("validation_passed", True)
                ),
                blocked_status=(
                    "blocked"
                    if bool(lava_npz_smoke_validation_summary.get("configured", False))
                    else "not_configured"
                ),
            ),
            "claim_scope": str(
                lava_npz_smoke_validation_summary.get("claim_scope", "")
            ),
            "market_execution_enabled": bool(
                lava_npz_smoke_validation_summary.get("market_execution_enabled", False)
            ),
        },
        "dt_lava_prototype_ci_smoke_gate": {
            "passed": dt_lava_prototype_gate_passed,
            "status": _passed_status(
                passed=dt_lava_prototype_gate_passed,
                blocked_status="blocked",
            ),
            "ci_smoke_ready": ci_smoke_ready,
            "claim_scope": "ci_smoke_only_not_promotion",
            "market_execution_enabled": False,
        },
        "v13_training_permission_gate": {
            "passed": v13_training_permission_gate_passed,
            "status": _passed_status(
                passed=v13_training_permission_gate_passed,
                blocked_status="blocked",
            ),
            "claim_scope": "source_readiness_for_dt_lava_training",
            "market_execution_enabled": False,
        },
        "dt_lava_training_promotion_gate": {
            "passed": False,
            "status": "ready_to_run" if dt_lava_training_ready else "not_run",
            "claim_scope": "dt_lava_strict_lp_offline_promotion",
            "market_execution_enabled": False,
        },
        "no_market_execution_safety_gate": {
            "passed": no_market_execution_safety_gate_passed,
            "status": _passed_status(
                passed=no_market_execution_safety_gate_passed,
                blocked_status="blocked",
            ),
            "claim_scope": "prove_non_execution_boundary",
            "market_execution_enabled": False,
        },
        "market_execution_gate": {
            "passed": False,
            "status": "out_of_scope",
            "claim_scope": "future_market_execution_contract",
            "market_execution_enabled": False,
        },
    }


def _passed_status(*, passed: bool, blocked_status: str) -> str:
    return "passed" if passed else blocked_status


def _render_markdown(summary: Mapping[str, Any]) -> str:
    blockers = _sequence(summary.get("blockers"))
    offline_strategy = _mapping(summary.get("offline_strategy_promotion"))
    gate_passport = _mapping(summary.get("gate_passport"))
    lines = [
        "# DT/LAVA Prototype Readiness",
        "",
        f"- Claim scope: `{summary['claim_scope']}`.",
        "- Upstream offline strategy promotion passed: "
        f"`{offline_strategy.get('promotion_gate_passed', False)}`.",
        f"- CI smoke ready: `{summary['ci_smoke_ready']}`.",
        "- DT/LAVA prototype gate passed: "
        f"`{summary['dt_lava_prototype_gate_passed']}`.",
        f"- DT/LAVA training ready: `{summary['dt_lava_training_ready']}`.",
        f"- Promotion gate passed: `{summary['promotion_gate_passed']}`.",
        f"- Market execution gate passed: `{summary['market_execution_gate_passed']}`.",
        "- No-market-execution safety gate passed: "
        f"`{summary['no_market_execution_safety_gate_passed']}`.",
        "- Boundary: `market_execution_enabled=false`.",
        f"- Next gate: `{summary['next_gate']}`.",
        "",
        "## Gate Passport",
    ]
    for gate_name, gate_value in gate_passport.items():
        gate = _mapping(gate_value)
        lines.append(
            "- "
            f"`{gate_name}`: status=`{gate.get('status')}`, "
            f"passed=`{gate.get('passed')}`"
        )
    lines.extend(
        [
            "",
            "## Blockers",
        ]
    )
    if blockers:
        for blocker in blockers:
            blocker_mapping = _mapping(blocker)
            lines.append(
                "- "
                f"`{blocker_mapping.get('code')}`: "
                f"{blocker_mapping.get('message')} "
                f"Next: {blocker_mapping.get('next_step')}"
            )
    else:
        lines.append("- None for CI smoke readiness; promotion/execution still require separate gates.")
    lines.append("")
    return "\n".join(lines)


def _next_gate(
    *,
    ci_smoke_ready: bool,
    dt_lava_training_ready: bool,
    blockers: Sequence[Mapping[str, Any]],
) -> str:
    blocker_codes = {str(blocker.get("code")) for blocker in blockers}
    if "candidate_frame_pickle_missing" in blocker_codes:
        return "materialize_lava_schedule_neighbor_candidate_frame"
    if not ci_smoke_ready:
        return "fix_lava_candidate_frame_for_npz_smoke"
    if not dt_lava_training_ready:
        return "close_v13_source_readiness_before_training"
    return "run_dt_lava_offline_training_and_strict_lp_promotion_gate"


def _load_pickle(path: Path) -> Any:
    with path.open("rb") as file:
        return pickle.load(file)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as file:
        for chunk in iter(lambda: file.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _blocker(
    code: str,
    message: str,
    next_step: str,
    *,
    severity: str = "blocker",
) -> dict[str, str]:
    return {
        "code": code,
        "severity": severity,
        "message": message,
        "next_step": next_step,
    }


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list | tuple) else []


def _int_or_default(value: Any, default: int) -> int:
    if isinstance(value, bool):
        return default
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    return default


__all__ = [
    "CLAIM_SCOPE",
    "SUMMARY_JSON_NAME",
    "SUMMARY_MARKDOWN_NAME",
    "build_dt_lava_prototype_readiness_summary",
    "write_dt_lava_prototype_readiness_packet",
]
