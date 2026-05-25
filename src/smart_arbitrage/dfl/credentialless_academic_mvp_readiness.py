"""Credentialless academic MVP readiness packet.

This packet deliberately separates the diploma MVP from market-submission grade
source readiness. SCMO credentials may still be required for market-submittable
receipt proof, but they are not required to prove the operator-preview and
offline DT/LAVA prototype boundaries.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
import json
from pathlib import Path
from typing import Any, Final

CLAIM_SCOPE: Final[str] = "credentialless_academic_mvp_readiness_not_market_execution"
VALIDATION_CLAIM_SCOPE: Final[str] = (
    "credentialless_academic_mvp_readiness_validation_not_market_execution"
)
SUMMARY_JSON_NAME: Final[str] = "credentialless_academic_mvp_readiness_summary.json"
SUMMARY_MARKDOWN_NAME: Final[str] = "credentialless_academic_mvp_readiness_summary.md"
VALIDATION_JSON_NAME: Final[str] = (
    "credentialless_academic_mvp_readiness_validation.json"
)
FORBIDDEN_MARKET_PAYLOAD_KEYS: Final[frozenset[str]] = frozenset(
    {
        "proposed_bid",
        "proposedBid",
        "market_order_payload",
        "marketOrderPayload",
        "order_payload",
        "cleared_trade",
        "clearedTrade",
        "dispatch_command",
        "dispatchCommand",
    }
)
ALLOWED_DT_ACTION_TARGETS: Final[frozenset[str]] = frozenset(
    {
        "candidate_id",
        "candidate_index",
        "schedule_family",
        "schedule_block",
        "candidate_id_or_schedule_family",
        "candidate_id_or_schedule_block",
        "candidate_index_or_schedule_family",
        "candidate_index_or_schedule_block",
    }
)
REQUIRED_V2_PLUS_ROLE: Final[str] = "teacher_comparator_fallback"
REQUIRED_PASSED_PASSPORT_GATES: Final[tuple[str, ...]] = (
    "operator_preview_gate",
    "dam_bid_recommendation_preview_gate",
    "academic_source_governance_gate",
    "dt_lava_prototype_ci_smoke_gate",
    "lava_npz_smoke_packet_validation_gate",
    "dfl_dt_prototype_contract_gate",
    "v13_gated_teacher_contract_gate",
    "offline_challenger_non_promotion_gate",
    "prototype_evidence_scorecard_gate",
    "market_execution_safety_gate",
)
NON_REQUIRED_BLOCKED_PASSPORT_GATES: Final[tuple[str, ...]] = (
    "market_submission_receipt_gate",
    "dt_lava_training_promotion_gate",
    "market_execution_gate",
)
REQUIRED_TOP_LEVEL_FLAGS: Final[Mapping[str, bool]] = {
    "academic_mvp_gate_passed": True,
    "market_submission_ready": False,
    "market_execution_gate_passed": False,
    "promotion_gate_passed": False,
    "permits_model_training": False,
    "market_execution_enabled": False,
    "no_market_execution_safety_gate_passed": True,
}


def build_credentialless_academic_mvp_readiness_summary(
    *,
    operator_preview: Mapping[str, Any] | None,
    v13_acquisition_summary: Mapping[str, Any] | None,
    dt_lava_prototype_readiness: Mapping[str, Any] | None,
    teacher_summary: Mapping[str, Any] | None,
    teacher_validation: Mapping[str, Any] | None,
    offline_challenger_summary: Mapping[str, Any] | None,
    offline_challenger_validation: Mapping[str, Any] | None,
    dt_research_shadow_sequence_summary: Mapping[str, Any] | None = None,
    dt_research_shadow_smoke_summary: Mapping[str, Any] | None = None,
    dt_research_shadow_evaluation_validation: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the credentialless academic MVP gate summary."""

    operator_gate = _operator_preview_gate(operator_preview)
    source_governance = _source_governance(v13_acquisition_summary)
    prototype_gate = _dt_lava_prototype_gate(dt_lava_prototype_readiness)
    teacher_gate = _teacher_contract_gate(teacher_summary, teacher_validation)
    challenger_gate = _offline_challenger_gate(
        offline_challenger_summary,
        offline_challenger_validation,
    )
    dt_research_shadow_gate = _dt_research_shadow_gate(
        dt_research_shadow_sequence_summary,
        dt_research_shadow_smoke_summary,
        dt_research_shadow_evaluation_validation,
    )
    prototype_contract = _prototype_contract(
        operator_gate=operator_gate,
        prototype_gate=prototype_gate,
        teacher_gate=teacher_gate,
        challenger_gate=challenger_gate,
    )
    prototype_phase_readiness = _prototype_phase_readiness(
        source_governance=source_governance,
        prototype_gate=prototype_gate,
        teacher_gate=teacher_gate,
        challenger_gate=challenger_gate,
    )
    prototype_evidence_scorecard = _prototype_evidence_scorecard(
        operator_gate=operator_gate,
        prototype_gate=prototype_gate,
        teacher_gate=teacher_gate,
        challenger_gate=challenger_gate,
        prototype_contract=prototype_contract,
    )

    recursive_market_execution_true = any(
        _contains_market_execution_enabled_true(payload)
        for payload in (
            operator_preview,
            v13_acquisition_summary,
            dt_lava_prototype_readiness,
            teacher_summary,
            teacher_validation,
            offline_challenger_summary,
            offline_challenger_validation,
            dt_research_shadow_evaluation_validation,
        )
    )
    academic_mvp_gate_passed = bool(
        operator_gate["passed"]
        and source_governance["academic_mvp_source_governance_passed"]
        and prototype_gate["passed_for_academic_mvp"]
        and teacher_gate["passed_for_academic_mvp"]
        and challenger_gate["passed_for_academic_mvp"]
        and dt_research_shadow_gate["passed_for_academic_mvp"]
        and prototype_contract["prototype_contract_gate_passed"]
        and not recursive_market_execution_true
    )
    no_market_execution_safety_gate_passed = not recursive_market_execution_true
    gate_passport = _academic_mvp_gate_passport(
        operator_gate=operator_gate,
        source_governance=source_governance,
        prototype_gate=prototype_gate,
        teacher_gate=teacher_gate,
        challenger_gate=challenger_gate,
        prototype_contract=prototype_contract,
        prototype_evidence_scorecard=prototype_evidence_scorecard,
        dt_research_shadow_gate=dt_research_shadow_gate,
        no_market_execution_safety_gate_passed=no_market_execution_safety_gate_passed,
    )

    return {
        "claim_scope": CLAIM_SCOPE,
        "generated_at": (generated_at or datetime.now(UTC)).isoformat(),
        "academic_mvp_gate_passed": academic_mvp_gate_passed,
        "operator_preview_gate": operator_gate,
        "source_governance": source_governance,
        "dt_lava_prototype_gate": prototype_gate,
        "dt_lava_teacher_contract_gate": teacher_gate,
        "offline_challenger_gate": challenger_gate,
        "dt_research_shadow_gate": dt_research_shadow_gate,
        "prototype_contract": prototype_contract,
        "prototype_evidence_scorecard": prototype_evidence_scorecard,
        "prototype_phase_readiness": prototype_phase_readiness,
        "gate_passport": gate_passport,
        "market_submission_ready": False,
        "market_execution_gate_passed": False,
        "promotion_gate_passed": False,
        "permits_model_training": False,
        "market_execution_enabled": False,
        "no_market_execution_safety_gate_passed": no_market_execution_safety_gate_passed,
        "next_gate": _next_gate(
            academic_mvp_gate_passed=academic_mvp_gate_passed,
            source_governance=source_governance,
            prototype_gate=prototype_gate,
            teacher_gate=teacher_gate,
            challenger_gate=challenger_gate,
        ),
    }


def write_credentialless_academic_mvp_readiness_packet(
    *,
    output_dir: str | Path,
    operator_preview: Mapping[str, Any] | None,
    v13_acquisition_summary: Mapping[str, Any] | None,
    dt_lava_prototype_readiness: Mapping[str, Any] | None,
    teacher_summary: Mapping[str, Any] | None,
    teacher_validation: Mapping[str, Any] | None,
    offline_challenger_summary: Mapping[str, Any] | None,
    offline_challenger_validation: Mapping[str, Any] | None,
    dt_research_shadow_sequence_summary: Mapping[str, Any] | None = None,
    dt_research_shadow_smoke_summary: Mapping[str, Any] | None = None,
    dt_research_shadow_evaluation_validation: Mapping[str, Any] | None = None,
    generated_at: datetime | None = None,
) -> dict[str, str]:
    """Write credentialless academic MVP JSON and Markdown artifacts."""

    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    summary = build_credentialless_academic_mvp_readiness_summary(
        operator_preview=operator_preview,
        v13_acquisition_summary=v13_acquisition_summary,
        dt_lava_prototype_readiness=dt_lava_prototype_readiness,
        teacher_summary=teacher_summary,
        teacher_validation=teacher_validation,
        offline_challenger_summary=offline_challenger_summary,
        offline_challenger_validation=offline_challenger_validation,
        dt_research_shadow_sequence_summary=dt_research_shadow_sequence_summary,
        dt_research_shadow_smoke_summary=dt_research_shadow_smoke_summary,
        dt_research_shadow_evaluation_validation=(
            dt_research_shadow_evaluation_validation
        ),
        generated_at=generated_at,
    )
    summary_json_path = output_path / SUMMARY_JSON_NAME
    summary_markdown_path = output_path / SUMMARY_MARKDOWN_NAME
    validation_json_path = output_path / VALIDATION_JSON_NAME
    validation = validate_credentialless_academic_mvp_readiness_summary(summary)
    summary_json_path.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary_markdown_path.write_text(_render_markdown(summary), encoding="utf-8")
    validation_json_path.write_text(
        json.dumps(validation, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return {
        "summary_json": str(summary_json_path),
        "summary_markdown": str(summary_markdown_path),
        "validation_json": str(validation_json_path),
    }


def validate_credentialless_academic_mvp_readiness_summary(
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate a credentialless academic MVP packet without FastAPI."""

    failures: list[str] = []
    gate_results: dict[str, dict[str, Any]] = {}
    if summary.get("claim_scope") != CLAIM_SCOPE:
        failures.append("invalid_claim_scope")
    for flag_name, expected_value in REQUIRED_TOP_LEVEL_FLAGS.items():
        if summary.get(flag_name) is not expected_value:
            failures.append(f"invalid_top_level_flag:{flag_name}")

    if _contains_market_execution_enabled_true(summary):
        failures.append("nested_market_execution_enabled_true")

    academic_mvp_gate_failures = (
        [] if summary.get("academic_mvp_gate_passed") is True else ["academic_mvp_gate_not_passed"]
    )
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="academic_mvp_gate",
        gate_failures=academic_mvp_gate_failures,
    )

    gate_passport = _mapping(summary.get("gate_passport"))
    for gate_name in REQUIRED_PASSED_PASSPORT_GATES:
        gate = _mapping(gate_passport.get(gate_name))
        gate_failures: list[str] = []
        if not gate:
            gate_failures.append("missing_gate")
        elif gate.get("passed") is not True:
            gate_failures.append("required_gate_not_passed")
        _add_validation_gate_result(
            gate_results=gate_results,
            failures=failures,
            gate_name=gate_name,
            gate_failures=gate_failures,
        )

    for gate_name in NON_REQUIRED_BLOCKED_PASSPORT_GATES:
        gate = _mapping(gate_passport.get(gate_name))
        gate_failures = []
        if not gate:
            gate_failures.append("missing_gate")
        else:
            if gate.get("required_for_academic_mvp") is not False:
                gate_failures.append("future_gate_required_for_academic_mvp")
            if gate.get("passed") is True:
                gate_failures.append(
                    f"future_gate_passed_for_credentialless_scope:{gate_name}"
                )
        _add_validation_gate_result(
            gate_results=gate_results,
            failures=failures,
            gate_name=gate_name,
            gate_failures=gate_failures,
        )

    prototype_contract = _prototype_contract_validation(summary)
    prototype_contract_failures = [
        failure
        for failure in _sequence(prototype_contract.get("failures"))
        if str(failure).strip()
    ]
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="prototype_contract",
        gate_failures=[str(failure) for failure in prototype_contract_failures],
    )

    phase_readiness = _phase_readiness_validation(summary)
    phase_readiness_failures = [
        failure
        for failure in _sequence(phase_readiness.get("failures"))
        if str(failure).strip()
    ]
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="prototype_phase_readiness",
        gate_failures=[str(failure) for failure in phase_readiness_failures],
    )

    prototype_evidence_scorecard = _prototype_evidence_scorecard_validation(summary)
    scorecard_failures = [
        failure
        for failure in _sequence(prototype_evidence_scorecard.get("failures"))
        if str(failure).strip()
    ]
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="prototype_evidence_scorecard",
        gate_failures=[str(failure) for failure in scorecard_failures],
    )

    lava_npz_validation = _mapping(
        _mapping(summary.get("dt_lava_prototype_gate")).get(
            "lava_npz_smoke_validation"
        )
    )
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="lava_npz_smoke_packet_validation",
        gate_failures=_lava_npz_smoke_validation_failures(lava_npz_validation),
    )
    teacher_packet_validation = _mapping(
        _mapping(summary.get("dt_lava_teacher_contract_gate")).get(
            "teacher_packet_validation"
        )
    )
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="teacher_packet_validation",
        gate_failures=_teacher_packet_validation_failures(
            teacher_packet_validation
        ),
    )
    offline_challenger_packet_validation = _mapping(
        _mapping(summary.get("offline_challenger_gate")).get(
            "offline_challenger_packet_validation"
        )
    )
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="offline_challenger_packet_validation",
        gate_failures=_offline_challenger_packet_validation_failures(
            offline_challenger_packet_validation
        ),
    )
    dt_research_shadow_gate = _dt_research_shadow_gate_validation(summary)
    _add_validation_gate_result(
        gate_results=gate_results,
        failures=failures,
        gate_name="dt_research_shadow_gate",
        gate_failures=[
            str(failure)
            for failure in _sequence(dt_research_shadow_gate.get("failures"))
            if str(failure).strip()
        ],
    )

    passed = not failures
    return {
        "claim_scope": VALIDATION_CLAIM_SCOPE,
        "validated_at": datetime.now(UTC).isoformat(),
        "passed": passed,
        "failures": failures,
        "gate_results": gate_results,
        "prototype_contract": {
            key: value
            for key, value in prototype_contract.items()
            if key != "failures"
        },
        "prototype_phase_readiness": {
            key: value
            for key, value in phase_readiness.items()
            if key != "failures"
        },
        "prototype_evidence_scorecard": {
            key: value
            for key, value in prototype_evidence_scorecard.items()
            if key != "failures"
        },
        "dt_research_shadow_gate": {
            key: value
            for key, value in dt_research_shadow_gate.items()
            if key != "failures"
        },
        "market_execution_enabled": False,
    }


def _operator_preview_gate(
    operator_preview: Mapping[str, Any] | None,
) -> dict[str, Any]:
    failures: list[str] = []
    if operator_preview is None:
        return {
            "configured": False,
            "passed": False,
            "failures": ["operator_preview_missing"],
            "market_execution_enabled": False,
        }

    required_values = {
        "market_scope": "dam_hourly_planning_preview",
        "market_venue": "DAM",
        "interval_minutes": 60,
        "read_model_boundary": "operator_preview_no_market_submission",
        "market_gate_status": "not_evaluated_preview_only",
        "bid_eligibility_status": "not_applicable_no_proposed_bid",
        "proposed_bid_status": "not_emitted_operator_preview",
    }
    for key, expected_value in required_values.items():
        if operator_preview.get(key) != expected_value:
            failures.append(f"unexpected_{key}:{operator_preview.get(key)!r}")

    if bool(operator_preview.get("market_execution_enabled", False)):
        failures.append("market_execution_enabled_true")
    forbidden_paths = sorted(
        _forbidden_market_payload_paths(operator_preview)
    )
    failures.extend(f"forbidden_payload_key:{path}" for path in forbidden_paths)
    if _contains_market_execution_enabled_true(operator_preview):
        failures.append("nested_market_execution_enabled_true")

    schedule = _sequence(operator_preview.get("recommendation_schedule"))
    if not schedule:
        failures.append("recommendation_schedule_empty")
    bid_preview = _sequence(operator_preview.get("bid_recommendation_preview"))
    if not bid_preview:
        failures.append("bid_recommendation_preview_empty")
    if schedule and bid_preview and len(schedule) != len(bid_preview):
        failures.append(
            "bid_recommendation_preview_row_count_mismatch:"
            f"{len(bid_preview)}!={len(schedule)}"
        )
    failures.extend(_bid_recommendation_preview_failures(bid_preview))
    bid_preview_summary = _bid_recommendation_preview_summary(
        bid_preview,
        interval_minutes=operator_preview.get("interval_minutes"),
    )

    v13_readiness = _mapping(operator_preview.get("v13_readiness"))
    if bool(v13_readiness.get("market_execution_enabled", False)):
        failures.append("v13_readiness_market_execution_enabled_true")

    return {
        "configured": True,
        "passed": not failures,
        "failures": failures,
        "tenant_id": str(operator_preview.get("tenant_id", "")),
        "selected_strategy_id": str(operator_preview.get("selected_strategy_id", "")),
        "market_scope": operator_preview.get("market_scope"),
        "market_venue": operator_preview.get("market_venue"),
        "interval_minutes": operator_preview.get("interval_minutes"),
        "read_model_boundary": operator_preview.get("read_model_boundary"),
        "market_gate_status": operator_preview.get("market_gate_status"),
        "bid_eligibility_status": operator_preview.get("bid_eligibility_status"),
        "proposed_bid_status": operator_preview.get("proposed_bid_status"),
        "recommendation_schedule_rows": len(schedule),
        "bid_recommendation_preview_rows": len(bid_preview),
        "bid_recommendation_preview_status": "non_submittable_dam_preview",
        "bid_preview_summary": bid_preview_summary,
        "v13_gate_status": v13_readiness.get("gate_status"),
        "v13_top_priority_blocker": v13_readiness.get("top_priority_blocker"),
        "source_governance_status": v13_readiness.get("source_governance_status"),
        "source_governance_label": v13_readiness.get("source_governance_label"),
        "market_submission_receipt_gate_status": v13_readiness.get(
            "market_submission_receipt_gate_status"
        ),
        "market_execution_enabled": False,
    }


def _bid_recommendation_preview_failures(
    bid_preview: list[Any],
) -> list[str]:
    failures: list[str] = []
    for index, raw_point in enumerate(bid_preview):
        point = _mapping(raw_point)
        if not point:
            failures.append(f"bid_recommendation_preview_row_not_mapping:{index}")
            continue
        if point.get("market_venue") != "DAM":
            failures.append(f"unexpected_bid_recommendation_preview_market_venue:{index}")
        if point.get("side") not in {"BUY", "SELL", "HOLD"}:
            failures.append(f"unexpected_bid_recommendation_preview_side:{index}")
        if point.get("operator_action") not in {"charge", "discharge", "hold"}:
            failures.append(f"unexpected_bid_recommendation_preview_operator_action:{index}")
        quantity_mw = _float_or_none(point.get("quantity_mw"))
        if quantity_mw is None or quantity_mw < 0.0:
            failures.append(f"invalid_bid_recommendation_preview_quantity_mw:{index}")
        elif point.get("side") in {"BUY", "SELL"} and quantity_mw <= 0.0:
            failures.append(
                f"zero_bid_recommendation_preview_trade_quantity_mw:{index}"
            )
        if _float_or_none(point.get("indicative_limit_price_uah_mwh")) is None:
            failures.append(
                f"invalid_bid_recommendation_preview_indicative_price:{index}"
            )
        if point.get("preview_only") is not True:
            failures.append(f"bid_recommendation_preview_not_preview_only:{index}")
        if bool(point.get("market_execution_enabled", False)):
            failures.append(f"bid_recommendation_preview_market_execution_enabled_true:{index}")
        if point.get("market_order_payload_emitted") is not False:
            failures.append(f"bid_recommendation_preview_market_order_payload_emitted:{index}")
        if point.get("proposed_bid_status") != "not_emitted_operator_preview":
            failures.append(f"unexpected_bid_recommendation_preview_proposed_bid_status:{index}")
        if point.get("read_model_boundary") != "operator_preview_no_market_submission":
            failures.append(f"unexpected_bid_recommendation_preview_read_model_boundary:{index}")
    return failures


def _bid_recommendation_preview_summary(
    bid_preview: list[Any],
    *,
    interval_minutes: object,
) -> dict[str, Any]:
    interval_hours = _float_or_default(interval_minutes, 60.0) / 60.0
    buy_rows = 0
    sell_rows = 0
    hold_rows = 0
    total_buy_mwh = 0.0
    total_sell_mwh = 0.0
    max_quantity_mw = 0.0
    indicative_buy_notional_uah = 0.0
    indicative_sell_notional_uah = 0.0
    for raw_point in bid_preview:
        point = _mapping(raw_point)
        side = str(point.get("side", ""))
        quantity_mw = _float_or_none(point.get("quantity_mw"))
        price_uah_mwh = _float_or_none(point.get("indicative_limit_price_uah_mwh"))
        if quantity_mw is None:
            quantity_mw = 0.0
        if price_uah_mwh is None:
            price_uah_mwh = 0.0
        quantity_mw = max(quantity_mw, 0.0)
        interval_mwh = quantity_mw * interval_hours
        max_quantity_mw = max(max_quantity_mw, quantity_mw)
        if side == "BUY":
            buy_rows += 1
            total_buy_mwh += interval_mwh
            indicative_buy_notional_uah += interval_mwh * price_uah_mwh
        elif side == "SELL":
            sell_rows += 1
            total_sell_mwh += interval_mwh
            indicative_sell_notional_uah += interval_mwh * price_uah_mwh
        elif side == "HOLD":
            hold_rows += 1
    return {
        "claim_scope": "dam_delivery_day_bid_recommendation_preview_not_market_submission",
        "row_count": len(bid_preview),
        "buy_rows": buy_rows,
        "sell_rows": sell_rows,
        "hold_rows": hold_rows,
        "total_buy_mwh": round(total_buy_mwh, 6),
        "total_sell_mwh": round(total_sell_mwh, 6),
        "max_quantity_mw": round(max_quantity_mw, 6),
        "indicative_buy_notional_uah": round(indicative_buy_notional_uah, 6),
        "indicative_sell_notional_uah": round(indicative_sell_notional_uah, 6),
        "has_buy_or_sell_recommendation": bool(buy_rows or sell_rows),
        "preview_only": True,
        "market_order_payload_emitted": False,
        "proposed_bid_emitted": False,
        "read_model_boundary": "operator_preview_no_market_submission",
        "market_execution_enabled": False,
    }


def _source_governance(
    v13_acquisition_summary: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if v13_acquisition_summary is None:
        return {
            "configured": False,
            "academic_mvp_source_governance_passed": False,
            "v13_explicit_receipts_gate_passed": False,
            "market_submission_receipt_gate_status": "missing_v13_summary",
            "scmo_credentials_required_for_diploma_mvp": False,
            "market_execution_enabled": False,
        }

    readiness = _mapping(v13_acquisition_summary.get("readiness_summary"))
    source_inventory = _mapping(
        v13_acquisition_summary.get("source_inventory_summary")
    )
    source_evidence = _mapping(
        v13_acquisition_summary.get("acquisition_source_evidence_summary")
    )
    blocked_required_sources = sorted(
        {
            str(source)
            for source in (
                _sequence(source_inventory.get("blocked_required_sources"))
                + _sequence(source_evidence.get("blocked_required_sources"))
            )
        }
    )
    explicit_receipts_blocked = "explicit_dam_publication_receipts" in (
        blocked_required_sources
    )
    safe_switch = _mapping(
        v13_acquisition_summary.get("safe_switch_deficit_summary")
    )
    total_missing_safe_switch_examples = _int_or_default(
        safe_switch.get("total_missing_examples"),
        0,
    )
    lead_audit = _mapping(
        v13_acquisition_summary.get("receipt_source_lead_audit_summary")
    )
    receipt_audit = _mapping(
        v13_acquisition_summary.get("receipt_source_audit_summary")
    )
    scmo_preflight = _mapping(
        v13_acquisition_summary.get("scmo_ws_security_preflight_summary")
    )
    auth_blocked_count = _int_or_default(lead_audit.get("auth_blocked_count"), 0)
    credentialless_observation_count = (
        _int_or_default(receipt_audit.get("probe_count"), 0)
        + _int_or_default(lead_audit.get("dataset_level_metadata_only_count"), 0)
        + _int_or_default(lead_audit.get("probe_negative_count"), 0)
    )
    public_credentialless_source_observed = credentialless_observation_count > 0
    credential_ready = bool(scmo_preflight.get("credential_material_ready", False))
    signed_download_ready = bool(
        scmo_preflight.get("signed_download_request_ready", False)
    )
    validated_receipt_csv_ready = bool(
        lead_audit.get("validated_receipt_csv_ready", False)
    )
    receipt_csv_generated = bool(
        lead_audit.get("receipt_csv_generated", False)
        or receipt_audit.get("receipt_csv_generated", False)
    )
    candidate_receipt_source_found = bool(
        lead_audit.get("candidate_receipt_source_found", False)
        or receipt_audit.get("candidate_receipt_source_found", False)
    )
    publication_receipt_verified = bool(
        not explicit_receipts_blocked
        and validated_receipt_csv_ready
        and receipt_csv_generated
    )
    source_publication_timestamp_available = publication_receipt_verified
    market_availability_claim = False
    market_submission_status = "ready"
    if explicit_receipts_blocked and (auth_blocked_count > 0 or not credential_ready):
        market_submission_status = "blocked_external_access"
    elif explicit_receipts_blocked:
        market_submission_status = "blocked_missing_explicit_receipts"

    market_execution_enabled = _contains_market_execution_enabled_true(
        v13_acquisition_summary
    )
    academic_mvp_source_governance_passed = bool(
        not market_execution_enabled
        and total_missing_safe_switch_examples == 0
        and explicit_receipts_blocked
        and public_credentialless_source_observed
        and not publication_receipt_verified
        and not market_availability_claim
    )
    source_governance_evidence_status = (
        "public_credentialless_source_observed_receipt_not_verified"
        if public_credentialless_source_observed and not publication_receipt_verified
        else "credentialless_source_evidence_missing"
    )
    return {
        "configured": True,
        "academic_mvp_source_governance_passed": (
            academic_mvp_source_governance_passed
        ),
        "source_governance_evidence_status": source_governance_evidence_status,
        "public_credentialless_source_observed": public_credentialless_source_observed,
        "credentialless_observation_count": credentialless_observation_count,
        "candidate_receipt_source_found": candidate_receipt_source_found,
        "receipt_csv_generated": receipt_csv_generated,
        "publication_receipt_verified": publication_receipt_verified,
        "source_publication_timestamp_available": source_publication_timestamp_available,
        "market_availability_claim": market_availability_claim,
        "v13_candidate_generation_ready": bool(
            v13_acquisition_summary.get(
                "v13_candidate_generation_ready",
                readiness.get("v13_candidate_generation_ready", False),
            )
        ),
        "v13_explicit_receipts_gate_passed": not explicit_receipts_blocked,
        "blocked_required_sources": blocked_required_sources,
        "total_missing_safe_switch_examples": total_missing_safe_switch_examples,
        "max_prior_material_safe_switch_examples": _int_or_default(
            readiness.get("max_prior_material_safe_switch_examples"),
            0,
        ),
        "min_safe_examples_required": _int_or_default(
            readiness.get("min_safe_examples_required"),
            20,
        ),
        "market_submission_receipt_gate_status": market_submission_status,
        "scmo_credentials_required_for_diploma_mvp": False,
        "scmo_credentials_required_for_market_submission_grade_receipts": (
            explicit_receipts_blocked
        ),
        "scmo_credential_material_ready": credential_ready,
        "scmo_signed_download_request_ready": signed_download_ready,
        "auth_blocked_receipt_leads": auth_blocked_count,
        "validated_receipt_csv_ready": validated_receipt_csv_ready,
        "market_execution_enabled": False,
    }


def _dt_lava_prototype_gate(
    dt_lava_prototype_readiness: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if dt_lava_prototype_readiness is None:
        return {
            "configured": False,
            "passed_for_academic_mvp": False,
            "failures": ["dt_lava_prototype_readiness_missing"],
            "market_execution_enabled": False,
        }
    gate_passport = _mapping(dt_lava_prototype_readiness.get("gate_passport"))
    lava_gate = _mapping(gate_passport.get("lava_npz_smoke_packet_validation_gate"))
    no_execution_gate = _mapping(gate_passport.get("no_market_execution_safety_gate"))
    lava_validation = _lava_npz_smoke_validation_summary(
        dt_lava_prototype_readiness.get("lava_npz_smoke_validation")
    )
    failures: list[str] = []
    if not bool(dt_lava_prototype_readiness.get("ci_smoke_ready", False)):
        failures.append("ci_smoke_not_ready")
    if not bool(dt_lava_prototype_readiness.get("dt_lava_prototype_gate_passed", False)):
        failures.append("dt_lava_prototype_gate_not_passed")
    if not bool(lava_gate.get("passed", False)):
        failures.append("lava_npz_smoke_validation_not_passed")
    failures.extend(_lava_npz_smoke_validation_failures(lava_validation))
    if not bool(no_execution_gate.get("passed", False)):
        failures.append("no_market_execution_safety_gate_not_passed")
    if _contains_market_execution_enabled_true(dt_lava_prototype_readiness):
        failures.append("market_execution_enabled_true")

    return {
        "configured": True,
        "passed_for_academic_mvp": not failures,
        "failures": failures,
        "ci_smoke_ready": bool(dt_lava_prototype_readiness.get("ci_smoke_ready", False)),
        "dt_lava_prototype_gate_passed": bool(
            dt_lava_prototype_readiness.get("dt_lava_prototype_gate_passed", False)
        ),
        "lava_npz_smoke_packet_validation_passed": bool(
            lava_gate.get("passed", False)
            and lava_validation.get("validation_passed", False)
        ),
        "lava_npz_smoke_validation": lava_validation,
        "dt_lava_training_ready": bool(
            dt_lava_prototype_readiness.get("dt_lava_training_ready", False)
        ),
        "permits_model_training": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _lava_npz_smoke_validation_failures(
    validation: Mapping[str, Any],
) -> list[str]:
    failures: list[str] = []
    if not bool(validation.get("configured", False)):
        return ["lava_npz_smoke_packet_validation_missing"]
    if validation.get("claim_scope") != (
        "lava_npz_margin_smoke_packet_validation_not_market_execution"
    ):
        failures.append("lava_npz_smoke_packet_validation_invalid_claim_scope")
    if not bool(validation.get("validation_passed", False)):
        failures.append("lava_npz_smoke_packet_validation_not_passed")
    required_true_flags = {
        "artifact_hashes_valid": "lava_npz_smoke_packet_hashes_invalid",
        "metrics_valid": "lava_npz_smoke_packet_metrics_invalid",
        "aggregate_valid": "lava_npz_smoke_packet_aggregate_invalid",
        "npz_contract_valid": "lava_npz_smoke_packet_contract_invalid",
        "baseline_comparison_valid": "lava_npz_smoke_packet_baseline_invalid",
        "baseline_comparison_ready": "lava_npz_smoke_packet_baseline_not_ready",
    }
    for flag_name, failure_name in required_true_flags.items():
        if not bool(validation.get(flag_name, False)):
            failures.append(failure_name)
    required_false_flags = {
        "promotion_gate": "lava_npz_smoke_packet_promotion_gate_true",
        "permits_model_training": "lava_npz_smoke_packet_permits_training_true",
        "market_execution_enabled": (
            "lava_npz_smoke_packet_market_execution_enabled_true"
        ),
    }
    for flag_name, failure_name in required_false_flags.items():
        if bool(validation.get(flag_name, False)):
            failures.append(failure_name)
    return failures


def _lava_npz_smoke_validation_summary(value: object) -> dict[str, Any]:
    validation = _mapping(value)
    if not validation:
        return {
            "configured": False,
            "claim_scope": "",
            "validation_passed": False,
            "artifact_hashes_valid": False,
            "metrics_valid": False,
            "aggregate_valid": False,
            "npz_contract_valid": False,
            "baseline_comparison_valid": False,
            "baseline_comparison_ready": False,
            "promotion_gate": False,
            "permits_model_training": False,
            "market_execution_enabled": False,
        }
    return {
        "configured": bool(validation.get("configured", False)),
        "claim_scope": str(validation.get("claim_scope", "")),
        "validation_passed": bool(validation.get("validation_passed", False)),
        "artifact_hashes_valid": bool(validation.get("artifact_hashes_valid", False)),
        "metrics_valid": bool(validation.get("metrics_valid", False)),
        "aggregate_valid": bool(validation.get("aggregate_valid", False)),
        "npz_contract_valid": bool(validation.get("npz_contract_valid", False)),
        "baseline_comparison_valid": bool(
            validation.get("baseline_comparison_valid", False)
        ),
        "baseline_comparison_ready": bool(
            validation.get("baseline_comparison_ready", False)
        ),
        "promotion_gate": bool(validation.get("promotion_gate", False)),
        "permits_model_training": bool(
            validation.get("permits_model_training", False)
        ),
        "market_execution_enabled": bool(
            validation.get("market_execution_enabled", False)
        ),
    }


def _teacher_contract_gate(
    teacher_summary: Mapping[str, Any] | None,
    teacher_validation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    if teacher_summary is None:
        return {
            "configured": False,
            "passed_for_academic_mvp": False,
            "failures": ["teacher_summary_missing"],
            "teacher_packet_validation": _teacher_packet_validation_summary(
                teacher_validation
            ),
            "market_execution_enabled": False,
        }
    dataset_summary = _mapping(teacher_summary.get("dataset_summary"))
    claim_boundary = _mapping(teacher_summary.get("claim_boundary"))
    feature_contract = _mapping(teacher_summary.get("feature_contract"))
    teacher_contract_summary = _teacher_contract_summary(
        teacher_summary.get("teacher_contract_summary")
    )
    gate_passport = _mapping(teacher_summary.get("gate_passport"))
    contract_gate = _mapping(gate_passport.get("teacher_dataset_contract_gate"))
    safe_switch_gate = _mapping(gate_passport.get("safe_switch_coverage_gate"))
    teacher_packet_validation = _teacher_packet_validation_summary(teacher_validation)
    failures: list[str] = []
    if not bool(contract_gate.get("passed", False)):
        failures.append("teacher_dataset_contract_gate_not_passed")
    if not bool(safe_switch_gate.get("passed", False)):
        failures.append("safe_switch_coverage_gate_not_passed")
    failures.extend(
        _teacher_packet_validation_failures(teacher_packet_validation)
    )
    permitted_rows = _int_or_default(
        dataset_summary.get("permitted_model_training_rows"),
        0,
    )
    if permitted_rows != 0:
        failures.append("permitted_model_training_rows_not_zero")
    if bool(dataset_summary.get("dt_lava_training_dataset_ready", False)):
        failures.append("dt_lava_training_dataset_ready_true")
    if bool(dataset_summary.get("v13_training_permission_gate_passed", False)):
        failures.append("v13_training_permission_gate_passed_true")
    if _contains_market_execution_enabled_true(teacher_summary):
        failures.append("market_execution_enabled_true")

    target_label_space = str(
        claim_boundary.get(
            "target_label_space",
            feature_contract.get("dt_action_target_contract", ""),
        )
    )
    v2_plus_role = str(feature_contract.get("v2_plus_role", ""))
    feature_action_target = str(feature_contract.get("dt_action_target_contract", ""))
    if target_label_space not in ALLOWED_DT_ACTION_TARGETS:
        failures.append(f"invalid_dt_action_target:{target_label_space}")
    if feature_action_target and feature_action_target not in ALLOWED_DT_ACTION_TARGETS:
        failures.append(f"invalid_dt_action_target_contract:{feature_action_target}")
    if v2_plus_role != REQUIRED_V2_PLUS_ROLE:
        failures.append(f"invalid_v2_plus_role:{v2_plus_role}")
    if not teacher_contract_summary["required_dfl_input_groups_present"]:
        failures.append("teacher_contract_missing_required_dfl_input_groups")
    if not teacher_contract_summary["required_dfl_target_groups_present"]:
        failures.append("teacher_contract_missing_required_dfl_target_groups")
    if not teacher_contract_summary["required_dt_input_groups_present"]:
        failures.append("teacher_contract_missing_required_dt_input_groups")
    if bool(teacher_contract_summary["raw_hourly_action_imitation"]):
        failures.append("teacher_contract_raw_hourly_action_imitation_true")
    if bool(teacher_contract_summary["market_execution_enabled"]):
        failures.append("teacher_contract_market_execution_enabled_true")
    return {
        "configured": True,
        "passed_for_academic_mvp": not failures,
        "failures": failures,
        "rows": _int_or_default(dataset_summary.get("rows"), 0),
        "train_selection_rows": _int_or_default(
            dataset_summary.get("train_selection_rows"),
            0,
        ),
        "permitted_model_training_rows": permitted_rows,
        "dt_lava_training_dataset_ready": False,
        "safe_switch_coverage_gate_passed": bool(safe_switch_gate.get("passed", False)),
        "v13_training_permission_gate_passed": False,
        "target_label_space": target_label_space,
        "v2_plus_role": v2_plus_role,
        "teacher_contract_summary": teacher_contract_summary,
        "teacher_packet_validation": teacher_packet_validation,
        "market_execution_enabled": False,
    }


def _teacher_packet_validation_failures(
    teacher_packet_validation: Mapping[str, Any],
) -> list[str]:
    failures: list[str] = []
    if not bool(teacher_packet_validation.get("configured", False)):
        return ["teacher_packet_validation_missing"]
    if teacher_packet_validation.get("claim_scope") != (
        "v13_dt_lava_teacher_packet_validation_not_market_execution"
    ):
        failures.append("teacher_packet_validation_invalid_claim_scope")
    if not bool(teacher_packet_validation.get("passed", False)):
        failures.append("teacher_packet_validation_not_passed")
    gate_failure_names = {
        "candidate_schedule_teacher_contract_passed": (
            "teacher_packet_validation_candidate_schedule_teacher_contract_not_passed"
        ),
        "training_permission_consistency_passed": (
            "teacher_packet_validation_training_permission_consistency_not_passed"
        ),
        "promotion_execution_blocked_passed": (
            "teacher_packet_validation_promotion_execution_blocked_not_passed"
        ),
        "no_market_execution_passed": (
            "teacher_packet_validation_no_market_execution_not_passed"
        ),
    }
    for flag_name, failure_name in gate_failure_names.items():
        if not bool(teacher_packet_validation.get(flag_name, False)):
            failures.append(failure_name)
    if bool(teacher_packet_validation.get("market_execution_enabled", False)):
        failures.append("teacher_packet_validation_market_execution_enabled_true")
    return failures


def _teacher_packet_validation_summary(value: object) -> dict[str, Any]:
    validation = _mapping(value)
    if not validation:
        return {
            "configured": False,
            "claim_scope": "",
            "passed": False,
            "candidate_schedule_teacher_contract_passed": False,
            "training_permission_consistency_passed": False,
            "promotion_execution_blocked_passed": False,
            "no_market_execution_passed": False,
            "market_execution_enabled": False,
        }
    gate_results = _mapping(validation.get("gate_results"))
    return {
        "configured": True,
        "claim_scope": str(validation.get("claim_scope", "")),
        "passed": bool(validation.get("passed", False)),
        "candidate_schedule_teacher_contract_passed": _validation_gate_passed(
            gate_results,
            "candidate_schedule_teacher_contract",
        ),
        "training_permission_consistency_passed": _validation_gate_passed(
            gate_results,
            "training_permission_consistency",
        ),
        "promotion_execution_blocked_passed": _validation_gate_passed(
            gate_results,
            "promotion_execution_blocked",
        ),
        "no_market_execution_passed": _validation_gate_passed(
            gate_results,
            "no_market_execution",
        ),
        "market_execution_enabled": bool(
            validation.get("market_execution_enabled", False)
        ),
    }


def _validation_gate_passed(
    gate_results: Mapping[str, Any],
    gate_name: str,
) -> bool:
    gate = _mapping(gate_results.get(gate_name))
    return bool(gate.get("passed", False))


def _dt_research_shadow_gate(
    sequence_summary: Mapping[str, Any] | None,
    smoke_summary: Mapping[str, Any] | None,
    evaluation_validation_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if sequence_summary is None and smoke_summary is None:
        return {
            "configured": False,
            "passed_for_academic_mvp": False,
            "status": "not_configured",
            "required_for_academic_mvp": False,
            "market_execution_enabled": False,
        }

    sequence = _mapping(sequence_summary)
    smoke = _mapping(smoke_summary)
    dataset_summary = _mapping(sequence.get("dataset_summary"))
    split_metadata = _mapping(sequence.get("split_metadata"))
    sequence_boundary = _mapping(sequence.get("claim_boundary"))
    state_contract = _mapping(sequence.get("dt_state_feature_contract"))
    reward_contract = _mapping(sequence.get("dt_reward_target_contract"))
    tensor_contract = _mapping(smoke.get("dt_tensor_contract"))
    evaluation_packet = _mapping(smoke.get("evaluation_packet_summary"))
    evaluation_validation = _mapping(evaluation_validation_summary)
    evaluation_validation_source = (
        "sidecar_validation_json"
        if evaluation_validation_summary is not None
        else "smoke_summary_embedded"
    )
    evaluation_validation_passed = (
        evaluation_validation.get("passed") is True
        if evaluation_validation_summary is not None
        else evaluation_packet.get("validation_passed") is True
    )
    evaluation_metrics = _mapping(smoke.get("evaluation_metrics"))
    failures: list[str] = []
    if sequence.get("claim_scope") != (
        "dt_research_shadow_sequence_dataset_not_promotable_not_market_execution"
    ):
        failures.append("dt_research_shadow_sequence_invalid_claim_scope")
    if smoke.get("claim_scope") != (
        "dt_research_shadow_transformer_smoke_not_promotable_not_market_execution"
    ):
        failures.append("dt_research_shadow_smoke_invalid_claim_scope")
    if split_metadata.get("split_strategy") != "chronological_delivery_timestamp":
        failures.append("dt_research_shadow_split_strategy_not_chronological")
    if split_metadata.get("chronological_split_passed") is not True:
        failures.append("dt_research_shadow_chronological_split_not_passed")
    for flag_name in (
        "publication_receipt_verified",
        "source_publication_timestamp_available",
        "market_availability_claim",
    ):
        if split_metadata.get(flag_name) is not False:
            failures.append(f"dt_research_shadow_flag_not_false:{flag_name}")
    if split_metadata.get("research_shadow_not_promotable") is not True:
        failures.append("dt_research_shadow_not_promotable_not_true")
    if sequence_boundary.get("action_target") != "candidate_index_or_schedule_family":
        failures.append("dt_research_shadow_invalid_action_target")
    if sequence_boundary.get("raw_hourly_buy_sell_hold_action_target") is not False:
        failures.append("dt_research_shadow_raw_hourly_action_target_true")
    if state_contract.get("state_contract_passed") is not True:
        failures.append("dt_research_shadow_state_contract_not_passed")
    if reward_contract.get("reward_contract_passed") is not True:
        failures.append("dt_research_shadow_reward_contract_not_passed")
    if tensor_contract.get("state_contract_passed") is not True:
        failures.append("dt_research_shadow_tensor_state_contract_not_passed")
    if tensor_contract.get("reward_contract_passed") is not True:
        failures.append("dt_research_shadow_tensor_reward_contract_not_passed")
    if tensor_contract.get("action_feasibility_mask_attached") is not True:
        failures.append("dt_research_shadow_action_feasibility_mask_missing")
    if tensor_contract.get("action_feasibility_mask_applied_to_loss") is not True:
        failures.append("dt_research_shadow_action_feasibility_mask_not_applied_to_loss")
    if tensor_contract.get("action_feasibility_mask_applied_to_eval") is not True:
        failures.append("dt_research_shadow_action_feasibility_mask_not_applied_to_eval")
    if evaluation_packet.get("claim_scope") != (
        "dt_research_shadow_evaluation_packet_not_promotable_not_market_execution"
    ):
        failures.append("dt_research_shadow_evaluation_packet_invalid_claim_scope")
    if evaluation_packet.get("market_execution_enabled") is not False:
        failures.append("dt_research_shadow_evaluation_packet_market_execution_enabled")
    if evaluation_packet.get("validation_passed") is not True:
        failures.append("dt_research_shadow_evaluation_packet_validation_not_passed")
    if evaluation_validation_summary is not None:
        if evaluation_validation.get("claim_scope") != (
            "dt_research_shadow_evaluation_validation_not_market_execution"
        ):
            failures.append("dt_research_shadow_evaluation_validation_invalid_claim_scope")
        if evaluation_validation.get("passed") is not True:
            failures.append("dt_research_shadow_evaluation_validation_not_passed")
        if _contains_market_execution_enabled_true(evaluation_validation):
            failures.append("dt_research_shadow_evaluation_validation_market_execution")

    research_shadow_rows = _int_or_default(
        dataset_summary.get("research_shadow_training_rows"),
        0,
    )
    promotable_rows = _int_or_default(
        dataset_summary.get("promotable_v13_permitted_training_rows"),
        0,
    )
    if research_shadow_rows <= 0:
        failures.append("dt_research_shadow_training_rows_missing")
    if promotable_rows != 0:
        failures.append("dt_research_shadow_promotable_rows_not_zero")
    if dataset_summary.get("v13_training_permission_gate_passed") is not False:
        failures.append("dt_research_shadow_v13_permission_not_false")
    if smoke.get("deterministic_safety_projection_passed") is not True:
        failures.append("dt_research_shadow_safety_projection_not_passed")
    if smoke.get("dt_promotion_gate_passed") is not False:
        failures.append("dt_research_shadow_promotion_gate_not_false")
    if _contains_market_execution_enabled_true(
        {"sequence_summary": sequence, "smoke_summary": smoke}
    ):
        failures.append("dt_research_shadow_market_execution_enabled_true")
    for metric_name in (
        "dt_selected_mean_regret_uah",
        "v2_plus_mean_regret_uah",
        "v2_plus_mean_value_uah",
        "strict_mean_regret_uah",
        "strict_mean_value_uah",
        "behavior_cloning_mean_regret_uah",
        "behavior_cloning_mean_value_uah",
    ):
        if _float_or_none(evaluation_metrics.get(metric_name)) is None:
            failures.append(f"dt_research_shadow_missing_metric:{metric_name}")
    infeasible_action_predictions = _int_or_default(
        evaluation_metrics.get("infeasible_action_prediction_count"),
        -1,
    )
    if infeasible_action_predictions != 0:
        failures.append("dt_research_shadow_infeasible_action_predictions")

    return {
        "configured": True,
        "passed_for_academic_mvp": not failures,
        "status": "passed" if not failures else "blocked",
        "failures": failures,
        "split_strategy": str(split_metadata.get("split_strategy", "")),
        "chronological_split_passed": bool(
            split_metadata.get("chronological_split_passed", False)
        ),
        "available_teacher_rows": _int_or_default(
            dataset_summary.get("available_teacher_rows"),
            0,
        ),
        "train_selection_rows": _int_or_default(
            dataset_summary.get("train_selection_rows"),
            0,
        ),
        "research_shadow_training_rows": research_shadow_rows,
        "promotable_v13_permitted_training_rows": promotable_rows,
        "forecast_context_required_families": [
            str(value)
            for value in _sequence(
                dataset_summary.get("forecast_context_required_families")
            )
        ],
        "forecast_context_present_families": [
            str(value)
            for value in _sequence(
                dataset_summary.get("forecast_context_present_families")
            )
        ],
        "forecast_context_missing_families": [
            str(value)
            for value in _sequence(
                dataset_summary.get("forecast_context_missing_families")
            )
        ],
        "forecast_context_coverage_passed": bool(
            dataset_summary.get("forecast_context_coverage_passed", False)
        ),
        "forecast_context_coverage_status": str(
            dataset_summary.get("forecast_context_coverage_status", "")
        ),
        "forecast_context_coverage_required_for_full_dt_prototype": bool(
            dataset_summary.get(
                "forecast_context_coverage_required_for_full_dt_prototype",
                False,
            )
        ),
        "state_contract_passed": bool(state_contract.get("state_contract_passed", False)),
        "reward_contract_passed": bool(
            reward_contract.get("reward_contract_passed", False)
        ),
        "action_feasibility_mask_attached": bool(
            tensor_contract.get("action_feasibility_mask_attached", False)
        ),
        "action_feasibility_mask_applied_to_loss": bool(
            tensor_contract.get("action_feasibility_mask_applied_to_loss", False)
        ),
        "action_feasibility_mask_applied_to_eval": bool(
            tensor_contract.get("action_feasibility_mask_applied_to_eval", False)
        ),
        "infeasible_action_prediction_count": infeasible_action_predictions,
        "state_context_groups": [
            str(value)
            for value in _sequence(state_contract.get("required_state_context_groups"))
        ],
        "missing_state_context_groups": [
            str(value)
            for value in _sequence(state_contract.get("missing_state_context_groups"))
        ],
        "return_to_go_target": str(
            reward_contract.get(
                "return_to_go_target",
                tensor_contract.get("return_to_go_target", ""),
            )
        ),
        "evaluation_packet_claim_scope": str(evaluation_packet.get("claim_scope", "")),
        "evaluation_packet_primary_metric": str(
            evaluation_packet.get("primary_metric", "")
        ),
        "evaluation_packet_validation_passed": bool(
            evaluation_validation_passed
        ),
        "evaluation_packet_validation_source": evaluation_validation_source,
        "publication_receipt_verified": False,
        "source_publication_timestamp_available": False,
        "market_availability_claim": False,
        "research_shadow_not_promotable": True,
        "requested_model_backbone": str(smoke.get("requested_model_backbone", "")),
        "model_backbone": str(smoke.get("model_backbone", "")),
        "model_backbone_selection_reason": str(
            smoke.get("model_backbone_selection_reason", "")
        ),
        "hf_decision_transformer_available": bool(
            smoke.get("hf_decision_transformer_available", False)
        ),
        "hf_decision_transformer_status": str(
            smoke.get("hf_decision_transformer_status", "")
        ),
        "train_sequence_count": _int_or_default(
            smoke.get("train_sequence_count"),
            0,
        ),
        "evaluation_sequence_count": _int_or_default(
            smoke.get("evaluation_sequence_count"),
            0,
        ),
        "evaluation_metrics": {
            key: evaluation_metrics.get(key)
            for key in (
                "dt_selected_mean_regret_uah",
                "dt_selected_mean_value_uah",
                "v2_plus_mean_regret_uah",
                "v2_plus_mean_value_uah",
                "strict_mean_regret_uah",
                "strict_mean_value_uah",
                "behavior_cloning_mean_regret_uah",
                "behavior_cloning_mean_value_uah",
                "infeasible_action_prediction_count",
                "accuracy_secondary",
            )
            if key in evaluation_metrics
        },
        "promotion_blocker": (
            "explicit_dam_publication_receipts_missing_publication_receipt_not_verified"
        ),
        "v13_training_permission_gate_passed": False,
        "dt_promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _teacher_contract_summary(value: object) -> dict[str, Any]:
    summary = _mapping(value)
    return {
        "claim_scope": str(
            summary.get(
                "claim_scope",
                "candidate_schedule_teacher_contract_not_market_execution",
            )
        ),
        "required_dfl_input_groups_present": bool(
            summary.get("required_dfl_input_groups_present", False)
        ),
        "required_dfl_target_groups_present": bool(
            summary.get("required_dfl_target_groups_present", False)
        ),
        "required_dt_input_groups_present": bool(
            summary.get("required_dt_input_groups_present", False)
        ),
        "dfl_input_groups": [
            str(group)
            for group in _sequence(summary.get("dfl_input_groups"))
            if str(group).strip()
        ],
        "dfl_target_groups": [
            str(group)
            for group in _sequence(summary.get("dfl_target_groups"))
            if str(group).strip()
        ],
        "dt_input_groups": [
            str(group)
            for group in _sequence(summary.get("dt_input_groups"))
            if str(group).strip()
        ],
        "target_label_space": str(summary.get("target_label_space", "")),
        "dt_action_target_contract": str(
            summary.get("dt_action_target_contract", "")
        ),
        "v2_plus_role": str(summary.get("v2_plus_role", "")),
        "training_permission_status": str(
            summary.get("training_permission_status", "")
        ),
        "train_selection_rows": _int_or_default(
            summary.get("train_selection_rows"),
            0,
        ),
        "permitted_model_training_rows": _int_or_default(
            summary.get("permitted_model_training_rows"),
            0,
        ),
        "training_rows_blocked_by_v13_source_readiness": bool(
            summary.get("training_rows_blocked_by_v13_source_readiness", False)
        ),
        "raw_hourly_action_imitation": bool(
            summary.get("raw_hourly_action_imitation", False)
        ),
        "market_execution_enabled": bool(
            summary.get("market_execution_enabled", False)
        ),
    }


def _offline_challenger_gate(
    offline_challenger_summary: Mapping[str, Any] | None,
    offline_challenger_validation: Mapping[str, Any] | None,
) -> dict[str, Any]:
    challenger_packet_validation = _offline_challenger_packet_validation_summary(
        offline_challenger_validation
    )
    if offline_challenger_summary is None:
        return {
            "configured": False,
            "passed_for_academic_mvp": False,
            "failures": ["offline_challenger_summary_missing"],
            "offline_challenger_packet_validation": challenger_packet_validation,
            "market_execution_enabled": False,
        }
    gate = _mapping(offline_challenger_summary.get("gate"))
    metrics = _mapping(gate.get("metrics"))
    control_comparison_summary = _control_comparison_summary(
        metrics.get("control_comparison_summary")
    )
    promotion_gate = _mapping(offline_challenger_summary.get("promotion_gate"))
    failures: list[str] = []
    if str(gate.get("decision", "")) != "blocked":
        failures.append("offline_challenger_decision_not_blocked")
    if bool(metrics.get("offline_dt_lava_challenger_gate_passed", False)):
        failures.append("offline_dt_lava_challenger_gate_passed_true")
    if not bool(metrics.get("deterministic_safety_projection_passed", False)):
        failures.append("deterministic_safety_projection_not_passed")
    if not bool(metrics.get("required_control_roles_present", False)):
        failures.append("required_control_roles_missing")
    if not bool(metrics.get("behavior_cloning_control_present", False)):
        failures.append("behavior_cloning_control_missing")
    failures.extend(
        _offline_challenger_packet_validation_failures(
            challenger_packet_validation
        )
    )
    if _contains_market_execution_enabled_true(offline_challenger_summary):
        failures.append("market_execution_enabled_true")

    return {
        "configured": True,
        "passed_for_academic_mvp": not failures,
        "failures": failures,
        "decision": str(gate.get("decision", "")),
        "teacher_dataset_ready": bool(metrics.get("teacher_dataset_ready", False)),
        "deterministic_safety_projection_passed": bool(
            metrics.get("deterministic_safety_projection_passed", False)
        ),
        "required_control_roles_present": bool(
            metrics.get("required_control_roles_present", False)
        ),
        "behavior_cloning_control_present": bool(
            metrics.get("behavior_cloning_control_present", False)
        ),
        "bridge_evidence_passed": bool(metrics.get("bridge_evidence_passed", False)),
        "bridge_gate_passed": bool(metrics.get("bridge_gate_passed", False)),
        "control_comparison_summary": control_comparison_summary,
        "promotion_gate_passed": bool(
            promotion_gate.get("offline_dt_lava_challenger_gate_passed", False)
        ),
        "production_promote": bool(promotion_gate.get("production_promote", False)),
        "offline_challenger_packet_validation": challenger_packet_validation,
        "market_execution_enabled": False,
    }


def _offline_challenger_packet_validation_failures(
    validation: Mapping[str, Any],
) -> list[str]:
    failures: list[str] = []
    if not bool(validation.get("configured", False)):
        return ["offline_challenger_packet_validation_missing"]
    if validation.get("claim_scope") != (
        "v13_dt_lava_offline_challenger_packet_validation_not_market_execution"
    ):
        failures.append("offline_challenger_packet_validation_invalid_claim_scope")
    if not bool(validation.get("passed", False)):
        failures.append("offline_challenger_packet_validation_not_passed")
    gate_failure_names = {
        "strict_control_comparison_passed": (
            "offline_challenger_validation_strict_control_comparison_not_passed"
        ),
        "deterministic_safety_projection_passed": (
            "offline_challenger_validation_deterministic_safety_projection_not_passed"
        ),
        "non_promotion_execution_boundary_passed": (
            "offline_challenger_validation_non_promotion_execution_boundary_not_passed"
        ),
        "no_market_execution_passed": (
            "offline_challenger_validation_no_market_execution_not_passed"
        ),
    }
    for flag_name, failure_name in gate_failure_names.items():
        if not bool(validation.get(flag_name, False)):
            failures.append(failure_name)
    if bool(validation.get("market_execution_enabled", False)):
        failures.append("offline_challenger_validation_market_execution_enabled_true")
    return failures


def _offline_challenger_packet_validation_summary(value: object) -> dict[str, Any]:
    validation = _mapping(value)
    if not validation:
        return {
            "configured": False,
            "claim_scope": "",
            "passed": False,
            "strict_control_comparison_passed": False,
            "deterministic_safety_projection_passed": False,
            "non_promotion_execution_boundary_passed": False,
            "no_market_execution_passed": False,
            "market_execution_enabled": False,
        }
    gate_results = _mapping(validation.get("gate_results"))
    return {
        "configured": True,
        "claim_scope": str(validation.get("claim_scope", "")),
        "passed": bool(validation.get("passed", False)),
        "strict_control_comparison_passed": _validation_gate_passed(
            gate_results,
            "strict_control_comparison",
        ),
        "deterministic_safety_projection_passed": _validation_gate_passed(
            gate_results,
            "deterministic_safety_projection",
        ),
        "non_promotion_execution_boundary_passed": _validation_gate_passed(
            gate_results,
            "non_promotion_execution_boundary",
        ),
        "no_market_execution_passed": _validation_gate_passed(
            gate_results,
            "no_market_execution",
        ),
        "market_execution_enabled": bool(
            validation.get("market_execution_enabled", False)
        ),
    }


def _control_comparison_summary(value: object) -> dict[str, Any]:
    summary = _mapping(value)
    source_summaries = [
        _source_model_control_summary(item)
        for item in _sequence(summary.get("source_model_summaries"))
    ]
    return {
        "claim_scope": str(
            summary.get(
                "claim_scope",
                "strict_lp_oracle_control_comparison_not_market_execution",
            )
        ),
        "required_control_roles": [
            str(role)
            for role in _sequence(summary.get("required_control_roles"))
            if str(role).strip()
        ],
        "required_control_roles_present": bool(
            summary.get("required_control_roles_present", False)
        ),
        "behavior_cloning_control_present": bool(
            summary.get("behavior_cloning_control_present", False)
        ),
        "source_model_count": _int_or_default(summary.get("source_model_count"), 0),
        "validation_tenant_anchor_count": _int_or_default(
            summary.get("validation_tenant_anchor_count"),
            0,
        ),
        "best_observed_challenger_role": summary.get(
            "best_observed_challenger_role"
        ),
        "best_observed_source_model_name": summary.get(
            "best_observed_source_model_name"
        ),
        "best_observed_mean_regret_improvement_ratio_vs_v2_plus": _float_or_default(
            summary.get("best_observed_mean_regret_improvement_ratio_vs_v2_plus"),
            0.0,
        ),
        "source_model_summaries": source_summaries,
        "market_execution_enabled": False,
    }


def _source_model_control_summary(value: object) -> dict[str, Any]:
    summary = _mapping(value)
    return {
        "source_model_name": str(summary.get("source_model_name", "")),
        "tenant_count": _int_or_default(summary.get("tenant_count"), 0),
        "validation_tenant_anchor_count": _int_or_default(
            summary.get("validation_tenant_anchor_count"),
            0,
        ),
        "strict_mean_regret_uah": _float_or_default(
            summary.get("strict_mean_regret_uah"),
            0.0,
        ),
        "strict_median_regret_uah": _float_or_default(
            summary.get("strict_median_regret_uah"),
            0.0,
        ),
        "v2_plus_mean_regret_uah": _float_or_default(
            summary.get("v2_plus_mean_regret_uah"),
            0.0,
        ),
        "v2_plus_median_regret_uah": _float_or_default(
            summary.get("v2_plus_median_regret_uah"),
            0.0,
        ),
        "behavior_cloning_mean_regret_uah": _float_or_default(
            summary.get("behavior_cloning_mean_regret_uah"),
            0.0,
        ),
        "behavior_cloning_median_regret_uah": _float_or_default(
            summary.get("behavior_cloning_median_regret_uah"),
            0.0,
        ),
        "challenger_summaries": [
            _challenger_control_summary(item)
            for item in _sequence(summary.get("challenger_summaries"))
        ],
        "market_execution_enabled": False,
    }


def _challenger_control_summary(value: object) -> dict[str, Any]:
    summary = _mapping(value)
    return {
        "source_model_name": str(summary.get("source_model_name", "")),
        "selection_role": str(summary.get("selection_role", "")),
        "validation_tenant_anchor_count": _int_or_default(
            summary.get("validation_tenant_anchor_count"),
            0,
        ),
        "mean_regret_uah": _float_or_default(summary.get("mean_regret_uah"), 0.0),
        "median_regret_uah": _float_or_default(
            summary.get("median_regret_uah"),
            0.0,
        ),
        "mean_regret_improvement_ratio_vs_v2_plus": _float_or_default(
            summary.get("mean_regret_improvement_ratio_vs_v2_plus"),
            0.0,
        ),
        "mean_regret_improvement_ratio_vs_strict": _float_or_default(
            summary.get("mean_regret_improvement_ratio_vs_strict"),
            0.0,
        ),
        "median_not_worse_vs_v2_plus": bool(
            summary.get("median_not_worse_vs_v2_plus", False)
        ),
        "median_not_worse_vs_strict": bool(
            summary.get("median_not_worse_vs_strict", False)
        ),
        "beats_behavior_cloning": bool(
            summary.get("beats_behavior_cloning", False)
        ),
        "market_execution_enabled": False,
    }


def _add_validation_gate_result(
    *,
    gate_results: dict[str, dict[str, Any]],
    failures: list[str],
    gate_name: str,
    gate_failures: list[str],
) -> None:
    gate_passed = not gate_failures
    gate_results[gate_name] = {
        "passed": gate_passed,
        "failures": gate_failures,
        "market_execution_enabled": False,
    }
    failures.extend(gate_failures)


def _prototype_contract_validation(summary: Mapping[str, Any]) -> dict[str, Any]:
    prototype_contract = _mapping(summary.get("prototype_contract"))
    evaluation_contract = _mapping(prototype_contract.get("evaluation_contract"))
    failures: list[str] = []
    if not prototype_contract:
        failures.append("prototype_contract_missing")
    if prototype_contract.get("claim_scope") != (
        "credentialless_dfl_dt_prototype_contract_not_market_execution"
    ):
        failures.append("prototype_contract_invalid_claim_scope")
    if prototype_contract.get("product_boundary") != (
        "dam_delivery_day_operator_recommendation_preview"
    ):
        failures.append("prototype_contract_invalid_product_boundary")
    dt_action_target_contract = str(
        prototype_contract.get("dt_action_target_contract", "")
    )
    if dt_action_target_contract not in ALLOWED_DT_ACTION_TARGETS:
        failures.append(f"prototype_contract_invalid_dt_action:{dt_action_target_contract}")
    v2_plus_role = str(prototype_contract.get("v2_plus_role", ""))
    if v2_plus_role != REQUIRED_V2_PLUS_ROLE:
        failures.append(f"prototype_contract_invalid_v2_plus_role:{v2_plus_role}")
    if prototype_contract.get("raw_hourly_action_imitation") is not False:
        failures.append("prototype_contract_raw_hourly_action_imitation")
    if prototype_contract.get("prototype_contract_gate_passed") is not True:
        failures.append("prototype_contract_gate_not_passed")
    for flag_name in (
        "required_controls_present",
        "behavior_cloning_control_present",
        "deterministic_safety_projection_passed",
    ):
        if evaluation_contract.get(flag_name) is not True:
            failures.append(f"evaluation_contract_flag_not_true:{flag_name}")
    if evaluation_contract.get("market_execution_enabled") is not False:
        failures.append("evaluation_contract_market_execution_enabled_not_false")
    return {
        "claim_scope": str(prototype_contract.get("claim_scope", "")),
        "product_boundary": str(prototype_contract.get("product_boundary", "")),
        "dt_action_target_contract": dt_action_target_contract,
        "v2_plus_role": v2_plus_role,
        "raw_hourly_action_imitation": bool(
            prototype_contract.get("raw_hourly_action_imitation", False)
        ),
        "prototype_contract_gate_passed": bool(
            prototype_contract.get("prototype_contract_gate_passed", False)
        ),
        "evaluation_contract": dict(evaluation_contract),
        "failures": failures,
        "market_execution_enabled": False,
    }


def _phase_readiness_validation(summary: Mapping[str, Any]) -> dict[str, Any]:
    phase_readiness = _mapping(summary.get("prototype_phase_readiness"))
    failures: list[str] = []
    if not phase_readiness:
        failures.append("prototype_phase_readiness_missing")
    if phase_readiness.get("claim_scope") != (
        "credentialless_dfl_dt_prototype_phase_readiness_not_market_execution"
    ):
        failures.append("prototype_phase_readiness_invalid_claim_scope")
    if phase_readiness.get("market_execution_enabled") is not False:
        failures.append("prototype_phase_readiness_market_execution_enabled_not_false")

    phase_0 = _mapping(phase_readiness.get("phase_0_v13_source_readiness"))
    if phase_0.get("status") != "blocked_market_submission_receipts":
        failures.append("phase_0_unexpected_status")
    if phase_0.get("explicit_receipts_gate_passed") is not False:
        failures.append("phase_0_explicit_receipts_gate_must_remain_blocked")
    if phase_0.get("safe_switch_floor_passed") is not True:
        failures.append("phase_0_safe_switch_floor_not_passed")
    if phase_0.get("ready_for_training") is not False:
        failures.append("phase_0_ready_for_training_must_be_false")

    phase_1 = _mapping(phase_readiness.get("phase_1_lava_npz_smoke"))
    if phase_1.get("status") != "passed_ci_smoke_not_promotion":
        failures.append("phase_1_unexpected_status")
    if phase_1.get("gate_passed") is not True:
        failures.append("phase_1_gate_not_passed")

    phase_2 = _mapping(phase_readiness.get("phase_2_v13_gated_teacher_contract"))
    if phase_2.get("status") != "passed_contract_training_rows_gated":
        failures.append("phase_2_unexpected_status")
    if _int_or_default(phase_2.get("permitted_model_training_rows"), -1) != 0:
        failures.append("phase_2_permitted_training_rows_not_zero")

    phase_3 = _mapping(phase_readiness.get("phase_3_offline_challenger"))
    if phase_3.get("status") != "passed_non_promotion_evidence":
        failures.append("phase_3_unexpected_status")
    if phase_3.get("promotion_gate_passed") is not False:
        failures.append("phase_3_promotion_gate_must_be_false")

    phase_4 = _mapping(phase_readiness.get("phase_4_full_schedule_dfl"))
    if phase_4.get("status") != "future_work_not_started":
        failures.append("phase_4_unexpected_status")
    if phase_4.get("gate_passed") is not False:
        failures.append("phase_4_gate_must_not_pass")

    if _contains_market_execution_enabled_true(phase_readiness):
        failures.append("prototype_phase_readiness_nested_market_execution_enabled_true")
    return {
        "claim_scope": str(phase_readiness.get("claim_scope", "")),
        "phase_0_status": str(phase_0.get("status", "")),
        "phase_1_status": str(phase_1.get("status", "")),
        "phase_2_status": str(phase_2.get("status", "")),
        "phase_3_status": str(phase_3.get("status", "")),
        "phase_4_status": str(phase_4.get("status", "")),
        "failures": failures,
        "market_execution_enabled": False,
    }


def _prototype_evidence_scorecard_validation(
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    scorecard = _mapping(summary.get("prototype_evidence_scorecard"))
    failures: list[str] = []
    if not scorecard:
        failures.append("prototype_evidence_scorecard_missing")
    if scorecard.get("claim_scope") != (
        "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution"
    ):
        failures.append("prototype_evidence_scorecard_invalid_claim_scope")
    if scorecard.get("scorecard_passed_for_academic_mvp") is not True:
        failures.append("prototype_evidence_scorecard_not_passed")
    if _int_or_default(scorecard.get("operator_bid_preview_rows"), 0) <= 0:
        failures.append("scorecard_missing_operator_bid_preview_rows")
    if scorecard.get("operator_bid_preview_has_buy_or_sell") is not True:
        failures.append("scorecard_missing_buy_or_sell_preview")
    if scorecard.get("lava_npz_validation_passed") is not True:
        failures.append("scorecard_lava_npz_validation_not_passed")
    if scorecard.get("lava_npz_baseline_comparison_ready") is not True:
        failures.append("scorecard_lava_npz_baseline_not_ready")
    if _int_or_default(scorecard.get("teacher_rows"), 0) <= 0:
        failures.append("scorecard_missing_teacher_rows")
    if _int_or_default(scorecard.get("teacher_train_selection_rows"), 0) <= 0:
        failures.append("scorecard_missing_teacher_train_selection_rows")
    if _int_or_default(scorecard.get("teacher_permitted_model_training_rows"), -1) != 0:
        failures.append("scorecard_teacher_permitted_training_rows_not_zero")
    if str(scorecard.get("dt_action_target_contract", "")) not in ALLOWED_DT_ACTION_TARGETS:
        failures.append("scorecard_invalid_dt_action_target_contract")
    if scorecard.get("offline_challenger_evidence_passed") is not True:
        failures.append("scorecard_offline_challenger_evidence_not_passed")
    if scorecard.get("offline_challenger_promotion_gate_passed") is not False:
        failures.append("scorecard_offline_challenger_promotion_gate_not_false")
    if scorecard.get("strict_v2_plus_behavior_cloning_controls_present") is not True:
        failures.append("scorecard_controls_not_present")
    if scorecard.get("deterministic_safety_projection_passed") is not True:
        failures.append("scorecard_safety_projection_not_passed")
    if _int_or_default(scorecard.get("validation_tenant_anchor_count"), 0) <= 0:
        failures.append("scorecard_missing_validation_anchor_count")
    if scorecard.get("v2_plus_role") != REQUIRED_V2_PLUS_ROLE:
        failures.append("scorecard_invalid_v2_plus_role")
    for flag_name in (
        "market_submission_ready",
        "permits_model_training",
        "promotion_gate_passed",
        "market_execution_enabled",
    ):
        if scorecard.get(flag_name) is not False:
            failures.append(f"scorecard_flag_not_false:{flag_name}")
    if _contains_market_execution_enabled_true(scorecard):
        failures.append("prototype_evidence_scorecard_nested_market_execution_enabled_true")
    return {
        "claim_scope": str(scorecard.get("claim_scope", "")),
        "scorecard_passed_for_academic_mvp": bool(
            scorecard.get("scorecard_passed_for_academic_mvp", False)
        ),
        "operator_bid_preview_rows": _int_or_default(
            scorecard.get("operator_bid_preview_rows"),
            0,
        ),
        "teacher_train_selection_rows": _int_or_default(
            scorecard.get("teacher_train_selection_rows"),
            0,
        ),
        "teacher_permitted_model_training_rows": _int_or_default(
            scorecard.get("teacher_permitted_model_training_rows"),
            0,
        ),
        "validation_tenant_anchor_count": _int_or_default(
            scorecard.get("validation_tenant_anchor_count"),
            0,
        ),
        "failures": failures,
        "market_execution_enabled": False,
    }


def _dt_research_shadow_gate_validation(
    summary: Mapping[str, Any],
) -> dict[str, Any]:
    gate = _mapping(summary.get("dt_research_shadow_gate"))
    failures: list[str] = []
    if not gate or gate.get("configured") is not True:
        return {
            "configured": False,
            "passed_for_academic_mvp": False,
            "failures": failures,
            "market_execution_enabled": False,
        }
    if gate.get("passed_for_academic_mvp") is not True:
        failures.append("dt_research_shadow_gate_not_passed")
    if gate.get("split_strategy") != "chronological_delivery_timestamp":
        failures.append("dt_research_shadow_gate_not_chronological")
    if _int_or_default(gate.get("research_shadow_training_rows"), 0) <= 0:
        failures.append("dt_research_shadow_gate_missing_research_rows")
    if _int_or_default(gate.get("promotable_v13_permitted_training_rows"), -1) != 0:
        failures.append("dt_research_shadow_gate_promotable_rows_not_zero")
    for flag_name in (
        "publication_receipt_verified",
        "source_publication_timestamp_available",
        "market_availability_claim",
        "dt_promotion_gate_passed",
        "market_execution_enabled",
    ):
        if gate.get(flag_name) is not False:
            failures.append(f"dt_research_shadow_gate_flag_not_false:{flag_name}")
    if gate.get("research_shadow_not_promotable") is not True:
        failures.append("dt_research_shadow_gate_not_promotable_not_true")
    if gate.get("action_feasibility_mask_attached") is not True:
        failures.append("dt_research_shadow_gate_action_feasibility_mask_missing")
    if gate.get("action_feasibility_mask_applied_to_loss") is not True:
        failures.append(
            "dt_research_shadow_gate_action_feasibility_mask_not_applied_to_loss"
        )
    if gate.get("action_feasibility_mask_applied_to_eval") is not True:
        failures.append(
            "dt_research_shadow_gate_action_feasibility_mask_not_applied_to_eval"
        )
    if _int_or_default(gate.get("infeasible_action_prediction_count"), -1) != 0:
        failures.append("dt_research_shadow_gate_infeasible_action_predictions")
    if _contains_market_execution_enabled_true(gate):
        failures.append("dt_research_shadow_gate_nested_market_execution_enabled_true")
    return {
        "configured": True,
        "passed_for_academic_mvp": bool(gate.get("passed_for_academic_mvp", False)),
        "research_shadow_training_rows": _int_or_default(
            gate.get("research_shadow_training_rows"),
            0,
        ),
        "promotable_v13_permitted_training_rows": _int_or_default(
            gate.get("promotable_v13_permitted_training_rows"),
            0,
        ),
        "failures": failures,
        "market_execution_enabled": False,
    }


def _prototype_contract(
    *,
    operator_gate: Mapping[str, Any],
    prototype_gate: Mapping[str, Any],
    teacher_gate: Mapping[str, Any],
    challenger_gate: Mapping[str, Any],
) -> dict[str, Any]:
    teacher_contract_summary = _mapping(teacher_gate.get("teacher_contract_summary"))
    control_comparison_summary = _mapping(
        challenger_gate.get("control_comparison_summary")
    )
    required_controls = [
        "strict_reference",
        "schedule_value_learner_v2_plus_reference",
        "filtered_behavior_cloning_reference",
    ]
    dt_action_target_contract = str(
        teacher_contract_summary.get(
            "dt_action_target_contract",
            teacher_gate.get("target_label_space", ""),
        )
    )
    raw_hourly_action_imitation = bool(
        teacher_contract_summary.get("raw_hourly_action_imitation", False)
    )
    v2_plus_role = str(teacher_contract_summary.get("v2_plus_role", ""))
    evaluation_contract = {
        "primary_metric": "strict_lp_oracle_regret_value_vs_v2_plus",
        "required_controls": required_controls,
        "required_controls_present": bool(
            control_comparison_summary.get("required_control_roles_present", False)
        ),
        "behavior_cloning_control_present": bool(
            control_comparison_summary.get("behavior_cloning_control_present", False)
        ),
        "deterministic_safety_projection_required": True,
        "deterministic_safety_projection_passed": bool(
            challenger_gate.get("deterministic_safety_projection_passed", False)
        ),
        "market_execution_enabled": False,
    }
    prototype_contract_gate_passed = bool(
        operator_gate.get("passed", False)
        and prototype_gate.get("passed_for_academic_mvp", False)
        and teacher_gate.get("passed_for_academic_mvp", False)
        and challenger_gate.get("passed_for_academic_mvp", False)
        and dt_action_target_contract in ALLOWED_DT_ACTION_TARGETS
        and v2_plus_role == REQUIRED_V2_PLUS_ROLE
        and not raw_hourly_action_imitation
        and evaluation_contract["required_controls_present"]
        and evaluation_contract["behavior_cloning_control_present"]
        and evaluation_contract["deterministic_safety_projection_passed"]
    )
    return {
        "claim_scope": "credentialless_dfl_dt_prototype_contract_not_market_execution",
        "product_boundary": "dam_delivery_day_operator_recommendation_preview",
        "operator_outputs": [
            "recommendation_schedule",
            "bid_recommendation_preview",
        ],
        "forbidden_outputs": sorted(FORBIDDEN_MARKET_PAYLOAD_KEYS),
        "dfl_input_groups": [
            str(group)
            for group in _sequence(teacher_contract_summary.get("dfl_input_groups"))
            if str(group).strip()
        ],
        "dfl_target_groups": [
            str(group)
            for group in _sequence(teacher_contract_summary.get("dfl_target_groups"))
            if str(group).strip()
        ],
        "dt_input_groups": [
            str(group)
            for group in _sequence(teacher_contract_summary.get("dt_input_groups"))
            if str(group).strip()
        ],
        "dt_action_target_contract": dt_action_target_contract,
        "raw_hourly_action_imitation": raw_hourly_action_imitation,
        "v2_plus_role": v2_plus_role,
        "training_permission_status": str(
            teacher_contract_summary.get("training_permission_status", "")
        ),
        "permitted_model_training_rows": _int_or_default(
            teacher_gate.get("permitted_model_training_rows"),
            0,
        ),
        "evaluation_contract": evaluation_contract,
        "prototype_contract_gate_passed": prototype_contract_gate_passed,
        "promotion_gate_passed": False,
        "market_execution_gate_passed": False,
        "market_execution_enabled": False,
    }


def _prototype_phase_readiness(
    *,
    source_governance: Mapping[str, Any],
    prototype_gate: Mapping[str, Any],
    teacher_gate: Mapping[str, Any],
    challenger_gate: Mapping[str, Any],
) -> dict[str, Any]:
    explicit_receipts_gate_passed = bool(
        source_governance.get("v13_explicit_receipts_gate_passed", False)
    )
    safe_switch_floor_passed = (
        _int_or_default(
            source_governance.get("total_missing_safe_switch_examples"),
            0,
        )
        == 0
    )
    v13_training_permission_passed = bool(
        teacher_gate.get("v13_training_permission_gate_passed", False)
    )
    prototype_gate_passed = bool(prototype_gate.get("passed_for_academic_mvp", False))
    teacher_contract_passed = bool(teacher_gate.get("passed_for_academic_mvp", False))
    challenger_evidence_passed = bool(
        challenger_gate.get("passed_for_academic_mvp", False)
    )
    promotion_gate_passed = bool(challenger_gate.get("promotion_gate_passed", False))
    control_comparison_summary = _mapping(
        challenger_gate.get("control_comparison_summary")
    )
    return {
        "claim_scope": "credentialless_dfl_dt_prototype_phase_readiness_not_market_execution",
        "phase_0_v13_source_readiness": {
            "status": "ready_for_training"
            if explicit_receipts_gate_passed and v13_training_permission_passed
            else "blocked_market_submission_receipts",
            "explicit_receipts_gate_passed": explicit_receipts_gate_passed,
            "safe_switch_floor_passed": safe_switch_floor_passed,
            "ready_for_training": v13_training_permission_passed,
            "required_for_academic_mvp": False,
            "market_execution_enabled": False,
        },
        "phase_1_lava_npz_smoke": {
            "status": "passed_ci_smoke_not_promotion"
            if prototype_gate_passed
            else "blocked_ci_smoke",
            "gate_passed": prototype_gate_passed,
            "permits_model_training": False,
            "promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "phase_2_v13_gated_teacher_contract": {
            "status": "passed_contract_training_rows_gated"
            if teacher_contract_passed
            else "blocked_teacher_contract",
            "gate_passed": teacher_contract_passed,
            "target_label_space": str(teacher_gate.get("target_label_space", "")),
            "permitted_model_training_rows": _int_or_default(
                teacher_gate.get("permitted_model_training_rows"),
                0,
            ),
            "permits_model_training": False,
            "market_execution_enabled": False,
        },
        "phase_3_offline_challenger": {
            "status": "passed_non_promotion_evidence"
            if challenger_evidence_passed and not promotion_gate_passed
            else "blocked_or_promoted_outside_credentialless_scope",
            "gate_passed_for_academic_mvp": challenger_evidence_passed,
            "promotion_gate_passed": promotion_gate_passed,
            "evaluation_metric": "strict_lp_oracle_regret_value_vs_v2_plus",
            "required_control_roles_present": bool(
                control_comparison_summary.get("required_control_roles_present", False)
            ),
            "behavior_cloning_control_present": bool(
                control_comparison_summary.get("behavior_cloning_control_present", False)
            ),
            "validation_tenant_anchor_count": _int_or_default(
                control_comparison_summary.get("validation_tenant_anchor_count"),
                0,
            ),
            "market_execution_enabled": False,
        },
        "phase_4_full_schedule_dfl": {
            "status": "future_work_not_started",
            "gate_passed": False,
            "required_for_academic_mvp": False,
            "claim_boundary": "not_full_dfl",
            "market_execution_enabled": False,
        },
        "market_execution_enabled": False,
    }


def _prototype_evidence_scorecard(
    *,
    operator_gate: Mapping[str, Any],
    prototype_gate: Mapping[str, Any],
    teacher_gate: Mapping[str, Any],
    challenger_gate: Mapping[str, Any],
    prototype_contract: Mapping[str, Any],
) -> dict[str, Any]:
    bid_preview_summary = _mapping(operator_gate.get("bid_preview_summary"))
    lava_validation = _mapping(prototype_gate.get("lava_npz_smoke_validation"))
    control_comparison_summary = _mapping(
        challenger_gate.get("control_comparison_summary")
    )
    controls_present = bool(
        control_comparison_summary.get("required_control_roles_present", False)
    ) and bool(control_comparison_summary.get("behavior_cloning_control_present", False))
    scorecard_passed = bool(
        operator_gate.get("passed", False)
        and _int_or_default(operator_gate.get("bid_recommendation_preview_rows"), 0) > 0
        and bid_preview_summary.get("has_buy_or_sell_recommendation") is True
        and prototype_gate.get("passed_for_academic_mvp") is True
        and lava_validation.get("validation_passed") is True
        and lava_validation.get("baseline_comparison_ready") is True
        and teacher_gate.get("passed_for_academic_mvp") is True
        and _int_or_default(teacher_gate.get("rows"), 0) > 0
        and _int_or_default(teacher_gate.get("train_selection_rows"), 0) > 0
        and _int_or_default(
            teacher_gate.get("permitted_model_training_rows"),
            -1,
        )
        == 0
        and challenger_gate.get("passed_for_academic_mvp") is True
        and challenger_gate.get("promotion_gate_passed") is False
        and controls_present
        and challenger_gate.get("deterministic_safety_projection_passed") is True
        and _int_or_default(
            control_comparison_summary.get("validation_tenant_anchor_count"),
            0,
        )
        > 0
        and prototype_contract.get("v2_plus_role") == REQUIRED_V2_PLUS_ROLE
        and not _contains_market_execution_enabled_true(
            {
                "operator_gate": operator_gate,
                "prototype_gate": prototype_gate,
                "teacher_gate": teacher_gate,
                "challenger_gate": challenger_gate,
                "prototype_contract": prototype_contract,
            }
        )
    )
    return {
        "claim_scope": "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
        "scorecard_passed_for_academic_mvp": scorecard_passed,
        "operator_bid_preview_rows": _int_or_default(
            operator_gate.get("bid_recommendation_preview_rows"),
            0,
        ),
        "operator_bid_preview_has_buy_or_sell": bool(
            bid_preview_summary.get("has_buy_or_sell_recommendation", False)
        ),
        "lava_npz_validation_passed": bool(
            lava_validation.get("validation_passed", False)
        ),
        "lava_npz_baseline_comparison_ready": bool(
            lava_validation.get("baseline_comparison_ready", False)
        ),
        "teacher_rows": _int_or_default(teacher_gate.get("rows"), 0),
        "teacher_train_selection_rows": _int_or_default(
            teacher_gate.get("train_selection_rows"),
            0,
        ),
        "teacher_permitted_model_training_rows": _int_or_default(
            teacher_gate.get("permitted_model_training_rows"),
            0,
        ),
        "dt_action_target_contract": str(
            prototype_contract.get("dt_action_target_contract", "")
        ),
        "offline_challenger_evidence_passed": bool(
            challenger_gate.get("passed_for_academic_mvp", False)
        ),
        "offline_challenger_promotion_gate_passed": bool(
            challenger_gate.get("promotion_gate_passed", False)
        ),
        "offline_challenger_decision": str(challenger_gate.get("decision", "")),
        "strict_v2_plus_behavior_cloning_controls_present": controls_present,
        "deterministic_safety_projection_passed": bool(
            challenger_gate.get("deterministic_safety_projection_passed", False)
        ),
        "validation_tenant_anchor_count": _int_or_default(
            control_comparison_summary.get("validation_tenant_anchor_count"),
            0,
        ),
        "best_observed_challenger_role": control_comparison_summary.get(
            "best_observed_challenger_role"
        ),
        "best_observed_mean_regret_improvement_ratio_vs_v2_plus": _float_or_default(
            control_comparison_summary.get(
                "best_observed_mean_regret_improvement_ratio_vs_v2_plus"
            ),
            0.0,
        ),
        "v2_plus_role": str(prototype_contract.get("v2_plus_role", "")),
        "market_submission_ready": False,
        "permits_model_training": False,
        "promotion_gate_passed": False,
        "market_execution_enabled": False,
    }


def _contains_market_execution_enabled_true(value: object) -> bool:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if key == "market_execution_enabled" and bool(item):
                return True
            if _contains_market_execution_enabled_true(item):
                return True
        return False
    if isinstance(value, list | tuple):
        return any(_contains_market_execution_enabled_true(item) for item in value)
    return False


def _academic_mvp_gate_passport(
    *,
    operator_gate: Mapping[str, Any],
    source_governance: Mapping[str, Any],
    prototype_gate: Mapping[str, Any],
    teacher_gate: Mapping[str, Any],
    challenger_gate: Mapping[str, Any],
    prototype_contract: Mapping[str, Any],
    prototype_evidence_scorecard: Mapping[str, Any],
    dt_research_shadow_gate: Mapping[str, Any],
    no_market_execution_safety_gate_passed: bool,
) -> dict[str, dict[str, Any]]:
    operator_preview_passed = bool(operator_gate.get("passed", False))
    bid_preview_rows = _int_or_default(
        operator_gate.get("bid_recommendation_preview_rows"),
        0,
    )
    bid_preview_summary = _mapping(operator_gate.get("bid_preview_summary"))
    source_governance_passed = bool(
        source_governance.get("academic_mvp_source_governance_passed", False)
    )
    prototype_passed = bool(prototype_gate.get("passed_for_academic_mvp", False))
    teacher_passed = bool(teacher_gate.get("passed_for_academic_mvp", False))
    challenger_passed = bool(challenger_gate.get("passed_for_academic_mvp", False))
    prototype_contract_passed = bool(
        prototype_contract.get("prototype_contract_gate_passed", False)
    )
    v13_training_permission_passed = bool(
        teacher_gate.get("v13_training_permission_gate_passed", False)
    )
    market_submission_status = str(
        source_governance.get(
            "market_submission_receipt_gate_status",
            "blocked_external_access",
        )
    )
    lava_npz_validation = _mapping(prototype_gate.get("lava_npz_smoke_validation"))
    lava_npz_validation_passed = bool(
        prototype_gate.get("lava_npz_smoke_packet_validation_passed", False)
    )
    prototype_evidence_scorecard_passed = bool(
        prototype_evidence_scorecard.get("scorecard_passed_for_academic_mvp", False)
    )

    return {
        "operator_preview_gate": {
            "passed": operator_preview_passed,
            "status": "passed" if operator_preview_passed else "blocked",
            "claim_scope": "dam_delivery_day_operator_recommendation_preview",
            "market_execution_enabled": False,
        },
        "dam_bid_recommendation_preview_gate": {
            "passed": operator_preview_passed and bid_preview_rows > 0,
            "status": "passed"
            if operator_preview_passed and bid_preview_rows > 0
            else "blocked",
            "claim_scope": "non_submittable_dam_buy_sell_hold_preview",
            "bid_recommendation_preview_rows": bid_preview_rows,
            "buy_rows": _int_or_default(bid_preview_summary.get("buy_rows"), 0),
            "sell_rows": _int_or_default(bid_preview_summary.get("sell_rows"), 0),
            "hold_rows": _int_or_default(bid_preview_summary.get("hold_rows"), 0),
            "total_buy_mwh": _float_or_default(
                bid_preview_summary.get("total_buy_mwh"),
                0.0,
            ),
            "total_sell_mwh": _float_or_default(
                bid_preview_summary.get("total_sell_mwh"),
                0.0,
            ),
            "market_execution_enabled": False,
        },
        "academic_source_governance_gate": {
            "passed": source_governance_passed,
            "status": "passed" if source_governance_passed else "blocked",
            "claim_scope": "credentialless_academic_mvp_source_governance",
            "public_credentialless_source_observed": bool(
                source_governance.get("public_credentialless_source_observed", False)
            ),
            "publication_receipt_verified": bool(
                source_governance.get("publication_receipt_verified", False)
            ),
            "source_publication_timestamp_available": bool(
                source_governance.get(
                    "source_publication_timestamp_available",
                    False,
                )
            ),
            "market_availability_claim": bool(
                source_governance.get("market_availability_claim", False)
            ),
            "market_execution_enabled": False,
        },
        "market_submission_receipt_gate": {
            "passed": False,
            "status": market_submission_status,
            "claim_scope": "market_submission_grade_receipt_readiness",
            "required_for_academic_mvp": False,
            "market_execution_enabled": False,
        },
        "dt_lava_prototype_ci_smoke_gate": {
            "passed": prototype_passed,
            "status": "passed" if prototype_passed else "blocked",
            "claim_scope": "lava_npz_ci_smoke_validation_not_promotion",
            "market_execution_enabled": False,
        },
        "lava_npz_smoke_packet_validation_gate": {
            "passed": lava_npz_validation_passed,
            "status": "passed" if lava_npz_validation_passed else "blocked",
            "claim_scope": str(
                lava_npz_validation.get(
                    "claim_scope",
                    "lava_npz_margin_smoke_packet_validation_not_market_execution",
                )
            ),
            "artifact_hashes_valid": bool(
                lava_npz_validation.get("artifact_hashes_valid", False)
            ),
            "metrics_valid": bool(lava_npz_validation.get("metrics_valid", False)),
            "aggregate_valid": bool(
                lava_npz_validation.get("aggregate_valid", False)
            ),
            "npz_contract_valid": bool(
                lava_npz_validation.get("npz_contract_valid", False)
            ),
            "baseline_comparison_valid": bool(
                lava_npz_validation.get("baseline_comparison_valid", False)
            ),
            "permits_model_training": False,
            "promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "dfl_dt_prototype_contract_gate": {
            "passed": prototype_contract_passed,
            "status": "passed" if prototype_contract_passed else "blocked",
            "claim_scope": "dfl_dt_prototype_contract_not_promotion",
            "market_execution_enabled": False,
        },
        "v13_gated_teacher_contract_gate": {
            "passed": teacher_passed,
            "status": "passed" if teacher_passed else "blocked",
            "claim_scope": "candidate_index_or_schedule_family_teacher_contract",
            "teacher_packet_validation_passed": bool(
                _mapping(teacher_gate.get("teacher_packet_validation")).get(
                    "passed",
                    False,
                )
            ),
            "permitted_model_training_rows": _int_or_default(
                teacher_gate.get("permitted_model_training_rows"),
                0,
            ),
            "permits_model_training": False,
            "market_execution_enabled": False,
        },
        "offline_challenger_non_promotion_gate": {
            "passed": challenger_passed,
            "status": "passed" if challenger_passed else "blocked",
            "claim_scope": "offline_challenger_packet_explains_non_promotion",
            "offline_challenger_packet_validation_passed": bool(
                _mapping(
                    challenger_gate.get("offline_challenger_packet_validation")
                ).get("passed", False)
            ),
            "promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "prototype_evidence_scorecard_gate": {
            "passed": prototype_evidence_scorecard_passed,
            "status": "passed" if prototype_evidence_scorecard_passed else "blocked",
            "claim_scope": str(
                prototype_evidence_scorecard.get(
                    "claim_scope",
                    "credentialless_dfl_dt_prototype_evidence_scorecard_not_market_execution",
                )
            ),
            "operator_bid_preview_rows": _int_or_default(
                prototype_evidence_scorecard.get("operator_bid_preview_rows"),
                0,
            ),
            "teacher_train_selection_rows": _int_or_default(
                prototype_evidence_scorecard.get("teacher_train_selection_rows"),
                0,
            ),
            "validation_tenant_anchor_count": _int_or_default(
                prototype_evidence_scorecard.get("validation_tenant_anchor_count"),
                0,
            ),
            "permits_model_training": False,
            "promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "dt_research_shadow_smoke_gate": {
            "passed": bool(dt_research_shadow_gate.get("passed_for_academic_mvp", False)),
            "status": str(dt_research_shadow_gate.get("status", "not_configured")),
            "claim_scope": "dt_research_shadow_not_promotable_not_market_execution",
            "required_for_academic_mvp": False,
            "research_shadow_training_rows": _int_or_default(
                dt_research_shadow_gate.get("research_shadow_training_rows"),
                0,
            ),
            "promotable_v13_permitted_training_rows": _int_or_default(
                dt_research_shadow_gate.get("promotable_v13_permitted_training_rows"),
                0,
            ),
            "model_backbone": str(dt_research_shadow_gate.get("model_backbone", "")),
            "model_backbone_selection_reason": str(
                dt_research_shadow_gate.get("model_backbone_selection_reason", "")
            ),
            "hf_decision_transformer_available": bool(
                dt_research_shadow_gate.get("hf_decision_transformer_available", False)
            ),
            "state_contract_passed": bool(
                dt_research_shadow_gate.get("state_contract_passed", False)
            ),
            "reward_contract_passed": bool(
                dt_research_shadow_gate.get("reward_contract_passed", False)
            ),
            "action_feasibility_mask_attached": bool(
                dt_research_shadow_gate.get("action_feasibility_mask_attached", False)
            ),
            "action_feasibility_mask_applied_to_loss": bool(
                dt_research_shadow_gate.get(
                    "action_feasibility_mask_applied_to_loss",
                    False,
                )
            ),
            "action_feasibility_mask_applied_to_eval": bool(
                dt_research_shadow_gate.get(
                    "action_feasibility_mask_applied_to_eval",
                    False,
                )
            ),
            "infeasible_action_prediction_count": _int_or_default(
                dt_research_shadow_gate.get("infeasible_action_prediction_count"),
                -1,
            ),
            "evaluation_packet_primary_metric": str(
                dt_research_shadow_gate.get("evaluation_packet_primary_metric", "")
            ),
            "evaluation_packet_validation_passed": bool(
                dt_research_shadow_gate.get(
                    "evaluation_packet_validation_passed",
                    False,
                )
            ),
            "evaluation_packet_validation_source": str(
                dt_research_shadow_gate.get("evaluation_packet_validation_source", "")
            ),
            "publication_receipt_verified": False,
            "market_availability_claim": False,
            "promotion_gate_passed": False,
            "market_execution_enabled": False,
        },
        "dt_lava_training_promotion_gate": {
            "passed": False,
            "status": "ready_for_offline_training_benchmark"
            if v13_training_permission_passed
            else "blocked_until_v13_source_readiness",
            "claim_scope": "future_dt_lava_strict_lp_oracle_promotion",
            "required_for_academic_mvp": False,
            "market_execution_enabled": False,
        },
        "market_execution_safety_gate": {
            "passed": no_market_execution_safety_gate_passed,
            "status": "passed" if no_market_execution_safety_gate_passed else "blocked",
            "claim_scope": "prove_no_market_execution_enabled_true",
            "market_execution_enabled": False,
        },
        "market_execution_gate": {
            "passed": False,
            "status": "out_of_scope",
            "claim_scope": "future_market_execution_contract",
            "required_for_academic_mvp": False,
            "market_execution_enabled": False,
        },
    }


def _forbidden_market_payload_paths(value: object, *, prefix: str = "") -> list[str]:
    paths: list[str] = []
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key)
            path = f"{prefix}.{key_text}" if prefix else key_text
            if key_text in FORBIDDEN_MARKET_PAYLOAD_KEYS:
                paths.append(path)
            paths.extend(_forbidden_market_payload_paths(item, prefix=path))
    elif isinstance(value, list | tuple):
        for index, item in enumerate(value):
            path = f"{prefix}[{index}]" if prefix else f"[{index}]"
            paths.extend(_forbidden_market_payload_paths(item, prefix=path))
    return paths


def _next_gate(
    *,
    academic_mvp_gate_passed: bool,
    source_governance: Mapping[str, Any],
    prototype_gate: Mapping[str, Any],
    teacher_gate: Mapping[str, Any],
    challenger_gate: Mapping[str, Any],
) -> str:
    if not bool(source_governance.get("academic_mvp_source_governance_passed", False)):
        return "fix_academic_mvp_source_governance_packet"
    if not bool(prototype_gate.get("passed_for_academic_mvp", False)):
        return "fix_dt_lava_prototype_ci_smoke_packet"
    if not bool(teacher_gate.get("passed_for_academic_mvp", False)):
        return "fix_v13_gated_teacher_contract_packet"
    if not bool(challenger_gate.get("passed_for_academic_mvp", False)):
        return "fix_offline_challenger_non_promotion_packet"
    if academic_mvp_gate_passed:
        return "credentialless_academic_mvp_ready_for_thesis_demo"
    return "fix_operator_preview_gate"


def _render_markdown(summary: Mapping[str, Any]) -> str:
    source_governance = _mapping(summary.get("source_governance"))
    operator_gate = _mapping(summary.get("operator_preview_gate"))
    prototype_gate = _mapping(summary.get("dt_lava_prototype_gate"))
    teacher_gate = _mapping(summary.get("dt_lava_teacher_contract_gate"))
    challenger_gate = _mapping(summary.get("offline_challenger_gate"))
    prototype_contract = _mapping(summary.get("prototype_contract"))
    prototype_evidence_scorecard = _mapping(
        summary.get("prototype_evidence_scorecard")
    )
    dt_research_shadow_gate = _mapping(summary.get("dt_research_shadow_gate"))
    dt_research_shadow_metrics = _mapping(
        dt_research_shadow_gate.get("evaluation_metrics")
    )
    phase_readiness = _mapping(summary.get("prototype_phase_readiness"))
    bid_preview_summary = _mapping(operator_gate.get("bid_preview_summary"))
    control_comparison_summary = _mapping(
        challenger_gate.get("control_comparison_summary")
    )
    gate_passport = _mapping(summary.get("gate_passport"))
    market_execution_safety_gate = _mapping(
        gate_passport.get("market_execution_safety_gate")
    )
    return (
        "# Credentialless Academic MVP Readiness\n\n"
        f"- Academic MVP gate passed: `{summary['academic_mvp_gate_passed']}`\n"
        "- SCMO credentials are not required for the diploma MVP.\n"
        "- Missing SCMO credentials block only market-submission-grade receipt readiness.\n"
        f"- Market submission receipt gate: `{source_governance.get('market_submission_receipt_gate_status')}`\n"
        f"- Operator preview gate passed: `{operator_gate.get('passed')}`\n"
        f"- DAM bid recommendation preview rows: `{operator_gate.get('bid_recommendation_preview_rows')}`"
        " (non-submittable).\n"
        "- DAM preview buy/sell MWh: "
        f"`{bid_preview_summary.get('total_buy_mwh')}` / "
        f"`{bid_preview_summary.get('total_sell_mwh')}`.\n"
        f"- DT/LAVA prototype gate passed: `{prototype_gate.get('passed_for_academic_mvp')}`\n"
        "- DFL/DT prototype contract: "
        f"`{prototype_contract.get('prototype_contract_gate_passed')}`; "
        f"DT action target: `{prototype_contract.get('dt_action_target_contract')}`; "
        "evaluation: `strict_lp_oracle_regret_value_vs_v2_plus`.\n"
        "- Prototype evidence scorecard: "
        f"`{prototype_evidence_scorecard.get('scorecard_passed_for_academic_mvp')}`; "
        "bid-preview rows / teacher rows / challenger anchors: "
        f"`{prototype_evidence_scorecard.get('operator_bid_preview_rows')}` / "
        f"`{prototype_evidence_scorecard.get('teacher_train_selection_rows')}` / "
        f"`{prototype_evidence_scorecard.get('validation_tenant_anchor_count')}`.\n"
        "- DT research-shadow rows: "
        f"`{dt_research_shadow_gate.get('research_shadow_training_rows')}`; "
        "promotable V13 rows: "
        f"`{dt_research_shadow_gate.get('promotable_v13_permitted_training_rows')}`.\n"
        "- DT forecast-context coverage: "
        f"`{dt_research_shadow_gate.get('forecast_context_coverage_status')}`; "
        "present/missing: "
        f"`{dt_research_shadow_gate.get('forecast_context_present_families')}` / "
        f"`{dt_research_shadow_gate.get('forecast_context_missing_families')}`.\n"
        "- DT/V2+/strict/BC regret: "
        f"`{dt_research_shadow_metrics.get('dt_selected_mean_regret_uah')}` / "
        f"`{dt_research_shadow_metrics.get('v2_plus_mean_regret_uah')}` / "
        f"`{dt_research_shadow_metrics.get('strict_mean_regret_uah')}` / "
        f"`{dt_research_shadow_metrics.get('behavior_cloning_mean_regret_uah')}`.\n"
        "- DT/V2+/strict/BC value: "
        f"`{dt_research_shadow_metrics.get('dt_selected_mean_value_uah')}` / "
        f"`{dt_research_shadow_metrics.get('v2_plus_mean_value_uah')}` / "
        f"`{dt_research_shadow_metrics.get('strict_mean_value_uah')}` / "
        f"`{dt_research_shadow_metrics.get('behavior_cloning_mean_value_uah')}`.\n"
        "- Phase 0 V13 source readiness: "
        f"`{_mapping(phase_readiness.get('phase_0_v13_source_readiness')).get('status')}`.\n"
        "- Phase 1 LAVA NPZ smoke: "
        f"`{_mapping(phase_readiness.get('phase_1_lava_npz_smoke')).get('status')}`.\n"
        "- Phase 2 teacher contract: "
        f"`{_mapping(phase_readiness.get('phase_2_v13_gated_teacher_contract')).get('status')}`.\n"
        "- Phase 3 offline challenger: "
        f"`{_mapping(phase_readiness.get('phase_3_offline_challenger')).get('status')}`.\n"
        "- Phase 4 schedule-level DFL: "
        f"`{_mapping(phase_readiness.get('phase_4_full_schedule_dfl')).get('status')}`.\n"
        f"- Teacher contract gate passed: `{teacher_gate.get('passed_for_academic_mvp')}`\n"
        f"- Offline challenger packet gate passed: `{challenger_gate.get('passed_for_academic_mvp')}`\n"
        "- Offline challenger control anchors: "
        f"`{control_comparison_summary.get('validation_tenant_anchor_count')}`.\n"
        "- Market execution safety gate passed: "
        f"`{market_execution_safety_gate.get('passed')}`\n"
        "- DT/LAVA training remains blocked until V13 source-readiness gates pass.\n"
        "- `market_execution_enabled=false`.\n"
    )


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _sequence(value: object) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, tuple):
        return list(value)
    return []


def _int_or_default(value: object, default: int) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value)
    return default


def _float_or_none(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def _float_or_default(value: object, default: float) -> float:
    parsed = _float_or_none(value)
    return default if parsed is None else parsed


__all__ = [
    "CLAIM_SCOPE",
    "SUMMARY_JSON_NAME",
    "SUMMARY_MARKDOWN_NAME",
    "VALIDATION_CLAIM_SCOPE",
    "VALIDATION_JSON_NAME",
    "build_credentialless_academic_mvp_readiness_summary",
    "validate_credentialless_academic_mvp_readiness_summary",
    "write_credentialless_academic_mvp_readiness_packet",
]
