"""Materialize four-case HF live safe-switch shadow demo evidence."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import csv
from dataclasses import dataclass
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any, Literal
from urllib.parse import urlencode
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RUN_SLUG = "hf_live_safe_switch_shadow_demo_evidence_2026_06_01"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data" / "research_runs" / DEFAULT_RUN_SLUG
PREVIEW_SOURCE_ID = "hf_live_safe_switch_value_aligned_shadow"
SAFE_FALLBACK_FAMILY = "schedule_value_learner_v2_plus"

ExpectedOutcome = Literal[
    "official_nonfallback",
    "forecast_nonfallback",
    "guarded_abstention",
]


@dataclass(frozen=True)
class DemoCaseSpec:
    case_id: str
    label: str
    market_venue: str
    target_delivery_date: str
    expected_outcome: ExpectedOutcome


DEFAULT_DEMO_CASES: tuple[DemoCaseSpec, ...] = (
    DemoCaseSpec(
        case_id="official_dam_proof",
        label="Official DAM proof",
        market_venue="DAM",
        target_delivery_date="2026-05-02",
        expected_outcome="official_nonfallback",
    ),
    DemoCaseSpec(
        case_id="forecast_dam_action",
        label="Forecast DAM action",
        market_venue="DAM",
        target_delivery_date="2026-06-02",
        expected_outcome="forecast_nonfallback",
    ),
    DemoCaseSpec(
        case_id="forecast_dam_abstention",
        label="Forecast DAM abstention",
        market_venue="DAM",
        target_delivery_date="2026-06-03",
        expected_outcome="guarded_abstention",
    ),
    DemoCaseSpec(
        case_id="forecast_idm_abstention",
        label="IDM abstention",
        market_venue="IDM",
        target_delivery_date="2026-06-02",
        expected_outcome="guarded_abstention",
    ),
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Capture durable HF live safe-switch shadow demo evidence. The packet "
            "is preview-only and refuses market-execution artifacts."
        )
    )
    parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--tenant-id", default="client_003_dnipro_factory")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
    args = parser.parse_args(argv)

    responses = {
        spec.case_id: _fetch_shadow_preview(
            api_base_url=args.api_base_url,
            tenant_id=args.tenant_id,
            market_venue=spec.market_venue,
            target_delivery_date=spec.target_delivery_date,
        )
        for spec in DEFAULT_DEMO_CASES
    }
    packet = build_hf_live_safe_switch_shadow_demo_evidence(
        run_slug=args.run_slug,
        tenant_id=args.tenant_id,
        case_responses=responses,
    )
    paths = write_hf_live_safe_switch_shadow_demo_evidence(
        output_dir=args.output_dir,
        packet=packet,
        response_payloads=responses,
    )
    result = {
        "summary_json": str(paths["summary_json"]),
        "summary_md": str(paths["summary_md"]),
        "demo_cases_csv": str(paths["demo_cases_csv"]),
        "response_dir": str(paths["response_dir"]),
        "demo_evidence_passed": packet["demo_evidence_passed"],
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "market_order_payload_emitted": False,
    }
    json.dump(result, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0 if packet["demo_evidence_passed"] else 2


def build_hf_live_safe_switch_shadow_demo_evidence(
    *,
    run_slug: str,
    tenant_id: str,
    case_responses: Mapping[str, Mapping[str, Any]],
    case_specs: Sequence[DemoCaseSpec] = DEFAULT_DEMO_CASES,
) -> dict[str, Any]:
    cases = [
        _demo_case(spec, case_responses[spec.case_id])
        for spec in case_specs
        if spec.case_id in case_responses
    ]
    flags_safe = all(
        not _truthy(case[key])
        for case in cases
        for key in (
            "market_execution_enabled",
            "market_order_payload_emitted",
            "promotion_gate_passed",
            "dt_lava_ready",
            "prohibited_market_payload_present",
        )
    )
    demo_evidence_passed = bool(cases) and flags_safe and all(
        _truthy(case["case_passed"]) for case in cases
    )
    return {
        "run_slug": run_slug,
        "claim_scope": "hf_live_safe_switch_shadow_demo_evidence",
        "tenant_id": tenant_id,
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "demo_evidence_passed": demo_evidence_passed,
        "case_count": float(len(cases)),
        "nonfallback_case_count": float(
            sum(1 for case in cases if str(case["selected_schedule_family"]) != SAFE_FALLBACK_FAMILY)
        ),
        "guarded_abstention_case_count": float(
            sum(1 for case in cases if _truthy(case["guard_abstained_to_safe_fallback"]))
        ),
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "production_market_promotion_gate_passed": False,
        "proposed_bid_emitted": False,
        "market_order_payload_emitted": False,
        "cases": cases,
    }


def write_hf_live_safe_switch_shadow_demo_evidence(
    *,
    output_dir: Path,
    packet: Mapping[str, Any],
    response_payloads: Mapping[str, Mapping[str, Any]] | None = None,
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    response_dir = output_dir / "responses"
    response_dir.mkdir(exist_ok=True)
    paths = {
        "summary_json": output_dir / "summary.json",
        "summary_md": output_dir / "summary.md",
        "demo_cases_csv": output_dir / "demo_cases.csv",
        "response_dir": response_dir,
    }
    paths["summary_json"].write_text(
        json.dumps(dict(packet), indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    paths["summary_md"].write_text(_summary_markdown(packet), encoding="utf-8")
    cases = packet.get("cases", [])
    if not isinstance(cases, Sequence):
        raise ValueError("packet cases must be a sequence.")
    _write_csv(paths["demo_cases_csv"], [dict(case) for case in cases if isinstance(case, Mapping)])
    for case_id, payload in (response_payloads or {}).items():
        (response_dir / f"{case_id}.json").write_text(
            json.dumps(dict(payload), indent=2, ensure_ascii=False),
            encoding="utf-8",
        )
    return paths


def _fetch_shadow_preview(
    *,
    api_base_url: str,
    tenant_id: str,
    market_venue: str,
    target_delivery_date: str,
) -> dict[str, Any]:
    query = {
        "tenant_id": tenant_id,
        "preview_source": PREVIEW_SOURCE_ID,
        "market_venue": market_venue,
        "target_delivery_date": target_delivery_date,
    }
    url = f"{api_base_url.rstrip('/')}/dashboard/shadow-recommendation-preview?{urlencode(query)}"
    with urlopen(url, timeout=60) as response:  # noqa: S310 - local operator demo endpoint.
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("shadow preview response must be a JSON object.")
    return payload


def _demo_case(spec: DemoCaseSpec, response: Mapping[str, Any]) -> dict[str, Any]:
    metrics = response.get("comparison_metrics", {})
    if not isinstance(metrics, Mapping):
        metrics = {}
    schedule = response.get("recommendation_schedule", [])
    if not isinstance(schedule, Sequence):
        schedule = []

    selected_family = str(response.get("selected_schedule_family", ""))
    non_hold_count = _non_hold_count(schedule)
    selected_value = _number(metrics.get("selected_candidate_estimated_value_uah"))
    guard_abstained = _number(metrics.get("guard_abstained_to_safe_fallback")) >= 1.0 or _number(
        metrics.get("forecast_guard_abstained_to_safe_fallback")
    ) >= 1.0
    common_safe = (
        response.get("preview_source_id") == PREVIEW_SOURCE_ID
        and str(response.get("market_venue", "")).upper() == spec.market_venue
        and not _truthy(response.get("market_execution_enabled"))
        and not _truthy(response.get("market_order_payload_emitted"))
        and not _truthy(response.get("promotion_gate_passed"))
        and not _truthy(response.get("dt_lava_ready"))
        and not _has_market_payload(response)
    )
    case_passed = common_safe and _expected_outcome_passed(
        spec=spec,
        metrics=metrics,
        selected_family=selected_family,
        non_hold_count=non_hold_count,
        selected_value=selected_value,
        guard_abstained=guard_abstained,
    )
    return {
        "case_id": spec.case_id,
        "case_label": spec.label,
        "market_venue": spec.market_venue,
        "target_delivery_date": spec.target_delivery_date,
        "expected_outcome": spec.expected_outcome,
        "case_passed": case_passed,
        "preview_source_id": str(response.get("preview_source_id", "")),
        "preview_status": str(response.get("preview_status", "")),
        "selected_schedule_family": selected_family,
        "target_delivery_window_start": str(response.get("target_delivery_window_start", "")),
        "schedule_row_count": float(len(schedule)),
        "non_hold_row_count": float(non_hold_count),
        "selected_candidate_estimated_value_uah": selected_value,
        "predicted_regret_delta_vs_v2_plus_uah": _number(
            metrics.get("predicted_regret_delta_vs_v2_plus_uah")
        ),
        "source_backed_price_context": _number(metrics.get("source_backed_price_context")),
        "official_context_published": _number(metrics.get("official_context_published")),
        "forecast_context_pre_publication": _number(metrics.get("forecast_context_pre_publication")),
        "forecast_guard_audit_passed": _number(metrics.get("forecast_guard_audit_passed")),
        "candidate_template_grid_forecast_guarded": _number(
            metrics.get("candidate_template_grid_forecast_guarded")
        ),
        "guard_abstained_to_safe_fallback": guard_abstained,
        "market_execution_enabled": _truthy(response.get("market_execution_enabled")),
        "market_order_payload_emitted": _truthy(response.get("market_order_payload_emitted")),
        "promotion_gate_passed": _truthy(response.get("promotion_gate_passed")),
        "dt_lava_ready": _truthy(response.get("dt_lava_ready")),
        "prohibited_market_payload_present": _has_market_payload(response),
    }


def _expected_outcome_passed(
    *,
    spec: DemoCaseSpec,
    metrics: Mapping[str, Any],
    selected_family: str,
    non_hold_count: int,
    selected_value: float,
    guard_abstained: bool,
) -> bool:
    source_backed = _number(metrics.get("source_backed_price_context")) >= 1.0
    if spec.expected_outcome == "official_nonfallback":
        return (
            source_backed
            and _number(metrics.get("official_context_published")) >= 1.0
            and selected_family != SAFE_FALLBACK_FAMILY
            and non_hold_count > 0
            and selected_value > 0.0
        )
    if spec.expected_outcome == "forecast_nonfallback":
        return (
            source_backed
            and _number(metrics.get("forecast_context_pre_publication")) >= 1.0
            and _number(metrics.get("candidate_template_grid_forecast_guarded")) >= 1.0
            and _number(metrics.get("forecast_guard_audit_passed")) >= 1.0
            and selected_family != SAFE_FALLBACK_FAMILY
            and non_hold_count > 0
            and selected_value > 0.0
        )
    return (
        source_backed
        and _number(metrics.get("forecast_context_pre_publication")) >= 1.0
        and selected_family == SAFE_FALLBACK_FAMILY
        and non_hold_count == 0
        and guard_abstained
    )


def _summary_markdown(packet: Mapping[str, Any]) -> str:
    lines = [
        "# HF Live Safe-Switch Shadow Demo Evidence",
        "",
        f"- Run slug: `{packet['run_slug']}`",
        f"- Demo evidence passed: `{packet['demo_evidence_passed']}`",
        f"- Case count: `{packet['case_count']}`",
        f"- Non-fallback cases: `{packet['nonfallback_case_count']}`",
        f"- Guarded abstention cases: `{packet['guarded_abstention_case_count']}`",
        "- Market execution enabled: `False`",
        "- Promotion gate passed: `False`",
        "- ProposedBid emitted: `False`",
        "- Market order payload emitted: `False`",
        "",
        "## Cases",
    ]
    for case in packet.get("cases", []):
        if not isinstance(case, Mapping):
            continue
        lines.append(
            "- "
            f"{case['case_label']}: `{case['selected_schedule_family']}` / "
            f"{int(float(case['non_hold_row_count']))} non-HOLD rows / "
            f"passed=`{case['case_passed']}`"
        )
    lines.append("")
    lines.append("This packet is shadow/demo evidence only, not V13 training or market execution.")
    return "\n".join(lines)


def _non_hold_count(schedule: Sequence[Any]) -> int:
    count = 0
    for row in schedule:
        if not isinstance(row, Mapping):
            continue
        action = str(row.get("action", "")).strip().lower()
        power = abs(_number(row.get("recommended_net_power_mw")))
        if action not in {"", "hold"} or power >= 0.005:
            count += 1
    return count


def _has_market_payload(response: Mapping[str, Any]) -> bool:
    return any(key in response for key in ("proposed_bid", "market_order_payload", "market_order"))


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def _number(value: object) -> float:
    if isinstance(value, bool):
        return 1.0 if value else 0.0
    if isinstance(value, int | float | str):
        return float(value)
    return 0.0


def _truthy(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, int | float):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes"}
    return False


if __name__ == "__main__":
    raise SystemExit(main())
