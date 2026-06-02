"""Audit future-date HF safe-switch guards on source-backed forecast rows."""

from __future__ import annotations

import argparse
from collections import defaultdict
from collections.abc import Mapping, Sequence
import csv
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
import sys
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for import_path in (PROJECT_ROOT, SRC_ROOT):
    import_path_text = str(import_path)
    if import_path_text not in sys.path:
        sys.path.insert(0, import_path_text)

from smart_arbitrage.dfl.hf_live_safe_switch_preview import (  # noqa: E402
    build_hf_live_safe_switch_candidate_rows,
    template_grid_specs,
)
from smart_arbitrage.dfl.hf_safe_switch_scorer import (  # noqa: E402
    load_hf_safe_switch_inference_bundle,
    score_hf_safe_switch_candidate_rows,
    select_hf_safe_switch_candidate,
    summarize_hf_safe_switch_guard,
)

CLAIM_SCOPE = "hf_live_safe_switch_forecast_guard_audit_shadow_not_promotable"
DEFAULT_CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "research_runs"
    / "week5_hf_live_safe_switch_inference_2026_06_01"
    / "hf_safe_switch_scorer_model_checkpoint"
)
BASELINE_TEMPLATE_GRID_ID = "candidate_library_value_aligned"
FORECAST_TEMPLATE_GRID_ID = "candidate_library_forecast_guarded"
DEFAULT_TEMPLATE_GRIDS = (BASELINE_TEMPLATE_GRID_ID, FORECAST_TEMPLATE_GRID_ID)
DEFAULT_MARKET_VENUES = ("DAM", "IDM")
DEFAULT_THRESHOLD_UAH = 100.0
MAX_PREDICTED_TAIL_RISK_PROBABILITY = 0.5
MAX_FAMILY_TAIL_RISK_PROBABILITY = 1.0


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only HF live safe-switch forecast guard audit. Uses source-backed "
            "pre-publication forecast rows only; no LP solve, no training, and no market payloads."
        )
    )
    parser.add_argument("--tenant-id", default="client_003_dnipro_factory")
    parser.add_argument("--market-venues", default=",".join(DEFAULT_MARKET_VENUES))
    parser.add_argument("--target-dates", default="")
    parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    parser.add_argument("--template-grids", default=",".join(DEFAULT_TEMPLATE_GRIDS))
    parser.add_argument("--run-slug", default="")
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)

    target_dates = _parse_target_dates(args.target_dates)
    market_venues = tuple(
        item.strip().upper() for item in args.market_venues.split(",") if item.strip()
    )
    template_grids = tuple(
        item.strip() for item in args.template_grids.split(",") if item.strip()
    )
    run_slug = args.run_slug or (
        "hf_live_safe_switch_forecast_guard_audit_"
        f"{target_dates[0].isoformat()}_{target_dates[-1].isoformat()}"
    )
    output_dir = args.output_dir or PROJECT_ROOT / "data" / "research_runs" / run_slug
    scored_blocks = collect_forecast_scored_blocks(
        tenant_id=args.tenant_id,
        market_venues=market_venues,
        target_dates=target_dates,
        checkpoint_dir=args.checkpoint_dir,
        template_grid_names=template_grids,
    )
    tables = build_forecast_guard_audit_tables(
        scored_blocks,
        default_threshold_uah=DEFAULT_THRESHOLD_UAH,
    )
    write_forecast_guard_audit_outputs(
        output_dir=output_dir,
        run_slug=run_slug,
        tenant_id=args.tenant_id,
        tables=tables,
    )
    return 0 if tables["summary"]["forecast_scored_block_count"] > 0 else 2


def collect_forecast_scored_blocks(
    *,
    tenant_id: str,
    market_venues: Sequence[str],
    target_dates: Sequence[date],
    checkpoint_dir: Path,
    template_grid_names: Sequence[str] = DEFAULT_TEMPLATE_GRIDS,
) -> list[dict[str, Any]]:
    """Score future/pre-publication forecast contexts without invoking LP."""

    from api import main as api_main  # noqa: PLC0415

    bundle = load_hf_safe_switch_inference_bundle(checkpoint_dir)
    battery_defaults = api_main._resolve_tenant_battery_defaults(tenant_id=tenant_id)  # noqa: SLF001
    blocks: list[dict[str, Any]] = []
    for market_venue in market_venues:
        resolved_market_venue = api_main._normalize_operator_market_venue(market_venue)  # noqa: SLF001
        for target_delivery_date in target_dates:
            try:
                price_context = api_main._build_official_oree_price_context(  # noqa: SLF001
                    market_venue=resolved_market_venue,
                    target_delivery_date=target_delivery_date,
                    selected_strategy_id=(
                        "nbeatsx_official_idm_v0"
                        if resolved_market_venue == "IDM"
                        else "nbeatsx_official_v0"
                    ),
                )
                validate_pre_publication_forecast_price_context(price_context)
            except Exception as error:  # noqa: BLE001
                blocks.append(
                    {
                        "target_delivery_date": target_delivery_date.isoformat(),
                        "market_venue": resolved_market_venue,
                        "template_grid_id": "not_scored",
                        "price_context_status": "blocked_no_complete_forecast_context",
                        "blocked_reason": str(error),
                        "scored_candidates": [],
                    }
                )
                continue
            load_frame = api_main._operator_load_frame(  # noqa: SLF001
                tenant_id=tenant_id,
                anchor_timestamp=price_context.anchor_timestamp,
            )
            soc_resolution = api_main._resolve_operator_soc(  # noqa: SLF001
                tenant_id=tenant_id,
                battery_defaults=battery_defaults,
                load_frame=load_frame,
            )
            for template_grid_id in template_grid_names:
                candidate_rows = build_hf_live_safe_switch_candidate_rows(
                    tenant_id=tenant_id,
                    source_model_name=price_context.forecast_model_name
                    or f"forecast_{resolved_market_venue.lower()}_live",
                    anchor_timestamp=price_context.anchor_timestamp,
                    forecast=price_context.delivery_forecast,
                    battery_metrics=battery_defaults.metrics,
                    starting_soc_fraction=soc_resolution.starting_soc_fraction,
                    candidate_families=bundle.candidate_families,
                    template_specs=template_grid_specs(template_grid_id),
                )
                score_result = score_hf_safe_switch_candidate_rows(
                    bundle=bundle,
                    candidate_rows=candidate_rows,
                )
                blocks.append(
                    {
                        "target_delivery_date": price_context.target_delivery_date.isoformat(),
                        "market_venue": resolved_market_venue,
                        "template_grid_id": template_grid_id,
                        "price_context_status": price_context.price_context_status,
                        "forecast_model_name": price_context.forecast_model_name,
                        "selected_schedule_family": score_result["selected_schedule_family"],
                        "selection_reason": score_result["selection_reason"],
                        "abstained_to_v2_plus": bool(score_result["abstained_to_v2_plus"]),
                        "scored_candidates": score_result["scored_candidates"],
                    }
                )
    return blocks


def validate_pre_publication_forecast_price_context(price_context: Any) -> None:
    """Reject official, synthetic, or incomplete contexts for this forecast audit."""

    status = str(getattr(price_context, "price_context_status", ""))
    forecast = list(getattr(price_context, "delivery_forecast", []) or [])
    if status != "pre_publication_forecast":
        raise ValueError(f"Expected pre_publication_forecast rows, got {status!r}.")
    if len(forecast) != 24:
        raise ValueError(f"Expected 24 hourly forecast rows, got {len(forecast)}.")


def build_forecast_guard_audit_tables(
    scored_blocks: Sequence[Mapping[str, Any]],
    *,
    default_threshold_uah: float = DEFAULT_THRESHOLD_UAH,
    max_predicted_tail_risk_probability: float = MAX_PREDICTED_TAIL_RISK_PROBABILITY,
    max_family_tail_risk_probability: float = MAX_FAMILY_TAIL_RISK_PROBABILITY,
) -> dict[str, Any]:
    candidate_rows: list[dict[str, Any]] = []
    summary_rows: list[dict[str, Any]] = []
    blocked_contexts = [
        block for block in scored_blocks if not list(block.get("scored_candidates", []) or [])
    ]
    for block in scored_blocks:
        scored_candidates = list(block.get("scored_candidates", []) or [])
        if not scored_candidates:
            continue
        summary_row = _forecast_guard_summary_row(
            block,
            threshold_uah=default_threshold_uah,
            max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
            max_family_tail_risk_probability=max_family_tail_risk_probability,
        )
        summary_rows.append(summary_row)
        for candidate in scored_candidates:
            candidate_rows.append(
                _candidate_score_row(
                    candidate,
                    target_delivery_date=str(block["target_delivery_date"]),
                    market_venue=str(block["market_venue"]),
                    template_grid_id=str(block["template_grid_id"]),
                    price_context_status=str(block["price_context_status"]),
                    threshold_uah=default_threshold_uah,
                    max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
                    max_family_tail_risk_probability=max_family_tail_risk_probability,
                )
            )
    summary = _summary_payload(
        candidate_rows=candidate_rows,
        summary_rows=summary_rows,
        blocked_context_count=len(blocked_contexts),
    )
    return {
        "candidate_scores": candidate_rows,
        "forecast_guard_summary": summary_rows,
        "summary": summary,
    }


def write_forecast_guard_audit_outputs(
    *,
    output_dir: Path,
    run_slug: str,
    tenant_id: str,
    tables: Mapping[str, Any],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "candidate_scores_csv": output_dir / "candidate_scores.csv",
        "forecast_guard_summary_csv": output_dir / "forecast_guard_summary.csv",
        "summary_json": output_dir / "summary.json",
        "summary_md": output_dir / "summary.md",
    }
    _write_csv(paths["candidate_scores_csv"], tables["candidate_scores"])
    _write_csv(paths["forecast_guard_summary_csv"], tables["forecast_guard_summary"])
    summary = {
        **dict(tables["summary"]),
        "run_slug": run_slug,
        "tenant_id": tenant_id,
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
        "proposed_bid_emitted": False,
        "market_order_payload_emitted": False,
    }
    paths["summary_json"].write_text(
        json.dumps(summary, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    paths["summary_md"].write_text(_summary_markdown(summary), encoding="utf-8")
    return paths


def _forecast_guard_summary_row(
    block: Mapping[str, Any],
    *,
    threshold_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float,
) -> dict[str, Any]:
    scored_candidates = list(block["scored_candidates"])
    selection = select_hf_safe_switch_candidate(
        scored_candidates,
        threshold_uah=threshold_uah,
        max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
    )
    selected = dict(selection["selected_candidate"])
    diagnostics = summarize_hf_safe_switch_guard(
        scored_candidates,
        selected_candidate=selected,
        threshold_uah=threshold_uah,
        max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
        max_family_tail_risk_probability=max_family_tail_risk_probability,
    )
    selected_value = _candidate_value(selected)
    return {
        "target_delivery_date": str(block["target_delivery_date"]),
        "market_venue": str(block["market_venue"]),
        "template_grid_id": str(block["template_grid_id"]),
        "price_context_status": str(block["price_context_status"]),
        "threshold_uah": float(threshold_uah),
        "selected_schedule_family": str(selection["selected_schedule_family"]),
        "selection_reason": str(selection["selection_reason"]),
        "abstained_to_v2_plus": bool(selection["abstained_to_v2_plus"]),
        "switch_selected_nonfallback": not bool(selection["abstained_to_v2_plus"]),
        "selected_schedule_value_uah": selected_value,
        "best_template_schedule_value_uah": float(diagnostics["best_template_schedule_value_uah"]),
        "selected_vs_best_template_value_gap_uah": float(
            diagnostics["selected_vs_best_template_value_gap_uah"]
        ),
        "threshold_guard_failed_count": float(diagnostics["threshold_guard_failed_count"]),
        "predicted_tail_guard_failed_count": float(
            diagnostics["predicted_tail_guard_failed_count"]
        ),
        "family_tail_guard_failed_count": float(diagnostics["family_tail_guard_failed_count"]),
        "safety_guard_failed_count": float(diagnostics["safety_guard_failed_count"]),
        "eligible_nonfallback_candidate_count": float(
            diagnostics["eligible_nonfallback_candidate_count"]
        ),
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
    }


def _candidate_score_row(
    candidate: Mapping[str, Any],
    *,
    target_delivery_date: str,
    market_venue: str,
    template_grid_id: str,
    price_context_status: str,
    threshold_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float,
) -> dict[str, Any]:
    predicted_delta = float(candidate["predicted_regret_delta_vs_v2_plus_uah"])
    predicted_tail = float(candidate["predicted_tail_risk_probability"])
    family_tail = float(candidate.get("family_tail_risk_probability", 0.0))
    safety_violations = int(candidate.get("safety_violation_count", 0) or 0)
    return {
        "target_delivery_date": target_delivery_date,
        "market_venue": market_venue,
        "template_grid_id": template_grid_id,
        "price_context_status": price_context_status,
        "candidate_family": str(candidate["dt_schedule_family_target"]),
        "candidate_id": str(candidate["dt_candidate_id_target"]),
        "predicted_regret_delta_vs_v2_plus_uah": predicted_delta,
        "predicted_tail_risk_probability": predicted_tail,
        "family_tail_risk_probability": family_tail,
        "schedule_value_uah": _candidate_value(candidate),
        "total_throughput_mwh": float(candidate.get("total_throughput_mwh", 0.0)),
        "safety_violation_count": float(safety_violations),
        "template_clip_count": float(candidate.get("template_clip_count", 0.0)),
        "threshold_guard_passed": predicted_delta < -float(threshold_uah),
        "predicted_tail_guard_passed": (
            predicted_tail <= max_predicted_tail_risk_probability
        ),
        "family_tail_guard_passed": family_tail <= max_family_tail_risk_probability,
        "safety_guard_passed": safety_violations == 0,
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
    }


def _summary_payload(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    summary_rows: Sequence[Mapping[str, Any]],
    blocked_context_count: int,
) -> dict[str, Any]:
    update_gate = _forecast_update_gate(summary_rows)
    passed = bool(update_gate["forecast_candidate_library_update_gate_passed"])
    return {
        "claim_scope": CLAIM_SCOPE,
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "source_price_scope": "source_backed_pre_publication_forecast_rows_only",
        "forecast_scored_block_count": float(len(summary_rows)),
        "blocked_context_count": float(blocked_context_count),
        "candidate_score_row_count": float(len(candidate_rows)),
        "forecast_guard_summary_row_count": float(len(summary_rows)),
        "forecast_candidate_library_audit_passed": passed,
        "recommended_action": (
            "manual_review_required_before_forecast_grid_update"
            if passed
            else "keep_current_forecast_hold_candidate_library_needs_redesign"
        ),
        "summary_message": (
            "candidate_library_forecast_guarded passes read-only forecast guard checks; manual review is required before any live switch."
            if passed
            else "forecast-date HF abstains correctly; forecast candidate library still weak."
        ),
        **update_gate,
    }


def _forecast_update_gate(summary_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    grouped = _aggregate_by_template_grid(summary_rows)
    baseline = grouped.get(BASELINE_TEMPLATE_GRID_ID)
    candidate = grouped.get(FORECAST_TEMPLATE_GRID_ID)
    if baseline is None or candidate is None:
        return _failed_update_gate("missing_baseline_or_forecast_grid_summary")
    tail_failure_delta = (
        float(candidate["predicted_tail_guard_failed_count"])
        - float(baseline["predicted_tail_guard_failed_count"])
    )
    selected_value_improvement = (
        float(candidate["mean_selected_schedule_value_uah"])
        - float(baseline["mean_selected_schedule_value_uah"])
    )
    safety_failures = float(candidate["safety_guard_failed_count"])
    switch_count = float(candidate["switch_count"])
    reason = ""
    if safety_failures > 0.0:
        reason = "forecast_grid_safety_failures"
    elif tail_failure_delta > 0.0:
        reason = "forecast_grid_tail_risk_failures_increased"
    elif switch_count < 1.0:
        reason = "no_guard_passing_forecast_nonfallback"
    elif selected_value_improvement <= 0.0:
        reason = "forecast_grid_selected_value_not_improved"
    passed = reason == ""
    return {
        "forecast_candidate_library_update_gate_passed": passed,
        "update_gate_threshold_uah": DEFAULT_THRESHOLD_UAH,
        "update_gate_baseline_template_grid_id": BASELINE_TEMPLATE_GRID_ID,
        "update_gate_candidate_template_grid_id": FORECAST_TEMPLATE_GRID_ID,
        "candidate_library_forecast_guarded_switch_count": switch_count,
        "candidate_library_forecast_guarded_selected_value_improvement_uah": selected_value_improvement,
        "candidate_library_forecast_guarded_tail_failure_delta_count": tail_failure_delta,
        "candidate_library_forecast_guarded_safety_failure_count": safety_failures,
        "update_gate_failed_reason": reason,
    }


def _aggregate_by_template_grid(
    summary_rows: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, float]]:
    grouped: defaultdict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in summary_rows:
        grouped[str(row["template_grid_id"])].append(row)
    aggregates: dict[str, dict[str, float]] = {}
    for template_grid_id, rows in grouped.items():
        aggregates[template_grid_id] = {
            "switch_count": float(
                sum(1.0 for row in rows if bool(row["switch_selected_nonfallback"]))
            ),
            "mean_selected_schedule_value_uah": _mean(
                [float(row["selected_schedule_value_uah"]) for row in rows]
            ),
            "predicted_tail_guard_failed_count": float(
                sum(float(row["predicted_tail_guard_failed_count"]) for row in rows)
            ),
            "safety_guard_failed_count": float(
                sum(float(row["safety_guard_failed_count"]) for row in rows)
            ),
        }
    return aggregates


def _failed_update_gate(reason: str) -> dict[str, Any]:
    return {
        "forecast_candidate_library_update_gate_passed": False,
        "update_gate_threshold_uah": DEFAULT_THRESHOLD_UAH,
        "update_gate_baseline_template_grid_id": BASELINE_TEMPLATE_GRID_ID,
        "update_gate_candidate_template_grid_id": FORECAST_TEMPLATE_GRID_ID,
        "candidate_library_forecast_guarded_switch_count": 0.0,
        "candidate_library_forecast_guarded_selected_value_improvement_uah": 0.0,
        "candidate_library_forecast_guarded_tail_failure_delta_count": 0.0,
        "candidate_library_forecast_guarded_safety_failure_count": 0.0,
        "update_gate_failed_reason": reason,
    }


def _parse_target_dates(raw_value: str) -> tuple[date, ...]:
    if not raw_value.strip():
        today = datetime.now(tz=UTC).date()
        return (today + timedelta(days=1), today + timedelta(days=2))
    target_dates = tuple(
        date.fromisoformat(item.strip())
        for item in raw_value.split(",")
        if item.strip()
    )
    if not target_dates:
        raise ValueError("At least one target date is required.")
    return target_dates


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = sorted({key for row in rows for key in row})
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _summary_markdown(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# HF Live Safe-Switch Forecast Guard Audit",
            "",
            f"- Claim scope: `{summary['claim_scope']}`",
            f"- Tenant: `{summary['tenant_id']}`",
            f"- Scored forecast blocks: {summary['forecast_scored_block_count']}",
            f"- Blocked contexts: {summary['blocked_context_count']}",
            f"- Forecast grid update gate: {summary['forecast_candidate_library_update_gate_passed']}",
            f"- Recommended action: `{summary['recommended_action']}`",
            f"- Message: {summary['summary_message']}",
            "",
            "No LP solve, no training, no ProposedBid, and no market order payloads were emitted.",
            "",
        ]
    )


def _candidate_value(candidate: Mapping[str, Any]) -> float:
    value = candidate.get("schedule_value_uah")
    if value is None:
        value = candidate.get("decision_value_uah", 0.0)
    return float(value)


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


if __name__ == "__main__":
    raise SystemExit(main())
