"""Audit live HF safe-switch selection guards on source-backed OREE days."""

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
    DEFAULT_TEMPLATE_GRIDS,
    build_hf_live_safe_switch_candidate_rows,
    template_grid_specs,
)
from smart_arbitrage.dfl.hf_safe_switch_scorer import (  # noqa: E402
    load_hf_safe_switch_inference_bundle,
    score_hf_safe_switch_candidate_rows,
    select_hf_safe_switch_candidate,
    summarize_hf_safe_switch_guard,
)

CLAIM_SCOPE = "hf_live_safe_switch_selection_audit_shadow_not_promotable"
DEFAULT_CHECKPOINT_DIR = (
    PROJECT_ROOT
    / "data"
    / "research_runs"
    / "week5_hf_live_safe_switch_inference_2026_06_01"
    / "hf_safe_switch_scorer_model_checkpoint"
)
UPDATE_GATE_THRESHOLD_UAH = 100.0
MAX_VALUE_GAP_RATIO_FOR_BUNDLE_UPDATE = 0.75
UPDATE_GATE_CANDIDATE_TEMPLATE_GRIDS = (
    "candidate_library_value_aligned",
    "candidate_library_v2",
)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Read-only HF live safe-switch selection audit. Uses source-backed "
            "OREE rows only; no LP solve, no training, and no market payloads."
        )
    )
    parser.add_argument("--tenant-id", default="client_003_dnipro_factory")
    parser.add_argument("--market-venue", default="DAM")
    parser.add_argument("--start-date", type=_parse_date, required=True)
    parser.add_argument("--end-date", type=_parse_date, required=True)
    parser.add_argument("--checkpoint-dir", type=Path, default=DEFAULT_CHECKPOINT_DIR)
    parser.add_argument("--thresholds-uah", default="50,75,90,95,100")
    parser.add_argument("--template-grid", default=",".join(DEFAULT_TEMPLATE_GRIDS))
    parser.add_argument(
        "--run-slug",
        default=f"hf_live_safe_switch_selection_audit_{datetime.now(tz=UTC):%Y_%m_%d}",
    )
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args(argv)

    output_dir = args.output_dir or PROJECT_ROOT / "data" / "research_runs" / args.run_slug
    thresholds = _parse_thresholds(args.thresholds_uah)
    template_grid_names = tuple(
        item.strip() for item in args.template_grid.split(",") if item.strip()
    )
    scored_blocks = collect_live_scored_blocks(
        tenant_id=args.tenant_id,
        market_venue=args.market_venue,
        start_date=args.start_date,
        end_date=args.end_date,
        checkpoint_dir=args.checkpoint_dir,
        template_grid_names=template_grid_names,
    )
    tables = build_selection_audit_tables(
        scored_blocks,
        thresholds_uah=thresholds,
        default_threshold_uah=100.0 if 100.0 in thresholds else thresholds[-1],
    )
    write_selection_audit_outputs(
        output_dir=output_dir,
        run_slug=args.run_slug,
        tenant_id=args.tenant_id,
        market_venue=args.market_venue,
        thresholds_uah=thresholds,
        tables=tables,
    )
    return 0 if scored_blocks else 2


def collect_live_scored_blocks(
    *,
    tenant_id: str,
    market_venue: str,
    start_date: date,
    end_date: date,
    checkpoint_dir: Path,
    template_grid_names: Sequence[str] = DEFAULT_TEMPLATE_GRIDS,
) -> list[dict[str, Any]]:
    """Score official OREE contexts without invoking LP or synthetic fallbacks."""

    from api import main as api_main  # noqa: PLC0415

    if end_date < start_date:
        raise ValueError("end_date must be on or after start_date.")
    bundle = load_hf_safe_switch_inference_bundle(checkpoint_dir)
    battery_defaults = api_main._resolve_tenant_battery_defaults(tenant_id=tenant_id)  # noqa: SLF001
    blocks: list[dict[str, Any]] = []
    for target_date in _date_range(start_date, end_date):
        try:
            price_context = api_main._build_official_oree_price_context(  # noqa: SLF001
                market_venue=market_venue,
                target_delivery_date=target_date,
                selected_strategy_id=(
                    "nbeatsx_official_idm_v0"
                    if market_venue.upper() == "IDM"
                    else "nbeatsx_official_v0"
                ),
            )
            validate_source_backed_price_context(price_context)
        except Exception as error:  # noqa: BLE001
            blocks.append(
                {
                    "target_delivery_date": target_date.isoformat(),
                    "template_grid_id": "not_scored",
                    "price_context_status": "blocked_no_complete_official_oree_row",
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
                or f"official_oree_{market_venue.lower()}_live",
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
                    "template_grid_id": template_grid_id,
                    "price_context_status": price_context.price_context_status,
                    "selected_schedule_family": score_result["selected_schedule_family"],
                    "selection_reason": score_result["selection_reason"],
                    "abstained_to_v2_plus": bool(score_result["abstained_to_v2_plus"]),
                    "scored_candidates": score_result["scored_candidates"],
                }
            )
    return [block for block in blocks if block["scored_candidates"]]


def validate_source_backed_price_context(price_context: Any) -> None:
    """Reject pre-publication, synthetic, or incomplete price contexts."""

    status = str(getattr(price_context, "price_context_status", ""))
    forecast = list(getattr(price_context, "delivery_forecast", []) or [])
    if status != "official_published":
        raise ValueError(f"Expected official_published OREE rows, got {status!r}.")
    if len(forecast) != 24:
        raise ValueError(f"Expected 24 hourly official OREE rows, got {len(forecast)}.")


def build_selection_audit_tables(
    scored_blocks: Sequence[Mapping[str, Any]],
    *,
    thresholds_uah: Sequence[float],
    default_threshold_uah: float = 100.0,
    max_predicted_tail_risk_probability: float = 0.5,
    max_family_tail_risk_probability: float = 1.0,
) -> dict[str, Any]:
    candidate_rows: list[dict[str, Any]] = []
    threshold_rows: list[dict[str, Any]] = []
    selected_by_default: dict[tuple[str, str], str] = {}
    for block in scored_blocks:
        scored_candidates = list(block.get("scored_candidates", []) or [])
        if not scored_candidates:
            continue
        target_date = str(block["target_delivery_date"])
        template_grid_id = str(block.get("template_grid_id", "default"))
        default_selection = select_hf_safe_switch_candidate(
            scored_candidates,
            threshold_uah=default_threshold_uah,
            max_predicted_tail_risk_probability=max_predicted_tail_risk_probability,
            max_family_tail_risk_probability=max_family_tail_risk_probability,
        )
        selected_by_default[(target_date, template_grid_id)] = str(
            default_selection["selected_schedule_family"]
        )
        for candidate in scored_candidates:
            candidate_rows.append(
                _candidate_score_row(
                    candidate,
                    target_delivery_date=target_date,
                    template_grid_id=template_grid_id,
                    threshold_uah=default_threshold_uah,
                    max_predicted_tail_risk_probability=(
                        max_predicted_tail_risk_probability
                    ),
                    max_family_tail_risk_probability=max_family_tail_risk_probability,
                )
            )
    for template_grid_id in sorted({row["template_grid_id"] for row in candidate_rows}):
        grid_blocks = [
            block
            for block in scored_blocks
            if str(block.get("template_grid_id", "default")) == template_grid_id
            and block.get("scored_candidates")
        ]
        for threshold in thresholds_uah:
            threshold_rows.append(
                _threshold_summary_row(
                    grid_blocks,
                    template_grid_id=template_grid_id,
                    threshold_uah=float(threshold),
                    max_predicted_tail_risk_probability=(
                        max_predicted_tail_risk_probability
                    ),
                    max_family_tail_risk_probability=max_family_tail_risk_probability,
                )
            )
    template_rows = _template_summary_rows(
        candidate_rows,
        selected_by_default=selected_by_default,
    )
    summary = _summary_payload(
        candidate_rows=candidate_rows,
        threshold_rows=threshold_rows,
        template_rows=template_rows,
    )
    return {
        "candidate_scores": candidate_rows,
        "threshold_summary": threshold_rows,
        "template_summary": template_rows,
        "summary": summary,
    }


def write_selection_audit_outputs(
    *,
    output_dir: Path,
    run_slug: str,
    tenant_id: str,
    market_venue: str,
    thresholds_uah: Sequence[float],
    tables: Mapping[str, Any],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "candidate_scores_csv": output_dir / "candidate_scores.csv",
        "threshold_summary_csv": output_dir / "threshold_summary.csv",
        "template_summary_csv": output_dir / "template_summary.csv",
        "summary_json": output_dir / "summary.json",
        "summary_md": output_dir / "summary.md",
    }
    _write_csv(paths["candidate_scores_csv"], tables["candidate_scores"])
    _write_csv(paths["threshold_summary_csv"], tables["threshold_summary"])
    _write_csv(paths["template_summary_csv"], tables["template_summary"])
    summary = {
        **dict(tables["summary"]),
        "run_slug": run_slug,
        "tenant_id": tenant_id,
        "market_venue": market_venue.upper(),
        "thresholds_uah": [float(value) for value in thresholds_uah],
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


def _candidate_score_row(
    candidate: Mapping[str, Any],
    *,
    target_delivery_date: str,
    template_grid_id: str,
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
        "template_grid_id": template_grid_id,
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


def _threshold_summary_row(
    blocks: Sequence[Mapping[str, Any]],
    *,
    template_grid_id: str,
    threshold_uah: float,
    max_predicted_tail_risk_probability: float,
    max_family_tail_risk_probability: float,
) -> dict[str, Any]:
    switch_count = 0
    selected_values: list[float] = []
    best_values: list[float] = []
    value_gaps: list[float] = []
    diagnostics_totals: defaultdict[str, float] = defaultdict(float)
    for block in blocks:
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
        switch_count += int(not bool(selection["abstained_to_v2_plus"]))
        selected_values.append(_candidate_value(selected))
        best_values.append(float(diagnostics["best_template_schedule_value_uah"]))
        value_gaps.append(float(diagnostics["selected_vs_best_template_value_gap_uah"]))
        for key in (
            "threshold_guard_failed_count",
            "predicted_tail_guard_failed_count",
            "family_tail_guard_failed_count",
            "safety_guard_failed_count",
            "eligible_nonfallback_candidate_count",
        ):
            diagnostics_totals[key] += float(diagnostics[key])
    day_count = len(blocks)
    return {
        "template_grid_id": template_grid_id,
        "threshold_uah": float(threshold_uah),
        "source_backed_day_count": float(day_count),
        "switch_count": float(switch_count),
        "abstention_count": float(day_count - switch_count),
        "switch_rate": _ratio(switch_count, day_count),
        "mean_selected_schedule_value_uah": _mean(selected_values),
        "mean_best_template_schedule_value_uah": _mean(best_values),
        "mean_selected_vs_best_template_value_gap_uah": _mean(value_gaps),
        "threshold_guard_failed_count": diagnostics_totals[
            "threshold_guard_failed_count"
        ],
        "predicted_tail_guard_failed_count": diagnostics_totals[
            "predicted_tail_guard_failed_count"
        ],
        "family_tail_guard_failed_count": diagnostics_totals[
            "family_tail_guard_failed_count"
        ],
        "safety_guard_failed_count": diagnostics_totals["safety_guard_failed_count"],
        "eligible_nonfallback_candidate_count": diagnostics_totals[
            "eligible_nonfallback_candidate_count"
        ],
        "market_execution_enabled": False,
        "promotion_gate_passed": False,
        "dt_lava_ready": False,
    }


def _template_summary_rows(
    candidate_rows: Sequence[Mapping[str, Any]],
    *,
    selected_by_default: Mapping[tuple[str, str], str],
) -> list[dict[str, Any]]:
    grouped: defaultdict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in candidate_rows:
        grouped[(str(row["template_grid_id"]), str(row["candidate_family"]))].append(row)
    summary_rows: list[dict[str, Any]] = []
    for (template_grid_id, family), rows in sorted(grouped.items()):
        selected_count = sum(
            1
            for row in rows
            if selected_by_default.get(
                (str(row["target_delivery_date"]), template_grid_id)
            )
            == family
        )
        summary_rows.append(
            {
                "template_grid_id": template_grid_id,
                "candidate_family": family,
                "row_count": float(len(rows)),
                "selected_count_at_default_threshold": float(selected_count),
                "mean_schedule_value_uah": _mean(
                    [float(row["schedule_value_uah"]) for row in rows]
                ),
                "mean_predicted_regret_delta_vs_v2_plus_uah": _mean(
                    [
                        float(row["predicted_regret_delta_vs_v2_plus_uah"])
                        for row in rows
                    ]
                ),
                "mean_predicted_tail_risk_probability": _mean(
                    [float(row["predicted_tail_risk_probability"]) for row in rows]
                ),
                "safety_violation_count": float(
                    sum(float(row["safety_violation_count"]) for row in rows)
                ),
                "market_execution_enabled": False,
                "promotion_gate_passed": False,
                "dt_lava_ready": False,
            }
        )
    return summary_rows


def _summary_payload(
    *,
    candidate_rows: Sequence[Mapping[str, Any]],
    threshold_rows: Sequence[Mapping[str, Any]],
    template_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    source_day_count = len(
        {
            (str(row["target_delivery_date"]), str(row["template_grid_id"]))
            for row in candidate_rows
        }
    )
    update_gate = _candidate_library_update_gate(threshold_rows)
    audit_passed = bool(update_gate["candidate_library_update_gate_passed"])
    candidate_grid_id = str(update_gate["update_gate_candidate_template_grid_id"])
    return {
        "claim_scope": CLAIM_SCOPE,
        "generated_at_utc": datetime.now(tz=UTC).isoformat(),
        "source_price_scope": "official_oree_observed_rows_only",
        "source_backed_block_count": float(source_day_count),
        "candidate_score_row_count": float(len(candidate_rows)),
        "threshold_summary_row_count": float(len(threshold_rows)),
        "template_summary_row_count": float(len(template_rows)),
        "audit_passed_for_bundle_update": audit_passed,
        "recommended_action": (
            "manual_review_required_before_bundle_update"
            if audit_passed
            else "keep_current_bundle_candidate_library_needs_redesign"
        ),
        "summary_message": (
            f"{candidate_grid_id} passes the read-only audit gates; manual review is still required before any bundle update."
            if audit_passed
            else "Candidate library remains blocked; keep current bundle and redesign templates before retuning."
        ),
        **update_gate,
    }


def _candidate_library_update_gate(
    threshold_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    indexed = {
        (str(row["template_grid_id"]), float(row["threshold_uah"])): row
        for row in threshold_rows
    }
    baseline = indexed.get(("default", UPDATE_GATE_THRESHOLD_UAH))
    candidate_grid_id = next(
        (
            grid_id
            for grid_id in UPDATE_GATE_CANDIDATE_TEMPLATE_GRIDS
            if (grid_id, UPDATE_GATE_THRESHOLD_UAH) in indexed
        ),
        "",
    )
    candidate = indexed.get((candidate_grid_id, UPDATE_GATE_THRESHOLD_UAH))
    if baseline is None or candidate is None:
        return _failed_update_gate(
            "missing_default_or_candidate_library_100uah_summary",
            candidate_grid_id=candidate_grid_id or "candidate_library_value_aligned",
        )
    baseline_gap = float(baseline["mean_selected_vs_best_template_value_gap_uah"])
    candidate_gap = float(candidate["mean_selected_vs_best_template_value_gap_uah"])
    gap_ratio = candidate_gap / baseline_gap if baseline_gap > 0.0 else 1.0
    selected_value_improvement = float(
        candidate["mean_selected_schedule_value_uah"]
    ) - float(baseline["mean_selected_schedule_value_uah"])
    tail_failure_delta = float(candidate["predicted_tail_guard_failed_count"]) - float(
        baseline["predicted_tail_guard_failed_count"]
    )
    safety_failures = float(candidate["safety_guard_failed_count"])
    reason = ""
    if safety_failures > 0.0:
        reason = "candidate_library_v2_safety_failures"
    elif tail_failure_delta > 0.0:
        reason = "candidate_library_v2_tail_risk_failures_increased"
    elif selected_value_improvement <= 0.0:
        reason = "selected_value_not_improved"
    elif gap_ratio > MAX_VALUE_GAP_RATIO_FOR_BUNDLE_UPDATE:
        reason = "value_gap_not_substantially_reduced"
    passed = reason == ""
    return {
        "candidate_library_update_gate_passed": passed,
        "update_gate_threshold_uah": UPDATE_GATE_THRESHOLD_UAH,
        "update_gate_baseline_template_grid_id": "default",
        "update_gate_candidate_template_grid_id": candidate_grid_id,
        f"{candidate_grid_id}_selected_value_improvement_uah": selected_value_improvement,
        f"{candidate_grid_id}_value_gap_improvement_uah": baseline_gap - candidate_gap,
        f"{candidate_grid_id}_value_gap_ratio_vs_default": gap_ratio,
        f"{candidate_grid_id}_tail_failure_delta_count": tail_failure_delta,
        f"{candidate_grid_id}_safety_failure_count": safety_failures,
        "update_gate_failed_reason": reason,
    }


def _failed_update_gate(reason: str, *, candidate_grid_id: str) -> dict[str, Any]:
    return {
        "candidate_library_update_gate_passed": False,
        "update_gate_threshold_uah": UPDATE_GATE_THRESHOLD_UAH,
        "update_gate_baseline_template_grid_id": "default",
        "update_gate_candidate_template_grid_id": candidate_grid_id,
        f"{candidate_grid_id}_selected_value_improvement_uah": 0.0,
        f"{candidate_grid_id}_value_gap_improvement_uah": 0.0,
        f"{candidate_grid_id}_value_gap_ratio_vs_default": 1.0,
        f"{candidate_grid_id}_tail_failure_delta_count": 0.0,
        f"{candidate_grid_id}_safety_failure_count": 0.0,
        "update_gate_failed_reason": reason,
    }


def _summary_markdown(summary: Mapping[str, Any]) -> str:
    return "\n".join(
        [
            "# HF Live Safe-Switch Selection Audit",
            "",
            f"- Claim scope: `{summary['claim_scope']}`",
            f"- Tenant: `{summary['tenant_id']}`",
            f"- Market venue: `{summary['market_venue']}`",
            f"- Source-backed scored blocks: {summary['source_backed_block_count']}",
            f"- Candidate rows: {summary['candidate_score_row_count']}",
            f"- Audit passed for bundle update: {summary['audit_passed_for_bundle_update']}",
            f"- Recommended action: `{summary['recommended_action']}`",
            f"- Summary: {summary['summary_message']}",
            f"- Candidate-library update gate: `{summary['candidate_library_update_gate_passed']}`",
            f"- Update gate failed reason: `{summary['update_gate_failed_reason']}`",
            "",
            "Non-execution flags remain false for promotion and market execution.",
        ]
    )


def _write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: _csv_value(row.get(key)) for key in fieldnames})


def _parse_thresholds(value: str) -> tuple[float, ...]:
    thresholds = tuple(float(item.strip()) for item in value.split(",") if item.strip())
    if not thresholds:
        raise ValueError("thresholds-uah must contain at least one numeric value.")
    return thresholds


def _parse_date(value: str) -> date:
    return date.fromisoformat(value)


def _date_range(start_date: date, end_date: date) -> list[date]:
    return [
        start_date + timedelta(days=offset)
        for offset in range((end_date - start_date).days + 1)
    ]


def _candidate_value(candidate: Mapping[str, Any]) -> float:
    value = candidate.get("schedule_value_uah")
    if value is None:
        value = candidate.get("decision_value_uah", 0.0)
    return float(value)


def _mean(values: Sequence[float]) -> float:
    return float(sum(values) / len(values)) if values else 0.0


def _ratio(numerator: int, denominator: int) -> float:
    return float(numerator / denominator) if denominator else 0.0


def _csv_value(value: Any) -> Any:
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


if __name__ == "__main__":
    raise SystemExit(main())
