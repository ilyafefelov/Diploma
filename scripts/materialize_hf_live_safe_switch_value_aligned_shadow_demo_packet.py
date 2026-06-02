"""Materialize a two-case value-aligned HF live shadow demo packet."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
import csv
from datetime import UTC, datetime
import json
from pathlib import Path
import sys
from typing import Any
from urllib.parse import urlencode
from urllib.request import urlopen

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_PROMOTION_GATE_JSON = (
	PROJECT_ROOT
	/ "data"
	/ "research_runs"
	/ "hf_live_safe_switch_value_aligned_shadow_promotion_proof_2026_05_01_2026_06_01"
	/ "promotion_gate.json"
)
DEFAULT_OUTPUT_DIR = (
	PROJECT_ROOT
	/ "data"
	/ "research_runs"
	/ "hf_live_safe_switch_value_aligned_shadow_demo_packet_2026_06_01"
)
DEFAULT_RUN_SLUG = "hf_live_safe_switch_value_aligned_shadow_demo_packet_2026_06_01"


def main(argv: Sequence[str] | None = None) -> int:
	parser = argparse.ArgumentParser(
		description=(
			"Capture a durable two-case demo packet for the value-aligned HF live "
			"safe-switch shadow endpoint. This remains preview-only evidence."
		)
	)
	parser.add_argument("--api-base-url", default="http://127.0.0.1:8000")
	parser.add_argument("--tenant-id", default="client_003_dnipro_factory")
	parser.add_argument("--market-venue", default="DAM")
	parser.add_argument("--switch-day-date", default="2026-05-02")
	parser.add_argument("--promotion-gate-json", type=Path, default=DEFAULT_PROMOTION_GATE_JSON)
	parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
	parser.add_argument("--run-slug", default=DEFAULT_RUN_SLUG)
	args = parser.parse_args(argv)

	latest_response = _fetch_shadow_preview(
		api_base_url=args.api_base_url,
		tenant_id=args.tenant_id,
		market_venue=args.market_venue,
		target_delivery_date=None,
	)
	switch_day_response = _fetch_shadow_preview(
		api_base_url=args.api_base_url,
		tenant_id=args.tenant_id,
		market_venue=args.market_venue,
		target_delivery_date=args.switch_day_date,
	)
	promotion_gate = _load_json(args.promotion_gate_json)
	packet = build_value_aligned_shadow_demo_packet(
		run_slug=args.run_slug,
		latest_response=latest_response,
		switch_day_response=switch_day_response,
		promotion_gate=promotion_gate,
	)
	paths = write_value_aligned_shadow_demo_packet(
		output_dir=args.output_dir,
		packet=packet,
	)
	result = {
		"demo_packet_json": str(paths["demo_packet_json"]),
		"demo_packet_md": str(paths["demo_packet_md"]),
		"demo_cases_csv": str(paths["demo_cases_csv"]),
		"demo_packet_passed": packet["demo_packet_passed"],
		"market_execution_enabled": False,
		"production_market_promotion_gate_passed": False,
	}
	json.dump(result, sys.stdout, indent=2, sort_keys=True)
	sys.stdout.write("\n")
	return 0 if packet["demo_packet_passed"] else 2


def build_value_aligned_shadow_demo_packet(
	*,
	run_slug: str,
	latest_response: Mapping[str, Any],
	switch_day_response: Mapping[str, Any],
	promotion_gate: Mapping[str, Any],
) -> dict[str, Any]:
	latest_case = _demo_case("latest_official_dam_day", latest_response)
	switch_day_case = _demo_case("source_backed_switch_day_2026_05_02", switch_day_response)
	gate_passed = _truthy(promotion_gate.get("shadow_promotion_gate_passed"))
	flags_safe = all(
		not _truthy(payload.get(flag))
		for payload in (latest_response, switch_day_response, promotion_gate)
		for flag in (
			"market_execution_enabled",
			"market_order_payload_emitted",
			"promotion_gate_passed",
			"production_market_promotion_gate_passed",
			"dt_lava_ready",
		)
	)
	demo_passed = (
		gate_passed
		and flags_safe
		and latest_case["selected_schedule_family"] == "schedule_value_learner_v2_plus"
		and switch_day_case["selected_schedule_family"] != "schedule_value_learner_v2_plus"
		and float(switch_day_case["selected_candidate_estimated_value_uah"]) > 0.0
	)
	return {
		"run_slug": run_slug,
		"claim_scope": "hf_live_safe_switch_value_aligned_shadow_demo_packet",
		"generated_at_utc": datetime.now(tz=UTC).isoformat(),
		"demo_packet_passed": demo_passed,
		"market_execution_enabled": False,
		"production_market_promotion_gate_passed": False,
		"proposed_bid_emitted": False,
		"market_order_payload_emitted": False,
		"shadow_promotion_gate_passed": gate_passed,
		"latest_case": latest_case,
		"switch_day_case": switch_day_case,
		"cases": [latest_case, switch_day_case],
	}


def write_value_aligned_shadow_demo_packet(
	*,
	output_dir: Path,
	packet: Mapping[str, Any],
) -> dict[str, Path]:
	output_dir.mkdir(parents=True, exist_ok=True)
	paths = {
		"demo_packet_json": output_dir / "demo_packet.json",
		"demo_packet_md": output_dir / "demo_packet.md",
		"demo_cases_csv": output_dir / "demo_cases.csv",
	}
	paths["demo_packet_json"].write_text(
		json.dumps(dict(packet), indent=2, ensure_ascii=False),
		encoding="utf-8",
	)
	paths["demo_packet_md"].write_text(_demo_markdown(packet), encoding="utf-8")
	cases = packet.get("cases", [])
	if not isinstance(cases, Sequence):
		raise ValueError("packet cases must be a sequence.")
	_write_csv(paths["demo_cases_csv"], [dict(case) for case in cases if isinstance(case, Mapping)])
	return paths


def _fetch_shadow_preview(
	*,
	api_base_url: str,
	tenant_id: str,
	market_venue: str,
	target_delivery_date: str | None,
) -> dict[str, Any]:
	query = {
		"tenant_id": tenant_id,
		"preview_source": "hf_live_safe_switch_value_aligned_shadow",
		"market_venue": market_venue,
	}
	if target_delivery_date is not None:
		query["target_delivery_date"] = target_delivery_date
	url = f"{api_base_url.rstrip('/')}/dashboard/shadow-recommendation-preview?{urlencode(query)}"
	with urlopen(url, timeout=60) as response:  # noqa: S310 - local operator demo endpoint.
		payload = json.loads(response.read().decode("utf-8"))
	if not isinstance(payload, dict):
		raise ValueError("shadow preview response must be a JSON object.")
	return payload


def _demo_case(case_id: str, response: Mapping[str, Any]) -> dict[str, Any]:
	metrics = response.get("comparison_metrics", {})
	if not isinstance(metrics, Mapping):
		metrics = {}
	return {
		"case_id": case_id,
		"preview_source_id": str(response.get("preview_source_id", "")),
		"preview_status": str(response.get("preview_status", "")),
		"selected_schedule_family": str(response.get("selected_schedule_family", "")),
		"target_delivery_window_start": str(response.get("target_delivery_window_start", "")),
		"schedule_row_count": float(len(response.get("recommendation_schedule", []) or [])),
		"selected_candidate_estimated_value_uah": _number(
			metrics.get("selected_candidate_estimated_value_uah")
		),
		"predicted_regret_delta_vs_v2_plus_uah": _number(
			metrics.get("predicted_regret_delta_vs_v2_plus_uah")
		),
		"market_execution_enabled": _truthy(response.get("market_execution_enabled")),
		"market_order_payload_emitted": _truthy(response.get("market_order_payload_emitted")),
		"promotion_gate_passed": _truthy(response.get("promotion_gate_passed")),
		"dt_lava_ready": _truthy(response.get("dt_lava_ready")),
	}


def _demo_markdown(packet: Mapping[str, Any]) -> str:
	latest = packet["latest_case"]
	switch_day = packet["switch_day_case"]
	return "\n".join(
		[
			"# HF Live Safe-Switch Value-Aligned Shadow Demo Packet",
			"",
			f"- Run slug: `{packet['run_slug']}`",
			f"- Demo packet passed: `{packet['demo_packet_passed']}`",
			f"- Shadow promotion gate passed: `{packet['shadow_promotion_gate_passed']}`",
			f"- Latest case family: `{latest['selected_schedule_family']}`",
			f"- Switch-day family: `{switch_day['selected_schedule_family']}`",
			f"- Switch-day selected value: {switch_day['selected_candidate_estimated_value_uah']} UAH",
			f"- Switch-day predicted delta vs V2+: {switch_day['predicted_regret_delta_vs_v2_plus_uah']} UAH",
			"",
			"Market execution remains disabled: no ProposedBid and no market order payload.",
		]
	)


def _load_json(path: Path) -> dict[str, Any]:
	value = json.loads(path.read_text(encoding="utf-8-sig"))
	if not isinstance(value, dict):
		raise ValueError(f"{path} must contain a JSON object.")
	return value


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
