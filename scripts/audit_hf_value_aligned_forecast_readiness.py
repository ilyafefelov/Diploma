"""Audit HF value-aligned shadow preview readiness through the local API.

This is a read-only demo helper. It verifies that the operator-facing
``hf_live_safe_switch_value_aligned_shadow`` path returns either 24 source-backed
rows or an explicit blocked reason for DAM/IDM latest, today, tomorrow, and day+2.
It never emits ProposedBid, market order payloads, or execution flags.
"""

from __future__ import annotations

import argparse
from collections import Counter
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
import json
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CLAIM_SCOPE = "hf_value_aligned_forecast_readiness_shadow_demo_only"
PREVIEW_SOURCE_ID = "hf_live_safe_switch_value_aligned_shadow"
DEFAULT_TENANT_ID = "client_003_dnipro_factory"
DEFAULT_BASE_URL = "http://127.0.0.1:8000"
DEFAULT_MARKET_VENUES = ("DAM", "IDM")
DEFAULT_TIMEZONE = "Europe/Kyiv"
CASE_LABELS = ("latest_official", "today", "tomorrow", "day_plus_2")


@dataclass(frozen=True, slots=True)
class ReadinessCase:
	market_venue: str
	target_selection: str
	target_delivery_date: date | None


def main(argv: Sequence[str] | None = None) -> int:
	parser = argparse.ArgumentParser(
		description=(
			"Write an 8-case HF value-aligned shadow readiness matrix from the running API. "
			"No LP solve, no synthetic prices, no market payloads."
		)
	)
	parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
	parser.add_argument("--tenant-id", default=DEFAULT_TENANT_ID)
	parser.add_argument("--market-venues", default=",".join(DEFAULT_MARKET_VENUES))
	parser.add_argument("--today", type=date.fromisoformat)
	parser.add_argument("--timezone", default=DEFAULT_TIMEZONE)
	parser.add_argument("--output-dir", type=Path)
	parser.add_argument("--timeout-seconds", type=float, default=60.0)
	args = parser.parse_args(argv)

	resolved_today = args.today or datetime.now(tz=ZoneInfo(args.timezone)).date()
	market_venues = tuple(
		item.strip().upper() for item in args.market_venues.split(",") if item.strip()
	)
	cases = build_readiness_cases(today=resolved_today, market_venues=market_venues)
	rows = collect_readiness_rows(
		base_url=args.base_url,
		tenant_id=args.tenant_id,
		cases=cases,
		timeout_seconds=args.timeout_seconds,
	)
	summary = build_readiness_summary(
		rows,
		tenant_id=args.tenant_id,
		today=resolved_today,
	)
	output_dir = args.output_dir or (
		PROJECT_ROOT
		/ "data"
		/ "research_runs"
		/ f"hf_value_aligned_forecast_readiness_{resolved_today.isoformat()}"
	)
	write_readiness_summary(output_dir=output_dir, summary=summary)
	return 0 if bool(summary["all_execution_flags_false"]) else 2


def build_readiness_cases(
	*,
	today: date,
	market_venues: Sequence[str] = DEFAULT_MARKET_VENUES,
) -> list[ReadinessCase]:
	cases: list[ReadinessCase] = []
	for market_venue in market_venues:
		resolved_market = market_venue.strip().upper()
		if not resolved_market:
			continue
		cases.extend(
			[
				ReadinessCase(resolved_market, "latest_official", None),
				ReadinessCase(resolved_market, "today", today),
				ReadinessCase(resolved_market, "tomorrow", today + timedelta(days=1)),
				ReadinessCase(resolved_market, "day_plus_2", today + timedelta(days=2)),
			]
		)
	return cases


def collect_readiness_rows(
	*,
	base_url: str,
	tenant_id: str,
	cases: Sequence[ReadinessCase],
	timeout_seconds: float = 60.0,
) -> list[dict[str, Any]]:
	rows: list[dict[str, Any]] = []
	for case in cases:
		url = shadow_preview_url(base_url=base_url, tenant_id=tenant_id, case=case)
		try:
			payload = fetch_json(url, timeout_seconds=timeout_seconds)
		except (HTTPError, URLError, TimeoutError) as error:
			rows.append(readiness_row_from_error(case=case, url=url, error=error))
			continue
		rows.append(readiness_row_from_preview(case=case, url=url, payload=payload))
	return rows


def shadow_preview_url(*, base_url: str, tenant_id: str, case: ReadinessCase) -> str:
	params = {
		"tenant_id": tenant_id,
		"preview_source": PREVIEW_SOURCE_ID,
		"market_venue": case.market_venue,
	}
	if case.target_delivery_date is not None:
		params["target_delivery_date"] = case.target_delivery_date.isoformat()
	return f"{base_url.rstrip('/')}/dashboard/shadow-recommendation-preview?{urlencode(params)}"


def fetch_json(url: str, *, timeout_seconds: float) -> dict[str, Any]:
	request = Request(url, headers={"Accept": "application/json"})
	with urlopen(request, timeout=timeout_seconds) as response:  # noqa: S310 - local audit URL
		return json.loads(response.read().decode("utf-8"))


def readiness_row_from_preview(
	*,
	case: ReadinessCase,
	url: str,
	payload: Mapping[str, Any],
) -> dict[str, Any]:
	metrics = _numeric_metrics(payload.get("comparison_metrics", {}))
	schedule = list(payload.get("recommendation_schedule", []) or [])
	status = str(payload.get("preview_status", "unknown"))
	source_mode = source_mode_from_metrics(metrics=metrics, preview_status=status)
	warnings = [str(item) for item in payload.get("readiness_warnings", []) or []]
	target_start = payload.get("target_delivery_window_start")
	return {
		"case_id": f"{case.market_venue.lower()}_{case.target_selection}",
		"request_url": url,
		"market_venue": case.market_venue,
		"target_selection": case.target_selection,
		"requested_target_delivery_date": (
			case.target_delivery_date.isoformat() if case.target_delivery_date is not None else None
		),
		"resolved_target_delivery_date": _date_prefix(target_start),
		"preview_status": status,
		"source_mode": source_mode,
		"row_count": float(len(schedule)),
		"forecast_rows_loaded": float(metrics.get("forecast_rows_loaded", 0.0)),
		"source_backed_price_context_available": float(
			metrics.get("source_backed_price_context_available", 0.0)
		),
		"request_fallback_materialized": float(metrics.get("request_fallback_materialized", 0.0)),
		"same_day_forecast_refresh": float(metrics.get("same_day_forecast_refresh", 0.0)),
		"forecast_generated_at": None,
		"training_cutoff": None,
		"block_reason": warnings[0] if status.startswith("blocked") and warnings else "",
		"non_hold_row_count": float(
			sum(1 for row in schedule if str(row.get("action", "")).lower() != "hold")
		),
		"guard_abstained_to_safe_fallback": float(
			metrics.get("guard_abstained_to_safe_fallback", 0.0)
		),
		"threshold_guard_failed_count": float(metrics.get("threshold_guard_failed_count", 0.0)),
		"predicted_tail_guard_failed_count": float(
			metrics.get("predicted_tail_guard_failed_count", 0.0)
		),
		"safety_guard_failed_count": float(metrics.get("safety_guard_failed_count", 0.0)),
		"market_execution_enabled": bool(payload.get("market_execution_enabled", False)),
		"market_order_payload_emitted": bool(payload.get("market_order_payload_emitted", False)),
		"promotion_gate_passed": bool(payload.get("promotion_gate_passed", False)),
		"dt_lava_ready": bool(payload.get("dt_lava_ready", False)),
		"proposed_bid_status": str(payload.get("proposed_bid_status", "")),
	}


def readiness_row_from_error(
	*,
	case: ReadinessCase,
	url: str,
	error: BaseException,
) -> dict[str, Any]:
	detail = str(error)
	if isinstance(error, HTTPError):
		try:
			body = error.read().decode("utf-8")
			detail = body or detail
		except Exception:  # noqa: BLE001
			pass
	return {
		"case_id": f"{case.market_venue.lower()}_{case.target_selection}",
		"request_url": url,
		"market_venue": case.market_venue,
		"target_selection": case.target_selection,
		"requested_target_delivery_date": (
			case.target_delivery_date.isoformat() if case.target_delivery_date is not None else None
		),
		"resolved_target_delivery_date": None,
		"preview_status": "blocked_http_error",
		"source_mode": "blocked_missing_source_backed_price_context",
		"row_count": 0.0,
		"forecast_rows_loaded": 0.0,
		"source_backed_price_context_available": 0.0,
		"request_fallback_materialized": 0.0,
		"same_day_forecast_refresh": 0.0,
		"forecast_generated_at": None,
		"training_cutoff": None,
		"block_reason": detail,
		"non_hold_row_count": 0.0,
		"guard_abstained_to_safe_fallback": 1.0,
		"threshold_guard_failed_count": 0.0,
		"predicted_tail_guard_failed_count": 0.0,
		"safety_guard_failed_count": 0.0,
		"market_execution_enabled": False,
		"market_order_payload_emitted": False,
		"promotion_gate_passed": False,
		"dt_lava_ready": False,
		"proposed_bid_status": "",
	}


def source_mode_from_metrics(
	*,
	metrics: Mapping[str, float],
	preview_status: str = "",
) -> str:
	if preview_status.startswith("blocked"):
		return "blocked_missing_source_backed_price_context"
	if float(metrics.get("source_backed_price_context_available", 0.0)) < 1.0:
		return "blocked_missing_source_backed_price_context"
	if float(metrics.get("official_context_published", 0.0)) >= 1.0:
		return "official_published"
	if float(metrics.get("request_fallback_materialized", 0.0)) >= 1.0:
		return "request_fallback_materialized"
	if float(metrics.get("same_day_forecast_refresh", 0.0)) >= 1.0 or float(
		metrics.get("forecast_context_same_day_refresh", 0.0)
	) >= 1.0:
		return "same_day_forecast_refresh"
	if float(metrics.get("forecast_context_pre_publication", 0.0)) >= 1.0:
		return "pre_publication_forecast"
	return "source_backed_price_context_unknown"


def build_readiness_summary(
	rows: Sequence[Mapping[str, Any]],
	*,
	tenant_id: str,
	today: date,
) -> dict[str, Any]:
	source_mode_counts = Counter(str(row["source_mode"]) for row in rows)
	execution_violations = [
		row
		for row in rows
		if bool(row["market_execution_enabled"])
		or bool(row["market_order_payload_emitted"])
		or bool(row["promotion_gate_passed"])
		or bool(row["dt_lava_ready"])
	]
	return {
		"claim_scope": CLAIM_SCOPE,
		"generated_at_utc": datetime.now(tz=UTC).isoformat(timespec="seconds"),
		"tenant_id": tenant_id,
		"today": today.isoformat(),
		"preview_source_id": PREVIEW_SOURCE_ID,
		"case_count": float(len(rows)),
		"ready_24_row_case_count": float(
			sum(
				1
				for row in rows
				if float(row["row_count"]) == 24.0
				and float(row["source_backed_price_context_available"]) == 1.0
			)
		),
		"blocked_case_count": float(
			sum(1 for row in rows if str(row["source_mode"]).startswith("blocked"))
		),
		"request_fallback_materialized_case_count": float(
			sum(1 for row in rows if float(row["request_fallback_materialized"]) >= 1.0)
		),
		"same_day_forecast_refresh_case_count": float(
			sum(1 for row in rows if float(row["same_day_forecast_refresh"]) >= 1.0)
		),
		"non_hold_case_count": float(sum(1 for row in rows if float(row["non_hold_row_count"]) > 0.0)),
		"source_mode_counts": dict(source_mode_counts),
		"all_execution_flags_false": len(execution_violations) == 0,
		"execution_flag_violation_count": float(len(execution_violations)),
		"market_execution_enabled": False,
		"promotion_gate_passed": False,
		"dt_lava_ready": False,
		"proposed_bid_emitted": False,
		"market_order_payload_emitted": False,
		"readiness_matrix": [dict(row) for row in rows],
	}


def write_readiness_summary(*, output_dir: Path, summary: Mapping[str, Any]) -> Path:
	output_dir.mkdir(parents=True, exist_ok=True)
	path = output_dir / "summary.json"
	path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
	return path


def _numeric_metrics(raw_metrics: Any) -> dict[str, float]:
	if not isinstance(raw_metrics, Mapping):
		return {}
	metrics: dict[str, float] = {}
	for key, value in raw_metrics.items():
		try:
			metrics[str(key)] = float(value)
		except (TypeError, ValueError):
			continue
	return metrics


def _date_prefix(value: Any) -> str | None:
	if value is None:
		return None
	text = str(value)
	return text[:10] if len(text) >= 10 else text


if __name__ == "__main__":
	raise SystemExit(main())
