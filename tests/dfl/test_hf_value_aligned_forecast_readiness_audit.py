from __future__ import annotations

from datetime import date

from scripts.audit_hf_value_aligned_forecast_readiness import (
	ReadinessCase,
	build_readiness_cases,
	build_readiness_summary,
	readiness_row_from_preview,
	source_mode_from_metrics,
)


def test_readiness_cases_cover_dam_idm_latest_today_tomorrow_day_plus_two() -> None:
	cases = build_readiness_cases(today=date(2026, 6, 2), market_venues=("DAM", "IDM"))

	assert [f"{case.market_venue.lower()}_{case.target_selection}" for case in cases] == [
		"dam_latest_official",
		"dam_today",
		"dam_tomorrow",
		"dam_day_plus_2",
		"idm_latest_official",
		"idm_today",
		"idm_tomorrow",
		"idm_day_plus_2",
	]
	assert cases[0].target_delivery_date is None
	assert cases[1].target_delivery_date == date(2026, 6, 2)
	assert cases[2].target_delivery_date == date(2026, 6, 3)
	assert cases[3].target_delivery_date == date(2026, 6, 4)


def test_source_mode_mapping_uses_public_readiness_metrics() -> None:
	assert (
		source_mode_from_metrics(
			metrics={"source_backed_price_context_available": 1.0, "official_context_published": 1.0}
		)
		== "official_published"
	)
	assert (
		source_mode_from_metrics(
			metrics={
				"source_backed_price_context_available": 1.0,
				"request_fallback_materialized": 1.0,
			}
		)
		== "request_fallback_materialized"
	)
	assert (
		source_mode_from_metrics(
			metrics={"source_backed_price_context_available": 1.0, "same_day_forecast_refresh": 1.0}
		)
		== "same_day_forecast_refresh"
	)
	assert (
		source_mode_from_metrics(
			metrics={
				"source_backed_price_context_available": 1.0,
				"forecast_context_pre_publication": 1.0,
			}
		)
		== "pre_publication_forecast"
	)
	assert (
		source_mode_from_metrics(metrics={"source_backed_price_context_available": 0.0})
		== "blocked_missing_source_backed_price_context"
	)


def test_readiness_summary_preserves_non_execution_contract() -> None:
	rows = [
		readiness_row_from_preview(
			case=ReadinessCase("DAM", "today", date(2026, 6, 2)),
			url="http://127.0.0.1:8000/dashboard/shadow-recommendation-preview",
			payload=_preview_payload(
				status="ready",
				metrics={
					"source_backed_price_context_available": 1.0,
					"same_day_forecast_refresh": 1.0,
					"forecast_rows_loaded": 24.0,
				},
				actions=["hold"] * 24,
			),
		),
		readiness_row_from_preview(
			case=ReadinessCase("IDM", "day_plus_2", date(2026, 6, 4)),
			url="http://127.0.0.1:8000/dashboard/shadow-recommendation-preview",
			payload=_preview_payload(
				status="ready",
				metrics={
					"source_backed_price_context_available": 1.0,
					"forecast_context_pre_publication": 1.0,
					"forecast_rows_loaded": 24.0,
				},
				actions=["hold"] * 20 + ["charge", "discharge", "hold", "hold"],
			),
		),
	]

	summary = build_readiness_summary(
		rows,
		tenant_id="client_003_dnipro_factory",
		today=date(2026, 6, 2),
	)

	assert summary["case_count"] == 2.0
	assert summary["ready_24_row_case_count"] == 2.0
	assert summary["blocked_case_count"] == 0.0
	assert summary["non_hold_case_count"] == 1.0
	assert summary["same_day_forecast_refresh_case_count"] == 1.0
	assert summary["all_execution_flags_false"] is True
	assert summary["market_execution_enabled"] is False
	assert summary["market_order_payload_emitted"] is False
	assert summary["proposed_bid_emitted"] is False


def _preview_payload(
	*,
	status: str,
	metrics: dict[str, float],
	actions: list[str],
) -> dict[str, object]:
	return {
		"preview_status": status,
		"target_delivery_window_start": "2026-06-02T00:00:00",
		"comparison_metrics": metrics,
		"recommendation_schedule": [{"action": action} for action in actions],
		"market_execution_enabled": False,
		"market_order_payload_emitted": False,
		"promotion_gate_passed": False,
		"dt_lava_ready": False,
		"proposed_bid_status": "not_emitted",
		"readiness_warnings": [],
	}
