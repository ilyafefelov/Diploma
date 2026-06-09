from datetime import UTC, datetime, timedelta

import polars as pl

from smart_arbitrage.resources.dfl_training_store import InMemoryDflTrainingStore


def test_in_memory_store_returns_latest_schedule_value_production_gate_rows() -> None:
	store = InMemoryDflTrainingStore()
	older_generated_at = datetime(2026, 5, 10, 12, tzinfo=UTC)
	latest_generated_at = older_generated_at + timedelta(hours=1)

	store.upsert_schedule_value_production_gate_frame(
		_schedule_value_gate_frame(
			generated_at=older_generated_at,
			source_model_names=["tft_silver_v0"],
		)
	)
	store.upsert_schedule_value_production_gate_frame(
		_schedule_value_gate_frame(
			generated_at=latest_generated_at,
			source_model_names=["nbeatsx_silver_v0", "tft_silver_v0"],
		)
	)

	latest_frame = store.latest_schedule_value_production_gate_frame()

	assert latest_frame.select("generated_at").unique().item() == latest_generated_at
	assert latest_frame["source_model_name"].to_list() == [
		"nbeatsx_silver_v0",
		"tft_silver_v0",
	]


def _schedule_value_gate_frame(
	*,
	generated_at: datetime,
	source_model_names: list[str],
) -> pl.DataFrame:
	return pl.DataFrame(
		{
			"source_model_name": source_model_names,
			"tenant_count": [5 for _ in source_model_names],
			"latest_validation_tenant_anchor_count": [90 for _ in source_model_names],
			"latest_strict_mean_regret_uah": [314.8126598731152 for _ in source_model_names],
			"latest_selected_mean_regret_uah": [248.48758297808885 for _ in source_model_names],
			"latest_strict_median_regret_uah": [202.60626109078976 for _ in source_model_names],
			"latest_selected_median_regret_uah": [89.89137186765288 for _ in source_model_names],
			"latest_mean_regret_improvement_ratio_vs_strict": [
				0.21068109815456143 for _ in source_model_names
			],
			"latest_median_not_worse": [True for _ in source_model_names],
			"latest_source_signal": [True for _ in source_model_names],
			"rolling_window_count": [4 for _ in source_model_names],
			"rolling_strict_pass_window_count": [3 for _ in source_model_names],
			"rolling_development_pass_window_count": [4 for _ in source_model_names],
			"robust_research_challenger": [True for _ in source_model_names],
			"allowed_challenger": [
				f"dfl_schedule_value_learner_v2_{source_model_name}"
				for source_model_name in source_model_names
			],
			"fallback_strategy": [
				"strict_similar_day_default_fallback" for _ in source_model_names
			],
			"promotion_blocker": ["none" for _ in source_model_names],
			"production_promote": [True for _ in source_model_names],
			"market_execution_enabled": [False for _ in source_model_names],
			"claim_scope": [
				"dfl_schedule_value_production_gate_offline_strategy_not_market_execution"
				for _ in source_model_names
			],
			"academic_scope": [
				"Offline/read-model default-fallback gate for the Schedule/Value Learner V2."
				for _ in source_model_names
			],
			"not_full_dfl": [True for _ in source_model_names],
			"not_market_execution": [True for _ in source_model_names],
			"generated_at": [generated_at for _ in source_model_names],
		}
	)
