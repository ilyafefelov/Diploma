from __future__ import annotations

from functools import cache
import json
import os
from typing import Any, Protocol

import polars as pl


class DflTrainingStore(Protocol):
    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None: ...

    def upsert_training_example_frame(self, training_example_frame: pl.DataFrame) -> None: ...

    def upsert_action_label_frame(self, action_label_frame: pl.DataFrame) -> None: ...

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None: ...

    def upsert_relaxed_pilot_frame(self, relaxed_pilot_frame: pl.DataFrame) -> None: ...

    def latest_relaxed_pilot_frame(self, *, tenant_id: str) -> pl.DataFrame: ...


class NullDflTrainingStore:
    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None:
        return None

    def upsert_training_example_frame(self, training_example_frame: pl.DataFrame) -> None:
        return None

    def upsert_action_label_frame(self, action_label_frame: pl.DataFrame) -> None:
        return None

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None:
        return None

    def upsert_relaxed_pilot_frame(self, relaxed_pilot_frame: pl.DataFrame) -> None:
        return None

    def latest_relaxed_pilot_frame(self, *, tenant_id: str) -> pl.DataFrame:
        return pl.DataFrame()


class InMemoryDflTrainingStore:
    def __init__(self) -> None:
        self.training_frame = pl.DataFrame()
        self.training_example_frame = pl.DataFrame()
        self.action_label_frame = pl.DataFrame()
        self.pilot_frame = pl.DataFrame()
        self.relaxed_pilot_frame = pl.DataFrame()

    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None:
        self.training_frame = training_frame.clone()

    def upsert_training_example_frame(self, training_example_frame: pl.DataFrame) -> None:
        self.training_example_frame = training_example_frame.clone()

    def upsert_action_label_frame(self, action_label_frame: pl.DataFrame) -> None:
        self.action_label_frame = action_label_frame.clone()

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None:
        self.pilot_frame = pilot_frame.clone()

    def upsert_relaxed_pilot_frame(self, relaxed_pilot_frame: pl.DataFrame) -> None:
        self.relaxed_pilot_frame = _append_or_replace(
            self.relaxed_pilot_frame,
            relaxed_pilot_frame,
            subset=["pilot_name", "evaluation_id"],
        )

    def latest_relaxed_pilot_frame(self, *, tenant_id: str) -> pl.DataFrame:
        if self.relaxed_pilot_frame.height == 0:
            return pl.DataFrame()
        tenant_frame = self.relaxed_pilot_frame.filter(pl.col("tenant_id") == tenant_id)
        if tenant_frame.height == 0:
            return pl.DataFrame()
        return tenant_frame.sort(["anchor_timestamp", "forecast_model_name"])


class PostgresDflTrainingStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._ensure_schema()

    def _connect(self) -> Any:
        from psycopg import connect
        from psycopg.rows import dict_row

        return connect(self._dsn, row_factory=dict_row)

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfl_action_label_vectors (
                        action_label_id TEXT PRIMARY KEY,
                        evaluation_id TEXT NOT NULL,
                        strict_baseline_evaluation_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        anchor_timestamp TIMESTAMP NOT NULL,
                        split_name TEXT NOT NULL,
                        is_final_holdout BOOLEAN NOT NULL,
                        horizon_start TIMESTAMP NOT NULL,
                        horizon_end TIMESTAMP NOT NULL,
                        horizon_hours INTEGER NOT NULL,
                        market_venue TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        forecast_model_name TEXT NOT NULL,
                        source_strategy_kind TEXT NOT NULL,
                        strict_baseline_forecast_model_name TEXT NOT NULL,
                        target_strategy_name TEXT NOT NULL,
                        forecast_price_vector_uah_mwh JSONB NOT NULL,
                        actual_price_vector_uah_mwh JSONB NOT NULL,
                        candidate_signed_dispatch_vector_mw JSONB NOT NULL,
                        strict_baseline_signed_dispatch_vector_mw JSONB NOT NULL,
                        oracle_signed_dispatch_vector_mw JSONB NOT NULL,
                        oracle_charge_mw_vector JSONB NOT NULL,
                        oracle_discharge_mw_vector JSONB NOT NULL,
                        oracle_soc_before_mwh_vector JSONB NOT NULL,
                        oracle_soc_after_mwh_vector JSONB NOT NULL,
                        oracle_degradation_penalty_vector_uah JSONB NOT NULL,
                        target_charge_mask JSONB NOT NULL,
                        target_discharge_mask JSONB NOT NULL,
                        target_hold_mask JSONB NOT NULL,
                        candidate_net_value_uah DOUBLE PRECISION NOT NULL,
                        strict_baseline_net_value_uah DOUBLE PRECISION NOT NULL,
                        oracle_net_value_uah DOUBLE PRECISION NOT NULL,
                        candidate_regret_uah DOUBLE PRECISION NOT NULL,
                        strict_baseline_regret_uah DOUBLE PRECISION NOT NULL,
                        regret_delta_vs_strict_baseline_uah DOUBLE PRECISION NOT NULL,
                        candidate_total_throughput_mwh DOUBLE PRECISION NOT NULL,
                        strict_baseline_total_throughput_mwh DOUBLE PRECISION NOT NULL,
                        candidate_total_degradation_penalty_uah DOUBLE PRECISION NOT NULL,
                        strict_baseline_total_degradation_penalty_uah DOUBLE PRECISION NOT NULL,
                        candidate_safety_violation_count INTEGER NOT NULL,
                        strict_baseline_safety_violation_count INTEGER NOT NULL,
                        data_quality_tier TEXT NOT NULL,
                        observed_coverage_ratio DOUBLE PRECISION NOT NULL,
                        claim_scope TEXT NOT NULL,
                        not_full_dfl BOOLEAN NOT NULL,
                        not_market_execution BOOLEAN NOT NULL,
                        generated_at TIMESTAMP NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfl_training_example_vectors (
                        training_example_id TEXT PRIMARY KEY,
                        evaluation_id TEXT NOT NULL,
                        baseline_evaluation_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        anchor_timestamp TIMESTAMP NOT NULL,
                        horizon_start TIMESTAMP NOT NULL,
                        horizon_end TIMESTAMP NOT NULL,
                        horizon_hours INTEGER NOT NULL,
                        market_venue TEXT NOT NULL,
                        currency TEXT NOT NULL,
                        forecast_model_name TEXT NOT NULL,
                        strategy_kind TEXT NOT NULL,
                        baseline_strategy_name TEXT NOT NULL,
                        baseline_forecast_model_name TEXT NOT NULL,
                        forecast_price_vector_uah_mwh JSONB NOT NULL,
                        actual_price_vector_uah_mwh JSONB NOT NULL,
                        candidate_dispatch_vector_mw JSONB NOT NULL,
                        baseline_dispatch_vector_mw JSONB NOT NULL,
                        candidate_degradation_penalty_vector_uah JSONB NOT NULL,
                        baseline_degradation_penalty_vector_uah JSONB NOT NULL,
                        candidate_net_value_uah DOUBLE PRECISION NOT NULL,
                        baseline_net_value_uah DOUBLE PRECISION NOT NULL,
                        oracle_net_value_uah DOUBLE PRECISION NOT NULL,
                        candidate_regret_uah DOUBLE PRECISION NOT NULL,
                        baseline_regret_uah DOUBLE PRECISION NOT NULL,
                        regret_delta_vs_baseline_uah DOUBLE PRECISION NOT NULL,
                        total_throughput_mwh DOUBLE PRECISION NOT NULL,
                        total_degradation_penalty_uah DOUBLE PRECISION NOT NULL,
                        candidate_feasible BOOLEAN NOT NULL,
                        baseline_feasible BOOLEAN NOT NULL,
                        safety_violation_count INTEGER NOT NULL,
                        data_quality_tier TEXT NOT NULL,
                        observed_coverage_ratio DOUBLE PRECISION NOT NULL,
                        claim_scope TEXT NOT NULL,
                        not_full_dfl BOOLEAN NOT NULL,
                        not_market_execution BOOLEAN NOT NULL,
                        generated_at TIMESTAMP NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfl_training_examples (
                        training_example_id TEXT PRIMARY KEY,
                        evaluation_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        anchor_timestamp TIMESTAMP NOT NULL,
                        forecast_model_name TEXT NOT NULL,
                        strategy_kind TEXT NOT NULL,
                        market_venue TEXT NOT NULL,
                        horizon_hours INTEGER NOT NULL,
                        starting_soc_fraction DOUBLE PRECISION NOT NULL,
                        starting_soc_source TEXT NOT NULL,
                        lp_committed_action TEXT NOT NULL,
                        lp_committed_power_mw DOUBLE PRECISION NOT NULL,
                        first_action_net_power_mw DOUBLE PRECISION NOT NULL,
                        decision_value_uah DOUBLE PRECISION NOT NULL,
                        forecast_objective_value_uah DOUBLE PRECISION NOT NULL,
                        oracle_value_uah DOUBLE PRECISION NOT NULL,
                        regret_uah DOUBLE PRECISION NOT NULL,
                        regret_ratio DOUBLE PRECISION NOT NULL,
                        total_degradation_penalty_uah DOUBLE PRECISION NOT NULL,
                        total_throughput_mwh DOUBLE PRECISION NOT NULL,
                        efc_proxy DOUBLE PRECISION NOT NULL,
                        mean_forecast_price_uah_mwh DOUBLE PRECISION NOT NULL,
                        mean_actual_price_uah_mwh DOUBLE PRECISION NOT NULL,
                        forecast_mae_uah_mwh DOUBLE PRECISION NOT NULL,
                        forecast_rmse_uah_mwh DOUBLE PRECISION NOT NULL,
                        forecast_smape DOUBLE PRECISION NOT NULL,
                        directional_accuracy DOUBLE PRECISION NOT NULL,
                        spread_ranking_quality DOUBLE PRECISION NOT NULL,
                        top_k_price_recall DOUBLE PRECISION NOT NULL,
                        training_weight DOUBLE PRECISION NOT NULL,
                        data_quality_tier TEXT NOT NULL,
                        observed_coverage_ratio DOUBLE PRECISION NOT NULL,
                        market_price_cap_max DOUBLE PRECISION NOT NULL,
                        market_price_cap_min DOUBLE PRECISION NOT NULL,
                        market_regime_id TEXT NOT NULL,
                        days_since_regime_change DOUBLE PRECISION NOT NULL,
                        is_price_cap_changed_recently DOUBLE PRECISION NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS regret_weighted_dfl_pilot_runs (
                        pilot_name TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        forecast_model_name TEXT NOT NULL,
                        scope TEXT NOT NULL,
                        train_rows INTEGER NOT NULL,
                        validation_rows INTEGER NOT NULL,
                        regret_weighted_bias_uah_mwh DOUBLE PRECISION NOT NULL,
                        validation_weighted_mae_before DOUBLE PRECISION NOT NULL,
                        validation_weighted_mae_after DOUBLE PRECISION NOT NULL,
                        validation_weighted_mae_delta DOUBLE PRECISION NOT NULL,
                        mean_validation_regret_uah DOUBLE PRECISION NOT NULL,
                        expanded_to_all_tenants_ready BOOLEAN NOT NULL,
                        academic_scope TEXT NOT NULL,
                        PRIMARY KEY (pilot_name, tenant_id, forecast_model_name)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS dfl_relaxed_lp_pilot_runs (
                        pilot_name TEXT NOT NULL,
                        evaluation_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        forecast_model_name TEXT NOT NULL,
                        anchor_timestamp TIMESTAMP NOT NULL,
                        horizon_hours INTEGER NOT NULL,
                        relaxed_realized_value_uah DOUBLE PRECISION NOT NULL,
                        relaxed_oracle_value_uah DOUBLE PRECISION NOT NULL,
                        relaxed_regret_uah DOUBLE PRECISION NOT NULL,
                        first_charge_mw DOUBLE PRECISION NOT NULL,
                        first_discharge_mw DOUBLE PRECISION NOT NULL,
                        academic_scope TEXT NOT NULL,
                        PRIMARY KEY (pilot_name, evaluation_id)
                    )
                    """
                )
            connection.commit()

    def upsert_action_label_frame(self, action_label_frame: pl.DataFrame) -> None:
        if action_label_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO dfl_action_label_vectors (
                        action_label_id,
                        evaluation_id,
                        strict_baseline_evaluation_id,
                        tenant_id,
                        anchor_timestamp,
                        split_name,
                        is_final_holdout,
                        horizon_start,
                        horizon_end,
                        horizon_hours,
                        market_venue,
                        currency,
                        forecast_model_name,
                        source_strategy_kind,
                        strict_baseline_forecast_model_name,
                        target_strategy_name,
                        forecast_price_vector_uah_mwh,
                        actual_price_vector_uah_mwh,
                        candidate_signed_dispatch_vector_mw,
                        strict_baseline_signed_dispatch_vector_mw,
                        oracle_signed_dispatch_vector_mw,
                        oracle_charge_mw_vector,
                        oracle_discharge_mw_vector,
                        oracle_soc_before_mwh_vector,
                        oracle_soc_after_mwh_vector,
                        oracle_degradation_penalty_vector_uah,
                        target_charge_mask,
                        target_discharge_mask,
                        target_hold_mask,
                        candidate_net_value_uah,
                        strict_baseline_net_value_uah,
                        oracle_net_value_uah,
                        candidate_regret_uah,
                        strict_baseline_regret_uah,
                        regret_delta_vs_strict_baseline_uah,
                        candidate_total_throughput_mwh,
                        strict_baseline_total_throughput_mwh,
                        candidate_total_degradation_penalty_uah,
                        strict_baseline_total_degradation_penalty_uah,
                        candidate_safety_violation_count,
                        strict_baseline_safety_violation_count,
                        data_quality_tier,
                        observed_coverage_ratio,
                        claim_scope,
                        not_full_dfl,
                        not_market_execution,
                        generated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (action_label_id)
                    DO UPDATE SET
                        evaluation_id = EXCLUDED.evaluation_id,
                        strict_baseline_evaluation_id = EXCLUDED.strict_baseline_evaluation_id,
                        tenant_id = EXCLUDED.tenant_id,
                        anchor_timestamp = EXCLUDED.anchor_timestamp,
                        split_name = EXCLUDED.split_name,
                        is_final_holdout = EXCLUDED.is_final_holdout,
                        horizon_start = EXCLUDED.horizon_start,
                        horizon_end = EXCLUDED.horizon_end,
                        horizon_hours = EXCLUDED.horizon_hours,
                        market_venue = EXCLUDED.market_venue,
                        currency = EXCLUDED.currency,
                        forecast_model_name = EXCLUDED.forecast_model_name,
                        source_strategy_kind = EXCLUDED.source_strategy_kind,
                        strict_baseline_forecast_model_name = EXCLUDED.strict_baseline_forecast_model_name,
                        target_strategy_name = EXCLUDED.target_strategy_name,
                        forecast_price_vector_uah_mwh = EXCLUDED.forecast_price_vector_uah_mwh,
                        actual_price_vector_uah_mwh = EXCLUDED.actual_price_vector_uah_mwh,
                        candidate_signed_dispatch_vector_mw = EXCLUDED.candidate_signed_dispatch_vector_mw,
                        strict_baseline_signed_dispatch_vector_mw = EXCLUDED.strict_baseline_signed_dispatch_vector_mw,
                        oracle_signed_dispatch_vector_mw = EXCLUDED.oracle_signed_dispatch_vector_mw,
                        oracle_charge_mw_vector = EXCLUDED.oracle_charge_mw_vector,
                        oracle_discharge_mw_vector = EXCLUDED.oracle_discharge_mw_vector,
                        oracle_soc_before_mwh_vector = EXCLUDED.oracle_soc_before_mwh_vector,
                        oracle_soc_after_mwh_vector = EXCLUDED.oracle_soc_after_mwh_vector,
                        oracle_degradation_penalty_vector_uah = EXCLUDED.oracle_degradation_penalty_vector_uah,
                        target_charge_mask = EXCLUDED.target_charge_mask,
                        target_discharge_mask = EXCLUDED.target_discharge_mask,
                        target_hold_mask = EXCLUDED.target_hold_mask,
                        candidate_net_value_uah = EXCLUDED.candidate_net_value_uah,
                        strict_baseline_net_value_uah = EXCLUDED.strict_baseline_net_value_uah,
                        oracle_net_value_uah = EXCLUDED.oracle_net_value_uah,
                        candidate_regret_uah = EXCLUDED.candidate_regret_uah,
                        strict_baseline_regret_uah = EXCLUDED.strict_baseline_regret_uah,
                        regret_delta_vs_strict_baseline_uah = EXCLUDED.regret_delta_vs_strict_baseline_uah,
                        candidate_total_throughput_mwh = EXCLUDED.candidate_total_throughput_mwh,
                        strict_baseline_total_throughput_mwh = EXCLUDED.strict_baseline_total_throughput_mwh,
                        candidate_total_degradation_penalty_uah = EXCLUDED.candidate_total_degradation_penalty_uah,
                        strict_baseline_total_degradation_penalty_uah = EXCLUDED.strict_baseline_total_degradation_penalty_uah,
                        candidate_safety_violation_count = EXCLUDED.candidate_safety_violation_count,
                        strict_baseline_safety_violation_count = EXCLUDED.strict_baseline_safety_violation_count,
                        data_quality_tier = EXCLUDED.data_quality_tier,
                        observed_coverage_ratio = EXCLUDED.observed_coverage_ratio,
                        claim_scope = EXCLUDED.claim_scope,
                        not_full_dfl = EXCLUDED.not_full_dfl,
                        not_market_execution = EXCLUDED.not_market_execution,
                        generated_at = EXCLUDED.generated_at
                    """,
                    [_action_label_values(row) for row in action_label_frame.iter_rows(named=True)],
                )
            connection.commit()

    def upsert_training_example_frame(self, training_example_frame: pl.DataFrame) -> None:
        if training_example_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO dfl_training_example_vectors (
                        training_example_id,
                        evaluation_id,
                        baseline_evaluation_id,
                        tenant_id,
                        anchor_timestamp,
                        horizon_start,
                        horizon_end,
                        horizon_hours,
                        market_venue,
                        currency,
                        forecast_model_name,
                        strategy_kind,
                        baseline_strategy_name,
                        baseline_forecast_model_name,
                        forecast_price_vector_uah_mwh,
                        actual_price_vector_uah_mwh,
                        candidate_dispatch_vector_mw,
                        baseline_dispatch_vector_mw,
                        candidate_degradation_penalty_vector_uah,
                        baseline_degradation_penalty_vector_uah,
                        candidate_net_value_uah,
                        baseline_net_value_uah,
                        oracle_net_value_uah,
                        candidate_regret_uah,
                        baseline_regret_uah,
                        regret_delta_vs_baseline_uah,
                        total_throughput_mwh,
                        total_degradation_penalty_uah,
                        candidate_feasible,
                        baseline_feasible,
                        safety_violation_count,
                        data_quality_tier,
                        observed_coverage_ratio,
                        claim_scope,
                        not_full_dfl,
                        not_market_execution,
                        generated_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (training_example_id)
                    DO UPDATE SET
                        evaluation_id = EXCLUDED.evaluation_id,
                        baseline_evaluation_id = EXCLUDED.baseline_evaluation_id,
                        tenant_id = EXCLUDED.tenant_id,
                        anchor_timestamp = EXCLUDED.anchor_timestamp,
                        horizon_start = EXCLUDED.horizon_start,
                        horizon_end = EXCLUDED.horizon_end,
                        horizon_hours = EXCLUDED.horizon_hours,
                        market_venue = EXCLUDED.market_venue,
                        currency = EXCLUDED.currency,
                        forecast_model_name = EXCLUDED.forecast_model_name,
                        strategy_kind = EXCLUDED.strategy_kind,
                        baseline_strategy_name = EXCLUDED.baseline_strategy_name,
                        baseline_forecast_model_name = EXCLUDED.baseline_forecast_model_name,
                        forecast_price_vector_uah_mwh = EXCLUDED.forecast_price_vector_uah_mwh,
                        actual_price_vector_uah_mwh = EXCLUDED.actual_price_vector_uah_mwh,
                        candidate_dispatch_vector_mw = EXCLUDED.candidate_dispatch_vector_mw,
                        baseline_dispatch_vector_mw = EXCLUDED.baseline_dispatch_vector_mw,
                        candidate_degradation_penalty_vector_uah = EXCLUDED.candidate_degradation_penalty_vector_uah,
                        baseline_degradation_penalty_vector_uah = EXCLUDED.baseline_degradation_penalty_vector_uah,
                        candidate_net_value_uah = EXCLUDED.candidate_net_value_uah,
                        baseline_net_value_uah = EXCLUDED.baseline_net_value_uah,
                        oracle_net_value_uah = EXCLUDED.oracle_net_value_uah,
                        candidate_regret_uah = EXCLUDED.candidate_regret_uah,
                        baseline_regret_uah = EXCLUDED.baseline_regret_uah,
                        regret_delta_vs_baseline_uah = EXCLUDED.regret_delta_vs_baseline_uah,
                        total_throughput_mwh = EXCLUDED.total_throughput_mwh,
                        total_degradation_penalty_uah = EXCLUDED.total_degradation_penalty_uah,
                        candidate_feasible = EXCLUDED.candidate_feasible,
                        baseline_feasible = EXCLUDED.baseline_feasible,
                        safety_violation_count = EXCLUDED.safety_violation_count,
                        data_quality_tier = EXCLUDED.data_quality_tier,
                        observed_coverage_ratio = EXCLUDED.observed_coverage_ratio,
                        claim_scope = EXCLUDED.claim_scope,
                        not_full_dfl = EXCLUDED.not_full_dfl,
                        not_market_execution = EXCLUDED.not_market_execution,
                        generated_at = EXCLUDED.generated_at
                    """,
                    [_training_example_values(row) for row in training_example_frame.iter_rows(named=True)],
                )
            connection.commit()

    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None:
        if training_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO dfl_training_examples (
                        training_example_id,
                        evaluation_id,
                        tenant_id,
                        anchor_timestamp,
                        forecast_model_name,
                        strategy_kind,
                        market_venue,
                        horizon_hours,
                        starting_soc_fraction,
                        starting_soc_source,
                        lp_committed_action,
                        lp_committed_power_mw,
                        first_action_net_power_mw,
                        decision_value_uah,
                        forecast_objective_value_uah,
                        oracle_value_uah,
                        regret_uah,
                        regret_ratio,
                        total_degradation_penalty_uah,
                        total_throughput_mwh,
                        efc_proxy,
                        mean_forecast_price_uah_mwh,
                        mean_actual_price_uah_mwh,
                        forecast_mae_uah_mwh,
                        forecast_rmse_uah_mwh,
                        forecast_smape,
                        directional_accuracy,
                        spread_ranking_quality,
                        top_k_price_recall,
                        training_weight,
                        data_quality_tier,
                        observed_coverage_ratio,
                        market_price_cap_max,
                        market_price_cap_min,
                        market_regime_id,
                        days_since_regime_change,
                        is_price_cap_changed_recently
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (training_example_id)
                    DO UPDATE SET
                        evaluation_id = EXCLUDED.evaluation_id,
                        tenant_id = EXCLUDED.tenant_id,
                        anchor_timestamp = EXCLUDED.anchor_timestamp,
                        forecast_model_name = EXCLUDED.forecast_model_name,
                        strategy_kind = EXCLUDED.strategy_kind,
                        market_venue = EXCLUDED.market_venue,
                        horizon_hours = EXCLUDED.horizon_hours,
                        starting_soc_fraction = EXCLUDED.starting_soc_fraction,
                        starting_soc_source = EXCLUDED.starting_soc_source,
                        lp_committed_action = EXCLUDED.lp_committed_action,
                        lp_committed_power_mw = EXCLUDED.lp_committed_power_mw,
                        first_action_net_power_mw = EXCLUDED.first_action_net_power_mw,
                        decision_value_uah = EXCLUDED.decision_value_uah,
                        forecast_objective_value_uah = EXCLUDED.forecast_objective_value_uah,
                        oracle_value_uah = EXCLUDED.oracle_value_uah,
                        regret_uah = EXCLUDED.regret_uah,
                        regret_ratio = EXCLUDED.regret_ratio,
                        total_degradation_penalty_uah = EXCLUDED.total_degradation_penalty_uah,
                        total_throughput_mwh = EXCLUDED.total_throughput_mwh,
                        efc_proxy = EXCLUDED.efc_proxy,
                        mean_forecast_price_uah_mwh = EXCLUDED.mean_forecast_price_uah_mwh,
                        mean_actual_price_uah_mwh = EXCLUDED.mean_actual_price_uah_mwh,
                        forecast_mae_uah_mwh = EXCLUDED.forecast_mae_uah_mwh,
                        forecast_rmse_uah_mwh = EXCLUDED.forecast_rmse_uah_mwh,
                        forecast_smape = EXCLUDED.forecast_smape,
                        directional_accuracy = EXCLUDED.directional_accuracy,
                        spread_ranking_quality = EXCLUDED.spread_ranking_quality,
                        top_k_price_recall = EXCLUDED.top_k_price_recall,
                        training_weight = EXCLUDED.training_weight,
                        data_quality_tier = EXCLUDED.data_quality_tier,
                        observed_coverage_ratio = EXCLUDED.observed_coverage_ratio,
                        market_price_cap_max = EXCLUDED.market_price_cap_max,
                        market_price_cap_min = EXCLUDED.market_price_cap_min,
                        market_regime_id = EXCLUDED.market_regime_id,
                        days_since_regime_change = EXCLUDED.days_since_regime_change,
                        is_price_cap_changed_recently = EXCLUDED.is_price_cap_changed_recently
                    """,
                    [_training_values(row) for row in training_frame.iter_rows(named=True)],
                )
            connection.commit()

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None:
        if pilot_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO regret_weighted_dfl_pilot_runs (
                        pilot_name,
                        tenant_id,
                        forecast_model_name,
                        scope,
                        train_rows,
                        validation_rows,
                        regret_weighted_bias_uah_mwh,
                        validation_weighted_mae_before,
                        validation_weighted_mae_after,
                        validation_weighted_mae_delta,
                        mean_validation_regret_uah,
                        expanded_to_all_tenants_ready,
                        academic_scope
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pilot_name, tenant_id, forecast_model_name)
                    DO UPDATE SET
                        scope = EXCLUDED.scope,
                        train_rows = EXCLUDED.train_rows,
                        validation_rows = EXCLUDED.validation_rows,
                        regret_weighted_bias_uah_mwh = EXCLUDED.regret_weighted_bias_uah_mwh,
                        validation_weighted_mae_before = EXCLUDED.validation_weighted_mae_before,
                        validation_weighted_mae_after = EXCLUDED.validation_weighted_mae_after,
                        validation_weighted_mae_delta = EXCLUDED.validation_weighted_mae_delta,
                        mean_validation_regret_uah = EXCLUDED.mean_validation_regret_uah,
                        expanded_to_all_tenants_ready = EXCLUDED.expanded_to_all_tenants_ready,
                        academic_scope = EXCLUDED.academic_scope
                    """,
                    [_pilot_values(row) for row in pilot_frame.iter_rows(named=True)],
                )
            connection.commit()

    def upsert_relaxed_pilot_frame(self, relaxed_pilot_frame: pl.DataFrame) -> None:
        if relaxed_pilot_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO dfl_relaxed_lp_pilot_runs (
                        pilot_name,
                        evaluation_id,
                        tenant_id,
                        forecast_model_name,
                        anchor_timestamp,
                        horizon_hours,
                        relaxed_realized_value_uah,
                        relaxed_oracle_value_uah,
                        relaxed_regret_uah,
                        first_charge_mw,
                        first_discharge_mw,
                        academic_scope
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (pilot_name, evaluation_id)
                    DO UPDATE SET
                        tenant_id = EXCLUDED.tenant_id,
                        forecast_model_name = EXCLUDED.forecast_model_name,
                        anchor_timestamp = EXCLUDED.anchor_timestamp,
                        horizon_hours = EXCLUDED.horizon_hours,
                        relaxed_realized_value_uah = EXCLUDED.relaxed_realized_value_uah,
                        relaxed_oracle_value_uah = EXCLUDED.relaxed_oracle_value_uah,
                        relaxed_regret_uah = EXCLUDED.relaxed_regret_uah,
                        first_charge_mw = EXCLUDED.first_charge_mw,
                        first_discharge_mw = EXCLUDED.first_discharge_mw,
                        academic_scope = EXCLUDED.academic_scope
                    """,
                    [_relaxed_pilot_values(row) for row in relaxed_pilot_frame.iter_rows(named=True)],
                )
            connection.commit()

    def latest_relaxed_pilot_frame(self, *, tenant_id: str) -> pl.DataFrame:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM dfl_relaxed_lp_pilot_runs
                    WHERE tenant_id = %s
                    ORDER BY anchor_timestamp, forecast_model_name
                    """,
                    (tenant_id,),
                )
                rows = cursor.fetchall()
        return pl.DataFrame([dict(row) for row in rows])


def _training_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["training_example_id"],
        row["evaluation_id"],
        row["tenant_id"],
        row["anchor_timestamp"],
        row["forecast_model_name"],
        row["strategy_kind"],
        row["market_venue"],
        row["horizon_hours"],
        row["starting_soc_fraction"],
        row["starting_soc_source"],
        row["lp_committed_action"],
        row["lp_committed_power_mw"],
        row["first_action_net_power_mw"],
        row["decision_value_uah"],
        row["forecast_objective_value_uah"],
        row["oracle_value_uah"],
        row["regret_uah"],
        row["regret_ratio"],
        row["total_degradation_penalty_uah"],
        row["total_throughput_mwh"],
        row["efc_proxy"],
        row["mean_forecast_price_uah_mwh"],
        row["mean_actual_price_uah_mwh"],
        row["forecast_mae_uah_mwh"],
        row["forecast_rmse_uah_mwh"],
        row["forecast_smape"],
        row["directional_accuracy"],
        row["spread_ranking_quality"],
        row["top_k_price_recall"],
        row["training_weight"],
        row["data_quality_tier"],
        row["observed_coverage_ratio"],
        row["market_price_cap_max"],
        row["market_price_cap_min"],
        row["market_regime_id"],
        row["days_since_regime_change"],
        row["is_price_cap_changed_recently"],
    )


def _training_example_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["training_example_id"],
        row["evaluation_id"],
        row["baseline_evaluation_id"],
        row["tenant_id"],
        row["anchor_timestamp"],
        row["horizon_start"],
        row["horizon_end"],
        row["horizon_hours"],
        row["market_venue"],
        row["currency"],
        row["forecast_model_name"],
        row["strategy_kind"],
        row["baseline_strategy_name"],
        row["baseline_forecast_model_name"],
        json.dumps(row["forecast_price_vector_uah_mwh"]),
        json.dumps(row["actual_price_vector_uah_mwh"]),
        json.dumps(row["candidate_dispatch_vector_mw"]),
        json.dumps(row["baseline_dispatch_vector_mw"]),
        json.dumps(row["candidate_degradation_penalty_vector_uah"]),
        json.dumps(row["baseline_degradation_penalty_vector_uah"]),
        row["candidate_net_value_uah"],
        row["baseline_net_value_uah"],
        row["oracle_net_value_uah"],
        row["candidate_regret_uah"],
        row["baseline_regret_uah"],
        row["regret_delta_vs_baseline_uah"],
        row["total_throughput_mwh"],
        row["total_degradation_penalty_uah"],
        row["candidate_feasible"],
        row["baseline_feasible"],
        row["safety_violation_count"],
        row["data_quality_tier"],
        row["observed_coverage_ratio"],
        row["claim_scope"],
        row["not_full_dfl"],
        row["not_market_execution"],
        row["generated_at"],
    )


def _action_label_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["action_label_id"],
        row["evaluation_id"],
        row["strict_baseline_evaluation_id"],
        row["tenant_id"],
        row["anchor_timestamp"],
        row["split_name"],
        row["is_final_holdout"],
        row["horizon_start"],
        row["horizon_end"],
        row["horizon_hours"],
        row["market_venue"],
        row["currency"],
        row["forecast_model_name"],
        row["source_strategy_kind"],
        row["strict_baseline_forecast_model_name"],
        row["target_strategy_name"],
        json.dumps(row["forecast_price_vector_uah_mwh"]),
        json.dumps(row["actual_price_vector_uah_mwh"]),
        json.dumps(row["candidate_signed_dispatch_vector_mw"]),
        json.dumps(row["strict_baseline_signed_dispatch_vector_mw"]),
        json.dumps(row["oracle_signed_dispatch_vector_mw"]),
        json.dumps(row["oracle_charge_mw_vector"]),
        json.dumps(row["oracle_discharge_mw_vector"]),
        json.dumps(row["oracle_soc_before_mwh_vector"]),
        json.dumps(row["oracle_soc_after_mwh_vector"]),
        json.dumps(row["oracle_degradation_penalty_vector_uah"]),
        json.dumps(row["target_charge_mask"]),
        json.dumps(row["target_discharge_mask"]),
        json.dumps(row["target_hold_mask"]),
        row["candidate_net_value_uah"],
        row["strict_baseline_net_value_uah"],
        row["oracle_net_value_uah"],
        row["candidate_regret_uah"],
        row["strict_baseline_regret_uah"],
        row["regret_delta_vs_strict_baseline_uah"],
        row["candidate_total_throughput_mwh"],
        row["strict_baseline_total_throughput_mwh"],
        row["candidate_total_degradation_penalty_uah"],
        row["strict_baseline_total_degradation_penalty_uah"],
        row["candidate_safety_violation_count"],
        row["strict_baseline_safety_violation_count"],
        row["data_quality_tier"],
        row["observed_coverage_ratio"],
        row["claim_scope"],
        row["not_full_dfl"],
        row["not_market_execution"],
        row["generated_at"],
    )


def _pilot_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["pilot_name"],
        row["tenant_id"],
        row["forecast_model_name"],
        row["scope"],
        row["train_rows"],
        row["validation_rows"],
        row["regret_weighted_bias_uah_mwh"],
        row["validation_weighted_mae_before"],
        row["validation_weighted_mae_after"],
        row["validation_weighted_mae_delta"],
        row["mean_validation_regret_uah"],
        row["expanded_to_all_tenants_ready"],
        row["academic_scope"],
    )


def _relaxed_pilot_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["pilot_name"],
        row["evaluation_id"],
        row["tenant_id"],
        row["forecast_model_name"],
        row["anchor_timestamp"],
        row["horizon_hours"],
        row["relaxed_realized_value_uah"],
        row["relaxed_oracle_value_uah"],
        row["relaxed_regret_uah"],
        row["first_charge_mw"],
        row["first_discharge_mw"],
        row["academic_scope"],
    )


def _append_or_replace(
    base_frame: pl.DataFrame, incoming_frame: pl.DataFrame, *, subset: list[str]
) -> pl.DataFrame:
    if incoming_frame.height == 0:
        return base_frame
    if base_frame.height == 0:
        return incoming_frame.clone()
    return pl.concat([base_frame, incoming_frame], how="diagonal_relaxed").unique(
        subset=subset,
        keep="last",
    )


@cache
def get_dfl_training_store() -> DflTrainingStore:
    dsn = os.environ.get("SMART_ARBITRAGE_DFL_TRAINING_DSN") or os.environ.get("SMART_ARBITRAGE_MARKET_DATA_DSN")
    if dsn is None:
        return NullDflTrainingStore()
    return PostgresDflTrainingStore(dsn)
