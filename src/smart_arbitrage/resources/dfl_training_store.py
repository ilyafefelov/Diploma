from __future__ import annotations

from functools import cache
import os
from typing import Any, Protocol

import polars as pl


class DflTrainingStore(Protocol):
    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None: ...

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None: ...


class NullDflTrainingStore:
    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None:
        return None

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None:
        return None


class InMemoryDflTrainingStore:
    def __init__(self) -> None:
        self.training_frame = pl.DataFrame()
        self.pilot_frame = pl.DataFrame()

    def upsert_training_frame(self, training_frame: pl.DataFrame) -> None:
        self.training_frame = training_frame.clone()

    def upsert_pilot_frame(self, pilot_frame: pl.DataFrame) -> None:
        self.pilot_frame = pilot_frame.clone()


class PostgresDflTrainingStore:
    def __init__(self, dsn: str) -> None:
        self._dsn = dsn
        self._ensure_schema()

    def _connect(self) -> Any:
        from psycopg import connect

        return connect(self._dsn)

    def _ensure_schema(self) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
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


@cache
def get_dfl_training_store() -> DflTrainingStore:
    dsn = os.environ.get("SMART_ARBITRAGE_DFL_TRAINING_DSN") or os.environ.get("SMART_ARBITRAGE_MARKET_DATA_DSN")
    if dsn is None:
        return NullDflTrainingStore()
    return PostgresDflTrainingStore(dsn)
