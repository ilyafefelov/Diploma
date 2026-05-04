from __future__ import annotations

from functools import cache
import json
import os
from typing import Any, Protocol

import polars as pl


class SimulatedTradeStore(Protocol):
    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None: ...


class NullSimulatedTradeStore:
    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None:
        return None


class InMemorySimulatedTradeStore:
    def __init__(self) -> None:
        self.episode_frame = pl.DataFrame()
        self.transition_frame = pl.DataFrame()

    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None:
        self.episode_frame = episode_frame.clone()
        self.transition_frame = transition_frame.clone()


class PostgresSimulatedTradeStore:
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
                    CREATE TABLE IF NOT EXISTS simulated_trade_episodes (
                        episode_id TEXT PRIMARY KEY,
                        tenant_id TEXT NOT NULL,
                        market_venue TEXT NOT NULL,
                        anchor_timestamp TIMESTAMP NOT NULL,
                        scenario_index INTEGER NOT NULL,
                        horizon_hours INTEGER NOT NULL,
                        baseline_value_uah DOUBLE PRECISION NOT NULL,
                        oracle_value_uah DOUBLE PRECISION NOT NULL,
                        regret_uah DOUBLE PRECISION NOT NULL,
                        seed INTEGER NOT NULL
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS simulated_dispatch_transitions (
                        episode_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        market_venue TEXT NOT NULL,
                        scenario_index INTEGER NOT NULL,
                        step_index INTEGER NOT NULL,
                        interval_start TIMESTAMP NOT NULL,
                        state_soc_before DOUBLE PRECISION NOT NULL,
                        state_soc_after DOUBLE PRECISION NOT NULL,
                        state_soh DOUBLE PRECISION NOT NULL,
                        action TEXT NOT NULL,
                        recommended_net_power_mw DOUBLE PRECISION NOT NULL,
                        feasible_net_power_mw DOUBLE PRECISION NOT NULL,
                        market_price_uah_mwh DOUBLE PRECISION NOT NULL,
                        reward_uah DOUBLE PRECISION NOT NULL,
                        degradation_penalty_uah DOUBLE PRECISION NOT NULL,
                        baseline_value_uah DOUBLE PRECISION NOT NULL,
                        oracle_value_uah DOUBLE PRECISION NOT NULL,
                        regret_uah DOUBLE PRECISION NOT NULL,
                        cleared_trade_provenance TEXT NOT NULL,
                        cleared_trade JSONB NOT NULL,
                        PRIMARY KEY (episode_id, step_index)
                    )
                    """
                )
            connection.commit()

    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                if episode_frame.height:
                    cursor.executemany(
                        """
                        INSERT INTO simulated_trade_episodes (
                            episode_id,
                            tenant_id,
                            market_venue,
                            anchor_timestamp,
                            scenario_index,
                            horizon_hours,
                            baseline_value_uah,
                            oracle_value_uah,
                            regret_uah,
                            seed
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (episode_id)
                        DO UPDATE SET
                            tenant_id = EXCLUDED.tenant_id,
                            market_venue = EXCLUDED.market_venue,
                            anchor_timestamp = EXCLUDED.anchor_timestamp,
                            scenario_index = EXCLUDED.scenario_index,
                            horizon_hours = EXCLUDED.horizon_hours,
                            baseline_value_uah = EXCLUDED.baseline_value_uah,
                            oracle_value_uah = EXCLUDED.oracle_value_uah,
                            regret_uah = EXCLUDED.regret_uah,
                            seed = EXCLUDED.seed
                        """,
                        [_episode_values(row) for row in episode_frame.iter_rows(named=True)],
                    )
                if transition_frame.height:
                    cursor.executemany(
                        """
                        INSERT INTO simulated_dispatch_transitions (
                            episode_id,
                            tenant_id,
                            market_venue,
                            scenario_index,
                            step_index,
                            interval_start,
                            state_soc_before,
                            state_soc_after,
                            state_soh,
                            action,
                            recommended_net_power_mw,
                            feasible_net_power_mw,
                            market_price_uah_mwh,
                            reward_uah,
                            degradation_penalty_uah,
                            baseline_value_uah,
                            oracle_value_uah,
                            regret_uah,
                            cleared_trade_provenance,
                            cleared_trade
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        ON CONFLICT (episode_id, step_index)
                        DO UPDATE SET
                            tenant_id = EXCLUDED.tenant_id,
                            market_venue = EXCLUDED.market_venue,
                            scenario_index = EXCLUDED.scenario_index,
                            interval_start = EXCLUDED.interval_start,
                            state_soc_before = EXCLUDED.state_soc_before,
                            state_soc_after = EXCLUDED.state_soc_after,
                            state_soh = EXCLUDED.state_soh,
                            action = EXCLUDED.action,
                            recommended_net_power_mw = EXCLUDED.recommended_net_power_mw,
                            feasible_net_power_mw = EXCLUDED.feasible_net_power_mw,
                            market_price_uah_mwh = EXCLUDED.market_price_uah_mwh,
                            reward_uah = EXCLUDED.reward_uah,
                            degradation_penalty_uah = EXCLUDED.degradation_penalty_uah,
                            baseline_value_uah = EXCLUDED.baseline_value_uah,
                            oracle_value_uah = EXCLUDED.oracle_value_uah,
                            regret_uah = EXCLUDED.regret_uah,
                            cleared_trade_provenance = EXCLUDED.cleared_trade_provenance,
                            cleared_trade = EXCLUDED.cleared_trade
                        """,
                        [_transition_values(row) for row in transition_frame.iter_rows(named=True)],
                    )
            connection.commit()


def _episode_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["episode_id"],
        row["tenant_id"],
        row["market_venue"],
        row["anchor_timestamp"],
        row["scenario_index"],
        row["horizon_hours"],
        row["baseline_value_uah"],
        row["oracle_value_uah"],
        row["regret_uah"],
        row["seed"],
    )


def _transition_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["episode_id"],
        row["tenant_id"],
        row["market_venue"],
        row["scenario_index"],
        row["step_index"],
        row["interval_start"],
        row["state_soc_before"],
        row["state_soc_after"],
        row["state_soh"],
        row["action"],
        row["recommended_net_power_mw"],
        row["feasible_net_power_mw"],
        row["market_price_uah_mwh"],
        row["reward_uah"],
        row["degradation_penalty_uah"],
        row["baseline_value_uah"],
        row["oracle_value_uah"],
        row["regret_uah"],
        row["cleared_trade_provenance"],
        json.dumps(row["cleared_trade"]),
    )


@cache
def get_simulated_trade_store() -> SimulatedTradeStore:
    dsn = os.environ.get("SMART_ARBITRAGE_SIMULATED_TRADE_DSN") or os.environ.get("SMART_ARBITRAGE_MARKET_DATA_DSN")
    if dsn is None:
        return NullSimulatedTradeStore()
    return PostgresSimulatedTradeStore(dsn)
