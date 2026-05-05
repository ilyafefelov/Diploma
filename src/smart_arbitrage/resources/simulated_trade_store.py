from __future__ import annotations

from functools import cache
import json
import os
from typing import Any, Protocol

import polars as pl


class SimulatedTradeStore(Protocol):
    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None: ...

    def upsert_decision_transformer_trajectory_frame(self, trajectory_frame: pl.DataFrame) -> None: ...

    def upsert_simulated_live_trading_frame(self, live_trading_frame: pl.DataFrame) -> None: ...

    def latest_decision_transformer_trajectory_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame: ...

    def latest_simulated_live_trading_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame: ...


class NullSimulatedTradeStore:
    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None:
        return None

    def upsert_decision_transformer_trajectory_frame(self, trajectory_frame: pl.DataFrame) -> None:
        return None

    def upsert_simulated_live_trading_frame(self, live_trading_frame: pl.DataFrame) -> None:
        return None

    def latest_decision_transformer_trajectory_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame:
        return pl.DataFrame()

    def latest_simulated_live_trading_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame:
        return pl.DataFrame()


class InMemorySimulatedTradeStore:
    def __init__(self) -> None:
        self.episode_frame = pl.DataFrame()
        self.transition_frame = pl.DataFrame()
        self.decision_transformer_trajectory_frame = pl.DataFrame()
        self.simulated_live_trading_frame = pl.DataFrame()

    def upsert_training_frames(self, *, episode_frame: pl.DataFrame, transition_frame: pl.DataFrame) -> None:
        self.episode_frame = episode_frame.clone()
        self.transition_frame = transition_frame.clone()

    def upsert_decision_transformer_trajectory_frame(self, trajectory_frame: pl.DataFrame) -> None:
        self.decision_transformer_trajectory_frame = _append_or_replace(
            self.decision_transformer_trajectory_frame,
            trajectory_frame,
            subset=["episode_id", "step_index"],
        )

    def upsert_simulated_live_trading_frame(self, live_trading_frame: pl.DataFrame) -> None:
        self.simulated_live_trading_frame = _append_or_replace(
            self.simulated_live_trading_frame,
            live_trading_frame,
            subset=["episode_id", "step_index"],
        )

    def latest_decision_transformer_trajectory_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame:
        return _latest_tenant_frame(self.decision_transformer_trajectory_frame, tenant_id=tenant_id, limit=limit)

    def latest_simulated_live_trading_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame:
        return _latest_tenant_frame(self.simulated_live_trading_frame, tenant_id=tenant_id, limit=limit)


class PostgresSimulatedTradeStore:
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
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS decision_transformer_trajectories (
                        episode_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        market_venue TEXT NOT NULL,
                        scenario_index INTEGER NOT NULL,
                        step_index INTEGER NOT NULL,
                        interval_start TIMESTAMP NOT NULL,
                        state_soc_before DOUBLE PRECISION NOT NULL,
                        state_soc_after DOUBLE PRECISION NOT NULL,
                        state_soh DOUBLE PRECISION NOT NULL,
                        state_market_price_uah_mwh DOUBLE PRECISION NOT NULL,
                        action_charge_mw DOUBLE PRECISION NOT NULL,
                        action_discharge_mw DOUBLE PRECISION NOT NULL,
                        reward_uah DOUBLE PRECISION NOT NULL,
                        return_to_go_uah DOUBLE PRECISION NOT NULL,
                        degradation_penalty_uah DOUBLE PRECISION NOT NULL,
                        baseline_value_uah DOUBLE PRECISION NOT NULL,
                        oracle_value_uah DOUBLE PRECISION NOT NULL,
                        regret_uah DOUBLE PRECISION NOT NULL,
                        academic_scope TEXT NOT NULL,
                        PRIMARY KEY (episode_id, step_index)
                    )
                    """
                )
                cursor.execute(
                    """
                    CREATE TABLE IF NOT EXISTS simulated_live_trading_rows (
                        episode_id TEXT NOT NULL,
                        tenant_id TEXT NOT NULL,
                        interval_start TIMESTAMP NOT NULL,
                        step_index INTEGER NOT NULL,
                        state_soc_before DOUBLE PRECISION NOT NULL,
                        state_soc_after DOUBLE PRECISION NOT NULL,
                        proposed_trade_side TEXT NOT NULL,
                        proposed_quantity_mw DOUBLE PRECISION NOT NULL,
                        feasible_net_power_mw DOUBLE PRECISION NOT NULL,
                        market_price_uah_mwh DOUBLE PRECISION NOT NULL,
                        reward_uah DOUBLE PRECISION NOT NULL,
                        gatekeeper_status TEXT NOT NULL,
                        paper_trade_provenance TEXT NOT NULL,
                        settlement_id TEXT,
                        live_mode_warning TEXT NOT NULL,
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

    def upsert_decision_transformer_trajectory_frame(self, trajectory_frame: pl.DataFrame) -> None:
        if trajectory_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO decision_transformer_trajectories (
                        episode_id,
                        tenant_id,
                        market_venue,
                        scenario_index,
                        step_index,
                        interval_start,
                        state_soc_before,
                        state_soc_after,
                        state_soh,
                        state_market_price_uah_mwh,
                        action_charge_mw,
                        action_discharge_mw,
                        reward_uah,
                        return_to_go_uah,
                        degradation_penalty_uah,
                        baseline_value_uah,
                        oracle_value_uah,
                        regret_uah,
                        academic_scope
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (episode_id, step_index)
                    DO UPDATE SET
                        tenant_id = EXCLUDED.tenant_id,
                        market_venue = EXCLUDED.market_venue,
                        scenario_index = EXCLUDED.scenario_index,
                        interval_start = EXCLUDED.interval_start,
                        state_soc_before = EXCLUDED.state_soc_before,
                        state_soc_after = EXCLUDED.state_soc_after,
                        state_soh = EXCLUDED.state_soh,
                        state_market_price_uah_mwh = EXCLUDED.state_market_price_uah_mwh,
                        action_charge_mw = EXCLUDED.action_charge_mw,
                        action_discharge_mw = EXCLUDED.action_discharge_mw,
                        reward_uah = EXCLUDED.reward_uah,
                        return_to_go_uah = EXCLUDED.return_to_go_uah,
                        degradation_penalty_uah = EXCLUDED.degradation_penalty_uah,
                        baseline_value_uah = EXCLUDED.baseline_value_uah,
                        oracle_value_uah = EXCLUDED.oracle_value_uah,
                        regret_uah = EXCLUDED.regret_uah,
                        academic_scope = EXCLUDED.academic_scope
                    """,
                    [_trajectory_values(row) for row in trajectory_frame.iter_rows(named=True)],
                )
            connection.commit()

    def upsert_simulated_live_trading_frame(self, live_trading_frame: pl.DataFrame) -> None:
        if live_trading_frame.height == 0:
            return None
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.executemany(
                    """
                    INSERT INTO simulated_live_trading_rows (
                        episode_id,
                        tenant_id,
                        interval_start,
                        step_index,
                        state_soc_before,
                        state_soc_after,
                        proposed_trade_side,
                        proposed_quantity_mw,
                        feasible_net_power_mw,
                        market_price_uah_mwh,
                        reward_uah,
                        gatekeeper_status,
                        paper_trade_provenance,
                        settlement_id,
                        live_mode_warning
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (episode_id, step_index)
                    DO UPDATE SET
                        tenant_id = EXCLUDED.tenant_id,
                        interval_start = EXCLUDED.interval_start,
                        state_soc_before = EXCLUDED.state_soc_before,
                        state_soc_after = EXCLUDED.state_soc_after,
                        proposed_trade_side = EXCLUDED.proposed_trade_side,
                        proposed_quantity_mw = EXCLUDED.proposed_quantity_mw,
                        feasible_net_power_mw = EXCLUDED.feasible_net_power_mw,
                        market_price_uah_mwh = EXCLUDED.market_price_uah_mwh,
                        reward_uah = EXCLUDED.reward_uah,
                        gatekeeper_status = EXCLUDED.gatekeeper_status,
                        paper_trade_provenance = EXCLUDED.paper_trade_provenance,
                        settlement_id = EXCLUDED.settlement_id,
                        live_mode_warning = EXCLUDED.live_mode_warning
                    """,
                    [_live_trading_values(row) for row in live_trading_frame.iter_rows(named=True)],
                )
            connection.commit()

    def latest_decision_transformer_trajectory_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM decision_transformer_trajectories
                    WHERE tenant_id = %s
                    ORDER BY interval_start, episode_id, step_index
                    LIMIT %s
                    """,
                    (tenant_id, limit),
                )
                rows = cursor.fetchall()
        return pl.DataFrame([dict(row) for row in rows])

    def latest_simulated_live_trading_frame(self, *, tenant_id: str, limit: int = 200) -> pl.DataFrame:
        with self._connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT *
                    FROM simulated_live_trading_rows
                    WHERE tenant_id = %s
                    ORDER BY interval_start, episode_id, step_index
                    LIMIT %s
                    """,
                    (tenant_id, limit),
                )
                rows = cursor.fetchall()
        return pl.DataFrame([dict(row) for row in rows])


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


def _trajectory_values(row: dict[str, Any]) -> tuple[Any, ...]:
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
        row["state_market_price_uah_mwh"],
        row["action_charge_mw"],
        row["action_discharge_mw"],
        row["reward_uah"],
        row["return_to_go_uah"],
        row["degradation_penalty_uah"],
        row["baseline_value_uah"],
        row["oracle_value_uah"],
        row["regret_uah"],
        row["academic_scope"],
    )


def _live_trading_values(row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        row["episode_id"],
        row["tenant_id"],
        row["interval_start"],
        row["step_index"],
        row["state_soc_before"],
        row["state_soc_after"],
        row["proposed_trade_side"],
        row["proposed_quantity_mw"],
        row["feasible_net_power_mw"],
        row["market_price_uah_mwh"],
        row["reward_uah"],
        row["gatekeeper_status"],
        row["paper_trade_provenance"],
        row.get("settlement_id"),
        row["live_mode_warning"],
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


def _latest_tenant_frame(frame: pl.DataFrame, *, tenant_id: str, limit: int) -> pl.DataFrame:
    if frame.height == 0:
        return pl.DataFrame()
    tenant_frame = frame.filter(pl.col("tenant_id") == tenant_id)
    if tenant_frame.height == 0:
        return pl.DataFrame()
    return tenant_frame.sort(["interval_start", "episode_id", "step_index"]).head(limit)


@cache
def get_simulated_trade_store() -> SimulatedTradeStore:
    dsn = os.environ.get("SMART_ARBITRAGE_SIMULATED_TRADE_DSN") or os.environ.get("SMART_ARBITRAGE_MARKET_DATA_DSN")
    if dsn is None:
        return NullSimulatedTradeStore()
    return PostgresSimulatedTradeStore(dsn)
