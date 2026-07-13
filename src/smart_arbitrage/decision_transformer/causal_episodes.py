"""Causal, time-ordered episode rows for a future offline DT study.

This contract deliberately separates point-in-time state from post-delivery
action/reward labels.  It is a corpus-preparation primitive, not permission to
train or promote a Decision Transformer.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Final

import polars as pl

CAUSAL_EPISODE_REQUIRED_COLUMNS: Final[frozenset[str]] = frozenset(
    {
        "tenant_id",
        "source_model_name",
        "anchor_timestamp",
        "starting_soc_fraction",
        "forecast_p10_uah_mwh",
        "forecast_p50_uah_mwh",
        "forecast_p90_uah_mwh",
        "price_lag_24_uah_mwh",
        "weather_temperature_c",
        "calendar_hour_sin",
        "calendar_hour_cos",
        "poland_lag24_uah_mwh",
        "actual_price_uah_mwh_vector",
        "teacher_dispatch_mw_vector",
        "teacher_soc_before_fraction_vector",
        "teacher_soc_after_fraction_vector",
        "teacher_degradation_penalty_uah_vector",
        "teacher_solver_status",
        "market_execution_enabled",
    }
)

_STATE_VECTOR_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("forecast_p10_uah_mwh", "state_forecast_p10_uah_mwh"),
    ("forecast_p50_uah_mwh", "state_forecast_p50_uah_mwh"),
    ("forecast_p90_uah_mwh", "state_forecast_p90_uah_mwh"),
    ("price_lag_24_uah_mwh", "state_price_lag_24_uah_mwh"),
    ("weather_temperature_c", "state_weather_temperature_c"),
    ("calendar_hour_sin", "state_calendar_hour_sin"),
    ("calendar_hour_cos", "state_calendar_hour_cos"),
    ("poland_lag24_uah_mwh", "state_poland_lag24_uah_mwh"),
)


def build_causal_temporal_episode_frame(episode_seed_frame: pl.DataFrame) -> pl.DataFrame:
    """Expand 24-hour forecast context into causal state/action/label transitions.

    Realized price is deliberately emitted only as ``label_actual_price_uah_mwh``.
    The returned corpus is still training-blocked until the independent V13
    source-family gate is passed.
    """

    missing = sorted(CAUSAL_EPISODE_REQUIRED_COLUMNS.difference(episode_seed_frame.columns))
    if missing:
        raise ValueError(f"episode_seed_frame is missing required columns: {missing}")
    if episode_seed_frame.filter(pl.col("market_execution_enabled") != False).height:  # noqa: E712
        raise ValueError("causal episode seeds require market_execution_enabled=false")

    rows: list[dict[str, Any]] = []
    for seed in episode_seed_frame.sort(["tenant_id", "anchor_timestamp"]).iter_rows(named=True):
        vectors = _vectors(seed)
        widths = {len(values) for values in vectors.values()}
        if len(widths) != 1:
            raise ValueError("causal episode seeds require equal vector lengths")
        width = widths.pop()
        if width == 0:
            raise ValueError("causal episode seeds require at least one horizon step")
        anchor = _datetime(seed["anchor_timestamp"], field_name="anchor_timestamp")
        episode_id = f"{seed['tenant_id']}|{seed['source_model_name']}|{anchor.isoformat()}"
        rewards = [
            vectors["actual_price_uah_mwh_vector"][index]
            * vectors["teacher_dispatch_mw_vector"][index]
            - vectors["teacher_degradation_penalty_uah_vector"][index]
            for index in range(width)
        ]
        returns_to_go = _returns_to_go(rewards)
        for step_index in range(width):
            row = {
                "episode_id": episode_id,
                "tenant_id": str(seed["tenant_id"]),
                "source_model_name": str(seed["source_model_name"]),
                "anchor_timestamp": anchor,
                "step_index": step_index,
                "interval_start": anchor + timedelta(hours=step_index + 1),
                "state_soc_before_fraction": vectors["teacher_soc_before_fraction_vector"][step_index],
                "state_soc_after_fraction_label": vectors["teacher_soc_after_fraction_vector"][step_index],
                "action_signed_dispatch_mw": vectors["teacher_dispatch_mw_vector"][step_index],
                "label_actual_price_uah_mwh": vectors["actual_price_uah_mwh_vector"][step_index],
                "label_reward_uah": rewards[step_index],
                "label_return_to_go_uah": returns_to_go[step_index],
                "label_degradation_penalty_uah": vectors[
                    "teacher_degradation_penalty_uah_vector"
                ][step_index],
                "teacher_solver_status": str(seed["teacher_solver_status"]),
                "dt_training_eligible": False,
                "market_execution_enabled": False,
                "claim_scope": "causal_temporal_episode_corpus_not_dt_training_or_market_execution",
            }
            for input_name, output_name in _STATE_VECTOR_COLUMNS:
                row[output_name] = vectors[input_name][step_index]
            rows.append(row)
    return pl.DataFrame(rows).sort(["tenant_id", "anchor_timestamp", "step_index"])


def _vectors(seed: dict[str, Any]) -> dict[str, list[float]]:
    names = [input_name for input_name, _ in _STATE_VECTOR_COLUMNS] + [
        "actual_price_uah_mwh_vector",
        "teacher_dispatch_mw_vector",
        "teacher_soc_before_fraction_vector",
        "teacher_soc_after_fraction_vector",
        "teacher_degradation_penalty_uah_vector",
    ]
    return {name: _float_vector(seed[name], field_name=name) for name in names}


def _float_vector(value: Any, *, field_name: str) -> list[float]:
    if not isinstance(value, (list, tuple)):
        raise ValueError(f"{field_name} must be a numeric vector")
    return [float(item) for item in value]


def _datetime(value: Any, *, field_name: str) -> datetime:
    if not isinstance(value, datetime):
        raise ValueError(f"{field_name} must be a datetime")
    return value


def _returns_to_go(rewards: list[float]) -> list[float]:
    total = 0.0
    values: list[float] = []
    for reward in reversed(rewards):
        total += reward
        values.append(total)
    values.reverse()
    return values
