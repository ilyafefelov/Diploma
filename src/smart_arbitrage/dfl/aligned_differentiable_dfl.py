"""Input contract for the preregistered aligned differentiable DFL experiment.

The experiment compares the same contextual transformer under forecast loss and
hybrid forecast-plus-decision loss. This module intentionally accepts only a
fully source-backed, point-in-time feature panel; it must refuse the older
candidate library, whose outcome fields are useful labels but not sufficient
full-history context.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

import numpy as np
import polars as pl

ALIGNED_DFL_FEATURE_COLUMNS: Final[tuple[str, ...]] = (
    "forecast_p50_uah_mwh",
    "price_lag_24_uah_mwh",
    "weather_temperature_c",
    "calendar_hour_sin",
    "calendar_hour_cos",
    "forecast_p10_uah_mwh",
    "forecast_p90_uah_mwh",
    "poland_lag24_uah_mwh",
)
_IDENTITY_COLUMNS: Final[frozenset[str]] = frozenset(
    {"tenant_id", "source_model_name", "anchor_timestamp", "starting_soc_fraction"}
)
_LABEL_COLUMNS: Final[frozenset[str]] = frozenset(
    {"actual_price_uah_mwh_vector"}
)


@dataclass(frozen=True, slots=True)
class AlignedDflContextTensor:
    """Prior-safe context tensor with labels deliberately kept separate."""

    features: np.ndarray
    feature_names: tuple[str, ...]


def build_aligned_dfl_context_tensor(frame: pl.DataFrame) -> AlignedDflContextTensor:
    """Validate and convert a complete point-in-time DFL context panel."""

    required = _IDENTITY_COLUMNS | _LABEL_COLUMNS | set(ALIGNED_DFL_FEATURE_COLUMNS)
    missing = required.difference(frame.columns)
    if missing:
        raise ValueError(
            "aligned DFL panel is missing required prior-safe columns: "
            f"{sorted(missing)}"
        )
    if frame.is_empty():
        raise ValueError("aligned DFL panel must contain at least one row.")
    vectors: list[np.ndarray] = []
    horizon: int | None = None
    for row in frame.select(ALIGNED_DFL_FEATURE_COLUMNS).iter_rows(named=True):
        columns: list[np.ndarray] = []
        for feature_name in ALIGNED_DFL_FEATURE_COLUMNS:
            value = row[feature_name]
            if not isinstance(value, list) or not value:
                raise ValueError(
                    f"aligned DFL feature {feature_name!r} must be a non-empty list."
                )
            column = np.asarray(value, dtype=np.float64)
            if not np.isfinite(column).all():
                raise ValueError(
                    f"aligned DFL feature {feature_name!r} must contain finite values."
                )
            columns.append(column)
        row_horizon = len(columns[0])
        if any(len(column) != row_horizon for column in columns):
            raise ValueError("aligned DFL feature vectors must share one horizon per row.")
        if horizon is None:
            horizon = row_horizon
        elif horizon != row_horizon:
            raise ValueError("aligned DFL panel must use one shared forecast horizon.")
        vectors.append(np.stack(columns, axis=-1))
    return AlignedDflContextTensor(
        features=np.stack(vectors, axis=0),
        feature_names=ALIGNED_DFL_FEATURE_COLUMNS,
    )
