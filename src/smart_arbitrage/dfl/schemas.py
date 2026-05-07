"""Strict internal contracts for DFL research examples."""

from __future__ import annotations

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, model_validator


class DFLTrainingExampleV2(BaseModel):
    """One research-only decision instance for future DFL experiments."""

    model_config = ConfigDict(extra="forbid", strict=True)

    training_example_id: str
    evaluation_id: str
    baseline_evaluation_id: str
    tenant_id: str
    anchor_timestamp: datetime
    horizon_start: datetime
    horizon_end: datetime
    horizon_hours: int
    market_venue: Literal["DAM"]
    currency: Literal["UAH"]
    forecast_model_name: str
    strategy_kind: str
    baseline_strategy_name: Literal["strict_similar_day"]
    baseline_forecast_model_name: Literal["strict_similar_day"]
    forecast_price_vector_uah_mwh: list[float]
    actual_price_vector_uah_mwh: list[float]
    candidate_dispatch_vector_mw: list[float]
    baseline_dispatch_vector_mw: list[float]
    candidate_degradation_penalty_vector_uah: list[float]
    baseline_degradation_penalty_vector_uah: list[float]
    candidate_net_value_uah: float
    baseline_net_value_uah: float
    oracle_net_value_uah: float
    candidate_regret_uah: float
    baseline_regret_uah: float
    regret_delta_vs_baseline_uah: float
    total_throughput_mwh: float
    total_degradation_penalty_uah: float
    candidate_feasible: bool
    baseline_feasible: bool
    safety_violation_count: int
    data_quality_tier: Literal["thesis_grade"]
    observed_coverage_ratio: float
    claim_scope: Literal["dfl_training_examples_not_full_dfl"]
    not_full_dfl: bool
    not_market_execution: bool
    generated_at: datetime

    @model_validator(mode="after")
    def validate_vectors_and_claim_flags(self) -> DFLTrainingExampleV2:
        vector_lengths = {
            len(self.forecast_price_vector_uah_mwh),
            len(self.actual_price_vector_uah_mwh),
            len(self.candidate_dispatch_vector_mw),
            len(self.baseline_dispatch_vector_mw),
            len(self.candidate_degradation_penalty_vector_uah),
            len(self.baseline_degradation_penalty_vector_uah),
        }
        if vector_lengths != {self.horizon_hours}:
            raise ValueError("all DFL vector lengths must match horizon_hours")
        if self.horizon_hours <= 0:
            raise ValueError("horizon_hours must be positive")
        if self.horizon_end < self.horizon_start:
            raise ValueError("horizon_end must not be earlier than horizon_start")
        if self.observed_coverage_ratio < 1.0:
            raise ValueError("DFL training examples require observed coverage ratio of 1.0")
        if not self.not_full_dfl or not self.not_market_execution:
            raise ValueError("DFL training examples must remain not_full_dfl and not_market_execution")
        return self
