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


class DFLActionLabelV1(BaseModel):
    """One strict LP/oracle action-label row for future DFL experiments."""

    model_config = ConfigDict(extra="forbid", strict=True)

    action_label_id: str
    evaluation_id: str
    strict_baseline_evaluation_id: str
    tenant_id: str
    anchor_timestamp: datetime
    split_name: Literal["train_selection", "final_holdout"]
    is_final_holdout: bool
    horizon_start: datetime
    horizon_end: datetime
    horizon_hours: int
    market_venue: Literal["DAM"]
    currency: Literal["UAH"]
    forecast_model_name: str
    source_strategy_kind: str
    strict_baseline_forecast_model_name: Literal["strict_similar_day"]
    target_strategy_name: Literal["oracle_lp"]
    forecast_price_vector_uah_mwh: list[float]
    actual_price_vector_uah_mwh: list[float]
    candidate_signed_dispatch_vector_mw: list[float]
    strict_baseline_signed_dispatch_vector_mw: list[float]
    oracle_signed_dispatch_vector_mw: list[float]
    oracle_charge_mw_vector: list[float]
    oracle_discharge_mw_vector: list[float]
    oracle_soc_before_mwh_vector: list[float]
    oracle_soc_after_mwh_vector: list[float]
    oracle_degradation_penalty_vector_uah: list[float]
    target_charge_mask: list[int]
    target_discharge_mask: list[int]
    target_hold_mask: list[int]
    candidate_net_value_uah: float
    strict_baseline_net_value_uah: float
    oracle_net_value_uah: float
    candidate_regret_uah: float
    strict_baseline_regret_uah: float
    regret_delta_vs_strict_baseline_uah: float
    candidate_total_throughput_mwh: float
    strict_baseline_total_throughput_mwh: float
    candidate_total_degradation_penalty_uah: float
    strict_baseline_total_degradation_penalty_uah: float
    candidate_safety_violation_count: int
    strict_baseline_safety_violation_count: int
    data_quality_tier: Literal["thesis_grade"]
    observed_coverage_ratio: float
    claim_scope: Literal["dfl_action_label_panel_not_full_dfl"]
    not_full_dfl: bool
    not_market_execution: bool
    generated_at: datetime

    @model_validator(mode="after")
    def validate_vectors_and_claim_flags(self) -> "DFLActionLabelV1":
        vector_lengths = {
            len(self.forecast_price_vector_uah_mwh),
            len(self.actual_price_vector_uah_mwh),
            len(self.candidate_signed_dispatch_vector_mw),
            len(self.strict_baseline_signed_dispatch_vector_mw),
            len(self.oracle_signed_dispatch_vector_mw),
            len(self.oracle_charge_mw_vector),
            len(self.oracle_discharge_mw_vector),
            len(self.oracle_soc_before_mwh_vector),
            len(self.oracle_soc_after_mwh_vector),
            len(self.oracle_degradation_penalty_vector_uah),
            len(self.target_charge_mask),
            len(self.target_discharge_mask),
            len(self.target_hold_mask),
        }
        if vector_lengths != {self.horizon_hours}:
            raise ValueError("all DFL action-label vector lengths must match horizon_hours")
        if self.horizon_hours <= 0:
            raise ValueError("horizon_hours must be positive")
        if self.horizon_end < self.horizon_start:
            raise ValueError("horizon_end must not be earlier than horizon_start")
        if self.split_name == "final_holdout" and not self.is_final_holdout:
            raise ValueError("final_holdout rows must set is_final_holdout=true")
        if self.split_name == "train_selection" and self.is_final_holdout:
            raise ValueError("train_selection rows must set is_final_holdout=false")
        if self.observed_coverage_ratio < 1.0:
            raise ValueError("DFL action labels require observed coverage ratio of 1.0")
        if not self.not_full_dfl or not self.not_market_execution:
            raise ValueError("DFL action labels must remain not_full_dfl and not_market_execution")
        for charge, discharge, hold in zip(
            self.target_charge_mask,
            self.target_discharge_mask,
            self.target_hold_mask,
            strict=True,
        ):
            if charge + discharge + hold != 1:
                raise ValueError("target action masks must be one-hot per horizon step")
        return self
