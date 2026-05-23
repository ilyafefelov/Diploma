from __future__ import annotations

import polars as pl

from smart_arbitrage.dfl.ua_context_v13_acquisition import (
    build_dfl_ua_context_acquisition_source_evidence_v13_frame,
    build_dfl_ua_context_acquisition_readiness_v13_frame,
    build_dfl_ua_context_source_inventory_v13_frame,
)


TENANT = "client_003_dnipro_factory"
SOURCE = "nbeatsx_official_global_panel_horizon_calibrated_v1"


def test_v13_source_inventory_blocks_missing_targeted_ua_sources() -> None:
    inventory = build_dfl_ua_context_source_inventory_v13_frame(
        _v12_source_inventory(),
        _v12_readiness(prior_examples=7),
    )

    required = inventory.filter(pl.col("required_for_v13_candidate_generation"))

    assert {
        "measured_or_source_backed_tenant_load_pv",
        "explicit_dam_publication_receipts",
        "richer_grid_outage_archive",
        "extended_ukrainian_dam_weather_history",
        "v12_safe_teacher_label_support",
    }.issubset(set(required["source_family"].to_list()))
    assert "blocked_missing_source" in set(required["source_status"].to_list())
    assert "blocked_insufficient_safe_teacher_labels" in set(
        required["source_status"].to_list()
    )
    assert set(inventory["market_execution_enabled"].to_list()) == {False}
    assert set(inventory["no_eu_rows_as_ukrainian_targets"].to_list()) == {True}


def test_v13_source_evidence_accepts_proxy_load_and_grid_but_not_rule_only_dam() -> None:
    evidence = build_dfl_ua_context_acquisition_source_evidence_v13_frame(
        _dam_backfill(mode="market_rule_deadline"),
        _weather_load_backfill(status="context_ready"),
        _grid_backfill(status="context_ready"),
        _coverage_gate(ready=True),
    )

    rows_by_family = {
        str(row["source_family"]): row for row in evidence.iter_rows(named=True)
    }

    assert rows_by_family["measured_or_source_backed_tenant_load_pv"][
        "source_status"
    ] == "ready_prior_context"
    assert rows_by_family["richer_grid_outage_archive"]["source_status"] == (
        "ready_prior_context"
    )
    assert rows_by_family["explicit_dam_publication_receipts"]["source_status"] == (
        "partial_context_rule_deadline_without_row_receipts"
    )
    assert set(evidence["market_execution_enabled"].to_list()) == {False}


def test_v13_source_inventory_uses_acquired_source_evidence() -> None:
    evidence = build_dfl_ua_context_acquisition_source_evidence_v13_frame(
        _dam_backfill(mode="market_rule_deadline"),
        _weather_load_backfill(status="context_ready"),
        _grid_backfill(status="context_ready"),
        _coverage_gate(ready=True),
    )

    inventory = build_dfl_ua_context_source_inventory_v13_frame(
        _v12_source_inventory(),
        _v12_readiness(prior_examples=7),
        ua_context_acquisition_source_evidence_v13_frame=evidence,
    )

    rows_by_family = {
        str(row["source_family"]): row for row in inventory.iter_rows(named=True)
    }

    assert rows_by_family["open_meteo_archive"]["source_status"] == (
        "ready_prior_context"
    )
    assert rows_by_family["tenant_load_pv_proxy"]["source_status"] == (
        "ready_prior_context"
    )
    assert rows_by_family["richer_grid_outage_archive"]["source_status"] == (
        "ready_prior_context"
    )
    assert rows_by_family["explicit_dam_publication_receipts"]["source_status"] == (
        "partial_context_rule_deadline_without_row_receipts"
    )


def test_v13_readiness_requires_sources_and_safe_teacher_support() -> None:
    inventory = build_dfl_ua_context_source_inventory_v13_frame(
        _v12_source_inventory(),
        _v12_readiness(prior_examples=7),
    )

    readiness = build_dfl_ua_context_acquisition_readiness_v13_frame(
        _v12_readiness(prior_examples=7),
        inventory,
        min_prior_material_safe_switch_examples_for_dt=20,
    )

    assert readiness["v13_candidate_generation_ready"].to_list() == [False]
    assert readiness["readiness_decision"].to_list() == ["data_acquisition_needed"]
    assert "measured_or_source_backed_tenant_load_pv:blocked_missing_source" in readiness[
        "blocking_context_families"
    ].item()
    assert readiness["dt_lava_ready"].to_list() == [False]
    assert readiness["target_label_space"].to_list() == [
        "v13_precondition_context_coverage"
    ]
    assert readiness["raw_hourly_action_imitation"].to_list() == [False]
    assert readiness["market_execution_enabled"].to_list() == [False]


def test_v13_readiness_can_open_candidate_generation_when_context_is_ready() -> None:
    evidence = build_dfl_ua_context_acquisition_source_evidence_v13_frame(
        _dam_backfill(mode="explicit_source_metadata"),
        _weather_load_backfill(status="context_ready"),
        _grid_backfill(status="context_ready"),
        _coverage_gate(ready=True),
    )
    inventory = build_dfl_ua_context_source_inventory_v13_frame(
        _v12_source_inventory(all_ready=True),
        _v12_readiness(prior_examples=24),
        ua_context_acquisition_source_evidence_v13_frame=evidence,
    )

    readiness = build_dfl_ua_context_acquisition_readiness_v13_frame(
        _v12_readiness(prior_examples=24),
        inventory,
        min_prior_material_safe_switch_examples_for_dt=20,
    )

    assert readiness["v13_candidate_generation_ready"].to_list() == [True]
    assert readiness["readiness_decision"].to_list() == [
        "v13_candidate_generation_ready"
    ]
    assert readiness["blocking_context_families"].to_list() == ["none"]
    assert readiness["recommended_next_step"].to_list() == [
        "build_v13_lower_tail_risk_candidates"
    ]


def test_v13_readiness_ignores_final_holdout_label_mutation() -> None:
    base_v12 = _v12_readiness(prior_examples=24).with_columns(
        pl.lit(123.0).alias("label_final_holdout_regret_uah")
    )
    mutated_v12 = base_v12.with_columns(
        pl.lit(9999.0).alias("label_final_holdout_regret_uah")
    )
    inventory = build_dfl_ua_context_source_inventory_v13_frame(
        _v12_source_inventory(all_ready=True),
        base_v12,
    )

    base = build_dfl_ua_context_acquisition_readiness_v13_frame(base_v12, inventory)
    changed = build_dfl_ua_context_acquisition_readiness_v13_frame(
        mutated_v12,
        inventory,
    )

    assert base.select(_READINESS_STABLE_COLUMNS).equals(
        changed.select(_READINESS_STABLE_COLUMNS)
    )


def _v12_source_inventory(*, all_ready: bool = False) -> pl.DataFrame:
    current_coverage = 1.0 if all_ready else 0.593
    optional_coverage = 1.0 if all_ready else 0.0
    rows = [
        {
            "source_family": "oree_dam_history",
            "source_group": "current_ukrainian_source",
            "source_status": "context_ready" if all_ready else "partial_context",
            "coverage_ratio": current_coverage,
            "required_for_v12_candidate_generation": True,
            "optional_source_hook": False,
            "market_execution_enabled": False,
        },
        {
            "source_family": "open_meteo_archive",
            "source_group": "current_ukrainian_source",
            "source_status": "context_ready" if all_ready else "partial_context",
            "coverage_ratio": current_coverage,
            "required_for_v12_candidate_generation": True,
            "optional_source_hook": False,
            "market_execution_enabled": False,
        },
        {
            "source_family": "tenant_load_pv_proxy",
            "source_group": "current_ukrainian_source",
            "source_status": "context_ready" if all_ready else "partial_context",
            "coverage_ratio": current_coverage,
            "required_for_v12_candidate_generation": True,
            "optional_source_hook": False,
            "market_execution_enabled": False,
        },
        {
            "source_family": "ukrenergo_grid_event_archive",
            "source_group": "current_ukrainian_source",
            "source_status": "context_ready" if all_ready else "partial_context",
            "coverage_ratio": current_coverage,
            "required_for_v12_candidate_generation": True,
            "optional_source_hook": False,
            "market_execution_enabled": False,
        },
        {
            "source_family": "calendar_publication_rules",
            "source_group": "current_ukrainian_source",
            "source_status": "context_ready" if all_ready else "partial_context",
            "coverage_ratio": current_coverage,
            "required_for_v12_candidate_generation": True,
            "optional_source_hook": False,
            "market_execution_enabled": False,
        },
        {
            "source_family": "measured_or_source_backed_tenant_load_pv",
            "source_group": "optional_new_ukrainian_source",
            "source_status": "context_ready" if all_ready else "blocked_missing_source",
            "coverage_ratio": optional_coverage,
            "required_for_v12_candidate_generation": False,
            "optional_source_hook": True,
            "market_execution_enabled": False,
        },
        {
            "source_family": "explicit_dam_publication_receipts",
            "source_group": "optional_new_ukrainian_source",
            "source_status": "context_ready" if all_ready else "blocked_missing_source",
            "coverage_ratio": optional_coverage,
            "required_for_v12_candidate_generation": False,
            "optional_source_hook": True,
            "market_execution_enabled": False,
        },
        {
            "source_family": "richer_grid_outage_archive",
            "source_group": "optional_new_ukrainian_source",
            "source_status": "context_ready" if all_ready else "blocked_missing_source",
            "coverage_ratio": optional_coverage,
            "required_for_v12_candidate_generation": False,
            "optional_source_hook": True,
            "market_execution_enabled": False,
        },
    ]
    if all_ready:
        rows.append(
            {
                "source_family": "extended_ukrainian_dam_weather_history",
                "source_group": "targeted_ukrainian_acquisition",
                "source_status": "context_ready",
                "coverage_ratio": 1.0,
                "required_for_v12_candidate_generation": False,
                "optional_source_hook": True,
                "market_execution_enabled": False,
            }
        )
    return pl.DataFrame(rows)


def _v12_readiness(*, prior_examples: int) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "source_model_name": SOURCE,
                "prior_material_safe_switch_example_count": prior_examples,
                "min_prior_material_safe_switch_examples_for_dt": 20,
                "final_material_safe_switch_example_count": 0,
                "v12_generated_tail_risk_count": 0,
                "dt_lava_ready": prior_examples >= 20,
                "readiness_decision": "dt_lava_candidate_index_ready"
                if prior_examples >= 20
                else "blocked_insufficient_prior_safe_switch_examples",
                "target_label_space": "schedule_candidate_index",
                "raw_hourly_action_imitation": False,
                "market_execution_enabled": False,
            }
        ]
    )


_READINESS_STABLE_COLUMNS = [
    "tenant_id",
    "source_model_name",
    "v13_candidate_generation_ready",
    "readiness_decision",
    "blocking_context_families",
    "recommended_next_step",
]


def _dam_backfill(*, mode: str) -> pl.DataFrame:
    status = "context_ready" if mode != "missing" else "missing_publication_time"
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "source_model_name": SOURCE,
                "anchor_timestamp": "2026-04-29T23:00:00",
                "split_name": "train",
                "anchor_key": f"{TENANT}|{SOURCE}|2026-04-29T23:00:00",
                "prior_available": status == "context_ready",
                "dam_publication_backfill_status": status,
                "publication_evidence_mode": mode,
                "source_publication_timestamp": "2026-04-28T14:00:00",
                "market_execution_enabled": False,
            }
        ]
    )


def _weather_load_backfill(*, status: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "source_model_name": SOURCE,
                "anchor_timestamp": "2026-04-29T23:00:00",
                "split_name": "train",
                "anchor_key": f"{TENANT}|{SOURCE}|2026-04-29T23:00:00",
                "prior_available": status == "context_ready",
                "weather_load_pv_backfill_status": status,
                "selector_feature_net_load_mw": 0.72,
                "selector_feature_pv_estimate_mw": 0.11,
                "context_source": "open_meteo_archive_plus_tenant_load_proxy",
                "market_execution_enabled": False,
            }
        ]
    )


def _grid_backfill(*, status: str) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "source_model_name": SOURCE,
                "anchor_timestamp": "2026-04-29T23:00:00",
                "split_name": "train",
                "anchor_key": f"{TENANT}|{SOURCE}|2026-04-29T23:00:00",
                "prior_available": status == "context_ready",
                "grid_event_backfill_status": status,
                "source_coverage_start_timestamp": "2025-01-01T00:00:00",
                "source_coverage_end_timestamp": "2026-04-30T23:00:00",
                "context_source": "ukrenergo_grid_event_signal_history",
                "market_execution_enabled": False,
            }
        ]
    )


def _coverage_gate(*, ready: bool) -> pl.DataFrame:
    return pl.DataFrame(
        [
            {
                "tenant_id": TENANT,
                "source_model_name": SOURCE,
                "anchor_timestamp": "2026-04-29T23:00:00",
                "split_name": "train",
                "anchor_key": f"{TENANT}|{SOURCE}|2026-04-29T23:00:00",
                "v11_candidate_generation_ready": ready,
                "context_backfill_gate_decision": (
                    "context_backfill_ready" if ready else "data_acquisition_needed"
                ),
                "market_execution_enabled": False,
            }
        ]
    )
