from smart_arbitrage.defs import defs


REQUIRED_TAXONOMY_TAGS = {
    "medallion",
    "domain",
    "elt_stage",
    "ml_stage",
    "evidence_scope",
}
ALLOWED_MEDALLIONS = {"bronze", "silver", "gold"}
ALLOWED_ELT_STAGES = {"extract_load", "transform", "publish"}
ALLOWED_ML_STAGES = {
    "source_data",
    "feature_engineering",
    "forecasting",
    "evaluation",
    "calibration",
    "selection",
    "diagnostics",
    "training_data",
    "pilot",
}
ALLOWED_EVIDENCE_SCOPES = {"demo", "thesis_grade", "research_only", "not_market_execution"}


def test_real_data_calibration_assets_use_readable_lineage_groups() -> None:
    groups_by_key = {
        asset_key.to_user_string(): group_name
        for asset in defs.assets or []
        for asset_key, group_name in asset.group_names_by_key.items()
    }

    assert groups_by_key["observed_market_price_history_bronze"] == "bronze_market_data"
    assert groups_by_key["tenant_historical_weather_bronze"] == "bronze_weather"
    assert groups_by_key["real_data_benchmark_silver_feature_frame"] == "silver_real_data_benchmark"
    assert groups_by_key["real_data_rolling_origin_benchmark_frame"] == "gold_real_data_benchmark"
    assert groups_by_key["regret_weighted_forecast_calibration_frame"] == "gold_calibration"
    assert groups_by_key["horizon_regret_weighted_forecast_calibration_frame"] == "gold_calibration"
    assert groups_by_key["calibrated_value_aware_ensemble_frame"] == "gold_selector_diagnostics"
    assert groups_by_key["forecast_dispatch_sensitivity_frame"] == "gold_selector_diagnostics"
    assert groups_by_key["risk_adjusted_value_gate_frame"] == "gold_selector_diagnostics"


def test_all_assets_use_medallion_prefixed_groups_and_standard_taxonomy_tags() -> None:
    for asset in defs.assets or []:
        for asset_key, group_name in asset.group_names_by_key.items():
            tags = asset.tags_by_key.get(asset_key, {})

            assert REQUIRED_TAXONOMY_TAGS.issubset(tags), asset_key.to_user_string()
            assert tags["medallion"] in ALLOWED_MEDALLIONS, asset_key.to_user_string()
            assert group_name.startswith(f"{tags['medallion']}_"), asset_key.to_user_string()
            assert tags["elt_stage"] in ALLOWED_ELT_STAGES, asset_key.to_user_string()
            assert tags["ml_stage"] in ALLOWED_ML_STAGES, asset_key.to_user_string()
            assert tags["evidence_scope"] in ALLOWED_EVIDENCE_SCOPES, asset_key.to_user_string()
