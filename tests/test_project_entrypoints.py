from __future__ import annotations

import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_package_imports_without_test_pythonpath() -> None:
    result = subprocess.run(
        [
            "uv",
            "run",
            "python",
            "-c",
            "import smart_arbitrage; print(smart_arbitrage.__file__)",
        ],
        cwd=PROJECT_ROOT,
        env=_environment_without_pythonpath(),
        capture_output=True,
        text=True,
        timeout=60,
    )

    assert result.returncode == 0, result.stderr
    assert "src" in result.stdout


def test_dg_list_defs_loads_without_manual_pythonpath() -> None:
    result = subprocess.run(
        ["uv", "run", "dg", "list", "defs", "--json"],
        cwd=PROJECT_ROOT,
        env=_environment_without_pythonpath(),
        capture_output=True,
        text=True,
        timeout=90,
    )

    assert result.returncode == 0, result.stderr
    assert "real_data_value_aware_ensemble_frame" in result.stdout
    assert "dfl_training_frame" in result.stdout
    assert "regret_weighted_dfl_pilot_frame" in result.stdout


def test_verify_wrapper_uses_project_mypy_file_set() -> None:
    verify_script = (PROJECT_ROOT / "scripts" / "verify.ps1").read_text(
        encoding="utf-8"
    )

    assert (
        'Invoke-OptionalPythonTool -DisplayName "Mypy" -ModuleName "mypy" '
        '-Arguments @("--config-file", "pyproject.toml")'
    ) in verify_script


def test_official_batch_runner_refreshes_exit_code_after_wait_process() -> None:
    runner_script = (
        PROJECT_ROOT / "scripts" / "run-official-schedule-value-batches.ps1"
    ).read_text(encoding="utf-8")

    assert "$process.WaitForExit()" in runner_script
    assert "$exitCode = $process.ExitCode" in runner_script
    assert (
        'Select-String -LiteralPath $stderrPath -Pattern "RUN_SUCCESS" -Quiet'
        in runner_script
    )
    assert "if ($exitCode -ne 0)" in runner_script
    assert '[ValidateSet("chronological", "latest_first")]' in runner_script
    assert "attempt_manifest.json" in runner_script
    assert "enabled_official_model_names_csv" in runner_script
    assert "nbeatsx_max_steps: $NbeatsxMaxSteps" in runner_script
    assert "tft_max_epochs: $TftMaxEpochs" in runner_script


def test_official_global_panel_batch_runner_writes_resumable_backfill_config() -> None:
    runner_script = (
        PROJECT_ROOT / "scripts" / "run-official-global-panel-batches.ps1"
    ).read_text(encoding="utf-8")

    assert (
        "real_data_official_global_panel_nbeatsx_backfill_week3.yaml" in runner_script
    )
    assert "attempt_manifest.json" in runner_script
    assert "[int]$EndAnchorIndex = 0" in runner_script
    assert "$ResolvedEndAnchorIndex = $TotalAnchors" in runner_script
    assert (
        "nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame"
        in runner_script
    )
    assert "max_eval_windows: $TotalAnchors" in runner_script
    assert "anchor_batch_start_index: $anchorIndex" in runner_script
    assert "anchor_batch_size: $BatchSize" in runner_script
    assert 'resume_generated_at_iso: "$GeneratedAtIso"' in runner_script
    assert "merge_persisted_batches: true" in runner_script
    assert (
        "nbeatsx_official_global_panel_rolling_horizon_calibration_frame"
        in runner_script
    )
    assert (
        "dfl_official_global_panel_schedule_value_production_gate_frame"
        in runner_script
    )


def test_tft_quantile_gate_batch_runner_writes_calibrated_schedule_config() -> None:
    runner_script = (
        PROJECT_ROOT / "scripts" / "run-tft-quantile-gate-batches.ps1"
    ).read_text(encoding="utf-8")

    assert "[ValidateSet(\"compose\", \"host\")]" in runner_script
    assert "attempt_manifest.json" in runner_script
    assert '"--attempt-kind", "official_global_panel_backfill"' in runner_script
    assert "$env:SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN = $HostPostgresDsn" in runner_script
    assert "[switch]$ReuseMaterializedInputs" in runner_script
    assert "[string]$DagsterHome" in runner_script
    assert "$env:DAGSTER_HOME = $DagsterHome" in runner_script
    assert 'if ($ReuseMaterializedInputs)' in runner_script
    assert '$officialSelection = "tft_official_global_panel_rolling_strict_lp_benchmark_frame"' in runner_script
    assert "tft_official_global_panel_rolling_strict_lp_benchmark_frame" in runner_script
    assert (
        "tft_official_global_panel_horizon_quantile_calibrated_strict_lp_benchmark_frame"
        in runner_script
    )
    assert (
        "dfl_tft_calibrated_combined_v2_plus_strict_lp_benchmark_frame"
        in runner_script
    )
    assert "max_eval_windows: $TotalAnchors" in runner_script
    assert "anchor_batch_start_index: $anchorIndex" in runner_script
    assert "anchor_batch_size: $BatchSize" in runner_script
    assert 'resume_generated_at_iso: "$GeneratedAtIso"' in runner_script
    assert "merge_persisted_batches: true" in runner_script
    assert "tft_max_epochs: $TftMaxEpochs" in runner_script
    assert "tft_max_steps: $TftMaxSteps" in runner_script
    assert (
        "$calibratedTftModels = "
        '"tft_official_global_panel_p10_v1_horizon_quantile_calibrated_v1,'
        "tft_official_global_panel_v1_horizon_quantile_calibrated_v1,"
        'tft_official_global_panel_p90_v1_horizon_quantile_calibrated_v1"'
    ) in runner_script
    assert 'forecast_model_names_csv: "$calibratedTftModels"' in runner_script


def test_poland_lag24_calibrated_batch_runner_writes_resumable_veto_config() -> None:
    runner_script = (
        PROJECT_ROOT / "scripts" / "run-poland-lag24-calibrated-batches.ps1"
    ).read_text(encoding="utf-8")

    assert "attempt_manifest.json" in runner_script
    assert '"--attempt-kind", "official_global_panel_backfill"' in runner_script
    assert "official_global_panel_poland_lag24_experimental_rolling_strict_lp_benchmark_frame" in runner_script
    assert "max_eval_windows: $TotalAnchors" in runner_script
    assert "anchor_batch_start_index: $anchorIndex" in runner_script
    assert "anchor_batch_size: $BatchSize" in runner_script
    assert 'resume_generated_at_iso: "$GeneratedAtIso"' in runner_script
    assert "merge_persisted_batches: true" in runner_script
    assert "official_global_panel_poland_lag24_experimental_horizon_calibrated_strict_lp_benchmark_frame" in runner_script
    assert "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame" in runner_script
    assert "dfl_poland_lag24_prior_tail_risk_veto_frame" in runner_script
    assert "market_execution_enabled: true" not in runner_script.lower()


def test_official_attempt_resume_summary_script_uses_manifest_contract() -> None:
    resume_script = (
        PROJECT_ROOT / "scripts" / "summarize_official_evidence_attempt_resume.py"
    ).read_text(encoding="utf-8")

    assert "summarize_official_evidence_attempt_resume" in resume_script
    assert "--manifest" in resume_script
    assert "--persisted-anchor-counts-csv" in resume_script
    assert "--persisted-anchor-count" in resume_script
    assert "--strategy-kind" in resume_script
    assert "anchor_counts_by_model_for_generated_at" in resume_script


def test_official_evidence_monitor_wrapper_delegates_to_resume_summary() -> None:
    monitor_script = (
        PROJECT_ROOT / "scripts" / "monitor-official-evidence-attempt.ps1"
    ).read_text(encoding="utf-8")

    assert "[string]$ManifestPath" in monitor_script
    assert "[string]$StrategyKind" in monitor_script
    assert '[string]$GeneratedAtIso = ""' in monitor_script
    assert '[string]$OutputPath = ""' in monitor_script
    assert "ManifestPath is required" in monitor_script
    assert "StrategyKind is required" in monitor_script
    assert "summarize_official_evidence_attempt_resume.py" in monitor_script
    assert "--manifest" in monitor_script
    assert "--strategy-kind" in monitor_script
    assert "--generated-at-iso" in monitor_script
    assert "--output" in monitor_script
    assert "Get-Content -LiteralPath $resolvedOutputPath" in monitor_script


def test_schedule_value_registry_export_cli_accepts_evidence_attachments() -> None:
    export_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_schedule_value_production_gate_registry.py"
    ).read_text(encoding="utf-8")

    assert "--attempt-manifest" in export_script
    assert "--monitor-snapshot" in export_script
    assert "--learner-frame-pickle" in export_script
    assert "attempt_manifest_path=args.attempt_manifest" in export_script
    assert "monitor_snapshot_path=args.monitor_snapshot" in export_script
    assert "learner_trace_frame=learner_frame" in export_script


def test_schedule_value_v2_plus_comparison_export_cli_requires_gate_inputs() -> None:
    export_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_schedule_value_v2_plus_comparison.py"
    ).read_text(encoding="utf-8")

    assert "--strict-frame-pickle" in export_script
    assert "--learner-frame-pickle" in export_script
    assert "--regret-decomposition-pickle" in export_script
    assert "--rolling-robustness-pickle" in export_script
    assert "--dagster-run-id" in export_script
    assert "build_dfl_schedule_value_learner_v2_plus_comparison_packet" in export_script
    assert "write_dfl_schedule_value_learner_v2_plus_comparison_packet" in export_script


def test_market_coupling_ablation_export_cli_requires_evidence_input() -> None:
    export_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_market_coupling_ablation_packet.py"
    ).read_text(encoding="utf-8")

    assert "--ablation-frame-pickle" in export_script
    assert "--run-slug" in export_script
    assert "--dagster-run-id" in export_script
    assert "build_dfl_market_coupling_v2_plus_ablation_packet" in export_script
    assert "write_dfl_market_coupling_v2_plus_ablation_packet" in export_script


def test_v2_plus_dfl_dt_bridge_packet_cli_exports_negative_evidence() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_v2_plus_dfl_dt_bridge_packet.py"
    ).read_text(encoding="utf-8")

    assert "--bridge-frame-pickle" in export_script
    assert "--run-slug" in export_script
    assert "--dagster-run-id" in export_script
    assert "--asset-check-status" in export_script
    assert "build_dfl_v2_plus_dfl_dt_bridge_packet" in export_script
    assert "write_dfl_v2_plus_dfl_dt_bridge_packet" in export_script
    assert "negative_evidence" in export_script


def test_v10_tail_risk_transfer_closure_packet_cli_exports_negative_evidence() -> None:
    export_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_v10_tail_risk_transfer_closure_packet.py"
    ).read_text(encoding="utf-8")

    assert "--tail-risk-audit-pickle" in export_script
    assert "--learning-ceiling-pickle" in export_script
    assert "--run-slug" in export_script
    assert "--dagster-run-id" in export_script
    assert "--asset-check-status" in export_script
    assert "build_dfl_v10_tail_risk_transfer_closure_packet" in export_script
    assert "write_dfl_v10_tail_risk_transfer_closure_packet" in export_script
    assert "negative_evidence" in export_script


def test_ua_context_backfill_readiness_packet_cli_exports_v11_gate() -> None:
    export_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_ua_context_backfill_readiness_packet.py"
    ).read_text(encoding="utf-8")
    config = (
        PROJECT_ROOT
        / "configs"
        / "real_data_dfl_ua_context_acquisition_v11_precondition_week3.yaml"
    ).read_text(encoding="utf-8")
    doc = (
        PROJECT_ROOT / "docs" / "technical" / "DFL_UA_CONTEXT_ACQUISITION_V1.md"
    ).read_text(encoding="utf-8")

    assert "--source-inventory-pickle" in export_script
    assert "--coverage-gate-pickle" in export_script
    assert "--run-slug" in export_script
    assert "build_dfl_ua_context_backfill_readiness_packet" in export_script
    assert "write_dfl_ua_context_backfill_readiness_packet" in export_script
    assert "v11_candidate_generation_ready" in export_script
    assert "dfl_ua_context_source_inventory_frame" in config
    assert "dfl_ua_context_backfill_coverage_gate_frame" in config
    assert "Offline Strategy Promotion" in doc


def test_ua_v12_safe_teacher_packet_cli_exports_dt_lava_gate() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_ua_v12_safe_teacher_packet.py"
    ).read_text(encoding="utf-8")
    config = (
        PROJECT_ROOT
        / "configs"
        / "real_data_dfl_ua_context_v12_backfill_week3.yaml"
    ).read_text(encoding="utf-8")
    doc = (
        PROJECT_ROOT
        / "docs"
        / "technical"
        / "DFL_UA_CONTEXT_V12_SAFE_TEACHER_BACKFILL.md"
    ).read_text(encoding="utf-8")

    assert "--source-inventory-pickle" in export_script
    assert "--readiness-decision-pickle" in export_script
    assert "--run-slug" in export_script
    assert "build_dfl_ua_v12_safe_teacher_backfill_packet" in export_script
    assert "write_dfl_ua_v12_safe_teacher_backfill_packet" in export_script
    assert "dt_lava_ready" in export_script
    assert "dfl_ua_context_source_expansion_inventory_v12_frame" in config
    assert "dfl_ua_v12_dt_lava_readiness_decision_frame" in config
    assert "Offline Strategy Promotion" in doc


def test_ua_context_v13_acquisition_packet_cli_exports_candidate_gate() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_ua_context_v13_acquisition_packet.py"
    ).read_text(encoding="utf-8")
    config = (
        PROJECT_ROOT
        / "configs"
        / "real_data_dfl_ua_context_v13_acquisition_week3.yaml"
    ).read_text(encoding="utf-8")
    docs = (
        PROJECT_ROOT / "docs" / "technical" / "DFL_UA_CONTEXT_ACQUISITION_V13.md"
    ).read_text(encoding="utf-8")

    assert "build_dfl_ua_context_v13_acquisition_packet" in export_script
    assert "write_dfl_ua_context_v13_acquisition_packet" in export_script
    assert "--source-evidence-pickle" in export_script
    assert "v13_candidate_generation_ready" in export_script
    assert "dfl_ua_context_acquisition_source_evidence_v13_frame" in config
    assert "dfl_ua_context_source_inventory_v13_frame" in config
    assert "dfl_ua_context_acquisition_readiness_v13_frame" in config
    assert "dfl_ua_context_acquisition_source_evidence_v13_frame" in docs
    assert "data_acquisition_needed" in docs
    assert "market_execution_enabled=false" in docs
    assert "Offline Strategy Promotion" in docs


def test_poland_lag24_experimental_schedule_value_packet_cli_exports_near_miss() -> None:
    export_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_poland_lag24_experimental_schedule_value_packet.py"
    ).read_text(encoding="utf-8")

    assert "--comparison-frame-pickle" in export_script
    assert "--raw-strict-frame-pickle" in export_script
    assert "--run-slug" in export_script
    assert "--dagster-run-id" in export_script
    assert "build_poland_lag24_experimental_schedule_value_packet" in export_script
    assert "write_poland_lag24_experimental_schedule_value_packet" in export_script
    assert "promotes_over_frozen_v2_plus" in export_script


def test_poland_lag24_tail_risk_audit_cli_exports_near_miss_autopsy() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_poland_lag24_tail_risk_packet.py"
    ).read_text(encoding="utf-8")

    assert "--baseline-strict-rows-csv" in export_script
    assert "--challenger-strict-rows-csv" in export_script
    assert "--generated-at-iso" in export_script
    assert "--challenger-model-name" in export_script
    assert "build_poland_lag24_tail_risk_audit_frame" in export_script
    assert "write_poland_lag24_tail_risk_packet" in export_script
    assert "oracle_loss_avoidance_is_diagnostic_only" in export_script


def test_poland_lag24_prior_veto_cli_exports_prior_only_selector() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_poland_lag24_prior_veto_packet.py"
    ).read_text(encoding="utf-8")

    assert "--tail-risk-audit-csv" in export_script
    assert "--ridge-alpha" in export_script
    assert "--min-prior-rows" in export_script
    assert "--promotion-min-improvement-ratio" in export_script
    assert "build_poland_lag24_prior_veto_frame" in export_script
    assert "write_poland_lag24_prior_veto_packet" in export_script
    assert "selector_is_prior_only" in export_script


def test_poland_lag24_calibrated_schedule_value_config_routes_new_model_names() -> None:
    run_config = (
        PROJECT_ROOT
        / "configs"
        / "real_data_official_global_panel_poland_lag24_calibrated_schedule_value_week3.yaml"
    ).read_text(encoding="utf-8")

    assert "official_global_panel_poland_lag24_experimental_nbeatsx_horizon_calibration_frame" in run_config
    assert "official_global_panel_poland_lag24_experimental_tft_horizon_quantile_calibration_frame" in run_config
    assert "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_strict_lp_benchmark_frame" in run_config
    assert "dfl_poland_lag24_calibrated_schedule_value_learner_v2_plus_robustness_frame" in run_config
    assert "dfl_poland_lag24_rolling_vs_frozen_v2_plus_gate_frame" in run_config
    assert "dfl_poland_lag24_candidate_value_ranker_frame" in run_config
    assert "nbeatsx_official_global_panel_poland_lag24_horizon_calibrated_v1" in run_config
    assert "tft_official_global_panel_poland_lag24_horizon_quantile_calibrated_v1" in run_config
    assert "market_execution_enabled: true" not in run_config.lower()


def test_tft_quantile_screen_packet_cli_exports_blocked_screen_evidence() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_tft_quantile_screen_packet.py"
    ).read_text(encoding="utf-8")

    assert "--raw-strict-frame-pickle" in export_script
    assert "--candidate-library-pickle" in export_script
    assert "--augmented-gate-frame-pickle" in export_script
    assert "--asset-check-status" in export_script
    assert "--tft-source-models-csv" in export_script
    assert "build_dfl_tft_quantile_screen_packet" in export_script
    assert "write_dfl_tft_quantile_screen_packet" in export_script
    assert "gate_blockers" in export_script


def test_entsoe_poland_governance_ablation_runner_exports_packet() -> None:
    runner_script = (
        PROJECT_ROOT / "scripts" / "run-entsoe-poland-governance-ablation.ps1"
    ).read_text(encoding="utf-8")

    assert "real_data_dfl_entsoe_poland_feature_ablation_week3.yaml" in runner_script
    assert "ENTSOE_TOKEN" in runner_script
    assert "ENTSOE_SECURITY_TOKEN" in runner_script
    assert "ENTSO_E_SECURITY_TOKEN" in runner_script
    assert "entsoe_token" in runner_script
    assert '("-e", "ENTSOE_TOKEN")' in runner_script
    assert "GetRelativePath" in runner_script
    assert "poland_neighbor_market_snapshot_bronze" in runner_script
    assert "poland_neighbor_market_snapshot_feature_candidate_frame" in runner_script
    assert "nbu_eur_uah_fx_metadata_frame" in runner_script
    assert "entsoe_poland_lagged_feature_candidate_frame" in runner_script
    assert "entsoe_poland_feature_governance_frame" in runner_script
    assert "official_forecast_exogenous_feature_route_frame" in runner_script
    assert "dfl_market_coupling_v2_plus_ablation_frame" in runner_script
    assert "materialize_market_coupling_ablation_packet.py" in runner_script
    assert "docker cp" in runner_script
    assert "market_execution_enabled = $false" in runner_script
    assert "[switch]$DryRun" in runner_script


def test_poland_neighbor_market_snapshot_packet_cli_exists() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_poland_neighbor_market_snapshot_packet.py"
    ).read_text(encoding="utf-8")

    assert "--snapshot-frame-pickle" in export_script
    assert "--feature-candidate-frame-pickle" in export_script
    assert "build_poland_neighbor_market_snapshot_packet" in export_script
    assert "write_poland_neighbor_market_snapshot_packet" in export_script


def test_entsoe_poland_governance_closure_packet_cli_exists() -> None:
    export_script = (
        PROJECT_ROOT / "scripts" / "materialize_entsoe_poland_governance_closure_packet.py"
    ).read_text(encoding="utf-8")

    assert "--snapshot-frame-pickle" in export_script
    assert "--hourly-feature-frame-pickle" in export_script
    assert "--governance-closure-frame-pickle" in export_script
    assert "--dagster-run-id" in export_script
    assert "--materialization-command" in export_script
    assert "build_entsoe_poland_governance_closure_packet" in export_script
    assert "write_entsoe_poland_governance_closure_packet" in export_script


def test_entsoe_file_library_energy_prices_fetch_cli_is_secret_safe() -> None:
    fetch_script = (
        PROJECT_ROOT / "scripts" / "fetch_entsoe_file_library_energy_prices.py"
    ).read_text(encoding="utf-8")

    assert "load_entsoe_file_library_credentials" in fetch_script
    assert "request_entsoe_fms_token" in fetch_script
    assert "--env-file" in fetch_script
    assert "--token-only" in fetch_script
    assert "--list-only" in fetch_script
    assert "--config-output" in fetch_script
    assert "safe_entsoe_fms_smoke_receipt" in fetch_script
    assert "access_token" not in fetch_script
    assert "entsoe_password" not in fetch_script


def test_hf_official_job_submission_cli_is_guarded_by_submit_flag() -> None:
    submit_script = (
        PROJECT_ROOT / "scripts" / "submit_hf_official_schedule_value_job.py"
    ).read_text(encoding="utf-8")

    assert "--payload" in submit_script
    assert "--output" in submit_script
    assert "--submit" in submit_script
    assert "submit=args.submit" in submit_script
    assert "default is a dry-run receipt" in submit_script


def test_unified_official_evidence_runner_switches_local_and_hf_backends() -> None:
    runner_script = (PROJECT_ROOT / "scripts" / "run-official-evidence.ps1").read_text(
        encoding="utf-8"
    )

    assert '[ValidateSet("local", "hf")]' in runner_script
    assert '[ValidateSet("compose", "host")]' in runner_script
    assert '[string]$LocalMode = "compose"' in runner_script
    assert "[string]$HostPostgresDsn" in runner_script
    assert "SMART_ARBITRAGE_STRATEGY_EVALUATION_DSN" in runner_script
    assert "postgresql://smart:arbitrage@localhost:" in runner_script
    assert '[string]$Backend = "local"' in runner_script
    assert "run-official-schedule-value-batches.ps1" in runner_script
    assert "official-host-batch-" in runner_script
    assert ".\\.venv\\Scripts\\dagster.exe" in runner_script
    assert "scripts\\check_training_runtime.py" in runner_script
    assert (
        '@(($preflightCommand -join " "), ($manifestCommand -join " "))'
        in runner_script
    )
    assert "build_hf_official_schedule_value_job.py" in runner_script
    assert "submit_hf_official_schedule_value_job.py" in runner_script
    assert "if ($Submit)" in runner_script
    assert "--submit" in runner_script
    assert "Offline Strategy Promotion evidence only" in runner_script


def test_training_runtime_preflight_reports_host_and_optional_docker_runtime() -> None:
    preflight_script = (
        PROJECT_ROOT / "scripts" / "check_training_runtime.py"
    ).read_text(encoding="utf-8")

    assert "--include-docker" in preflight_script
    assert "torch.cuda.is_available()" in preflight_script
    assert "torch.cuda.get_device_name" in preflight_script
    assert "torch.cuda.get_device_properties" in preflight_script
    assert "docker compose exec -T" in preflight_script
    assert "market_execution_enabled" in preflight_script
    assert "Offline Strategy Promotion evidence only" in preflight_script


def _environment_without_pythonpath() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    return environment
