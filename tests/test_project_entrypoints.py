from __future__ import annotations

import json
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


def test_verify_wrapper_can_run_optional_lava_npz_margin_smoke() -> None:
    verify_script = (PROJECT_ROOT / "scripts" / "verify.ps1").read_text(
        encoding="utf-8"
    )
    smoke_script = (
        PROJECT_ROOT / "scripts" / "materialize_lava_npz_margin_smoke_packet.py"
    ).read_text(encoding="utf-8")

    assert "SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE" in verify_script
    assert "Invoke-OptionalLavaNpzMarginSmoke" in verify_script
    assert "materialize_lava_npz_margin_smoke_packet.py" in verify_script
    assert "validate_lava_npz_margin_smoke_packet.py" in verify_script
    assert "sys.path.insert" in smoke_script
    assert (
        "data\\research_runs\\week3_dfl_ua_context_acquisition_v13_safe_switch_only"
        in verify_script
    )
    assert "dfl_ua_context_v13_acquisition_summary.json" in verify_script
    assert ".tmp_runtime\\verify_lava_npz_margin_smoke" in verify_script
    assert "candidate frame is not configured" in verify_script
    assert "market_execution_enabled=false" in verify_script
    assert "promotion_gate=false" in verify_script
    assert "permits_model_training=false" in verify_script
    assert "market_execution_enabled: true" not in verify_script.lower()


def test_local_start_script_sets_operator_preview_store_dsns() -> None:
    start_script = (PROJECT_ROOT / "scripts" / "start-local-project.ps1").read_text(
        encoding="utf-8"
    )

    assert (
        '$localPostgresDsn = "postgresql://smart:arbitrage@localhost:5432/smart_arbitrage"'
        in start_script
    )
    assert "SMART_ARBITRAGE_MARKET_DATA_DSN" in start_script
    assert "SMART_ARBITRAGE_FORECAST_DSN" in start_script
    assert "$env:SMART_ARBITRAGE_MARKET_DATA_DSN = $localPostgresDsn" in start_script
    assert "$env:SMART_ARBITRAGE_FORECAST_DSN = $localPostgresDsn" in start_script


def test_local_start_script_exports_selected_api_port_to_dashboard() -> None:
    start_script = (PROJECT_ROOT / "scripts" / "start-local-project.ps1").read_text(
        encoding="utf-8"
    )

    assert '$env:NUXT_API_BASE = "http://127.0.0.1:$ApiPort"' in start_script


def test_local_start_script_bootstraps_docker_desktop_before_compose() -> None:
    start_script = (PROJECT_ROOT / "scripts" / "start-local-project.ps1").read_text(
        encoding="utf-8"
    )

    assert "Test-DockerDaemonReady" in start_script
    assert "Start-DockerDesktopIfAvailable" in start_script
    assert "Wait-DockerDaemon" in start_script
    assert "& docker info" in start_script
    assert "Docker Desktop.exe" in start_script
    assert "Docker daemon is not reachable" in start_script
    assert "Docker did not become ready within" in start_script
    assert "docker compose up -d @composeServices" in start_script


def test_posix_local_start_script_exists_for_mac_linux() -> None:
    start_script = (PROJECT_ROOT / "scripts" / "start-local-project.sh").read_text(
        encoding="utf-8"
    )

    assert "#!/usr/bin/env bash" in start_script
    assert "--api-port" in start_script
    assert "--dashboard-port" in start_script
    assert "PYTHON_PATH=\"$REPO_ROOT/.venv/bin/python\"" in start_script
    assert "WINDOWS_PYTHON_PATH=\"$REPO_ROOT/.venv/Scripts/python.exe\"" in (
        start_script
    )
    assert "docker compose up -d \"${compose_services[@]}\"" in start_script
    assert "npm -C dashboard run dev -- --host 127.0.0.1 --port" in start_script
    assert "NUXT_API_BASE=\"http://127.0.0.1:$API_PORT\"" in start_script


def test_posix_local_start_script_bootstraps_docker_daemon() -> None:
    start_script = (PROJECT_ROOT / "scripts" / "start-local-project.sh").read_text(
        encoding="utf-8"
    )

    assert "docker info" in start_script
    assert "start_docker_if_available" in start_script
    assert "wait_for_docker" in start_script
    assert "open -a Docker" in start_script
    assert "systemctl start docker" in start_script
    assert "Docker daemon is not reachable" in start_script
    assert "Docker did not become ready within" in start_script


def test_api_start_dev_sh_accepts_posix_virtualenv_path() -> None:
    start_script = (PROJECT_ROOT / "api" / "start-dev.sh").read_text(
        encoding="utf-8"
    )

    assert "POSIX_PYTHON_PATH=\"$REPO_ROOT/.venv/bin/python\"" in start_script
    assert "WINDOWS_PYTHON_PATH=\"$REPO_ROOT/.venv/Scripts/python.exe\"" in (
        start_script
    )
    assert "PYTHON_PATH=\"$POSIX_PYTHON_PATH\"" in start_script


def test_dashboard_package_exposes_scoped_vitest_script() -> None:
    package_json = json.loads(
        (PROJECT_ROOT / "dashboard" / "package.json").read_text(encoding="utf-8")
    )

    assert package_json["scripts"]["test:unit"] == "vitest run"


def test_dt_lava_prototype_readiness_packet_entrypoint_is_documented() -> None:
    script_path = PROJECT_ROOT / "scripts" / "materialize_dt_lava_prototype_readiness_packet.py"
    academic_mvp_script_path = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_credentialless_academic_mvp_readiness_packet.py"
    )
    academic_mvp_validation_script_path = (
        PROJECT_ROOT
        / "scripts"
        / "validate_credentialless_academic_mvp_readiness_packet.py"
    )
    dt_research_shadow_script_path = (
        PROJECT_ROOT / "scripts" / "materialize_dt_research_shadow_packet.py"
    )
    bridge_doc_path = PROJECT_ROOT / "docs" / "technical" / "DFL_LAVA_SCHEDULE_NEIGHBOR_BRIDGE.md"

    assert script_path.exists()
    assert academic_mvp_script_path.exists()
    assert academic_mvp_validation_script_path.exists()
    assert dt_research_shadow_script_path.exists()

    script = script_path.read_text(encoding="utf-8")
    academic_mvp_script = academic_mvp_script_path.read_text(encoding="utf-8")
    academic_mvp_validation_script = academic_mvp_validation_script_path.read_text(
        encoding="utf-8"
    )
    bridge_doc = bridge_doc_path.read_text(encoding="utf-8")
    verify_script = (PROJECT_ROOT / "scripts" / "verify.ps1").read_text(
        encoding="utf-8"
    )
    api_main = (PROJECT_ROOT / "api" / "main.py").read_text(encoding="utf-8")
    dashboard_academic_mvp_proxy = (
        PROJECT_ROOT
        / "dashboard"
        / "server"
        / "api"
        / "control-plane"
        / "dashboard"
        / "academic-mvp-readiness.get.ts"
    ).read_text(encoding="utf-8")

    assert "SUMMARY_JSON_NAME" in script
    assert "SUMMARY_MARKDOWN_NAME" in script
    assert "--lava-npz-smoke-validation-json" in script
    assert "candidate_frame_pickle_missing" in script
    assert "market_execution_enabled" in script
    assert "materialize_dt_lava_prototype_readiness_packet.py" in bridge_doc
    assert "--lava-npz-smoke-validation-json" in bridge_doc
    assert "materialize_credentialless_academic_mvp_readiness_packet.py" in bridge_doc
    assert "validate_credentialless_academic_mvp_readiness_packet.py" in bridge_doc
    assert "materialize_dt_research_shadow_packet.py" in bridge_doc
    assert "--dt-research-shadow-sequence-summary-json" in bridge_doc
    assert (
        "source_publication_timestamp is not required for offline "
        "research-shadow DT prototype"
        in bridge_doc
    )
    assert "SCMO credentials are not required for the diploma MVP" in bridge_doc
    assert "market_submission_ready=false" in bridge_doc
    assert "--operator-preview-json" in academic_mvp_script
    assert "--tenant-id" in academic_mvp_script
    assert "--teacher-validation-json" in academic_mvp_script
    assert "--teacher-validation-json" in bridge_doc
    assert "--offline-challenger-validation-json" in academic_mvp_script
    assert "--offline-challenger-validation-json" in bridge_doc
    assert "ProposedBid" in academic_mvp_script
    assert "/dashboard/academic-mvp-readiness" in api_main
    assert "/dashboard/academic-mvp-readiness" in dashboard_academic_mvp_proxy
    assert "AcademicMvpReadinessResponse" in dashboard_academic_mvp_proxy
    assert "validate_credentialless_academic_mvp_readiness_summary" in (
        academic_mvp_validation_script
    )
    assert "materialize_dt_lava_prototype_readiness_packet.py" in verify_script
    assert "--lava-npz-smoke-validation-json" in verify_script
    assert "market_execution_enabled: true" not in script.lower()
    assert "market_execution_enabled: true" not in academic_mvp_script.lower()


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
    teacher_packet_script = (
        PROJECT_ROOT / "scripts" / "materialize_v13_dt_lava_teacher_packet.py"
    ).read_text(encoding="utf-8")
    receipt_validator_script = (
        PROJECT_ROOT / "scripts" / "validate_oree_dam_publication_receipts.py"
    ).read_text(encoding="utf-8")
    safe_switch_validator_script = (
        PROJECT_ROOT / "scripts" / "validate_ua_context_safe_switch_examples_v13.py"
    ).read_text(encoding="utf-8")
    safe_switch_candidate_audit_script = (
        PROJECT_ROOT / "scripts" / "audit_ua_context_safe_switch_candidates_v13.py"
    ).read_text(encoding="utf-8")
    safe_switch_review_backlog_script = (
        PROJECT_ROOT
        / "scripts"
        / "export_ua_context_v13_safe_switch_review_backlog.py"
    ).read_text(encoding="utf-8")
    safe_switch_curation_worksheet_script = (
        PROJECT_ROOT
        / "scripts"
        / "export_ua_context_v13_safe_switch_curation_worksheet.py"
    ).read_text(encoding="utf-8")
    safe_switch_curation_extract_script = (
        PROJECT_ROOT
        / "scripts"
        / "extract_ua_context_v13_safe_switch_examples_from_curation.py"
    ).read_text(encoding="utf-8")
    input_preflight_script = (
        PROJECT_ROOT / "scripts" / "preflight_ua_context_v13_acquisition_inputs.py"
    ).read_text(encoding="utf-8")
    input_config_builder_script = (
        PROJECT_ROOT / "scripts" / "build_v13_acquisition_input_config.py"
    ).read_text(encoding="utf-8")
    receipt_probe_script = (
        PROJECT_ROOT / "scripts" / "probe_oree_dam_publication_receipts.py"
    ).read_text(encoding="utf-8")
    receipt_audit_script = (
        PROJECT_ROOT / "scripts" / "audit_oree_dam_publication_receipt_sources.py"
    ).read_text(encoding="utf-8")
    receipt_lead_audit_script = (
        PROJECT_ROOT / "scripts" / "audit_v13_dam_receipt_source_leads.py"
    ).read_text(encoding="utf-8")
    public_oree_candidate_audit_script = (
        PROJECT_ROOT / "scripts" / "audit_oree_v13_receipt_candidates.py"
    ).read_text(encoding="utf-8")
    policy_publication_evidence_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_oree_policy_publication_deadline_evidence.py"
    ).read_text(encoding="utf-8")
    energy_map_metadata_probe_script = (
        PROJECT_ROOT / "scripts" / "probe_energy_map_dam_receipt_metadata.py"
    ).read_text(encoding="utf-8")
    scmo_soap_download_probe_script = (
        PROJECT_ROOT / "scripts" / "probe_scmo_dam_soap_download.py"
    ).read_text(encoding="utf-8")
    scmo_ws_security_preflight_script = (
        PROJECT_ROOT / "scripts" / "preflight_scmo_dam_ws_security_credentials.py"
    ).read_text(encoding="utf-8")
    scmo_receipt_normalizer_script = (
        PROJECT_ROOT
        / "scripts"
        / "normalize_scmo_dam_publication_receipt_export.py"
    ).read_text(encoding="utf-8")
    scmo_receipt_fetch_script = (
        PROJECT_ROOT / "scripts" / "fetch_scmo_dam_publication_receipt_export.py"
    ).read_text(encoding="utf-8")
    receipt_observation_script = (
        PROJECT_ROOT / "scripts" / "capture_oree_dam_publication_observations.py"
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
    assert "build_dfl_v13_dt_lava_teacher_packet" in teacher_packet_script
    assert "write_dfl_v13_dt_lava_teacher_packet" in teacher_packet_script
    assert "--teacher-contract-pickle" in teacher_packet_script
    assert "market_execution_enabled" in teacher_packet_script
    assert "--source-evidence-pickle" in export_script
    assert "--source-evidence-csv" in export_script
    assert "--source-inventory-csv" in export_script
    assert "--readiness-csv" in export_script
    assert "--receipt-source-audit-json" in export_script
    assert "--receipt-source-lead-audit-json" in export_script
    assert "--policy-publication-evidence-json" in export_script
    assert "--safe-switch-candidate-audit-json" in export_script
    assert "--acquisition-input-preflight-json" in export_script
    assert "--scmo-ws-security-preflight-json" in export_script
    assert "v13_candidate_generation_ready" in export_script
    assert "normalize_dfl_ua_dam_publication_receipts_frame" in receipt_validator_script
    assert "--input" in receipt_validator_script
    assert "--output" in receipt_validator_script
    assert (
        "normalize_dfl_ua_context_safe_switch_examples_v13_frame"
        in safe_switch_validator_script
    )
    assert "--input" in safe_switch_validator_script
    assert "--output" in safe_switch_validator_script
    assert "permits_model_training" in safe_switch_validator_script
    assert (
        "audit_dfl_ua_context_safe_switch_candidate_source_v13_frame"
        in safe_switch_candidate_audit_script
    )
    assert "--material-label-column" in safe_switch_candidate_audit_script
    assert "--tail-risk-label-column" in safe_switch_candidate_audit_script
    assert "market execution" in safe_switch_candidate_audit_script
    assert (
        "build_dfl_ua_context_safe_switch_review_backlog_v13_frame"
        in safe_switch_review_backlog_script
    )
    assert "--candidate-rows-csv" in safe_switch_review_backlog_script
    assert "--acquisition-targets-csv" in safe_switch_review_backlog_script
    assert "candidate_can_satisfy_v13_without_validation" in safe_switch_review_backlog_script
    assert "market execution" in safe_switch_review_backlog_script
    assert (
        "build_dfl_ua_context_safe_switch_curation_worksheet_v13_frame"
        in safe_switch_curation_worksheet_script
    )
    assert "--review-backlog-csv" in safe_switch_curation_worksheet_script
    assert "pending worksheet rows do not satisfy V13".lower() in (
        safe_switch_curation_worksheet_script.lower()
    )
    assert (
        "extract_dfl_ua_context_safe_switch_examples_from_curation_v13_frame"
        in safe_switch_curation_extract_script
    )
    assert "--curation-worksheet-csv" in safe_switch_curation_extract_script
    assert "market execution" in safe_switch_curation_extract_script
    assert "validate_v13_acquisition_inputs" in input_preflight_script
    assert "oree_dam_publication_receipts_csv_path" in input_preflight_script
    assert "ua_context_safe_switch_examples_csv_path" in input_preflight_script
    assert "data_acquisition_needed" in input_preflight_script
    assert "build_v13_acquisition_input_config" in input_config_builder_script
    assert "--dam-receipts-csv" in input_config_builder_script
    assert "--safe-switch-csv" in input_config_builder_script
    assert "validate_v13_acquisition_inputs" in input_config_builder_script
    assert "market_execution_enabled" in input_config_builder_script
    assert "build_oree_dam_publication_receipt_probe" in receipt_probe_script
    assert "--month" in receipt_probe_script
    assert "--output" in receipt_probe_script
    assert "build_oree_dam_publication_receipt_source_audit" in receipt_audit_script
    assert "--months" in receipt_audit_script
    assert "--probe-json" in receipt_audit_script
    assert "--probe-output-dir" in receipt_audit_script
    assert (
        "audit_dfl_ua_context_dam_receipt_source_leads_v13_frame"
        in receipt_lead_audit_script
    )
    assert "--input" in receipt_lead_audit_script
    assert "--output" in receipt_lead_audit_script
    assert "does not emit receipt rows" in receipt_lead_audit_script
    assert "summarize_oree_v13_receipt_candidate_audit" in (
        public_oree_candidate_audit_script
    )
    assert "--month" in public_oree_candidate_audit_script
    assert "--delivery-date" in public_oree_candidate_audit_script
    assert "--output-json" in public_oree_candidate_audit_script
    assert "--output-csv" in public_oree_candidate_audit_script
    assert "market execution" in public_oree_candidate_audit_script
    assert "build_oree_policy_publication_deadline_evidence_frame" in (
        policy_publication_evidence_script
    )
    assert "--candidate-audit-json" in policy_publication_evidence_script
    assert "--output-csv" in policy_publication_evidence_script
    assert "--summary-json" in policy_publication_evidence_script
    assert "does not satisfy V13 explicit receipt readiness" in (
        policy_publication_evidence_script
    )
    assert "build_energy_map_dam_receipt_metadata_leads_v13_frame" in (
        energy_map_metadata_probe_script
    )
    assert "--dataset-id" in energy_map_metadata_probe_script
    assert "--input-json" in energy_map_metadata_probe_script
    assert "file_level_publication_metadata_only" in energy_map_metadata_probe_script
    assert "market execution" in energy_map_metadata_probe_script
    assert "build_scmo_dam_soap_download_probe" in scmo_soap_download_probe_script
    assert "--soap-action" in scmo_soap_download_probe_script
    assert "--credential-mode" in scmo_soap_download_probe_script
    assert "preflight-gated-mtls-username-token" in scmo_soap_download_probe_script
    assert "sanitize_scmo_soap_request_for_artifact" in scmo_soap_download_probe_script
    assert "--normalized-output" in scmo_soap_download_probe_script
    assert "candidate DownloadResponse/ISOTEDATA" in scmo_soap_download_probe_script
    assert "build_scmo_ws_security_credential_preflight" in (
        scmo_ws_security_preflight_script
    )
    assert "SCMO_CLIENT_CERT_PEM" in scmo_ws_security_preflight_script
    assert "SCMO_CLIENT_KEY_PEM" in scmo_ws_security_preflight_script
    assert "SCMO_CLIENT_KEY_PASSWORD" in scmo_ws_security_preflight_script
    assert "SCMO_CLIENT_P12" in scmo_ws_security_preflight_script
    assert "SCMO_CLIENT_P12_PASSWORD" in scmo_ws_security_preflight_script
    assert "credential_file_pair_valid" in scmo_ws_security_preflight_script
    assert "never writes secret values" in scmo_ws_security_preflight_script
    assert "normalize_scmo_dam_publication_receipt_export_frame" in (
        scmo_receipt_normalizer_script
    )
    assert "--input-format" in scmo_receipt_normalizer_script
    assert '"zip"' in scmo_receipt_normalizer_script
    assert '"zip"' in scmo_receipt_fetch_script
    assert '"zip"' in scmo_soap_download_probe_script
    assert '"html"' in scmo_receipt_normalizer_script
    assert '"html"' in scmo_receipt_fetch_script
    assert '"html"' in scmo_soap_download_probe_script
    assert "--source-publication-timestamp-column" in scmo_receipt_normalizer_script
    assert "--v13-base-config" in scmo_receipt_normalizer_script
    assert "--v13-safe-switch-csv" in scmo_receipt_normalizer_script
    assert "--v13-output-config" in scmo_receipt_normalizer_script
    assert "--v13-preflight-output" in scmo_receipt_normalizer_script
    assert "market_execution_enabled" in scmo_receipt_normalizer_script
    assert "fetch_result_from_scmo_export_response" in scmo_receipt_fetch_script
    assert "SCMO_COOKIE" in scmo_receipt_fetch_script
    assert "--normalized-output" in scmo_receipt_fetch_script
    assert "--v13-base-config" in scmo_receipt_fetch_script
    assert "--v13-preflight-output" in scmo_receipt_fetch_script
    assert "build_oree_dam_publication_observation_frame" in receipt_observation_script
    assert "--delivery-date" in receipt_observation_script
    assert "--output-csv" in receipt_observation_script
    assert "without creating V13 receipt rows" in receipt_observation_script
    assert "dfl_ua_dam_publication_receipts_overlay_frame" in config
    assert "oree_dam_publication_receipts_csv_path" in config
    assert "dfl_ua_context_acquisition_source_evidence_v13_frame" in config
    assert "dfl_ua_context_source_inventory_v13_frame" in config
    assert "dfl_ua_context_acquisition_readiness_v13_frame" in config
    assert "dfl_ua_dam_publication_receipts_overlay_frame" in docs
    assert "materialize_v13_dt_lava_teacher_packet.py" in docs
    assert "dfl_v13_dt_lava_teacher_summary.json" in docs
    assert "candidate id" in docs
    assert "schedule-family targets" in docs
    assert "source_publication_timestamp" in docs
    assert "oree_dam_publication_receipts_csv_path" in docs
    assert "probe_oree_dam_publication_receipts.py" in docs
    assert "audit_oree_dam_publication_receipt_sources.py" in docs
    assert "audit_oree_v13_receipt_candidates.py" in docs
    assert "materialize_oree_policy_publication_deadline_evidence.py" in docs
    assert "price_DAM_IDM_05.2026.xls" in docs
    assert "policy_publication_deadline_kyiv" in docs
    assert "observation_only" in docs
    assert "audit_v13_dam_receipt_source_leads.py" in docs
    assert "probe_energy_map_dam_receipt_metadata.py" in docs
    assert "probe_scmo_dam_soap_download.py" in docs
    assert "preflight-gated-mtls-username-token" in docs
    assert "ws_security_signature_applied=false" in docs
    assert "preflight-gated-signed-ws-security" in docs
    assert "xml_signature_builder_available" in docs
    assert "preflight_scmo_dam_ws_security_credentials.py" in docs
    assert "--scmo-ws-security-preflight-json" in docs
    assert "SCMO_CLIENT_CERT_PEM" in docs
    assert "SCMO_CLIENT_P12" in docs
    assert "SCMO_CLIENT_KEY_PASSWORD" in docs
    assert "credential_file_pair_valid=true" in docs
    assert "secret values" in docs
    assert "normalize_scmo_dam_publication_receipt_export.py" in docs
    assert "fetch_scmo_dam_publication_receipt_export.py" in docs
    assert "ISOTEDATA" in docs
    assert "HTML table" in docs
    assert "--v13-base-config" in docs
    assert "--v13-preflight-output" in docs
    assert "source_publication_timestamp" in docs
    assert "wsdl_response_returned_signed_download_required" in docs
    assert "file_level_publication_metadata_only" in docs
    assert "capture_oree_dam_publication_observations.py" in docs
    assert "source_observed_at_utc" in docs
    assert "--attempt-log-json" in receipt_observation_script
    assert "--max-attempts" in receipt_observation_script
    assert "can_satisfy_v13_explicit_receipts=false" in docs
    assert "dataset-level" in docs
    assert "not_sufficient_for_v13_receipts" in docs
    assert "dfl_ua_context_acquisition_source_evidence_v13_frame" in docs
    assert "data_acquisition_needed" in docs
    assert "Safe-Switch Support Deficit" in docs
    assert "safe_switch_deficit_summary" in export_script
    assert "safe_switch_acquisition_target_summary" in export_script
    assert "source_acquisition_backlog_summary" in export_script
    assert "UA_CONTEXT_V13_SOURCE_ACQUISITION_BACKLOG_CSV_ARTIFACT_NAME" in export_script
    assert "primary_blocking_source_family" in export_script
    assert "receipt_source_audit_summary" in export_script
    assert "UA_CONTEXT_V13_RECEIPT_SOURCE_AUDIT_JSON_ARTIFACT_NAME" in export_script
    assert "receipt_source_lead_audit_summary" in export_script
    assert (
        "UA_CONTEXT_V13_RECEIPT_SOURCE_LEAD_AUDIT_JSON_ARTIFACT_NAME"
        in export_script
    )
    assert "safe_switch_candidate_audit_summary" in export_script
    assert (
        "UA_CONTEXT_V13_SAFE_SWITCH_CANDIDATE_AUDITS_JSON_ARTIFACT_NAME"
        in export_script
    )
    assert "acquisition_input_preflight_summary" in export_script
    assert (
        "UA_CONTEXT_V13_ACQUISITION_INPUT_PREFLIGHT_JSON_ARTIFACT_NAME"
        in export_script
    )
    assert "scmo_ws_security_preflight_summary" in export_script
    assert (
        "UA_CONTEXT_V13_SCMO_WS_SECURITY_PREFLIGHT_JSON_ARTIFACT_NAME"
        in export_script
    )
    assert "Safe-Switch Acquisition Targets" in docs
    assert "Safe-Switch Example Backfill Input" in docs
    assert "ua_context_safe_switch_examples_csv_path" in docs
    assert "validate_ua_context_safe_switch_examples_v13.py" in docs
    assert "audit_ua_context_safe_switch_candidates_v13.py" in docs
    assert "export_ua_context_v13_safe_switch_review_backlog.py" in docs
    assert "export_ua_context_v13_safe_switch_curation_worksheet.py" in docs
    assert "extract_ua_context_v13_safe_switch_examples_from_curation.py" in docs
    assert "candidate_can_satisfy_v13_without_validation=false" in docs
    assert "normalized_safe_switch_csv_ready=false" in docs
    assert "preflight_ua_context_v13_acquisition_inputs.py" in docs
    assert "build_v13_acquisition_input_config.py" in docs
    assert "Source Acquisition Backlog" in docs
    assert "Acquisition Input Preflight" in docs
    assert "primary_blocking_source_family" in docs
    assert "receipt_source_audit_summary" in docs
    assert "receipt_source_lead_audit_summary" in docs
    assert "safe_switch_candidate_audit_summary" in docs
    assert "acquisition_input_preflight_summary" in docs
    assert "scmo_ws_security_preflight_summary" in docs
    assert "market_execution_enabled=false" in docs
    assert "Offline Strategy Promotion" in docs


def test_agents_md_preserves_v13_claim_boundary() -> None:
    agents = (PROJECT_ROOT / "AGENTS.md").read_text(encoding="utf-8")

    assert "DAM/IDM hourly recommendation preview" in agents
    assert "V13 є gate для acquisition/source-readiness" in agents
    assert "щонайменше `20` prior/train non-tail-risk material safe-switch examples" in agents
    assert "explicit OREE DAM/IDM source/publication evidence for preview" in agents
    assert "ua_context_safe_switch_examples_csv_path" in agents
    assert "validate_ua_context_safe_switch_examples_v13.py" in agents
    assert "audit_ua_context_safe_switch_candidates_v13.py" in agents
    assert "export_ua_context_v13_safe_switch_review_backlog.py" in agents
    assert "export_ua_context_v13_safe_switch_curation_worksheet.py" in agents
    assert "extract_ua_context_v13_safe_switch_examples_from_curation.py" in agents
    assert "candidate_can_satisfy_v13_without_validation=false" in agents
    assert "audit_v13_dam_receipt_source_leads.py" in agents
    assert "probe_scmo_dam_soap_download.py" in agents
    assert "preflight-gated-mtls-username-token" in agents
    assert "ws_security_signature_applied=false" in agents
    assert "preflight-gated-signed-ws-security" in agents
    assert "xml_signature_builder_available" in agents
    assert "preflight_scmo_dam_ws_security_credentials.py" in agents
    assert "--scmo-ws-security-preflight-json" in agents
    assert "SCMO_CLIENT_CERT_PEM" in agents
    assert "SCMO_CLIENT_P12" in agents
    assert "SCMO_CLIENT_KEY_PASSWORD" in agents
    assert "cert/key loadability" in agents
    assert "never writes secret values" in agents
    assert "normalize_scmo_dam_publication_receipt_export.py" in agents
    assert "fetch_scmo_dam_publication_receipt_export.py" in agents
    assert "SCMO credentials are not required for the diploma MVP" in agents
    assert (
        "source_publication_timestamp is not required for offline "
        "research-shadow DT prototype"
        in agents
    )
    assert "Do not run SCMO credential/probe work by default" in agents
    assert "materialize_credentialless_academic_mvp_readiness_packet.py" in agents
    assert "materialize_dt_research_shadow_packet.py" in agents
    assert "--dt-research-shadow-sequence-summary-json" in agents
    assert "--teacher-validation-json" in agents
    assert "--offline-challenger-validation-json" in agents
    assert "ISOTEDATA" in agents
    assert "HTML table" in agents
    assert "--v13-base-config" in agents
    assert "full_v13_gate_evaluated=false" in agents
    assert "source_publication_timestamp" in agents
    assert "wsdl_response_returned_signed_download_required" in agents
    assert "capture_oree_dam_publication_observations.py" in agents
    assert "source_observed_at_utc" in agents
    assert "--attempt-log-json" in agents
    assert "source_probe_status=hdata_not_found" in agents
    assert "preflight_ua_context_v13_acquisition_inputs.py" in agents
    assert "build_v13_acquisition_input_config.py" in agents
    assert "materialize_v13_dt_lava_teacher_packet.py" in agents
    assert "SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE" in agents
    assert "`permits_model_training=false`" in agents
    assert "`market_execution_enabled=false` залишається обов'язковим" in agents
    assert "не генерують market-submittable `ProposedBid`" in agents
    assert "не є deployed DT controller" in agents
    assert "full differentiable DFL controller" in agents
    assert "генерує оптимальні заявки (ProposedBid)" not in agents
    assert "зменшує регрет на **67%**" not in agents
    assert "черги команд BUY/SELL/HOLD" not in agents
    assert "ProposedTrade" not in agents


def test_current_goal_boundary_doc_preserves_v13_scope() -> None:
    boundary_path = (
        PROJECT_ROOT / "docs" / "technical" / "CURRENT_GOAL_BOUNDARY_V13.md"
    )
    boundary = boundary_path.read_text(encoding="utf-8")
    readme = (PROJECT_ROOT / "README.md").read_text(encoding="utf-8")
    context = (PROJECT_ROOT / "CONTEXT.md").read_text(encoding="utf-8")

    assert "DAM/IDM hourly recommendation preview" in boundary
    assert "This is not" in boundary
    assert "market-submittable DAM/IDM bids" in boundary
    assert "no deployed Decision Transformer control" in boundary
    assert "no full differentiable DFL claim" in boundary
    assert "`market_execution_enabled=false`" in boundary
    assert "`20` prior/train non-tail-risk material safe-switch examples" in boundary
    assert "explicit OREE DAM/IDM source/publication evidence for preview" in boundary
    assert "`ready_rows=0/5`" in boundary
    assert "`77` safe-switch examples" in boundary
    assert "Yi et al. 2025" in boundary
    assert "Sang et al." in boundary
    assert "Decision Transformer" in boundary
    assert "CURRENT_GOAL_BOUNDARY_V13.md" in readme
    assert "CURRENT_GOAL_BOUNDARY_V13.md" in context


def test_lava_npz_margin_smoke_packet_cli_preserves_research_boundary() -> None:
    packet_script = (
        PROJECT_ROOT / "scripts" / "materialize_lava_npz_margin_smoke_packet.py"
    ).read_text(encoding="utf-8")
    teacher_contract_script = (
        PROJECT_ROOT
        / "scripts"
        / "materialize_v13_dt_lava_teacher_contract_from_candidate_frame.py"
    ).read_text(encoding="utf-8")
    validator_script = (
        PROJECT_ROOT / "scripts" / "validate_lava_npz_margin_smoke_packet.py"
    ).read_text(encoding="utf-8")
    aggregate_script = (
        PROJECT_ROOT / "scripts" / "aggregate_dt_lava_research_metrics.py"
    ).read_text(encoding="utf-8")
    bridge_doc = (
        PROJECT_ROOT / "docs" / "technical" / "DFL_LAVA_SCHEDULE_NEIGHBOR_BRIDGE.md"
    ).read_text(encoding="utf-8")
    pulse_doc = (
        PROJECT_ROOT
        / "docs"
        / "technical"
        / "pulse"
        / "5-24"
        / "11-dt-lava-fast-honest-path-analysis.md"
    ).read_text(encoding="utf-8")

    assert "PACKET_CLAIM_SCOPE" in packet_script
    assert "write_lava_npz_smoke_artifact_from_candidate_frame" in packet_script
    assert "run_lava_npz_margin_smoke" in packet_script
    assert "aggregate_dt_lava_research_metrics_payloads" in packet_script
    assert "dt_lava_research_metrics_aggregate.json" in packet_script
    assert "lava_npz_margin_smoke_packet_validation.json" in packet_script
    assert "--v13-acquisition-summary-json" in packet_script
    assert "v13_acquisition_summary_json" in packet_script
    assert "artifact_sha256" in packet_script
    assert "ci_smoke_only" in packet_script
    assert "promotion_gate" in packet_script
    assert "raw_hourly_action_imitation" in packet_script
    assert "market_execution_enabled" in packet_script
    assert (
        "build_dfl_v13_gated_dt_lava_teacher_contract_frame"
        in teacher_contract_script
    )
    assert "--readiness-csv" in teacher_contract_script
    assert "DT_ACTION_TARGET_CONTRACT" in teacher_contract_script
    assert "V2_PLUS_ROLE" in teacher_contract_script
    assert "market_execution_enabled" in teacher_contract_script
    assert "VALIDATION_CLAIM_SCOPE" in validator_script
    assert "artifact_sha256" in validator_script
    assert "SHA256 mismatch" in validator_script
    assert "v13_acquisition_summary_json" in validator_script
    assert "v13_max_prior_material_safe_switch_examples" in validator_script
    assert "v13_min_safe_examples_required" in validator_script
    assert "v13_candidate_generation_ready does not match" in validator_script
    assert "does not match validated packet artifacts" in validator_script
    assert "market_execution_enabled" in validator_script
    assert "aggregate_dt_lava_research_metrics_payloads" in aggregate_script
    assert "materialize_lava_npz_margin_smoke_packet.py" in bridge_doc
    assert "validate_lava_npz_margin_smoke_packet.py" in bridge_doc
    assert "aggregate_dt_lava_research_metrics.py" in bridge_doc
    assert "dt_lava_research_metrics_aggregate.json" in bridge_doc
    assert "lava_npz_margin_smoke_packet_validation.json" in bridge_doc
    assert "SMART_ARBITRAGE_VERIFY_LAVA_NPZ_CANDIDATE_FRAME_PICKLE" in bridge_doc
    assert ".\\scripts\\verify.ps1" in bridge_doc
    assert ".tmp_runtime\\verify_lava_npz_margin_smoke" in bridge_doc
    assert "--v13-acquisition-summary-json" in bridge_doc
    assert "dfl_ua_context_v13_acquisition_summary.json" in bridge_doc
    assert "max_prior_material_safe_switch_examples" in bridge_doc
    assert "min_safe_examples_required" in bridge_doc
    assert "contradictory V13 readiness claim" in bridge_doc
    assert "Manifest summary counters" in bridge_doc
    assert "SHA256" in bridge_doc
    assert "lava_npz_margin_smoke_manifest.json" in bridge_doc
    assert "materialize_v13_dt_lava_teacher_contract_from_candidate_frame.py" in (
        bridge_doc
    )
    assert "materialize_v13_dt_lava_teacher_packet.py" in bridge_doc
    assert "dfl_v13_gated_dt_lava_teacher_contract_frame_safe_switch_only.pkl" in (
        bridge_doc
    )
    assert "0 permitted" in bridge_doc
    assert "candidate_id_or_schedule_family" in bridge_doc
    assert "teacher_comparator_fallback" in bridge_doc
    assert "CI-fast" in bridge_doc
    assert "not training, not full DFL, not DT deployment, and not market" in bridge_doc
    assert "not a 4-window promotion gate" in bridge_doc
    assert "without making it a promotion gate" in pulse_doc
    assert "aggregate JSON" in pulse_doc
    assert "validation summary" in pulse_doc


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
