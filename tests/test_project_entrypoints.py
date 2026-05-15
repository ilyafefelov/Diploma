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
    assert "--dagster-run-id" in export_script
    assert "build_dfl_schedule_value_learner_v2_plus_comparison_packet" in export_script
    assert "write_dfl_schedule_value_learner_v2_plus_comparison_packet" in export_script


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
