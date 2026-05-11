from __future__ import annotations

import os
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_package_imports_without_test_pythonpath() -> None:
    result = subprocess.run(
        ["uv", "run", "python", "-c", "import smart_arbitrage; print(smart_arbitrage.__file__)"],
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
    verify_script = (PROJECT_ROOT / "scripts" / "verify.ps1").read_text(encoding="utf-8")

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
    assert 'Select-String -LiteralPath $stderrPath -Pattern "RUN_SUCCESS" -Quiet' in runner_script
    assert "if ($exitCode -ne 0)" in runner_script
    assert '[ValidateSet("chronological", "latest_first")]' in runner_script
    assert "enabled_official_model_names_csv" in runner_script
    assert "nbeatsx_max_steps: $NbeatsxMaxSteps" in runner_script
    assert "tft_max_epochs: $TftMaxEpochs" in runner_script


def test_official_global_panel_batch_runner_writes_resumable_backfill_config() -> None:
    runner_script = (
        PROJECT_ROOT / "scripts" / "run-official-global-panel-batches.ps1"
    ).read_text(encoding="utf-8")

    assert "real_data_official_global_panel_nbeatsx_backfill_week3.yaml" in runner_script
    assert "nbeatsx_official_global_panel_rolling_strict_lp_benchmark_frame" in runner_script
    assert "anchor_batch_start_index: $anchorIndex" in runner_script
    assert "anchor_batch_size: $BatchSize" in runner_script
    assert "resume_generated_at_iso: \"$GeneratedAtIso\"" in runner_script
    assert "merge_persisted_batches: true" in runner_script
    assert "nbeatsx_official_global_panel_rolling_horizon_calibration_frame" in runner_script
    assert "dfl_official_global_panel_schedule_value_production_gate_frame" in runner_script


def _environment_without_pythonpath() -> dict[str, str]:
    environment = os.environ.copy()
    environment.pop("PYTHONPATH", None)
    return environment
