from pathlib import Path
import json
import subprocess
import sys

from smart_arbitrage.cloud.hf_official_jobs import (
    HfOfficialScheduleValueJobConfig,
    build_hf_official_schedule_value_job_payload,
)


def test_hf_official_job_payload_runs_same_official_schedule_value_gate() -> None:
    config = HfOfficialScheduleValueJobConfig(
        git_ref="codex/plan-next-slice-sunday-night-run",
        total_anchors_per_tenant=18,
        batch_size=4,
        anchor_batch_order="latest_first",
        enabled_official_models_csv="tft_official_v0",
        nbeatsx_max_steps=25,
        tft_max_epochs=5,
        run_slug="week3_hf_latest_tft_screen",
        artifact_repo_id="ilyafefelov/smart-arbitrage-official-evidence",
    )

    payload = build_hf_official_schedule_value_job_payload(config)
    script = str(payload["script"])

    assert payload["flavor"] == "t4-small"
    assert payload["timeout"] == "4h"
    assert payload["secrets"] == {"HF_TOKEN": "$HF_TOKEN"}
    assert "git clone --depth 1" in script
    assert "codex/plan-next-slice-sunday-night-run" in script
    assert "uv sync --extra dev --extra sota" in script
    assert "dagster asset materialize" in script
    assert "official_forecast_rolling_origin_benchmark_frame" in script
    assert "dfl_official_schedule_value_production_gate_frame" in script
    assert "anchor_batch_order: \"latest_first\"" in script
    assert "enabled_official_model_names_csv: \"tft_official_v0\"" in script
    assert "nbeatsx_max_steps: 25" in script
    assert "tft_max_epochs: 5" in script
    assert "hf_abc" not in script


def test_hf_official_job_payload_does_not_require_hub_secret_without_artifact_repo() -> None:
    payload = build_hf_official_schedule_value_job_payload(
        HfOfficialScheduleValueJobConfig(run_slug="local_artifact_only")
    )

    assert "secrets" not in payload
    assert "upload_folder" not in str(payload["script"])


def test_hf_official_job_cli_writes_payload_json(tmp_path: Path) -> None:
    output_path = tmp_path / "hf-job.json"

    subprocess.run(
        [
            sys.executable,
            "scripts/build_hf_official_schedule_value_job.py",
            "--git-ref",
            "codex/test",
            "--run-slug",
            "week3_hf_screen",
            "--artifact-repo-id",
            "ilyafefelov/smart-arbitrage-official-evidence",
            "--output",
            str(output_path),
        ],
        check=True,
    )

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["flavor"] == "t4-small"
    assert payload["secrets"] == {"HF_TOKEN": "$HF_TOKEN"}
    assert "codex/test" in payload["script"]
