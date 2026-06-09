from pathlib import Path
import json
import subprocess
import sys

from smart_arbitrage.cloud.hf_official_jobs import (
    HfOfficialScheduleValueJobConfig,
    build_hf_official_schedule_value_job_payload,
    submit_hf_official_schedule_value_job_payload,
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
    assert "official_evidence_attempt_manifest.json" in script
    assert "offline_strategy_promotion_evidence_attempt" in script
    assert "anchor_batch_order: \"latest_first\"" in script
    assert "enabled_official_model_names_csv: \"tft_official_v0\"" in script
    assert "nbeatsx_max_steps: 25" in script
    assert "tft_max_epochs: 5" in script
    assert "hf_abc" not in script
    assert payload["metadata"]["run_slug"] == "week3_hf_latest_tft_screen"
    assert payload["metadata"]["market_execution_enabled"] is False


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


def test_hf_official_job_submission_dry_run_writes_receipt_without_calling_hf(
    tmp_path: Path,
) -> None:
    payload_path = tmp_path / "payload.json"
    output_path = tmp_path / "receipt.json"
    payload_path.write_text(
        json.dumps(
            build_hf_official_schedule_value_job_payload(
                HfOfficialScheduleValueJobConfig(
                    run_slug="week3_hf_screen",
                    artifact_repo_id="ilyafefelov/smart-arbitrage-official-evidence",
                )
            )
        ),
        encoding="utf-8",
    )
    submit_calls: list[dict[str, object]] = []

    receipt = submit_hf_official_schedule_value_job_payload(
        payload_path,
        output_path=output_path,
        submit=False,
        submitter=lambda payload: submit_calls.append(payload) or {"id": "unexpected"},
    )

    assert submit_calls == []
    assert receipt["submitted"] is False
    assert receipt["submit_requested"] is False
    assert receipt["run_slug"] == "week3_hf_screen"
    assert receipt["token_required"] is True
    assert receipt["token_resolved"] is False
    assert receipt["market_execution_enabled"] is False
    assert receipt["claim_boundary"] == "offline_strategy_promotion_evidence_only_not_market_execution"
    assert "HF_TOKEN" not in output_path.read_text(encoding="utf-8")


def test_hf_official_job_submission_replaces_token_only_in_memory(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    output_path = tmp_path / "receipt.json"
    payload_path.write_text(
        json.dumps(
            build_hf_official_schedule_value_job_payload(
                HfOfficialScheduleValueJobConfig(
                    run_slug="week3_hf_submit",
                    artifact_repo_id="ilyafefelov/smart-arbitrage-official-evidence",
                )
            )
        ),
        encoding="utf-8",
    )
    submit_calls: list[dict[str, object]] = []

    receipt = submit_hf_official_schedule_value_job_payload(
        payload_path,
        output_path=output_path,
        submit=True,
        token_resolver=lambda: "hf_secret_token",
        submitter=lambda payload: submit_calls.append(payload)
        or {
            "id": "job-123",
            "url": "https://huggingface.co/jobs/user/job-123",
            "status": "QUEUED",
        },
    )

    assert submit_calls[0]["secrets"] == {"HF_TOKEN": "hf_secret_token"}
    receipt_text = output_path.read_text(encoding="utf-8")
    assert "hf_secret_token" not in receipt_text
    assert "$HF_TOKEN" not in receipt_text
    assert receipt["submitted"] is True
    assert receipt["job_id"] == "job-123"
    assert receipt["job_status"] == "QUEUED"


def test_hf_official_job_submission_blocks_missing_token(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    output_path = tmp_path / "receipt.json"
    payload_path.write_text(
        json.dumps(
            build_hf_official_schedule_value_job_payload(
                HfOfficialScheduleValueJobConfig(
                    run_slug="week3_hf_missing_token",
                    artifact_repo_id="ilyafefelov/smart-arbitrage-official-evidence",
                )
            )
        ),
        encoding="utf-8",
    )

    try:
        submit_hf_official_schedule_value_job_payload(
            payload_path,
            output_path=output_path,
            submit=True,
            token_resolver=lambda: None,
            submitter=lambda _payload: {"id": "unexpected"},
        )
    except RuntimeError as error:
        assert "HF_TOKEN is required" in str(error)
    else:
        raise AssertionError("artifact-upload payload submission should require a token")


def test_hf_official_job_submission_cli_defaults_to_dry_run(tmp_path: Path) -> None:
    payload_path = tmp_path / "payload.json"
    output_path = tmp_path / "receipt.json"
    payload_path.write_text(
        json.dumps(
            build_hf_official_schedule_value_job_payload(
                HfOfficialScheduleValueJobConfig(
                    run_slug="week3_hf_cli_dry_run",
                    artifact_repo_id="ilyafefelov/smart-arbitrage-official-evidence",
                )
            )
        ),
        encoding="utf-8",
    )

    subprocess.run(
        [
            sys.executable,
            "scripts/submit_hf_official_schedule_value_job.py",
            "--payload",
            str(payload_path),
            "--output",
            str(output_path),
        ],
        check=True,
    )

    receipt = json.loads(output_path.read_text(encoding="utf-8"))
    assert receipt["submitted"] is False
    assert receipt["submit_requested"] is False
    assert receipt["run_slug"] == "week3_hf_cli_dry_run"
