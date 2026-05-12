"""Hugging Face Jobs payloads for official forecast evidence runs."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
import importlib
import json
import os
from pathlib import Path
import re
from textwrap import dedent
from typing import Literal

from smart_arbitrage.forecasting.official_evidence_attempts import (
    OfficialEvidenceAttemptConfig,
    build_official_evidence_attempt_manifest,
)

OFFICIAL_SCHEDULE_VALUE_SELECTION = (
    "observed_market_price_history_bronze,"
    "tenant_historical_weather_bronze,"
    "real_data_benchmark_silver_feature_frame,"
    "official_forecast_rolling_origin_benchmark_frame,"
    "dfl_official_schedule_candidate_library_frame,"
    "dfl_official_schedule_candidate_library_v2_frame,"
    "dfl_official_schedule_value_learner_v2_frame,"
    "dfl_official_schedule_value_learner_v2_strict_lp_benchmark_frame,"
    "dfl_official_schedule_value_learner_v2_robustness_frame,"
    "dfl_official_schedule_value_production_gate_frame"
)
HF_OFFICIAL_JOB_CLAIM_BOUNDARY = (
    "offline_strategy_promotion_evidence_only_not_market_execution"
)
HF_JOB_HOURLY_PRICE_BY_FLAVOR: dict[str, float] = {
    "t4-small": 0.40,
}
HfJobSubmitter = Callable[[dict[str, object]], Mapping[str, object] | object]
HfTokenResolver = Callable[[], str | None]


@dataclass(frozen=True, slots=True)
class HfOfficialScheduleValueJobConfig:
    """Configuration for a generated Hugging Face Jobs payload."""

    repo_url: str = "https://github.com/ilyafefelov/Diploma.git"
    git_ref: str = "main"
    total_anchors_per_tenant: int = 18
    batch_size: int = 4
    anchor_batch_order: Literal["chronological", "latest_first"] = "latest_first"
    enabled_official_models_csv: str = "tft_official_v0"
    nbeatsx_max_steps: int = 25
    tft_max_epochs: int = 5
    flavor: str = "t4-small"
    timeout: str = "4h"
    run_slug: str = "week3_hf_official_schedule_value_screen"
    artifact_repo_id: str = ""


def build_hf_official_schedule_value_job_payload(
    config: HfOfficialScheduleValueJobConfig,
) -> dict[str, object]:
    """Build, but do not submit, a Hugging Face Jobs UV payload."""

    _validate_config(config)
    payload: dict[str, object] = {
        "script": _build_uv_script(config),
        "flavor": config.flavor,
        "timeout": config.timeout,
        "metadata": _payload_metadata(config),
    }
    if config.artifact_repo_id.strip():
        payload["secrets"] = {"HF_TOKEN": "$HF_TOKEN"}
    return payload


def submit_hf_official_schedule_value_job_payload(
    payload_path: Path,
    *,
    output_path: Path,
    submit: bool = False,
    token_resolver: HfTokenResolver | None = None,
    submitter: HfJobSubmitter | None = None,
) -> dict[str, object]:
    """Write a local receipt and optionally submit a generated HF Jobs payload."""

    payload = _read_payload(payload_path)
    metadata = _payload_metadata_from_payload(payload)
    token_required = _payload_requires_hf_token(payload)
    receipt: dict[str, object] = {
        "schema_version": 1,
        "payload_path": str(payload_path),
        "submit_requested": submit,
        "submitted": False,
        "job_id": None,
        "job_url": None,
        "job_status": None,
        "run_slug": metadata["run_slug"],
        "flavor": str(payload.get("flavor", "")),
        "timeout": str(payload.get("timeout", "")),
        "estimated_timeout_cost_usd": metadata["estimated_timeout_cost_usd"],
        "artifact_repo_id": metadata["artifact_repo_id"],
        "token_required": token_required,
        "token_resolved": False,
        "claim_boundary": HF_OFFICIAL_JOB_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
    }
    if submit:
        submission_payload = _payload_for_submission(
            payload,
            token_resolver=token_resolver,
        )
        receipt["token_resolved"] = token_required
        job = (submitter or _submit_with_huggingface_hub)(submission_payload)
        receipt.update(_job_receipt_fields(job))
        receipt["submitted"] = True
    _write_receipt(output_path, receipt)
    return receipt


def _build_uv_script(config: HfOfficialScheduleValueJobConfig) -> str:
    upload_block = _artifact_upload_block(config)
    config_yaml = _dagster_config_yaml(config)
    attempt_manifest_json = _attempt_manifest_json(config)
    return dedent(
        f"""
        # /// script
        # dependencies = ["huggingface-hub"]
        # ///

        import os
        from pathlib import Path
        import shutil
        import subprocess
        import textwrap

        RUN_SLUG = {config.run_slug!r}
        REPO_URL = {config.repo_url!r}
        GIT_REF = {config.git_ref!r}
        ASSET_SELECTION = {OFFICIAL_SCHEDULE_VALUE_SELECTION!r}
        ARTIFACT_REPO_ID = {config.artifact_repo_id!r}

        workdir = Path.cwd() / "smart_arbitrage_hf_job"
        repo_dir = workdir / "repo"
        artifacts_dir = workdir / "artifacts" / RUN_SLUG
        dagster_home = workdir / "dagster_home"
        config_path = workdir / "official_schedule_value.yaml"

        if repo_dir.exists():
            shutil.rmtree(repo_dir)
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        dagster_home.mkdir(parents=True, exist_ok=True)

        subprocess.run(
            f"git clone --depth 1 --branch {{GIT_REF}} {{REPO_URL}} {{repo_dir}}",
            shell=True,
            check=True,
        )
        config_path.write_text(
            textwrap.dedent(
                {config_yaml!r}
            ).strip() + "\\n",
            encoding="utf-8",
        )
        env = os.environ.copy()
        env["DAGSTER_HOME"] = str(dagster_home)
        subprocess.run(
            "uv sync --extra dev --extra sota",
            cwd=repo_dir,
            env=env,
            shell=True,
            check=True,
        )
        subprocess.run(
            (
                "uv run dagster asset materialize "
                "-m smart_arbitrage.defs "
                f"--select {{ASSET_SELECTION}} "
                f"-c {{config_path}}"
            ),
            cwd=repo_dir,
            env=env,
            shell=True,
            check=True,
        )

        storage_dir = dagster_home / "storage"
        for asset_name in (
            "official_forecast_rolling_origin_benchmark_frame",
            "dfl_official_schedule_value_production_gate_frame",
        ):
            source = storage_dir / asset_name
            if source.exists():
                shutil.copy2(source, artifacts_dir / f"{{asset_name}}.pkl")
        (artifacts_dir / "claim_boundary.txt").write_text(
            "research/offline evidence only; not market execution\\n",
            encoding="utf-8",
        )
        (artifacts_dir / "official_evidence_attempt_manifest.json").write_text(
            {attempt_manifest_json!r},
            encoding="utf-8",
        )
        {upload_block}
        print(f"HF official schedule-value artifacts: {{artifacts_dir}}")
        """
    ).strip()


def _attempt_manifest_json(config: HfOfficialScheduleValueJobConfig) -> str:
    manifest = build_official_evidence_attempt_manifest(
        OfficialEvidenceAttemptConfig(
            attempt_kind="official_schedule_value",
            generated_at_iso=config.run_slug,
            total_anchors=config.total_anchors_per_tenant,
            batch_size=config.batch_size,
            anchor_batch_order=config.anchor_batch_order,
            enabled_official_models_csv=config.enabled_official_models_csv,
            nbeatsx_max_steps=config.nbeatsx_max_steps,
            tft_max_epochs=config.tft_max_epochs,
            asset_selection=OFFICIAL_SCHEDULE_VALUE_SELECTION,
            downstream_gate_enabled=True,
            downstream_selection="dfl_official_schedule_value_production_gate_frame",
            run_root="hf://official_schedule_value_jobs",
        )
    )
    return json.dumps(manifest, indent=2, sort_keys=True) + "\n"


def _payload_metadata(config: HfOfficialScheduleValueJobConfig) -> dict[str, object]:
    return {
        "schema_version": 1,
        "run_slug": config.run_slug,
        "artifact_repo_id": config.artifact_repo_id,
        "claim_boundary": HF_OFFICIAL_JOB_CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "flavor": config.flavor,
        "timeout": config.timeout,
        "estimated_timeout_cost_usd": _estimated_timeout_cost_usd(
            config.flavor,
            config.timeout,
        ),
    }


def _dagster_config_yaml(config: HfOfficialScheduleValueJobConfig) -> str:
    return f"""
    ops:
      official_forecast_rolling_origin_benchmark_frame:
        config:
          tenant_ids_csv: "client_001_kyiv_mall,client_002_lviv_office,client_003_dnipro_factory,client_004_kharkiv_hospital,client_005_odesa_hotel"
          max_eval_anchors_per_tenant: {config.total_anchors_per_tenant}
          anchor_batch_start_index: 0
          anchor_batch_size: {config.batch_size}
          anchor_batch_order: "{config.anchor_batch_order}"
          enabled_official_model_names_csv: "{config.enabled_official_models_csv}"
          merge_persisted_batches: false
          horizon_hours: 24
          nbeatsx_max_steps: {config.nbeatsx_max_steps}
          nbeatsx_random_seed: 20260511
          tft_max_epochs: {config.tft_max_epochs}
          tft_batch_size: 32
          tft_learning_rate: 0.005
          tft_hidden_size: 12
          tft_hidden_continuous_size: 6
    """


def _artifact_upload_block(config: HfOfficialScheduleValueJobConfig) -> str:
    if not config.artifact_repo_id.strip():
        return ""
    return dedent(
        """
        from huggingface_hub import HfApi

        if "HF_TOKEN" not in os.environ:
            raise RuntimeError("HF_TOKEN secret is required to upload artifacts.")
        HfApi().upload_folder(
            repo_id=ARTIFACT_REPO_ID,
            repo_type="dataset",
            folder_path=str(artifacts_dir),
            path_in_repo=RUN_SLUG,
        )
        """
    ).strip()


def _validate_config(config: HfOfficialScheduleValueJobConfig) -> None:
    if config.total_anchors_per_tenant < 1:
        raise ValueError("total_anchors_per_tenant must be positive.")
    if config.batch_size < 1:
        raise ValueError("batch_size must be positive.")
    if not config.enabled_official_models_csv.strip():
        raise ValueError("enabled_official_models_csv must not be blank.")
    if config.nbeatsx_max_steps < 1:
        raise ValueError("nbeatsx_max_steps must be positive.")
    if config.tft_max_epochs < 1:
        raise ValueError("tft_max_epochs must be positive.")
    if not config.run_slug.strip():
        raise ValueError("run_slug must not be blank.")


def _read_payload(payload_path: Path) -> dict[str, object]:
    if not payload_path.exists():
        raise FileNotFoundError(f"HF Jobs payload does not exist: {payload_path}")
    payload = json.loads(payload_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("HF Jobs payload JSON must be an object.")
    if "script" not in payload:
        raise ValueError("HF Jobs payload missing script.")
    return payload


def _payload_metadata_from_payload(payload: dict[str, object]) -> dict[str, object]:
    metadata = payload.get("metadata")
    if isinstance(metadata, dict):
        return {
            "run_slug": str(metadata.get("run_slug", _run_slug_from_script(payload))),
            "artifact_repo_id": str(metadata.get("artifact_repo_id", "")),
            "estimated_timeout_cost_usd": metadata.get(
                "estimated_timeout_cost_usd",
                _estimated_timeout_cost_usd(
                    str(payload.get("flavor", "")),
                    str(payload.get("timeout", "")),
                ),
            ),
        }
    return {
        "run_slug": _run_slug_from_script(payload),
        "artifact_repo_id": _artifact_repo_from_script(payload),
        "estimated_timeout_cost_usd": _estimated_timeout_cost_usd(
            str(payload.get("flavor", "")),
            str(payload.get("timeout", "")),
        ),
    }


def _run_slug_from_script(payload: dict[str, object]) -> str:
    return _script_constant(str(payload.get("script", "")), "RUN_SLUG")


def _artifact_repo_from_script(payload: dict[str, object]) -> str:
    return _script_constant(str(payload.get("script", "")), "ARTIFACT_REPO_ID")


def _script_constant(script: str, name: str) -> str:
    match = re.search(rf"^{name}\s*=\s*['\"]([^'\"]*)['\"]", script, flags=re.MULTILINE)
    return match.group(1) if match else ""


def _payload_requires_hf_token(payload: dict[str, object]) -> bool:
    secrets = payload.get("secrets")
    if not isinstance(secrets, dict):
        return False
    return any(value == "$HF_TOKEN" for value in secrets.values())


def _payload_for_submission(
    payload: dict[str, object],
    *,
    token_resolver: HfTokenResolver | None,
) -> dict[str, object]:
    submission_payload = json.loads(json.dumps(payload))
    secrets = submission_payload.get("secrets")
    if isinstance(secrets, dict) and any(value == "$HF_TOKEN" for value in secrets.values()):
        token = _resolve_hf_token(token_resolver)
        if not token:
            raise RuntimeError(
                "HF_TOKEN is required to submit a payload that uploads artifacts."
            )
        submission_payload["secrets"] = {
            key: token if value == "$HF_TOKEN" else value for key, value in secrets.items()
        }
    return submission_payload


def _resolve_hf_token(token_resolver: HfTokenResolver | None) -> str | None:
    if token_resolver is not None:
        return token_resolver()
    environment_token = os.environ.get("HF_TOKEN")
    if environment_token:
        return environment_token
    try:
        hub = importlib.import_module("huggingface_hub")
    except ImportError:
        return None
    get_token = getattr(hub, "get_token", None)
    if not callable(get_token):
        return None
    token = get_token()
    return str(token) if token else None


def _submit_with_huggingface_hub(payload: dict[str, object]) -> object:
    hub = importlib.import_module("huggingface_hub")
    run_uv_job = getattr(hub, "run_uv_job")
    secrets = payload.get("secrets")
    return run_uv_job(
        str(payload["script"]),
        flavor=str(payload.get("flavor", "")),
        timeout=str(payload.get("timeout", "")),
        secrets=secrets if isinstance(secrets, dict) else None,
    )


def _job_receipt_fields(job: Mapping[str, object] | object) -> dict[str, object]:
    job_id = _job_value(job, "id")
    status = _job_value(job, "status")
    status_stage = _job_value(status, "stage") if status is not None else None
    return {
        "job_id": str(job_id) if job_id is not None else None,
        "job_url": _job_url(job),
        "job_status": str(status_stage or status) if status is not None else None,
    }


def _job_url(job: Mapping[str, object] | object) -> str | None:
    value = _job_value(job, "url")
    return str(value) if value is not None else None


def _job_value(job: Mapping[str, object] | object | None, key: str) -> object | None:
    if job is None:
        return None
    if isinstance(job, Mapping):
        return job.get(key)
    return getattr(job, key, None)


def _write_receipt(output_path: Path, receipt: dict[str, object]) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(receipt, indent=2, sort_keys=True), encoding="utf-8")


def _estimated_timeout_cost_usd(flavor: str, timeout: str) -> float | None:
    hourly_price = HF_JOB_HOURLY_PRICE_BY_FLAVOR.get(flavor)
    timeout_hours = _timeout_hours(timeout)
    if hourly_price is None or timeout_hours is None:
        return None
    return round(hourly_price * timeout_hours, 2)


def _timeout_hours(timeout: str) -> float | None:
    match = re.fullmatch(r"(?P<value>\d+(?:\.\d+)?)(?P<unit>[smhd])", timeout.strip())
    if match is None:
        return None
    value = float(match.group("value"))
    unit = match.group("unit")
    if unit == "s":
        return value / 3600
    if unit == "m":
        return value / 60
    if unit == "h":
        return value
    if unit == "d":
        return value * 24
    return None
