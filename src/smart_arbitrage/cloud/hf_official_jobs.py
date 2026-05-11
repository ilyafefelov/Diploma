"""Hugging Face Jobs payloads for official forecast evidence runs."""

from __future__ import annotations

from dataclasses import dataclass
from textwrap import dedent
from typing import Literal

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
    }
    if config.artifact_repo_id.strip():
        payload["secrets"] = {"HF_TOKEN": "$HF_TOKEN"}
    return payload


def _build_uv_script(config: HfOfficialScheduleValueJobConfig) -> str:
    upload_block = _artifact_upload_block(config)
    config_yaml = _dagster_config_yaml(config)
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
        {upload_block}
        print(f"HF official schedule-value artifacts: {{artifacts_dir}}")
        """
    ).strip()


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
