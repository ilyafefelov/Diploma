from __future__ import annotations

from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DAGSTER_INSTANCE_MOUNTS = {
    "dagster-history:/opt/dagster/dagster_home/history",
    "dagster-schedules:/opt/dagster/dagster_home/schedules",
    "dagster-storage:/opt/dagster/dagster_home/storage",
}


def test_dagster_webserver_and_daemon_share_local_instance_state() -> None:
    compose = yaml.safe_load((PROJECT_ROOT / "docker-compose.yaml").read_text(encoding="utf-8"))

    webserver_volumes = set(compose["services"]["dagster-webserver"]["volumes"])
    daemon_volumes = set(compose["services"]["dagster-daemon"]["volumes"])

    assert DAGSTER_INSTANCE_MOUNTS <= webserver_volumes
    assert DAGSTER_INSTANCE_MOUNTS <= daemon_volumes


def test_backend_image_installs_sota_forecast_adapters() -> None:
    dockerfile = (PROJECT_ROOT / "docker" / "backend.Dockerfile").read_text(encoding="utf-8")

    sync_commands = [
        line.strip()
        for line in dockerfile.splitlines()
        if line.strip().startswith("RUN uv sync")
    ]

    assert sync_commands
    assert all("--extra sota" in command for command in sync_commands)
