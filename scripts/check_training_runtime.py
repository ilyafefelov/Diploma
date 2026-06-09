"""Report local and optional Docker training runtime acceleration status."""

from __future__ import annotations

import argparse
import json
import platform
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Sequence


CLAIM_BOUNDARY = "Offline Strategy Promotion evidence only; not market execution"


def _host_torch_status() -> dict[str, Any]:
    status: dict[str, Any] = {
        "python_executable": sys.executable,
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "torch_importable": False,
        "torch_version": None,
        "cuda_available": False,
        "cuda_device_count": 0,
        "cuda_devices": [],
        "error": None,
    }
    try:
        import torch
    except Exception as exc:  # pragma: no cover - depends on local environment
        status["error"] = repr(exc)
        return status

    status["torch_importable"] = True
    status["torch_version"] = torch.__version__
    status["cuda_available"] = bool(torch.cuda.is_available())
    if not status["cuda_available"]:
        return status

    device_count = torch.cuda.device_count()
    status["cuda_device_count"] = device_count
    devices: list[dict[str, Any]] = []
    for index in range(device_count):
        properties = torch.cuda.get_device_properties(index)
        devices.append(
            {
                "index": index,
                "name": torch.cuda.get_device_name(index),
                "total_memory_gb": round(properties.total_memory / (1024**3), 3),
                "major": properties.major,
                "minor": properties.minor,
            }
        )
    status["cuda_devices"] = devices
    return status


def _docker_torch_status(service: str, timeout_seconds: int) -> dict[str, Any]:
    snippet = (
        "import json, platform, sys\n"
        "status = {'python_executable': sys.executable, 'python_version': platform.python_version(), "
        "'torch_importable': False, 'torch_version': None, 'cuda_available': False, "
        "'cuda_device_count': 0, 'cuda_devices': [], 'error': None}\n"
        "try:\n"
        "    import torch\n"
        "    status['torch_importable'] = True\n"
        "    status['torch_version'] = torch.__version__\n"
        "    status['cuda_available'] = bool(torch.cuda.is_available())\n"
        "    status['cuda_device_count'] = torch.cuda.device_count() if status['cuda_available'] else 0\n"
        "    if status['cuda_available']:\n"
        "        status['cuda_devices'] = [torch.cuda.get_device_name(i) for i in range(torch.cuda.device_count())]\n"
        "except Exception as exc:\n"
        "    status['error'] = repr(exc)\n"
        "print(json.dumps(status, sort_keys=True))\n"
    )
    docker_command_text = "docker compose exec -T"
    command = [
        "docker",
        "compose",
        "exec",
        "-T",
        service,
        "uv",
        "run",
        "python",
        "-c",
        snippet,
    ]
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            check=False,
        )
    except Exception as exc:  # pragma: no cover - depends on Docker availability
        return {
            "available": False,
            "command": docker_command_text,
            "service": service,
            "returncode": None,
            "status": None,
            "error": repr(exc),
        }

    parsed_status: dict[str, Any] | None = None
    if result.returncode == 0:
        try:
            parsed_status = json.loads(result.stdout.strip().splitlines()[-1])
        except Exception as exc:  # pragma: no cover - defensive parse guard
            parsed_status = {"error": f"could not parse docker torch status: {exc!r}"}

    return {
        "available": result.returncode == 0,
        "command": docker_command_text,
        "service": service,
        "returncode": result.returncode,
        "status": parsed_status,
        "stdout_tail": result.stdout.strip().splitlines()[-5:],
        "stderr_tail": result.stderr.strip().splitlines()[-5:],
        "error": None if result.returncode == 0 else result.stderr.strip(),
    }


def build_runtime_receipt(
    *,
    include_docker: bool,
    docker_service: str,
    docker_timeout_seconds: int,
) -> dict[str, Any]:
    receipt: dict[str, Any] = {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "claim_boundary": CLAIM_BOUNDARY,
        "market_execution_enabled": False,
        "host": _host_torch_status(),
        "docker": None,
    }
    if include_docker:
        receipt["docker"] = _docker_torch_status(docker_service, docker_timeout_seconds)
    return receipt


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Report host and optional Docker torch/CUDA runtime status."
    )
    parser.add_argument("--output", type=Path, default=None)
    parser.add_argument("--include-docker", action="store_true")
    parser.add_argument("--docker-service", default="dagster-webserver")
    parser.add_argument("--docker-timeout-seconds", type=int, default=60)
    args = parser.parse_args(argv)

    receipt = build_runtime_receipt(
        include_docker=args.include_docker,
        docker_service=args.docker_service,
        docker_timeout_seconds=args.docker_timeout_seconds,
    )
    payload = json.dumps(receipt, indent=2, sort_keys=True)
    if args.output is not None:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
