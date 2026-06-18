#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"
PYTHON_PATH="$REPO_ROOT/.venv/bin/python"
WINDOWS_PYTHON_PATH="$REPO_ROOT/.venv/Scripts/python.exe"
SRC_PATH="$REPO_ROOT/src"
LOG_DIR="$REPO_ROOT/.tmp_runtime/local-start"

API_PORT="${SMART_ARBITRAGE_API_PORT:-8000}"
DASHBOARD_PORT="${SMART_ARBITRAGE_DASHBOARD_PORT:-64163}"
DOCKER_STARTUP_TIMEOUT_SECONDS="${DOCKER_STARTUP_TIMEOUT_SECONDS:-120}"
SKIP_COMPOSE=0
WITH_TELEMETRY=0

usage() {
  cat <<'EOF'
Usage: scripts/start-local-project.sh [options]

Options:
  --api-port PORT                         FastAPI port. Default: 8000.
  --dashboard-port PORT                   Nuxt dashboard port. Default: 64163.
  --docker-startup-timeout-seconds VALUE  Docker daemon wait timeout. Default: 120.
  --skip-compose                          Skip Docker Compose support services.
  --with-telemetry                        Start MQTT telemetry ingestor/publisher.
  -h, --help                              Show this help.
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --api-port)
      API_PORT="$2"
      shift 2
      ;;
    --api-port=*)
      API_PORT="${1#*=}"
      shift
      ;;
    --dashboard-port)
      DASHBOARD_PORT="$2"
      shift 2
      ;;
    --dashboard-port=*)
      DASHBOARD_PORT="${1#*=}"
      shift
      ;;
    --docker-startup-timeout-seconds)
      DOCKER_STARTUP_TIMEOUT_SECONDS="$2"
      shift 2
      ;;
    --docker-startup-timeout-seconds=*)
      DOCKER_STARTUP_TIMEOUT_SECONDS="${1#*=}"
      shift
      ;;
    --skip-compose)
      SKIP_COMPOSE=1
      shift
      ;;
    --with-telemetry)
      WITH_TELEMETRY=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

docker_daemon_ready() {
  docker info >/dev/null 2>&1
}

start_docker_if_available() {
  case "$(uname -s)" in
    Darwin*)
      if command_exists open; then
        echo "Docker daemon is not reachable; starting Docker Desktop."
        open -a Docker >/dev/null 2>&1 && return 0
      fi
      ;;
    Linux*)
      if command_exists systemctl; then
        echo "Docker daemon is not reachable; trying to start the docker service."
        if [ "$(id -u)" -eq 0 ]; then
          systemctl start docker >/dev/null 2>&1 && return 0
        elif command_exists sudo; then
          sudo -n systemctl start docker >/dev/null 2>&1 && return 0
        fi
      fi
      ;;
  esac

  return 1
}

wait_for_docker() {
  local deadline
  deadline=$(($(date +%s) + DOCKER_STARTUP_TIMEOUT_SECONDS))

  while [ "$(date +%s)" -lt "$deadline" ]; do
    if docker_daemon_ready; then
      return 0
    fi
    sleep 2
  done

  echo "Docker did not become ready within ${DOCKER_STARTUP_TIMEOUT_SECONDS} seconds. Start Docker manually or rerun with --skip-compose." >&2
  return 1
}

start_logged_process() {
  local log_prefix="$1"
  shift
  local stdout_path="$LOG_DIR/$log_prefix.out.log"
  local stderr_path="$LOG_DIR/$log_prefix.err.log"

  nohup "$@" >"$stdout_path" 2>"$stderr_path" &
  echo "$!"
}

if [ ! -f "$PYTHON_PATH" ] && [ -f "$WINDOWS_PYTHON_PATH" ]; then
  PYTHON_PATH="$WINDOWS_PYTHON_PATH"
fi

if [ ! -f "$PYTHON_PATH" ]; then
  echo "Project virtual environment not found." >&2
  echo "Expected one of:" >&2
  echo "  $REPO_ROOT/.venv/bin/python" >&2
  echo "  $WINDOWS_PYTHON_PATH" >&2
  echo "Run: uv sync --extra dev" >&2
  exit 1
fi

if ! command_exists npm; then
  echo "npm was not found on PATH. Install Node.js or open a shell where npm is available." >&2
  exit 1
fi

mkdir -p "$LOG_DIR"
cd "$REPO_ROOT"

is_port_listening() {
  local port="$1"
  "$PYTHON_PATH" - "$port" <<'PY' >/dev/null 2>&1
import socket
import sys

port = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
    sys.exit(sock.connect_ex(("127.0.0.1", port)))
PY
}

if [ "$SKIP_COMPOSE" -eq 0 ]; then
  if ! command_exists docker; then
    echo "docker was not found on PATH. Install Docker Desktop or rerun with --skip-compose." >&2
    exit 1
  fi

  if ! docker_daemon_ready; then
    if ! start_docker_if_available; then
      echo "Docker daemon is not reachable. Start Docker Desktop or the docker service manually, then rerun this script or use --skip-compose." >&2
      exit 1
    fi
    wait_for_docker
  fi

  compose_services=(
    "postgres"
    "mqtt"
    "mlflow"
    "dagster-webserver"
    "dagster-daemon"
  )
  if [ "$WITH_TELEMETRY" -eq 1 ]; then
    compose_services+=("telemetry-ingestor" "telemetry-publisher")
  fi

  echo "Starting Docker services: ${compose_services[*]}"
  docker compose up -d "${compose_services[@]}"
fi

if [ -n "${PYTHONPATH:-}" ]; then
  export PYTHONPATH="$REPO_ROOT:$SRC_PATH:$PYTHONPATH"
else
  export PYTHONPATH="$REPO_ROOT:$SRC_PATH"
fi

export SMART_ARBITRAGE_API_PORT="$API_PORT"
export NUXT_API_BASE="http://127.0.0.1:$API_PORT"
LOCAL_POSTGRES_DSN="postgresql://smart:arbitrage@localhost:5432/smart_arbitrage"
export SMART_ARBITRAGE_MARKET_DATA_DSN="${SMART_ARBITRAGE_MARKET_DATA_DSN:-$LOCAL_POSTGRES_DSN}"
export SMART_ARBITRAGE_FORECAST_DSN="${SMART_ARBITRAGE_FORECAST_DSN:-$LOCAL_POSTGRES_DSN}"

if is_port_listening "$API_PORT"; then
  echo "FastAPI already listening on port $API_PORT; leaving it running."
else
  api_process_id="$(start_logged_process "api-$API_PORT" "$PYTHON_PATH" -m uvicorn api.main:app --host 127.0.0.1 --port "$API_PORT" --reload)"
  echo "Started FastAPI process $api_process_id."
fi

if is_port_listening "$DASHBOARD_PORT"; then
  echo "Dashboard already listening on port $DASHBOARD_PORT; leaving it running."
else
  dashboard_process_id="$(start_logged_process "dashboard-$DASHBOARD_PORT" npm -C dashboard run dev -- --host 127.0.0.1 --port "$DASHBOARD_PORT")"
  echo "Started dashboard process $dashboard_process_id."
fi

cat <<EOF

Local URLs
  Dashboard: http://127.0.0.1:$DASHBOARD_PORT/operator
  API:       http://127.0.0.1:$API_PORT
  API docs:  http://127.0.0.1:$API_PORT/docs
  Dagster:   http://127.0.0.1:3001
  MLflow:    http://127.0.0.1:5000

Logs
  $LOG_DIR
EOF
