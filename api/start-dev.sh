#!/usr/bin/env bash

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
REPO_ROOT="$(cd -- "$SCRIPT_DIR/.." >/dev/null 2>&1 && pwd)"
PYTHON_PATH="$REPO_ROOT/.venv/Scripts/python.exe"
PORT="${SMART_ARBITRAGE_API_PORT:-8000}"
SRC_PATH="$REPO_ROOT/src"

if [ ! -f "$PYTHON_PATH" ]; then
  echo "Project virtual environment not found at $PYTHON_PATH"
  exit 1
fi

cd "$REPO_ROOT" || exit 1

if [ -n "$PYTHONPATH" ]; then
  export PYTHONPATH="$REPO_ROOT:$SRC_PATH:$PYTHONPATH"
else
  export PYTHONPATH="$REPO_ROOT:$SRC_PATH"
fi

echo "Starting Smart Energy Arbitrage API"
echo "  URL: http://127.0.0.1:$PORT"
echo "  Docs: http://127.0.0.1:$PORT/docs"
echo "  OpenAPI: http://127.0.0.1:$PORT/openapi.json"
echo "  PYTHONPATH: $PYTHONPATH"

"$PYTHON_PATH" -m uvicorn api.main:app --host 127.0.0.1 --port "$PORT" --reload